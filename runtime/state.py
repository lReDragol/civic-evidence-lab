from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config.db_utils import SCHEMA_PATH, exec_schema
from config.source_health import (
    acceptance_mode,
    fixture_checksum,
    load_source_health_manifest,
    manifest_entry,
    smoke_fixture,
)


DAEMON_JOB_ID = "__daemon__"

SOURCE_HEALTH_MATRIX: dict[str, dict[str, str]] = {
    "government_news": {
        "primary_transport": "requests",
        "fallback_transport": "playwright",
        "expected_cadence": "daily",
        "quality_expectations": "html_listing",
        "fixture_sample": "https://government.ru/news/",
    },
    "government_docs": {
        "primary_transport": "requests",
        "fallback_transport": "playwright",
        "expected_cadence": "daily",
        "quality_expectations": "document_listing",
        "fixture_sample": "https://government.ru/docs/",
    },
    "kremlin_transcripts": {
        "primary_transport": "requests",
        "fallback_transport": "playwright",
        "expected_cadence": "daily",
        "quality_expectations": "html_listing",
        "fixture_sample": "https://kremlin.ru/events/president/transcripts/",
    },
    "pravo_gov": {
        "primary_transport": "requests",
        "fallback_transport": "requests_insecure",
        "expected_cadence": "daily",
        "quality_expectations": "official_documents",
        "fixture_sample": "https://pravo.gov.ru/",
    },
    "publication_pravo_https": {
        "primary_transport": "requests_insecure",
        "fallback_transport": "http",
        "expected_cadence": "daily",
        "quality_expectations": "official_publication",
        "fixture_sample": "https://publication.pravo.gov.ru/",
    },
    "publication_pravo_http": {
        "primary_transport": "http",
        "fallback_transport": "requests_insecure",
        "expected_cadence": "daily",
        "quality_expectations": "official_publication",
        "fixture_sample": "http://publication.pravo.gov.ru/",
    },
    "rosreestr_press_archive": {
        "primary_transport": "requests_insecure",
        "fallback_transport": "playwright",
        "expected_cadence": "daily",
        "quality_expectations": "archive_listing",
        "fixture_sample": "https://rosreestr.gov.ru/press/archive/",
    },
    "rosreestr_press_news": {
        "primary_transport": "requests_insecure",
        "fallback_transport": "playwright",
        "expected_cadence": "daily",
        "quality_expectations": "news_listing",
        "fixture_sample": "https://rosreestr.gov.ru/site/press/news/",
    },
    "senators": {
        "primary_transport": "requests",
        "fallback_transport": "playwright",
        "expected_cadence": "weekly",
        "quality_expectations": "directory_listing",
        "fixture_sample": "http://council.gov.ru/events/news/",
    },
}


def _source_health_defaults(source_key: str, settings: dict[str, Any] | None = None) -> dict[str, Any]:
    base = dict(SOURCE_HEALTH_MATRIX.get(source_key, {}))
    entry = manifest_entry(source_key, settings=settings)
    if entry:
        base.update(
            {
                "acceptance_mode": acceptance_mode(entry),
                "primary_urls": entry.get("primary_urls"),
                "fallback_urls": entry.get("fallback_urls"),
                "fixture_strategy": entry.get("fixture_strategy"),
                "expected_cadence": entry.get("expected_cadence"),
                "quality_expectations": entry.get("quality_expectations"),
                "required_for_gate": bool(entry.get("required_for_gate")),
            }
        )
        if entry.get("primary_urls") and not base.get("fixture_sample"):
            base["fixture_sample"] = (entry.get("primary_urls") or [None])[0]
    return {key: value for key, value in base.items() if value not in (None, "", [], {})}


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def now_iso() -> str:
    return utc_now_naive().isoformat()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    raw = raw.rstrip("Z")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def json_dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def json_loads(value: str | None, default: Any):
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _classify_failure_class(*, error_text: str | None = None, http_status: int | None = None) -> str | None:
    lowered = (error_text or "").lower()
    if http_status in {403, 429}:
        return "rate_limited" if http_status == 429 else "http_forbidden"
    if http_status == 404:
        return "not_found"
    if http_status and http_status >= 500:
        return "upstream_http"
    if "timeout" in lowered:
        return "timeout"
    if "tls" in lowered or "ssl" in lowered or "certificate" in lowered or "cert" in lowered:
        return "tls"
    if "404" in lowered:
        return "not_found"
    if "403" in lowered:
        return "http_forbidden"
    if "429" in lowered:
        return "rate_limited"
    if "connection" in lowered or "reset by peer" in lowered:
        return "connection"
    if "snapshot" in lowered:
        return "missing_snapshot"
    if "placeholder" in lowered or "map.svg" in lowered or "banner" in lowered or "icon" in lowered:
        return "bad_asset"
    if lowered:
        return "runtime_error"
    return None


def _quality_state_from_health(*, success: bool | None, state: str | None, last_error: str | None, warnings: list[str] | None = None) -> tuple[str, str | None, str | None]:
    warning_text = "; ".join(str(item) for item in (warnings or []) if item)
    combined_error = "; ".join(part for part in (last_error, warning_text) if part) or None
    failure_class = _classify_failure_class(error_text=combined_error)
    if success is False or state == "degraded":
        return "degraded", combined_error or "healthcheck_failed", failure_class
    if warning_text:
        lowered = warning_text.lower()
        if any(token in lowered for token in ("snapshot", "map.svg", "placeholder", "404", "ssl", "tls", "cert", "timeout")):
            return "degraded", warning_text, _classify_failure_class(error_text=warning_text) or failure_class
        return "warning", warning_text, failure_class
    return "ok", None, None


def ensure_runtime_schema(conn: sqlite3.Connection):
    exec_schema(conn, SCHEMA_PATH)


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def set_runtime_metadata(conn: sqlite3.Connection, key: str, value: Any):
    value_text = None if isinstance(value, (dict, list)) else (None if value is None else str(value))
    value_json = json_dumps(value) if isinstance(value, (dict, list, bool, int, float)) else None
    conn.execute(
        """
        INSERT INTO runtime_metadata(key, value_text, value_json, updated_at)
        VALUES(?,?,?,?)
        ON CONFLICT(key) DO UPDATE SET
            value_text=excluded.value_text,
            value_json=excluded.value_json,
            updated_at=excluded.updated_at
        """,
        (key, value_text, value_json, now_iso()),
    )
    conn.commit()


def get_runtime_metadata(conn: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = conn.execute(
        "SELECT value_text, value_json FROM runtime_metadata WHERE key=?",
        (key,),
    ).fetchone()
    if not row:
        return default
    value_text, value_json = row
    if value_json:
        return json_loads(value_json, default)
    return value_text if value_text is not None else default


def request_daemon_stop(conn: sqlite3.Connection, value: bool = True):
    set_runtime_metadata(conn, "daemon_stop_requested", bool(value))


def daemon_stop_requested(conn: sqlite3.Connection) -> bool:
    return bool(get_runtime_metadata(conn, "daemon_stop_requested", False))


def acquire_job_lease(
    conn: sqlite3.Connection,
    job_id: str,
    lease_owner: str,
    *,
    ttl_seconds: int = 1200,
    payload: dict[str, Any] | None = None,
    force: bool = False,
) -> bool:
    now_dt = datetime.now(timezone.utc)
    now_text = now_iso()
    row = conn.execute(
        "SELECT lease_owner, expires_at FROM job_leases WHERE job_id=?",
        (job_id,),
    ).fetchone()
    if row and not force:
        existing_owner, expires_at = row
        expires_dt = parse_iso(expires_at)
        if expires_dt and expires_dt > now_dt and existing_owner != lease_owner:
            return False

    expires_at = (utc_now_naive() + timedelta(seconds=max(30, int(ttl_seconds)))).isoformat()
    conn.execute(
        """
        INSERT INTO job_leases(job_id, lease_owner, started_at, heartbeat_at, expires_at, payload_json)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(job_id) DO UPDATE SET
            lease_owner=excluded.lease_owner,
            heartbeat_at=excluded.heartbeat_at,
            expires_at=excluded.expires_at,
            payload_json=excluded.payload_json
        """,
        (
            job_id,
            lease_owner,
            now_text,
            now_text,
            expires_at,
            json_dumps(payload or {}),
        ),
    )
    conn.commit()
    return True


def heartbeat_job_lease(
    conn: sqlite3.Connection,
    job_id: str,
    lease_owner: str,
    *,
    ttl_seconds: int = 1200,
):
    now = utc_now_naive()
    expires_at = (now + timedelta(seconds=max(30, int(ttl_seconds)))).isoformat()
    conn.execute(
        """
        UPDATE job_leases
        SET heartbeat_at=?, expires_at=?
        WHERE job_id=? AND lease_owner=?
        """,
        (now.isoformat(), expires_at, job_id, lease_owner),
    )
    if table_exists(conn, "job_runs"):
        conn.execute(
            """
            UPDATE job_runs
            SET heartbeat_at=?
            WHERE job_id=? AND owner=? AND status='running' AND finished_at IS NULL
            """,
            (now.isoformat(), job_id, lease_owner),
        )
    conn.commit()


def release_job_lease(conn: sqlite3.Connection, job_id: str, lease_owner: str | None = None):
    if lease_owner:
        conn.execute(
            "DELETE FROM job_leases WHERE job_id=? AND lease_owner=?",
            (job_id, lease_owner),
        )
    else:
        conn.execute("DELETE FROM job_leases WHERE job_id=?", (job_id,))
    conn.commit()


def active_job_lease(conn: sqlite3.Connection, job_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT job_id, lease_owner, started_at, heartbeat_at, expires_at, payload_json FROM job_leases WHERE job_id=?",
        (job_id,),
    ).fetchone()
    if not row:
        return None
    expires_dt = parse_iso(row[4])
    if expires_dt and expires_dt <= datetime.now(timezone.utc):
        return None
    return {
        "job_id": row[0],
        "lease_owner": row[1],
        "started_at": row[2],
        "heartbeat_at": row[3],
        "expires_at": row[4],
        "payload": json_loads(row[5], {}),
    }


def start_job_run(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    trigger_mode: str,
    requested_by: str,
    owner: str,
    pipeline_version: str | None = None,
    pipeline_run_id: int | None = None,
    attempt_no: int = 1,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO job_runs(
            job_id, trigger_mode, requested_by, owner, pipeline_version,
            pipeline_run_id, attempt_no, status, started_at
        ) VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (
            job_id,
            trigger_mode,
            requested_by,
            owner,
            pipeline_version,
            pipeline_run_id,
            attempt_no,
            "running",
            now_iso(),
        ),
    )
    conn.commit()
    run_id = int(cur.lastrowid)
    record_runtime_event(
        conn,
        level="info",
        event_type="job_started",
        stage="runtime",
        job_id=job_id,
        job_run_id=run_id,
        pipeline_run_id=pipeline_run_id,
        message=f"{job_id} started",
        payload={"trigger_mode": trigger_mode, "requested_by": requested_by, "owner": owner},
    )
    return run_id


def finish_job_run(conn: sqlite3.Connection, run_id: int, result: dict[str, Any]):
    status = "ok" if result.get("ok") else "failed"
    error_summary = None
    fatal_errors = result.get("fatal_errors") or []
    retriable_errors = result.get("retriable_errors") or []
    if fatal_errors:
        error_summary = "; ".join(str(item) for item in fatal_errors[:3])
    elif retriable_errors:
        error_summary = "; ".join(str(item) for item in retriable_errors[:3])

    started_row = conn.execute("SELECT started_at FROM job_runs WHERE id=?", (run_id,)).fetchone()
    started_dt = parse_iso(started_row[0]) if started_row else None
    finished_at = result.get("finished_at") or now_iso()
    finished_dt = parse_iso(finished_at)
    duration_ms = None
    if started_dt and finished_dt:
        duration_ms = int(max(0, (finished_dt - started_dt).total_seconds() * 1000))
    warnings = result.get("warnings") or []
    artifacts = result.get("artifacts") or {}
    items_skipped = int(result.get("items_skipped") or artifacts.get("items_skipped") or 0)
    items_failed = int(result.get("items_failed") or artifacts.get("items_failed") or len(fatal_errors))
    duplicate_items = int(result.get("duplicate_items") or artifacts.get("duplicate_items") or 0)
    last_message = str(result.get("last_message") or error_summary or status)

    conn.execute(
        """
        UPDATE job_runs
        SET status=?,
            finished_at=?,
            items_seen=?,
            items_new=?,
            items_updated=?,
            items_skipped=?,
            items_failed=?,
            duplicate_items=?,
            heartbeat_at=?,
            duration_ms=?,
            warnings_count=?,
            fatal_errors_count=?,
            retriable_errors_count=?,
            last_message=?,
            warnings_json=?,
            retriable_errors_json=?,
            fatal_errors_json=?,
            next_cursor=?,
            health_json=?,
            artifacts_json=?,
            error_summary=?
        WHERE id=?
        """,
        (
            status,
            finished_at,
            int(result.get("items_seen") or 0),
            int(result.get("items_new") or 0),
            int(result.get("items_updated") or 0),
            items_skipped,
            items_failed,
            duplicate_items,
            now_iso(),
            duration_ms,
            len(warnings),
            len(fatal_errors),
            len(retriable_errors),
            last_message,
            json_dumps(warnings),
            json_dumps(result.get("retriable_errors") or []),
            json_dumps(result.get("fatal_errors") or []),
            result.get("next_cursor"),
            json_dumps(result.get("health") or {}),
            json_dumps(artifacts),
            error_summary,
            run_id,
        ),
    )
    conn.commit()
    record_runtime_event(
        conn,
        level="info" if status == "ok" else "error",
        event_type="job_finished",
        stage="runtime",
        job_id=str(result.get("job_id") or ""),
        job_run_id=run_id,
        message=f"{result.get('job_id') or 'job'} {status}",
        error_type=None if status == "ok" else "job_failed",
        payload={
            "status": status,
            "items_seen": result.get("items_seen", 0),
            "items_new": result.get("items_new", 0),
            "items_updated": result.get("items_updated", 0),
            "items_skipped": items_skipped,
            "items_failed": items_failed,
            "duplicate_items": duplicate_items,
            "duration_ms": duration_ms,
        },
    )


def recover_abandoned_runs(conn: sqlite3.Connection, *, stale_seconds: int = 1800) -> dict[str, int]:
    now = datetime.now(timezone.utc)
    abandoned = 0
    released = 0

    for row in conn.execute(
        "SELECT id, job_id, owner, started_at FROM job_runs WHERE status='running' AND finished_at IS NULL"
    ).fetchall():
        run_id, job_id, owner, started_at = row
        lease = active_job_lease(conn, job_id)
        started_dt = parse_iso(started_at)
        if lease and lease.get("lease_owner") == owner:
            continue
        if started_dt and (now - started_dt).total_seconds() < stale_seconds:
            continue
        conn.execute(
            """
            UPDATE job_runs
            SET status='abandoned', finished_at=?, error_summary=COALESCE(error_summary, 'Recovered as abandoned')
            WHERE id=?
            """,
            (now_iso(), run_id),
        )
        abandoned += 1

    for row in conn.execute("SELECT job_id, lease_owner, expires_at FROM job_leases").fetchall():
        job_id, lease_owner, expires_at = row
        expires_dt = parse_iso(expires_at)
        if expires_dt and expires_dt <= now:
            conn.execute(
                "DELETE FROM job_leases WHERE job_id=? AND lease_owner=?",
                (job_id, lease_owner),
            )
            released += 1

    conn.commit()
    return {"abandoned_runs": abandoned, "released_leases": released}


def force_recover_job(conn: sqlite3.Connection, job_id: str, *, reason: str = "Force recovered") -> dict[str, int]:
    abandoned = conn.execute(
        """
        UPDATE job_runs
        SET status='abandoned', finished_at=?, error_summary=COALESCE(error_summary, ?)
        WHERE job_id=? AND status='running' AND finished_at IS NULL
        """,
        (now_iso(), reason, job_id),
    ).rowcount
    released = conn.execute("DELETE FROM job_leases WHERE job_id=?", (job_id,)).rowcount
    conn.commit()
    return {"abandoned_runs": int(abandoned or 0), "released_leases": int(released or 0)}


def start_pipeline_run(
    conn: sqlite3.Connection,
    *,
    pipeline_version: str,
    mode: str,
    requested_by: str,
    stages: list[str],
) -> int:
    cur = conn.execute(
        """
        INSERT INTO pipeline_runs(pipeline_version, mode, status, requested_by, started_at, stages_json)
        VALUES(?,?,?,?,?,?)
        """,
        (
            pipeline_version,
            mode,
            "running",
            requested_by,
            now_iso(),
            json_dumps(stages),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def finish_pipeline_run(
    conn: sqlite3.Connection,
    pipeline_run_id: int,
    *,
    ok: bool,
    result: dict[str, Any],
):
    errors = result.get("fatal_errors") or result.get("retriable_errors") or []
    conn.execute(
        """
        UPDATE pipeline_runs
        SET status=?, finished_at=?, result_json=?, error_summary=?
        WHERE id=?
        """,
        (
            "ok" if ok else "failed",
            now_iso(),
            json_dumps(result),
            "; ".join(str(item) for item in errors[:3]) if errors else None,
            pipeline_run_id,
        ),
    )
    conn.commit()


def has_recent_successful_run(conn: sqlite3.Connection, job_id: str, within_hours: int = 48) -> bool:
    cutoff = (utc_now_naive() - timedelta(hours=max(1, within_hours))).isoformat()
    row = conn.execute(
        "SELECT 1 FROM job_runs WHERE job_id=? AND status='ok' AND finished_at>=? LIMIT 1",
        (job_id, cutoff),
    ).fetchone()
    return row is not None


def latest_successful_pipeline_version(conn: sqlite3.Connection, mode: str | None = None) -> str | None:
    if mode:
        row = conn.execute(
            "SELECT pipeline_version FROM pipeline_runs WHERE status='ok' AND mode=? ORDER BY id DESC LIMIT 1",
            (mode,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT pipeline_version FROM pipeline_runs WHERE status='ok' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return row[0] if row else None


def update_source_sync_state(
    conn: sqlite3.Connection,
    *,
    source_key: str,
    source_id: int | None = None,
    success: bool | None = None,
    state: str | None = None,
    last_cursor: str | None = None,
    last_external_id: str | None = None,
    last_etag: str | None = None,
    last_hash: str | None = None,
    last_http_status: int | None = None,
    transport_mode: str | None = None,
    last_error: str | None = None,
    quality_state: str | None = None,
    quality_issue: str | None = None,
    failure_class: str | None = None,
    metadata: dict[str, Any] | None = None,
    current_job_id: str | None = None,
    is_collecting: bool | None = None,
    current_channel: str | None = None,
    current_telegram_session: str | None = None,
    items_current_run: int | None = None,
    items_today: int | None = None,
    duplicates_current_run: int | None = None,
    failed_items_current_run: int | None = None,
):
    existing = conn.execute(
        """
        SELECT consecutive_failures, metadata_json, quality_state, quality_issue, failure_class
        FROM source_sync_state
        WHERE source_key=?
        """,
        (source_key,),
    ).fetchone()
    consecutive_failures = int(existing[0]) if existing else 0
    if success is True:
        consecutive_failures = 0
    elif success is False:
        consecutive_failures += 1

    merged_metadata = json_loads(existing[1], {}) if existing and existing[1] else {}
    if metadata:
        merged_metadata.update(metadata)

    now = now_iso()
    resolved_state = state
    if resolved_state is None:
        if success is True:
            resolved_state = "ok"
        elif success is False and consecutive_failures >= 3:
            resolved_state = "degraded"
        elif success is False:
            resolved_state = "warning"
    if success is True:
        resolved_quality_state = quality_state or "ok"
        resolved_quality_issue = quality_issue
        resolved_failure_class = failure_class
    else:
        resolved_quality_state = quality_state or (existing[2] if existing and existing[2] else None)
        resolved_quality_issue = quality_issue if quality_issue is not None else (existing[3] if existing and existing[3] else None)
        resolved_failure_class = failure_class or (existing[4] if existing and existing[4] else None)
    if resolved_quality_state is None:
        resolved_quality_state = "ok" if success is True else "degraded" if resolved_state == "degraded" else "warning" if resolved_state == "warning" else "unknown"

    conn.execute(
        """
        INSERT INTO source_sync_state(
            source_key, source_id, state, quality_state, quality_issue, failure_class, last_success_at, last_attempt_at,
            consecutive_failures, last_cursor, last_external_id, last_etag, last_hash,
            last_http_status, transport_mode, last_error, current_job_id, is_collecting,
            current_channel, current_telegram_session, items_current_run, items_today,
            duplicates_current_run, failed_items_current_run, heartbeat_at, metadata_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(source_key) DO UPDATE SET
            source_id=COALESCE(excluded.source_id, source_sync_state.source_id),
            state=COALESCE(excluded.state, source_sync_state.state),
            quality_state=COALESCE(excluded.quality_state, source_sync_state.quality_state),
            quality_issue=CASE WHEN excluded.state='ok' THEN excluded.quality_issue ELSE COALESCE(excluded.quality_issue, source_sync_state.quality_issue) END,
            failure_class=CASE WHEN excluded.state='ok' THEN excluded.failure_class ELSE COALESCE(excluded.failure_class, source_sync_state.failure_class) END,
            last_success_at=COALESCE(excluded.last_success_at, source_sync_state.last_success_at),
            last_attempt_at=excluded.last_attempt_at,
            consecutive_failures=excluded.consecutive_failures,
            last_cursor=COALESCE(excluded.last_cursor, source_sync_state.last_cursor),
            last_external_id=COALESCE(excluded.last_external_id, source_sync_state.last_external_id),
            last_etag=COALESCE(excluded.last_etag, source_sync_state.last_etag),
            last_hash=COALESCE(excluded.last_hash, source_sync_state.last_hash),
            last_http_status=COALESCE(excluded.last_http_status, source_sync_state.last_http_status),
            transport_mode=COALESCE(excluded.transport_mode, source_sync_state.transport_mode),
            last_error=excluded.last_error,
            current_job_id=COALESCE(excluded.current_job_id, source_sync_state.current_job_id),
            is_collecting=COALESCE(excluded.is_collecting, source_sync_state.is_collecting),
            current_channel=COALESCE(excluded.current_channel, source_sync_state.current_channel),
            current_telegram_session=COALESCE(excluded.current_telegram_session, source_sync_state.current_telegram_session),
            items_current_run=COALESCE(excluded.items_current_run, source_sync_state.items_current_run),
            items_today=COALESCE(excluded.items_today, source_sync_state.items_today),
            duplicates_current_run=COALESCE(excluded.duplicates_current_run, source_sync_state.duplicates_current_run),
            failed_items_current_run=COALESCE(excluded.failed_items_current_run, source_sync_state.failed_items_current_run),
            heartbeat_at=COALESCE(excluded.heartbeat_at, source_sync_state.heartbeat_at),
            metadata_json=excluded.metadata_json
        """,
        (
            source_key,
            source_id,
            resolved_state,
            resolved_quality_state,
            resolved_quality_issue,
            resolved_failure_class,
            now if success else None,
            now,
            consecutive_failures,
            last_cursor,
            last_external_id,
            last_etag,
            last_hash,
            last_http_status,
            transport_mode,
            last_error,
            current_job_id,
            int(is_collecting) if is_collecting is not None else None,
            current_channel,
            current_telegram_session,
            items_current_run,
            items_today,
            duplicates_current_run,
            failed_items_current_run,
            now,
            json_dumps(merged_metadata),
        ),
    )
    conn.commit()


def record_runtime_event(
    conn: sqlite3.Connection,
    *,
    level: str = "info",
    event_type: str,
    stage: str | None = None,
    job_id: str | None = None,
    job_run_id: int | None = None,
    pipeline_run_id: int | None = None,
    source_key: str | None = None,
    source_type: str | None = None,
    telegram_session: str | None = None,
    channel: str | None = None,
    provider: str | None = None,
    model: str | None = None,
    item_id: int | None = None,
    raw_item_id: int | None = None,
    content_item_id: int | None = None,
    message: str | None = None,
    error_type: str | None = None,
    traceback_text: str | None = None,
    payload: dict[str, Any] | None = None,
) -> int | None:
    if not table_exists(conn, "runtime_events"):
        return None
    cur = conn.execute(
        """
        INSERT INTO runtime_events(
            created_at, level, event_type, stage, job_id, job_run_id, pipeline_run_id,
            source_key, source_type, telegram_session, channel, provider, model,
            item_id, raw_item_id, content_item_id, message, error_type, traceback_text, payload_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            now_iso(),
            level,
            event_type,
            stage,
            job_id,
            job_run_id,
            pipeline_run_id,
            source_key,
            source_type,
            telegram_session,
            channel,
            provider,
            model,
            item_id,
            raw_item_id,
            content_item_id,
            message,
            error_type,
            traceback_text,
            json_dumps(payload or {}),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def record_processing_skip(
    conn: sqlite3.Connection,
    *,
    stage: str,
    reason: str,
    source_key: str | None = None,
    content_item_id: int | None = None,
    raw_item_id: int | None = None,
    external_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> int | None:
    if not table_exists(conn, "processing_skips"):
        return None
    cur = conn.execute(
        """
        INSERT INTO processing_skips(stage, reason, source_key, content_item_id, raw_item_id, external_id, payload_json)
        VALUES(?,?,?,?,?,?,?)
        """,
        (stage, reason, source_key, content_item_id, raw_item_id, external_id, json_dumps(payload or {})),
    )
    conn.commit()
    record_runtime_event(
        conn,
        level="warning",
        event_type="processing_skip",
        stage=stage,
        source_key=source_key,
        content_item_id=content_item_id,
        raw_item_id=raw_item_id,
        message=f"{stage} skipped: {reason}",
        payload={"reason": reason, "external_id": external_id, **(payload or {})},
    )
    return int(cur.lastrowid)


def record_source_health_report(
    conn: sqlite3.Connection,
    report: dict[str, Any],
    *,
    transport_mode: str = "healthcheck",
    settings: dict[str, Any] | None = None,
) -> dict[str, int]:
    inserted = 0
    degraded = 0
    manifest = load_source_health_manifest(settings) if settings else {}
    for item in report.get("items", []):
        source_key = str(item.get("source") or item.get("url") or f"source-{inserted}")
        url = item.get("url")
        source_id = None
        if table_exists(conn, "sources") and url:
            row = conn.execute("SELECT id FROM sources WHERE url=? LIMIT 1", (url,)).fetchone()
            source_id = int(row[0]) if row else None

        health_meta = _source_health_defaults(source_key, settings=settings)
        health_meta.update(
            {
                "health_title": item.get("title"),
                "final_url": item.get("final_url"),
            }
        )
        fixture_smoke = item.get("fixture_smoke")
        if manifest and (not isinstance(fixture_smoke, dict) or not fixture_smoke):
            fixture_smoke = smoke_fixture(source_key, settings=settings, manifest=manifest)
        if fixture_smoke:
            health_meta["fixture_smoke"] = fixture_smoke
            if fixture_smoke.get("ok"):
                fixture_id = register_source_fixture(
                    conn,
                    source_key=source_key,
                    fixture_kind="archive_fixture" if fixture_smoke.get("archive_derived") else "local_fixture",
                    origin_url=url,
                    archive_url=((item.get("archive_url") or None) if isinstance(item, dict) else None),
                    local_path=fixture_smoke.get("fixture_path"),
                    metadata={
                        "acceptance_mode": fixture_smoke.get("acceptance_mode"),
                        "quality_expectations": health_meta.get("quality_expectations"),
                    },
                )
                health_meta["fixture_id"] = fixture_id
                health_meta["fallback_used"] = "archive" if fixture_smoke.get("archive_derived") else "fixture"
                health_meta["archive_derived"] = bool(fixture_smoke.get("archive_derived"))
        failure_class = _classify_failure_class(
            error_text=str(item.get("error") or ""),
            http_status=int(item.get("status")) if item.get("status") is not None else None,
        )
        effective_success = bool(item.get("ok")) or bool(fixture_smoke and fixture_smoke.get("ok"))
        effective_state = "degraded" if not effective_success else "ok"
        quality_state, quality_issue, resolved_failure_class = _quality_state_from_health(
            success=effective_success,
            state=effective_state,
            last_error=item.get("error"),
        )
        conn.execute(
            """
            INSERT INTO source_health_checks(
                source_key, source_id, checked_at, url, ok, status_code, elapsed_sec, final_url,
                content_type, length, title, link_count, transport_mode, error, payload_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                source_key,
                source_id,
                item.get("checked_at") or now_iso(),
                url,
                1 if item.get("ok") else 0,
                item.get("status"),
                item.get("elapsed_sec"),
                item.get("final_url"),
                item.get("content_type"),
                int(item.get("length") or 0),
                item.get("title"),
                int(item.get("link_count") or 0),
                transport_mode,
                item.get("error"),
                json_dumps(item),
            ),
        )
        update_source_sync_state(
            conn,
            source_key=source_key,
            source_id=source_id,
            success=effective_success,
            state=effective_state,
            last_http_status=item.get("status"),
            transport_mode=transport_mode,
            last_error=None if effective_success else item.get("error"),
            quality_state=quality_state,
            quality_issue=quality_issue,
            failure_class=resolved_failure_class or failure_class,
            metadata=health_meta,
        )
        inserted += 1
        if not item.get("ok"):
            degraded += 1
    conn.commit()
    return {"inserted": inserted, "degraded": degraded}


def register_source_fixture(
    conn: sqlite3.Connection,
    *,
    source_key: str,
    fixture_kind: str,
    local_path: str | None,
    origin_url: str | None = None,
    archive_url: str | None = None,
    checksum: str | None = None,
    captured_at: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> int | None:
    if not local_path or not table_exists(conn, "source_fixtures"):
        return None
    path = Path(str(local_path))
    if not path.exists():
        return None
    resolved_checksum = checksum or fixture_checksum(path)
    row = conn.execute(
        """
        SELECT id
        FROM source_fixtures
        WHERE source_key=? AND local_path=? AND is_active=1
        LIMIT 1
        """,
        (source_key, str(path)),
    ).fetchone()
    if row:
        fixture_id = int(row[0])
        conn.execute(
            """
            UPDATE source_fixtures
            SET fixture_kind=?, origin_url=?, archive_url=?, checksum=?, captured_at=?, metadata_json=?, is_active=1
            WHERE id=?
            """,
            (
                fixture_kind,
                origin_url,
                archive_url,
                resolved_checksum,
                captured_at or now_iso(),
                json_dumps(metadata or {}),
                fixture_id,
            ),
        )
        conn.commit()
        return fixture_id
    cur = conn.execute(
        """
        INSERT INTO source_fixtures(
            source_key, fixture_kind, origin_url, archive_url, local_path, checksum, captured_at, is_active, metadata_json
        ) VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (
            source_key,
            fixture_kind,
            origin_url,
            archive_url,
            str(path),
            resolved_checksum,
            captured_at or now_iso(),
            1,
            json_dumps(metadata or {}),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def active_source_fixtures(conn: sqlite3.Connection, source_key: str) -> list[dict[str, Any]]:
    if not table_exists(conn, "source_fixtures"):
        return []
    rows = conn.execute(
        """
        SELECT id, source_key, fixture_kind, origin_url, archive_url, local_path, checksum, captured_at, is_active, metadata_json
        FROM source_fixtures
        WHERE source_key=? AND is_active=1
        ORDER BY id DESC
        """,
        (source_key,),
    ).fetchall()
    return [
        {
            "id": int(row[0]),
            "source_key": row[1],
            "fixture_kind": row[2],
            "origin_url": row[3],
            "archive_url": row[4],
            "local_path": row[5],
            "checksum": row[6],
            "captured_at": row[7],
            "is_active": int(row[8] or 0),
            "metadata": json_loads(row[9], {}),
        }
        for row in rows
    ]


def record_dead_letter(
    conn: sqlite3.Connection,
    *,
    failure_stage: str,
    source_key: str | None = None,
    source_id: int | None = None,
    raw_item_id: int | None = None,
    external_id: str | None = None,
    attachment_id: int | None = None,
    content_item_id: int | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    payload: dict[str, Any] | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO dead_letter_items(
            source_key, source_id, raw_item_id, external_id, attachment_id, content_item_id,
            failure_stage, error_type, error_message, payload_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?)
        """,
        (
            source_key,
            source_id,
            raw_item_id,
            external_id,
            attachment_id,
            content_item_id,
            failure_stage,
            error_type,
            error_message,
            json_dumps(payload or {}),
        ),
    )
    conn.commit()
    dead_id = int(cur.lastrowid)
    record_runtime_event(
        conn,
        level="error",
        event_type="dead_letter",
        stage=failure_stage,
        source_key=source_key,
        raw_item_id=raw_item_id,
        content_item_id=content_item_id,
        message=error_message or failure_stage,
        error_type=error_type,
        payload={
            "dead_letter_id": dead_id,
            "source_id": source_id,
            "external_id": external_id,
            "attachment_id": attachment_id,
            **(payload or {}),
        },
    )
    return dead_id


def runtime_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    now_text = now_iso()
    cutoff = (utc_now_naive() - timedelta(days=1)).isoformat()
    running_jobs = int(
        conn.execute(
            "SELECT COUNT(*) FROM job_leases WHERE job_id != ? AND expires_at > ?",
            (DAEMON_JOB_ID, now_text),
        ).fetchone()[0]
    )
    failed_last_day = int(
        conn.execute(
            """
            SELECT COUNT(*) FROM job_runs
            WHERE status IN ('failed', 'abandoned')
              AND started_at >= ?
            """,
            (cutoff,),
        ).fetchone()[0]
    )
    pending_candidates = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM relation_candidates
            WHERE CASE
                WHEN candidate_state IS NULL THEN promotion_state
                WHEN promotion_state IS NOT NULL AND candidate_state='pending' AND promotion_state!='pending' THEN promotion_state
                ELSE candidate_state
            END IN ('pending', 'review')
            """
        ).fetchone()[0]
    ) if table_exists(conn, "relation_candidates") else 0
    degraded_sources = int(
        conn.execute(
            "SELECT COUNT(*) FROM source_sync_state WHERE COALESCE(quality_state, state)='degraded'"
        ).fetchone()[0]
    ) if table_exists(conn, "source_sync_state") else 0
    dead_letters = int(
        conn.execute("SELECT COUNT(*) FROM dead_letter_items WHERE resolved_at IS NULL").fetchone()[0]
    ) if table_exists(conn, "dead_letter_items") else 0
    return {
        "daemon_running": active_job_lease(conn, DAEMON_JOB_ID) is not None,
        "running_jobs": running_jobs,
        "failed_last_day": failed_last_day,
        "pending_candidates": pending_candidates,
        "degraded_sources": degraded_sources,
        "dead_letters": dead_letters,
        "latest_pipeline_version": latest_successful_pipeline_version(conn),
        "analysis_built_from_pipeline_version": get_runtime_metadata(conn, "analysis_built_from_pipeline_version"),
        "obsidian_built_from_pipeline_version": get_runtime_metadata(conn, "obsidian_built_from_pipeline_version"),
    }


def open_runtime_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA wal_autocheckpoint = 100")
    conn.execute("PRAGMA journal_size_limit = 10485760")
    ensure_runtime_schema(conn)
    return conn
