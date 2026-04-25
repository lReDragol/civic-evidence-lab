from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config.db_utils import SCHEMA_PATH, exec_schema


DAEMON_JOB_ID = "__daemon__"


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
    return int(cur.lastrowid)


def finish_job_run(conn: sqlite3.Connection, run_id: int, result: dict[str, Any]):
    status = "ok" if result.get("ok") else "failed"
    error_summary = None
    fatal_errors = result.get("fatal_errors") or []
    retriable_errors = result.get("retriable_errors") or []
    if fatal_errors:
        error_summary = "; ".join(str(item) for item in fatal_errors[:3])
    elif retriable_errors:
        error_summary = "; ".join(str(item) for item in retriable_errors[:3])

    conn.execute(
        """
        UPDATE job_runs
        SET status=?,
            finished_at=?,
            items_seen=?,
            items_new=?,
            items_updated=?,
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
            result.get("finished_at") or now_iso(),
            int(result.get("items_seen") or 0),
            int(result.get("items_new") or 0),
            int(result.get("items_updated") or 0),
            json_dumps(result.get("warnings") or []),
            json_dumps(result.get("retriable_errors") or []),
            json_dumps(result.get("fatal_errors") or []),
            result.get("next_cursor"),
            json_dumps(result.get("health") or {}),
            json_dumps(result.get("artifacts") or {}),
            error_summary,
            run_id,
        ),
    )
    conn.commit()


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
    metadata: dict[str, Any] | None = None,
):
    existing = conn.execute(
        """
        SELECT consecutive_failures, metadata_json
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

    conn.execute(
        """
        INSERT INTO source_sync_state(
            source_key, source_id, state, last_success_at, last_attempt_at,
            consecutive_failures, last_cursor, last_external_id, last_etag, last_hash,
            last_http_status, transport_mode, last_error, metadata_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(source_key) DO UPDATE SET
            source_id=COALESCE(excluded.source_id, source_sync_state.source_id),
            state=COALESCE(excluded.state, source_sync_state.state),
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
            metadata_json=excluded.metadata_json
        """,
        (
            source_key,
            source_id,
            resolved_state,
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
            json_dumps(merged_metadata),
        ),
    )
    conn.commit()


def record_source_health_report(
    conn: sqlite3.Connection,
    report: dict[str, Any],
    *,
    transport_mode: str = "healthcheck",
) -> dict[str, int]:
    inserted = 0
    degraded = 0
    for item in report.get("items", []):
        source_key = str(item.get("source") or item.get("url") or f"source-{inserted}")
        url = item.get("url")
        source_id = None
        if table_exists(conn, "sources") and url:
            row = conn.execute("SELECT id FROM sources WHERE url=? LIMIT 1", (url,)).fetchone()
            source_id = int(row[0]) if row else None

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
            success=bool(item.get("ok")),
            state="degraded" if not item.get("ok") else "ok",
            last_http_status=item.get("status"),
            transport_mode=transport_mode,
            last_error=item.get("error"),
            metadata={
                "health_title": item.get("title"),
                "final_url": item.get("final_url"),
            },
        )
        inserted += 1
        if not item.get("ok"):
            degraded += 1
    conn.commit()
    return {"inserted": inserted, "degraded": degraded}


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
    return int(cur.lastrowid)


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
            "SELECT COUNT(*) FROM relation_candidates WHERE promotion_state IN ('pending', 'review')"
        ).fetchone()[0]
    ) if table_exists(conn, "relation_candidates") else 0
    degraded_sources = int(
        conn.execute("SELECT COUNT(*) FROM source_sync_state WHERE state='degraded'").fetchone()[0]
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
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    ensure_runtime_schema(conn)
    return conn
