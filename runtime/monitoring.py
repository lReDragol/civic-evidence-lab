from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

from config.db_utils import PROJECT_ROOT, get_db, load_settings, setup_logging
from runtime.state import DAEMON_JOB_ID, get_runtime_metadata, parse_iso, table_exists


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    try:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]
    except sqlite3.Error as exc:
        logging.getLogger(__name__).exception("Monitoring query failed")
        raise RuntimeError("Monitoring unavailable: database query failed") from exc


def _scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = (), default: Any = 0) -> Any:
    try:
        row = conn.execute(sql, params).fetchone()
        return row[0] if row else default
    except sqlite3.Error as exc:
        logging.getLogger(__name__).exception("Monitoring scalar query failed")
        raise RuntimeError("Monitoring unavailable: database query failed") from exc


def _columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    except sqlite3.Error:
        return set()


def _json_load(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _iso_date(value: str | None) -> str | None:
    if not value:
        return None
    dt = parse_iso(str(value))
    return dt.date().isoformat() if dt else None


def _coverage_days(earliest: str | None, latest: str | None = None) -> int:
    start = parse_iso(earliest)
    end = parse_iso(latest) or datetime.now(timezone.utc)
    if not start:
        return 0
    return max(1, (end.date() - start.date()).days + 1)


def _since_start(conn: sqlite3.Connection) -> str | None:
    value = get_runtime_metadata(conn, "mode_247_last_started_at") if table_exists(conn, "runtime_metadata") else None
    return str(value) if value else None


def _day_start_iso() -> str:
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=None).isoformat()


def get_active_jobs(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not table_exists(conn, "job_leases"):
        return []
    now = _now_iso()
    items = _rows(
        conn,
        """
        SELECT jl.job_id, jl.lease_owner, jl.started_at, jl.heartbeat_at, jl.expires_at, jl.payload_json,
               jr.id AS job_run_id, jr.status, jr.items_seen, jr.items_new, jr.items_updated,
               jr.items_skipped, jr.items_failed, jr.duplicate_items, jr.error_summary
        FROM job_leases jl
        LEFT JOIN job_runs jr ON jr.job_id=jl.job_id AND jr.owner=jl.lease_owner
             AND jr.status='running' AND jr.finished_at IS NULL
        WHERE jl.expires_at > ?
        ORDER BY CASE WHEN jl.job_id=? THEN 1 ELSE 0 END, jl.started_at DESC
        LIMIT 80
        """,
        (now, DAEMON_JOB_ID),
    )
    for item in items:
        heartbeat = parse_iso(item.get("heartbeat_at"))
        item["heartbeat_age_sec"] = int((datetime.now(timezone.utc) - heartbeat).total_seconds()) if heartbeat else None
    return items


def get_collection_coverage(conn: sqlite3.Connection) -> dict[str, Any]:
    if not table_exists(conn, "content_items"):
        return {"earliest_item_date": None, "latest_item_date": None, "coverage_days": 0}
    earliest_pair: tuple[datetime, str] | None = None
    latest_pair: tuple[datetime, str] | None = None
    rows = conn.execute(
        """
        SELECT published_at, collected_at
        FROM content_items
        WHERE published_at IS NOT NULL OR collected_at IS NOT NULL
        """
    ).fetchall()
    for published_at, collected_at in rows:
        for candidate in (published_at, collected_at):
            dt = parse_iso(str(candidate) if candidate is not None else None)
            if not dt:
                continue
            raw = str(candidate)
            if earliest_pair is None or dt < earliest_pair[0]:
                earliest_pair = (dt, raw)
            if latest_pair is None or dt > latest_pair[0]:
                latest_pair = (dt, raw)
    earliest = earliest_pair[1] if earliest_pair else None
    latest = latest_pair[1] if latest_pair else None
    return {
        "earliest_item_date": earliest,
        "latest_item_date": latest,
        "coverage_days": _coverage_days(earliest, latest),
    }


def get_duplicate_report(conn: sqlite3.Connection, since: str | None = None) -> dict[str, Any]:
    where = "1=1"
    params: list[Any] = []
    if since:
        where = "collected_at >= ?"
        params.append(since)
    raw_dupes = _rows(
        conn,
        f"""
        SELECT source_id, external_id, COUNT(*) AS count
        FROM raw_source_items
        WHERE external_id IS NOT NULL AND external_id != '' AND {where}
        GROUP BY source_id, external_id
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 20
        """,
        tuple(params),
    ) if table_exists(conn, "raw_source_items") else []
    content_dupes = _rows(
        conn,
        f"""
        SELECT source_id, external_id, COUNT(*) AS count
        FROM content_items
        WHERE external_id IS NOT NULL AND external_id != '' AND {where}
        GROUP BY source_id, external_id
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 20
        """,
        tuple(params),
    ) if table_exists(conn, "content_items") else []
    body_hash_dupes: list[dict[str, Any]] = []
    duplicate_body_hashes = 0
    cols = _columns(conn, "content_items")
    if "body_hash" in cols:
        body_hash_dupes = _rows(
            conn,
            f"""
            SELECT body_hash, COUNT(*) AS count
            FROM content_items
            WHERE body_hash IS NOT NULL AND body_hash != '' AND {where}
            GROUP BY body_hash
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 20
            """,
            tuple(params),
        )
        duplicate_body_hashes = sum(int(row.get("count") or 0) for row in body_hash_dupes)
    else:
        # Compatibility fallback for old DB snapshots.
        seen: dict[str, int] = {}
        for row in conn.execute("SELECT body_text FROM content_items WHERE body_text IS NOT NULL LIMIT 5000").fetchall():
            digest = hashlib.sha256(str(row[0] or "").strip().encode("utf-8")).hexdigest()
            seen[digest] = seen.get(digest, 0) + 1
        duplicate_body_hashes = sum(count for count in seen.values() if count > 1)

    log_duplicates = _duplicate_log_messages()
    total_content = int(_scalar(conn, "SELECT COUNT(*) FROM content_items", default=0)) if table_exists(conn, "content_items") else 0
    duplicate_items = sum(int(row.get("count") or 0) for row in content_dupes) + duplicate_body_hashes
    return {
        "raw_external_id_duplicates": raw_dupes,
        "content_external_id_duplicates": content_dupes,
        "body_hash_duplicates": body_hash_dupes,
        "duplicate_body_hashes": duplicate_body_hashes,
        "duplicate_items_estimate": duplicate_items,
        "duplicate_ratio": round(duplicate_items / total_content, 4) if total_content else 0.0,
        "multi_item_clusters": int(_scalar(conn, "SELECT COUNT(*) FROM content_clusters WHERE COALESCE(item_count,0)>1", default=0)) if table_exists(conn, "content_clusters") else 0,
        "log_duplicates": log_duplicates,
    }


def _duplicate_log_messages() -> list[dict[str, Any]]:
    log_path = PROJECT_ROOT / "log.log"
    if not log_path.exists():
        log_path = PROJECT_ROOT / "app.log"
    if not log_path.exists():
        return []
    counts: dict[str, int] = {}
    line_re = re.compile(r"^\d{4}-\d\d-\d\d[^\[]+\[(?P<level>[^\]]+)\]\s+(?P<rest>.*)$")
    try:
        lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()[-5000:]
    except OSError:
        return []
    for line in lines:
        match = line_re.match(line)
        normalized = match.group("rest") if match else line
        normalized = re.sub(r"\b\d+\b", "#", normalized)
        counts[normalized] = counts.get(normalized, 0) + 1
    return [
        {"message": message, "count": count}
        for message, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)
        if count > 1
    ][:20]


def get_runtime_overview(conn: sqlite3.Connection, since_start: bool = True) -> dict[str, Any]:
    since = _since_start(conn) if since_start else None
    today = _day_start_iso()
    active = get_active_jobs(conn)
    coverage = get_collection_coverage(conn)
    duplicates = get_duplicate_report(conn)
    daemon = next((job for job in active if job.get("job_id") == DAEMON_JOB_ID), None)
    job_since_where = "started_at >= ?" if since else "1=1"
    job_params = (since,) if since else ()
    return {
        "daemon_running": daemon is not None,
        "mode_247_enabled": str(get_runtime_metadata(conn, "mode_247_enabled") or "").lower() in {"true", "1", "yes"} if table_exists(conn, "runtime_metadata") else False,
        "uptime_started_at": since,
        "active_jobs_now": len([job for job in active if job.get("job_id") != DAEMON_JOB_ID]),
        "failed_jobs_24h": int(_scalar(conn, "SELECT COUNT(*) FROM job_runs WHERE status IN ('failed','abandoned') AND started_at >= datetime('now','-1 day')", default=0)) if table_exists(conn, "job_runs") else 0,
        "collected_total": int(_scalar(conn, "SELECT COUNT(*) FROM content_items", default=0)) if table_exists(conn, "content_items") else 0,
        "collected_today": int(_scalar(conn, "SELECT COUNT(*) FROM content_items WHERE collected_at >= ?", (today,), default=0)) if table_exists(conn, "content_items") else 0,
        "collected_current_run": int(_scalar(conn, f"SELECT COALESCE(SUM(items_new),0) FROM job_runs WHERE {job_since_where}", job_params, default=0)) if table_exists(conn, "job_runs") else 0,
        "processed_current_run": int(_scalar(conn, f"SELECT COALESCE(SUM(items_seen),0) FROM job_runs WHERE {job_since_where} AND job_id NOT LIKE '%telegram%'", job_params, default=0)) if table_exists(conn, "job_runs") else 0,
        "classified_current_run": int(_scalar(conn, f"SELECT COALESCE(SUM(items_seen),0) FROM job_runs WHERE {job_since_where} AND job_id IN ('tagger','llm','classifier_audit')", job_params, default=0)) if table_exists(conn, "job_runs") else 0,
        "dead_letters_unresolved": int(_scalar(conn, "SELECT COUNT(*) FROM dead_letter_items WHERE resolved_at IS NULL", default=0)) if table_exists(conn, "dead_letter_items") else 0,
        "degraded_sources": int(_scalar(conn, "SELECT COUNT(*) FROM source_sync_state WHERE state='degraded' OR quality_state='degraded'", default=0)) if table_exists(conn, "source_sync_state") else 0,
        "duplicate_ratio": duplicates.get("duplicate_ratio", 0.0),
        **coverage,
    }


def get_source_metrics(conn: sqlite3.Connection, since: str | None = None) -> list[dict[str, Any]]:
    if not table_exists(conn, "sources"):
        return []
    today = _day_start_iso()
    items_since_sql = "AND ci.collected_at >= ?" if since else ""
    sql_params: list[Any] = []
    if since:
        sql_params.append(since)
    sql_params.extend([today, since])
    rows = _rows(
        conn,
        f"""
        SELECT s.id AS source_id, s.name AS source_name, s.category AS source_type, s.url,
               COALESCE(ss.state, 'unknown') AS status, COALESCE(ss.quality_state, 'unknown') AS quality_state,
               ss.is_collecting, ss.current_job_id, ss.current_channel, ss.current_telegram_session,
               ss.consecutive_failures, ss.last_success_at, ss.last_attempt_at, ss.failure_class, ss.last_error,
               ss.last_cursor, ss.last_external_id, ss.transport_mode,
               COUNT(ci.id) AS total_collected,
               SUM(CASE WHEN ci.collected_at >= ? THEN 1 ELSE 0 END) AS collected_today,
               SUM(CASE WHEN ci.collected_at >= COALESCE(?, '0000') THEN 1 ELSE 0 END) AS collected_current_run,
               MIN(COALESCE(ci.published_at, ci.collected_at)) AS earliest_item_date,
               MAX(COALESCE(ci.published_at, ci.collected_at)) AS latest_item_date
        FROM sources s
        LEFT JOIN source_sync_state ss ON ss.source_id=s.id OR ss.source_key=s.category
        LEFT JOIN content_items ci ON ci.source_id=s.id {items_since_sql}
        GROUP BY s.id
        ORDER BY COALESCE(ss.is_collecting,0) DESC,
                 CASE WHEN ss.state IN ('degraded','failed') OR ss.quality_state='degraded' THEN 0 ELSE 1 END,
                 ss.last_attempt_at DESC
        LIMIT 500
        """,
        tuple(sql_params),
    )
    for row in rows:
        row["coverage_days"] = _coverage_days(row.get("earliest_item_date"), row.get("latest_item_date"))
    return rows


def get_telegram_session_metrics(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not table_exists(conn, "telegram_sessions"):
        return []
    rows = _rows(
        conn,
        """
        SELECT session_key, client_type, session_path, status, assigned_count, current_job_id,
               current_source_id, current_channel, collecting_now, last_message_id, last_message_date,
               collected_current_run, collected_today, duplicates_skipped, failed_items,
               failure_class, cooldown_until, last_success_at, last_attempt_at, heartbeat_at, metadata_json
        FROM telegram_sessions
        ORDER BY CASE
            WHEN collecting_now=1 THEN 0
            WHEN status='active' THEN 1
            WHEN status='cooldown' THEN 2
            WHEN status='failed' THEN 3
            ELSE 4 END,
            session_key
        """
    )
    for row in rows:
        row["metadata"] = _json_load(row.pop("metadata_json", None), {})
        row["display_status"] = "collecting_now" if row.get("collecting_now") else row.get("status")
    return rows


def get_model_metrics(conn: sqlite3.Connection, since: str | None = None) -> list[dict[str, Any]]:
    if not table_exists(conn, "ai_task_attempts"):
        return []
    params: list[Any] = []
    where = "1=1"
    if since:
        where = "started_at >= ?"
        params.append(since)
    rows = _rows(
        conn,
        f"""
        SELECT provider, model_name AS model, COALESCE(task_type, 'unknown') AS task_type,
               status, COALESCE(failure_kind, 'ok') AS failure_kind, latency_ms, started_at,
               tokens_in, tokens_out, estimated_cost
        FROM ai_task_attempts
        WHERE {where}
        """,
        tuple(params),
    )
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    latencies: dict[tuple[str, str, str], list[int]] = {}
    for row in rows:
        key = (str(row.get("provider") or "unknown"), str(row.get("model") or "unknown"), str(row.get("task_type") or "unknown"))
        item = grouped.setdefault(
            key,
            {
                "provider": key[0],
                "model": key[1],
                "task_type": key[2],
                "status": "idle",
                "requests": 0,
                "success": 0,
                "failed": 0,
                "timeouts": 0,
                "rate_limited": 0,
                "schema_violations": 0,
                "tokens_in": 0,
                "tokens_out": 0,
                "estimated_cost": 0.0,
                "last_call": None,
                "last_error": None,
            },
        )
        item["requests"] += 1
        if row.get("status") == "ok":
            item["success"] += 1
        elif row.get("status") == "running":
            item["status"] = "working_now"
        else:
            item["failed"] += 1
            item["last_error"] = row.get("failure_kind")
        if row.get("failure_kind") == "timeout":
            item["timeouts"] += 1
        if row.get("failure_kind") == "rate":
            item["rate_limited"] += 1
        if row.get("failure_kind") == "schema_violation":
            item["schema_violations"] += 1
        if row.get("latency_ms") is not None:
            latencies.setdefault(key, []).append(int(row["latency_ms"]))
        item["tokens_in"] += int(row.get("tokens_in") or 0)
        item["tokens_out"] += int(row.get("tokens_out") or 0)
        item["estimated_cost"] += float(row.get("estimated_cost") or 0)
        item["last_call"] = max([v for v in [item.get("last_call"), row.get("started_at")] if v], default=None)
    for key, item in grouped.items():
        values = sorted(latencies.get(key, []))
        item["avg_latency_ms"] = int(mean(values)) if values else None
        item["p95_latency_ms"] = values[int((len(values) - 1) * 0.95)] if values else None
        if item["status"] != "working_now":
            if item["rate_limited"]:
                item["status"] = "rate_limited"
            elif item["failed"] and not item["success"]:
                item["status"] = "failed"
            elif item["success"]:
                item["status"] = "active"
            else:
                item["status"] = "idle"
    return sorted(
        grouped.values(),
        key=lambda item: ({"working_now": 0, "active": 1, "rate_limited": 2, "failed": 3, "idle": 4}.get(item["status"], 5), item["provider"], item["model"]),
    )


def get_processing_funnel(conn: sqlite3.Connection, since: str | None = None) -> dict[str, dict[str, Any]]:
    def count(sql: str) -> int:
        return int(_scalar(conn, sql, default=0))

    return {
        "raw_collected": {"count": count("SELECT COUNT(*) FROM raw_source_items") if table_exists(conn, "raw_source_items") else 0},
        "raw_stored": {"count": count("SELECT COUNT(*) FROM content_items") if table_exists(conn, "content_items") else 0},
        "deduplicated": {"count": count("SELECT COUNT(*) FROM content_clusters") if table_exists(conn, "content_clusters") else 0},
        "ocr_processed": {"count": count("SELECT COUNT(*) FROM attachments WHERE COALESCE(ocr_text,'') != ''") if table_exists(conn, "attachments") else 0},
        "asr_processed": {"count": count("SELECT COUNT(*) FROM attachments WHERE COALESCE(asr_text,'') != ''") if "asr_text" in _columns(conn, "attachments") else 0},
        "tagged": {"count": count("SELECT COUNT(DISTINCT content_item_id) FROM content_tags") if table_exists(conn, "content_tags") else 0},
        "semantic_indexed": {"count": count("SELECT COUNT(*) FROM semantic_neighbors") if table_exists(conn, "semantic_neighbors") else 0},
        "llm_classified": {"count": count("SELECT COUNT(*) FROM content_items WHERE llm_processed=1") if "llm_processed" in _columns(conn, "content_items") else 0},
        "entities_extracted": {"count": count("SELECT COUNT(*) FROM entity_mentions") if table_exists(conn, "entity_mentions") else 0},
        "claims_extracted": {"count": count("SELECT COUNT(*) FROM claims") if table_exists(conn, "claims") else 0},
        "verified": {"count": count("SELECT COUNT(*) FROM verifications") if table_exists(conn, "verifications") else 0},
        "events_built": {"count": count("SELECT COUNT(*) FROM events") if table_exists(conn, "events") else 0},
        "relations_built": {"count": count("SELECT COUNT(*) FROM relation_candidates") if table_exists(conn, "relation_candidates") else 0},
        "ready_for_review": {"count": count("SELECT COUNT(*) FROM review_tasks WHERE status IN ('open','needs_review')") if table_exists(conn, "review_tasks") else 0},
        "exported": {"count": count("SELECT COUNT(*) FROM job_runs WHERE job_id IN ('obsidian_export','analysis_snapshot') AND status='ok'") if table_exists(conn, "job_runs") else 0},
    }


def get_error_report(conn: sqlite3.Connection, since: str | None = None) -> dict[str, Any]:
    params: list[Any] = []
    where = "1=1"
    if since:
        where = "detected_at >= ?"
        params.append(since)
    dead = _rows(
        conn,
        f"""
        SELECT detected_at AS time, failure_stage AS stage, source_key, content_item_id, raw_item_id,
               error_type, error_message AS short_message,
               CASE WHEN resolved_at IS NULL THEN 0 ELSE 1 END AS resolved
        FROM dead_letter_items
        WHERE {where}
        ORDER BY resolved ASC, detected_at DESC
        LIMIT 100
        """,
        tuple(params),
    ) if table_exists(conn, "dead_letter_items") else []
    runtime_errors = _rows(
        conn,
        """
        SELECT created_at AS time, stage, source_key, content_item_id, raw_item_id, error_type, message AS short_message, 0 AS resolved
        FROM runtime_events
        WHERE level IN ('error','critical') OR error_type IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 100
        """,
    ) if table_exists(conn, "runtime_events") else []
    failed_jobs = _rows(
        conn,
        """
        SELECT started_at AS time, job_id AS stage, NULL AS source_key, NULL AS content_item_id,
               NULL AS raw_item_id, status AS error_type, error_summary AS short_message,
               CASE WHEN status='ok' THEN 1 ELSE 0 END AS resolved
        FROM job_runs
        WHERE status IN ('failed','abandoned')
        ORDER BY id DESC
        LIMIT 100
        """,
    ) if table_exists(conn, "job_runs") else []
    return {
        "dead_letters": dead,
        "runtime_errors": runtime_errors,
        "failed_jobs": failed_jobs,
        "unresolved_count": sum(1 for item in dead if not item.get("resolved")),
    }


def get_5h_run_report(conn: sqlite3.Connection, run_started_at: str, run_finished_at: str) -> dict[str, Any]:
    payload = get_full_monitoring_payload(conn, since=run_started_at)
    payload["run_report"] = {
        "start_time": run_started_at,
        "finish_time": run_finished_at,
        "duration": _duration_text(run_started_at, run_finished_at),
    }
    return payload


def _duration_text(start: str, finish: str) -> str:
    start_dt = parse_iso(start)
    finish_dt = parse_iso(finish)
    if not start_dt or not finish_dt:
        return "unknown"
    seconds = int((finish_dt - start_dt).total_seconds())
    return f"{seconds // 3600}h {(seconds % 3600) // 60}m {seconds % 60}s"


def get_full_monitoring_payload(conn: sqlite3.Connection, since: str | None = None) -> dict[str, Any]:
    overview = get_runtime_overview(conn, since_start=since is None)
    active_jobs = get_active_jobs(conn)
    daemon_lease = next((job for job in active_jobs if job.get("job_id") == DAEMON_JOB_ID), None)
    sources = get_source_metrics(conn, since=since)
    telegram_sessions = get_telegram_session_metrics(conn)
    models = get_model_metrics(conn, since=since)
    funnel = get_processing_funnel(conn, since=since)
    duplicates = get_duplicate_report(conn, since=since)
    errors = get_error_report(conn, since=since)
    coverage = get_collection_coverage(conn)
    key_status = {
        str(row.get("status") or "unknown"): int(row.get("count") or 0)
        for row in _rows(conn, "SELECT status, COUNT(*) AS count FROM llm_keys GROUP BY status")
    } if table_exists(conn, "llm_keys") else {}
    provider_rows = _rows(conn, "SELECT provider, status, active_key_count, last_checked_at, last_success_at FROM llm_provider_health ORDER BY provider") if table_exists(conn, "llm_provider_health") else []
    failure_kinds = {
        str(row.get("failure_kind") or "unknown"): int(row.get("count") or 0)
        for row in _rows(
            conn,
            """
            SELECT COALESCE(failure_kind,'unknown') AS failure_kind, COUNT(*) AS count
            FROM ai_task_attempts
            WHERE status='failed'
            GROUP BY COALESCE(failure_kind,'unknown')
            ORDER BY count DESC
            LIMIT 20
            """,
        )
    } if table_exists(conn, "ai_task_attempts") else {}
    relation_gate = {
        "promoted_same_case_cluster": int(_scalar(conn, "SELECT COUNT(*) FROM relation_candidates WHERE candidate_state='promoted' AND candidate_type='same_case_cluster'", default=0)) if table_exists(conn, "relation_candidates") else 0,
        "promoted_with_location_entity": int(_scalar(conn, """
            SELECT COUNT(*)
            FROM relation_candidates rc
            JOIN entities ea ON ea.id=rc.entity_a_id
            JOIN entities eb ON eb.id=rc.entity_b_id
            WHERE rc.candidate_state='promoted' AND (ea.entity_type='location' OR eb.entity_type='location')
        """, default=0)) if table_exists(conn, "relation_candidates") and table_exists(conn, "entities") else 0,
        "review_zero_support": int(_scalar(conn, "SELECT COUNT(*) FROM relation_candidates WHERE candidate_state='review' AND COALESCE(support_items,0)=0", default=0)) if table_exists(conn, "relation_candidates") else 0,
    }
    logs = _monitoring_logs(conn)
    telegram_counts = {
        "sessions": telegram_sessions,
        "active_sessions": sum(1 for item in telegram_sessions if item.get("status") == "active"),
        "cooldown_sessions": sum(1 for item in telegram_sessions if item.get("status") == "cooldown"),
        "failed_sessions": sum(1 for item in telegram_sessions if item.get("status") == "failed"),
        "assigned_channels": sum(int(item.get("assigned_count") or 0) for item in telegram_sessions),
    }
    agent_rows = _rows(
        conn,
        """
        SELECT target_group, status, COUNT(*) AS count
        FROM agent_tasks
        GROUP BY target_group, status
        ORDER BY target_group, status
        """,
    ) if table_exists(conn, "agent_tasks") else []
    tasks_by_group: dict[str, dict[str, int]] = {}
    open_tasks = 0
    for row in agent_rows:
        group = str(row.get("target_group") or "unknown")
        status = str(row.get("status") or "unknown")
        count = int(row.get("count") or 0)
        tasks_by_group.setdefault(group, {})[status] = count
        if status in {"queued", "running", "open", "needs_review"}:
            open_tasks += count
    search_evidence = {
        "total": int(_scalar(conn, "SELECT COUNT(*) FROM search_evidence", default=0)) if table_exists(conn, "search_evidence") else 0,
        "recent": _rows(
            conn,
            """
            SELECT provider, model, url, title, source_tier, confidence, retrieved_at
            FROM search_evidence
            ORDER BY id DESC
            LIMIT 12
            """,
        ) if table_exists(conn, "search_evidence") else [],
    }
    return {
        "generated_at": _now_iso(),
        "overview": overview,
        "runtime": {
            "enabled": overview["mode_247_enabled"],
            "daemon_running": overview["daemon_running"],
            "last_heartbeat": (
                get_runtime_metadata(conn, "daemon_last_seen_at") if table_exists(conn, "runtime_metadata") else None
            ) or (daemon_lease.get("heartbeat_at") if daemon_lease else None),
            "last_catchup": get_runtime_metadata(conn, "last_collect_catchup_finished_at") if table_exists(conn, "runtime_metadata") else None,
            "autostart_status": get_runtime_metadata(conn, "mode_247_autostart_status") if table_exists(conn, "runtime_metadata") else None,
            "running_jobs": active_jobs,
        },
        "active_jobs": active_jobs,
        "sources": sources,
        "telegram": telegram_counts,
        "telegram_sessions": telegram_sessions,
        "models": models,
        "ai": {
            "keys": {
                "active": int(key_status.get("active", 0)),
                "cooldown": int(key_status.get("cooldown", 0)),
                "removed": int(key_status.get("removed", 0)),
                "by_status": key_status,
            },
            "providers": provider_rows,
            "failure_kinds": failure_kinds,
        },
        "agents": {
            "open_tasks": open_tasks,
            "tasks_by_group": tasks_by_group,
            "search_evidence": search_evidence,
        },
        "funnel": funnel,
        "duplicates": duplicates,
        "errors": errors,
        "coverage": coverage,
        "ingest": {
            "content": overview["collected_total"],
            "attachments": int(_scalar(conn, "SELECT COUNT(*) FROM attachments", default=0)) if table_exists(conn, "attachments") else 0,
            "document_reviews": int(_scalar(conn, "SELECT COUNT(*) FROM review_tasks WHERE queue_key='documents' AND status IN ('open','needs_review')", default=0)) if table_exists(conn, "review_tasks") else 0,
            "events": int(_scalar(conn, "SELECT COUNT(*) FROM events", default=0)) if table_exists(conn, "events") else 0,
            "facts": int(_scalar(conn, "SELECT COUNT(*) FROM event_facts", default=0)) if table_exists(conn, "event_facts") else 0,
            "relations": int(_scalar(conn, "SELECT COUNT(*) FROM entity_relations", default=0)) if table_exists(conn, "entity_relations") else 0,
            "relation_candidates": int(_scalar(conn, "SELECT COUNT(*) FROM relation_candidates", default=0)) if table_exists(conn, "relation_candidates") else 0,
        },
        "quality": {
            "relation_gate": relation_gate,
            "degraded_sources": overview["degraded_sources"],
            "reviewed_baseline_ready": bool(get_runtime_metadata(conn, "reviewed_baseline_ready", False)) if table_exists(conn, "runtime_metadata") else False,
        },
        "logs": logs,
    }


def _monitoring_logs(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    logs = []
    for row in _rows(conn, "SELECT created_at, level, message, event_type FROM runtime_events ORDER BY id DESC LIMIT 40") if table_exists(conn, "runtime_events") else []:
        logs.append({"level": row.get("level") or "info", "message": f"{row.get('event_type')}: {row.get('message')}", "started_at": row.get("created_at")})
    for row in _rows(conn, "SELECT job_id, status, started_at, items_new, error_summary FROM job_runs ORDER BY id DESC LIMIT 40") if table_exists(conn, "job_runs") else []:
        status = str(row.get("status") or "unknown")
        level = "error" if status in {"failed", "abandoned"} else "success" if status == "ok" else "info"
        message = f"{row.get('job_id')}: {status}"
        if row.get("items_new"):
            message += f" · new {row.get('items_new')}"
        if row.get("error_summary"):
            message += f" · {row.get('error_summary')}"
        logs.append({"level": level, "message": message, "started_at": row.get("started_at")})
    return sorted(logs, key=lambda item: item.get("started_at") or "", reverse=True)[:80]


def write_markdown_report(report: dict[str, Any], path: Path) -> None:
    overview = report.get("overview", {})
    coverage = report.get("coverage", {})
    lines = [
        "# Civic Evidence Lab 24/7 Report",
        "",
        f"- Generated at: {report.get('generated_at')}",
        f"- Collected total: {overview.get('collected_total')}",
        f"- Collected today: {overview.get('collected_today')}",
        f"- Collected current run: {overview.get('collected_current_run')}",
        f"- Earliest item date: {coverage.get('earliest_item_date')}",
        f"- Latest item date: {coverage.get('latest_item_date')}",
        f"- Coverage days: {coverage.get('coverage_days')}",
        f"- Dead letters unresolved: {overview.get('dead_letters_unresolved')}",
        f"- Degraded sources: {overview.get('degraded_sources')}",
        f"- Duplicate ratio: {overview.get('duplicate_ratio')}",
        "",
        "## Top Sources",
    ]
    for source in (report.get("sources") or [])[:20]:
        lines.append(f"- {source.get('source_name')}: total={source.get('total_collected')} today={source.get('collected_today')} status={source.get('status')}")
    lines.extend(["", "## Telegram Sessions"])
    for session in report.get("telegram_sessions") or []:
        lines.append(f"- {session.get('session_key')}: {session.get('display_status')} channel={session.get('current_channel')} collected={session.get('collected_current_run')}")
    lines.extend(["", "## Models"])
    for model in (report.get("models") or [])[:20]:
        lines.append(f"- {model.get('provider')}/{model.get('model')} {model.get('task_type')}: {model.get('status')} ok={model.get('success')} failed={model.get('failed')}")
    lines.extend(["", "## Errors"])
    for item in (report.get("errors", {}).get("dead_letters") or [])[:20]:
        lines.append(f"- {item.get('time')} {item.get('stage')}: {item.get('error_type')} {item.get('short_message')}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Civic Evidence Lab runtime monitoring")
    parser.add_argument("--summary", action="store_true")
    parser.add_argument("--active", action="store_true")
    parser.add_argument("--sources", action="store_true")
    parser.add_argument("--telegram", action="store_true")
    parser.add_argument("--models", action="store_true")
    parser.add_argument("--duplicates", action="store_true")
    parser.add_argument("--errors", action="store_true")
    parser.add_argument("--export")
    parser.add_argument("--report-5h", action="store_true")
    parser.add_argument("--started-at")
    parser.add_argument("--finished-at")
    parser.add_argument("--markdown")
    args = parser.parse_args(argv)

    settings = load_settings()
    setup_logging(settings)
    conn = get_db(settings)
    try:
        if args.report_5h:
            started = args.started_at or _since_start(conn) or _now_iso()
            finished = args.finished_at or _now_iso()
            payload = get_5h_run_report(conn, started, finished)
        elif args.active:
            payload = get_active_jobs(conn)
        elif args.sources:
            payload = get_source_metrics(conn)
        elif args.telegram:
            payload = get_telegram_session_metrics(conn)
        elif args.models:
            payload = get_model_metrics(conn)
        elif args.duplicates:
            payload = get_duplicate_report(conn)
        elif args.errors:
            payload = get_error_report(conn)
        elif args.summary:
            payload = get_runtime_overview(conn)
        else:
            payload = get_full_monitoring_payload(conn)
    finally:
        conn.close()

    if args.export:
        target = Path(args.export)
        if not target.is_absolute():
            target = PROJECT_ROOT / target
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    if args.markdown:
        target = Path(args.markdown)
        if not target.is_absolute():
            target = PROJECT_ROOT / target
        write_markdown_report(payload, target)
    _print_json(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
