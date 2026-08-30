from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

METRICS_SCHEMA = """
CREATE TABLE IF NOT EXISTS autonomy_test_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at TEXT NOT NULL,
    metric_group TEXT NOT NULL,
    metric_key TEXT NOT NULL,
    metric_value_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_atm_group_key
    ON autonomy_test_metrics(metric_group, metric_key);
CREATE INDEX IF NOT EXISTS idx_atm_collected
    ON autonomy_test_metrics(collected_at);
"""


def ensure_metrics_table(conn: sqlite3.Connection):
    for stmt in METRICS_SCHEMA.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    conn.commit()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _store_metric(conn: sqlite3.Connection, group: str, key: str, value: Any):
    conn.execute(
        "INSERT INTO autonomy_test_metrics(collected_at, metric_group, metric_key, metric_value_json) VALUES(?,?,?,?)",
        (_utc_now_iso(), group, key, json.dumps(value, ensure_ascii=False, default=str)),
    )


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def _count_unprocessed(conn: sqlite3.Connection, table: str, column: str) -> int | None:
    if column not in _table_columns(conn, table):
        return None
    return conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {column}=0 OR {column} IS NULL"
    ).fetchone()[0]


def collect_metrics_snapshot(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    conn = get_db(settings)
    try:
        ensure_metrics_table(conn)
        now = _utc_now_iso()

        # === SOURCES ===
        for row in conn.execute(
            "SELECT source_key, state, quality_state, quality_issue, consecutive_failures, "
            "last_success_at, last_attempt_at, transport_mode, last_error FROM source_sync_state"
        ).fetchall():
            _store_metric(conn, "source", row[0], {
                "state": row[1], "quality_state": row[2], "quality_issue": row[3],
                "consecutive_failures": row[4], "last_success_at": row[5],
                "last_attempt_at": row[6], "transport_mode": row[7], "last_error": row[8],
            })

        # === CONTENT ===
        total = conn.execute("SELECT COUNT(*) FROM content_items").fetchone()[0]
        by_status = dict(conn.execute(
            "SELECT COALESCE(status,'active'), COUNT(*) FROM content_items GROUP BY COALESCE(status,'active')"
        ).fetchall())
        by_type = dict(conn.execute(
            "SELECT COALESCE(content_type,'unknown'), COUNT(*) FROM content_items GROUP BY COALESCE(content_type,'unknown')"
        ).fetchall())
        llm_unprocessed = _count_unprocessed(conn, "content_items", "llm_processed")
        ner_unprocessed = _count_unprocessed(conn, "content_items", "ner_processed")
        claims_unprocessed = _count_unprocessed(conn, "content_items", "claims_processed")
        garbage_unchecked = _count_unprocessed(conn, "content_items", "garbage_checked")
        _store_metric(conn, "content", "summary", {
            "total": total, "by_status": by_status, "by_type": by_type,
            "llm_unprocessed": llm_unprocessed, "ner_unprocessed": ner_unprocessed,
            "claims_unprocessed": claims_unprocessed, "garbage_unchecked": garbage_unchecked,
        })

        # === ENTITIES ===
        entity_total = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        entity_by_type = dict(conn.execute("SELECT entity_type, COUNT(*) FROM entities GROUP BY entity_type").fetchall())
        relations_total = conn.execute("SELECT COUNT(*) FROM entity_relations").fetchone()[0]
        relations_by_type = dict(conn.execute(
            "SELECT relation_type, COUNT(*) FROM entity_relations GROUP BY relation_type ORDER BY COUNT(*) DESC LIMIT 20"
        ).fetchall())
        mentions_total = conn.execute("SELECT COUNT(*) FROM entity_mentions").fetchone()[0]
        new_entities_unsearched = 0
        try:
            new_entities_unsearched = conn.execute("SELECT COUNT(*) FROM ner_new_entities WHERE search_triggered=0").fetchone()[0]
        except Exception:
            pass
        _store_metric(conn, "entities", "summary", {
            "total": entity_total, "by_type": entity_by_type,
            "relations_total": relations_total, "relations_by_type": relations_by_type,
            "mentions_total": mentions_total, "new_entities_unsearched": new_entities_unsearched,
        })

        # === CLAIMS ===
        claims_total = conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
        claims_by_status = dict(conn.execute("SELECT status, COUNT(*) FROM claims GROUP BY status").fetchall())
        claims_by_type = dict(conn.execute("SELECT claim_type, COUNT(*) FROM claims GROUP BY claim_type").fetchall())
        contradictions = 0
        try:
            contradictions = conn.execute("SELECT COUNT(*) FROM claim_contradictions").fetchone()[0]
        except Exception:
            pass
        _store_metric(conn, "claims", "summary", {
            "total": claims_total, "by_status": claims_by_status, "by_type": claims_by_type,
            "contradictions": contradictions,
        })

        # === LLM KEYS ===
        from llm.key_pool import list_active_keys
        active_keys = list_active_keys(conn)
        by_provider: dict[str, int] = {}
        by_provider_model: dict[str, int] = {}
        for k in active_keys:
            p = k["provider"]
            by_provider[p] = by_provider.get(p, 0) + 1
            pm = f"{p}/{k.get('model_name', '?')}"
            by_provider_model[pm] = by_provider_model.get(pm, 0) + 1

        key_failures_24h = conn.execute(
            "SELECT provider, COUNT(*) FROM llm_key_failures WHERE created_at >= datetime('now', '-1 day') GROUP BY provider"
        ).fetchall()
        _store_metric(conn, "llm", "keys", {
            "active_total": len(active_keys), "by_provider": by_provider,
            "by_provider_model": by_provider_model,
            "failures_24h": {r[0]: r[1] for r in key_failures_24h},
        })

        # === JOB RUNS (last 1h) ===
        recent_runs = conn.execute(
            "SELECT job_id, status, COUNT(*) as cnt, "
            "SUM(items_new) as new, SUM(items_updated) as upd, SUM(items_seen) as seen "
            "FROM job_runs WHERE started_at >= datetime('now', '-1 hour') "
            "GROUP BY job_id, status"
        ).fetchall()
        job_stats = {}
        for r in recent_runs:
            jid = r[0]
            if jid not in job_stats:
                job_stats[jid] = {"ok": 0, "failed": 0, "items_new": 0, "items_updated": 0, "items_seen": 0}
            status = r[1]
            cnt = r[2]
            if status == "ok":
                job_stats[jid]["ok"] += cnt
            else:
                job_stats[jid]["failed"] += cnt
            job_stats[jid]["items_new"] += int(r[3] or 0)
            job_stats[jid]["items_updated"] += int(r[4] or 0)
            job_stats[jid]["items_seen"] += int(r[5] or 0)
        _store_metric(conn, "jobs", "last_hour", job_stats)

        # === LLM TASK ATTEMPTS (if table exists) ===
        try:
            llm_attempts = conn.execute(
                "SELECT provider, model_name, status, COUNT(*) FROM ai_task_attempts "
                "WHERE started_at >= datetime('now', '-1 hour') "
                "GROUP BY provider, model_name, status"
            ).fetchall()
            attempt_stats = {}
            for r in llm_attempts:
                key = f"{r[0]}/{r[1]}"
                if key not in attempt_stats:
                    attempt_stats[key] = {"ok": 0, "failed": 0}
                if r[2] == "ok":
                    attempt_stats[key]["ok"] += r[3]
                else:
                    attempt_stats[key]["failed"] += r[3]
            if attempt_stats:
                _store_metric(conn, "llm", "attempts_last_hour", attempt_stats)
        except Exception:
            pass

        # === DEAD LETTERS ===
        dl_count = 0
        try:
            dl_count = conn.execute("SELECT COUNT(*) FROM dead_letter_items WHERE resolved_at IS NULL").fetchone()[0]
        except Exception:
            pass
        _store_metric(conn, "errors", "dead_letters", dl_count)

        # === DAEMON ALERTS ===
        recent_alerts = []
        try:
            rows = conn.execute(
                "SELECT alert_type, severity, message FROM daemon_alerts WHERE created_at >= datetime('now', '-1 hour') ORDER BY id DESC LIMIT 20"
            ).fetchall()
            recent_alerts = [{"type": r[0], "severity": r[1], "message": r[2]} for r in rows]
        except Exception:
            pass
        if recent_alerts:
            _store_metric(conn, "alerts", "last_hour", recent_alerts)

        conn.commit()
        return {"ok": True, "collected_at": now}

    finally:
        conn.close()


def run_metrics_collector(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    result = collect_metrics_snapshot(settings)
    log.info("Metrics snapshot collected at %s", result.get("collected_at"))
    return result
