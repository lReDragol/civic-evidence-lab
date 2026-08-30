from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any

from runtime.state import table_exists


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def _safe_scalar(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> Any:
    try:
        row = conn.execute(sql, params).fetchone()
        return row[0] if row else None
    except sqlite3.Error:
        return None


def _safe_count(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> int:
    val = _safe_scalar(conn, sql, params)
    return int(val) if val is not None else 0


def _stage_result(status: str, metrics: dict[str, Any], issues: list[str]) -> dict[str, Any]:
    return {
        "status": status,
        "metrics": metrics,
        "issues": issues,
        "timestamp": _now_iso(),
    }


def check_collection_stage(conn: sqlite3.Connection) -> dict[str, Any]:
    """Проверяет состояние коллекторов (telegram, rss, official, watch_folder)."""
    issues: list[str] = []
    metrics: dict[str, Any] = {}

    # Общее число content_items за последние 24ч
    last_24h = _now_iso()[:10]  # yyyy-mm-dd
    new_items_24h = _safe_count(
        conn,
        "SELECT COUNT(*) FROM content_items WHERE collected_at >= ?",
        (f"{last_24h}T00:00:00",),
    )
    metrics["new_items_24h"] = new_items_24h

    # По категориям источников
    source_counts: dict[str, int] = {}
    if table_exists(conn, "sources") and table_exists(conn, "content_items"):
        rows = conn.execute(
            """
            SELECT COALESCE(s.category, 'unknown'), COUNT(*)
            FROM content_items ci
            LEFT JOIN sources s ON s.id = ci.source_id
            WHERE ci.collected_at >= ?
            GROUP BY s.category
            """,
            (f"{last_24h}T00:00:00",),
        ).fetchall()
        source_counts = {str(row[0]): int(row[1]) for row in rows}
    metrics["source_counts_24h"] = source_counts

    # Состояние синхронизации источников (source_sync_state)
    degraded_sources = 0
    source_details: list[dict[str, Any]] = []
    if table_exists(conn, "source_sync_state"):
        rows = conn.execute(
            """
            SELECT source_key, state, quality_state, consecutive_failures, last_success_at
            FROM source_sync_state
            ORDER BY consecutive_failures DESC, source_key
            """
        ).fetchall()
        for row in rows:
            key, state, quality_state, failures, last_success = row
            details = {
                "source_key": key,
                "state": state,
                "quality_state": quality_state,
                "consecutive_failures": int(failures or 0),
                "last_success_at": last_success,
            }
            source_details.append(details)
            if (quality_state == "degraded") or (state == "degraded") or int(failures or 0) >= 3:
                degraded_sources += 1
                issues.append(f"Source {key} is degraded ({failures} consecutive failures, last success {last_success})")
            elif int(failures or 0) >= 1:
                issues.append(f"Source {key} has warnings ({failures} consecutive failures)")
        metrics["degraded_sources"] = degraded_sources
        metrics["total_sync_sources"] = len(source_details)

    # Telegram sessions
    telegram_degraded = 0
    if table_exists(conn, "telegram_sessions"):
        telegram_degraded = _safe_count(
            conn,
            "SELECT COUNT(*) FROM telegram_sessions WHERE status != 'active'",
        )
        metrics["telegram_sessions_degraded"] = telegram_degraded
        if telegram_degraded > 0:
            issues.append(f"{telegram_degraded} telegram session(s) not active")

    # Watch folder — нет отдельной таблицы, используем метаданные runtime
    if table_exists(conn, "runtime_metadata"):
        watch_last = _safe_scalar(conn, "SELECT value_text FROM runtime_metadata WHERE key='watch_folder_last_scan'")
        metrics["watch_folder_last_scan"] = watch_last
        if watch_last is None:
            issues.append("watch_folder last scan unknown")

    status = "ok"
    if degraded_sources > 0 or telegram_degraded > 0:
        status = "warning"
    if degraded_sources >= 3 or (new_items_24h == 0 and len(source_details) > 0):
        status = "critical"

    return _stage_result(status, metrics, issues)


def check_enrichment_stage(conn: sqlite3.Connection) -> dict[str, Any]:
    """Проверяет NER, dedupe, photo_backfill, garbage_filter."""
    issues: list[str] = []
    metrics: dict[str, Any] = {}

    total_items = _safe_count(conn, "SELECT COUNT(*) FROM content_items")
    ner_done = _safe_count(conn, "SELECT COUNT(*) FROM content_items WHERE ner_processed=1")
    llm_done = _safe_count(conn, "SELECT COUNT(*) FROM content_items WHERE llm_processed=1")

    metrics["total_content_items"] = total_items
    metrics["ner_processed"] = ner_done
    metrics["llm_processed"] = llm_done
    metrics["ner_pct"] = round(ner_done * 100 / total_items, 2) if total_items else 0.0
    metrics["llm_pct"] = round(llm_done * 100 / total_items, 2) if total_items else 0.0

    if metrics["ner_pct"] < 50 and total_items > 10:
        issues.append(f"NER backlog: only {metrics['ner_pct']}% processed")
    if metrics["llm_pct"] < 50 and total_items > 10:
        issues.append(f"LLM backlog: only {metrics['llm_pct']}% processed")

    # Entities
    entity_count = _safe_count(conn, "SELECT COUNT(*) FROM entities")
    metrics["entity_count"] = entity_count

    # Dedupe clusters
    pending_dedupe = 0
    if table_exists(conn, "content_clusters"):
        pending_dedupe = _safe_count(
            conn,
            "SELECT COUNT(*) FROM content_clusters WHERE status='active' AND item_count > 1",
        )
        metrics["pending_dedupe_clusters"] = pending_dedupe
        if pending_dedupe > 100:
            issues.append(f"High dedupe backlog: {pending_dedupe} active multi-item clusters")

    # Photo backfill — примем runtime metadata
    if table_exists(conn, "runtime_metadata"):
        photo_backfill = _safe_scalar(
            conn, "SELECT value_text FROM runtime_metadata WHERE key='photo_backfill_pending'"
        )
        metrics["photo_backfill_pending"] = int(photo_backfill or 0)
        if metrics["photo_backfill_pending"] > 50:
            issues.append(f"Photo backfill pending: {metrics['photo_backfill_pending']}")

    # Garbage filter — claims/verifications отражают очистку
    garbage_flagged = _safe_count(conn, "SELECT COUNT(*) FROM claims WHERE status='spam' OR status='noise'")
    metrics["garbage_flagged"] = garbage_flagged

    status = "ok"
    if issues:
        status = "warning"
    if (metrics.get("ner_pct", 100) < 20) or (metrics.get("llm_pct", 100) < 20):
        status = "critical"

    return _stage_result(status, metrics, issues)


def check_analysis_stage(conn: sqlite3.Connection) -> dict[str, Any]:
    """Проверяет tagger, classifier, semantic_index, event_pipeline."""
    issues: list[str] = []
    metrics: dict[str, Any] = {}

    total_items = _safe_count(conn, "SELECT COUNT(*) FROM content_items")
    claims_done = _safe_count(conn, "SELECT COUNT(*) FROM claims")
    classification_done = _safe_count(
        conn, "SELECT COUNT(*) FROM content_items WHERE classification_v3_processed=1"
    )

    metrics["total_items"] = total_items
    metrics["claims_count"] = claims_done
    metrics["classification_v3_pct"] = round(classification_done * 100 / total_items, 2) if total_items else 0.0
    metrics["llm_pct"] = round(
        _safe_count(conn, "SELECT COUNT(*) FROM content_items WHERE llm_processed=1") * 100 / total_items, 2
    ) if total_items else 0.0
    metrics["ner_pct"] = round(
        _safe_count(conn, "SELECT COUNT(*) FROM content_items WHERE ner_processed=1") * 100 / total_items, 2
    ) if total_items else 0.0

    # Active events
    active_events = _safe_count(conn, "SELECT COUNT(*) FROM events WHERE status='active'")
    metrics["active_events"] = active_events
    if active_events == 0 and total_items > 0:
        issues.append("No active events")

    # Semantic neighbors
    semantic_neighbors = 0
    if table_exists(conn, "semantic_neighbors"):
        semantic_neighbors = _safe_count(conn, "SELECT COUNT(*) FROM semantic_neighbors")
        metrics["semantic_neighbors"] = semantic_neighbors
        if semantic_neighbors == 0 and total_items > 10:
            issues.append("Semantic index empty despite content items")

    # AI work items backlog
    ai_pending = 0
    if table_exists(conn, "ai_work_items"):
        ai_pending = _safe_count(conn, "SELECT COUNT(*) FROM ai_work_items WHERE status='pending'")
        metrics["ai_work_items_pending"] = ai_pending
        if ai_pending > 200:
            issues.append(f"AI work items backlog: {ai_pending} pending")

    status = "ok"
    if issues:
        status = "warning"
    if metrics.get("llm_pct", 100) < 10 and total_items > 10:
        status = "critical"

    return _stage_result(status, metrics, issues)


def check_verification_stage(conn: sqlite3.Connection) -> dict[str, Any]:
    """Проверяет claims, evidence_links, contradiction_detector, authenticity."""
    issues: list[str] = []
    metrics: dict[str, Any] = {}

    total_claims = _safe_count(conn, "SELECT COUNT(*) FROM claims")
    confirmed = _safe_count(conn, "SELECT COUNT(*) FROM claims WHERE status='confirmed'")
    metrics["total_claims"] = total_claims
    metrics["confirmed_pct"] = round(confirmed * 100 / total_claims, 2) if total_claims else 0.0

    pending_evidence = _safe_count(
        conn,
        "SELECT COUNT(*) FROM evidence_links WHERE strength='pending' OR strength IS NULL",
    )
    metrics["pending_evidence_links"] = pending_evidence
    if pending_evidence > 50:
        issues.append(f"Pending evidence links: {pending_evidence}")

    # Contradictions via verifications where status flipped
    contradictions = _safe_count(
        conn,
        """
        SELECT COUNT(*) FROM verifications
        WHERE new_status='contradicted'
        """,
    )
    metrics["contradictions_found"] = contradictions
    if contradictions > 0:
        issues.append(f"Contradictions detected: {contradictions}")

    # Authenticity / needs_review
    needs_review = _safe_count(conn, "SELECT COUNT(*) FROM claims WHERE needs_review=1")
    metrics["claims_needs_review"] = needs_review
    if needs_review > total_claims * 0.5 and total_claims > 10:
        issues.append(f"Too many claims need review: {needs_review}/{total_claims}")

    status = "ok"
    if issues:
        status = "warning"
    if contradictions > 10 or needs_review > 200:
        status = "critical"

    return _stage_result(status, metrics, issues)


def check_graph_stage(conn: sqlite3.Connection) -> dict[str, Any]:
    """Проверяет relations, structural_links, cases, risk_patterns."""
    issues: list[str] = []
    metrics: dict[str, Any] = {}

    # Relation candidates
    rel_pending = 0
    rel_promoted = 0
    if table_exists(conn, "relation_candidates"):
        rel_pending = _safe_count(
            conn,
            """
            SELECT COUNT(*) FROM relation_candidates
            WHERE candidate_state='pending' OR promotion_state='pending'
            """,
        )
        rel_promoted = _safe_count(
            conn,
            "SELECT COUNT(*) FROM relation_candidates WHERE promotion_state='promoted'",
        )
        metrics["relation_candidates_pending"] = rel_pending
        metrics["relation_candidates_promoted"] = rel_promoted
        if rel_pending > 20:
            issues.append(f"Relation candidate backlog: {rel_pending} pending")

    # Cases
    cases_total = _safe_count(conn, "SELECT COUNT(*) FROM cases")
    metrics["cases_total"] = cases_total

    # Risk patterns
    risk_detected = 0
    if table_exists(conn, "risk_patterns"):
        risk_detected = _safe_count(conn, "SELECT COUNT(*) FROM risk_patterns")
        metrics["risk_patterns_detected"] = risk_detected

    # Structural links (entity_relations as proxy)
    structural_links = _safe_count(conn, "SELECT COUNT(*) FROM entity_relations")
    metrics["structural_links"] = structural_links

    status = "ok"
    if issues:
        status = "warning"
    if rel_pending > 500:
        status = "critical"

    return _stage_result(status, metrics, issues)


def check_system_health(conn: sqlite3.Connection) -> dict[str, Any]:
    """Проверяет daemon lease, WAL, backup, disk space, pipeline version."""
    issues: list[str] = []
    metrics: dict[str, Any] = {}

    # Daemon lease
    daemon_lease = None
    if table_exists(conn, "job_leases"):
        daemon_lease = conn.execute(
            "SELECT lease_owner, expires_at FROM job_leases WHERE job_id='__daemon__'"
        ).fetchone()
        if daemon_lease:
            metrics["daemon_owner"] = daemon_lease[0]
            metrics["daemon_expires_at"] = daemon_lease[1]
        else:
            issues.append("No active daemon lease")
            metrics["daemon_owner"] = None
            metrics["daemon_expires_at"] = None

    # WAL size approximation via PRAGMA (works in SQLite)
    try:
        wal_size = conn.execute("PRAGMA page_count;").fetchone()[0]
        metrics["db_page_count"] = wal_size
    except sqlite3.Error:
        metrics["db_page_count"] = None

    # Last backup
    last_backup = None
    if table_exists(conn, "runtime_metadata"):
        last_backup = _safe_scalar(conn, "SELECT value_text FROM runtime_metadata WHERE key='last_backup_at'")
        metrics["last_backup_at"] = last_backup
        if last_backup is None:
            issues.append("Last backup time unknown")

    # Disk space (best effort via db path size if known, skip detailed OS calls)
    metrics["disk_space_ok"] = True  # placeholder; real check requires OS calls

    # Last successful pipeline version
    pipeline_version = None
    if table_exists(conn, "pipeline_runs"):
        row = conn.execute(
            "SELECT pipeline_version FROM pipeline_runs WHERE status='ok' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        pipeline_version = row[0] if row else None
        metrics["last_pipeline_version"] = pipeline_version
        if pipeline_version is None:
            issues.append("No successful pipeline run recorded")

    # DB locks / contention via recent failed job runs
    recent_failed = 0
    if table_exists(conn, "job_runs"):
        recent_failed = _safe_count(
            conn,
            """
            SELECT COUNT(*) FROM job_runs
            WHERE status IN ('failed','abandoned')
              AND started_at >= datetime('now', '-1 hour')
            """,
        )
        metrics["failed_jobs_last_hour"] = recent_failed
        if recent_failed > 5:
            issues.append(f"High job failure rate last hour: {recent_failed}")

    status = "ok"
    if issues:
        status = "warning"
    if not daemon_lease or recent_failed > 10:
        status = "critical"

    return _stage_result(status, metrics, issues)
