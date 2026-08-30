from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from agents.bus import enqueue_agent_task
from agents.pipeline_monitor import (
    check_analysis_stage,
    check_collection_stage,
    check_enrichment_stage,
    check_graph_stage,
    check_system_health,
    check_verification_stage,
)
from config.db_utils import get_db
from runtime.state import set_runtime_metadata


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def _create_review_task(
    conn: sqlite3.Connection,
    *,
    task_key: str,
    subject_type: str,
    subject_id: int | None,
    suggested_action: str,
    machine_reason: str,
    confidence: float = 0.0,
    candidate_payload: dict[str, Any] | None = None,
) -> int | None:
    """Insert or ignore a review_task for operator attention."""
    try:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO review_tasks(
                task_key, queue_key, subject_type, subject_id,
                suggested_action, confidence, machine_reason, candidate_payload, status, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                task_key,
                "pipeline_monitor",
                subject_type,
                subject_id,
                suggested_action,
                confidence,
                machine_reason,
                json.dumps(candidate_payload or {}, ensure_ascii=False, default=str),
                "open",
                _now_iso(),
                _now_iso(),
            ),
        )
        conn.commit()
        if cur.rowcount and cur.rowcount > 0:
            return int(cur.lastrowid)
    except sqlite3.Error:
        # If table/schema mismatch, silently skip
        pass
    return None


def _emit_alert_task(
    conn: sqlite3.Connection,
    *,
    stage: str,
    issue: str,
    metrics: dict[str, Any],
    priority: int = 40,
) -> dict[str, Any]:
    """Enqueue an agent_task for a pipeline stage alert."""
    payload = {
        "alert_source": "pipeline_monitor",
        "stage": stage,
        "issue": issue,
        "metrics": metrics,
        "timestamp": _now_iso(),
    }
    return enqueue_agent_task(
        conn,
        task_type="pipeline_alert",
        requester_group="pipeline_monitor",
        target_group="ops_review",
        subject_type="pipeline_stage",
        subject_id=None,
        payload=payload,
        priority=priority,
        input_hash=json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str),
    )


def run_monitoring_cycle(settings: dict[str, Any]) -> dict[str, Any]:
    """Run all pipeline checks, store results in runtime_metadata, and create tasks for issues."""
    conn = get_db(settings)
    try:
        results: dict[str, Any] = {}
        created_review_tasks: list[dict[str, Any]] = []
        created_agent_tasks: list[dict[str, Any]] = []

        checks = [
            ("collection", check_collection_stage),
            ("enrichment", check_enrichment_stage),
            ("analysis", check_analysis_stage),
            ("verification", check_verification_stage),
            ("graph", check_graph_stage),
            ("system_health", check_system_health),
        ]

        for stage_name, check_fn in checks:
            result = check_fn(conn)
            results[stage_name] = result
            # Store full result JSON in runtime_metadata
            set_runtime_metadata(conn, f"pipeline_monitor:{stage_name}", result)

            status = result.get("status", "ok")
            issues = result.get("issues", [])
            metrics = result.get("metrics", {})

            if status in ("warning", "critical") and issues:
                for issue in issues:
                    review_id = _create_review_task(
                        conn,
                        task_key=f"pipeline:{stage_name}:{_now_iso()}:{issue[:80]}",
                        subject_type="pipeline_stage",
                        subject_id=None,
                        suggested_action="review",
                        machine_reason=issue,
                        confidence=0.9 if status == "critical" else 0.6,
                        candidate_payload={"stage": stage_name, "metrics": metrics, "issue": issue},
                    )
                    if review_id:
                        created_review_tasks.append({"stage": stage_name, "review_task_id": review_id, "issue": issue})

                    # Emit agent task for critical issues and select warnings
                    if status == "critical" or "backlog" in issue.lower() or "degraded" in issue.lower():
                        agent_task = _emit_alert_task(
                            conn,
                            stage=stage_name,
                            issue=issue,
                            metrics=metrics,
                            priority=20 if status == "critical" else 35,
                        )
                        created_agent_tasks.append({"stage": stage_name, "agent_task": agent_task, "issue": issue})

        # Store aggregate snapshot
        snapshot = {
            "checked_at": _now_iso(),
            "stages": {name: res["status"] for name, res in results.items()},
            "total_issues": sum(len(r.get("issues", [])) for r in results.values()),
        }
        set_runtime_metadata(conn, "pipeline_monitor:last_cycle", snapshot)

        return {
            "results": results,
            "snapshot": snapshot,
            "review_tasks": created_review_tasks,
            "agent_tasks": created_agent_tasks,
        }
    finally:
        conn.close()
