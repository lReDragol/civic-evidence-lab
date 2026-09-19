from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any

from agents.bus import (
    complete_agent_task,
    enqueue_agent_task,
    lease_agent_task,
    record_agent_message,
)
from agents.search import persist_search_result
from config.db_utils import get_db, load_settings
from llm.key_pool import choose_key_for_stage, record_key_failure, record_key_success
from llm.provider_router import run_ai_task

log = logging.getLogger(__name__)

SEARCH_BATCH_SIZE = 50
TASK_LEASE_SECONDS = 20 * 60


def _json_loads(value: Any, default: Any = None) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def _has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _enqueue_ner_entity_search_tasks(conn: sqlite3.Connection, *, limit: int = SEARCH_BATCH_SIZE) -> int:
    if not _has_table(conn, "ner_new_entities"):
        return 0

    rows = conn.execute(
        """
        SELECT id, entity_id, entity_type, canonical_name, source_content_id
        FROM ner_new_entities
        WHERE search_triggered = 0
        ORDER BY discovered_at ASC, id ASC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    created_or_seen = 0
    for row in rows:
        payload = {
            "query": f"{row['canonical_name']} {row['entity_type'] or ''} официальные источники связи документы",
            "needed_evidence": "entity_context_or_official_bridge",
            "entity_id": int(row["entity_id"]) if row["entity_id"] is not None else None,
            "entity_name": row["canonical_name"],
            "entity_type": row["entity_type"] or "unknown",
            "source_content_id": int(row["source_content_id"]) if row["source_content_id"] is not None else None,
            "accepted_sources": ["official", "archive", "registry", "court", "parliamentary", "documentary"],
            "reject_if_only_telegram": True,
        }
        task = enqueue_agent_task(
            conn,
            task_type="search_request",
            requester_group="entity_resolution",
            target_group="search_classify",
            subject_type="entity",
            subject_id=int(row["entity_id"]) if row["entity_id"] is not None else None,
            payload=payload,
            acceptance={
                "required": ["citation"],
                "forbidden": ["telegram_only_without_official_support"],
            },
            priority=45,
        )
        if task.get("created"):
            record_agent_message(
                conn,
                task_id=int(task["task_id"]),
                message_type="search_request",
                sender_group="entity_resolution",
                recipient_group="search_classify",
                payload=payload,
            )
        conn.execute("UPDATE ner_new_entities SET search_triggered = 1 WHERE id=?", (int(row["id"]),))
        created_or_seen += 1
    conn.commit()
    return created_or_seen


def _task_payload(task: dict[str, Any]) -> dict[str, Any]:
    payload = _json_loads(task.get("payload_json"), {})
    return payload if isinstance(payload, dict) else {}


def _task_acceptance(task: dict[str, Any]) -> dict[str, Any]:
    acceptance = _json_loads(task.get("acceptance_json"), {})
    return acceptance if isinstance(acceptance, dict) else {}


def _build_ai_task(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "stage": "agent_search",
        "task_id": int(task["id"]),
        "task_type": task.get("task_type"),
        "requester_group": task.get("requester_group"),
        "target_group": task.get("target_group"),
        "subject_type": task.get("subject_type"),
        "subject_id": task.get("subject_id"),
        "payload": _task_payload(task),
        "acceptance": _task_acceptance(task),
    }


def _run_one_search_task(conn: sqlite3.Connection, task: dict[str, Any]) -> dict[str, Any]:
    task_id = int(task["id"])
    lease_owner = str(task.get("lease_owner") or "")
    key = choose_key_for_stage(conn, stage="agent_search", requires_web_search=True)
    if not key:
        complete_agent_task(
            conn,
            task_id,
            status="needs_retry",
            failure_kind="no_web_search_key",
            error_text="No active key/model supports agent_search with web search",
            lease_owner=lease_owner,
            lease_token=task.get("lease_token"),
        )
        return {"ok": False, "failure_kind": "no_web_search_key", "search_evidence_written": 0}

    model_name = key.get("model_name") or key.get("model") or ""
    if not model_name:
        complete_agent_task(
            conn,
            task_id,
            status="needs_retry",
            failure_kind="provider_model",
            error_text=f"No model_name for key {key.get('key_id')}",
            lease_owner=lease_owner,
            lease_token=task.get("lease_token"),
        )
        return {"ok": False, "failure_kind": "provider_model", "search_evidence_written": 0}

    try:
        result = run_ai_task(
            conn=conn,
            provider=key["provider"],
            model=model_name,
            api_key=key["api_key"],
            task=_build_ai_task(task),
        )
        result.setdefault("provider", key["provider"])
        result.setdefault("model", model_name)
        record_key_success(conn, int(key["key_id"]))
        persisted = persist_search_result(conn, task_id, result, lease_owner=lease_owner, lease_token=task.get("lease_token"))
        return {"ok": True, **persisted}
    except Exception as exc:
        try:
            record_key_failure(conn, int(key["key_id"]), failure_kind="api_error", error_text=str(exc))
        except Exception as failure_exc:  # pragma: no cover - defensive logging
            log.warning("agent_search key failure recording failed: %s", failure_exc)
        complete_agent_task(
            conn,
            task_id,
            status="needs_retry",
            failure_kind="api_error",
            error_text=str(exc),
            lease_owner=lease_owner,
            lease_token=task.get("lease_token"),
        )
        log.warning("agent_search task %s failed: %s", task_id, exc)
        return {"ok": False, "failure_kind": "api_error", "search_evidence_written": 0}


def run_agent_search(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    conn = get_db(settings)
    try:
        enqueued_from_ner = _enqueue_ner_entity_search_tasks(conn, limit=SEARCH_BATCH_SIZE)

        processed = 0
        completed = 0
        retry = 0
        evidence_written = 0
        seen_task_ids: set[int] = set()
        lease_owner = str(settings.get("agent_search_worker") or "agent_search")

        for _ in range(SEARCH_BATCH_SIZE):
            task = lease_agent_task(
                conn,
                lease_owner=lease_owner,
                target_group="search_classify",
                lease_seconds=TASK_LEASE_SECONDS,
            )
            if not task:
                break
            task_id = int(task["id"])
            if task_id in seen_task_ids:
                break
            seen_task_ids.add(task_id)
            processed += 1
            result = _run_one_search_task(conn, task)
            if result.get("ok"):
                completed += 1
                evidence_written += int(result.get("search_evidence_written") or 0)
            else:
                retry += 1

        return {
            "ok": True,
            "items_seen": processed,
            "items_new": evidence_written,
            "items_updated": completed,
            "warnings": [] if retry == 0 else [f"{retry} search tasks need retry"],
            "artifacts": {
                "ner_tasks_enqueued": enqueued_from_ner,
                "agent_tasks_processed": processed,
                "agent_tasks_completed": completed,
                "agent_tasks_needs_retry": retry,
                "search_evidence_written": evidence_written,
            },
        }
    finally:
        conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    result = run_agent_search()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
