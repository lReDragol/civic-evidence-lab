from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True)


def _json_hash(value: Any) -> str:
    return hashlib.sha256(_json_dumps(value).encode("utf-8")).hexdigest()


def _normalize_key_part(value: Any) -> str:
    text = str(value or "").strip().lower()
    return "".join(ch if ch.isalnum() or ch in {"_", "-", ":"} else "_" for ch in text)[:120] or "none"


def _generated_task_key(
    *,
    task_type: str,
    requester_group: str,
    target_group: str,
    subject_type: str,
    subject_id: int | None,
    input_hash: str,
) -> str:
    return ":".join(
        [
            "agent",
            _normalize_key_part(task_type),
            _normalize_key_part(requester_group),
            _normalize_key_part(target_group),
            _normalize_key_part(subject_type),
            _normalize_key_part(subject_id if subject_id is not None else "none"),
            input_hash[:24],
        ]
    )


def _row_to_dict(row: sqlite3.Row | tuple | None) -> dict[str, Any] | None:
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def enqueue_agent_task(
    conn: sqlite3.Connection,
    *,
    task_type: str,
    requester_group: str,
    target_group: str,
    subject_type: str,
    subject_id: int | None = None,
    payload: dict[str, Any] | None = None,
    acceptance: dict[str, Any] | None = None,
    priority: int = 50,
    input_hash: str | None = None,
    task_key: str | None = None,
    subject_key: str | None = None,
    parent_task_id: int | None = None,
    trace_id: str | None = None,
    deadline_at: str | None = None,
    max_attempts: int = 3,
    commit: bool = True,
) -> dict[str, Any]:
    payload = dict(payload or {})
    acceptance = dict(acceptance or {})
    input_hash = input_hash or _json_hash({"payload": payload, "acceptance": acceptance, "subject_key": subject_key})
    task_key = task_key or _generated_task_key(
        task_type=task_type,
        requester_group=requester_group,
        target_group=target_group,
        subject_type=subject_type,
        subject_id=subject_id,
        input_hash=input_hash,
    )
    now = _now_iso()
    subject_column = "subject_key" if "subject_key" in {r[1] for r in conn.execute("PRAGMA table_info(agent_tasks)")} else "subject_id"
    subject_value = str(subject_key or subject_id or "none") if subject_column == "subject_key" else subject_id
    cur = conn.execute(
        f"""
        INSERT OR IGNORE INTO agent_tasks(
            task_key, task_type, requester_group, target_group, subject_type, {subject_column},
            priority, status, input_hash, payload_json, acceptance_json, created_at, updated_at,
            parent_task_id, trace_id, deadline_at, max_attempts
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            task_key,
            task_type,
            requester_group,
            target_group,
            subject_type,
            subject_value,
            int(priority),
            "pending",
            input_hash,
            _json_dumps(payload),
            _json_dumps(acceptance),
            now,
            now,
            parent_task_id, trace_id or uuid.uuid4().hex, deadline_at, min(3, max(1, int(max_attempts))),
        ),
    )
    created = cur.rowcount > 0
    row = conn.execute("SELECT id, status FROM agent_tasks WHERE task_key=?", (task_key,)).fetchone()
    if commit:
        conn.commit()
    return {"task_id": int(row["id"] if isinstance(row, sqlite3.Row) else row[0]), "created": created, "status": row["status"] if isinstance(row, sqlite3.Row) else row[1]}


def lease_agent_task(
    conn: sqlite3.Connection,
    *,
    lease_owner: str,
    target_group: str | None = None,
    lease_seconds: int = 300,
) -> dict[str, Any] | None:
    now = _now_iso()
    params: list[Any] = [now, now, now, now]
    group_filter = ""
    if target_group:
        group_filter = "AND target_group=?"
        params.append(target_group)
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=max(30, int(lease_seconds)))).replace(
        tzinfo=None,
        microsecond=0,
    ).isoformat()
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("""UPDATE agent_tasks SET status='insufficient_evidence', updated_at=?,
            lease_token=NULL, lease_owner=NULL, lease_expires_at=NULL
            WHERE status IN ('pending','needs_retry','running') AND
            ((deadline_at IS NOT NULL AND deadline_at<=?) OR
             (attempt_count>=max_attempts AND (status!='running' OR lease_expires_at<=?)))""", (now, now, now))
        row = conn.execute(
            f"""
            SELECT id
            FROM agent_tasks
            WHERE ((
                    status IN ('pending', 'needs_retry')
                    AND (lease_expires_at IS NULL OR lease_expires_at<=?)
                  )
               OR (
                    status='running'
                    AND lease_expires_at IS NOT NULL
                    AND lease_expires_at<=?
                  ))
              AND (available_at IS NULL OR available_at<=?)
              AND (deadline_at IS NULL OR deadline_at>?)
              AND attempt_count<max_attempts
              {group_filter}
            ORDER BY priority ASC, id ASC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        task_id = int(row["id"] if isinstance(row, sqlite3.Row) else row[0])
        changed = conn.execute(
            """
            UPDATE agent_tasks
            SET status='running', lease_owner=?, lease_expires_at=?, updated_at=?,
                attempt_count=COALESCE(attempt_count, 0)+1, lease_token=?, heartbeat_at=?
            WHERE id=?
              AND (
                    status IN ('pending', 'needs_retry')
                    OR (status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at<=?)
                  )
            """,
            (lease_owner, expires_at, now, uuid.uuid4().hex, now, task_id, now),
        ).rowcount
        if changed != 1:
            conn.rollback()
            return None
        leased = conn.execute("SELECT * FROM agent_tasks WHERE id=?", (task_id,)).fetchone()
        conn.commit()
        return _row_to_dict(leased)
    except Exception:
        conn.rollback()
        raise


def heartbeat_agent_task(
    conn: sqlite3.Connection,
    task_id: int,
    *,
    lease_owner: str,
    lease_seconds: int = 300,
    lease_token: str,
) -> bool:
    now = _now_iso()
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=max(30, int(lease_seconds)))).replace(
        tzinfo=None,
        microsecond=0,
    ).isoformat()
    changed = conn.execute(
        """
        UPDATE agent_tasks
        SET lease_expires_at=?, heartbeat_at=?, updated_at=?
        WHERE id=? AND status='running' AND lease_owner=? AND lease_token=?
            AND lease_expires_at>? AND (deadline_at IS NULL OR deadline_at>?)
        """,
        (expires_at, now, now, int(task_id), lease_owner, lease_token, now, now),
    ).rowcount
    conn.commit()
    return changed == 1


def complete_agent_task(
    conn: sqlite3.Connection,
    task_id: int,
    *,
    result: dict[str, Any] | None = None,
    status: str = "completed",
    failure_kind: str | None = None,
    error_text: str | None = None,
    lease_owner: str | None = None,
    lease_token: str | None = None,
    commit: bool = True,
    retry_seconds: int = 60,
) -> bool:
    now = _now_iso()
    if not lease_owner or not lease_token:
        raise ValueError("lease_owner and lease_token are required to complete an agent task")
    if status not in {"completed", "needs_retry", "failed", "insufficient_evidence", "needs_user_access"}:
        raise ValueError("Invalid completion status")
    available_at = (datetime.now(timezone.utc) + timedelta(seconds=max(30, retry_seconds))).replace(tzinfo=None, microsecond=0).isoformat()
    changed = conn.execute(
        """
        UPDATE agent_tasks
        SET status=CASE WHEN ?='needs_retry' AND attempt_count>=max_attempts THEN 'insufficient_evidence' ELSE ? END,
            result_json=?, failure_kind=?, error_text=?, lease_owner=NULL, lease_expires_at=NULL, lease_token=NULL,
            available_at=CASE WHEN ?='needs_retry' THEN ? ELSE NULL END,
            heartbeat_at=NULL,
            completed_at=CASE WHEN ?='completed' THEN ? ELSE completed_at END,
            updated_at=?
        WHERE id=? AND status='running' AND lease_owner=? AND lease_token=?
            AND lease_expires_at>? AND (deadline_at IS NULL OR deadline_at>?)
        """,
        (
            status,
            status,
            _json_dumps(result or {}),
            failure_kind,
            error_text,
            status, available_at,
            status,
            now,
            now,
            int(task_id),
            lease_owner, lease_token, now, now,
        ),
    ).rowcount
    if commit:
        conn.commit()
    return changed == 1


def record_agent_message(
    conn: sqlite3.Connection,
    *,
    task_id: int,
    message_type: str,
    sender_group: str,
    recipient_group: str,
    payload: dict[str, Any] | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO agent_messages(task_id, message_type, sender_group, recipient_group, payload_json, created_at)
        VALUES(?,?,?,?,?,?)
        """,
        (int(task_id), message_type, sender_group, recipient_group, _json_dumps(payload or {}), _now_iso()),
    )
    conn.commit()
    return int(cur.lastrowid)


def record_agent_artifact(
    conn: sqlite3.Connection,
    *,
    task_id: int | None = None,
    artifact_type: str,
    payload: dict[str, Any] | None = None,
    confidence: float = 0,
    subject_type: str | None = None,
    subject_id: int | None = None,
    source_links: list[str] | None = None,
    subject_key: str | None = None,
    commit: bool = True,
) -> int:
    subject_column = "subject_key" if "subject_key" in {r[1] for r in conn.execute("PRAGMA table_info(agent_artifacts)")} else "subject_id"
    cur = conn.execute(
        f"""
        INSERT INTO agent_artifacts(
            task_id, artifact_type, subject_type, {subject_column}, payload_json, confidence, source_links_json, created_at
        ) VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            int(task_id) if task_id is not None else None,
            artifact_type,
            subject_type,
            (subject_key or str(subject_id) if subject_id is not None else subject_key) if subject_column == "subject_key" else subject_id,
            _json_dumps(payload or {}),
            float(confidence or 0),
            json.dumps(source_links or [], ensure_ascii=False),
            _now_iso(),
        ),
    )
    if commit:
        conn.commit()
    return int(cur.lastrowid)


def enqueue_document_search_task(
    conn: sqlite3.Connection,
    *,
    content_id: int,
    document_identifiers: dict[str, Any],
    review_task_id: int | None = None,
    source_links: list[str] | None = None,
) -> dict[str, Any]:
    payload = {
        "needed_evidence": "official_source_search",
        "content_item_id": int(content_id),
        "review_task_id": int(review_task_id) if review_task_id is not None else None,
        "document_identifiers": document_identifiers,
        "accepted_sources": ["official", "archive"],
        "reject_if_only_telegram": True,
        "source_links": source_links or [],
    }
    return enqueue_agent_task(
        conn,
        task_type="search_request",
        requester_group="data_structuring",
        target_group="search_classify",
        subject_type="content_item",
        subject_id=int(content_id),
        payload=payload,
        acceptance={
            "required": ["official_url_or_archive_candidate", "text_compare_candidate"],
            "verdict_without_match": "needs_review",
        },
        priority=25,
        input_hash=_json_hash(payload),
    )


def enqueue_relation_gap_task(conn: sqlite3.Connection, row: dict[str, Any]) -> dict[str, Any]:
    candidate_id = int(row["candidate_id"])
    payload = {
        "needed_evidence": "official_or_documentary_bridge",
        "candidate_id": candidate_id,
        "issue": row.get("issue"),
        "promotion_block_reason": row.get("promotion_block_reason"),
        "entities": [
            {"id": row.get("entity_a_id"), "name": row.get("entity_a_name")},
            {"id": row.get("entity_b_id"), "name": row.get("entity_b_name")},
        ],
        "bridge_types": row.get("bridge_types") or [],
        "evidence_mix": row.get("evidence_mix") or {},
        "accepted_sources": ["official", "archive", "documentary"],
        "reject_if_only_telegram": True,
        "source_links": row.get("source_links") or [],
    }
    task = enqueue_agent_task(
        conn,
        task_type="relation_gap",
        requester_group="relation_audit",
        target_group="search_classify",
        subject_type="relation_candidate",
        subject_id=candidate_id,
        payload=payload,
        acceptance={
            "required": ["event_fact_or_official_bridge"],
            "forbidden": ["telegram_only", "same_case_only"],
        },
        priority=30,
        input_hash=_json_hash(payload),
    )
    if task.get("created"):
        record_agent_message(
            conn,
            task_id=int(task["task_id"]),
            message_type="search_request",
            sender_group="relation_audit",
            recipient_group="search_classify",
            payload=payload,
        )
    return task
