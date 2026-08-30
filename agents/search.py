from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse

from agents.bus import complete_agent_task, record_agent_artifact


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True)


def _json_loads(value: Any, default: Any = None) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def _hash_text(value: str) -> str:
    return hashlib.sha256(str(value or "").strip().lower().encode("utf-8")).hexdigest()


def _dedupe_key(url: str | None, title: str | None = None) -> str:
    raw_url = str(url or "").strip()
    if raw_url:
        parsed = urlparse(raw_url)
        host = parsed.netloc.lower()
        path = parsed.path.rstrip("/")
        return f"url:{host}{path}".lower()
    return f"title:{_hash_text(title or '')[:32]}"


def _task_payload(conn: sqlite3.Connection, task_id: int) -> dict[str, Any]:
    row = conn.execute("SELECT payload_json FROM agent_tasks WHERE id=?", (int(task_id),)).fetchone()
    if not row:
        return {}
    raw = row["payload_json"] if isinstance(row, sqlite3.Row) else row[0]
    payload = _json_loads(raw, {})
    return payload if isinstance(payload, dict) else {}


def _result_items(result: dict[str, Any]) -> list[dict[str, Any]]:
    output_json = result.get("output_json")
    if not isinstance(output_json, dict):
        output_json = {}
    items = output_json.get("search_results")
    if not isinstance(items, list):
        items = output_json.get("citations") if isinstance(output_json.get("citations"), list) else []
    if not items and isinstance(result.get("citations"), list):
        items = result["citations"]
    normalized: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, str):
            normalized.append({"url": item})
        elif isinstance(item, dict):
            normalized.append(dict(item))
    return normalized


def persist_search_result(conn: sqlite3.Connection, task_id: int, result: dict[str, Any]) -> dict[str, Any]:
    payload = _task_payload(conn, task_id)
    query_text = str(
        payload.get("query")
        or payload.get("query_text")
        or payload.get("needed_evidence")
        or payload.get("document_identifiers")
        or f"agent-task:{task_id}"
    )
    query_hash = _hash_text(query_text)
    provider = str(result.get("provider") or "")
    model = str(result.get("model") or "")
    written = 0
    for item in _result_items(result):
        url = str(item.get("url") or item.get("source_url") or "").strip()
        title = str(item.get("title") or item.get("name") or "").strip()
        snippet = str(item.get("snippet") or item.get("text") or item.get("summary") or "").strip()
        dedupe_key = _dedupe_key(url, title)
        try:
            confidence = float(item.get("confidence") or result.get("confidence") or 0)
        except (TypeError, ValueError):
            confidence = 0.0
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO search_evidence(
                task_id, query_hash, query_text, provider, model, url, title, snippet,
                citation_json, source_tier, confidence, dedupe_key
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(task_id),
                query_hash,
                query_text,
                provider,
                model,
                url or None,
                title or None,
                snippet or None,
                _json_dumps(item),
                item.get("source_tier") or item.get("tier"),
                confidence,
                dedupe_key,
            ),
        )
        written += max(0, int(cur.rowcount or 0))
    record_agent_artifact(
        conn,
        task_id=int(task_id),
        artifact_type="search_result",
        payload={"query_text": query_text, "result": result, "search_evidence_written": written},
        confidence=float(result.get("confidence") or 0),
    )
    complete_agent_task(
        conn,
        int(task_id),
        result={"search_evidence_written": written, "provider": provider, "model": model},
    )
    conn.commit()
    return {"search_evidence_written": written, "query_hash": query_hash}
