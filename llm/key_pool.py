from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypeVar


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


PROVIDER_CATALOG: dict[str, list[dict[str, Any]]] = {
    "openai": [
        {
            "model_name": "gpt-5",
            "capability_tier": 5,
            "supports_web_search": 1,
            "supports_reasoning": 1,
            "supports_background": 1,
            "stage_roles": ["event_synthesis", "arbiter", "relation_reasoning"],
        }
    ],
    "perplexity": [
        {
            "model_name": "sonar-reasoning-pro",
            "capability_tier": 5,
            "supports_web_search": 1,
            "supports_reasoning": 1,
            "supports_background": 0,
            "stage_roles": ["structured_extract", "event_link_hint", "relation_reasoning"],
        },
        {
            "model_name": "sonar-deep-research",
            "capability_tier": 5,
            "supports_web_search": 1,
            "supports_reasoning": 1,
            "supports_background": 0,
            "stage_roles": ["event_synthesis", "evidence_research"],
        },
    ],
    "groq": [
        {
            "model_name": "groq/compound",
            "capability_tier": 4,
            "supports_web_search": 1,
            "supports_reasoning": 1,
            "supports_background": 0,
            "stage_roles": ["clean_factual_text", "structured_extract", "tag_reasoning"],
        },
        {
            "model_name": "groq/compound-mini",
            "capability_tier": 3,
            "supports_web_search": 1,
            "supports_reasoning": 1,
            "supports_background": 0,
            "stage_roles": ["triage", "tag_reasoning"],
        },
    ],
    "mistral": [
        {
            "model_name": "mistral-medium-2505",
            "capability_tier": 4,
            "supports_web_search": 1,
            "supports_reasoning": 1,
            "supports_background": 0,
            "stage_roles": ["clean_factual_text", "structured_extract", "event_link_hint"],
        }
    ],
    "openrouter": [
        {
            "model_name": "openrouter/auto",
            "capability_tier": 3,
            "supports_web_search": 1,
            "supports_reasoning": 1,
            "supports_background": 0,
            "stage_roles": ["overflow", "fallback"],
        }
    ],
}

SUPPORTED_PROVIDERS = tuple(PROVIDER_CATALOG.keys())
T = TypeVar("T")


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _is_sqlite_lock(error: sqlite3.OperationalError) -> bool:
    text = str(error).lower()
    return "database is locked" in text or "database table is locked" in text


def _with_sqlite_write_retry(operation: Callable[[], T], conn: sqlite3.Connection, *, attempts: int = 8) -> T:
    last_error: sqlite3.OperationalError | None = None
    for index in range(max(1, int(attempts))):
        try:
            return operation()
        except sqlite3.OperationalError as error:
            if not _is_sqlite_lock(error):
                raise
            last_error = error
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            time.sleep(min(0.2, 0.025 * (2**index)))
    if last_error is not None:
        raise last_error
    raise RuntimeError("sqlite write retry failed without an error")


def _hash_key(provider: str, api_key: str) -> str:
    return hashlib.sha256(f"{provider}:{api_key}".encode("utf-8")).hexdigest()


def _extract_balanced_json_array(text: str, marker: str) -> list[dict[str, Any]]:
    marker_index = text.find(marker)
    if marker_index < 0:
        return []
    array_start = text.find("[", marker_index)
    if array_start < 0:
        return []
    depth = 0
    in_string = False
    escape = False
    for idx in range(array_start, len(text)):
        char = text[idx]
        if escape:
            escape = False
            continue
        if char == "\\":
            escape = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                candidate = text[array_start : idx + 1]
                parsed = json.loads(candidate)
                return parsed if isinstance(parsed, list) else []
    return []


def _load_key_file(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    try:
        payload = json.loads(text)
        return payload if isinstance(payload, dict) else {"keys": []}
    except json.JSONDecodeError:
        keys = _extract_balanced_json_array(text, '"keys"')
        counts_match = re.search(r'"provider_counts"\s*:\s*(\{[\s\S]*?\})\s*,', text)
        provider_counts = {}
        if counts_match:
            try:
                provider_counts = json.loads(counts_match.group(1))
            except json.JSONDecodeError:
                provider_counts = {}
        return {"keys": keys, "provider_counts": provider_counts}


def _refresh_provider_health(conn: sqlite3.Connection):
    providers = {row[0] for row in conn.execute("SELECT DISTINCT provider FROM llm_provider_models")}
    for provider in providers:
        row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) AS active_keys,
                MAX(last_used_at) AS last_success
            FROM llm_keys
            WHERE provider=?
            """,
            (provider,),
        ).fetchone()
        active_keys = int((row[0] or 0) if row else 0)
        status = "healthy" if active_keys > 0 else "depleted"
        conn.execute(
            """
            INSERT INTO llm_provider_health(provider, status, active_key_count, last_success_at, updated_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(provider) DO UPDATE SET
                status=excluded.status,
                active_key_count=excluded.active_key_count,
                last_success_at=COALESCE(excluded.last_success_at, llm_provider_health.last_success_at),
                updated_at=excluded.updated_at
            """,
            (provider, status, active_keys, row[1] if row else None, now_iso()),
        )


def reactivate_recoverable_keys(conn: sqlite3.Connection) -> dict[str, int]:
    recovered = 0
    statements = [
        (
            """
            UPDATE llm_keys
            SET status='active', failure_count=0, removed_at=NULL, updated_at=?
            WHERE status='removed'
              AND (
                    (last_failure_kind='provider' AND (
                        LOWER(COALESCE(last_error, '')) LIKE '%invalid_tools%'
                        OR LOWER(COALESCE(last_error, '')) LIKE '%connector is not supported%'
                        OR LOWER(COALESCE(last_error, '')) LIKE '%not a valid model id%'
                        OR LOWER(COALESCE(last_error, '')) LIKE '%invalid model%'
                    ))
                    OR LOWER(COALESCE(last_error, '')) LIKE '%429%'
                    OR LOWER(COALESCE(last_error, '')) LIKE '%rate limit%'
                    OR LOWER(COALESCE(last_error, '')) LIKE '%too many requests%'
              )
            """,
        ),
        (
            """
            UPDATE llm_keys
            SET status='active', failure_count=0, removed_at=NULL, updated_at=?
            WHERE status='removed'
              AND last_failure_kind='timeout'
            """,
        ),
    ]
    for (sql,) in statements:
        before = conn.total_changes
        conn.execute(sql, (now_iso(),))
        recovered += conn.total_changes - before
    _refresh_provider_health(conn)
    conn.commit()
    return {"reactivated": recovered}


def bootstrap_provider_catalog(conn: sqlite3.Connection) -> dict[str, int]:
    inserted = 0
    updated = 0
    for provider, models in PROVIDER_CATALOG.items():
        for model in models:
            cursor = conn.execute(
                """
                INSERT INTO llm_provider_models(
                    provider, model_name, capability_tier, stage_roles_json,
                    supports_web_search, supports_reasoning, supports_background,
                    is_active, metadata_json, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(provider, model_name) DO UPDATE SET
                    capability_tier=excluded.capability_tier,
                    stage_roles_json=excluded.stage_roles_json,
                    supports_web_search=excluded.supports_web_search,
                    supports_reasoning=excluded.supports_reasoning,
                    supports_background=excluded.supports_background,
                    is_active=excluded.is_active,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    provider,
                    model["model_name"],
                    int(model.get("capability_tier", 1)),
                    _json_dumps(model.get("stage_roles", [])),
                    int(bool(model.get("supports_web_search"))),
                    int(bool(model.get("supports_reasoning"))),
                    int(bool(model.get("supports_background"))),
                    int(bool(model.get("is_active", 1))),
                    _json_dumps({"origin": "catalog"}),
                    now_iso(),
                    now_iso(),
                ),
            )
            if cursor.rowcount == 1:
                inserted += 1
            else:
                updated += 1
        conn.execute(
            """
            INSERT INTO llm_provider_health(provider, status, active_key_count, updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(provider) DO NOTHING
            """,
            (provider, "unknown", 0, now_iso()),
        )
    _refresh_provider_health(conn)
    conn.commit()
    return {"inserted": inserted, "updated": updated}


def import_keys_from_file(
    conn: sqlite3.Connection,
    path: str | Path,
    supported_providers: tuple[str, ...] | None = None,
) -> dict[str, int]:
    payload = _load_key_file(Path(path))
    supported = set(supported_providers or SUPPORTED_PROVIDERS)
    inserted = 0
    updated = 0
    skipped = 0
    for entry in payload.get("keys", []):
        if not isinstance(entry, dict):
            skipped += 1
            continue
        provider = str(entry.get("provider") or "").strip().lower()
        api_key = str(entry.get("api_key") or "").strip()
        if not provider or not api_key or provider not in supported:
            skipped += 1
            continue
        key_hash = _hash_key(provider, api_key)
        existing = conn.execute(
            "SELECT id, status, failure_count, removed_at FROM llm_keys WHERE key_hash=?",
            (key_hash,),
        ).fetchone()
        metadata = {
            "imported_from": str(path),
            "raw_status": entry.get("status") or "active",
        }
        if existing:
            existing_status = str(existing[1] or "active")
            effective_status = "removed" if existing_status == "removed" else "active"
            conn.execute(
                """
                UPDATE llm_keys
                SET provider=?, api_key=?, status=?, metadata_json=?, updated_at=?
                WHERE id=?
                """,
                (
                    provider,
                    api_key,
                    effective_status,
                    _json_dumps(metadata),
                    now_iso(),
                    int(existing[0]),
                ),
            )
            updated += 1
        else:
            conn.execute(
                """
                INSERT INTO llm_keys(
                    provider, api_key, key_hash, status, failure_count, metadata_json, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (provider, api_key, key_hash, "active", 0, _json_dumps(metadata), now_iso(), now_iso()),
            )
            inserted += 1
    _refresh_provider_health(conn)
    conn.commit()
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def record_key_failure(
    conn: sqlite3.Connection,
    key_id: int,
    *,
    failure_kind: str,
    error_text: str,
    remove_threshold: int = 3,
    failure_code: str | None = None,
) -> dict[str, Any]:
    def operation() -> dict[str, Any]:
        row = conn.execute(
            "SELECT provider, failure_count, status FROM llm_keys WHERE id=?",
            (int(key_id),),
        ).fetchone()
        if row is None:
            raise KeyError(f"Unknown llm key: {key_id}")
        provider = str(row[0])
        new_count = int(row[1] or 0) + 1
        removed = new_count >= int(remove_threshold or 3)
        conn.execute(
            """
            INSERT INTO llm_key_failures(key_id, provider, failure_kind, failure_code, error_text, created_at)
            VALUES(?,?,?,?,?,?)
            """,
            (int(key_id), provider, failure_kind, failure_code, error_text, now_iso()),
        )
        conn.execute(
            """
            UPDATE llm_keys
            SET failure_count=?, status=?, last_error=?, last_failure_kind=?, removed_at=?, updated_at=?
            WHERE id=?
            """,
            (
                new_count,
                "removed" if removed else "active",
                error_text,
                failure_kind,
                now_iso() if removed else None,
                now_iso(),
                int(key_id),
            ),
        )
        _refresh_provider_health(conn)
        conn.commit()
        return {"key_id": int(key_id), "provider": provider, "removed": removed, "failure_count": new_count}

    return _with_sqlite_write_retry(operation, conn)


def record_key_success(conn: sqlite3.Connection, key_id: int) -> None:
    def operation() -> None:
        conn.execute(
            """
            UPDATE llm_keys
            SET last_used_at=?, updated_at=?, failure_count=0, status='active'
            WHERE id=?
            """,
            (now_iso(), now_iso(), int(key_id)),
        )
        _refresh_provider_health(conn)
        conn.commit()

    _with_sqlite_write_retry(operation, conn)


def list_active_keys(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            k.id,
            k.provider,
            k.api_key,
            k.failure_count,
            k.last_used_at,
            m.model_name,
            m.capability_tier,
            m.stage_roles_json
        FROM llm_keys k
        JOIN llm_provider_models m
          ON m.provider = k.provider
         AND m.is_active = 1
         AND m.supports_web_search = 1
        WHERE k.status='active'
        ORDER BY
            k.failure_count ASC,
            COALESCE(k.last_used_at, '') ASC,
            m.capability_tier DESC,
            k.id ASC
        """
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                "key_id": int(row[0]),
                "provider": str(row[1]),
                "api_key": str(row[2]),
                "failure_count": int(row[3] or 0),
                "last_used_at": row[4],
                "model_name": str(row[5]),
                "capability_tier": int(row[6] or 1),
                "stage_roles": json.loads(row[7] or "[]"),
            }
        )
    return items


def choose_key_for_stage(
    conn: sqlite3.Connection,
    *,
    stage: str,
    provider_priority: list[str] | tuple[str, ...] | None = None,
    exclude_key_ids: set[int] | None = None,
) -> dict[str, Any] | None:
    exclude = exclude_key_ids or set()
    candidates = [item for item in list_active_keys(conn) if item["key_id"] not in exclude]
    if not candidates:
        return None
    priority = list(provider_priority or ["mistral", "perplexity", "groq", "openrouter", "openai"])

    def rank(item: dict[str, Any]) -> tuple[int, int, int, str, int]:
        try:
            provider_rank = priority.index(item["provider"])
        except ValueError:
            provider_rank = len(priority) + 1
        stage_roles = set(item.get("stage_roles") or [])
        role_bonus = 0 if stage in stage_roles or "overflow" in stage_roles else 1
        return (
            role_bonus,
            provider_rank,
            -int(item.get("capability_tier") or 1),
            str(item.get("last_used_at") or ""),
            int(item["key_id"]),
        )

    candidates.sort(key=rank)
    return candidates[0]
