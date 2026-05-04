from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    from telethon import TelegramClient, errors
except Exception:  # pragma: no cover - optional dependency on some CI hosts
    TelegramClient = None
    errors = None

from config.db_utils import PROJECT_ROOT, SECRETS_PATH, get_db, load_settings
from runtime.state import update_source_sync_state

from .telegram_collector import (
    _enqueue_document_review,
    _insert_relevance_votes,
    _load_telegram_cursor,
    _telegram_source_key,
    classify_message_relevance,
)
from .telegram_session_pool import (
    active_telegram_sessions,
    assign_telegram_sources,
    import_telegram_sessions,
    mark_session_result,
)


log = logging.getLogger(__name__)


def _load_api_credentials(settings: dict[str, Any]) -> tuple[int | None, str | None]:
    api_id = settings.get("telegram_api_id")
    api_hash = settings.get("telegram_api_hash")
    env_api_id = os.getenv("CIVIC_TG_API_ID") or os.getenv("TELEGRAM_API_ID")
    env_api_hash = os.getenv("CIVIC_TG_API_HASH") or os.getenv("TELEGRAM_API_HASH")
    if env_api_id:
        try:
            api_id = int(env_api_id)
        except ValueError:
            api_id = None
    if env_api_hash:
        api_hash = env_api_hash
    if (not api_id or not api_hash) and SECRETS_PATH.exists():
        try:
            secrets = json.loads(SECRETS_PATH.read_text(encoding="utf-8"))
            api_id = api_id or secrets.get("telegram_api_id")
            api_hash = api_hash or secrets.get("telegram_api_hash")
        except Exception:
            pass
    try:
        api_id = int(api_id) if api_id else None
    except (TypeError, ValueError):
        api_id = None
    return api_id, str(api_hash) if api_hash else None


def _handle(url: str) -> str:
    text = str(url or "").strip()
    text = text.replace("https://", "").replace("http://", "")
    text = text.removeprefix("t.me/s/").removeprefix("t.me/")
    text = text.strip("/@ ")
    return text


def _message_url(handle: str, message_id: int | str) -> str:
    return f"https://t.me/{handle.strip('/@ ')}/{message_id}"


def _source_rows_for_session(conn, session_key: str, assignment_version: str) -> list[Any]:
    return conn.execute(
        """
        SELECT s.id, s.name, s.url, s.subcategory, s.is_official, s.credibility_tier,
               s.owner, s.bias_notes, s.political_alignment, s.notes
        FROM telegram_source_assignments a
        JOIN sources s ON s.id = a.source_id
        WHERE a.session_key=?
          AND a.assignment_version=?
          AND a.is_active=1
          AND s.category='telegram'
          AND s.is_active=1
        ORDER BY s.id
        """,
        (session_key, assignment_version),
    ).fetchall()


async def _collect_source(client, source, conn, settings: dict[str, Any]) -> tuple[int, int]:
    handle = _handle(source["url"])
    if not handle:
        return 0, 0
    source_id = int(source["id"])
    source_key = _telegram_source_key(source_id)
    limit = int(settings.get("telegram_posts_per_channel", 100) or 100)
    offset_id = _load_telegram_cursor(conn, source_id)
    store_mode = str(settings.get("telegram_store_mode", "negative_only") or "negative_only")
    storage_dir = Path(settings.get("processed_telegram", str(PROJECT_ROOT / "processed" / "telegram")))
    items_seen = 0
    items_new = 0
    max_external_id = offset_id
    last_hash = None

    try:
        entity = await client.get_entity(handle)
    except Exception as error:
        update_source_sync_state(
            conn,
            source_key=source_key,
            source_id=source_id,
            success=False,
            transport_mode="telegram_telethon",
            failure_class="resolve_failed",
            last_error=f"{type(error).__name__}: {error}",
            metadata={"handle": handle},
        )
        return 0, 0

    async for msg in client.iter_messages(entity, limit=limit, min_id=offset_id):
        if not msg:
            continue
        items_seen += 1
        ext_id = str(msg.id)
        existing = conn.execute(
            "SELECT id FROM raw_source_items WHERE source_id=? AND external_id=?",
            (source_id, ext_id),
        ).fetchone()
        if existing:
            continue

        text = msg.message or ""
        has_media = bool(getattr(msg, "media", None))
        relevance = classify_message_relevance(text, has_media=has_media, source=source, store_mode=store_mode)
        if not relevance.get("keep", True):
            continue

        public_url = _message_url(handle, msg.id)
        date_iso = msg.date.isoformat() if getattr(msg, "date", None) else ""
        payload = {
            "message_id": msg.id,
            "date": date_iso,
            "text": text,
            "views": getattr(msg, "views", None),
            "forwards": getattr(msg, "forwards", None),
            "has_media": has_media,
            "source_title": source["name"],
            "channel_handle": handle,
            "public_url": public_url,
            "transport": "telegram_telethon",
            "relevance": relevance,
        }
        raw_json = json.dumps(payload, ensure_ascii=False, default=str)
        hash_sha = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
        raw_cur = conn.execute(
            """
            INSERT INTO raw_source_items(source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed)
            VALUES(?,?,?,?,?,0)
            """,
            (source_id, ext_id, raw_json, datetime.now().isoformat(), hash_sha),
        )
        title = (text.split("\n", 1)[0].strip() if text else f"Telegram post {ext_id}")[:200]
        content_cur = conn.execute(
            """
            INSERT INTO content_items(source_id, raw_item_id, external_id, content_type, title, body_text, published_at, collected_at, url, status)
            VALUES(?,?,?,?,?,?,?,?,?,'raw_signal')
            """,
            (source_id, raw_cur.lastrowid, ext_id, "post", title, text, date_iso, datetime.now().isoformat(), public_url),
        )
        content_id = int(content_cur.lastrowid)
        _insert_relevance_votes(conn, content_id, relevance)
        _enqueue_document_review(
            conn,
            content_id=content_id,
            source_id=source_id,
            external_id=ext_id,
            public_url=public_url,
            relevance=relevance,
        )

        if has_media:
            base_dir = storage_dir / "telethon" / re.sub(r"[^\w.-]+", "_", handle)
            base_dir.mkdir(parents=True, exist_ok=True)
            target = base_dir / f"{ext_id}"
            conn.execute(
                """
                INSERT INTO attachments(content_item_id, file_path, attachment_type, hash_sha256, file_size, mime_type)
                VALUES(?,?,?,?,?,?)
                """,
                (content_id, str(target), "media", "", 0, ""),
            )

        items_new += 1
        max_external_id = max(max_external_id, int(msg.id))
        last_hash = hash_sha

    update_source_sync_state(
        conn,
        source_key=source_key,
        source_id=source_id,
        success=True,
        last_cursor=str(max_external_id) if max_external_id else None,
        last_external_id=str(max_external_id) if max_external_id else None,
        last_hash=last_hash,
        transport_mode="telegram_telethon",
        metadata={"handle": handle, "items_seen": items_seen, "items_new": items_new},
    )
    return items_seen, items_new


async def _collect_with_sessions(settings: dict[str, Any]) -> dict[str, Any]:
    if TelegramClient is None:
        return {"ok": False, "items_seen": 0, "items_new": 0, "warnings": [], "fatal_errors": ["telethon_not_available"]}

    api_id, api_hash = _load_api_credentials(settings)
    if not api_id or not api_hash:
        return {"ok": False, "items_seen": 0, "items_new": 0, "warnings": [], "fatal_errors": ["telegram_api_credentials_missing"]}

    conn = get_db(settings)
    try:
        import_result = import_telegram_sessions(conn, settings)
        assignment_result = assign_telegram_sources(conn)
        assignment_version = assignment_result["assignment_version"]
        sessions = [item for item in active_telegram_sessions(conn) if item.get("client_type") == "telethon"]
        items_seen = 0
        items_new = 0
        warnings: list[str] = []

        for session in sessions:
            session_key = str(session["session_key"])
            source_rows = _source_rows_for_session(conn, session_key, assignment_version)
            if not source_rows:
                continue
            client = TelegramClient(str(Path(session["session_path"]).with_suffix("")), api_id, api_hash)
            try:
                await client.connect()
                if not await client.is_user_authorized():
                    mark_session_result(
                        conn,
                        session_key,
                        success=False,
                        failure_class="unauthorized_session",
                        metadata={"reason": "is_user_authorized_false"},
                    )
                    warnings.append(f"{session_key}:unauthorized_session")
                    continue
                for source in source_rows:
                    seen, new = await _collect_source(client, source, conn, settings)
                    items_seen += seen
                    items_new += new
                mark_session_result(conn, session_key, success=True, metadata={"assigned": len(source_rows), "items_new": items_new})
            except Exception as error:
                failure_class = "runtime_error"
                cooldown_until = None
                if errors is not None and isinstance(error, getattr(errors, "FloodWaitError", ())):
                    failure_class = "flood_wait"
                    seconds = int(getattr(error, "seconds", 300) or 300)
                    cooldown_until = (datetime.utcnow() + timedelta(seconds=seconds)).isoformat()
                elif "timeout" in str(error).lower():
                    failure_class = "timeout"
                    cooldown_until = (datetime.utcnow() + timedelta(minutes=10)).isoformat()
                mark_session_result(
                    conn,
                    session_key,
                    success=False,
                    failure_class=failure_class,
                    cooldown_until=cooldown_until,
                    metadata={"error": f"{type(error).__name__}: {error}"},
                )
                warnings.append(f"{session_key}:{failure_class}:{type(error).__name__}")
            finally:
                try:
                    await client.disconnect()
                except Exception:
                    pass

        update_source_sync_state(
            conn,
            source_key="telegram_telethon_pool",
            success=True,
            transport_mode="telegram_telethon",
            metadata={"sessions": import_result, "assignments": assignment_result, "items_new": items_new},
        )
        conn.commit()
        return {
            "ok": True,
            "items_seen": items_seen,
            "items_new": items_new,
            "items_updated": 0,
            "warnings": warnings[:20],
            "artifacts": {
                "sessions": import_result,
                "assignments": assignment_result,
                "transport": "telegram_telethon",
            },
        }
    finally:
        conn.close()


def collect_telegram_pool(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    return asyncio.run(_collect_with_sessions(settings or load_settings()))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    print(json.dumps(collect_telegram_pool(), ensure_ascii=False, indent=2, default=str))

