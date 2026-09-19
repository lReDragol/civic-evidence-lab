from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from telethon import TelegramClient, errors
except Exception:  # pragma: no cover - optional dependency on some CI hosts
    TelegramClient = None
    errors = None

from config.db_utils import SECRETS_PATH, get_db, load_settings
from db.file_store import attach_file, ensure_raw_blob
from runtime.state import update_source_sync_state
from .evidence_archive import EvidenceArchive, ArchiveError, _hash_file

from .telegram_collector import (
    _enqueue_document_review,
    _insert_relevance_votes,
    _telegram_source_key,
    classify_message_relevance,
)
from .telegram_session_pool import (
    active_telegram_sessions,
    assign_telegram_sources,
    import_telegram_sessions,
    mark_session_progress,
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


class _NoCommit:
    """Keep shared state helpers inside the collector's per-message transaction."""

    def __init__(self, conn):
        self.conn = conn

    def execute(self, *args, **kwargs):
        return self.conn.execute(*args, **kwargs)

    def commit(self):
        pass


class TelegramSourceError(RuntimeError):
    def __init__(self, failure_class, error_type):
        self.failure_class = failure_class
        super().__init__(error_type)


def _is_flood_wait(error):
    return errors is not None and isinstance(error, getattr(errors, "FloodWaitError", ()))


def _cursor(conn, source_id):
    row = conn.execute(
        "SELECT last_external_id FROM source_sync_state WHERE source_key=?",
        (_telegram_source_key(source_id),),
    ).fetchone()
    # MAX(raw.external_id) is not proof of a contiguous completed traversal.
    try:
        return max(0, int(row[0])) if row and row[0] is not None else 0
    except (TypeError, ValueError):
        return 0


def _payload(msg, source, handle, relevance):
    media = getattr(msg, "document", None) or getattr(msg, "photo", None)
    payload = {
        "message_id": msg.id,
        "date": msg.date.isoformat() if getattr(msg, "date", None) else "",
        "edit_date": msg.edit_date.isoformat() if getattr(msg, "edit_date", None) else None,
        "text": getattr(msg, "message", None) or "",
        "views": getattr(msg, "views", None),
        "forwards": getattr(msg, "forwards", None),
        "has_media": bool(getattr(msg, "media", None)),
        "media_identity": str(getattr(media, "id", "")),
        "media_type": type(getattr(msg, "media", None)).__name__,
        "source_title": source["name"],
        "channel_handle": handle,
        "public_url": _message_url(handle, msg.id),
        "transport": "telegram_telethon",
        "relevance": relevance,
        "collection_decision": {
            "status": "kept" if relevance.get("keep", True) else "skipped",
            "reasons": [] if relevance.get("keep", True) else (relevance.get("reasons") or ["filtered"]),
        },
    }
    stable = {key: payload[key] for key in (
        "text", "edit_date", "media_identity", "media_type", "has_media",
    )}
    payload["_content_fingerprint"] = hashlib.sha256(
        json.dumps(stable, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    if payload["has_media"] and media is None:
        payload["media_archive"] = {"status": "not_downloadable", "reason": "no_photo_or_document"}
    return payload


async def _persist_message(client, msg, source, conn, settings, archive):
    source_id = int(source["id"])
    ext_id = str(msg.id)
    text = getattr(msg, "message", None) or ""
    relevance = classify_message_relevance(
        text, has_media=bool(getattr(msg, "media", None)), source=source,
        store_mode=str(settings.get("telegram_store_mode", "negative_only") or "negative_only"),
    )
    payload = _payload(msg, source, _handle(source["url"]), relevance)
    raw_json = json.dumps(payload, ensure_ascii=False, default=str)
    raw_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
    existing = conn.execute(
        "SELECT id, raw_payload, hash_sha256 FROM raw_source_items WHERE source_id=? AND external_id=?",
        (source_id, ext_id),
    ).fetchone()
    keep = relevance.get("keep", True)
    changed = False
    if existing:
        original = json.loads(existing["raw_payload"] or "{}")
        previous = original.get("_content_fingerprint")
        revision = conn.execute(
            "SELECT metadata_json FROM raw_blobs WHERE raw_item_id=? AND blob_type='telegram_revision' ORDER BY id DESC LIMIT 1",
            (existing["id"],),
        ).fetchone()
        if revision:
            previous = json.loads(revision[0] or "{}").get("content_fingerprint", previous)
        if previous is None:
            # Legacy records lack stable fingerprints. Only positively observed
            # text/edit-date changes count as edits; counters are not revisions.
            changed = (original.get("text", "") != text or
                       original.get("edit_date") != payload["edit_date"])
        else:
            changed = previous != payload["_content_fingerprint"]

    media = getattr(msg, "document", None) or getattr(msg, "photo", None)
    archived = None
    if keep and media is not None:
        valid_media = False
        if existing and not changed:
            for row in conn.execute(
                "SELECT file_path, hash_sha256, file_size, metadata_json FROM raw_blobs "
                "WHERE raw_item_id=? AND blob_type IN ('photo','document')",
                (existing["id"],),
            ):
                path = Path(row[0])
                metadata = json.loads(row[3] or "{}")
                if (metadata.get("media_identity") == payload["media_identity"]
                        and path.is_file() and row[1] and path.stat().st_size == row[2]
                        and _hash_file(path) == row[1]):
                    valid_media = True
                    break
        if not valid_media:
            archived = await archive.download(client, msg)
            payload["media_archive"] = {
                "status": "archived", "sha256": archived.sha256, "file_size": archived.size,
            }
            raw_json = json.dumps(payload, ensure_ascii=False, default=str)
            raw_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()

    # Preserve original raw payload, canonical/user-edited text and old media.
    # Observed revisions are immutable files, not destructive UPDATEs.
    if changed:
        payload["observed_at"] = datetime.now(timezone.utc).isoformat()
        raw_json = json.dumps(payload, ensure_ascii=False, default=str)
    revision_file = archive.store_bytes(raw_json.encode("utf-8")) if changed else None
    if existing:
        raw_id = int(existing["id"])
    else:
        raw_id = conn.execute(
            "INSERT INTO raw_source_items(source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed) "
            "VALUES(?,?,?,?,?,?)",
            (source_id, ext_id, raw_json, datetime.now().isoformat(), raw_hash, 0 if keep else 1),
        ).lastrowid
    if revision_file:
        ensure_raw_blob(
            conn, raw_id, revision_file.path, "telegram_revision",
            original_url=f'{payload["public_url"]}#revision-{revision_file.sha256}',
            mime_type="application/json", hash_sha256=revision_file.sha256, file_size=revision_file.size,
            metadata={"content_fingerprint": payload["_content_fingerprint"], "edit_date": payload["edit_date"]},
        )
    content = conn.execute(
        "SELECT id FROM content_items WHERE raw_item_id=? ORDER BY id LIMIT 1", (raw_id,),
    ).fetchone()
    created_content = False
    if keep and content is None:
        title = (text.split("\n", 1)[0].strip() if text else f"Telegram post {ext_id}")[:200]
        body_hash = hashlib.sha256(" ".join(text.split()).encode("utf-8")).hexdigest() if text.strip() else None
        content_id = conn.execute(
            "INSERT INTO content_items(source_id, raw_item_id, external_id, content_type, title, body_text, body_hash, "
            "published_at, collected_at, url, status) VALUES(?,?,?,?,?,?,?,?,?,?,'raw_signal')",
            (source_id, raw_id, ext_id, "post", title, text, body_hash, payload["date"],
             datetime.now().isoformat(), payload["public_url"]),
        ).lastrowid
        _insert_relevance_votes(conn, content_id, relevance)
        _enqueue_document_review(
            conn, content_id=content_id, source_id=source_id, external_id=ext_id,
            public_url=payload["public_url"], relevance=relevance,
        )
        created_content = True
    else:
        content_id = content[0] if content else None
    if archived and content_id:
        file = getattr(msg, "file", None)
        attach_file(
            conn, content_id, raw_id, archived.path,
            "document" if getattr(msg, "document", None) is not None else "photo",
            original_url=f'{payload["public_url"]}#sha256-{archived.sha256}',
            hash_sha256=archived.sha256, file_size=archived.size,
            mime_type=getattr(file, "mime_type", None) or "application/octet-stream",
            metadata={"transport": "telegram_telethon", "original_filename": getattr(file, "name", None),
                      "media_identity": payload["media_identity"]},
        )
    outcome = "new" if created_content else "updated" if changed else "duplicate" if existing else "skipped"
    return outcome, raw_hash if not existing else existing["hash_sha256"]


async def _collect_source(
    client, source, conn, settings: dict[str, Any], *,
    session_key: str | None = None, current_job_id: str = "telegram_telethon_pool",
) -> tuple[int, int]:
    handle = _handle(source["url"])
    source_id = int(source["id"])
    source_key = _telegram_source_key(source_id)
    limit = max(1, min(1000, int(settings.get("telegram_posts_per_channel", 100) or 100)))
    edit_limit = max(0, min(100, int(settings.get("telegram_edit_scan_limit", 20))))
    recent_limit = max(0, min(100, int(settings.get("telegram_recent_capture_limit", 20))))
    offset_id = _cursor(conn, source_id)
    checkpoint = offset_id
    counts = {"new": 0, "duplicate": 0, "skipped": 0, "updated": 0}
    seen = 0
    last_hash = None
    common = dict(
        source_key=source_key, source_id=source_id, transport_mode="telegram_telethon",
        current_job_id=current_job_id, current_channel=handle, current_telegram_session=session_key,
    )

    def state(db, *, success, collecting, failure=None):
        update_source_sync_state(
            db, **common, success=success, is_collecting=collecting,
            last_cursor=str(checkpoint), last_external_id=str(checkpoint), last_hash=last_hash,
            failure_class=failure, last_error=failure,
            items_current_run=counts["new"], duplicates_current_run=counts["duplicate"],
            failed_items_current_run=1 if failure else 0,
            metadata={"handle": handle, "items_seen": seen, "items_new": counts["new"],
                      "duplicates": counts["duplicate"], "skipped": counts["skipped"],
                      "items_updated": counts["updated"], "checkpoint_traversal": "oldest_first",
                      "recent_capture_limit": recent_limit,
                      "recent_capture_advances_checkpoint": False},
        )

    def progress(db, collecting, **kwargs):
        if session_key:
            mark_session_progress(
                db, session_key, source_id=source_id, channel=handle,
                collecting_now=collecting, current_job_id=current_job_id, **kwargs,
            )

    async def consume(msg, advance):
        nonlocal checkpoint, seen, last_hash
        previous_checkpoint, previous_hash = checkpoint, last_hash
        seen += 1
        conn.execute("SAVEPOINT telegram_message")
        try:
            outcome, item_hash = await _persist_message(client, msg, source, conn, settings, archive)
            if advance:
                checkpoint = int(msg.id)
                last_hash = item_hash
            counts[outcome] += 1
            deferred = _NoCommit(conn)
            state(deferred, success=True, collecting=True)
            progress(
                deferred, True, last_message_id=str(msg.id),
                last_message_date=msg.date.isoformat() if getattr(msg, "date", None) else None,
                collected_delta=int(outcome == "new"), duplicate_delta=int(outcome == "duplicate"),
            )
            conn.execute("RELEASE SAVEPOINT telegram_message")
            conn.commit()
        except BaseException:
            # Helpers receive a no-commit facade so partial inserts cannot escape.
            if conn.in_transaction:
                conn.execute("ROLLBACK TO SAVEPOINT telegram_message")
                conn.execute("RELEASE SAVEPOINT telegram_message")
            checkpoint, last_hash = previous_checkpoint, previous_hash
            if "outcome" in locals():
                counts[outcome] -= 1
            raise

    stage = "resolve_failed"
    try:
        if not handle:
            raise ValueError("empty_handle")
        state(conn, success=None, collecting=True)
        progress(conn, True)
        entity = await client.get_entity(handle)
        stage = "collection_failed"
        archive = EvidenceArchive(settings)
        # Preserve disappearing recent posts even while the contiguous history
        # traversal is far behind. A sampled tail is never a coverage checkpoint.
        if recent_limit:
            async for msg in client.iter_messages(entity, limit=recent_limit, min_id=offset_id, reverse=False):
                if msg:
                    await consume(msg, False)
        # A finite descending page plus MAX(id) permanently skips the backlog.
        # Telethon reverse=True makes min_id an exclusive oldest-first offset.
        async for msg in client.iter_messages(entity, limit=limit, min_id=offset_id, reverse=True):
            if not msg:
                continue
            if int(msg.id) <= checkpoint:
                raise TelegramSourceError("non_contiguous_order", "message_order")
            await consume(msg, True)

        # Revisit only a bounded tail of previously checkpointed IDs. Never move
        # the forward checkpoint for edits/deletions and never overwrite user text.
        if edit_limit and offset_id:
            ids = [int(row[0]) for row in conn.execute(
                "SELECT external_id FROM raw_source_items WHERE source_id=? "
                "AND CAST(external_id AS INTEGER)>0 AND CAST(external_id AS INTEGER)<=? "
                "ORDER BY CAST(external_id AS INTEGER) DESC LIMIT ?",
                (source_id, offset_id, edit_limit),
            )]
            if ids:
                for msg in await client.get_messages(entity, ids=ids):
                    if msg and int(msg.id) in ids:
                        await consume(msg, False)
        state(conn, success=True, collecting=False)
        progress(conn, False)
        return seen, counts["new"]
    except BaseException as error:
        failure = (
            "flood_wait" if _is_flood_wait(error) else
            "cancelled" if isinstance(error, asyncio.CancelledError) else
            "timeout" if isinstance(error, TimeoutError) else
            getattr(error, "failure_class", None) or
            ("archive_failed" if isinstance(error, ArchiveError) else stage)
        )
        state(conn, success=False, collecting=False, failure=failure)
        progress(conn, False, failed_delta=1)
        run_counts = {"seen": seen, **counts}
        if _is_flood_wait(error) or not isinstance(error, Exception):
            error.telegram_counts = run_counts
            raise
        # Exception messages may contain session paths, request objects or keys.
        safe_error = TelegramSourceError(failure, type(error).__name__)
        safe_error.telegram_counts = run_counts
        raise safe_error from None


async def _collect_with_sessions(settings: dict[str, Any]) -> dict[str, Any]:
    if TelegramClient is None:
        return {"ok": False, "items_seen": 0, "items_new": 0, "warnings": [], "fatal_errors": ["telethon_not_available"]}

    api_id, api_hash = _load_api_credentials(settings)
    if not api_id or not api_hash:
        return {"ok": False, "items_seen": 0, "items_new": 0, "warnings": [], "fatal_errors": ["telegram_api_credentials_missing"]}

    conn = get_db(settings)
    try:
        import_result = import_telegram_sessions(conn, settings)
        # Discovery specs contain session paths. They are operational input, not
        # evidence or safe collector diagnostics.
        import_summary = {key: import_result.get(key) for key in ("ok", "active_count", "failed_count")}
        assignment_result = assign_telegram_sources(conn)
        assignment_version = assignment_result["assignment_version"]
        sessions = [item for item in active_telegram_sessions(conn) if item.get("client_type") == "telethon"]
        items_seen = 0
        items_new = 0
        items_updated = 0
        warnings: list[str] = []

        for session in sessions:
            session_key = str(session["session_key"])
            source_rows = _source_rows_for_session(conn, session_key, assignment_version)
            if not source_rows:
                continue
            client = None
            try:
                client = TelegramClient(
                    str(Path(session["session_path"]).with_suffix("")), api_id, api_hash,
                    flood_sleep_threshold=0,
                )
                await client.connect()
                if not await client.is_user_authorized():
                    mark_session_result(
                        conn,
                        session_key,
                        success=False,
                        failure_class="unauthorized_session",
                        metadata={"reason": "is_user_authorized_false"},
                    )
                    warnings.append("unauthorized_session")
                    continue
                session_items_new = 0
                for source in source_rows:
                    seen, new = await _collect_source(
                        client,
                        source,
                        conn,
                        settings,
                        session_key=session_key,
                        current_job_id="telegram_telethon_pool",
                    )
                    items_seen += seen
                    items_new += new
                    session_items_new += new
                    source_state = conn.execute(
                        "SELECT metadata_json FROM source_sync_state WHERE source_key=?",
                        (_telegram_source_key(int(source["id"])),),
                    ).fetchone()
                    if source_state:
                        items_updated += int(json.loads(source_state[0] or "{}").get("items_updated", 0))
                mark_session_result(conn, session_key, success=True, metadata={"assigned": len(source_rows), "items_new": session_items_new})
            except Exception as error:
                partial = getattr(error, "telegram_counts", {})
                items_seen += partial.get("seen", 0)
                items_new += partial.get("new", 0)
                items_updated += partial.get("updated", 0)
                failure_class = getattr(error, "failure_class", "runtime_error")
                cooldown_until = None
                if errors is not None and isinstance(error, getattr(errors, "FloodWaitError", ())):
                    failure_class = "flood_wait"
                    seconds = int(getattr(error, "seconds", 300) or 300)
                    cooldown_until = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=seconds)).isoformat()
                elif isinstance(error, TimeoutError) or failure_class == "timeout":
                    failure_class = "timeout"
                    cooldown_until = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=10)).isoformat()
                mark_session_result(
                    conn,
                    session_key,
                    success=False,
                    failure_class=failure_class,
                    cooldown_until=cooldown_until,
                    metadata={"error_type": type(error).__name__},
                )
                warnings.append(f"{failure_class}:{type(error).__name__}")
            finally:
                try:
                    if client is not None:
                        await client.disconnect()
                except Exception:
                    pass

        update_source_sync_state(
            conn,
            source_key="telegram_telethon_pool",
            success=not warnings,
            transport_mode="telegram_telethon",
            metadata={"sessions": import_summary, "assignments": assignment_result, "items_new": items_new},
        )
        conn.commit()
        return {
            "ok": not warnings,
            "items_seen": items_seen,
            "items_new": items_new,
            "items_updated": items_updated,
            "warnings": warnings[:20],
            "artifacts": {
                "sessions": import_summary,
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
