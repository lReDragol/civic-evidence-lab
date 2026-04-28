import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in os.sys.path:
    os.sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings, ensure_dirs
from db.file_store import materialize_attachment
from runtime.state import record_dead_letter, update_source_sync_state

log = logging.getLogger(__name__)

try:
    from pyrogram import Client
    from pyrogram.types import Message
    HAVE_PYROGRAM = True
except ImportError:
    HAVE_PYROGRAM = False
    log.error("pyrogram not installed")


def _get_source_id(conn: sqlite3.Connection, url: str) -> Optional[int]:
    handle = url.replace("t.me/", "@").strip()
    row = conn.execute(
        "SELECT id FROM sources WHERE (url=? OR url=?) AND category='telegram' AND is_active=1",
        (url, handle),
    ).fetchone()
    return row[0] if row else None


def _telegram_source_key(source_id: int) -> str:
    return f"telegram:{int(source_id)}"


def _load_telegram_cursor(conn: sqlite3.Connection, source_id: int) -> int:
    row = conn.execute(
        "SELECT last_external_id FROM source_sync_state WHERE source_key=? LIMIT 1",
        (_telegram_source_key(source_id),),
    ).fetchone()
    if row and row[0] is not None:
        try:
            return int(str(row[0]).strip())
        except (TypeError, ValueError):
            pass
    last_ext_id_row = conn.execute(
        "SELECT CAST(external_id AS INTEGER) FROM raw_source_items WHERE source_id=? ORDER BY CAST(external_id AS INTEGER) DESC LIMIT 1",
        (source_id,),
    ).fetchone()
    return int(last_ext_id_row[0]) if last_ext_id_row and last_ext_id_row[0] is not None else 0


async def collect_channel(app: Client, channel_url: str, source_id: int, conn: sqlite3.Connection, settings: dict, limit: int = 100):
    handle = channel_url.replace("https://t.me/", "@").replace("http://t.me/", "@").replace("t.me/", "@")
    if not handle.startswith("@"):
        handle = "@" + handle

    source_key = _telegram_source_key(source_id)
    source_name = handle
    try:
        peer = await app.get_chat(handle)
        source_name = getattr(peer, "title", handle) or handle
    except Exception as e:
        log.error("Cannot resolve %s: %s", handle, e)
        update_source_sync_state(
            conn,
            source_key=source_key,
            source_id=source_id,
            success=False,
            transport_mode="telegram",
            last_error=f"{type(e).__name__}: {e}",
            metadata={"channel_handle": handle, "channel_url": channel_url},
        )
        return 0

    offset_id = _load_telegram_cursor(conn, source_id)

    storage_dir = Path(settings.get("processed_telegram", str(Path(__file__).resolve().parent.parent / "processed" / "telegram")))

    collected = 0
    max_external_id = offset_id
    last_hash = None
    async for msg in app.get_chat_history(peer.id, limit=limit, offset_id=offset_id):
        if not msg:
            continue
        ext_id = str(msg.id)
        existing = conn.execute(
            "SELECT id FROM raw_source_items WHERE source_id=? AND external_id=?",
            (source_id, ext_id),
        ).fetchone()
        if existing:
            continue

        text = msg.text or msg.caption or ""
        date_iso = msg.date.isoformat() if msg.date else ""

        payload = {
            "message_id": msg.id,
            "date": date_iso,
            "text": text,
            "views": msg.views,
            "forwards": msg.forwards,
            "post_author": msg.post_author,
            "grouped_id": msg.grouped_id,
            "reply_to_message_id": msg.reply_to_message_id if msg.reply_to_message else None,
            "has_media": bool(msg.media),
            "source_title": source_name,
            "channel_handle": handle,
        }
        raw_json = json.dumps(payload, ensure_ascii=False, default=str)
        hash_sha = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()

        cur = conn.execute(
            """INSERT INTO raw_source_items(source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed)
               VALUES(?,?,?,?,?,0)""",
            (source_id, ext_id, raw_json, datetime.now().isoformat(), hash_sha),
        )
        raw_id = cur.lastrowid

        title = ""
        if text:
            first_line = text.split("\n")[0].strip()
            title = first_line[:200]

        cur2 = conn.execute(
            """INSERT INTO content_items(source_id, raw_item_id, external_id, content_type, title, body_text, published_at, collected_at, url, status)
               VALUES(?,?,?,?,?,?,?,?,?,'raw_signal')""",
            (source_id, raw_id, ext_id, "post", title, text, date_iso, datetime.now().isoformat(), channel_url),
        )
        content_id = cur2.lastrowid

        media_paths = []
        ts = msg.date.strftime("%Y-%m") if msg.date else "unknown"
        base_dir = storage_dir / ts / re.sub(r'[^\w]', '_', source_name)
        base_dir.mkdir(parents=True, exist_ok=True)

        if msg.photo:
            fname = f"{msg.id}_photo.jpg"
            fpath = base_dir / fname
            media_paths.append(("photo", fpath))

        if msg.video:
            ext = "mp4"
            fname = f"{msg.id}_video.{ext}"
            fpath = base_dir / fname
            media_paths.append(("video", fpath))

        if msg.document:
            doc = msg.document
            ext = "bin"
            if doc and doc.mime_type:
                ext_map = {
                    "video/mp4": "mp4", "audio/mpeg": "mp3", "audio/ogg": "ogg",
                    "application/pdf": "pdf", "image/png": "png", "image/jpeg": "jpg",
                }
                ext = ext_map.get(doc.mime_type, doc.mime_type.split("/")[-1][:5])
            fname = f"{msg.id}_doc.{ext}"
            fpath = base_dir / fname
            media_paths.append(("document", fpath))

        for media_type, fpath in media_paths:
            conn.execute(
                """INSERT INTO attachments(content_item_id, file_path, attachment_type, hash_sha256, file_size, mime_type)
                   VALUES(?,?,?,?,?,?)""",
                (content_id, str(fpath), media_type, "", 0, ""),
            )

        collected += 1
        max_external_id = max(max_external_id, int(msg.id))
        last_hash = hash_sha

    conn.commit()
    update_source_sync_state(
        conn,
        source_key=source_key,
        source_id=source_id,
        success=True,
        last_cursor=str(max_external_id) if max_external_id else None,
        last_external_id=str(max_external_id) if max_external_id else None,
        last_hash=last_hash,
        transport_mode="telegram",
        metadata={
            "channel_handle": handle,
            "channel_url": channel_url,
            "source_title": source_name,
            "collected": collected,
        },
    )
    log.info("Collected %d new messages from %s", collected, source_name)
    return collected


async def download_media_batch(app: Client, conn: sqlite3.Connection, settings: dict, limit: int = 200):
    rows = conn.execute(
        """
        SELECT a.id, a.file_path, a.attachment_type, r.source_id, r.external_id, s.url, c.id AS content_id
        FROM attachments a
        JOIN content_items c ON c.id = a.content_item_id
        JOIN raw_source_items r ON r.id = c.raw_item_id
        JOIN sources s ON s.id = r.source_id
        WHERE a.hash_sha256 = '' AND a.file_path != '' AND s.category = 'telegram'
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    if not rows:
        log.info("No media to download")
        return {"downloaded": 0, "failed": 0}

    downloaded = 0
    failed = 0
    for row in rows:
        att_id = row["id"]
        file_path = row["file_path"]
        att_type = row["attachment_type"]
        source_id = int(row["source_id"])
        external_id = row["external_id"]
        channel_url = row["url"]
        content_id = int(row["content_id"]) if row["content_id"] is not None else None
        source_key = _telegram_source_key(source_id)

        target = Path(file_path)
        if target.exists() and target.stat().st_size > 0:
            materialize_attachment(conn, att_id)
            downloaded += 1
            update_source_sync_state(
                conn,
                source_key=source_key,
                source_id=source_id,
                success=True,
                last_cursor=str(external_id),
                last_external_id=str(external_id),
                transport_mode="telegram_media",
                metadata={"attachment_id": att_id, "status": "materialized_existing"},
            )
            continue

        handle = channel_url.replace("https://t.me/", "@").replace("http://t.me/", "@").replace("t.me/", "@")
        if not handle.startswith("@"):
            handle = "@" + handle

        try:
            peer = await app.get_chat(handle)
            msg = await app.get_messages(peer.id, int(external_id))
            if msg:
                target.parent.mkdir(parents=True, exist_ok=True)
                download_type = None
                if att_type == "photo" and msg.photo:
                    download_type = msg.photo
                elif att_type == "video" and msg.video:
                    download_type = msg.video
                elif att_type == "document" and msg.document:
                    download_type = msg.document

                if download_type:
                    await app.download_media(download_type, file_name=str(target))
                    if target.exists():
                        materialize_attachment(conn, att_id)
                        downloaded += 1
                        update_source_sync_state(
                            conn,
                            source_key=source_key,
                            source_id=source_id,
                            success=True,
                            last_cursor=str(external_id),
                            last_external_id=str(external_id),
                            transport_mode="telegram_media",
                            metadata={"attachment_id": att_id, "status": "downloaded_media"},
                        )
        except Exception as e:
            log.warning("Failed to download %s/%s: %s", handle, external_id, e)
            failed += 1
            record_dead_letter(
                conn,
                failure_stage="telegram_media_download",
                source_key=source_key,
                source_id=source_id,
                external_id=str(external_id),
                attachment_id=att_id,
                content_item_id=content_id,
                error_type=type(e).__name__,
                error_message=str(e),
                payload={
                    "channel_url": channel_url,
                    "file_path": file_path,
                    "attachment_type": att_type,
                },
            )
            update_source_sync_state(
                conn,
                source_key=source_key,
                source_id=source_id,
                success=False,
                last_cursor=str(external_id),
                last_external_id=str(external_id),
                transport_mode="telegram_media",
                last_error=f"{type(e).__name__}: {e}",
                metadata={"attachment_id": att_id, "status": "download_failed"},
            )

    conn.commit()
    log.info("Downloaded %d media files", downloaded)
    return {"downloaded": downloaded, "failed": failed}


async def run_collect(settings: dict = None):
    if settings is None:
        settings = load_settings()

    if not HAVE_PYROGRAM:
        log.error("Pyrogram not available")
        return {"ok": False, "fatal_errors": ["pyrogram_not_available"]}

    api_id = settings.get("telegram_api_id")
    api_hash = settings.get("telegram_api_hash")

    env_api_id = os.getenv("CIVIC_TG_API_ID") or os.getenv("TELEGRAM_API_ID")
    env_api_hash = os.getenv("CIVIC_TG_API_HASH") or os.getenv("TELEGRAM_API_HASH")

    if env_api_id:
        api_id = int(env_api_id)
    if env_api_hash:
        api_hash = env_api_hash

    if not api_id or not api_hash:
        log.error("telegram_api_id/hash not set. Set in config/settings.json or env CIVIC_TG_API_ID / CIVIC_TG_API_HASH")
        return {"ok": False, "fatal_errors": ["telegram_api_credentials_missing"]}

    session_dir = Path(settings.get("telegram_session_dir", str(Path(__file__).resolve().parent.parent / "config")))
    session_path = session_dir / "news_collector"
    session_dir.mkdir(parents=True, exist_ok=True)

    conn = get_db(settings)
    channels = conn.execute(
        """
        SELECT id, url, name FROM sources
        WHERE category='telegram'
          AND is_active=1
          AND access_method IN ('telegram_tdlib', 'pyrogram', 'telegram')
        """
    ).fetchall()
    conn.close()

    if not channels:
        log.info("No active Telegram sources")
        return {"ok": True, "items_seen": 0, "items_new": 0, "items_updated": 0, "channels": 0}

    limit = settings.get("telegram_posts_per_channel", 100)

    app = Client(
        str(session_path),
        api_id=int(api_id),
        api_hash=str(api_hash),
        workdir=str(session_dir),
    )

    async with app:
        conn = get_db(settings)
        try:
            total = 0
            for ch in channels:
                n = await collect_channel(app, ch["url"], ch["id"], conn, settings, limit=limit)
                total += n
            log.info("Total collected: %d", total)

            media_stats = await download_media_batch(app, conn, settings)
            warnings = []
            if media_stats.get("failed"):
                warnings.append(f"telegram_media_failed:{media_stats['failed']}")
            return {
                "ok": True,
                "items_seen": len(channels),
                "items_new": total,
                "items_updated": int(media_stats.get("downloaded") or 0),
                "channels": len(channels),
                "media_failed": int(media_stats.get("failed") or 0),
                "warnings": warnings,
            }
        finally:
            conn.commit()
            conn.close()


def main():
    asyncio.run(run_collect())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main()
