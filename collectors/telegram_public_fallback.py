from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from config.db_utils import get_db, load_settings
from db.file_store import materialize_attachment
from runtime.state import record_dead_letter, update_source_sync_state

from .telegram_collector import (
    _enqueue_document_review,
    _insert_relevance_votes,
    _telegram_source_key,
    classify_message_relevance,
)

log = logging.getLogger(__name__)


def _channel_handle(url: str) -> str:
    text = str(url or "").strip()
    text = text.replace("https://", "").replace("http://", "")
    text = text.removeprefix("t.me/s/").removeprefix("t.me/")
    text = text.strip("/@ ")
    return text


def _public_url(handle: str) -> str:
    return f"https://t.me/s/{handle.strip('/@ ')}"


def _message_url(handle: str, external_id: str) -> str:
    return f"https://t.me/{handle.strip('/@ ')}/{external_id}"


def _extract_media_urls(node, base_url: str) -> list[str]:
    urls: list[str] = []
    for photo in node.select(".tgme_widget_message_photo_wrap"):
        style = photo.get("style") or ""
        match = re.search(r"url\(['\"]?([^'\")]+)", style)
        if match:
            urls.append(urljoin(base_url, match.group(1)))
    for img in node.select("img[src]"):
        urls.append(urljoin(base_url, img.get("src")))
    for source in node.select("video source[src], audio source[src]"):
        urls.append(urljoin(base_url, source.get("src")))
    return list(dict.fromkeys(url for url in urls if url))


def _parse_public_posts(html: str, handle: str, base_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html or "", "lxml")
    posts: list[dict[str, Any]] = []
    for node in soup.select(".tgme_widget_message[data-post]"):
        data_post = str(node.get("data-post") or "")
        if "/" not in data_post:
            continue
        post_handle, external_id = data_post.rsplit("/", 1)
        if post_handle.strip("/@ ") != handle.strip("/@ "):
            continue
        text_node = node.select_one(".tgme_widget_message_text")
        text = text_node.get_text("\n", strip=True) if text_node else ""
        time_node = node.select_one("time[datetime]")
        published_at = time_node.get("datetime") if time_node else ""
        media_urls = _extract_media_urls(node, base_url)
        posts.append(
            {
                "handle": handle,
                "external_id": external_id,
                "text": text,
                "published_at": published_at,
                "media_urls": media_urls,
                "public_url": _message_url(handle, external_id),
                "snapshot_url": base_url,
            }
        )
    return posts


def _source_row(conn, handle: str):
    variants = [
        f"https://t.me/{handle}",
        f"http://t.me/{handle}",
        f"t.me/{handle}",
        f"@{handle}",
    ]
    placeholders = ",".join("?" for _ in variants)
    row = conn.execute(
        f"""
        SELECT id, name, url, subcategory, is_official, credibility_tier, owner,
               bias_notes, political_alignment, notes
        FROM sources
        WHERE category='telegram' AND is_active=1 AND url IN ({placeholders})
        ORDER BY id
        LIMIT 1
        """,
        variants,
    ).fetchone()
    if row:
        return row
    cur = conn.execute(
        """
        INSERT INTO sources(name, category, subcategory, url, access_method, is_active, credibility_tier, notes)
        VALUES(?, 'telegram', 'media', ?, 'telegram_public', 1, 'B', ?)
        """,
        (handle.upper(), f"https://t.me/{handle}", "Created by telegram_public_fallback"),
    )
    source_id = cur.lastrowid
    return conn.execute(
        """
        SELECT id, name, url, subcategory, is_official, credibility_tier, owner,
               bias_notes, political_alignment, notes
        FROM sources WHERE id=?
        """,
        (source_id,),
    ).fetchone()


def _download_attachment(session, url: str, target: Path) -> tuple[int, str, str]:
    response = session.get(url, timeout=30, headers={"User-Agent": "CivicEvidenceLab/1.0"})
    response.raise_for_status()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(response.content or b"")
    mime = response.headers.get("content-type", "").split(";", 1)[0] or "application/octet-stream"
    sha = hashlib.sha256(target.read_bytes()).hexdigest()
    return target.stat().st_size, mime, sha


def _store_post(conn, settings: dict[str, Any], source, post: dict[str, Any]) -> tuple[bool, int | None]:
    source_id = int(source["id"])
    external_id = str(post["external_id"])
    existing = conn.execute(
        "SELECT id FROM raw_source_items WHERE source_id=? AND external_id=?",
        (source_id, external_id),
    ).fetchone()
    if existing:
        return False, None

    text = post.get("text") or ""
    relevance = classify_message_relevance(
        text,
        has_media=bool(post.get("media_urls")),
        source=source,
        store_mode=str(settings.get("telegram_store_mode", "negative_only") or "negative_only"),
    )
    if not relevance.get("keep", True):
        return False, None

    payload = {
        "message_id": external_id,
        "date": post.get("published_at") or "",
        "text": text,
        "has_media": bool(post.get("media_urls")),
        "media_urls": post.get("media_urls") or [],
        "source_title": source["name"],
        "channel_handle": post.get("handle"),
        "public_url": post.get("public_url"),
        "snapshot_url": post.get("snapshot_url"),
        "transport": "telegram_public_fallback",
        "relevance": relevance,
    }
    raw_json = json.dumps(payload, ensure_ascii=False, default=str)
    hash_sha = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
    raw_cur = conn.execute(
        """
        INSERT INTO raw_source_items(source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed)
        VALUES(?,?,?,?,?,0)
        """,
        (source_id, external_id, raw_json, datetime.now().isoformat(), hash_sha),
    )
    title = (text.split("\n", 1)[0].strip() if text else f"Telegram post {external_id}")[:200]
    content_cur = conn.execute(
        """
        INSERT INTO content_items(source_id, raw_item_id, external_id, content_type, title, body_text, published_at, collected_at, url, status)
        VALUES(?,?,?,?,?,?,?,?,?,'raw_signal')
        """,
        (
            source_id,
            raw_cur.lastrowid,
            external_id,
            "post",
            title,
            text,
            post.get("published_at") or "",
            datetime.now().isoformat(),
            post.get("public_url") or "",
        ),
    )
    content_id = int(content_cur.lastrowid)
    _insert_relevance_votes(conn, content_id, relevance)
    _enqueue_document_review(
        conn,
        content_id=content_id,
        source_id=source_id,
        external_id=external_id,
        public_url=post.get("public_url") or "",
        relevance=relevance,
    )
    return True, content_id


def collect_public_fallback(
    settings: dict[str, Any] | None = None,
    *,
    urls: list[str] | None = None,
    limit: int | None = None,
    session=None,
) -> dict[str, Any]:
    settings = settings or load_settings()
    urls = urls or list(settings.get("telegram_public_fallback_urls") or ["t.me/yep_news"])
    limit = int(limit if limit is not None else settings.get("telegram_public_fallback_limit", 30) or 30)
    session = session or requests.Session()
    processed_dir = Path(settings.get("processed_telegram", str(Path(__file__).resolve().parent.parent / "processed" / "telegram")))

    conn = get_db(settings)
    items_seen = 0
    items_new = 0
    items_updated = 0
    warnings: list[str] = []
    last_external_id = ""
    try:
        for url in urls:
            handle = _channel_handle(url)
            if not handle:
                continue
            source = _source_row(conn, handle)
            source_id = int(source["id"])
            source_key = _telegram_source_key(source_id)
            page_url = _public_url(handle)
            try:
                response = session.get(page_url, timeout=30, headers={"User-Agent": "CivicEvidenceLab/1.0"})
                response.raise_for_status()
                posts = _parse_public_posts(response.text, handle, page_url)
            except Exception as error:
                warnings.append(f"{handle}:{type(error).__name__}:{error}")
                update_source_sync_state(
                    conn,
                    source_key=source_key,
                    source_id=source_id,
                    success=False,
                    transport_mode="telegram_public_fallback",
                    failure_class="transport",
                    last_error=f"{type(error).__name__}: {error}",
                    metadata={"handle": handle, "url": page_url},
                )
                continue

            for post in posts[:limit]:
                items_seen += 1
                created, content_id = _store_post(conn, settings, source, post)
                if not created or not content_id:
                    continue
                items_new += 1
                last_external_id = str(post["external_id"])
                for idx, media_url in enumerate(post.get("media_urls") or []):
                    ext = ".jpg"
                    target = processed_dir / "public" / handle / f"{post['external_id']}_{idx}{ext}"
                    try:
                        size, mime, sha = _download_attachment(session, media_url, target)
                        cur = conn.execute(
                            """
                            INSERT INTO attachments(content_item_id, file_path, attachment_type, hash_sha256, file_size, mime_type)
                            VALUES(?,?,?,?,?,?)
                            """,
                            (content_id, str(target), "photo", sha, size, mime),
                        )
                        materialize_attachment(conn, cur.lastrowid)
                        items_updated += 1
                    except Exception as error:
                        warnings.append(f"media:{post['external_id']}:{type(error).__name__}:{error}")
                        record_dead_letter(
                            conn,
                            failure_stage="telegram_public_media_download",
                            source_key=source_key,
                            source_id=source_id,
                            external_id=str(post["external_id"]),
                            content_item_id=content_id,
                            error_type=type(error).__name__,
                            error_message=str(error),
                            payload={"media_url": media_url, "public_url": post.get("public_url")},
                        )
            update_source_sync_state(
                conn,
                source_key=source_key,
                source_id=source_id,
                success=True,
                last_cursor=last_external_id or None,
                last_external_id=last_external_id or None,
                transport_mode="telegram_public_fallback",
                metadata={"handle": handle, "url": page_url, "fallback_used": "public"},
            )
        update_source_sync_state(
            conn,
            source_key="telegram",
            success=True,
            last_external_id=last_external_id or None,
            transport_mode="telegram_public_fallback",
            metadata={"fallback_used": "public", "urls": urls},
        )
        conn.commit()
    finally:
        conn.close()

    return {
        "ok": not any(w.startswith(tuple(_channel_handle(url) for url in urls)) for w in warnings),
        "items_seen": items_seen,
        "items_new": items_new,
        "items_updated": items_updated,
        "warnings": warnings[:20],
        "artifacts": {"urls": urls, "transport": "telegram_public_fallback"},
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    print(json.dumps(collect_public_fallback(), ensure_ascii=False, indent=2))
