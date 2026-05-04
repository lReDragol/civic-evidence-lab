import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Mapping, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in os.sys.path:
    os.sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings, ensure_dirs
from db.file_store import materialize_attachment
from classifier.negative_filter import classify_negative_profile
from classifier.tagger_v2 import infer_tags_v2
from runtime.state import record_dead_letter, update_source_sync_state

log = logging.getLogger(__name__)

Client = None
HAVE_PYROGRAM = None


def _ensure_thread_event_loop() -> None:
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())


def _load_pyrogram_client():
    global Client, HAVE_PYROGRAM
    if Client is not None:
        return Client
    if HAVE_PYROGRAM is False:
        return None
    try:
        _ensure_thread_event_loop()
        from pyrogram import Client as PyrogramClient
    except ImportError:
        HAVE_PYROGRAM = False
        log.error("pyrogram not installed")
        return None
    Client = PyrogramClient
    HAVE_PYROGRAM = True
    return Client


RUSSIA_CONTEXT_PATTERNS = [
    r"\bросси[яйиюе]\b",
    r"\bроссийск",
    r"(?<![а-яёa-z])рф(?![а-яёa-z])",
    r"\bгосдум",
    r"\bгосударственн[а-яё]+\s+дум",
    r"\bправительств[а-яё]*\s+рф\b",
    r"\bкремл",
    r"\bминцифр",
    r"\bминфин",
    r"\bминюст",
    r"\bроскомнадзор",
    r"(?<![а-яёa-z])ркн(?![а-яёa-z])",
    r"\bфсб\b",
    r"\bмвд\b",
    r"\bпрокуратур",
    r"\bдепутат",
    r"\bшадаев",
    r"\bлантратов",
    r"\bпарфенов",
    r"\bпарфёнов",
    r"\bтасс\b",
    r"\b\.ru\b|\b\.рф\b|\b\.su\b",
]

POLICY_NEGATIVE_PATTERNS = [
    (r"\bштраф", 3.0, "restriction/fines"),
    (r"\bзапрет|\bогранич|\bблокиров|\bзаблокир|\bзамедл|\bроскомнадзор|(?<![а-яёa-z])ркн(?![а-яёa-z])", 3.5, "restriction/censorship"),
    (r"\bvpn\b|\bвпн\b|\bтрафик|\bинтернет|\bмессенджер", 2.5, "restriction/internet"),
    (r"\bперсональн[а-яё]+\s+данн|\bутечк[а-яё]+\s+персональн", 2.5, "restriction/privacy"),
    (r"\bgoogle analytics\b|\bтрансграничн[а-яё]+\s+передач", 2.0, "restriction/privacy"),
    (r"\bналог|\bплат[ауые]\b|\bсбор\b|\bтариф", 2.0, "economic/harm"),
    (r"\bтрадиционн[а-яё]+\s+ценност|\bпрокатн[а-яё]+\s+удостоверен", 2.5, "restriction/culture_control"),
]

DOCUMENT_SIGNAL_PATTERNS = [
    r"\bдокумент",
    r"\bскриншот",
    r"\bписьм[ооае]",
    r"\bтребовани[ея]",
    r"\bуведомлени[ея]",
    r"\bпредписани[ея]",
    r"\bпостановлени[ея]",
    r"\bприказ",
    r"\bдепутатск[а-яё]+\s+запрос",
    r"\bофициальн[а-яё]+\s+запрос",
    r"(?<![а-яёa-z])№\s*\d+",
    r"\b\d{1,2}\.\d{1,2}\.\d{4}\b",
    r"\bфз\b|\b\d+\s*-\s*фз\b",
]

LOW_VALUE_TELEGRAM_PATTERNS = [
    (r"^\s*(доброе утро|добрый вечер|спокойной ночи)[!.…\s]*$", "greeting_only"),
    (r"\bрозыгрыш\b|\bконкурс\b|\bподарок\b|\bгороскоп\b|\bмем\b", "entertainment_or_giveaway"),
    (r"\bреклам|\bпромокод|\bскидк|\bкупить\b|\bзаказать\b", "promo_or_ad"),
]


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("ё", "е").lower()).strip()


def _source_value(source: Optional[Mapping], key: str) -> str:
    if not source:
        return ""
    try:
        return str(source[key] or "")
    except Exception:
        getter = getattr(source, "get", None)
        return str(getter(key, "") if getter else "")


def _pattern_matches(text: str, patterns: list[str]) -> list[str]:
    return [pattern for pattern in patterns if re.search(pattern, text, flags=re.IGNORECASE | re.UNICODE)]


def _flatten_tags(tag_map) -> list[str]:
    return [tag for tags in (tag_map or {}).values() for tag, _score in tags]


def classify_message_relevance(
    text: str,
    *,
    has_media: bool,
    source: Optional[Mapping] = None,
    store_mode: str = "all",
) -> dict:
    """Precision-first gate for Telegram ingestion.

    The gate is deliberately conservative: global tech/politics posts are dropped
    unless they contain Russian actors, Russian institutions, or Russian legal context.
    """
    normalized = _normalize_text(text)
    source_context = _normalize_text(
        " ".join(
            [
                _source_value(source, "name"),
                _source_value(source, "url"),
                _source_value(source, "subcategory"),
                _source_value(source, "political_alignment"),
                _source_value(source, "notes"),
            ]
        )
    )
    tag_map = infer_tags_v2(text or "")
    tag_names = _flatten_tags(tag_map)
    negative_profile = classify_negative_profile(text or "", source=source, tag_names=tag_names)

    russia_matches = _pattern_matches(f"{normalized} {source_context}", RUSSIA_CONTEXT_PATTERNS)
    low_value_reasons = [reason for pattern, reason in LOW_VALUE_TELEGRAM_PATTERNS if re.search(pattern, normalized, flags=re.IGNORECASE | re.UNICODE)]

    policy_score = 0.0
    navigation_tags: list[str] = []
    for pattern, score, tag in POLICY_NEGATIVE_PATTERNS:
        if re.search(pattern, normalized, flags=re.IGNORECASE | re.UNICODE):
            policy_score += score
            navigation_tags.append(tag)

    document_matches = _pattern_matches(normalized, DOCUMENT_SIGNAL_PATTERNS)
    is_document_like = bool(has_media and (document_matches or policy_score >= 4.0))

    text_len = len(normalized)
    reasons: list[str] = list(dict.fromkeys(low_value_reasons))
    is_russia_related = bool(russia_matches)
    if not is_russia_related:
        reasons.append("not_russia_related")
    if text_len < 35 and not has_media:
        reasons.append("too_short_without_media")

    is_negative = bool(negative_profile.get("is_negative_public_interest"))
    is_policy_adverse = policy_score >= 4.0
    is_document_adverse = is_document_like and (is_policy_adverse or is_negative or bool(document_matches and policy_score >= 2.5))

    mode = (store_mode or "all").strip().lower()
    if mode == "all":
        keep = True
    elif mode == "filtered":
        keep = not low_value_reasons and is_russia_related
    elif mode == "news_only":
        keep = not low_value_reasons and is_russia_related and (is_negative or is_policy_adverse or is_document_like)
    elif mode == "negative_only":
        keep = not low_value_reasons and is_russia_related and (is_negative or is_policy_adverse or is_document_adverse)
        if not (is_negative or is_policy_adverse or is_document_adverse):
            reasons.append("not_negative_public_interest")
    else:
        keep = True

    return {
        "keep": bool(keep),
        "mode": mode,
        "reasons": sorted(set(reasons)),
        "is_russia_related": is_russia_related,
        "russia_reasons": russia_matches[:12],
        "is_negative_public_interest": is_negative,
        "negative_profile": negative_profile,
        "policy_score": round(policy_score, 2),
        "navigation_tags": sorted(set(navigation_tags + list(negative_profile.get("risk_tags") or []))),
        "is_document_like": is_document_like,
        "document_reasons": document_matches[:12],
        "tag_names": tag_names,
        "quality": {
            "text_length": text_len,
            "has_media": bool(has_media),
            "low_value_reasons": low_value_reasons,
        },
    }


def _insert_relevance_votes(conn: sqlite3.Connection, content_id: int, relevance: dict) -> None:
    votes: list[tuple[str, str, str, float, str, str | None]] = []
    for tag in relevance.get("navigation_tags") or []:
        votes.append((str(tag), "support", "telegram_relevance", 0.85, "telegram_relevance", None))
    if relevance.get("is_document_like"):
        votes.append(("document/screenshot", "support", "telegram_relevance", 0.95, "telegram_document_gate", None))
        votes.append(("document/authenticity_review", "support", "telegram_relevance", 0.9, "telegram_document_gate", None))
    if not relevance.get("keep"):
        for reason in relevance.get("reasons") or ["filtered"]:
            votes.append((f"filter/{reason}", "reject", "telegram_relevance", 0.9, "telegram_relevance", str(reason)))

    for tag_name, vote_value, signal_layer, confidence, voter_name, abstain_reason in votes:
        namespace = tag_name.split("/", 1)[0] if "/" in tag_name else None
        conn.execute(
            """
            INSERT INTO content_tag_votes(
                content_item_id, voter_name, tag_name, namespace, normalized_tag, vote_value,
                signal_layer, abstain_reason, confidence_raw, evidence_text, metadata_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                content_id,
                voter_name,
                tag_name,
                namespace,
                tag_name,
                vote_value,
                signal_layer,
                abstain_reason,
                confidence,
                "; ".join(relevance.get("document_reasons") or relevance.get("russia_reasons") or [])[:500],
                json.dumps({"relevance": relevance}, ensure_ascii=False),
            ),
        )


def _enqueue_document_review(
    conn: sqlite3.Connection,
    *,
    content_id: int,
    source_id: int,
    external_id: str,
    public_url: str,
    relevance: dict,
) -> None:
    if not relevance.get("is_document_like"):
        return
    task_key = f"telegram-document:{source_id}:{external_id}"
    conn.execute(
        """
        INSERT INTO review_tasks(
            task_key, queue_key, subject_type, subject_id, candidate_payload,
            suggested_action, confidence, machine_reason, source_links_json, status
        ) VALUES(?,?,?,?,?,?,?,?,?, 'open')
        ON CONFLICT(task_key) DO UPDATE SET
            candidate_payload=excluded.candidate_payload,
            confidence=MAX(review_tasks.confidence, excluded.confidence),
            machine_reason=excluded.machine_reason,
            source_links_json=excluded.source_links_json,
            updated_at=datetime('now')
        """,
        (
            task_key,
            "documents",
            "content_item",
            content_id,
            json.dumps(
                {
                    "source_id": source_id,
                    "external_id": external_id,
                    "document_reasons": relevance.get("document_reasons") or [],
                    "navigation_tags": relevance.get("navigation_tags") or [],
                    "policy_score": relevance.get("policy_score"),
                },
                ensure_ascii=False,
            ),
            "verify_document_screenshot",
            0.9,
            "Telegram post has media and official-document signals; OCR/search/authenticity verification required.",
            json.dumps([public_url], ensure_ascii=False),
        ),
    )


def _get_source_id(conn: sqlite3.Connection, url: str) -> Optional[int]:
    handle = url.replace("t.me/", "@").strip()
    row = conn.execute(
        "SELECT id FROM sources WHERE (url=? OR url=?) AND category='telegram' AND is_active=1",
        (url, handle),
    ).fetchone()
    return row[0] if row else None


def _telegram_source_key(source_id: int) -> str:
    return f"telegram:{int(source_id)}"


def _telegram_session_candidates(session_dir: Path, session_path: Path) -> list[Path]:
    candidates = [
        session_path.with_suffix(".session"),
        session_dir / f"{session_path.name}.session",
    ]
    return list(dict.fromkeys(candidates))


def _existing_telegram_session_file(session_dir: Path, session_path: Path) -> Path | None:
    for candidate in _telegram_session_candidates(session_dir, session_path):
        try:
            if candidate.exists() and candidate.stat().st_size > 0:
                return candidate
        except OSError:
            continue
    return None


def _has_existing_telegram_session(session_dir: Path, session_path: Path) -> bool:
    return _existing_telegram_session_file(session_dir, session_path) is not None


def _is_authorized_telegram_session(session_file: Path) -> bool:
    try:
        conn = sqlite3.connect(session_file)
        try:
            row = conn.execute(
                "SELECT user_id, is_bot FROM sessions WHERE auth_key IS NOT NULL LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
    except Exception:
        return False
    if not row:
        return False
    return row[0] is not None or row[1] is not None


def _record_telegram_runtime_failure(
    settings: dict,
    *,
    fatal_error: str,
    failure_class: str,
    last_error: str | None = None,
) -> dict:
    conn = get_db(settings)
    try:
        update_source_sync_state(
            conn,
            source_key="telegram",
            success=False,
            transport_mode="telegram",
            failure_class=failure_class,
            last_error=last_error or fatal_error,
            metadata={"fatal_error": fatal_error},
        )
    finally:
        conn.close()
    return {
        "ok": False,
        "items_seen": 0,
        "items_new": 0,
        "items_updated": 0,
        "fatal_errors": [fatal_error],
        "warnings": [],
    }


def _telethon_pool_is_active(settings: dict) -> bool:
    conn = get_db(settings)
    try:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM telegram_sessions
            WHERE client_type='telethon'
              AND status='active'
            """
        ).fetchone()
        return bool(row and int(row[0] or 0) > 0)
    except Exception:
        return False
    finally:
        conn.close()


def _skip_legacy_runtime_failure(settings: dict, warning: str) -> dict:
    log.warning("%s", warning)
    conn = get_db(settings)
    try:
        update_source_sync_state(
            conn,
            source_key="telegram",
            success=True,
            state="warning",
            transport_mode="pyrogram",
            quality_state="warning",
            quality_issue=warning,
            last_error=warning,
            failure_class=None,
            metadata={"skipped": True, "reason": warning},
        )
    finally:
        conn.close()
    return {
        "ok": True,
        "items_seen": 0,
        "items_new": 0,
        "items_updated": 0,
        "warnings": [warning],
        "artifacts": {"skipped": True, "reason": warning},
    }


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
    source_row = conn.execute(
        """
        SELECT id, name, url, subcategory, is_official, credibility_tier, owner,
               bias_notes, political_alignment, notes
        FROM sources
        WHERE id=?
        """,
        (source_id,),
    ).fetchone()
    store_mode = str(settings.get("telegram_store_mode", "all") or "all")

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
        public_url = f"https://t.me/{handle.lstrip('@')}/{msg.id}"
        has_media = bool(msg.media or msg.photo or msg.video or msg.document)
        relevance = classify_message_relevance(
            text,
            has_media=has_media,
            source=source_row,
            store_mode=store_mode,
        )

        payload = {
            "message_id": msg.id,
            "date": date_iso,
            "text": text,
            "views": msg.views,
            "forwards": msg.forwards,
            "post_author": msg.post_author,
            "grouped_id": msg.grouped_id,
            "reply_to_message_id": msg.reply_to_message_id if msg.reply_to_message else None,
            "has_media": has_media,
            "source_title": source_name,
            "channel_handle": handle,
            "relevance": relevance,
        }
        raw_json = json.dumps(payload, ensure_ascii=False, default=str)
        hash_sha = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()

        if not relevance.get("keep", True):
            continue

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
            (source_id, raw_id, ext_id, "post", title, text, date_iso, datetime.now().isoformat(), public_url),
        )
        content_id = cur2.lastrowid
        _insert_relevance_votes(conn, content_id, relevance)
        _enqueue_document_review(
            conn,
            content_id=content_id,
            source_id=source_id,
            external_id=ext_id,
            public_url=public_url,
            relevance=relevance,
        )

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

    pyrogram_client = _load_pyrogram_client()
    if not pyrogram_client:
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

    if settings.get("telegram_require_existing_session", True):
        session_file = _existing_telegram_session_file(session_dir, session_path)
        if not session_file:
            log.error("Telegram session is missing; refusing to start interactive Pyrogram authorization")
            if _telethon_pool_is_active(settings):
                return _skip_legacy_runtime_failure(
                    settings,
                    "telegram_legacy_skipped:telethon_pool_active:session_missing",
                )
            return _record_telegram_runtime_failure(
                settings,
                fatal_error="telegram_session_missing",
                failure_class="auth",
                last_error="telegram_session_missing: create an authorized Pyrogram session or use telegram_public_fallback",
            )
        if not _is_authorized_telegram_session(session_file):
            log.error("Telegram session is present but unauthorized; refusing interactive Pyrogram authorization")
            if _telethon_pool_is_active(settings):
                return _skip_legacy_runtime_failure(
                    settings,
                    "telegram_legacy_skipped:telethon_pool_active:session_unauthorized",
                )
            return _record_telegram_runtime_failure(
                settings,
                fatal_error="telegram_session_unauthorized",
                failure_class="auth",
                last_error="telegram_session_unauthorized: session exists but has no user_id/bot marker",
            )

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

    app = pyrogram_client(
        str(session_path),
        api_id=int(api_id),
        api_hash=str(api_hash),
        workdir=str(session_dir),
    )

    try:
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
    except EOFError:
        return _record_telegram_runtime_failure(
            settings,
            fatal_error="telegram_session_missing",
            failure_class="auth",
            last_error="EOFError: Pyrogram requested interactive authorization in non-interactive runtime",
        )
    except Exception as error:
        if "The api_id/api_hash combination is invalid" in str(error):
            return _record_telegram_runtime_failure(
                settings,
                fatal_error="telegram_auth_invalid",
                failure_class="auth",
                last_error=f"{type(error).__name__}: {error}",
            )
        raise


def main():
    asyncio.run(run_collect())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main()
