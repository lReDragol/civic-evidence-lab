import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from telethon import TelegramClient, errors, functions, types

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from classifier.negative_filter import classify_negative_profile
from classifier.tagger_v2 import infer_tags_v2

DEFAULT_API_CONFIG = Path(r"D:\Python Program\ai_asistant\userdata\config.json")
DEFAULT_SESSION_SOURCE = Path(r"D:\Python Program\ai_asistant\userdata\telegram\211729602_telethon.session")
DEFAULT_SESSION_DIR = PROJECT_ROOT / "config" / "telegram_test_sessions"
DEFAULT_DB = PROJECT_ROOT / "db" / "news_telegram_test.db"
DEFAULT_MEDIA_DIR = PROJECT_ROOT / "processed" / "telegram_test"
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"
SEED_PATH = PROJECT_ROOT / "config" / "sources_seed.json"

log = logging.getLogger("telegram_test_collect")

AD_PATTERNS = [
    (r"\bреклам", 3.0),
    (r"\bпромокод", 3.0),
    (r"\bскидк", 2.5),
    (r"\bкупить\b|\bзаказать\b", 2.5),
    (r"\bподписывайт", 1.5),
    (r"\bнаш\s+канал\b", 1.5),
    (r"\bдонат\b|\bподдержать\s+канал\b", 2.0),
    (r"\bкурс\b|\bмарафон\b|\bвебинар\b", 1.5),
]

LOW_VALUE_PATTERNS = [
    (r"^\s*(доброе утро|добрый вечер|спокойной ночи)[!.…\s]*$", "greeting_only"),
    (r"^\s*(подписаться|комментировать|переслать|читать далее)\s*$", "button_or_link_label"),
    (r"\bрозыгрыш\b|\bконкурс\b|\bподарок\b|\bпобедител[ья]\b", "giveaway"),
    (r"\bгороскоп\b|\bанекдот\b|\bмем\b|\bкартинка дня\b", "entertainment"),
]

NEWS_PATTERNS = [
    (r"\bсообщил[аи]?\b|\bзаявил[аи]?\b|\bрассказал[аи]?\b", 2.0),
    (r"\bсуд\b|\bприговор\b|\bиск\b|\bарест\b|\bзадерж", 3.0),
    (r"\bзакон\b|\bзаконопроект\b|\bгосдум|\bсовфед\b", 3.0),
    (r"\bправительств|\bминистерств|\bмэр\b|\bгубернатор\b", 2.5),
    (r"\bминюст\b|\bроскомнадзор\b|\bпрокуратур|\bфсб\b|\bмвд\b", 3.0),
    (r"\bзакупк|\bконтракт|\bтендер|\bбанкрот|\bегрюл|\bинн\b", 3.0),
    (r"\bвыбор|\bголосован|\bцик\b", 2.5),
    (r"\bпожар\b|\bвзрыв\b|\bавари|\bпогиб|\bпострадал", 2.5),
    (r"\bобстрел|\bбпла\b|\bдрон\b|\bпво\b|\bракет", 2.5),
    (r"\bопубликован|\bвступил[ао]? в силу|\bпринял[аи]?\b", 2.0),
]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_name(value: str) -> str:
    cleaned = re.sub(r"[^\w.-]+", "_", value or "", flags=re.UNICODE).strip("._ ")
    return cleaned[:80] or "telegram"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _score_patterns(text: str, patterns: Iterable[Tuple[str, float]]) -> Tuple[float, List[str]]:
    score = 0.0
    reasons: List[str] = []
    for pattern, points in patterns:
        if re.search(pattern, text, flags=re.IGNORECASE):
            score += points
            reasons.append(pattern)
    return score, reasons


def classify_quality(text: str, has_media: bool) -> Dict[str, object]:
    normalized = re.sub(r"\s+", " ", (text or "").lower()).strip()
    ad_score, ad_reasons = _score_patterns(normalized, AD_PATTERNS)
    news_score, news_reasons = _score_patterns(normalized, NEWS_PATTERNS)
    url_count = len(re.findall(r"https?://|t\.me/|@\w+", normalized))

    if url_count >= 4 and news_score < 3:
        ad_score += 1.5
        ad_reasons.append("many_links")

    length = len(normalized)
    if length < 40 and not has_media:
        label = "low_signal"
    elif ad_score >= 3.0 and news_score < 4.0:
        label = "probable_ad_or_promo"
    elif news_score >= 4.0:
        label = "probable_news"
    elif news_score >= 2.0 or has_media:
        label = "uncertain_signal"
    else:
        label = "low_signal"

    return {
        "quality_label": label,
        "news_score": round(news_score, 2),
        "ad_score": round(ad_score, 2),
        "url_count": url_count,
        "text_length": length,
        "news_reasons": news_reasons[:8],
        "ad_reasons": ad_reasons[:8],
    }


def _flatten_tags(tag_map: Dict[int, List[Tuple[str, float]]]) -> List[str]:
    return [tag for tags in tag_map.values() for tag, _score in tags]


def collection_filter_decision(
    text: str,
    has_media: bool,
    quality: Dict[str, object],
    tag_map: Dict[int, List[Tuple[str, float]]],
    store_mode: str,
    source: Optional[sqlite3.Row] = None,
) -> Dict[str, object]:
    normalized = re.sub(r"\s+", " ", (text or "").lower()).strip()
    label = str(quality.get("quality_label") or "")
    news_score = float(quality.get("news_score") or 0.0)
    ad_score = float(quality.get("ad_score") or 0.0)
    text_length = int(quality.get("text_length") or 0)
    url_count = int(quality.get("url_count") or 0)
    level1_tags = [tag for tag, _score in tag_map.get(1, [])]
    tag_names = _flatten_tags(tag_map)
    negative_profile = classify_negative_profile(text, source=source, tag_names=tag_names)

    reasons: List[str] = []
    for pattern, reason in LOW_VALUE_PATTERNS:
        if re.search(pattern, normalized, flags=re.IGNORECASE):
            reasons.append(reason)

    if not normalized and not has_media:
        reasons.append("empty_message")
    if label == "probable_ad_or_promo":
        reasons.append("probable_ad_or_promo")
    if label == "low_signal" and not level1_tags:
        reasons.append("low_signal_without_event_tags")
    if text_length < 60 and not level1_tags and not has_media:
        reasons.append("too_short_without_context")
    if url_count >= 4 and text_length < 600 and news_score < 4.0:
        reasons.append("link_dump_without_news_context")
    if ad_score >= 4.0 and news_score < 5.0:
        reasons.append("ad_score_dominates_news_score")
    if negative_profile["party_self_promo_without_negative"]:
        reasons.append("party_self_promo_without_negative_signal")

    if store_mode == "all":
        keep = True
    elif store_mode == "filtered":
        keep = not reasons
    elif store_mode == "news_only":
        keep = not reasons and (label == "probable_news" or (level1_tags and news_score >= 2.0))
        if not keep and not reasons:
            reasons.append("not_enough_news_signals")
    elif store_mode == "negative_only":
        keep = not reasons and bool(negative_profile["is_negative_public_interest"])
        if not negative_profile["is_negative_public_interest"]:
            reasons.append("not_negative_public_interest")
        if negative_profile["source_context"]["is_strict_context"] and not keep:
            reasons.append("strict_party_or_deputy_threshold_not_met")
    else:
        keep = True

    return {
        "keep": keep,
        "reasons": reasons,
        "mode": store_mode,
        "level1_tags": level1_tags,
        "tag_names": tag_names,
        "negative_profile": negative_profile,
        "risk_tags": negative_profile["risk_tags"],
    }


def load_api_credentials(api_config: Path) -> Tuple[int, str]:
    env_api_id = os.getenv("DRAGO_TG_API_ID") or os.getenv("TELEGRAM_API_ID")
    env_api_hash = os.getenv("DRAGO_TG_API_HASH") or os.getenv("TELEGRAM_API_HASH")
    if env_api_id and env_api_hash:
        return int(env_api_id), str(env_api_hash)

    if api_config.exists():
        cfg = json.loads(api_config.read_text(encoding="utf-8"))
        api_id = cfg.get("telegram_api_id")
        api_hash = cfg.get("telegram_api_hash")
        if api_id and api_hash:
            return int(api_id), str(api_hash)

    raise RuntimeError("Telegram API credentials not found in env or api config")


def prepare_session(source: Path, session_name: str, refresh: bool) -> Path:
    if not source.exists():
        raise FileNotFoundError(f"Session source not found: {source}")

    DEFAULT_SESSION_DIR.mkdir(parents=True, exist_ok=True)
    target = DEFAULT_SESSION_DIR / f"{session_name}.session"
    if refresh or not target.exists():
        shutil.copy2(source, target)
    return target


def _inside_project(path: Path) -> bool:
    try:
        path.resolve().relative_to(PROJECT_ROOT.resolve())
        return True
    except ValueError:
        return False


def reset_db_if_requested(db_path: Path, reset: bool) -> None:
    if not reset:
        return
    if not _inside_project(db_path):
        raise RuntimeError(f"Refusing to reset DB outside project: {db_path}")
    for candidate in [db_path, db_path.with_suffix(db_path.suffix + "-wal"), db_path.with_suffix(db_path.suffix + "-shm")]:
        if candidate.exists():
            candidate.unlink()


def connect_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_test_db(conn: sqlite3.Connection) -> int:
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    sources = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    inserted = 0
    for src in sources:
        if src.get("category") != "telegram":
            continue
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO sources(
                name, category, subcategory, url, access_method,
                is_official, credibility_tier, region, country,
                owner, bias_notes, political_alignment, is_active,
                update_frequency, notes
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                src.get("name", ""),
                src.get("category", ""),
                src.get("subcategory"),
                src.get("url"),
                src.get("access_method") or "telegram",
                int(src.get("is_official", 0)),
                src.get("credibility_tier", "C"),
                src.get("region"),
                src.get("country", "RU"),
                src.get("owner"),
                src.get("bias_notes"),
                src.get("political_alignment"),
                int(src.get("is_active", 1)),
                src.get("update_frequency"),
                src.get("notes"),
            ),
        )
        inserted += cur.rowcount
    conn.commit()
    return inserted


def channel_handle(url: str) -> str:
    value = (url or "").strip()
    value = re.sub(r"^https?://", "", value)
    value = re.sub(r"^t\.me/", "", value)
    value = value.strip("/ ")
    if not value:
        raise ValueError("empty Telegram source url")
    return value if value.startswith("@") else f"@{value}"


def public_message_url(source_url: str, msg_id: int) -> str:
    handle = channel_handle(source_url).lstrip("@")
    return f"https://t.me/{handle}/{msg_id}"


def load_sources(conn: sqlite3.Connection, source_limit: int, only: Optional[List[str]]) -> List[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT id, name, url, subcategory, is_official, credibility_tier, owner, bias_notes, political_alignment, notes
        FROM sources
        WHERE category='telegram'
          AND is_active=1
          AND access_method IN ('telegram_tdlib', 'pyrogram', 'telegram')
        ORDER BY id
        """
    ).fetchall()
    if only:
        wanted = {x.lower().lstrip("@") for x in only}
        rows = [r for r in rows if channel_handle(r["url"]).lower().lstrip("@") in wanted]
    if source_limit > 0:
        rows = rows[:source_limit]
    return rows


def run_verification(
    db_path: Path,
    content_limit: int,
    verification_limit: int,
    external_checks: bool,
    external_evidence_db: Optional[Path] = None,
) -> Dict[str, object]:
    from verification.engine import process_claims_for_content
    from verification.evidence_linker import auto_link_by_content_type, auto_link_evidence
    from verification.external_corpus import verify_claims_against_external_corpus

    settings = {"db_path": str(db_path)}
    claims_result = process_claims_for_content(
        settings=settings,
        content_limit=content_limit,
        verification_limit=verification_limit,
        external_checks=external_checks,
    )
    evidence_result = auto_link_evidence(settings=settings, batch_size=verification_limit)
    official_result = auto_link_by_content_type(settings=settings)
    external_result = None
    if external_evidence_db:
        external_result = verify_claims_against_external_corpus(
            target_db=db_path,
            evidence_db=external_evidence_db,
            claim_limit=verification_limit,
        )
    return {
        "claims": claims_result,
        "entity_evidence": evidence_result,
        "official_evidence": official_result,
        "external_corpus": external_result,
    }


def insert_tags(
    conn: sqlite3.Connection,
    content_id: int,
    tag_map: Dict[int, List[Tuple[str, float]]],
    quality: Dict[str, object],
    filter_info: Dict[str, object],
) -> None:
    for level, tags in tag_map.items():
        for tag, score in tags:
            conn.execute(
                """
                INSERT OR IGNORE INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source)
                VALUES(?,?,?,?,?)
                """,
                (content_id, level, tag, float(score), "rule_v2"),
            )

    label = str(quality.get("quality_label") or "uncertain_signal")
    conn.execute(
        """
        INSERT OR IGNORE INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source)
        VALUES(?,?,?,?,?)
        """,
        (content_id, 3, f"quality:{label}", 1.0, "telegram_test_quality"),
    )
    if label == "probable_ad_or_promo":
        conn.execute(
            """
            INSERT OR IGNORE INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source)
            VALUES(?,?,?,?,?)
            """,
            (content_id, 3, "promo_risk", float(quality.get("ad_score") or 1.0), "telegram_test_quality"),
        )
    negative_profile = filter_info.get("negative_profile") or {}
    for tag_name in filter_info.get("risk_tags") or []:
        conn.execute(
            """
            INSERT OR IGNORE INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source)
            VALUES(?,?,?,?,?)
            """,
            (
                content_id,
                3,
                str(tag_name),
                min(float(negative_profile.get("negative_score") or 1.0) / 10.0, 1.0),
                "negative_filter",
            ),
        )
    for reason in filter_info.get("reasons") or []:
        conn.execute(
            """
            INSERT OR IGNORE INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source)
            VALUES(?,?,?,?,?)
            """,
            (content_id, 3, f"filter:{reason}", 1.0, "telegram_test_filter"),
        )


async def download_message_media(
    client: TelegramClient,
    conn: sqlite3.Connection,
    raw_id: int,
    content_id: int,
    msg,
    media_dir: Path,
    source_name: str,
) -> Optional[str]:
    if not msg.media:
        return None
    month = msg.date.strftime("%Y-%m") if msg.date else "unknown"
    target_dir = media_dir / month / _safe_name(source_name)
    target_dir.mkdir(parents=True, exist_ok=True)
    downloaded = await client.download_media(msg, file=str(target_dir))
    if not downloaded:
        return None

    path = Path(downloaded)
    if not path.exists() or not path.is_file():
        return None

    sha = _sha256_file(path)
    size = path.stat().st_size
    suffix = path.suffix.lower().lstrip(".")
    mime = {
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "webp": "image/webp",
        "mp4": "video/mp4",
        "pdf": "application/pdf",
    }.get(suffix, "")
    attachment_type = "photo" if mime.startswith("image/") else "video" if mime.startswith("video/") else "document"

    cur = conn.execute(
        """
        INSERT OR IGNORE INTO raw_blobs(
            raw_item_id, blob_type, file_path, original_filename, storage_rel_path,
            original_url, mime_type, file_size, hash_sha256, metadata_json, missing_on_disk
        ) VALUES(?,?,?,?,?,?,?,?,?,?,0)
        """,
        (
            raw_id,
            attachment_type,
            str(path),
            path.name,
            str(path.relative_to(PROJECT_ROOT)) if _inside_project(path) else str(path),
            None,
            mime,
            size,
            sha,
            json.dumps({"telegram_message_id": msg.id}, ensure_ascii=False),
        ),
    )
    blob_id = cur.lastrowid if cur.rowcount else None
    if blob_id is None:
        row = conn.execute(
            "SELECT id FROM raw_blobs WHERE raw_item_id=? AND original_url IS NULL",
            (raw_id,),
        ).fetchone()
        blob_id = row["id"] if row else None

    conn.execute(
        """
        INSERT OR IGNORE INTO attachments(
            content_item_id, blob_id, file_path, attachment_type, hash_sha256, file_size, mime_type, is_original
        ) VALUES(?,?,?,?,?,?,?,1)
        """,
        (content_id, blob_id, str(path), attachment_type, sha, size, mime),
    )
    return str(path)


async def ensure_joined_channel(client: TelegramClient, entity, wait_limit: int) -> Tuple[str, Optional[str]]:
    if not isinstance(entity, types.Channel):
        return "not_channel", None
    try:
        await client(functions.channels.JoinChannelRequest(entity))
        return "joined", None
    except errors.UserAlreadyParticipantError:
        return "already_joined", None
    except errors.FloodWaitError as exc:
        if exc.seconds <= wait_limit:
            log.info("Join flood wait: sleeping for %ss", exc.seconds)
            await asyncio.sleep(exc.seconds)
            try:
                await client(functions.channels.JoinChannelRequest(entity))
                return "joined_after_wait", None
            except errors.UserAlreadyParticipantError:
                return "already_joined", None
            except Exception as retry_exc:
                return "failed", f"{type(retry_exc).__name__}: {retry_exc}"
        return "failed", f"FloodWaitError: {exc.seconds}s"
    except Exception as exc:
        return "failed", f"{type(exc).__name__}: {exc}"


async def collect_channel(
    client: TelegramClient,
    conn: sqlite3.Connection,
    source: sqlite3.Row,
    per_channel: int,
    store_mode: str,
    download_media: bool,
    media_dir: Path,
    join_channels: bool,
    join_wait_limit: int,
) -> Dict[str, object]:
    handle = channel_handle(source["url"])
    result = {
        "source": source["name"],
        "handle": handle,
        "seen": 0,
        "inserted": 0,
        "skipped": 0,
        "filtered": 0,
        "filter_reasons": {},
        "join_status": "not_requested",
        "errors": [],
    }

    try:
        entity = await client.get_entity(handle)
    except Exception as exc:
        result["errors"].append(f"resolve: {type(exc).__name__}: {exc}")
        return result

    if join_channels:
        status, error = await ensure_joined_channel(client, entity, join_wait_limit)
        result["join_status"] = status
        if error:
            result["errors"].append(f"join: {error}")

    async for msg in client.iter_messages(entity, limit=per_channel):
        result["seen"] += 1
        if not getattr(msg, "id", None):
            result["skipped"] += 1
            continue

        ext_id = str(msg.id)
        exists = conn.execute(
            "SELECT id FROM raw_source_items WHERE source_id=? AND external_id=?",
            (source["id"], ext_id),
        ).fetchone()
        if exists:
            result["skipped"] += 1
            continue

        text = msg.message or ""
        quality = classify_quality(text, has_media=bool(msg.media))
        tag_map = infer_tags_v2(text)
        filter_info = collection_filter_decision(text, bool(msg.media), quality, tag_map, store_mode, source=source)
        if not filter_info["keep"]:
            result["skipped"] += 1
            result["filtered"] += 1
            counter = Counter(result["filter_reasons"])
            reasons = filter_info.get("reasons") or ["filtered"]
            for reason in reasons:
                counter[str(reason)] += 1
            result["filter_reasons"] = dict(counter)
            continue

        payload = {
            "message_id": msg.id,
            "date": msg.date.isoformat() if msg.date else None,
            "text": text,
            "views": getattr(msg, "views", None),
            "forwards": getattr(msg, "forwards", None),
            "post_author": getattr(msg, "post_author", None),
            "grouped_id": str(getattr(msg, "grouped_id", "") or ""),
            "reply_to_message_id": getattr(getattr(msg, "reply_to", None), "reply_to_msg_id", None),
            "has_media": bool(msg.media),
            "source_title": getattr(entity, "title", None) or source["name"],
            "channel_handle": handle,
            "quality": quality,
            "filter": filter_info,
        }
        raw_json = json.dumps(payload, ensure_ascii=False, default=str)
        raw_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()

        cur = conn.execute(
            """
            INSERT INTO raw_source_items(source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed)
            VALUES(?,?,?,?,?,0)
            """,
            (source["id"], ext_id, raw_json, _now(), raw_hash),
        )
        raw_id = cur.lastrowid

        title = (text.splitlines()[0].strip() if text.strip() else f"Telegram message {msg.id}")[:200]
        cur = conn.execute(
            """
            INSERT INTO content_items(
                source_id, raw_item_id, external_id, content_type, title, body_text,
                published_at, collected_at, url, status
            ) VALUES(?,?,?,?,?,?,?,?,?,'raw_signal')
            """,
            (
                source["id"],
                raw_id,
                ext_id,
                "post",
                title,
                text,
                msg.date.isoformat() if msg.date else None,
                _now(),
                public_message_url(source["url"], msg.id),
            ),
        )
        content_id = cur.lastrowid
        insert_tags(conn, content_id, tag_map, quality, filter_info)

        if download_media and msg.media:
            try:
                await download_message_media(client, conn, raw_id, content_id, msg, media_dir, source["name"])
            except Exception as exc:
                result["errors"].append(f"download:{msg.id}: {type(exc).__name__}: {exc}")

        result["inserted"] += 1

    conn.execute("UPDATE sources SET last_checked_at=? WHERE id=?", (_now(), source["id"]))
    conn.commit()
    return result


async def run(args: argparse.Namespace) -> Dict[str, object]:
    db_path = Path(args.db).resolve()
    reset_db_if_requested(db_path, args.reset)

    api_id, api_hash = load_api_credentials(Path(args.api_config))
    session_file = prepare_session(Path(args.session_source), args.session_name, args.refresh_session)

    conn = connect_db(db_path)
    try:
        seeded = init_test_db(conn)
        sources = load_sources(conn, args.source_limit, args.only)
        if not sources:
            raise RuntimeError("No Telegram sources found in test DB")

        client = TelegramClient(str(session_file.with_suffix("")), api_id, api_hash)
        await client.connect()
        try:
            if not await client.is_user_authorized():
                raise RuntimeError("Telegram test session is not authorized")
            me = await client.get_me()
            username = (me.username or "").lower()
            if args.expected_username and username != args.expected_username.lower().lstrip("@"):
                raise RuntimeError(f"Unexpected Telegram account @{username or '?'}")

            results = []
            for source in sources:
                item = await collect_channel(
                    client,
                    conn,
                    source,
                    args.per_channel,
                    args.store_mode,
                    args.download_media,
                    Path(args.media_dir).resolve(),
                    args.join_channels,
                    args.join_wait_limit,
                )
                results.append(item)
                log.info(
                    "%s: inserted=%s seen=%s skipped=%s filtered=%s join=%s errors=%s",
                    item["handle"],
                    item["inserted"],
                    item["seen"],
                    item["skipped"],
                    item.get("filtered", 0),
                    item.get("join_status", "not_requested"),
                    len(item["errors"]),
                )
                if args.pause > 0:
                    await asyncio.sleep(args.pause)

            totals = {
                "seeded_sources": seeded,
                "sources_total": len(sources),
                "messages_seen": sum(int(r["seen"]) for r in results),
                "messages_inserted": sum(int(r["inserted"]) for r in results),
                "messages_skipped": sum(int(r["skipped"]) for r in results),
                "messages_filtered": sum(int(r.get("filtered", 0)) for r in results),
                "sources_with_errors": sum(1 for r in results if r["errors"]),
                "sources_joined": sum(1 for r in results if r.get("join_status") in {"joined", "joined_after_wait"}),
                "sources_already_joined": sum(1 for r in results if r.get("join_status") == "already_joined"),
                "sources_join_failed": sum(1 for r in results if r.get("join_status") == "failed"),
            }
            filter_reasons = Counter()
            for item in results:
                filter_reasons.update(item.get("filter_reasons") or {})
            label_counts = {
                row["tag_name"]: row["n"]
                for row in conn.execute(
                    """
                    SELECT tag_name, COUNT(*) AS n
                    FROM content_tags
                    WHERE tag_source='telegram_test_quality'
                    GROUP BY tag_name
                    ORDER BY n DESC
                    """
                ).fetchall()
            }
            verification_result = None
            if args.run_verification:
                conn.close()
                verification_result = run_verification(
                    db_path,
                    args.verification_content_limit,
                    args.verification_limit,
                    args.verification_external,
                    Path(args.verification_evidence_db).resolve() if args.verification_evidence_db else None,
                )
                conn = connect_db(db_path)
            return {
                "db": str(db_path),
                "session": args.session_name,
                "account": f"@{username}" if username else "",
                "store_mode": args.store_mode,
                "totals": totals,
                "filter_reasons": dict(filter_reasons),
                "quality_tags": label_counts,
                "verification": verification_result,
                "results": results,
            }
        finally:
            await client.disconnect()
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect Telegram sources into an isolated test DB via Telethon")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--reset", action="store_true", help="Delete the test DB before collecting")
    parser.add_argument("--api-config", default=str(DEFAULT_API_CONFIG))
    parser.add_argument("--session-source", default=str(DEFAULT_SESSION_SOURCE))
    parser.add_argument("--session-name", default="campus5197")
    parser.add_argument("--expected-username", default="campus5197")
    parser.add_argument("--refresh-session", action="store_true", help="Refresh local copy of the Telethon session")
    parser.add_argument("--per-channel", type=int, default=20)
    parser.add_argument("--source-limit", type=int, default=0, help="0 means all Telegram sources")
    parser.add_argument("--only", nargs="*", help="Optional channel handles without @")
    parser.add_argument(
        "--store-mode",
        choices=["all", "filtered", "news_only", "negative_only"],
        default="negative_only",
        help="all keeps everything; filtered drops ads/noise; news_only keeps stronger news signals; negative_only keeps only adverse public-interest signals",
    )
    parser.add_argument("--join-channels", action="store_true", help="Subscribe the test account to resolved public channels")
    parser.add_argument("--join-wait-limit", type=int, default=90, help="Max seconds to wait on Telegram join flood wait")
    parser.add_argument("--download-media", action="store_true")
    parser.add_argument("--media-dir", default=str(DEFAULT_MEDIA_DIR))
    parser.add_argument("--report", default=str(PROJECT_ROOT / "reports" / "telegram_test_collect_latest.json"))
    parser.add_argument("--run-verification", action="store_true", help="Run claims/evidence verification after collection")
    parser.add_argument("--verification-content-limit", type=int, default=500)
    parser.add_argument("--verification-limit", type=int, default=200)
    parser.add_argument("--verification-external", action="store_true", help="Enable external registry HTTP checks")
    parser.add_argument("--verification-evidence-db", help="Optional external DB path for temporary corpus corroboration")
    parser.add_argument("--pause", type=float, default=1.0)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = parse_args()
    result = asyncio.run(run(args))
    output = json.dumps(result, ensure_ascii=False, indent=2)
    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
