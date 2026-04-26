from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

from config.db_utils import get_db

WORD_RE = re.compile(r"[0-9a-zа-яё]+", re.IGNORECASE)
SPACE_RE = re.compile(r"\s+")


def now_iso() -> str:
    return datetime.now(UTC).replace(tzinfo=None, microsecond=0).isoformat()


def open_db(settings: dict[str, Any]) -> sqlite3.Connection:
    return get_db(settings)


def json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    text = text.replace("\u00a0", " ")
    return SPACE_RE.sub(" ", text).strip()


def normalize_text(value: Any) -> str:
    text = clean_text(value).lower().replace("ё", "е")
    text = re.sub(r"https?://\S+", " ", text)
    tokens = WORD_RE.findall(text)
    return " ".join(tokens)


def slugify(value: str, fallback: str = "item", max_len: int = 80) -> str:
    tokens = WORD_RE.findall(normalize_text(value))
    if not tokens:
        return fallback
    return "-".join(tokens)[:max_len] or fallback


def title_signature(title: str, body: str = "", *, token_limit: int = 14) -> str:
    title_tokens = WORD_RE.findall(normalize_text(title))
    if title_tokens:
        return " ".join(title_tokens[:token_limit])
    body_tokens = WORD_RE.findall(normalize_text(body))
    return " ".join(body_tokens[:token_limit])


def body_signature(body: str, *, token_limit: int = 32) -> str:
    body_tokens = WORD_RE.findall(normalize_text(body))
    return " ".join(body_tokens[:token_limit])


def stable_hash(*parts: Any, prefix: str = "") -> str:
    payload = "||".join(clean_text(part) for part in parts if clean_text(part))
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
    return f"{prefix}{digest}"


def parse_money_amount(value: Any) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    text = re.sub(r"[^\d,.\-]", "", text)
    if not text:
        return None
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def parse_json(value: Any, default: Any = None) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def row_value(row: Any, key: str | int, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(key, int):
        try:
            return row[key]
        except Exception:
            return default
    try:
        return row[key]
    except Exception:
        return default


def source_host(url: str) -> str:
    try:
        return urlparse(clean_text(url)).netloc.lower()
    except Exception:
        return ""


def resolve_source_for_url(
    conn: sqlite3.Connection,
    *,
    url: str,
    fallback_name: str,
    fallback_category: str,
    fallback_subcategory: str = "",
    is_official: int = 1,
) -> int:
    cleaned_url = clean_text(url)
    host = source_host(cleaned_url)
    if host:
        row = conn.execute(
            """
            SELECT id
            FROM sources
            WHERE lower(url) LIKE ?
            ORDER BY is_official DESC, id ASC
            LIMIT 1
            """,
            (f"%{host}%",),
        ).fetchone()
        if row:
            return int(row[0])
    row = conn.execute(
        "SELECT id FROM sources WHERE url=? AND category=? LIMIT 1",
        (cleaned_url, fallback_category),
    ).fetchone()
    if row:
        return int(row[0])
    cur = conn.execute(
        """
        INSERT INTO sources(
            name, category, subcategory, url, access_method, is_official,
            credibility_tier, update_frequency, is_active, notes
        ) VALUES(?,?,?,?,?,?,?,?,1,?)
        """,
        (
            fallback_name,
            fallback_category,
            fallback_subcategory or None,
            cleaned_url,
            "html",
            int(is_official),
            "A" if is_official else "B",
            "manual",
            "Created by enrichment pipeline",
        ),
    )
    return int(cur.lastrowid)


def ensure_raw_item(
    conn: sqlite3.Connection,
    *,
    source_id: int,
    external_id: str,
    raw_payload: Any,
    is_processed: int = 1,
) -> int:
    raw_json = raw_payload if isinstance(raw_payload, str) else json_dumps(raw_payload)
    raw_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
    row = conn.execute(
        "SELECT id FROM raw_source_items WHERE source_id=? AND external_id=? LIMIT 1",
        (source_id, external_id),
    ).fetchone()
    if row:
        raw_id = int(row[0])
        conn.execute(
            """
            UPDATE raw_source_items
            SET raw_payload=?, hash_sha256=?, collected_at=?, is_processed=?
            WHERE id=?
            """,
            (raw_json, raw_hash, now_iso(), int(is_processed), raw_id),
        )
        return raw_id
    cur = conn.execute(
        """
        INSERT INTO raw_source_items(source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed)
        VALUES(?,?,?,?,?,?)
        """,
        (source_id, external_id, raw_json, now_iso(), raw_hash, int(is_processed)),
    )
    return int(cur.lastrowid)


def update_content_search(conn: sqlite3.Connection, *, content_id: int, title: str, body_text: str):
    old = conn.execute("SELECT title, body_text FROM content_items WHERE id=?", (content_id,)).fetchone()
    if old:
        conn.execute(
            "INSERT INTO content_search(content_search, rowid, title, body_text) VALUES('delete', ?, ?, ?)",
            (content_id, old["title"] or "", (old["body_text"] or "")[:50000]),
        )
    conn.execute(
        "INSERT INTO content_search(rowid, title, body_text) VALUES(?,?,?)",
        (content_id, title or "", (body_text or "")[:50000]),
    )


def ensure_content_item(
    conn: sqlite3.Connection,
    *,
    source_id: int,
    raw_item_id: int,
    external_id: str,
    content_type: str,
    title: str,
    body_text: str,
    published_at: str | None,
    url: str | None,
    status: str = "raw_signal",
    language: str = "ru",
) -> int:
    row = conn.execute(
        "SELECT id FROM content_items WHERE source_id=? AND external_id=? LIMIT 1",
        (source_id, external_id),
    ).fetchone()
    if row:
        content_id = int(row[0])
        conn.execute(
            """
            UPDATE content_items
            SET raw_item_id=?, content_type=?, title=?, body_text=?, published_at=?, url=?, status=?, language=?
            WHERE id=?
            """,
            (
                raw_item_id,
                content_type,
                clean_text(title) or None,
                clean_text(body_text)[:50000],
                clean_text(published_at) or None,
                clean_text(url) or None,
                status,
                language,
                content_id,
            ),
        )
        update_content_search(conn, content_id=content_id, title=clean_text(title), body_text=clean_text(body_text))
        return content_id

    cur = conn.execute(
        """
        INSERT INTO content_items(
            source_id, raw_item_id, external_id, content_type, title, body_text,
            published_at, collected_at, url, language, status
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            source_id,
            raw_item_id,
            external_id,
            content_type,
            clean_text(title) or None,
            clean_text(body_text)[:50000],
            clean_text(published_at) or None,
            now_iso(),
            clean_text(url) or None,
            language,
            status,
        ),
    )
    content_id = int(cur.lastrowid)
    conn.execute(
        "INSERT INTO content_search(rowid, title, body_text) VALUES(?,?,?)",
        (content_id, clean_text(title), clean_text(body_text)[:50000]),
    )
    return content_id


def ensure_entity_media(
    conn: sqlite3.Connection,
    *,
    entity_id: int,
    attachment_id: int,
    media_kind: str,
    source_url: str | None,
    caption: str | None = None,
    is_primary: int = 1,
    metadata: dict[str, Any] | None = None,
) -> int:
    row = conn.execute(
        """
        SELECT id
        FROM entity_media
        WHERE entity_id=? AND attachment_id=? AND media_kind=?
        LIMIT 1
        """,
        (entity_id, attachment_id, media_kind),
    ).fetchone()
    payload = (
        clean_text(source_url) or None,
        int(is_primary),
        clean_text(caption) or None,
        json_dumps(metadata or {}) if metadata else None,
    )
    if row:
        media_id = int(row[0])
        conn.execute(
            "UPDATE entity_media SET source_url=?, is_primary=?, caption=?, metadata_json=? WHERE id=?",
            payload + (media_id,),
        )
        return media_id
    cur = conn.execute(
        """
        INSERT INTO entity_media(entity_id, attachment_id, media_kind, source_url, is_primary, caption, metadata_json)
        VALUES(?,?,?,?,?,?,?)
        """,
        (entity_id, attachment_id, media_kind) + payload,
    )
    return int(cur.lastrowid)


def ensure_review_task(
    conn: sqlite3.Connection,
    *,
    task_key: str,
    queue_key: str,
    subject_type: str,
    subject_id: int | None = None,
    related_id: int | None = None,
    candidate_payload: dict[str, Any] | list[Any] | None = None,
    suggested_action: str,
    confidence: float = 0.0,
    machine_reason: str = "",
    source_links: Iterable[str] | None = None,
    status: str = "open",
    review_pack_id: str | None = None,
    reviewer: str | None = None,
    resolution_notes: str | None = None,
) -> int:
    payload_json = json_dumps(candidate_payload) if candidate_payload is not None else None
    source_links_json = json_dumps(list(dict.fromkeys(clean_text(item) for item in (source_links or []) if clean_text(item))))
    row = conn.execute("SELECT id FROM review_tasks WHERE task_key=? LIMIT 1", (task_key,)).fetchone()
    values = (
        queue_key,
        subject_type,
        subject_id,
        related_id,
        payload_json,
        suggested_action,
        float(confidence or 0),
        machine_reason or None,
        source_links_json or None,
        status,
        review_pack_id,
        reviewer,
        resolution_notes,
    )
    if row:
        task_id = int(row[0])
        conn.execute(
            """
            UPDATE review_tasks
            SET queue_key=?, subject_type=?, subject_id=?, related_id=?, candidate_payload=?,
                suggested_action=?, confidence=?, machine_reason=?, source_links_json=?,
                status=?, review_pack_id=COALESCE(?, review_pack_id), reviewer=COALESCE(?, reviewer),
                resolution_notes=COALESCE(?, resolution_notes), updated_at=?
            WHERE id=?
            """,
            values + (now_iso(), task_id),
        )
        return task_id
    cur = conn.execute(
        """
        INSERT INTO review_tasks(
            task_key, queue_key, subject_type, subject_id, related_id, candidate_payload,
            suggested_action, confidence, machine_reason, source_links_json, status,
            review_pack_id, reviewer, resolution_notes
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (task_key,) + values,
    )
    return int(cur.lastrowid)


def find_person_entity(conn: sqlite3.Connection, full_name: str) -> int | None:
    normalized = normalize_text(full_name)
    if not normalized:
        return None
    candidate_rows: list[tuple[int, str]] = []
    for row in conn.execute("SELECT entity_id, full_name FROM deputy_profiles").fetchall():
        candidate_rows.append((int(row_value(row, "entity_id", row_value(row, 0))), clean_text(row_value(row, "full_name", row_value(row, 1, "")))))
    for row in conn.execute("SELECT id, canonical_name FROM entities WHERE entity_type='person'").fetchall():
        candidate_rows.append((int(row_value(row, "id", row_value(row, 0))), clean_text(row_value(row, "canonical_name", row_value(row, 1, "")))))

    for entity_id, candidate_name in candidate_rows:
        if normalize_text(candidate_name) == normalized:
            return entity_id
    tokens = [token for token in normalized.split() if token]
    primary_terms: list[str] = []
    for token in tokens[:2]:
        if token not in primary_terms:
            primary_terms.append(token)
    if len(primary_terms) == 2:
        for entity_id, candidate_name in candidate_rows:
            candidate_tokens = set(normalize_text(candidate_name).split())
            if all(term in candidate_tokens for term in primary_terms):
                return entity_id
    return None


def maybe_parse_extra_photo(extra_data: Any) -> str:
    payload = parse_json(extra_data, default={})
    if not isinstance(payload, dict):
        return ""
    return clean_text(payload.get("photo_url"))


def ensure_dir(path: Path | str) -> Path:
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    return target
