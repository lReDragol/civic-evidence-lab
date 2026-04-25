from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings


log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "executive_sources.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.5",
}

FULL_NAME_RE = re.compile(
    r"\b[А-ЯЁ][а-яё-]+(?:\s+[А-ЯЁ][а-яё-]+){2}\b"
)
POSITION_MARKERS = (
    "министр",
    "руководител",
    "заместител",
    "председател",
    "директор",
    "начальник",
    "глава",
)
FULL_NAME_STOPWORDS = {
    "заместитель",
    "председатель",
    "председателя",
    "правительства",
    "руководитель",
    "руководителя",
    "аппарата",
    "министр",
    "министра",
    "директор",
    "директора",
    "начальник",
    "начальника",
    "глава",
    "службы",
    "секретарь",
    "секретаря",
    "пресс-секретарь",
}
PROFILE_BODY_SELECTORS = (
    ".manager_block",
    ".manager_block_content",
    ".managerbio",
    ".page_description",
    ".person__bio",
    ".person__body",
    ".person-card__description",
    ".article__content",
    ".content-body",
)
PROFILE_BODY_STOP_MARKERS = (
    "Информация размещена с согласия субъекта персональных данных",
    "Документ создан:",
    "Телефон:",
    "Адрес электронной почты:",
    "Телефон единого контактного центра",
    "Обратная связь по сайту",
    "Наш сайт использует файлы cookie",
    "Полная версия Версия для слабовидящих",
)
POSITION_SENTENCE_RE = re.compile(
    r"(?:назначен(?:а)?(?:\s+на\s+должность)?|занимает\s+должность)\s+"
    r"(?P<position>(?:статс-секретар[её]м?\s*[-–]\s*)?"
    r"(?:(?:перв(?:ым|ого|ый)\s+)?заместител(?:[ея]|ь)[^.,;:]{0,120}?руководител(?:[ея]|ь)[^.,;:]{0,80}"
    r"|руководител(?:[ея]|ь)[^.,;:]{0,120}"
    r"|министр[а-яё]{0,12}[^.,;:]{0,120}))",
    flags=re.IGNORECASE | re.UNICODE,
)
POSITION_TIMELINE_RE = re.compile(
    r"(?:С\s+\d{4}(?:\s+года)?\s*[–-]?\s*)"
    r"(?P<position>(?:статс-секретар[её]м?\s*[-–]\s*)?"
    r"(?:(?:перв(?:ым|ого|ый)\s+)?заместител(?:[ея]|ь)[^.;:]{0,120}?руководител(?:[ея]|ь)[^.;:]{0,80}"
    r"|руководител(?:[ея]|ь)[^.;:]{0,120}"
    r"|министр[а-яё]{0,12}[^.;:]{0,120}))",
    flags=re.IGNORECASE | re.UNICODE,
)
TEXT_DIRECTORY_RE = re.compile(
    r"(?P<full_name>[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+\s+[А-ЯЁ][а-яё-]+)\s+"
    r"(?P<position_title>(?:Первый\s+)?(?:Заместитель|заместитель|Руководитель|руководитель|Министр|министр|"
    r"Председатель|председатель|Глава|глава)[^.!?]{0,120}?)"
    r"(?=\s+подробнее|\s+Родил|\s+\d{4}\s*г|\s+Назначен|\s+Указом|\s+Распоряжением)",
    flags=re.UNICODE,
)


def _session():
    import requests

    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def normalize_text(value: Any) -> str:
    return clean_text(value).casefold()


def looks_like_full_name(value: str) -> bool:
    text = clean_text(value)
    if not text:
        return False
    match = FULL_NAME_RE.fullmatch(text)
    if match is None:
        return False
    tokens = [part.casefold() for part in text.split()]
    return all(token not in FULL_NAME_STOPWORDS for token in tokens)


def looks_like_position(value: str) -> bool:
    text = normalize_text(value)
    if not text or len(text) < 8:
        return False
    if "биограф" in text or "подробнее" in text:
        return False
    return any(marker in text for marker in POSITION_MARKERS)


def normalize_position_title(value: str) -> str:
    text = clean_text(value).strip(" .;:,")
    if not text:
        return ""
    text = re.sub(r"^С\s+\d{4}(?:\s+года)?\s*[–-]?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^статс-секретар[её]м", "Статс-секретарь", text, flags=re.IGNORECASE)
    text = re.sub(r"^первым\s+заместителем", "Первый заместитель", text, flags=re.IGNORECASE)
    text = re.sub(r"^первого\s+заместителя", "Первый заместитель", text, flags=re.IGNORECASE)
    text = re.sub(r"^заместителем", "Заместитель", text, flags=re.IGNORECASE)
    text = re.sub(r"^заместителя", "Заместитель", text, flags=re.IGNORECASE)
    text = re.sub(r"^руководителем", "Руководитель", text, flags=re.IGNORECASE)
    text = re.sub(r"^руководителя", "Руководитель", text, flags=re.IGNORECASE)
    text = re.sub(r"^министром", "Министр", text, flags=re.IGNORECASE)
    text = re.sub(r"^министра", "Министр", text, flags=re.IGNORECASE)
    text = clean_text(text).strip(" .;:,")
    if text:
        text = text[0].upper() + text[1:]
    return text


def slugify(value: str, fallback: str = "item") -> str:
    value = clean_text(value)
    cleaned = re.sub(r"[^\w\s-]+", "-", value, flags=re.UNICODE)
    cleaned = re.sub(r"\s+", "-", cleaned, flags=re.UNICODE).strip("-_ ")
    return cleaned or fallback


def load_sources_config(config_path: Path = CONFIG_PATH) -> list[dict[str, Any]]:
    return json.loads(config_path.read_text(encoding="utf-8"))


def ensure_source(conn: sqlite3.Connection, source_cfg: dict[str, Any]) -> int:
    url = clean_text(source_cfg.get("url"))
    category = clean_text(source_cfg.get("category") or "official_site")
    row = conn.execute(
        "SELECT id FROM sources WHERE url=? AND category=? LIMIT 1",
        (url, category),
    ).fetchone()
    params = (
        clean_text(source_cfg.get("name")),
        category,
        clean_text(source_cfg.get("subcategory")),
        url,
        clean_text(source_cfg.get("access_method") or "html"),
        1,
        clean_text(source_cfg.get("credibility_tier") or "A"),
        clean_text(source_cfg.get("update_frequency") or "weekly"),
        clean_text(source_cfg.get("notes")),
    )
    if row:
        update_params = (
            clean_text(source_cfg.get("name")),
            clean_text(source_cfg.get("subcategory")),
            clean_text(source_cfg.get("access_method") or "html"),
            1,
            clean_text(source_cfg.get("credibility_tier") or "A"),
            clean_text(source_cfg.get("update_frequency") or "weekly"),
            clean_text(source_cfg.get("notes")),
        )
        conn.execute(
            """
            UPDATE sources
            SET name=?, subcategory=?, access_method=?, is_official=?, credibility_tier=?,
                update_frequency=?, notes=?, last_checked_at=datetime('now'), is_active=1
            WHERE id=?
            """,
            update_params + (row[0],),
        )
        return int(row[0])

    cur = conn.execute(
        """
        INSERT INTO sources(
            name, category, subcategory, url, access_method, is_official,
            credibility_tier, update_frequency, notes, is_active, last_checked_at
        ) VALUES(?,?,?,?,?,?,?,?,?,1,datetime('now'))
        """,
        params,
    )
    return int(cur.lastrowid)


def _split_inline_name_position(text: str) -> tuple[str | None, str | None]:
    cleaned = clean_text(text)
    if not cleaned:
        return None, None
    if looks_like_full_name(cleaned):
        return cleaned, None

    matches = [match for match in FULL_NAME_RE.finditer(cleaned) if looks_like_full_name(match.group(0))]
    if not matches:
        return None, None
    match = next((item for item in reversed(matches) if item.end() == len(cleaned)), matches[-1])
    full_name = clean_text(match.group(0))
    prefix = clean_text(cleaned[:match.start()])
    suffix = clean_text(cleaned[match.end():])

    prefix = clean_text(prefix.strip(" -–,;:"))
    suffix = clean_text(suffix.strip(" -–,;:"))

    if looks_like_position(prefix) and not looks_like_position(suffix):
        return full_name, prefix
    if looks_like_position(suffix) and not looks_like_position(prefix):
        return full_name, suffix
    if prefix and suffix:
        combined = clean_text(f"{prefix} {suffix}")
        if looks_like_position(combined):
            return full_name, combined
    return full_name, None


def _candidate_position_texts(anchor) -> list[str]:
    candidates: list[str] = []
    nodes = []
    if anchor is not None:
        nodes.extend(
            [
                anchor,
                anchor.parent,
                anchor.find_next_sibling(),
            ]
        )
    for node in nodes:
        if node is None:
            continue
        texts = [clean_text(node.get_text(" ", strip=True))]
        try:
            texts.extend(
                clean_text(item.get_text(" ", strip=True))
                for item in node.select("p, li, div, span, h2, h3, h4, strong")
            )
        except Exception:
            pass
        for text in texts:
            if text and text not in candidates:
                candidates.append(text)
    return candidates


def _pick_position(candidates: list[str], full_name: str) -> str:
    filtered: list[str] = []
    for candidate in candidates:
        text = clean_text(candidate.replace(full_name, ""))
        text = re.sub(r"\bСмотреть биографию\b", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\bподробнее\b", "", text, flags=re.IGNORECASE)
        text = clean_text(text.strip(" -–,;:"))
        if looks_like_position(text):
            filtered.append(text)
    if not filtered:
        return ""
    filtered.sort(key=len)
    return filtered[0]


def parse_profile_links_directory(
    html: str,
    base_url: str,
    href_patterns: list[str] | tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    seen: set[str] = set()
    people: list[dict[str, Any]] = []
    for anchor in soup.select("a[href]"):
        href = clean_text(anchor.get("href"))
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue

        raw_text = clean_text(anchor.get_text(" ", strip=True))
        full_name, inline_position = _split_inline_name_position(raw_text)
        if not full_name or not looks_like_full_name(full_name):
            continue

        profile_url = urljoin(base_url, href)
        if href_patterns and not any(pattern in href or pattern in profile_url for pattern in href_patterns):
            continue
        if profile_url in seen:
            continue
        seen.add(profile_url)

        context_candidates = _candidate_position_texts(anchor)
        person = {
            "full_name": full_name,
            "position_title": inline_position or _pick_position(context_candidates, full_name),
            "profile_url": profile_url,
            "context_text": clean_text(" ".join(context_candidates)),
        }
        people.append(person)
    return people


def parse_text_directory(html: str) -> list[dict[str, Any]]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    text = clean_text(soup.get_text(" ", strip=True))
    seen: set[tuple[str, str]] = set()
    people: list[dict[str, Any]] = []
    for match in TEXT_DIRECTORY_RE.finditer(text):
        full_name = clean_text(match.group("full_name"))
        position_title = clean_text(match.group("position_title"))
        key = (full_name, position_title)
        if key in seen:
            continue
        seen.add(key)
        people.append(
            {
                "full_name": full_name,
                "position_title": position_title,
                "profile_url": "",
            }
        )
    return people


def parse_profile_page(html: str, page_url: str) -> dict[str, Any]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    name = ""
    heading = None
    for node in soup.select("h1, h2, h3, .title, .person__name"):
        text = clean_text(node.get_text(" ", strip=True))
        if looks_like_full_name(text):
            name = text
            heading = node
            break

    position = ""
    if heading is not None:
        sibling = heading
        for _ in range(8):
            sibling = sibling.find_next_sibling()
            node = sibling
            if node is None:
                break
            text = clean_text(node.get_text(" ", strip=True))
            if not text or text == name:
                continue
            if len(text) > 180:
                continue
            if "." in text or any(ch.isdigit() for ch in text):
                continue
            if text.lower().startswith(("с ", "в ", "до ", "родил")):
                continue
            if looks_like_position(text):
                position = normalize_position_title(text)
                break
        if not position:
            for container in (heading.parent, heading.parent.parent if heading.parent else None):
                if container is None:
                    continue
                text = clean_text(container.get_text(" ", strip=True))
                text = clean_text(text.replace(name, ""))
                text = re.sub(
                    r"\b(?:биографи[яию]|курируемые\s+департаменты|смотреть\s+биографию)\b",
                    "",
                    text,
                    flags=re.IGNORECASE,
                )
                for candidate in re.split(r"\s{2,}|(?<=\.)\s+", text):
                    candidate = clean_text(candidate.strip(" -–,;:"))
                    if looks_like_position(candidate):
                        position = normalize_position_title(candidate)
                        break
                if position:
                    break

    photo_url = ""
    normalized_name = normalize_text(name)
    surname = normalized_name.split(" ", 1)[0] if normalized_name else ""
    images: list[tuple[int, str]] = []
    for img in soup.select("img[src]"):
        src = clean_text(img.get("src"))
        if not src or src.startswith("data:"):
            continue
        alt = normalize_text(img.get("alt"))
        score = 0
        if normalized_name and normalized_name in alt:
            score += 5
        if surname and surname in normalize_text(src):
            score += 3
        if alt and "интервью" not in alt and "новость" not in alt:
            score += 1
        images.append((score, urljoin(page_url, src)))
    if images:
        images.sort(key=lambda item: item[0], reverse=True)
        photo_url = images[0][1]

    for tag in soup.select("script, style, noscript, svg"):
        tag.decompose()

    body_text = ""
    best_candidate = ""
    for selector in PROFILE_BODY_SELECTORS:
        node = soup.select_one(selector)
        if node is None:
            continue
        candidate = clean_text(node.get_text("\n", strip=True))
        candidate = re.sub(r"\bБиография\s+Публикации\b", "", candidate, flags=re.IGNORECASE)
        candidate = clean_text(candidate)
        if candidate:
            if len(candidate) > len(best_candidate):
                best_candidate = candidate
            if len(candidate) >= 220:
                body_text = candidate
                break
    if not body_text:
        fallback = clean_text(soup.get_text("\n", strip=True))
        body_text = fallback if len(fallback) > len(best_candidate) else best_candidate

    for marker in PROFILE_BODY_STOP_MARKERS:
        idx = body_text.find(marker)
        if idx > 0:
            body_text = body_text[:idx].strip()
    body_text = body_text[:12000]

    if not position and body_text:
        match = POSITION_SENTENCE_RE.search(body_text) or POSITION_TIMELINE_RE.search(body_text)
        if match:
            position = normalize_position_title(match.group("position"))

    return {
        "full_name": name,
        "position_title": position,
        "profile_url": page_url,
        "photo_url": photo_url,
        "bio_text": body_text,
    }


def _load_entity_extra(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None or not row["extra_data"]:
        return {}
    try:
        data = json.loads(row["extra_data"])
    except (json.JSONDecodeError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _upsert_entity(
    conn: sqlite3.Connection,
    entity_type: str,
    canonical_name: str,
    *,
    description: str = "",
    extra_updates: dict[str, Any] | None = None,
) -> int:
    row = conn.execute(
        "SELECT id, extra_data FROM entities WHERE entity_type=? AND canonical_name=? LIMIT 1",
        (entity_type, canonical_name),
    ).fetchone()
    if row:
        extra_data = _load_entity_extra(row)
        for key, value in (extra_updates or {}).items():
            if value:
                extra_data[key] = value
        conn.execute(
            "UPDATE entities SET description=?, extra_data=? WHERE id=?",
            (
                description or None,
                json.dumps(extra_data, ensure_ascii=False) if extra_data else None,
                row["id"],
            ),
        )
        return int(row["id"])

    cur = conn.execute(
        """
        INSERT INTO entities(entity_type, canonical_name, description, extra_data)
        VALUES(?,?,?,?)
        """,
        (
            entity_type,
            canonical_name,
            description or None,
            json.dumps(extra_updates or {}, ensure_ascii=False) if extra_updates else None,
        ),
    )
    return int(cur.lastrowid)


def _ensure_alias(conn: sqlite3.Connection, entity_id: int, alias: str):
    alias = clean_text(alias)
    if not alias or alias == str(entity_id):
        return
    conn.execute(
        "INSERT OR IGNORE INTO entity_aliases(entity_id, alias, alias_type) VALUES(?,?,?)",
        (entity_id, alias, "official_short"),
    )


def _mark_previous_positions_inactive(
    conn: sqlite3.Connection,
    entity_id: int,
    source_type: str,
    organization: str,
    current_title: str,
):
    conn.execute(
        """
        UPDATE official_positions
        SET is_active=0, ended_at=COALESCE(ended_at, date('now'))
        WHERE entity_id=? AND source_type=? AND organization=? AND is_active=1 AND position_title<>?
        """,
        (entity_id, source_type, organization, current_title),
    )


def _upsert_position(
    conn: sqlite3.Connection,
    *,
    entity_id: int,
    position_title: str,
    organization: str,
    source_url: str,
    source_type: str,
):
    existing = conn.execute(
        """
        SELECT id
        FROM official_positions
        WHERE entity_id=? AND position_title=? AND organization=? AND source_type=? AND is_active=1
        LIMIT 1
        """,
        (entity_id, position_title, organization, source_type),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE official_positions
            SET source_url=?, is_active=1, ended_at=NULL
            WHERE id=?
            """,
            (source_url or None, existing["id"]),
        )
        return int(existing["id"])

    cur = conn.execute(
        """
        INSERT INTO official_positions(
            entity_id, position_title, organization, started_at, source_url, source_type, is_active
        ) VALUES(?,?,?,?,?,?,1)
        """,
        (
            entity_id,
            position_title,
            organization,
            datetime.now().date().isoformat(),
            source_url or None,
            source_type,
        ),
    )
    return int(cur.lastrowid)


def _ext_id(source_cfg: dict[str, Any], person: dict[str, Any]) -> str:
    source_key = clean_text(source_cfg.get("key") or "executive")
    seed = clean_text(person.get("profile_url")) or clean_text(person.get("full_name"))
    if not seed:
        seed = hashlib.sha256(json.dumps(person, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:20]
    return f"{source_key}:{digest}"


def _upsert_raw_item(conn: sqlite3.Connection, source_id: int, external_id: str, payload: dict[str, Any]) -> int:
    raw_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    raw_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
    row = conn.execute(
        "SELECT id FROM raw_source_items WHERE source_id=? AND external_id=? LIMIT 1",
        (source_id, external_id),
    ).fetchone()
    if row:
        conn.execute(
            """
            UPDATE raw_source_items
            SET raw_payload=?, collected_at=?, hash_sha256=?, is_processed=1
            WHERE id=?
            """,
            (raw_json, datetime.now().isoformat(), raw_hash, row["id"]),
        )
        return int(row["id"])

    cur = conn.execute(
        """
        INSERT INTO raw_source_items(source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed)
        VALUES(?,?,?,?,?,1)
        """,
        (source_id, external_id, raw_json, datetime.now().isoformat(), raw_hash),
    )
    return int(cur.lastrowid)


def _upsert_content_item(
    conn: sqlite3.Connection,
    *,
    source_id: int,
    raw_item_id: int,
    external_id: str,
    title: str,
    body_text: str,
    published_at: str,
    url: str,
) -> int:
    row = conn.execute(
        "SELECT id, title, body_text FROM content_items WHERE source_id=? AND external_id=? LIMIT 1",
        (source_id, external_id),
    ).fetchone()
    if row:
        content_id = int(row["id"])
        conn.execute(
            """
            INSERT INTO content_search(content_search, rowid, title, body_text)
            VALUES('delete', ?, ?, ?)
            """,
            (content_id, row["title"] or "", (row["body_text"] or "")[:50000]),
        )
        conn.execute(
            """
            UPDATE content_items
            SET raw_item_id=?, title=?, body_text=?, published_at=?, url=?, status='raw_signal'
            WHERE id=?
            """,
            (raw_item_id, title, body_text[:50000], published_at or None, url or None, content_id),
        )
        conn.execute(
            "INSERT INTO content_search(rowid, title, body_text) VALUES(?,?,?)",
            (content_id, title, body_text[:50000]),
        )
        return content_id

    cur = conn.execute(
        """
        INSERT INTO content_items(
            source_id, raw_item_id, external_id, content_type, title, body_text,
            published_at, collected_at, url, status
        ) VALUES(?,?,?,?,?,?,?,?,?,'raw_signal')
        """,
        (
            source_id,
            raw_item_id,
            external_id,
            "profile",
            title,
            body_text[:50000],
            published_at or None,
            datetime.now().isoformat(),
            url or None,
        ),
    )
    content_id = int(cur.lastrowid)
    conn.execute(
        "INSERT INTO content_search(rowid, title, body_text) VALUES(?,?,?)",
        (content_id, title, body_text[:50000]),
    )
    return content_id


def _ensure_mention(conn: sqlite3.Connection, entity_id: int, content_item_id: int, mention_type: str):
    row = conn.execute(
        """
        SELECT id
        FROM entity_mentions
        WHERE entity_id=? AND content_item_id=? AND mention_type=?
        LIMIT 1
        """,
        (entity_id, content_item_id, mention_type),
    ).fetchone()
    if row:
        return int(row["id"])
    cur = conn.execute(
        """
        INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence)
        VALUES(?,?,?,1.0)
        """,
        (entity_id, content_item_id, mention_type),
    )
    return int(cur.lastrowid)


def store_person_record(
    conn: sqlite3.Connection,
    source_id: int,
    source_cfg: dict[str, Any],
    person: dict[str, Any],
) -> dict[str, int]:
    full_name = clean_text(person.get("full_name"))
    if not looks_like_full_name(full_name):
        raise ValueError(f"Invalid executive full name: {full_name!r}")

    organization = clean_text(person.get("organization") or source_cfg.get("organization"))
    position_title = clean_text(person.get("position_title"))
    profile_url = clean_text(person.get("profile_url"))
    photo_url = clean_text(person.get("photo_url"))
    bio_text = clean_text(person.get("bio_text"))
    published_at = clean_text(person.get("published_at")) or datetime.now().date().isoformat()
    source_type = f"executive_directory:{clean_text(source_cfg.get('key') or 'unknown')}"

    person_entity_id = _upsert_entity(
        conn,
        "person",
        full_name,
        description=position_title,
        extra_updates={
            "profile_url": profile_url or None,
            "photo_url": photo_url or None,
            "organization": organization or None,
            "source_type": source_type,
        },
    )
    organization_entity_id = None
    if organization:
        organization_entity_id = _upsert_entity(
            conn,
            "organization",
            organization,
            description="Официальный источник руководства",
            extra_updates={
                "directory_url": clean_text(source_cfg.get("url")),
                "source_type": source_type,
            },
        )

    for alias in person.get("aliases") or []:
        _ensure_alias(conn, person_entity_id, alias)

    if position_title and organization:
        _mark_previous_positions_inactive(
            conn,
            person_entity_id,
            source_type,
            organization,
            position_title,
        )
        _upsert_position(
            conn,
            entity_id=person_entity_id,
            position_title=position_title,
            organization=organization,
            source_url=profile_url or clean_text(source_cfg.get("url")),
            source_type=source_type,
        )

    payload = {
        "source_key": source_cfg.get("key"),
        "organization": organization,
        "full_name": full_name,
        "position_title": position_title,
        "profile_url": profile_url,
        "photo_url": photo_url,
        "bio_text": bio_text,
        "aliases": person.get("aliases") or [],
    }
    external_id = _ext_id(source_cfg, person)
    raw_item_id = _upsert_raw_item(conn, source_id, external_id, payload)

    title = full_name
    if position_title:
        title = f"{full_name} — {position_title}"

    body_parts = []
    if position_title:
        body_parts.append(position_title)
    if organization:
        body_parts.append(organization)
    if bio_text:
        body_parts.append("")
        body_parts.append(bio_text)
    if profile_url:
        body_parts.append("")
        body_parts.append(f"Профиль: {profile_url}")
    body_text = "\n".join(part for part in body_parts if part is not None)

    content_item_id = _upsert_content_item(
        conn,
        source_id=source_id,
        raw_item_id=raw_item_id,
        external_id=external_id,
        title=title,
        body_text=body_text,
        published_at=published_at,
        url=profile_url or clean_text(source_cfg.get("url")),
    )
    conn.execute(
        """
        DELETE FROM entity_mentions
        WHERE content_item_id=? AND mention_type IN ('subject', 'organization')
        """,
        (content_item_id,),
    )
    _ensure_mention(conn, person_entity_id, content_item_id, "subject")
    if organization_entity_id is not None:
        _ensure_mention(conn, organization_entity_id, content_item_id, "organization")

    return {
        "person_entity_id": person_entity_id,
        "organization_entity_id": organization_entity_id or 0,
        "content_item_id": content_item_id,
        "raw_item_id": raw_item_id,
    }


def _merge_person(base: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in update.items():
        if value:
            merged[key] = value
    aliases = list(dict.fromkeys([*(base.get("aliases") or []), *(update.get("aliases") or [])]))
    if aliases:
        merged["aliases"] = aliases
    return merged


def _fetch_html(session, url: str, timeout: int = 25) -> tuple[str, str]:
    import requests

    last_error: Exception | None = None
    for verify in (True, False):
        try:
            response = session.get(url, timeout=timeout, allow_redirects=True, verify=verify)
            response.raise_for_status()
            return response.text, response.url
        except requests.RequestException as exc:
            last_error = exc
            continue
    raise last_error or RuntimeError(f"Failed to fetch {url}")


def _fetch_profile_detail(session, person: dict[str, Any], timeout: int = 25) -> dict[str, Any]:
    profile_url = clean_text(person.get("profile_url"))
    if not profile_url:
        return person
    try:
        html, final_url = _fetch_html(session, profile_url, timeout=timeout)
    except Exception as exc:
        log.warning("Executive detail fetch failed for %s: %s", profile_url, exc)
        return person

    parsed = parse_profile_page(html, final_url)
    if not parsed.get("full_name"):
        return person
    return _merge_person(person, parsed)


def _people_from_source(session, source_cfg: dict[str, Any], timeout: int = 25) -> list[dict[str, Any]]:
    html, final_url = _fetch_html(session, clean_text(source_cfg.get("url")), timeout=timeout)
    mode = clean_text(source_cfg.get("mode") or "profile_links")
    if mode == "text_regex":
        people = parse_text_directory(html)
    else:
        people = parse_profile_links_directory(
            html,
            final_url,
            source_cfg.get("href_patterns") or [],
        )

    people = [
        {
            **person,
            "organization": clean_text(person.get("organization") or source_cfg.get("organization")),
        }
        for person in people
        if looks_like_full_name(clean_text(person.get("full_name")))
    ]

    if mode == "profile_links" and source_cfg.get("fetch_profiles"):
        detail_limit = int(source_cfg.get("detail_limit") or len(people))
        enriched: list[dict[str, Any]] = []
        for idx, person in enumerate(people):
            if idx < detail_limit:
                enriched.append(_fetch_profile_detail(session, person, timeout=timeout))
            else:
                enriched.append(person)
        people = enriched

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for person in people:
        key = (
            clean_text(person.get("full_name")),
            clean_text(person.get("position_title")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(person)

    exclude_patterns = [normalize_text(item) for item in (source_cfg.get("exclude_text_patterns") or []) if clean_text(item)]
    if exclude_patterns:
        filtered: list[dict[str, Any]] = []
        for person in deduped:
            haystack = normalize_text(
                " ".join(
                    [
                        clean_text(person.get("full_name")),
                        clean_text(person.get("position_title")),
                        clean_text(person.get("context_text")),
                    ]
                )
            )
            if any(pattern in haystack for pattern in exclude_patterns):
                continue
            filtered.append(person)
        deduped = filtered

    allowed_patterns = [normalize_text(item) for item in (source_cfg.get("allowed_position_patterns") or []) if clean_text(item)]
    if allowed_patterns:
        deduped = [
            person
            for person in deduped
            if any(pattern in normalize_text(person.get("position_title")) for pattern in allowed_patterns)
        ]

    return deduped


def _deactivate_missing_positions(
    conn: sqlite3.Connection,
    source_cfg: dict[str, Any],
    active_people: list[dict[str, Any]],
):
    source_type = f"executive_directory:{clean_text(source_cfg.get('key') or 'unknown')}"
    organization = clean_text(source_cfg.get("organization"))
    active_keys = {
        (
            clean_text(person.get("full_name")),
            clean_text(person.get("position_title")),
            organization,
        )
        for person in active_people
        if clean_text(person.get("position_title"))
    }
    rows = conn.execute(
        """
        SELECT op.id, e.canonical_name, op.position_title, op.organization
        FROM official_positions op
        JOIN entities e ON e.id = op.entity_id
        WHERE op.source_type=? AND op.organization=? AND op.is_active=1
        """,
        (source_type, organization),
    ).fetchall()
    for row in rows:
        key = (
            clean_text(row["canonical_name"]),
            clean_text(row["position_title"]),
            clean_text(row["organization"]),
        )
        if key in active_keys:
            continue
        conn.execute(
            "UPDATE official_positions SET is_active=0, ended_at=COALESCE(ended_at, date('now')) WHERE id=?",
            (row["id"],),
        )


def collect_source(
    conn: sqlite3.Connection,
    source_cfg: dict[str, Any],
    *,
    session=None,
    timeout: int = 25,
) -> dict[str, Any]:
    session = session or _session()
    source_id = ensure_source(conn, source_cfg)
    people = _people_from_source(session, source_cfg, timeout=timeout)
    stored = 0
    for person in people:
        store_person_record(conn, source_id, source_cfg, person)
        stored += 1
    _deactivate_missing_positions(conn, source_cfg, people)
    conn.commit()
    return {
        "source_key": source_cfg.get("key"),
        "source_id": source_id,
        "organization": source_cfg.get("organization"),
        "people_found": len(people),
        "people_stored": stored,
        "status": "ok",
    }


def collect_executive_directories(
    settings: dict[str, Any] | None = None,
    *,
    keys: list[str] | None = None,
    include_disabled: bool = False,
    timeout: int = 25,
) -> dict[str, Any]:
    settings = settings or load_settings()
    conn = get_db(settings)
    session = _session()
    try:
        configs = load_sources_config()
        selected = []
        keys_normalized = {key.casefold() for key in (keys or [])}
        for cfg in configs:
            if not include_disabled and not cfg.get("enabled", True):
                continue
            if keys_normalized and clean_text(cfg.get("key")).casefold() not in keys_normalized:
                continue
            selected.append(cfg)

        results = []
        for cfg in selected:
            try:
                results.append(collect_source(conn, cfg, session=session, timeout=timeout))
            except Exception as exc:
                log.exception("Executive collection failed for %s", cfg.get("key"))
                results.append(
                    {
                        "source_key": cfg.get("key"),
                        "organization": cfg.get("organization"),
                        "status": "error",
                        "error": str(exc),
                    }
                )
        return {
            "selected_sources": [cfg.get("key") for cfg in selected],
            "results": results,
            "ok": sum(1 for item in results if item.get("status") == "ok"),
            "errors": sum(1 for item in results if item.get("status") == "error"),
        }
    finally:
        conn.close()


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Collect executive directory pages from official government sources.")
    parser.add_argument("--source", action="append", dest="sources", help="Source key from config/executive_sources.json")
    parser.add_argument("--include-disabled", action="store_true", help="Include disabled configs")
    parser.add_argument("--timeout", type=int, default=25)
    args = parser.parse_args(argv)

    result = collect_executive_directories(
        load_settings(),
        keys=args.sources,
        include_disabled=args.include_disabled,
        timeout=args.timeout,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
