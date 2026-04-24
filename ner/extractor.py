import json
import logging
import os
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

NATASHA_TYPE_MAP = {
    "PER": "person",
    "ORG": "organization",
    "LOC": "location",
}

INN_RE = re.compile(r'\b(\d{10}|\d{12})\b')
OGRN_RE = re.compile(r'\b(\d{13}|\d{15})\b')
CASE_NUM_RE = re.compile(r'дел[аоу]?\s+(?:номер\s+)?(\d+[\-/]\d+[\-/]\d+)', re.I)
PHONE_RE = re.compile(r'\b(\+7[\-\s]?\(?\d{3}\)?[\-\s]?\d{3}[\-\s]?\d{2}[\-\s]?\d{2})\b')

NAME_NORMALIZE_RE = re.compile(r'\s+')

_natasha_ready = False
_segmenter = None
_ner_tagger = None


def _init_natasha():
    global _natasha_ready, _segmenter, _ner_tagger
    if _natasha_ready:
        return True
    try:
        from natasha import Segmenter, NewsEmbedding, NewsNERTagger
        emb = NewsEmbedding()
        _segmenter = Segmenter()
        _ner_tagger = NewsNERTagger(emb)
        _natasha_ready = True
        return True
    except Exception as e:
        log.warning("Natasha init failed: %s", e)
        return False


def _normalize_name(name: str) -> str:
    return NAME_NORMALIZE_RE.sub(' ', name.strip()).strip(' .,')


def _extract_with_natasha(text: str) -> List[Dict]:
    if not _init_natasha():
        return []
    try:
        from natasha import Doc
        doc = Doc(text)
        doc.segment(_segmenter)
        doc.tag_ner(_ner_tagger)
        results = []
        for span in doc.ner.spans:
            etype = NATASHA_TYPE_MAP.get(span.type, span.type.lower())
            name = _normalize_name(text[span.start:span.stop])
            if name and len(name) > 1:
                results.append({
                    "entity_type": etype,
                    "name": name,
                    "start": span.start,
                    "end": span.stop,
                    "source": "natasha",
                })
        return results
    except Exception as e:
        log.warning("Natasha NER failed: %s", e)
        return []


def _extract_regex_entities(text: str) -> List[Dict]:
    results = []
    for m in INN_RE.finditer(text):
        val = m.group(1)
        if len(val) in (10, 12):
            results.append({"entity_type": "inn", "name": val, "start": m.start(), "end": m.end(), "source": "regex"})
    for m in OGRN_RE.finditer(text):
        val = m.group(1)
        if len(val) in (13, 15):
            results.append({"entity_type": "ogrn", "name": val, "start": m.start(), "end": m.end(), "source": "regex"})
    for m in CASE_NUM_RE.finditer(text):
        results.append({"entity_type": "case_number", "name": m.group(1), "start": m.start(), "end": m.end(), "source": "regex"})
    return results


def extract_entities(text: str) -> List[Dict]:
    if not text or len(text.strip()) < 3:
        return []
    entities = _extract_with_natasha(text)
    regex_ents = _extract_regex_entities(text)
    seen_names = {(e["entity_type"], e["name"]) for e in entities}
    for e in regex_ents:
        key = (e["entity_type"], e["name"])
        if key not in seen_names:
            entities.append(e)
            seen_names.add(key)
    return entities


def _get_or_create_entity(conn: sqlite3.Connection, entity_type: str, canonical_name: str, extra: dict = None) -> int:
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type=? AND canonical_name=?",
        (entity_type, canonical_name),
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO entities(entity_type, canonical_name, inn, ogrn, description, extra_data) VALUES(?,?,?,?,?,?)",
        (
            entity_type,
            canonical_name,
            extra.get("inn") if extra else None,
            extra.get("ogrn") if extra else None,
            extra.get("description") if extra else None,
            json.dumps(extra, ensure_ascii=False) if extra else None,
        ),
    )
    return cur.lastrowid


def _add_alias(conn: sqlite3.Connection, entity_id: int, alias: str, alias_type: str = "spelling"):
    try:
        conn.execute(
            "INSERT OR IGNORE INTO entity_aliases(entity_id, alias, alias_type) VALUES(?,?,?)",
            (entity_id, alias, alias_type),
        )
    except Exception:
        pass


def _resolve_entity(conn: sqlite3.Connection, entity_type: str, name: str) -> Optional[int]:
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type=? AND canonical_name=?",
        (entity_type, name),
    ).fetchone()
    if row:
        return row[0]

    alias_row = conn.execute(
        "SELECT entity_id FROM entity_aliases WHERE alias=? LIMIT 1",
        (name,),
    ).fetchone()
    if alias_row:
        return alias_row[0]

    if entity_type == "person" and len(name.split()) >= 2:
        surname = name.split()[0]
        row2 = conn.execute(
            "SELECT id, canonical_name FROM entities WHERE entity_type='person' AND canonical_name LIKE ? LIMIT 5",
            (surname + "%",),
        ).fetchall()
        for r in row2:
            existing_parts = r[1].split()
            new_parts = name.split()
            if existing_parts[0] == new_parts[0]:
                return r[0]

    return None


def _deduplicate_entities(entities: List[Dict]) -> List[Dict]:
    seen = {}
    for e in entities:
        key = (e["entity_type"], e["name"].lower())
        if key not in seen:
            seen[key] = e
    return list(seen.values())


def process_content_entities(settings: dict = None, batch_size: int = 500):
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    rows = conn.execute(
        """
        SELECT c.id, c.body_text, c.title
        FROM content_items c
        WHERE (length(c.body_text) > 10 OR length(c.title) > 5)
          AND c.ner_processed = 0
        ORDER BY c.id
        LIMIT ?
        """,
        (batch_size,),
    ).fetchall()

    if not rows:
        log.info("No new content items for NER processing")
        conn.close()
        return

    log.info("Processing %d content items for entities", len(rows))

    total_entities = 0
    total_mentions = 0
    entity_cache: Dict[Tuple[str, str], int] = {}

    for row in rows:
        content_id = row["id"]
        text = f"{row['title'] or ''}\n{row['body_text'] or ''}"
        if not text.strip():
            conn.execute("UPDATE content_items SET ner_processed=1 WHERE id=?", (content_id,))
            continue

        entities = extract_entities(text)
        entities = _deduplicate_entities(entities)

        for ent in entities:
            etype = ent["entity_type"]
            name = ent["name"]

            cache_key = (etype, name)
            if cache_key in entity_cache:
                entity_id = entity_cache[cache_key]
            else:
                entity_id = _resolve_entity(conn, etype, name)
                if entity_id is None:
                    entity_id = _get_or_create_entity(conn, etype, name)
                    if etype == "person" and len(name.split()) >= 2:
                        short = name.split()[0]
                        if short != name:
                            _add_alias(conn, entity_id, short, "surname_only")
                entity_cache[cache_key] = entity_id
                total_entities += 1

            try:
                conn.execute(
                    "INSERT OR IGNORE INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES(?,?,?,?)",
                    (entity_id, content_id, etype, 1.0 if ent["source"] == "natasha" else 0.7),
                )
                total_mentions += 1
            except Exception:
                pass

        conn.execute("UPDATE content_items SET ner_processed=1 WHERE id=?", (content_id,))

    conn.commit()

    stats = {
        "entities_total": conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0],
        "persons": conn.execute("SELECT COUNT(*) FROM entities WHERE entity_type='person'").fetchone()[0],
        "organizations": conn.execute("SELECT COUNT(*) FROM entities WHERE entity_type='organization'").fetchone()[0],
        "locations": conn.execute("SELECT COUNT(*) FROM entities WHERE entity_type='location'").fetchone()[0],
        "mentions_total": conn.execute("SELECT COUNT(*) FROM entity_mentions").fetchone()[0],
    }
    log.info(
        "NER done: %d entities (%d persons, %d orgs, %d locs), %d mentions",
        stats["entities_total"], stats["persons"], stats["organizations"], stats["locations"], stats["mentions_total"],
    )

    conn.close()
    return stats


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    process_content_entities()


if __name__ == "__main__":
    main()
