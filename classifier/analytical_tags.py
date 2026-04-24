import json
import logging
import re
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

L4_PATTERNS = [
    {
        "tag": "коррупционная_схема",
        "description": "Коррупционная схема: пересечение госзакупок, коррупции и конфликта интересов",
        "required_tags": {
            "L1": ["procurement", "corruption"],
            "L3": ["possible_corruption", "possible_conflict_of_interest"],
        },
        "min_matches": 2,
        "any_level": ["procurement", "corruption", "possible_corruption", "possible_conflict_of_interest",
                       "ownership", "abuse_claim"],
    },
    {
        "tag": "репрессии_оппозиции",
        "description": "Репрессии оппозиции: задержания + права человека + необходимость проверки",
        "required_tags": {
            "L1": ["detention"],
            "L2": ["human_rights"],
        },
        "min_matches": 2,
        "any_level": ["detention", "human_rights", "censorship", "foreign_agent", "needs_verification"],
    },
    {
        "tag": "цензура_интернет",
        "description": "Цензура в интернете: блокировки + РКН + цензура",
        "required_tags": {
            "L1": ["censorship"],
        },
        "keyword_hints": ["ркн", "роскомнадзор", "блокиров", "запрещ", "экстремист", "интернет", "впн"],
        "min_matches": 1,
        "any_level": ["censorship", "foreign_agent", "extremism"],
    },
    {
        "tag": "аффилированность_чиновник",
        "description": "Аффилированность чиновника: собственность + госзакупки + конфликт интересов",
        "required_tags": {
            "L1": ["ownership", "procurement"],
        },
        "min_matches": 2,
        "any_level": ["ownership", "procurement", "possible_conflict_of_interest", "corruption",
                       "abuse_claim", "court_decision"],
    },
    {
        "tag": "вотум_доверия",
        "description": "Вотум доверия: голосование + Дума + политическое решение",
        "required_tags": {
            "L1": ["vote_record"],
            "L2": ["duma"],
        },
        "min_matches": 2,
        "any_level": ["vote_record", "duma", "legislative", "public_statement"],
    },
    {
        "tag": "мобилизация_права",
        "description": "Мобилизация и права: мобилизация + права человека + протесты",
        "required_tags": {
            "L1": ["mobilization_claim"],
        },
        "min_matches": 1,
        "any_level": ["mobilization_claim", "detention", "human_rights", "censorship", "protest"],
    },
    {
        "tag": "иностранный_агент",
        "description": "Иностранный агент: реестр + СМИ + иноагент",
        "required_tags": {
            "L1": ["foreign_agent"],
        },
        "min_matches": 1,
        "keyword_hints": ["иноагент", "иностранный агент", "реестр", "минюст", "смк", "сми"],
        "any_level": ["foreign_agent", "censorship", "registry"],
    },
    {
        "tag": "судебное_решение",
        "description": "Судебное решение: суд + приговор + статья УК",
        "required_tags": {
            "L1": ["court_decision"],
        },
        "min_matches": 1,
        "any_level": ["court_decision", "corruption_claim", "abuse_claim", "detention"],
    },
]


def _get_content_tags(conn, content_item_id: int) -> Dict:
    tags = {"L0": [], "L1": [], "L2": [], "L3": [], "all": set()}
    rows = conn.execute(
        "SELECT tag_level, tag_name FROM content_tags WHERE content_item_id=?",
        (content_item_id,),
    ).fetchall()
    for r in rows:
        level_key = f"L{r[0]}"
        tags[level_key].append(r[1])
        tags["all"].add(r[1])
    return tags


def _keyword_hints_match(text: str, hints: List[str]) -> bool:
    if not text or not hints:
        return True
    text_lower = text.lower()
    return any(h in text_lower for h in hints)


def compute_l4_tags(conn, content_item_id: int, body_text: str = None) -> List[Dict]:
    tags = _get_content_tags(conn, content_item_id)
    all_tags = tags["all"]
    result = []

    for pattern in L4_PATTERNS:
        match_count = 0
        for tag_name in pattern["any_level"]:
            if tag_name in all_tags:
                match_count += 1

        if match_count < pattern["min_matches"]:
            continue

        if pattern.get("keyword_hints"):
            text = body_text or ""
            if not text:
                ci = conn.execute(
                    "SELECT body_text, title FROM content_items WHERE id=?",
                    (content_item_id,),
                ).fetchone()
                if ci:
                    text = (ci["title"] or "") + " " + (ci["body_text"] or "")
            if not _keyword_hints_match(text, pattern["keyword_hints"]):
                continue

        required_match = True
        for level, required in pattern.get("required_tags", {}).items():
            if not any(t in all_tags for t in required):
                required_match = False
                break
        if not required_match:
            continue

        confidence = min(1.0, match_count / (len(pattern["any_level"]) * 0.5))
        result.append({
            "tag_name": pattern["tag"],
            "confidence": round(confidence, 2),
            "description": pattern["description"],
            "matched_count": match_count,
            "tag_source": "derived",
        })

    return result


def store_l4_tags(conn, content_item_id: int, l4_tags: List[Dict]) -> int:
    stored = 0
    for t in l4_tags:
        try:
            existing = conn.execute(
                "SELECT id FROM content_tags WHERE content_item_id=? AND tag_level=4 AND tag_name=?",
                (content_item_id, t["tag_name"]),
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE content_tags SET confidence=?, tag_source=? WHERE id=?",
                    (t["confidence"], t["tag_source"], existing[0]),
                )
            else:
                conn.execute(
                    "INSERT INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source) "
                    "VALUES(?,4,?,?,?)",
                    (content_item_id, t["tag_name"], t["confidence"], t["tag_source"]),
                )
            stored += 1
        except Exception as e:
            log.warning("Failed to store L4 tag %s for ci %d: %s", t["tag_name"], content_item_id, e)
    return stored


def compute_l4_tags_batch(conn, limit: int = 500) -> Dict:
    stats = {"processed": 0, "tags_added": 0, "items_with_l4": 0}

    content_items = conn.execute(
        """
        SELECT ci.id, ci.body_text, ci.title
        FROM content_items ci
        WHERE NOT EXISTS (
            SELECT 1 FROM content_tags ct WHERE ct.content_item_id = ci.id AND ct.tag_level = 4
        )
        AND (
            SELECT COUNT(DISTINCT ct.tag_name) FROM content_tags ct
            WHERE ct.content_item_id = ci.id AND ct.tag_level IN (1, 2)
        ) >= 2
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    for ci in content_items:
        l4_tags = compute_l4_tags(conn, ci["id"], ci["body_text"])
        if l4_tags:
            stored = store_l4_tags(conn, ci["id"], l4_tags)
            stats["tags_added"] += stored
            stats["items_with_l4"] += 1
        stats["processed"] += 1

    return stats


def compute_l4_for_all(conn) -> Dict:
    stats = {"processed": 0, "tags_added": 0, "items_with_l4": 0}

    total = conn.execute(
        """
        SELECT COUNT(DISTINCT ci.id)
        FROM content_items ci
        JOIN content_tags ct ON ct.content_item_id = ci.id
        WHERE ct.tag_level IN (1, 2)
        """
    ).fetchone()[0]

    log.info("Computing L4 tags for %d content items with L1/L2 tags", total)

    batch_size = 500
    offset = 0
    while True:
        batch_stats = compute_l4_tags_batch(conn, limit=batch_size)
        stats["processed"] += batch_stats["processed"]
        stats["tags_added"] += batch_stats["tags_added"]
        stats["items_with_l4"] += batch_stats["items_with_l4"]

        if batch_stats["processed"] < batch_size:
            break
        offset += batch_size

        conn.commit()
        log.info("L4 progress: %d processed, %d tags added", stats["processed"], stats["tags_added"])

    conn.commit()
    return stats


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Compute L4 analytical tags")
    parser.add_argument("--content-id", type=int, help="Specific content item ID")
    parser.add_argument("--all", action="store_true", help="Compute for all content items")
    parser.add_argument("--batch", type=int, default=500, help="Batch size")
    args = parser.parse_args()

    settings = load_settings()
    conn = get_db(settings)

    if args.content_id:
        tags = compute_l4_tags(conn, args.content_id)
        stored = store_l4_tags(conn, args.content_id, tags)
        conn.commit()
        print(json.dumps({"tags": tags, "stored": stored}, ensure_ascii=False, indent=2))
    elif args.all:
        stats = compute_l4_for_all(conn)
        print(json.dumps(stats, ensure_ascii=False, indent=2))
    else:
        stats = compute_l4_tags_batch(conn, limit=args.batch)
        conn.commit()
        print(json.dumps(stats, ensure_ascii=False, indent=2))

    conn.close()


if __name__ == "__main__":
    main()
