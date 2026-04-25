import json
import logging
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

PERSON_ORG_RELATIONS = {
    ("person", "organization"): [
        "mentioned_as_member",
        "mentioned_as_head",
        "mentioned_as_founder",
        "mentioned_together",
    ],
    ("organization", "person"): [
        "mentioned_as_member",
        "mentioned_as_head",
        "mentioned_as_founder",
        "mentioned_together",
    ],
}

PERSON_PERSON_RELATIONS = [
    "mentioned_together",
    "mentioned_as_ally",
    "mentioned_as_opponent",
    "mentioned_as_colleague",
]

PERSON_LOC_RELATIONS = {
    ("person", "location"): ["associated_with_location"],
    ("location", "person"): ["associated_with_location"],
}

ORG_ORG_RELATIONS = ["mentioned_together", "subsidiary", "parent_org"]

CO_OCCURRENCE_THRESHOLDS = {
    "min_items": 3,
    "min_sources": 2,
    "strong_items": 8,
    "strong_sources": 3,
    "moderate_items": 5,
    "moderate_sources": 2,
}
CO_OCCURRENCE_RELATION_TYPES = ("mentioned_together", "associated_with_location", "located_in")


def _strength_from_support(item_count: int, source_count: int) -> str:
    if item_count >= CO_OCCURRENCE_THRESHOLDS["strong_items"] and source_count >= CO_OCCURRENCE_THRESHOLDS["strong_sources"]:
        return "strong"
    if item_count >= CO_OCCURRENCE_THRESHOLDS["moderate_items"] and source_count >= CO_OCCURRENCE_THRESHOLDS["moderate_sources"]:
        return "moderate"
    return "weak"


def _infer_relation_type(e1_type: str, e2_type: str, count: int) -> str:
    if e1_type == "person" and e2_type == "person":
        return "mentioned_together"
    if e1_type == "person" and e2_type == "organization":
        return "mentioned_together"
    if e1_type == "organization" and e2_type == "person":
        return "mentioned_together"
    if e1_type == "person" and e2_type == "location":
        return "associated_with_location"
    if e1_type == "location" and e2_type == "person":
        return "associated_with_location"
    if e1_type == "organization" and e2_type == "organization":
        return "mentioned_together"
    if e1_type == "organization" and e2_type == "location":
        return "located_in"
    if e1_type == "location" and e2_type == "location":
        return "mentioned_together"
    return "mentioned_together"


def extract_co_occurrence_relations(settings: dict = None, batch_size: int = 5000) -> Dict:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    total_content = conn.execute(
        "SELECT COUNT(*) FROM (SELECT content_item_id FROM entity_mentions GROUP BY content_item_id HAVING COUNT(DISTINCT entity_id) >= 2)"
    ).fetchone()[0]
    log.info("Extracting co-occurrence relations from %d content items", total_content)

    conn.execute(
        """
        DELETE FROM entity_relations
        WHERE relation_type IN ({placeholders})
          AND COALESCE(detected_by, '') LIKE 'co_occurrence:%'
        """.format(placeholders=",".join("?" * len(CO_OCCURRENCE_RELATION_TYPES))),
        CO_OCCURRENCE_RELATION_TYPES,
    )

    pair_rows = conn.execute(
        """
        WITH pair_support AS (
            SELECT
                CASE WHEN em1.entity_id < em2.entity_id THEN em1.entity_id ELSE em2.entity_id END AS entity_a,
                CASE WHEN em1.entity_id < em2.entity_id THEN em2.entity_id ELSE em1.entity_id END AS entity_b,
                COUNT(DISTINCT em1.content_item_id) AS item_count,
                COUNT(DISTINCT ci.source_id) AS source_count
            FROM entity_mentions em1
            JOIN entity_mentions em2
              ON em1.content_item_id = em2.content_item_id
             AND em1.entity_id < em2.entity_id
            JOIN content_items ci ON ci.id = em1.content_item_id
            GROUP BY entity_a, entity_b
            HAVING item_count >= ? AND source_count >= ?
        )
        SELECT ps.entity_a, ps.entity_b, ps.item_count, ps.source_count,
               e1.entity_type AS e1_type, e2.entity_type AS e2_type
        FROM pair_support ps
        JOIN entities e1 ON e1.id = ps.entity_a
        JOIN entities e2 ON e2.id = ps.entity_b
        ORDER BY ps.source_count DESC, ps.item_count DESC, ps.entity_a, ps.entity_b
        """,
        (
            CO_OCCURRENCE_THRESHOLDS["min_items"],
            CO_OCCURRENCE_THRESHOLDS["min_sources"],
        ),
    ).fetchall()

    inserted = 0
    for row in pair_rows:
        e1_id, e2_id, item_count, source_count, e1_type, e2_type = row
        rel_type = _infer_relation_type(e1_type, e2_type, item_count)
        strength = _strength_from_support(item_count, source_count)
        conn.execute(
            """INSERT INTO entity_relations(from_entity_id, to_entity_id, relation_type, strength, detected_by)
               VALUES(?,?,?,?,?)""",
            (
                e1_id,
                e2_id,
                rel_type,
                strength,
                f"co_occurrence:items={item_count}:sources={source_count}",
            ),
        )
        inserted += 1

        if inserted % 500 == 0:
            conn.commit()

    conn.commit()

    stats = {
        "co_occurrence_pairs": len(pair_rows),
        "relations_inserted": inserted,
        "strong_relations": conn.execute("SELECT COUNT(*) FROM entity_relations WHERE strength = 'strong'").fetchone()[0],
        "moderate_relations": conn.execute("SELECT COUNT(*) FROM entity_relations WHERE strength = 'moderate'").fetchone()[0],
        "weak_relations": conn.execute("SELECT COUNT(*) FROM entity_relations WHERE strength = 'weak'").fetchone()[0],
    }

    log.info("Relations extracted: %s", stats)

    conn.close()
    return stats


def extract_head_role_relations(settings: dict = None) -> Dict:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    head_patterns = [
        (r'\b(?:глав|руководит|директор|председател|замглав|зам\.?пред|премьер[- ]?министр)\b.*\b(\w+ова\b|\w+ев\b|\w+ёв\b|\w+ин\b|\w+ский\b|\w+цкая\b|\w+ая\b)', 'head_of'),
        (r'\b(\w+ова\b|\w+ев\b|\w+ёв\b|\w+ин\b|\w+ский\b|\w+цкая\b|\w+ая\b).*(?:глав|руководит|директор|председател|замглав|премьер[- ]?министр)\b', 'head_of'),
        (r'\b(?:депутат|сенатор|министр|губернатор|мэр|полпред|посол|член.*совет)\b.*\b(\w+ова\b|\w+ев\b|\w+ёв\b|\w+ин\b|\w+ский\b)', 'member_of'),
        (r'\b(?:основател|учредител|владел|собственник)\b.*\b(\w+ова\b|\w+ев\b|\w+ёв\b|\w+ин\b|\w+ский\b)', 'founder_of'),
    ]

    org_patterns = [
        r'\b(?:ГД|Государственн.*Дум|Совет.*Федераци|Правительств|Министерств|ФСБ|ФСО|Рос(?:сийск|гвард|атом|нефт|нано)|Сбер|Газпром|Лукойл|Ростех|Роскосмос|РЖД|ВТБ|Един.*Росс|КПРФ|ЛДПР|Справедлив|Нов.*люд|ЦИК|Счетн.*палат|Следственн.*комитет|Прокуратур|Администрац|Кремл|Дум[аеуы])\b',
    ]

    log.info("Extracting head/role relations from content")

    rows = conn.execute(
        """
        SELECT c.id, c.body_text, c.title
        FROM content_items c
        WHERE length(c.body_text) > 50
        LIMIT 20000
        """
    ).fetchall()

    inserted = 0
    for row in rows:
        text = f"{row[1] or ''} {row[2] or ''}"

        org_matches = []
        for pat in org_patterns:
            for m in re.finditer(pat, text, re.I):
                org_name = m.group(0).strip()
                org_entity = conn.execute(
                    "SELECT id FROM entities WHERE entity_type = 'organization' AND canonical_name LIKE ? LIMIT 1",
                    (f"%{org_name[:15]}%",),
                ).fetchone()
                if org_entity:
                    org_matches.append(org_entity[0])

        if not org_matches:
            continue

        for pat, rel_type in head_patterns:
            for m in re.finditer(pat, text, re.I):
                person_name = m.group(1) if m.lastindex else ""
                if not person_name:
                    continue
                person_entity = conn.execute(
                    "SELECT id FROM entities WHERE entity_type = 'person' AND canonical_name LIKE ? LIMIT 1",
                    (f"{person_name[:6]}%",),
                ).fetchone()
                if not person_entity:
                    continue

                for org_id in org_matches:
                    existing = conn.execute(
                        "SELECT id FROM entity_relations WHERE from_entity_id = ? AND to_entity_id = ? AND relation_type = ?",
                        (person_entity[0], org_id, rel_type),
                    ).fetchone()
                    if not existing:
                        conn.execute(
                            """INSERT INTO entity_relations(from_entity_id, to_entity_id, relation_type, strength, detected_by, evidence_item_id)
                               VALUES(?,?,?,?,?,?)""",
                            (person_entity[0], org_id, rel_type, "moderate", f"regex:{pat[:30]}", row[0]),
                        )
                        inserted += 1

    conn.commit()

    log.info("Head/role relations: %d inserted", inserted)

    stats = {"role_relations_inserted": inserted}
    conn.close()
    return stats


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--co-occurrence", action="store_true", help="Extract co-occurrence relations")
    parser.add_argument("--roles", action="store_true", help="Extract head/role relations")
    parser.add_argument("--all", action="store_true", help="Run all extraction methods")
    args = parser.parse_args()

    if args.all or args.co_occurrence:
        result1 = extract_co_occurrence_relations()
        print(json.dumps(result1, ensure_ascii=False))

    if args.all or args.roles:
        result2 = extract_head_role_relations()
        print(json.dumps(result2, ensure_ascii=False))


if __name__ == "__main__":
    main()
