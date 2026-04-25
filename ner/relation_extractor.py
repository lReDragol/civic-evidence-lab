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
from graph.relation_candidates import rebuild_and_promote_relation_candidates

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
    result = rebuild_and_promote_relation_candidates(settings or load_settings())
    log.info("Relation candidates rebuilt: %s", result)
    return {
        "co_occurrence_pairs": int(result.get("relation_candidates_created", 0)),
        "relations_inserted": int(result.get("promoted_relations", 0)),
        "strong_relations": 0,
        "moderate_relations": 0,
        "weak_relations": int(result.get("relation_candidates_created", 0)),
        **result,
    }


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
