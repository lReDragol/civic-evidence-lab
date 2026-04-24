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
    "strong": 10,
    "moderate": 3,
    "weak": 1,
}


def _strength_from_count(count: int) -> str:
    if count >= CO_OCCURRENCE_THRESHOLDS["strong"]:
        return "strong"
    elif count >= CO_OCCURRENCE_THRESHOLDS["moderate"]:
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

    co_occurrence = defaultdict(int)

    offset = 0
    processed = 0
    while True:
        rows = conn.execute(
            """
            SELECT content_item_id, entity_id
            FROM entity_mentions
            ORDER BY content_item_id
            LIMIT ? OFFSET ?
            """,
            (batch_size, offset),
        ).fetchall()

        if not rows:
            break

        item_entities = defaultdict(set)
        for r in rows:
            item_entities[r[0]].add(r[1])

        for cid, eids in item_entities.items():
            if len(eids) < 2:
                continue
            sorted_eids = sorted(eids)
            for i in range(len(sorted_eids)):
                for j in range(i + 1, len(sorted_eids)):
                    pair = (sorted_eids[i], sorted_eids[j])
                    co_occurrence[pair] += 1

        offset += batch_size
        processed += len(rows)

    log.info("Computed %d co-occurrence pairs", len(co_occurrence))

    inserted = 0
    for (e1_id, e2_id), count in co_occurrence.items():
        if count < 2:
            continue

        e1 = conn.execute("SELECT entity_type FROM entities WHERE id = ?", (e1_id,)).fetchone()
        e2 = conn.execute("SELECT entity_type FROM entities WHERE id = ?", (e2_id,)).fetchone()
        if not e1 or not e2:
            continue

        e1_type = e1[0]
        e2_type = e2[0]

        rel_type = _infer_relation_type(e1_type, e2_type, count)
        strength = _strength_from_count(count)

        existing = conn.execute(
            "SELECT id FROM entity_relations WHERE from_entity_id = ? AND to_entity_id = ? AND relation_type = ?",
            (e1_id, e2_id, rel_type),
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE entity_relations SET strength = ?, detected_by = ? WHERE id = ?",
                (strength, f"co_occurrence:{count}", existing[0]),
            )
        else:
            conn.execute(
                """INSERT INTO entity_relations(from_entity_id, to_entity_id, relation_type, strength, detected_by)
                   VALUES(?,?,?,?,?)""",
                (e1_id, e2_id, rel_type, strength, f"co_occurrence:{count}"),
            )
        inserted += 1

        if count >= 3:
            existing_rev = conn.execute(
                "SELECT id FROM entity_relations WHERE from_entity_id = ? AND to_entity_id = ? AND relation_type = ?",
                (e2_id, e1_id, rel_type),
            ).fetchone()
            if not existing_rev:
                conn.execute(
                    """INSERT INTO entity_relations(from_entity_id, to_entity_id, relation_type, strength, detected_by)
                       VALUES(?,?,?,?,?)""",
                    (e2_id, e1_id, rel_type, strength, f"co_occurrence:{count}"),
                )

        if inserted % 500 == 0:
            conn.commit()

    conn.commit()

    top_rels = conn.execute(
        """
        SELECT er.relation_type, er.strength, er.detected_by,
               e1.canonical_name, e1.entity_type,
               e2.canonical_name, e2.entity_type
        FROM entity_relations er
        JOIN entities e1 ON e1.id = er.from_entity_id
        JOIN entities e2 ON e2.id = er.to_entity_id
        WHERE er.strength = 'strong'
        ORDER BY er.id DESC
        LIMIT 20
        """
    ).fetchall()

    with open(Path(settings.get("project_root", str(Path(__file__).resolve().parent.parent))) / "_tmp_rels.txt", "w", encoding="utf-8") as f:
        for r in top_rels:
            f.write(f"{r[0]} [{r[1]}] {r[3]} ({r[4]}) <-> {r[5]} ({r[6]}) [{r[2]}]\n")

    stats = {
        "co_occurrence_pairs": len(co_occurrence),
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
