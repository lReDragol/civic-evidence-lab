import json
import logging
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)


def _ensure_entity(conn, entity_type: str, name: str, description: str = "") -> int:
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type=? AND canonical_name=?",
        (entity_type, name),
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO entities(entity_type, canonical_name, description) VALUES(?,?,?)",
        (entity_type, name, description),
    )
    return cur.lastrowid


def _ensure_relation(conn, from_id: int, to_id: int, rel_type: str,
                     strength: str = "moderate", detected_by: str = "structural",
                     evidence_item_id: int = None) -> bool:
    existing = conn.execute(
        "SELECT id FROM entity_relations WHERE from_entity_id=? AND to_entity_id=? AND relation_type=?",
        (from_id, to_id, rel_type),
    ).fetchone()
    if existing:
        return False
    conn.execute(
        "INSERT INTO entity_relations(from_entity_id, to_entity_id, relation_type, "
        "evidence_item_id, strength, detected_by) VALUES(?,?,?,?,?,?)",
        (from_id, to_id, rel_type, evidence_item_id, strength, detected_by),
    )
    return True


def build_senator_region_relations(conn) -> int:
    created = 0
    rows = conn.execute(
        "SELECT dp.entity_id, dp.region FROM deputy_profiles dp "
        "JOIN official_positions op ON dp.entity_id = op.entity_id "
        "WHERE op.organization = 'Совет Федерации' AND dp.region IS NOT NULL AND dp.region != ''"
    ).fetchall()
    for entity_id, region_name in rows:
        region_eid = _ensure_entity(conn, "region", region_name, f"Субъект РФ: {region_name}")
        if _ensure_relation(conn, entity_id, region_eid, "represents_region", "strong", "council_gov_ru"):
            created += 1
    return created


def build_deputy_committee_relations(conn) -> int:
    created = 0
    rows = conn.execute(
        "SELECT dp.entity_id, dp.committee FROM deputy_profiles dp "
        "WHERE dp.committee IS NOT NULL AND dp.committee != ''"
    ).fetchall()
    for entity_id, committee_name in rows:
        com_eid = _ensure_entity(conn, "committee", committee_name, f"Комитет: {committee_name}")
        if _ensure_relation(conn, entity_id, com_eid, "member_of_committee", "strong", "deputy_profiles"):
            created += 1
    return created


def build_minjust_entity_relations(conn) -> int:
    created = 0
    rows = conn.execute(
        "SELECT ci.id, ci.title, ci.body_text, s.category FROM content_items ci "
        "JOIN sources s ON ci.source_id = s.id "
        "WHERE s.category = 'official_registry' AND ci.content_type = 'registry_entry'"
    ).fetchall()
    for ci_id, title, body, category in rows:
        if not title:
            continue
        entity_name = title.strip()
        entity_type = "person"
        if any(kw in entity_name.lower() for kw in ["ооо", "зао", "оао", "публичн", "компани", "фонд", "ассоциац"]):
            entity_type = "organization"
        elif any(kw in entity_name.lower() for kw in ["средств", "информаци", "материал"]):
            entity_type = "material"

        eid = _ensure_entity(conn, entity_type, entity_name, f"Реестр Минюста: {category}")
        if _ensure_relation(conn, eid, eid, "in_registry", "strong", "minjust_registry", ci_id):
            pass

        if body:
            inn_m = re.search(r"ИНН[:\s]*(\d{10,12})", body)
            if inn_m:
                inn = inn_m.group(1)
                conn.execute("UPDATE entities SET inn=? WHERE id=?", (inn, eid))

    return created


def build_zakupki_contract_relations(conn) -> int:
    created = 0
    rows = conn.execute(
        "SELECT ci.id, ci.title, ci.body_text FROM content_items ci "
        "JOIN sources s ON ci.source_id = s.id "
        "WHERE s.category = 'official_registry' AND ci.content_type = 'contract_entry'"
    ).fetchall()
    for ci_id, title, body in rows:
        if not body:
            continue

        customer = ""
        contractor = ""
        for m in re.finditer(r"Заказчик[:\s]*(.*?)(?:\n|$)", body):
            customer = m.group(1).strip()[:200]
        for m in re.finditer(r"Исполнитель[:\s]*(.*?)(?:\n|$)", body):
            contractor = m.group(1).strip()[:200]

        if customer and contractor:
            cust_eid = _ensure_entity(conn, "organization", customer, f"Заказчик: {customer}")
            cont_eid = _ensure_entity(conn, "organization", contractor, f"Исполнитель: {contractor}")
            if _ensure_relation(conn, cont_eid, cust_eid, "contracted_by", "strong", "zakupki_gov_ru", ci_id):
                created += 1

        inn_m = re.search(r"ИНН[:\s]*(\d{10,12})", body)
        if inn_m and contractor:
            cont_eid = _ensure_entity(conn, "organization", contractor)
            conn.execute("UPDATE entities SET inn=? WHERE id=?", (inn_m.group(1), cont_eid))

    return created


def build_kremlin_act_relations(conn) -> int:
    created = 0
    rows = conn.execute(
        "SELECT ci.id, ci.title, ci.body_text FROM content_items ci "
        "JOIN sources s ON ci.source_id = s.id "
        "WHERE s.category = 'official_site' AND ci.content_type = 'executive_act'"
    ).fetchall()
    for ci_id, title, body in rows:
        if not title:
            continue

        for m in re.finditer(r"Указ\s*Президента\s*РФ[^№]*№\s*([\d\-]+)", title + " " + (body or "")):
            decree_num = m.group(1)
            decree_eid = _ensure_entity(conn, "decree", f"Указ Президента РФ №{decree_num}")
            pres_eid = _ensure_entity(conn, "person", "Президент РФ", "Должность: Президент Российской Федерации")
            if _ensure_relation(conn, pres_eid, decree_eid, "signed_decree", "strong", "kremlin_ru", ci_id):
                created += 1

        for m in re.finditer(r"Постановление\s*Правительства[^№]*№\s*([\d\-]+)", title + " " + (body or "")):
            res_num = m.group(1)
            res_eid = _ensure_entity(conn, "resolution", f"Постановление Правительства РФ №{res_num}")
            gov_eid = _ensure_entity(conn, "organization", "Правительство РФ", "Правительство Российской Федерации")
            if _ensure_relation(conn, gov_eid, res_eid, "issued_resolution", "strong", "government_ru", ci_id):
                created += 1

    return created


def build_co_occurrence_from_mentions(conn, min_co_occurrence: int = 2) -> int:
    created = 0
    pairs = conn.execute(
        """
        SELECT em1.entity_id, em2.entity_id, COUNT(*) as co_count
        FROM entity_mentions em1
        JOIN entity_mentions em2 ON em1.content_item_id = em2.content_item_id
        WHERE em1.entity_id < em2.entity_id
        GROUP BY em1.entity_id, em2.entity_id
        HAVING co_count >= ?
        """,
        (min_co_occurrence,),
    ).fetchall()

    for e1, e2, count in pairs:
        strength = "strong" if count >= 5 else "moderate" if count >= 3 else "weak"
        detected = f"co_occurrence:{count}"
        if _ensure_relation(conn, e1, e2, "mentioned_together", strength, detected):
            created += 1

    return created


def build_structural_relations(conn) -> Dict:
    stats = {}

    log.info("Building senator → region relations...")
    stats["senator_regions"] = build_senator_region_relations(conn)

    log.info("Building deputy → committee relations...")
    stats["deputy_committees"] = build_deputy_committee_relations(conn)

    log.info("Building Минюст entity relations...")
    stats["minjust_relations"] = build_minjust_entity_relations(conn)

    log.info("Building закупки contract relations...")
    stats["zakupki_relations"] = build_zakupki_contract_relations(conn)

    log.info("Building Kremlin/ Government act relations...")
    stats["kremlin_relations"] = build_kremlin_act_relations(conn)

    log.info("Building co-occurrence relations from mentions...")
    stats["co_occurrence"] = build_co_occurrence_from_mentions(conn, min_co_occurrence=2)

    conn.commit()
    return stats


def get_involvement_map(conn, entity_id: int) -> Dict:
    result = {"entity_id": entity_id, "relations": {}, "stats": {}}

    entity = conn.execute("SELECT * FROM entities WHERE id=?", (entity_id,)).fetchone()
    if not entity:
        return result

    result["entity"] = {"type": entity["entity_type"], "name": entity["canonical_name"]}

    relations = {}
    for row in conn.execute(
        "SELECT er.relation_type, er.from_entity_id, er.to_entity_id, er.strength, er.detected_by, "
        "e_from.canonical_name as from_name, e_from.entity_type as from_type, "
        "e_to.canonical_name as to_name, e_to.entity_type as to_type "
        "FROM entity_relations er "
        "JOIN entities e_from ON e_from.id = er.from_entity_id "
        "JOIN entities e_to ON e_to.id = er.to_entity_id "
        "WHERE er.from_entity_id=? OR er.to_entity_id=?",
        (entity_id, entity_id),
    ).fetchall():
        rel_type = row["relation_type"]
        if rel_type not in relations:
            relations[rel_type] = []
        direction = "outgoing" if row["from_entity_id"] == entity_id else "incoming"
        other_name = row["to_name"] if direction == "outgoing" else row["from_name"]
        other_type = row["to_type"] if direction == "outgoing" else row["from_type"]
        relations[rel_type].append({
            "direction": direction,
            "entity_name": other_name,
            "entity_type": other_type,
            "strength": row["strength"],
            "detected_by": row["detected_by"],
        })

    result["relations"] = relations

    result["stats"]["official_positions"] = conn.execute(
        "SELECT position_title, organization, region, faction, is_active FROM official_positions WHERE entity_id=?",
        (entity_id,),
    ).fetchall()
    result["stats"]["party_memberships"] = conn.execute(
        "SELECT party_name, role, is_current FROM party_memberships WHERE entity_id=?",
        (entity_id,),
    ).fetchall()
    result["stats"]["bills_sponsored"] = conn.execute(
        "SELECT b.number, b.title, b.status FROM bill_sponsors bs "
        "JOIN bills b ON b.id = bs.bill_id WHERE bs.entity_id=?",
        (entity_id,),
    ).fetchall()
    result["stats"]["deputy_profile"] = conn.execute(
        "SELECT full_name, position, faction, region, committee FROM deputy_profiles WHERE entity_id=?",
        (entity_id,),
    ).fetchone()

    return result


def run_all(settings=None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)

    stats = build_structural_relations(conn)

    total_relations = conn.execute("SELECT COUNT(*) FROM entity_relations").fetchone()[0]
    stats["total_relations"] = total_relations

    rel_types = conn.execute(
        "SELECT relation_type, COUNT(*) FROM entity_relations GROUP BY relation_type ORDER BY COUNT(*) DESC"
    ).fetchall()
    stats["relation_type_breakdown"] = {r[0]: r[1] for r in rel_types}

    conn.close()
    return stats


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Build entity relations from structural data")
    parser.add_argument("--entity-id", type=int, help="Show involvement map for entity")
    args = parser.parse_args()

    if args.entity_id:
        settings = load_settings()
        conn = get_db(settings)
        result = get_involvement_map(conn, args.entity_id)
        conn.close()
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        stats = run_all()
        print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
