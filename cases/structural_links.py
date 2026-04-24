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


def populate_entity_relations_from_votes(conn) -> int:
    created = 0
    rows = conn.execute(
        "SELECT bv.entity_id, bvs.bill_id, bv.vote_result, bv.faction, b.number "
        "FROM bill_votes bv "
        "JOIN bill_vote_sessions bvs ON bvs.id = bv.vote_session_id "
        "JOIN bills b ON b.id = bvs.bill_id "
        "WHERE bv.entity_id IS NOT NULL"
    ).fetchall()
    for entity_id, bill_id, vote_result, faction, bill_number in rows:
        bill_entity = conn.execute(
            "SELECT id FROM entities WHERE entity_type='law' AND canonical_name=?",
            (bill_number,),
        ).fetchone()
        bill_eid = bill_entity[0] if bill_entity else None

        rel_type = {"за": "voted_for", "против": "voted_against",
                     "воздержался": "voted_abstained", "не голосовал": "voted_absent",
                     "отсутствовал": "voted_absent"}.get(vote_result, "voted")

        existing = conn.execute(
            "SELECT id FROM entity_relations WHERE from_entity_id=? AND to_entity_id=? AND relation_type=?",
            (entity_id, bill_eid, rel_type),
        ).fetchone() if bill_eid else None

        if not existing and bill_eid:
            conn.execute(
                "INSERT INTO entity_relations(from_entity_id, to_entity_id, relation_type, strength, detected_by) VALUES(?,?,?,?,'bill_votes')",
                (entity_id, bill_eid, rel_type, "strong" if vote_result in ("за", "против") else "moderate"),
            )
            created += 1
    return created


def populate_entity_relations_from_sponsors(conn) -> int:
    created = 0
    rows = conn.execute(
        "SELECT bs.entity_id, b.id, bs.faction, b.number FROM bill_sponsors bs "
        "JOIN bills b ON b.id = bs.bill_id WHERE bs.entity_id IS NOT NULL"
    ).fetchall()
    for entity_id, bill_id, faction, bill_number in rows:
        bill_entity = conn.execute(
            "SELECT id FROM entities WHERE entity_type='law' AND canonical_name=?",
            (bill_number,),
        ).fetchone()
        bill_eid = bill_entity[0] if bill_entity else None
        if not bill_eid:
            cur = conn.execute(
                "INSERT INTO entities(entity_type, canonical_name, description) VALUES(?,?,?)",
                ("law", bill_number, f"Законопроект {bill_number}"),
            )
            bill_eid = cur.lastrowid

        existing = conn.execute(
            "SELECT id FROM entity_relations WHERE from_entity_id=? AND to_entity_id=? AND relation_type='sponsored_bill'",
            (entity_id, bill_eid),
        ).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO entity_relations(from_entity_id, to_entity_id, relation_type, strength, detected_by) VALUES(?,?,'sponsored_bill','strong','bill_sponsors')",
                (entity_id, bill_eid),
            )
            created += 1
    return created


def populate_entity_relations_from_positions(conn) -> int:
    created = 0
    rows = conn.execute(
        "SELECT entity_id, organization, faction, position_title FROM official_positions WHERE is_active=1"
    ).fetchall()
    for entity_id, organization, faction, position_title in rows:
        org_entity = conn.execute(
            "SELECT id FROM entities WHERE entity_type='organization' AND canonical_name=?",
            (organization,),
        ).fetchone()
        org_eid = org_entity[0] if org_entity else None
        if not org_eid:
            cur = conn.execute(
                "INSERT INTO entities(entity_type, canonical_name, description) VALUES(?,?,?)",
                ("organization", organization, f"Организация: {organization}"),
            )
            org_eid = cur.lastrowid

        existing = conn.execute(
            "SELECT id FROM entity_relations WHERE from_entity_id=? AND to_entity_id=? AND relation_type='works_at'",
            (entity_id, org_eid),
        ).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO entity_relations(from_entity_id, to_entity_id, relation_type, strength, detected_by) VALUES(?,?,'works_at','strong','official_positions')",
                (entity_id, org_eid),
            )
            created += 1

        if faction:
            party_entity = conn.execute(
                "SELECT id FROM entities WHERE entity_type='party' AND canonical_name=?",
                (faction,),
            ).fetchone()
            party_eid = party_entity[0] if party_entity else None
            if not party_eid:
                cur = conn.execute(
                    "INSERT INTO entities(entity_type, canonical_name, description) VALUES(?,?,?)",
                    ("party", faction, f"Политическая партия: {faction}"),
                )
                party_eid = cur.lastrowid

            existing = conn.execute(
                "SELECT id FROM entity_relations WHERE from_entity_id=? AND to_entity_id=? AND relation_type='party_member'",
                (entity_id, party_eid),
            ).fetchone()
            if not existing:
                conn.execute(
                    "INSERT INTO entity_relations(from_entity_id, to_entity_id, relation_type, strength, detected_by) VALUES(?,?,'party_member','strong','party_memberships')",
                    (entity_id, party_eid),
                )
                created += 1
    return created


def link_bills_to_cases(conn) -> int:
    created = 0
    bills = conn.execute(
        "SELECT id, number, title, keywords, annotation FROM bills"
    ).fetchall()

    for bill_id, number, title, keywords_json, annotation in bills:
        search_terms = [number]
        if title:
            words = re.findall(r'[а-яё]{4,}', title.lower())
            search_terms.extend(words[:5])
        if keywords_json:
            try:
                kws = json.loads(keywords_json)
                search_terms.extend(kws[:5])
            except Exception:
                pass

        for term in search_terms[:5]:
            like = f"%{term}%"
            matching_claims = conn.execute(
                """
                SELECT cl.id, cl.claim_type, cl.status
                FROM claims cl
                JOIN content_items c ON c.id = cl.content_item_id
                WHERE (cl.claim_text LIKE ? OR c.title LIKE ? OR c.body_text LIKE ?)
                AND cl.id NOT IN (SELECT cc.claim_id FROM case_claims cc)
                LIMIT 20
                """,
                (like, like, like),
            ).fetchall()

            if not matching_claims:
                continue

            corruption_types = {"corruption_claim", "procurement_claim", "abuse_claim"}
            negative_types = corruption_types | {"detention", "censorship_action", "mobilization_claim"}
            has_corruption = any(r[1] in corruption_types for r in matching_claims)
            has_negative = any(r[1] in negative_types for r in matching_claims)
            n_matching = len(matching_claims)

            case_type = None
            if has_corruption and n_matching >= 3:
                case_type = "legislative_corruption"
            elif has_negative and n_matching >= 5:
                case_type = "public_opposition"
            elif n_matching >= 3:
                case_type = "legislative_impact"

            if not case_type:
                continue

            existing_case = conn.execute(
                "SELECT id FROM cases WHERE title LIKE ? AND case_type=? LIMIT 1",
                (f"%{number}%", case_type),
            ).fetchone()
            if existing_case:
                case_id = existing_case[0]
            else:
                cur = conn.execute(
                    "INSERT INTO cases(title, description, case_type, status, started_at) VALUES(?,?,?,?,?)",
                    (f"Закон {number}: {title or ''}",
                     f"Кейс связан с законопроектом {number}",
                     case_type, "open", datetime.now().isoformat()[:10]),
                )
                case_id = cur.lastrowid
                created += 1

            for claim_id, _, _ in matching_claims[:10]:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO case_claims(case_id, claim_id, role) VALUES(?,?,? )",
                        (case_id, claim_id, "allegation"),
                    )
                except Exception:
                    pass

            conn.execute(
                "INSERT OR IGNORE INTO case_events(case_id, event_date, event_title, event_description, event_order) VALUES(?,?,?,?,?)",
                (case_id, datetime.now().isoformat()[:10], f"Законопроект {number}",
                 f"Связанные претензии: {n_matching}", 0),
            )
            break

    return created


def process_votes_to_entities(conn) -> Dict:
    stats = {"votes_linked": 0, "entities_created": 0, "memberships_updated": 0, "positions_updated": 0}

    unmatched = conn.execute(
        "SELECT id, deputy_name, faction FROM bill_votes WHERE entity_id IS NULL AND deputy_name NOT LIKE 'Фракция:%'"
    ).fetchall()

    for vote_id, name, faction in unmatched:
        entity_id = None

        row = conn.execute(
            "SELECT entity_id FROM deputy_profiles WHERE full_name=?", (name,)
        ).fetchone()
        if row:
            entity_id = row[0]
        else:
            row = conn.execute(
                "SELECT id FROM entities WHERE entity_type='person' AND canonical_name LIKE ?",
                (f"%{name[:15]}%",),
            ).fetchone()
            if row:
                entity_id = row[0]

        if not entity_id:
            cur = conn.execute(
                "INSERT INTO entities(entity_type, canonical_name, description) VALUES(?,?,?)",
                ("person", name, f"Депутат. Фракция: {faction}" if faction else "Депутат"),
            )
            entity_id = cur.lastrowid
            stats["entities_created"] += 1

        try:
            conn.execute("UPDATE bill_votes SET entity_id=? WHERE id=?", (entity_id, vote_id))
            stats["votes_linked"] += 1
        except Exception:
            pass

        existing_dp = conn.execute(
            "SELECT id FROM deputy_profiles WHERE entity_id=?", (entity_id,)
        ).fetchone()
        if not existing_dp:
            conn.execute(
                "INSERT INTO deputy_profiles(entity_id, full_name, faction, is_active) VALUES(?,?,?,1)",
                (entity_id, name, faction),
            )

        if faction:
            existing_pm = conn.execute(
                "SELECT id FROM party_memberships WHERE entity_id=? AND party_name=? AND is_current=1",
                (entity_id, faction),
            ).fetchone()
            if not existing_pm:
                conn.execute(
                    "UPDATE party_memberships SET is_current=0 WHERE entity_id=? AND is_current=1",
                    (entity_id,),
                )
                conn.execute(
                    "INSERT INTO party_memberships(entity_id, party_name, role, is_current) VALUES(?,?,'член фракции',1)",
                    (entity_id, faction),
                )
                stats["memberships_updated"] += 1

            existing_pos = conn.execute(
                "SELECT id FROM official_positions WHERE entity_id=? AND position_title='Депутат Государственной Думы' AND is_active=1",
                (entity_id,),
            ).fetchone()
            if not existing_pos:
                conn.execute(
                    "INSERT INTO official_positions(entity_id, position_title, organization, faction, is_active, source_type) VALUES(?,?,'Государственная Дума РФ',?,1,'bill_votes')",
                    (entity_id, "Депутат Государственной Думы", faction),
                )
                stats["positions_updated"] += 1

    return stats


def run_all_structural_links(settings=None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)

    log.info("=== Structural data linking ===")

    log.info("Processing votes → entities...")
    vote_stats = process_votes_to_entities(conn)
    log.info("Votes: %s", vote_stats)

    log.info("Populating entity_relations from positions...")
    pos_rels = populate_entity_relations_from_positions(conn)
    log.info("Position relations: %d", pos_rels)

    log.info("Populating entity_relations from sponsors...")
    sp_rels = populate_entity_relations_from_sponsors(conn)
    log.info("Sponsor relations: %d", sp_rels)

    log.info("Populating entity_relations from votes...")
    vt_rels = populate_entity_relations_from_votes(conn)
    log.info("Vote relations: %d", vt_rels)

    log.info("Linking bills to cases...")
    case_links = link_bills_to_cases(conn)
    log.info("Bill→case links: %d new cases", case_links)

    conn.commit()
    conn.close()

    return {
        "votes_to_entities": vote_stats,
        "position_relations": pos_rels,
        "sponsor_relations": sp_rels,
        "vote_relations": vt_rels,
        "bill_case_links": case_links,
    }


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--what", choices=["all", "votes", "relations", "cases"], default="all")
    args = parser.parse_args()

    settings = load_settings()
    result = run_all_structural_links(settings)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
