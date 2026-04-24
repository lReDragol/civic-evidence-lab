import json
import logging
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


def _fmt_row(row, keys=None):
    if row is None:
        return None
    if keys is None:
        keys = row.keys() if hasattr(row, "keys") else range(len(row))
    return {k: row[k] for k in keys if row[k] is not None}


def build_involvement_map(conn, entity_id: int) -> Dict:
    entity = conn.execute(
        "SELECT id, entity_type, canonical_name, inn, ogrn, description FROM entities WHERE id=?",
        (entity_id,),
    ).fetchone()
    if not entity:
        return {"error": f"Entity {entity_id} not found"}

    result = {
        "entity": _fmt_row(entity),
        "positions": [],
        "party_history": [],
        "bills_sponsored": [],
        "votes": [],
        "claims": [],
        "cases": [],
        "risk_patterns": [],
        "relations": {},
        "quotes": [],
        "accountability": None,
    }

    for row in conn.execute(
        "SELECT position_title, organization, region, faction, started_at, ended_at, "
        "source_url, source_type, is_active FROM official_positions "
        "WHERE entity_id=? ORDER BY is_active DESC, started_at DESC",
        (entity_id,),
    ).fetchall():
        result["positions"].append(_fmt_row(row))

    for row in conn.execute(
        "SELECT party_name, role, started_at, ended_at, source_url, is_current "
        "FROM party_memberships WHERE entity_id=? ORDER BY is_current DESC, started_at DESC",
        (entity_id,),
    ).fetchall():
        result["party_history"].append(_fmt_row(row))

    for row in conn.execute(
        "SELECT b.number, b.title, b.bill_type, b.status, b.registration_date, b.duma_url "
        "FROM bill_sponsors bs JOIN bills b ON b.id = bs.bill_id "
        "WHERE bs.entity_id=? ORDER BY b.registration_date DESC",
        (entity_id,),
    ).fetchall():
        result["bills_sponsored"].append(_fmt_row(row))

    for row in conn.execute(
        "SELECT b.number, b.title, bv.vote_result, bvs.vote_date, bvs.vote_stage, bvs.result "
        "FROM bill_votes bv "
        "JOIN bill_vote_sessions bvs ON bvs.id = bv.vote_session_id "
        "JOIN bills b ON b.id = bvs.bill_id "
        "WHERE bv.entity_id=? ORDER BY bvs.vote_date DESC",
        (entity_id,),
    ).fetchall():
        result["votes"].append(_fmt_row(row))

    claims_query = """
        SELECT cl.id, cl.claim_text, cl.claim_type, cl.confidence_auto, cl.status,
               cl.needs_review, ci.title as source_title, s.name as source_name,
               s.credibility_tier, ci.published_at, ci.url
        FROM claims cl
        JOIN content_items ci ON ci.id = cl.content_item_id
        JOIN sources s ON s.id = ci.source_id
        WHERE cl.id IN (
            SELECT em.content_item_id FROM entity_mentions em
            WHERE em.entity_id = ?
        )
        OR cl.content_item_id IN (
            SELECT em.content_item_id FROM entity_mentions em
            WHERE em.entity_id = ?
        )
        ORDER BY ci.published_at DESC
        LIMIT 50
    """
    for row in conn.execute(claims_query, (entity_id, entity_id)).fetchall():
        result["claims"].append(_fmt_row(row))

    cases_query = """
        SELECT DISTINCT c.id, c.title, c.case_type, c.status, c.started_at,
               cc.role as claim_role
        FROM cases c
        JOIN case_claims cc ON cc.case_id = c.id
        JOIN claims cl ON cl.id = cc.claim_id
        WHERE cl.id IN (
            SELECT cl2.id FROM claims cl2
            JOIN entity_mentions em ON em.content_item_id = cl2.content_item_id
            WHERE em.entity_id = ?
        )
        ORDER BY c.started_at DESC
        LIMIT 30
    """
    for row in conn.execute(cases_query, (entity_id,)).fetchall():
        result["cases"].append(_fmt_row(row))

    for row in conn.execute(
        "SELECT pattern_type, description, risk_level, detected_at, needs_review "
        "FROM risk_patterns WHERE entity_ids LIKE ? ORDER BY detected_at DESC",
        (f'%{entity_id}%',),
    ).fetchall():
        result["risk_patterns"].append(_fmt_row(row))

    relations_query = """
        SELECT er.relation_type, er.strength, er.detected_by,
               e_from.id as from_id, e_from.canonical_name as from_name, e_from.entity_type as from_type,
               e_to.id as to_id, e_to.canonical_name as to_name, e_to.entity_type as to_type
        FROM entity_relations er
        JOIN entities e_from ON e_from.id = er.from_entity_id
        JOIN entities e_to ON e_to.id = er.to_entity_id
        WHERE er.from_entity_id = ? OR er.to_entity_id = ?
        ORDER BY er.strength DESC
    """
    for row in conn.execute(relations_query, (entity_id, entity_id)).fetchall():
        rel_type = row["relation_type"]
        if rel_type not in result["relations"]:
            result["relations"][rel_type] = []
        direction = "outgoing" if row["from_id"] == entity_id else "incoming"
        other_name = row["to_name"] if direction == "outgoing" else row["from_name"]
        other_type = row["to_type"] if direction == "outgoing" else row["from_type"]
        other_id = row["to_id"] if direction == "outgoing" else row["from_id"]
        result["relations"][rel_type].append({
            "direction": direction,
            "entity_id": other_id,
            "entity_name": other_name,
            "entity_type": other_type,
            "strength": row["strength"],
            "detected_by": row["detected_by"],
        })

    for row in conn.execute(
        "SELECT quote_text, rhetoric_class, is_flagged, context, ci.published_at "
        "FROM quotes q JOIN content_items ci ON ci.id = q.content_item_id "
        "WHERE q.entity_id=? ORDER BY ci.published_at DESC LIMIT 30",
        (entity_id,),
    ).fetchall():
        result["quotes"].append(_fmt_row(row))

    acc = conn.execute(
        "SELECT period, public_speeches_count, verifiable_claims_count, "
        "confirmed_contradictions, flagged_statements_count, votes_tracked_count, "
        "linked_cases_count, promises_made_count, promises_kept_count, calculated_score "
        "FROM accountability_index WHERE deputy_id IN "
        "(SELECT id FROM deputy_profiles WHERE entity_id=?) ORDER BY period DESC",
        (entity_id,),
    ).fetchone()
    if acc:
        result["accountability"] = _fmt_row(acc)

    return result


def build_summary_stats(conn, entity_id: int) -> Dict:
    stats = {
        "total_relations": conn.execute(
            "SELECT COUNT(*) FROM entity_relations WHERE from_entity_id=? OR to_entity_id=?",
            (entity_id, entity_id),
        ).fetchone()[0],
        "structural_relations": conn.execute(
            "SELECT COUNT(*) FROM entity_relations WHERE (from_entity_id=? OR to_entity_id=?) "
            "AND relation_type NOT IN ('mentioned_together','associated_with_location','located_in')",
            (entity_id, entity_id),
        ).fetchone()[0],
        "bills_sponsored": conn.execute(
            "SELECT COUNT(*) FROM bill_sponsors WHERE entity_id=?", (entity_id,)
        ).fetchone()[0],
        "claims_mentioned": conn.execute(
            "SELECT COUNT(DISTINCT cl.id) FROM claims cl "
            "JOIN entity_mentions em ON em.content_item_id = cl.content_item_id "
            "WHERE em.entity_id=?", (entity_id,)
        ).fetchone()[0],
        "cases_involved": conn.execute(
            "SELECT COUNT(DISTINCT c.id) FROM cases c "
            "JOIN case_claims cc ON cc.case_id = c.id "
            "JOIN claims cl ON cl.id = cc.claim_id "
            "JOIN entity_mentions em ON em.content_item_id = cl.content_item_id "
            "WHERE em.entity_id=?", (entity_id,)
        ).fetchone()[0],
        "risk_patterns": conn.execute(
            "SELECT COUNT(*) FROM risk_patterns WHERE entity_ids LIKE ?",
            (f'%{entity_id}%',),
        ).fetchone()[0],
        "quotes_flagged": conn.execute(
            "SELECT COUNT(*) FROM quotes WHERE entity_id=? AND is_flagged=1",
            (entity_id,),
        ).fetchone()[0],
    }
    return stats


def find_entity_by_name(conn, name: str) -> Optional[int]:
    row = conn.execute(
        "SELECT entity_id FROM entity_aliases WHERE alias = ? LIMIT 1",
        (name,),
    ).fetchone()
    if row:
        return row[0]
    row = conn.execute(
        "SELECT id FROM entities WHERE canonical_name LIKE ? LIMIT 1",
        (f"%{name}%",),
    ).fetchone()
    if row:
        return row[0]
    return None


def list_deputies_with_maps(conn, limit: int = 50) -> List[Dict]:
    rows = conn.execute(
        """
        SELECT dp.entity_id, dp.full_name, dp.position, dp.faction, dp.region,
               (SELECT COUNT(*) FROM bill_sponsors WHERE entity_id = dp.entity_id) as bill_count,
               (SELECT COUNT(*) FROM entity_relations WHERE from_entity_id = dp.entity_id
                AND relation_type NOT IN ('mentioned_together','associated_with_location','located_in')) as struct_rel_count,
               (SELECT COUNT(*) FROM risk_patterns WHERE entity_ids LIKE '%' || dp.entity_id || '%') as risk_count
        FROM deputy_profiles dp
        JOIN official_positions op ON op.entity_id = dp.entity_id
        WHERE op.is_active = 1
        ORDER BY bill_count DESC, struct_rel_count DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [_fmt_row(r) for r in rows]


def generate_map(conn, entity_id: int, include_full_map: bool = True) -> Dict:
    result = {"summary": build_summary_stats(conn, entity_id)}
    if include_full_map:
        result["map"] = build_involvement_map(conn, entity_id)
    return result


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Generate involvement map for official")
    parser.add_argument("--entity-id", type=int, help="Entity ID")
    parser.add_argument("--name", type=str, help="Search by name")
    parser.add_argument("--list", action="store_true", help="List deputies with maps")
    parser.add_argument("--summary-only", action="store_true", help="Only summary stats")
    parser.add_argument("--limit", type=int, default=20, help="List limit")
    args = parser.parse_args()

    settings = load_settings()
    conn = get_db(settings)

    if args.list:
        deputies = list_deputies_with_maps(conn, limit=args.limit)
        for d in deputies:
            print(f"  eid={d['entity_id']} {d['full_name']} | {d['position'][:40] if d.get('position') else ''} | "
                  f"faction={d.get('faction','')} | bills={d.get('bill_count',0)} | rels={d.get('struct_rel_count',0)} | risks={d.get('risk_count',0)}")
    elif args.entity_id or args.name:
        eid = args.entity_id
        if not eid and args.name:
            eid = find_entity_by_name(conn, args.name)
        if not eid:
            print("Entity not found")
            conn.close()
            return
        result = generate_map(conn, eid, include_full_map=not args.summary_only)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        print("Specify --entity-id, --name, or --list")

    conn.close()


if __name__ == "__main__":
    main()
