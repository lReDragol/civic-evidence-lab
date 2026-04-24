import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)


def reverify_unverified_claims(conn, limit: int = 200) -> Dict:
    stats = {"checked": 0, "upgraded": 0, "new_evidence": 0, "errors": 0}

    unverified = conn.execute(
        """
        SELECT cl.id, cl.claim_text, cl.claim_type, cl.status, cl.confidence_auto,
               ci.id as content_item_id, ci.published_at, ci.source_id
        FROM claims cl
        JOIN content_items ci ON ci.id = cl.content_item_id
        WHERE cl.status IN ('unverified', 'raw_signal')
        AND cl.needs_review = 1
        ORDER BY ci.published_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    for claim in unverified:
        try:
            result = _reverify_single_claim(conn, claim)
            stats["checked"] += 1
            if result.get("upgraded"):
                stats["upgraded"] += 1
            if result.get("new_evidence"):
                stats["new_evidence"] += 1
        except Exception as e:
            log.warning("Re-verify claim %d failed: %s", claim["id"], e)
            stats["errors"] += 1

    return stats


def _reverify_single_claim(conn, claim) -> Dict:
    result = {"claim_id": claim["id"], "upgraded": False, "new_evidence": False, "evidence_found": 0}
    claim_id = claim["id"]
    claim_text = claim["claim_text"] or ""
    claim_type = claim["claim_type"] or ""
    content_item_id = claim["content_item_id"]

    existing_evidence = conn.execute(
        "SELECT COUNT(*) FROM evidence_links WHERE claim_id=?", (claim_id,)
    ).fetchone()[0]

    new_links = []
    evidence_notes = []

    entity_ids = [r["entity_id"] for r in conn.execute(
        "SELECT entity_id FROM entity_mentions WHERE content_item_id=?",
        (content_item_id,),
    ).fetchall()]

    law_refs = conn.execute(
        "SELECT law_type, law_number, article FROM law_references WHERE content_item_id=?",
        (content_item_id,),
    ).fetchall()

    if claim_type == "vote_record" and entity_ids:
        for eid in entity_ids:
            vote_evidence = conn.execute(
                """
                SELECT bvs.vote_date, bvs.vote_stage, bvs.result, b.number, b.title
                FROM bill_vote_sessions bvs
                JOIN bill_votes bv ON bv.vote_session_id = bvs.id
                JOIN bills b ON b.id = bvs.bill_id
                WHERE bv.entity_id = ?
                ORDER BY bvs.vote_date DESC
                LIMIT 5
                """,
                (eid,),
            ).fetchall()
            for ve in vote_evidence:
                matching_ci = conn.execute(
                    "SELECT id FROM content_items WHERE title LIKE ? AND content_type='vote_record' LIMIT 1",
                    (f"%{ve['number']}%",),
                ).fetchone()
                if matching_ci and matching_ci["id"] != content_item_id:
                    new_links.append((claim_id, matching_ci["id"], "official_record", "strong",
                                      "Vote record confirms voting behavior"))
                evidence_notes.append(f"Голосование: {ve['number']} — {ve['result']} ({ve['vote_date']})")

            faction_votes = conn.execute(
                """
                SELECT bvs.vote_date, bvs.result, bv.faction, bv.vote_result, b.number
                FROM bill_vote_sessions bvs
                JOIN bill_votes bv ON bv.vote_session_id = bvs.id
                JOIN bills b ON b.id = bvs.bill_id
                WHERE bv.deputy_name LIKE 'Фракция:%'
                AND bvs.bill_id IN (SELECT id FROM bills)
                ORDER BY bvs.vote_date DESC
                LIMIT 5
                """,
            ).fetchall()

    if claim_type in ("detention", "court_decision", "corruption_claim", "abuse_claim") and entity_ids:
        for eid in entity_ids:
            inv_materials = conn.execute(
                """
                SELECT im.id, im.title, im.material_type, im.verification_status
                FROM investigative_materials im
                WHERE im.involved_entities LIKE ?
                AND im.verification_status IN ('verified', 'partially')
                LIMIT 5
                """,
                (f"%{eid}%",),
            ).fetchall()
            for im in inv_materials:
                if im.get("content_item_id"):
                    new_links.append((claim_id, im["content_item_id"], "investigative_material",
                                      "strong", f"Следственный материал: {im['title'][:80]}"))
                evidence_notes.append(f"Материал: {im['title'][:80]}")

            matching_acts = conn.execute(
                """
                SELECT ci.id, ci.title, ci.url
                FROM content_items ci
                JOIN sources s ON s.id = ci.source_id
                WHERE s.is_official = 1
                AND s.category IN ('official_site', 'official_registry')
                AND (ci.title LIKE ? OR ci.body_text LIKE ?)
                LIMIT 5
                """,
                (f"%{claim_text[:30]}%", f"%{claim_text[:30]}%"),
            ).fetchall()
            for act in matching_acts:
                if act["id"] != content_item_id:
                    new_links.append((claim_id, act["id"], "official_source", "strong",
                                      f"Официальный источник: {act['title'][:80]}"))

    if law_refs:
        for lr in law_refs:
            matching_bills = conn.execute(
                "SELECT id, number, title, status FROM bills WHERE number LIKE ? OR title LIKE ? LIMIT 3",
                (f"%{lr['law_number']}%", f"%{lr['law_number']}%"),
            ).fetchall()
            for bill in matching_bills:
                bill_ci = conn.execute(
                    "SELECT id FROM content_items WHERE title LIKE ? LIMIT 1",
                    (f"%{bill['number']}%",),
                ).fetchone()
                if bill_ci and bill_ci["id"] != content_item_id:
                    new_links.append((claim_id, bill_ci["id"], "legislative_record", "moderate",
                                      f"Законопроект {bill['number']} подтверждает"))
                evidence_notes.append(f"Закон: {bill['number']} — {bill['status']}")

    if entity_ids:
        for eid in entity_ids:
            positions = conn.execute(
                """
                SELECT position_title, organization, region, faction
                FROM official_positions WHERE entity_id=? AND is_active=1
                """,
                (eid,),
            ).fetchall()
            for pos in positions:
                org_name = pos["organization"]
                evidence_notes.append(f"Должность: {pos['position_title']} в {org_name}")
                org_ci = conn.execute(
                    "SELECT ci.id FROM content_items ci JOIN sources s ON ci.source_id = s.id "
                    "WHERE ci.title LIKE ? AND s.is_official = 1 LIMIT 1",
                    (f"%{org_name}%",),
                ).fetchone()
                if org_ci:
                    new_links.append((claim_id, org_ci["id"], "official_position", "moderate",
                                      f"Должность: {pos['position_title']} в {org_name}"))

            sponsors = conn.execute(
                """
                SELECT b.number, b.title, b.status FROM bill_sponsors bs
                JOIN bills b ON b.id = bs.bill_id
                WHERE bs.entity_id = ?
                LIMIT 5
                """,
                (eid,),
            ).fetchall()
            for sp in sponsors:
                evidence_notes.append(f"Спонсор закона: {sp['number']} ({sp['status']})")

    bill_numbers = re.findall(r"№\s*(\d{5,}[-–]\d+)", claim_text)
    for bn in bill_numbers:
        bn = bn.replace("–", "-")
        matching = conn.execute(
            "SELECT id, number, title, status FROM bills WHERE number=?", (bn,)
        ).fetchall()
        for bill in matching:
            evidence_notes.append(f"Законопроект {bill['number']} найден: {bill['status']}")

    person_names = re.findall(r"([А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.[А-ЯЁ]\.(?:\s+[А-ЯЁ][а-яё]+)?)", claim_text)
    person_names += re.findall(r"([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)", claim_text)
    for pname in person_names:
        matching = conn.execute(
            "SELECT dp.full_name, dp.position, dp.faction FROM deputy_profiles dp WHERE dp.full_name LIKE ?",
            (f"%{pname[:15]}%",),
        ).fetchall()
        for dp in matching:
            evidence_notes.append(f"Лицо в БД: {dp['full_name']} — {dp['position'][:50]}")

    for claim_id_, evidence_ci_id, ev_type, strength, notes in new_links:
        try:
            existing = conn.execute(
                "SELECT id FROM evidence_links WHERE claim_id=? AND evidence_item_id=? AND evidence_type=?",
                (claim_id_, evidence_ci_id, ev_type),
            ).fetchone()
            if not existing:
                conn.execute(
                    "INSERT INTO evidence_links(claim_id, evidence_item_id, evidence_type, strength, notes, linked_by) "
                    "VALUES(?,?,?,?,?,'re_verifier')",
                    (claim_id_, evidence_ci_id, ev_type, strength, notes),
                )
                result["new_evidence"] = True
        except Exception:
            pass

    total_evidence = existing_evidence + len(new_links) + len(evidence_notes)

    if evidence_notes and not result["new_evidence"]:
        result["evidence_found"] = len(evidence_notes)
        conn.execute(
            "INSERT INTO verifications(claim_id, verifier_type, old_status, new_status, notes, evidence_added) "
            "VALUES(?,?,?,?,?,?)",
            (claim_id, "re_verifier", claim["status"], claim["status"],
             f"Re-verified: {len(evidence_notes)} structural evidence found but no content_item links. "
             f"Evidence: {'; '.join(evidence_notes[:5])}", 0),
        )

    if result["new_evidence"]:
        new_evidence_count = existing_evidence + len(new_links)
        if new_evidence_count >= 3:
            new_status = "likely_true"
        elif new_evidence_count >= 2:
            new_status = "partially_confirmed"
        elif new_evidence_count >= 1:
            new_status = "partially_confirmed"
        else:
            new_status = None

        if new_status and new_status != claim["status"]:
            conn.execute(
                "UPDATE claims SET status=?, confidence_final=0.5 WHERE id=?",
                (new_status, claim_id),
            )
            conn.execute(
                "INSERT INTO verifications(claim_id, verifier_type, old_status, new_status, notes, evidence_added) "
                "VALUES(?,?,?,?,?,?)",
                (claim_id, "re_verifier", claim["status"], new_status,
                 f"Re-verified: {len(new_links)} new content_item evidence + {len(evidence_notes)} structural evidence",
                 len(new_links)),
            )
            result["upgraded"] = True

    return result


def reverify_claims_for_entity(conn, entity_id: int) -> Dict:
    stats = {"checked": 0, "upgraded": 0, "new_evidence": 0}

    claims = conn.execute(
        """
        SELECT DISTINCT cl.id, cl.claim_text, cl.claim_type, cl.status, cl.confidence_auto,
               ci.id as content_item_id, ci.published_at, ci.source_id
        FROM claims cl
        JOIN entity_mentions em ON em.content_item_id = cl.content_item_id
        JOIN content_items ci ON ci.id = cl.content_item_id
        WHERE em.entity_id = ?
        AND cl.status IN ('unverified', 'raw_signal')
        AND cl.needs_review = 1
        ORDER BY ci.published_at DESC
        LIMIT 50
        """,
        (entity_id,),
    ).fetchall()

    for claim in claims:
        result = _reverify_single_claim(conn, claim)
        stats["checked"] += 1
        if result.get("upgraded"):
            stats["upgraded"] += 1
        if result.get("new_evidence"):
            stats["new_evidence"] += 1

    return stats


def reverify_claims_for_bill(conn, bill_id: int) -> Dict:
    stats = {"checked": 0, "upgraded": 0, "new_evidence": 0}

    bill = conn.execute("SELECT number, title FROM bills WHERE id=?", (bill_id,)).fetchone()
    if not bill:
        return stats

    claims = conn.execute(
        """
        SELECT cl.id, cl.claim_text, cl.claim_type, cl.status, cl.confidence_auto,
               ci.id as content_item_id, ci.published_at, ci.source_id
        FROM claims cl
        JOIN content_items ci ON ci.id = cl.content_item_id
        WHERE (cl.claim_text LIKE ? OR ci.title LIKE ? OR ci.body_text LIKE ?)
        AND cl.status IN ('unverified', 'raw_signal')
        AND cl.needs_review = 1
        ORDER BY ci.published_at DESC
        LIMIT 50
        """,
        (f"%{bill['number']}%", f"%{bill['number']}%", f"%{bill['number']}%"),
    ).fetchall()

    for claim in claims:
        result = _reverify_single_claim(conn, claim)
        stats["checked"] += 1
        if result.get("upgraded"):
            stats["upgraded"] += 1
        if result.get("new_evidence"):
            stats["new_evidence"] += 1

    return stats


def run_reverification(settings=None, limit: int = 200):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)

    log.info("Starting re-verification of unverified claims...")
    stats = reverify_unverified_claims(conn, limit=limit)

    conn.commit()
    conn.close()

    log.info("Re-verification done: %d checked, %d upgraded, %d with new evidence",
             stats["checked"], stats["upgraded"], stats["new_evidence"])
    return stats


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Re-verify unverified claims with new evidence")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--entity-id", type=int, help="Re-verify claims for specific entity")
    parser.add_argument("--bill-id", type=int, help="Re-verify claims for specific bill")
    args = parser.parse_args()

    settings = load_settings()
    conn = get_db(settings)

    if args.entity_id:
        stats = reverify_claims_for_entity(conn, args.entity_id)
    elif args.bill_id:
        stats = reverify_claims_for_bill(conn, args.bill_id)
    else:
        stats = reverify_unverified_claims(conn, limit=args.limit)

    conn.commit()
    conn.close()
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
