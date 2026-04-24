import json
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

WEIGHTS = {
    "source_credibility": 0.15,
    "document_evidence": 0.25,
    "cross_source_corroboration": 0.20,
    "temporal_consistency": 0.10,
    "entity_verification": 0.15,
    "rhetoric_risk": 0.10,
    "contradiction_penalty": 0.05,
}

CREDIBILITY_TIER_SCORES = {"A": 0.9, "B": 0.6, "C": 0.3, "D": 0.1}
OFFICIAL_CATEGORIES = {"official_registry", "official_site"}
OFFICIAL_CONTENT_TYPES = {"registry_record", "court_record", "enforcement", "procurement", "bill", "transcript"}

STATUS_THRESHOLDS = {
    "confirmed": 0.80,
    "likely_true": 0.60,
    "partially_confirmed": 0.40,
    "unverified": 0.20,
    "likely_false": 0.10,
    "disproved": 0.00,
}


def _compute_status(score: float, rhetoric_risk: float = 0.0, corroboration: float = 0.0) -> str:
    if rhetoric_risk > 0.7 and corroboration < 0.2:
        return "manipulation"
    for status, threshold in STATUS_THRESHOLDS.items():
        if score >= threshold:
            return status
    return "disproved"


def compute_source_credibility(conn, claim_id: int, source_id: int) -> float:
    row = conn.execute(
        "SELECT credibility_tier, category, is_official FROM sources WHERE id=?",
        (source_id,),
    ).fetchone()
    if not row:
        return 0.1
    tier, category, is_official = row
    base = CREDIBILITY_TIER_SCORES.get(tier, 0.15)
    if category in OFFICIAL_CATEGORIES:
        base = min(1.0, base + 0.2)
    if is_official:
        base = min(1.0, base + 0.1)
    return base


def compute_document_evidence(conn, claim_id: int) -> float:
    links = conn.execute(
        """
        SELECT el.strength, c.content_type, s.category
        FROM evidence_links el
        LEFT JOIN content_items c ON c.id = el.evidence_item_id
        LEFT JOIN sources s ON s.id = c.source_id
        WHERE el.claim_id=?
        """,
        (claim_id,),
    ).fetchall()
    if not links:
        return 0.0

    score = 0.0
    for strength, content_type, source_category in links:
        link_score = {"strong": 0.5, "moderate": 0.3, "weak": 0.1}.get(strength, 0.1)
        if content_type in OFFICIAL_CONTENT_TYPES:
            link_score *= 1.5
        if source_category in OFFICIAL_CATEGORIES:
            link_score *= 1.3
        score += link_score
    return min(1.0, score)


def compute_cross_source_corroboration(conn, claim_id: int) -> float:
    claim_row = conn.execute("SELECT content_item_id, claim_text FROM claims WHERE id=?", (claim_id,)).fetchone()
    if not claim_row:
        return 0.0
    content_id, claim_text = claim_row

    source_ids = conn.execute(
        """
        SELECT DISTINCT s.id, s.category, s.political_alignment
        FROM content_items c
        JOIN sources s ON s.id = c.source_id
        JOIN entity_mentions em ON em.content_item_id = c.id
        WHERE em.entity_id IN (
            SELECT em2.entity_id FROM entity_mentions em2 WHERE em2.content_item_id=?
        )
        AND c.id != ?
        AND c.id IN (
            SELECT c2.id FROM content_items c2
            JOIN claims cl ON cl.content_item_id = c2.id
            WHERE cl.claim_type = (SELECT claim_type FROM claims WHERE id=?)
        )
        LIMIT 20
        """,
        (content_id, content_id, claim_id),
    ).fetchall()

    if not source_ids:
        return 0.0

    unique_categories = set(r[1] for r in source_ids if r[1])
    unique_alignments = set(r[2] for r in source_ids if r[2])
    n_sources = len(source_ids)

    score = 0.0
    if n_sources >= 3:
        score += 0.3
    elif n_sources >= 2:
        score += 0.2

    if len(unique_categories) >= 2:
        score += 0.3
    if len(unique_alignments) >= 2:
        score += 0.2
    if n_sources >= 5:
        score += 0.2

    return min(1.0, score)


def compute_temporal_consistency(conn, claim_id: int) -> float:
    claim_row = conn.execute(
        "SELECT content_item_id, claim_text FROM claims WHERE id=?", (claim_id,)
    ).fetchone()
    if not claim_row:
        return 0.0

    content_row = conn.execute(
        "SELECT published_at FROM content_items WHERE id=?", (claim_row[0],)
    ).fetchone()
    if not content_row or not content_row[0]:
        return 0.5

    claim_date_str = content_row[0][:10]
    try:
        claim_date = datetime.strptime(claim_date_str, "%Y-%m-%d")
    except ValueError:
        return 0.5

    evidence_dates = conn.execute(
        """
        SELECT c.published_at
        FROM evidence_links el
        JOIN content_items c ON c.id = el.evidence_item_id
        WHERE el.claim_id=? AND c.published_at IS NOT NULL AND c.published_at != ''
        """,
        (claim_id,),
    ).fetchall()

    if not evidence_dates:
        return 0.3

    scores = []
    for (date_str,) in evidence_dates:
        try:
            ev_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
            diff_days = (claim_date - ev_date).days
            if diff_days >= 0:
                if diff_days <= 30:
                    scores.append(0.8)
                elif diff_days <= 365:
                    scores.append(0.5)
                else:
                    scores.append(0.2)
            else:
                abs_diff = abs(diff_days)
                if abs_diff <= 7:
                    scores.append(0.6)
                elif abs_diff <= 30:
                    scores.append(0.4)
                else:
                    scores.append(0.1)
        except ValueError:
            continue

    return max(scores) if scores else 0.3


def compute_entity_verification(conn, claim_id: int) -> float:
    claim_row = conn.execute("SELECT claim_text FROM claims WHERE id=?", (claim_id,)).fetchone()
    if not claim_row:
        return 0.0
    claim_text = claim_row[0]

    inns = re.findall(r'\b(\d{10}|\d{12})\b', claim_text)
    case_nums = re.findall(r'дел[аоу]?\s+(?:номер\s+)?(\d+[\-/]\d+[\-/]\d+)', claim_text, re.I)
    names = re.findall(r'\b([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){1,2})\b', claim_text)

    verified = 0
    total_checks = 0

    for inn in inns[:3]:
        total_checks += 1
        row = conn.execute(
            "SELECT id FROM entities WHERE entity_type='inn' AND canonical_name=?",
            (inn,),
        ).fetchone()
        if row:
            verified += 1
        else:
            row2 = conn.execute(
                "SELECT id FROM content_items WHERE body_text LIKE ? AND content_type='registry_record' LIMIT 1",
                (f"%{inn}%",),
            ).fetchone()
            if row2:
                verified += 1

    for case_num in case_nums[:2]:
        total_checks += 1
        row = conn.execute(
            "SELECT id FROM content_items WHERE (title LIKE ? OR body_text LIKE ?) AND content_type='court_record' LIMIT 1",
            (f"%{case_num}%", f"%{case_num}%"),
        ).fetchone()
        if row:
            verified += 1

    for name in names[:2]:
        total_checks += 1
        row = conn.execute(
            "SELECT id FROM entities WHERE entity_type='person' AND canonical_name LIKE ? LIMIT 1",
            (f"%{name[:15]}%",),
        ).fetchone()
        if row:
            verified += 1

    if total_checks == 0:
        return 0.3
    return min(1.0, verified / total_checks)


def compute_rhetoric_risk(conn, claim_id: int) -> float:
    claim_row = conn.execute(
        "SELECT c.id, c.body_text, s.political_alignment FROM claims cl "
        "JOIN content_items c ON c.id = cl.content_item_id "
        "JOIN sources s ON s.id = c.source_id "
        "WHERE cl.id=?",
        (claim_id,),
    ).fetchone()
    if not claim_row:
        return 0.0

    content_id, body_text, alignment = claim_row

    risk = 0.0

    if alignment in ("pro_government", "government", "state_aligned"):
        risk += 0.2

    flagged_quotes = conn.execute(
        "SELECT COUNT(*) FROM quotes WHERE content_item_id=? AND is_flagged=1",
        (content_id,),
    ).fetchone()[0]
    if flagged_quotes > 0:
        risk += min(0.3, flagged_quotes * 0.1)

    manipulation_tags = conn.execute(
        "SELECT COUNT(*) FROM content_tags WHERE content_item_id=? AND tag_name IN ('possible_disinformation','flagged_rhetoric','manipulation_risk')",
        (content_id,),
    ).fetchone()[0]
    if manipulation_tags > 0:
        risk += 0.2

    negative_filter_tags = conn.execute(
        "SELECT COUNT(*) FROM content_tags WHERE content_item_id=? AND tag_name LIKE 'filter:%' OR tag_name LIKE 'negative:%'",
        (content_id,),
    ).fetchone()[0]
    if negative_filter_tags > 0:
        risk += 0.15

    return min(1.0, risk)


def compute_contradiction_penalty(conn, claim_id: int) -> float:
    claim_row = conn.execute(
        "SELECT content_item_id, claim_text, claim_type FROM claims WHERE id=?",
        (claim_id,),
    ).fetchone()
    if not claim_row:
        return 0.0

    content_id, claim_text, claim_type = claim_row

    entity_ids = conn.execute(
        "SELECT entity_id FROM entity_mentions WHERE content_item_id=?",
        (content_id,),
    ).fetchall()
    if not entity_ids:
        return 0.0

    contradiction_relations = 0
    for (eid,) in entity_ids[:5]:
        count = conn.execute(
            "SELECT COUNT(*) FROM entity_relations WHERE (from_entity_id=? OR to_entity_id=?) AND relation_type='contradicts'",
            (eid, eid),
        ).fetchone()[0]
        contradiction_relations += count

    contradicting_claims = conn.execute(
        """
        SELECT COUNT(*) FROM claims c2
        JOIN entity_mentions em ON em.content_item_id = c2.content_item_id
        WHERE em.entity_id IN ({})
        AND c2.id != ?
        AND c2.status IN ('likely_false','disproved','manipulation')
        """.format(",".join("?" for _ in entity_ids[:5])),
        [e[0] for e in entity_ids[:5]] + [claim_id],
    ).fetchone()[0]

    penalty = 0.0
    if contradiction_relations > 0:
        penalty += min(0.3, contradiction_relations * 0.1)
    if contradicting_claims > 0:
        penalty += min(0.5, contradicting_claims * 0.15)

    return min(1.0, penalty)


def compute_authenticity_score(conn, claim_id: int, source_id: int = None) -> Dict:
    if source_id is None:
        claim_src = conn.execute(
            "SELECT c.source_id FROM claims cl JOIN content_items c ON c.id = cl.content_item_id WHERE cl.id=?",
            (claim_id,),
        ).fetchone()
        source_id = claim_src[0] if claim_src else None

    factors = {
        "source_credibility": compute_source_credibility(conn, claim_id, source_id) if source_id else 0.1,
        "document_evidence": compute_document_evidence(conn, claim_id),
        "cross_source_corroboration": compute_cross_source_corroboration(conn, claim_id),
        "temporal_consistency": compute_temporal_consistency(conn, claim_id),
        "entity_verification": compute_entity_verification(conn, claim_id),
        "rhetoric_risk": compute_rhetoric_risk(conn, claim_id),
        "contradiction_penalty": compute_contradiction_penalty(conn, claim_id),
    }

    positive = (
        WEIGHTS["source_credibility"] * factors["source_credibility"]
        + WEIGHTS["document_evidence"] * factors["document_evidence"]
        + WEIGHTS["cross_source_corroboration"] * factors["cross_source_corroboration"]
        + WEIGHTS["temporal_consistency"] * factors["temporal_consistency"]
        + WEIGHTS["entity_verification"] * factors["entity_verification"]
    )
    negative = (
        WEIGHTS["rhetoric_risk"] * factors["rhetoric_risk"]
        + WEIGHTS["contradiction_penalty"] * factors["contradiction_penalty"]
    )

    total = max(0.0, min(1.0, positive - negative))
    status = _compute_status(total, factors["rhetoric_risk"], factors["cross_source_corroboration"])

    return {
        "score": round(total, 4),
        "status": status,
        "factors": {k: round(v, 4) for k, v in factors.items()},
        "positive_component": round(positive, 4),
        "negative_component": round(negative, 4),
    }


def store_verification(conn, claim_id: int, verifier_type: str, old_status: str, new_status: str,
                       notes: str = "", evidence_added: int = 0):
    try:
        conn.execute(
            "INSERT INTO verifications(claim_id, verifier_type, old_status, new_status, notes, evidence_added, verified_at) VALUES(?,?,?,?,?,?,?)",
            (claim_id, verifier_type, old_status, new_status, notes, evidence_added, datetime.now().isoformat()),
        )
    except Exception as e:
        log.warning("Failed to store verification for claim %d: %s", claim_id, e)


def reverify_all_claims(settings=None, limit: int = 500, claim_type_filter: Optional[str] = None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    conn.row_factory = sqlite3.Row

    query = "SELECT id, claim_text, claim_type, status FROM claims WHERE 1=1"
    params = []
    if claim_type_filter:
        query += " AND claim_type=?"
        params.append(claim_type_filter)
    query += " LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    log.info("Re-verifying %d claims with 7-factor model", len(rows))

    status_changes = {}
    updated = 0

    for row in rows:
        claim_id = row["id"]
        old_status = row["status"]

        result = compute_authenticity_score(conn, claim_id)
        new_status = result["status"]

        try:
            conn.execute(
                "UPDATE claims SET status=?, source_score=?, document_score=?, corroboration_score=?, "
                "temporal_consistency=?, cross_source_score=?, entity_verification_score=?, "
                "rhetoric_risk_score=?, contradiction_score=? WHERE id=?",
                (
                    new_status,
                    result["factors"]["source_credibility"],
                    result["factors"]["document_evidence"],
                    result["factors"]["cross_source_corroboration"],
                    result["factors"]["temporal_consistency"],
                    result["factors"]["cross_source_corroboration"],
                    result["factors"]["entity_verification"],
                    result["factors"]["rhetoric_risk"],
                    result["factors"]["contradiction_penalty"],
                    claim_id,
                ),
            )

            content_row = conn.execute(
                "SELECT c.id FROM claims cl JOIN content_items c ON c.id = cl.content_item_id WHERE cl.id=?",
                (claim_id,),
            ).fetchone()
            if content_row:
                conn.execute(
                    "UPDATE content_items SET authenticity_score=? WHERE id=?",
                    (result["score"], content_row[0]),
                )

            if old_status != new_status:
                store_verification(
                    conn, claim_id, "authenticity_model_v2", old_status, new_status,
                    notes=json.dumps(result["factors"], ensure_ascii=False),
                )
                status_changes.setdefault(f"{old_status}->{new_status}", 0)
                status_changes[f"{old_status}->{new_status}"] += 1

            updated += 1
            if updated % 100 == 0:
                conn.commit()
        except Exception as e:
            log.warning("Failed to update claim %d: %s", claim_id, e)

    conn.commit()

    new_status_counts = {}
    for s, cnt in conn.execute("SELECT status, COUNT(*) FROM claims GROUP BY status").fetchall():
        new_status_counts[s] = cnt

    verifications_count = conn.execute("SELECT COUNT(*) FROM verifications").fetchone()[0]

    log.info("Re-verification complete: %d claims updated", updated)
    log.info("Status changes: %s", status_changes)
    log.info("New distribution: %s", new_status_counts)
    log.info("Verifications audit: %d entries", verifications_count)

    conn.close()
    return {
        "claims_reverified": updated,
        "status_changes": status_changes,
        "new_distribution": new_status_counts,
        "verifications_count": verifications_count,
    }


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="7-factor authenticity model")
    parser.add_argument("--limit", type=int, default=500, help="Max claims to re-verify")
    parser.add_argument("--claim-type", help="Only verify claims of this type")
    parser.add_argument("--claim-id", type=int, help="Verify single claim")
    args = parser.parse_args()

    settings = load_settings()

    if args.claim_id:
        conn = get_db(settings)
        result = compute_authenticity_score(conn, args.claim_id)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        conn.close()
    else:
        result = reverify_all_claims(settings, limit=args.limit, claim_type_filter=args.claim_type)
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
