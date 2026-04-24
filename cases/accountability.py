import json
import logging
import os
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


def _get_deputy_stats(conn: sqlite3.Connection, deputy_id: int, entity_id: int, period: str) -> Dict:
    stats = {
        "public_speeches_count": 0,
        "verifiable_claims_count": 0,
        "confirmed_contradictions": 0,
        "flagged_statements_count": 0,
        "votes_tracked_count": 0,
        "linked_cases_count": 0,
        "promises_made_count": 0,
        "promises_kept_count": 0,
    }

    content_ids = conn.execute(
        """
        SELECT DISTINCT em.content_item_id
        FROM entity_mentions em
        WHERE em.entity_id = ?
        """,
        (entity_id,),
    ).fetchall()
    cid_list = [r[0] for r in content_ids]

    if not cid_list:
        return stats

    placeholders = ",".join("?" * len(cid_list))

    stats["public_speeches_count"] = conn.execute(
        f"SELECT COUNT(DISTINCT content_item_id) FROM claims WHERE content_item_id IN ({placeholders}) AND claim_type = 'public_statement'",
        cid_list,
    ).fetchone()[0]

    stats["verifiable_claims_count"] = conn.execute(
        f"""
        SELECT COUNT(DISTINCT c.id) FROM claims c
        LEFT JOIN evidence_links el ON el.claim_id = c.id
        WHERE c.content_item_id IN ({placeholders}) AND el.id IS NOT NULL
        """,
        cid_list,
    ).fetchone()[0]

    stats["confirmed_contradictions"] = conn.execute(
        f"""
        SELECT COUNT(*) FROM quotes q
        WHERE q.content_item_id IN ({placeholders}) AND q.is_flagged = 1
          AND q.rhetoric_class LIKE '%contradiction%'
        """,
        cid_list,
    ).fetchone()[0]

    stats["flagged_statements_count"] = conn.execute(
        f"""
        SELECT COUNT(*) FROM quotes q
        WHERE q.content_item_id IN ({placeholders}) AND q.is_flagged = 1
        """,
        cid_list,
    ).fetchone()[0]

    stats["votes_tracked_count"] = conn.execute(
        f"SELECT COUNT(*) FROM claims WHERE content_item_id IN ({placeholders}) AND claim_type = 'vote_record'",
        cid_list,
    ).fetchone()[0]

    stats["linked_cases_count"] = conn.execute(
        f"""
        SELECT COUNT(DISTINCT cc.case_id) FROM case_claims cc
        JOIN claims c ON c.id = cc.claim_id
        WHERE c.content_item_id IN ({placeholders})
        """,
        cid_list,
    ).fetchone()[0]

    stats["promises_made_count"] = conn.execute(
        f"""
        SELECT COUNT(*) FROM quotes q
        WHERE q.content_item_id IN ({placeholders}) AND q.rhetoric_class LIKE '%promise%'
        """,
        cid_list,
    ).fetchone()[0]

    stats["promises_kept_count"] = conn.execute(
        f"""
        SELECT COUNT(*) FROM claims c
        JOIN evidence_links el ON el.claim_id = c.id
        WHERE c.content_item_id IN ({placeholders})
          AND c.claim_type = 'public_statement'
          AND c.status = 'confirmed'
        """,
        cid_list,
    ).fetchone()[0]

    return stats


def compute_accountability_score(stats: Dict) -> float:
    speeches = stats.get("public_speeches_count", 0)
    verifiable = stats.get("verifiable_claims_count", 0)
    contradictions = stats.get("confirmed_contradictions", 0)
    flagged = stats.get("flagged_statements_count", 0)
    votes = stats.get("votes_tracked_count", 0)
    cases = stats.get("linked_cases_count", 0)
    promises_made = stats.get("promises_made_count", 0)
    promises_kept = stats.get("promises_kept_count", 0)

    if speeches == 0 and votes == 0:
        return 0.0

    transparency = min(1.0, (verifiable + votes) / max(1, speeches + votes))

    credibility_penalty = min(1.0, flagged * 0.05 + contradictions * 0.15)

    accountability_risk = min(1.0, cases * 0.1)

    if promises_made > 0:
        promise_fulfillment = promises_kept / promises_made
    else:
        promise_fulfillment = 0.5

    score = (transparency * 40) + (promise_fulfillment * 30) - (credibility_penalty * 20) - (accountability_risk * 10)

    return max(0.0, min(100.0, score))


def compute_all_indices(settings: dict = None, period: str = None) -> int:
    if settings is None:
        settings = load_settings()

    if period is None:
        period = datetime.now().strftime("%Y-%m")

    conn = get_db(settings)

    deputies = conn.execute(
        """
        SELECT dp.id, dp.entity_id, dp.full_name, dp.faction, dp.is_active
        FROM deputy_profiles dp
        WHERE dp.is_active = 1
        """
    ).fetchall()

    if not deputies:
        log.info("No active deputy profiles found")
        conn.close()
        return 0

    log.info("Computing accountability index for %d deputies (period=%s)", len(deputies), period)

    computed = 0
    for dep in deputies:
        deputy_id = dep["id"]
        entity_id = dep["entity_id"]

        stats = _get_deputy_stats(conn, deputy_id, entity_id, period)
        score = compute_accountability_score(stats)

        existing = conn.execute(
            "SELECT id FROM accountability_index WHERE deputy_id=? AND period=?",
            (deputy_id, period),
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE accountability_index SET
                   public_speeches_count=?, verifiable_claims_count=?,
                   confirmed_contradictions=?, flagged_statements_count=?,
                   votes_tracked_count=?, linked_cases_count=?,
                   promises_made_count=?, promises_kept_count=?,
                   calculated_score=?
                   WHERE deputy_id=? AND period=?""",
                (
                    stats["public_speeches_count"], stats["verifiable_claims_count"],
                    stats["confirmed_contradictions"], stats["flagged_statements_count"],
                    stats["votes_tracked_count"], stats["linked_cases_count"],
                    stats["promises_made_count"], stats["promises_kept_count"],
                    score, deputy_id, period,
                ),
            )
        else:
            conn.execute(
                """INSERT INTO accountability_index(
                    deputy_id, period, public_speeches_count, verifiable_claims_count,
                    confirmed_contradictions, flagged_statements_count,
                    votes_tracked_count, linked_cases_count,
                    promises_made_count, promises_kept_count, calculated_score
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    deputy_id, period,
                    stats["public_speeches_count"], stats["verifiable_claims_count"],
                    stats["confirmed_contradictions"], stats["flagged_statements_count"],
                    stats["votes_tracked_count"], stats["linked_cases_count"],
                    stats["promises_made_count"], stats["promises_kept_count"],
                    score,
                ),
            )

        computed += 1

    conn.commit()

    top_scores = conn.execute(
        """
        SELECT dp.full_name, dp.faction, ai.calculated_score, ai.flagged_statements_count, ai.linked_cases_count
        FROM accountability_index ai
        JOIN deputy_profiles dp ON dp.id = ai.deputy_id
        WHERE ai.period = ?
        ORDER BY ai.calculated_score ASC
        LIMIT 10
        """,
        (period,),
    ).fetchall()

    if top_scores:
        log.info("Bottom 10 accountability scores (%s):", period)
        for r in top_scores:
            log.info("  %s (%s): %.1f (flagged=%d, cases=%d)", r[0], r[1], r[2], r[3], r[4])

    conn.close()
    return computed


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    compute_all_indices()


if __name__ == "__main__":
    main()
