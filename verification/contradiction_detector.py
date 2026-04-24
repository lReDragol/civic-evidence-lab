import json
import logging
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

CLAIM_VOTE_CONTRADICTIONS = [
    (re.compile(r"голосовал\s+за|поддержал\s+закон|проголосовал\s+за", re.I), "за"),
    (re.compile(r"голосовал\s+против|выступил\s+против|отклонил|проголосовал\s+против", re.I), "против"),
    (re.compile(r"воздержался|не\s+голосовал|отсутствовал", re.I), "воздержался"),
]

QUOTE_CONTRADICTION_PAIRS = [
    (re.compile(r"снизил[аи]?\s+налог|уменьшил[аи]?\s+налог", re.I),
     re.compile(r"налог\s+вырос|увеличени[ея]\s+налог|повышени[ея]\s+налог", re.I)),
    (re.compile(r"поддержал[аи]?\s+(?:закон|законопроект|инициатив)", re.I),
     re.compile(r"против\s+(?:законопроект|инициатив|реформ)", re.I)),
    (re.compile(r"доход\s+составил|заявленн[ыый]+\s+доход|заработал", re.I),
     re.compile(r"скрыл[аи]?\s+доход|незадекларирован|превышени[ея]\s+доход", re.I)),
    (re.compile(r"не\s+име[еюю т]*\s+собственност|ничего\s+не\s+владе", re.I),
     re.compile(r"владеет|собственник|принадлежит", re.I)),
    (re.compile(r"бор[бь][еюю т]*\s+с\s+коррупц", re.I),
     re.compile(r"коррупц|взятк|откат|распил", re.I)),
]


def detect_claim_vote_contradictions(conn, entity_id: int) -> List[Dict]:
    contradictions = []
    claims = conn.execute(
        """
        SELECT cl.id, cl.claim_text, cl.claim_type
        FROM claims cl
        JOIN entity_mentions em ON em.content_item_id = cl.content_item_id
        WHERE em.entity_id = ? AND cl.claim_type IN ('vote_record', 'public_statement')
        ORDER BY cl.id DESC
        LIMIT 50
        """,
        (entity_id,),
    ).fetchall()

    if not claims:
        return contradictions

    votes = conn.execute(
        """
        SELECT bvs.bill_id, bvs.vote_date, bvs.vote_stage, bv.vote_result, b.number, b.title
        FROM bill_votes bv
        JOIN bill_vote_sessions bvs ON bvs.id = bv.vote_session_id
        JOIN bills b ON b.id = bvs.bill_id
        WHERE bv.entity_id = ?
        ORDER BY bvs.vote_date DESC
        LIMIT 50
        """,
        (entity_id,),
    ).fetchall()

    if not votes:
        return contradictions

    for claim in claims:
        claim_id, claim_text, claim_type = claim
        for pattern, expected_vote in CLAIM_VOTE_CONTRADICTIONS:
            if pattern.search(claim_text):
                for vote in votes:
                    bill_id, vote_date, stage, result, bill_number, bill_title = vote
                    if expected_vote != result and result in ("за", "против"):
                        contradiction_score = 0.7 if expected_vote == "за" and result == "против" else 0.5
                        contradictions.append({
                            "type": "claim_vs_vote",
                            "entity_id": entity_id,
                            "claim_id": claim_id,
                            "claim_text": claim_text[:200],
                            "claim_stated": expected_vote,
                            "vote_actual": result,
                            "bill_number": bill_number,
                            "bill_title": (bill_title or "")[:100],
                            "vote_date": vote_date,
                            "score": contradiction_score,
                        })
                break

    return contradictions


def detect_quote_contradictions(conn, entity_id: int) -> List[Dict]:
    contradictions = []
    quotes = conn.execute(
        """
        SELECT q.id, q.quote_text, q.rhetoric_class, ci.published_at
        FROM quotes q
        JOIN content_items ci ON ci.id = q.content_item_id
        WHERE q.entity_id = ?
        ORDER BY ci.published_at DESC
        LIMIT 50
        """,
        (entity_id,),
    ).fetchall()

    if len(quotes) < 2:
        return contradictions

    for i in range(len(quotes)):
        for j in range(i + 1, min(i + 10, len(quotes))):
            q1 = quotes[i]
            q2 = quotes[j]
            text1 = q1[1] or ""
            text2 = q2[1] or ""

            for pattern_a, pattern_b in QUOTE_CONTRADICTION_PAIRS:
                a_matches_1 = pattern_a.search(text1) and pattern_b.search(text2)
                a_matches_2 = pattern_a.search(text2) and pattern_b.search(text1)

                if a_matches_1 or a_matches_2:
                    contradictions.append({
                        "type": "quote_vs_quote",
                        "entity_id": entity_id,
                        "quote1_id": q1[0],
                        "quote1_text": text1[:200],
                        "quote1_date": q1[3] or "",
                        "quote2_id": q2[0],
                        "quote2_text": text2[:200],
                        "quote2_date": q2[3] or "",
                        "score": 0.5,
                    })
                    break

    return contradictions


def detect_income_contradictions(conn, entity_id: int) -> List[Dict]:
    contradictions = []
    profile = conn.execute(
        "SELECT dp.income_latest, dp.full_name FROM deputy_profiles dp WHERE dp.entity_id = ?",
        (entity_id,),
    ).fetchone()
    if not profile or not profile[0]:
        return contradictions

    declared_income = profile[0]
    name = profile[1]

    claims = conn.execute(
        """
        SELECT cl.id, cl.claim_text
        FROM claims cl
        JOIN entity_mentions em ON em.content_item_id = cl.content_item_id
        WHERE em.entity_id = ? AND cl.claim_type = 'corruption_claim'
        ORDER BY cl.id DESC
        LIMIT 20
        """,
        (entity_id,),
    ).fetchall()

    for claim in claims:
        claim_id, claim_text = claim
        income_m = re.search(r"доход\s*[:\-]?\s*(\d[\d\s]*(?:тыс|млн|млрд)?\.?\s*(?:руб|р\.))", claim_text, re.I)
        if income_m:
            claimed_income_str = income_m.group(1)
            contradictions.append({
                "type": "income_vs_declaration",
                "entity_id": entity_id,
                "claim_id": claim_id,
                "claim_text": claim_text[:200],
                "declared_income": declared_income,
                "claimed_income": claimed_income_str,
                "score": 0.4,
            })

    return contradictions


def store_contradictions(conn, contradictions: List[Dict]) -> int:
    stored = 0
    for c in contradictions:
        entity_id = c["entity_id"]
        c_type = c["type"]
        notes = json.dumps({k: v for k, v in c.items() if k not in ("entity_id", "type")}, ensure_ascii=False)

        existing = conn.execute(
            "SELECT id FROM entity_relations WHERE from_entity_id = ? AND relation_type = 'contradicts' "
            "AND detected_by = ?",
            (entity_id, c_type),
        ).fetchone()
        if existing:
            continue

        other_eid = entity_id
        if c_type == "claim_vs_vote":
            bill = conn.execute(
                "SELECT id FROM entities WHERE entity_type='law' AND canonical_name = ?",
                (c.get("bill_number", ""),),
            ).fetchone()
            if bill:
                other_eid = bill[0]
        elif c_type == "quote_vs_quote":
            other_eid = entity_id

        conn.execute(
            "INSERT INTO entity_relations(from_entity_id, to_entity_id, relation_type, "
            "evidence_item_id, strength, detected_by) VALUES(?,?,?,NULL,?,?,'contradiction_detector')",
            (entity_id, other_eid, "contradicts",
             "strong" if c.get("score", 0) >= 0.7 else "moderate",
             c_type),
        )
        stored += 1

        if c_type == "claim_vs_vote" and c.get("claim_id"):
            try:
                conn.execute(
                    "UPDATE claims SET contradiction_score = MAX(contradiction_score, ?) WHERE id = ?",
                    (c.get("score", 0.5), c["claim_id"]),
                )
            except Exception:
                pass

    return stored


def run_contradiction_detection(settings=None, entity_limit: int = 200) -> Dict:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)
    conn.row_factory = sqlite3.Row

    entities = conn.execute(
        """
        SELECT DISTINCT em.entity_id
        FROM entity_mentions em
        JOIN entities e ON e.id = em.entity_id
        WHERE e.entity_type = 'person'
        AND (
            EXISTS (SELECT 1 FROM bill_votes bv WHERE bv.entity_id = em.entity_id)
            OR EXISTS (SELECT 1 FROM quotes q WHERE q.entity_id = em.entity_id)
            OR EXISTS (SELECT 1 FROM claims cl JOIN entity_mentions em2 ON em2.content_item_id = cl.content_item_id WHERE em2.entity_id = em.entity_id)
        )
        LIMIT ?
        """,
        (entity_limit,),
    ).fetchall()

    total_contradictions = 0
    claim_vote = 0
    quote_quote = 0
    income = 0

    for row in entities:
        entity_id = row[0]

        cv = detect_claim_vote_contradictions(conn, entity_id)
        claim_vote += len(cv)

        qq = detect_quote_contradictions(conn, entity_id)
        quote_quote += len(qq)

        ic = detect_income_contradictions(conn, entity_id)
        income += len(ic)

        all_c = cv + qq + ic
        if all_c:
            stored = store_contradictions(conn, all_c)
            total_contradictions += stored

    conn.commit()
    conn.close()

    stats = {
        "entities_checked": len(entities),
        "total_contradictions": total_contradictions,
        "claim_vs_vote": claim_vote,
        "quote_vs_quote": quote_quote,
        "income_vs_declaration": income,
    }
    log.info("Contradiction detection: %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    result = run_contradiction_detection()
    print(json.dumps(result, ensure_ascii=False))
