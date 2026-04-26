import json
import logging
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

OFFICIAL_CATEGORIES = {"official_registry", "official_site"}
STRONG_EVIDENCE_TYPES = {"registry_record", "court_record", "enforcement", "procurement", "bill", "transcript"}


def _claims_have_cluster_column(conn) -> bool:
    return any(row[1] == "claim_cluster_id" for row in conn.execute("PRAGMA table_info(claims)").fetchall())


def _extract_key_terms(text: str, limit: int = 8) -> List[str]:
    terms = []
    terms.extend(re.findall(r'\b(\d{10}|\d{12})\b', text))
    for m in re.finditer(r'\b([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){1,2})\b', text):
        name = m.group(1).strip()
        parts = name.split()
        if 2 <= len(parts) <= 3 and all(len(p) > 1 for p in parts):
            terms.append(name)
    for m in re.finditer(r'(\d+[-–]\d+[-–]\d+)', text):
        terms.append(m.group(1))
    for m in re.finditer(r'(?:ФЗ|УК|КоАП|ГК|НК|ТК|ЖК)\s*РФ', text, re.I):
        terms.append(m.group(0))
    for m in re.finditer(r'\b(НДС|ВВП|ЖКХ|МВД|ФСБ|РКН|СК РФ|ФАС|ФССП)\b', text):
        terms.append(m.group(0))
    return list(dict.fromkeys(terms))[:limit]


def find_corroborating_sources(conn, claim_id: int) -> Dict:
    claim_row = conn.execute(
        "SELECT c.id, c.source_id, c.body_text, c.title, s.category, s.political_alignment "
        "FROM claims cl "
        "JOIN content_items c ON c.id = cl.content_item_id "
        "JOIN sources s ON s.id = c.source_id "
        "WHERE cl.id=?",
        (claim_id,),
    ).fetchone()
    if not claim_row:
        return {"corroboration_score": 0.0, "corroborating_sources": []}

    source_content_id, source_id, body_text, title, source_category, source_alignment = claim_row

    claim_row2 = conn.execute(
        "SELECT claim_text, claim_type, claim_cluster_id FROM claims WHERE id=?",
        (claim_id,),
    ).fetchone()
    claim_text = claim_row2[0] if claim_row2 else ""
    claim_type = claim_row2[1] if claim_row2 else ""
    claim_cluster_id = claim_row2[2] if claim_row2 and len(claim_row2) >= 3 else None

    key_terms = _extract_key_terms(f"{title or ''}\n{claim_text}")
    if not key_terms:
        key_terms = _extract_key_terms(body_text or "")
    if not key_terms:
        return {"corroboration_score": 0.0, "corroborating_sources": []}

    entity_ids = [r[0] for r in conn.execute(
        "SELECT entity_id FROM entity_mentions WHERE content_item_id=?", (source_content_id,)
    ).fetchall()]

    corroborating = []

    if claim_cluster_id and _claims_have_cluster_column(conn):
        cluster_rows = conn.execute(
            """
            SELECT c.id, s.id, s.name, s.category, s.political_alignment, s.credibility_tier,
                   c.content_type, c.title
            FROM claims cl
            JOIN content_items c ON c.id = cl.content_item_id
            JOIN sources s ON s.id = c.source_id
            WHERE cl.claim_cluster_id=?
              AND c.id != ?
            LIMIT 50
            """,
            (claim_cluster_id, source_content_id),
        ).fetchall()
        for row in cluster_rows:
            c_id, s_id, s_name, s_cat, s_align, s_tier, c_type, c_title = row
            corroborating.append({
                "content_item_id": c_id,
                "source_id": s_id,
                "source_name": s_name,
                "source_category": s_cat,
                "political_alignment": s_align,
                "credibility_tier": s_tier,
                "content_type": c_type,
                "title": c_title,
                "match_method": "claim_cluster_overlap",
            })

    if entity_ids:
        entity_results = conn.execute(
            """
            SELECT c.id, s.id, s.name, s.category, s.political_alignment, s.credibility_tier,
                   c.content_type, c.title
            FROM entity_mentions em
            JOIN content_items c ON c.id = em.content_item_id
            JOIN sources s ON s.id = c.source_id
            WHERE em.entity_id IN ({})
            AND c.id != ?
            AND c.id NOT IN (SELECT evidence_item_id FROM evidence_links WHERE claim_id=?)
            GROUP BY c.id
            LIMIT 50
            """.format(",".join("?" for _ in entity_ids)),
            entity_ids + [source_content_id, claim_id],
        ).fetchall()

        for row in entity_results:
            c_id, s_id, s_name, s_cat, s_align, s_tier, c_type, c_title = row
            corroborating.append({
                "content_item_id": c_id,
                "source_id": s_id,
                "source_name": s_name,
                "source_category": s_cat,
                "political_alignment": s_align,
                "credibility_tier": s_tier,
                "content_type": c_type,
                "title": c_title,
                "match_method": "entity_overlap",
            })

    for term in key_terms[:5]:
        like = f"%{term}%"
        term_results = conn.execute(
            """
            SELECT c.id, s.id, s.name, s.category, s.political_alignment, s.credibility_tier,
                   c.content_type, c.title
            FROM content_items c
            JOIN sources s ON s.id = c.source_id
            WHERE (c.title LIKE ? OR c.body_text LIKE ?)
            AND c.id != ?
            AND c.id NOT IN (SELECT evidence_item_id FROM evidence_links WHERE claim_id=?)
            LIMIT 20
            """,
            (like, like, source_content_id, claim_id),
        ).fetchall()

        for row in term_results:
            c_id, s_id, s_name, s_cat, s_align, s_tier, c_type, c_title = row
            if not any(c["content_item_id"] == c_id for c in corroborating):
                corroborating.append({
                    "content_item_id": c_id,
                    "source_id": s_id,
                    "source_name": s_name,
                    "source_category": s_cat,
                    "political_alignment": s_align,
                    "credibility_tier": s_tier,
                    "content_type": c_type,
                    "title": c_title,
                    "match_method": "term_overlap",
                })

    unique_sources = {}
    for c in corroborating:
        sid = c["source_id"]
        if sid not in unique_sources:
            unique_sources[sid] = c

    unique_categories = set(c["source_category"] for c in unique_sources.values() if c["source_category"])
    unique_alignments = set(c["political_alignment"] for c in unique_sources.values() if c["political_alignment"])
    n_sources = len(unique_sources)

    score = 0.0
    if n_sources >= 3:
        score += 0.3
    elif n_sources >= 2:
        score += 0.2

    if len(unique_categories) >= 2:
        score += 0.3
    if len(unique_categories) >= 3:
        score += 0.1
    if len(unique_alignments) >= 2:
        score += 0.2
    if n_sources >= 5:
        score += 0.1

    for c in unique_sources.values():
        if c["source_category"] in OFFICIAL_CATEGORIES:
            score += 0.1
            break
    for c in unique_sources.values():
        if c["content_type"] in STRONG_EVIDENCE_TYPES:
            score += 0.1
            break

    score = min(1.0, score)

    return {
        "corroboration_score": score,
        "n_independent_sources": n_sources,
        "unique_categories": list(unique_categories),
        "unique_alignments": list(unique_alignments),
        "corroborating_sources": list(unique_sources.values())[:10],
    }


def run_cross_source_corroboration(settings=None, limit: int = 500, min_status: str = "unverified"):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)

    status_order = {"unverified": 0, "likely_false": 1, "partially_confirmed": 2,
                    "likely_true": 3, "confirmed": 4}
    min_level = status_order.get(min_status, 0)

    rows = conn.execute(
        "SELECT id, status FROM claims WHERE needs_review=1 LIMIT ?",
        (limit,),
    ).fetchall()

    if not rows:
        rows = conn.execute("SELECT id, status FROM claims LIMIT ?", (limit,)).fetchall()

    log.info("Running cross-source corroboration on %d claims", len(rows))

    updated = 0
    new_evidence = 0

    for claim_id, current_status in rows:
        try:
            result = find_corroborating_sources(conn, claim_id)
            score = result["corroboration_score"]

            if score > 0:
                conn.execute(
                    "UPDATE claims SET cross_source_score=? WHERE id=?",
                    (score, claim_id),
                )

                for c_source in result["corroborating_sources"][:5]:
                    c_id = c_source["content_item_id"]
                    existing = conn.execute(
                        "SELECT id FROM evidence_links WHERE claim_id=? AND evidence_item_id=?",
                        (claim_id, c_id),
                    ).fetchone()
                    if not existing:
                        strength = "strong" if c_source["source_category"] in OFFICIAL_CATEGORIES else "moderate"
                        conn.execute(
                            "INSERT INTO evidence_links(claim_id, evidence_item_id, evidence_type, evidence_class, strength, notes, linked_by) VALUES(?,?,?,?,?,?,?)",
                            (
                             claim_id,
                             c_id,
                             "cross_source_corroboration",
                             "hard" if c_source["source_category"] in OFFICIAL_CATEGORIES or c_source["content_type"] in STRONG_EVIDENCE_TYPES else "support",
                             strength,
                             json.dumps({"source_name": c_source["source_name"],
                                         "source_category": c_source["source_category"],
                                         "match_method": c_source["match_method"]},
                                        ensure_ascii=False),
                             "corroboration_engine"),
                        )
                        new_evidence += 1

                from verification.authenticity_model import compute_authenticity_score, store_verification
                old_status = current_status
                new_result = compute_authenticity_score(conn, claim_id)
                new_status = new_result["status"]
                if old_status != new_status:
                    store_verification(conn, claim_id, "cross_source_corroboration", old_status, new_status,
                                       notes=f"corroboration_score={score:.3f}")
                    conn.execute("UPDATE claims SET status=? WHERE id=?", (new_status, claim_id))

                updated += 1

        except Exception as e:
            log.warning("Corroboration failed for claim %d: %s", claim_id, e)
            continue

    conn.commit()
    log.info("Cross-source corroboration: %d claims updated, %d new evidence links", updated, new_evidence)
    conn.close()
    return {"claims_updated": updated, "new_evidence_links": new_evidence}


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()

    settings = load_settings()
    result = run_cross_source_corroboration(settings, limit=args.limit)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
