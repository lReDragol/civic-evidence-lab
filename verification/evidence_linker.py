import json
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

OFFICIAL_CONTENT_TYPES = {
    "registry_record", "court_record", "enforcement", "procurement",
    "bill", "transcript", "deputy_profile",
}

OFFICIAL_SOURCE_CATEGORIES = {"official_registry", "official_site"}

EVIDENCE_TYPE_MAP = {
    "registry_record": "official_registry",
    "court_record": "court_document",
    "enforcement": "enforcement_record",
    "procurement": "procurement_record",
    "bill": "legislative_document",
    "transcript": "official_transcript",
    "deputy_profile": "official_profile",
}

STRENGTH_MAP = {
    "registry_record": "strong",
    "court_record": "strong",
    "enforcement": "strong",
    "procurement": "moderate",
    "bill": "moderate",
    "transcript": "strong",
    "deputy_profile": "strong",
}

SEED_EVIDENCE_TYPES = {
    "co_entity",
    "co_mention",
    "content_similarity",
    "mentioned_together",
}

HARD_EVIDENCE_TYPES = {
    "official_registry",
    "court_document",
    "enforcement_record",
    "procurement_record",
    "legislative_document",
    "official_transcript",
    "official_profile",
    "registry_record",
    "court_record",
    "enforcement",
    "procurement",
    "bill",
    "transcript",
    "vote_record",
}


def _score_evidence_item(ctype: str, source_category: str, credibility_tier: str,
                         same_entity_count: int) -> float:
    score = 0.0
    if ctype in OFFICIAL_CONTENT_TYPES:
        score += 3.0
    if source_category in OFFICIAL_SOURCE_CATEGORIES:
        score += 2.0
    if credibility_tier == "A":
        score += 1.0
    elif credibility_tier == "B":
        score += 0.5
    score += min(same_entity_count, 5) * 0.5
    return score


def _evidence_class_for(
    *,
    evidence_type: str | None,
    content_type: str | None,
    source_category: str | None,
    strength: str | None,
) -> str:
    evidence_type = str(evidence_type or "")
    content_type = str(content_type or "")
    source_category = str(source_category or "")
    strength = str(strength or "")

    if evidence_type in SEED_EVIDENCE_TYPES:
        return "seed"
    if (
        evidence_type in HARD_EVIDENCE_TYPES
        or content_type in OFFICIAL_CONTENT_TYPES
        or source_category in OFFICIAL_SOURCE_CATEGORIES
        or strength == "strong"
    ):
        return "hard"
    return "support"


def auto_link_evidence(settings: dict = None, batch_size: int = 200) -> Dict:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    claims = conn.execute(
        """
        SELECT c.id, c.content_item_id
        FROM claims c
        WHERE c.status = 'unverified' OR c.needs_review = 1
        ORDER BY c.id DESC
        LIMIT ?
        """,
        (batch_size,),
    ).fetchall()

    if not claims:
        log.info("No unverified claims to link evidence for")
        conn.close()
        return {"claims_processed": 0, "evidence_links_added": 0}

    log.info("Auto-linking evidence for %d claims (entity-mentions strategy)", len(claims))

    claims_processed = 0
    links_added = 0

    for claim_id, content_item_id in claims:
        claim_entities = conn.execute(
            "SELECT entity_id FROM entity_mentions WHERE content_item_id = ?",
            (content_item_id,),
        ).fetchall()
        if not claim_entities:
            continue

        entity_ids = [r[0] for r in claim_entities]
        placeholders = ",".join("?" for _ in entity_ids)

        evidence_rows = conn.execute(
            f"""
            SELECT
                em2.content_item_id,
                ci.content_type,
                s.category as source_category,
                s.credibility_tier,
                COUNT(DISTINCT em2.entity_id) as shared_entities
            FROM entity_mentions em2
            JOIN content_items ci ON ci.id = em2.content_item_id
            JOIN sources s ON s.id = ci.source_id
            WHERE em2.entity_id IN ({placeholders})
              AND em2.content_item_id != ?
            GROUP BY em2.content_item_id
            HAVING shared_entities >= 1
            ORDER BY shared_entities DESC
            LIMIT 20
            """,
            entity_ids + [content_item_id],
        ).fetchall()

        if not evidence_rows:
            continue

        best_evidence = []
        for ev_item_id, ctype, src_cat, cred_tier, shared in evidence_rows:
            score = _score_evidence_item(ctype, src_cat, cred_tier, shared)
            if score < 1.0:
                continue

            ev_type = "official_document"
            strength = "weak"
            if ctype in EVIDENCE_TYPE_MAP:
                ev_type = EVIDENCE_TYPE_MAP[ctype]
            if ctype in STRENGTH_MAP:
                strength = STRENGTH_MAP[ctype]
            elif src_cat in OFFICIAL_SOURCE_CATEGORIES:
                strength = "moderate"
            if score >= 5.0:
                strength = "strong"
            elif score >= 3.0 and strength == "weak":
                strength = "moderate"

            best_evidence.append({
                "content_item_id": ev_item_id,
                "type": ev_type,
                "strength": strength,
                "notes": f"Entity co-mention ({shared} shared) [{ctype}] score={score:.1f}",
                "score": score,
            })

        best_evidence.sort(key=lambda e: e["score"], reverse=True)
        best_evidence = best_evidence[:5]

        for ev in best_evidence:
            existing = conn.execute(
                "SELECT id FROM evidence_links WHERE claim_id = ? AND evidence_item_id = ?",
                (claim_id, ev["content_item_id"]),
            ).fetchone()
            if existing:
                continue

            conn.execute(
                """INSERT INTO evidence_links(claim_id, evidence_item_id, evidence_type, evidence_class, strength, notes)
                   VALUES(?,?,?,?,?,?)""",
                (claim_id, ev["content_item_id"], ev["type"], "seed", ev["strength"], ev["notes"]),
            )
            links_added += 1

        if best_evidence:
            claims_processed += 1
            max_score = max(e["score"] for e in best_evidence)
            if max_score >= 3.0:
                conn.execute(
                    "UPDATE claims SET document_score = COALESCE(document_score, 0) + 0.3 WHERE id = ?",
                    (claim_id,),
                )
            if max_score >= 5.0:
                conn.execute(
                    "UPDATE claims SET document_score = COALESCE(document_score, 0) + 0.3 WHERE id = ?",
                    (claim_id,),
                )

    conn.commit()

    log.info("Entity-mentions linking done: %d claims got evidence, %d links added",
             claims_processed, links_added)

    conn.close()
    return {"claims_processed": claims_processed, "evidence_links_added": links_added}


def auto_link_by_content_type(settings: dict = None) -> Dict:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    log.info("Linking official content items as evidence by shared entities")

    official_items = conn.execute(
        """
        SELECT ci.id, ci.content_type, ci.title, ci.source_id
        FROM content_items ci
        WHERE ci.content_type IN ({})
        LIMIT 500
        """.format(",".join(f"'{t}'" for t in OFFICIAL_CONTENT_TYPES)),
    ).fetchall()

    if not official_items:
        log.info("No official content items found")
        conn.close()
        return {"items_linked": 0}

    log.info("Found %d official items", len(official_items))

    items_linked = 0
    for item_id, ctype, title, source_id in official_items:
        item_entities = conn.execute(
            "SELECT entity_id FROM entity_mentions WHERE content_item_id = ?",
            (item_id,),
        ).fetchall()
        if not item_entities:
            continue

        entity_ids = [r[0] for r in item_entities]
        placeholders = ",".join("?" for _ in entity_ids)

        claim_rows = conn.execute(
            f"""
            SELECT DISTINCT c.id
            FROM claims c
            JOIN entity_mentions em ON em.content_item_id = c.content_item_id
            WHERE em.entity_id IN ({placeholders})
              AND (c.status = 'unverified' OR c.needs_review = 1)
              AND NOT EXISTS (
                  SELECT 1 FROM evidence_links el
                  WHERE el.claim_id = c.id AND el.evidence_item_id = ?
              )
            LIMIT 20
            """,
            entity_ids + [item_id],
        ).fetchall()

        ev_type = EVIDENCE_TYPE_MAP.get(ctype, "official_document")
        strength = STRENGTH_MAP.get(ctype, "moderate")

        for claim in claim_rows:
            conn.execute(
                """INSERT OR IGNORE INTO evidence_links(claim_id, evidence_item_id, evidence_type, evidence_class, strength, notes)
                   VALUES(?,?,?,?,?,?)""",
                (
                    claim[0],
                    item_id,
                    ev_type,
                    "hard" if ctype in {"registry_record", "court_record", "enforcement", "procurement", "bill", "transcript"} else "support",
                    strength,
                    f"Auto-linked from {ctype}: {str(title or '')[:50]}",
                ),
            )
            items_linked += 1

    conn.commit()

    log.info("Official items linked: %d evidence links", items_linked)

    stats = {"items_linked": items_linked}
    conn.close()
    return stats


def backfill_evidence_classes(settings: dict = None) -> Dict:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)
    try:
        rows = conn.execute(
            """
            SELECT
                el.id,
                el.evidence_type,
                el.strength,
                COALESCE(ci.content_type, '') AS content_type,
                COALESCE(s.category, '') AS source_category
            FROM evidence_links el
            LEFT JOIN content_items ci ON ci.id = el.evidence_item_id
            LEFT JOIN sources s ON s.id = ci.source_id
            """
        ).fetchall()

        updated = 0
        for evidence_id, evidence_type, strength, content_type, source_category in rows:
            evidence_class = _evidence_class_for(
                evidence_type=evidence_type,
                content_type=content_type,
                source_category=source_category,
                strength=strength,
            )
            cur = conn.execute(
                "UPDATE evidence_links SET evidence_class=? WHERE id=? AND COALESCE(evidence_class, '') != ?",
                (evidence_class, evidence_id, evidence_class),
            )
            updated += max(0, int(cur.rowcount or 0))

        conn.commit()
        return {"evidence_links_scanned": len(rows), "evidence_classes_updated": updated}
    finally:
        conn.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--claims", action="store_true")
    parser.add_argument("--official", action="store_true")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    if args.all or args.claims:
        result1 = auto_link_evidence()
        print(json.dumps(result1, ensure_ascii=False))

    if args.all or args.official:
        result2 = auto_link_by_content_type()
        print(json.dumps(result2, ensure_ascii=False))

    if args.all:
        result3 = backfill_evidence_classes()
        print(json.dumps(result3, ensure_ascii=False))


if __name__ == "__main__":
    main()
