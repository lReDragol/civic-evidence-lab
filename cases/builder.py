import json
import logging
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

LOW_SIGNAL_CLAIMS = {
    "заявил",
    "сказал",
    "сообщил",
    "пообещал",
    "обещал",
    "допрос",
    "допроса",
    "задержан",
    "задержали",
    "арестован",
    "арестовали",
    "владеет",
    "пригрозил",
}


def _get_entity_name(conn: sqlite3.Connection, entity_id: int) -> str:
    row = conn.execute("SELECT canonical_name FROM entities WHERE id=?", (entity_id,)).fetchone()
    return row[0] if row else f"entity_{entity_id}"


def _find_entity_clusters(conn: sqlite3.Connection) -> Dict[int, Set[int]]:
    rows = conn.execute(
        """
        SELECT c.id, em.entity_id
        FROM claims c
        JOIN content_items ci ON ci.id = c.content_item_id
        JOIN entity_mentions em ON em.content_item_id = ci.id
        JOIN entities e ON e.id = em.entity_id
        WHERE e.entity_type = 'person'
          AND c.status != 'confirmed'
        """
    ).fetchall()

    person_claims = defaultdict(set)
    for row in rows:
        person_claims[row["entity_id"]].add(row["id"])

    return dict(person_claims)


def _find_topic_clusters(conn: sqlite3.Connection) -> List[Dict]:
    rows = conn.execute(
        """
        SELECT ct.tag_name, ct.content_item_id, c.id as claim_id
        FROM content_tags ct
        JOIN claims c ON c.content_item_id = ct.content_item_id
        WHERE ct.tag_level = 1
          AND ct.tag_name IN ('corruption_claim', 'detention', 'court_decision',
                              'censorship_action', 'abuse_claim', 'procurement_claim')
        ORDER BY ct.tag_name, ct.content_item_id
        """
    ).fetchall()

    clusters = defaultdict(lambda: {"claim_ids": set(), "content_ids": set()})
    for row in rows:
        tag = row["tag_name"]
        clusters[tag]["claim_ids"].add(row["claim_id"])
        clusters[tag]["content_ids"].add(row["content_item_id"])
        clusters[tag]["tag"] = tag

    return list(clusters.values())


def _find_related_entities(conn: sqlite3.Connection, claim_ids: List[int]) -> List[int]:
    if not claim_ids:
        return []
    placeholders = ",".join("?" * len(claim_ids))
    rows = conn.execute(
        f"""
        SELECT DISTINCT em.entity_id
        FROM entity_mentions em
        JOIN entities e ON e.id = em.entity_id
        JOIN claims c ON c.content_item_id = em.content_item_id
        WHERE c.id IN ({placeholders}) AND e.entity_type IN ('person', 'organization')
        """,
        claim_ids,
    ).fetchall()
    return [r[0] for r in rows]


def _normalize_claim_text(text: str) -> str:
    value = re.sub(r"^[^\wА-Яа-яЁё]+", "", str(text or "").strip(), flags=re.UNICODE)
    value = re.sub(r"\s+", " ", value)
    return value.strip(" \t\r\n.,;:!?-–—\"'«»()[]")


def _is_low_signal_claim(text: str) -> bool:
    cleaned = _normalize_claim_text(text)
    if not cleaned:
        return True
    normalized = cleaned.casefold()
    if normalized in LOW_SIGNAL_CLAIMS:
        return True
    words = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", cleaned)
    alpha_words = [word for word in words if re.search(r"[A-Za-zА-Яа-яЁё]", word)]
    if len(alpha_words) <= 1 and len(cleaned) <= 18:
        return True
    if len(alpha_words) <= 2 and len(cleaned) <= 22 and not any(ch.isdigit() for ch in cleaned):
        return True
    return False


def _claim_rank(row: sqlite3.Row) -> Tuple[int, int, int, int]:
    status = str(row["status"] or "").strip().lower()
    status_score = {"verified": 4, "confirmed": 4, "partially_confirmed": 3, "open": 2, "unverified": 1}.get(status, 0)
    return (
        int(row["evidence_count"] or 0),
        status_score,
        len(_normalize_claim_text(row["claim_text"])),
        int(row["id"] or 0),
    )


def _filter_claim_ids(conn: sqlite3.Connection, claim_ids: List[int]) -> List[int]:
    if not claim_ids:
        return []
    placeholders = ",".join("?" * len(claim_ids))
    rows = conn.execute(
        f"""
        SELECT cl.id, cl.claim_text, cl.status,
               (SELECT COUNT(*) FROM evidence_links el WHERE el.claim_id = cl.id) AS evidence_count
        FROM claims cl
        WHERE cl.id IN ({placeholders})
        ORDER BY cl.id DESC
        """,
        claim_ids,
    ).fetchall()
    best_by_text: Dict[str, sqlite3.Row] = {}
    for row in rows:
        cleaned = _normalize_claim_text(row["claim_text"])
        if _is_low_signal_claim(cleaned):
            continue
        key = cleaned.casefold()
        current = best_by_text.get(key)
        if current is None or _claim_rank(row) > _claim_rank(current):
            best_by_text[key] = row
    return [int(row["id"]) for row in best_by_text.values()]


def _has_evidence(conn: sqlite3.Connection, claim_ids: List[int]) -> bool:
    if not claim_ids:
        return False
    placeholders = ",".join("?" * len(claim_ids))
    row = conn.execute(
        f"SELECT COUNT(*) FROM evidence_links WHERE claim_id IN ({placeholders})",
        claim_ids,
    ).fetchone()
    return row[0] > 0


CASE_TYPE_MAP = {
    "corruption_claim": "corruption",
    "detention": "repression",
    "court_decision": "legal",
    "censorship_action": "censorship",
    "abuse_claim": "abuse",
    "procurement_claim": "procurement_fraud",
}


def build_cases_from_entities(settings: dict = None, min_claims: int = 3) -> int:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    person_claims = _find_entity_clusters(conn)
    cases_created = 0

    for entity_id, claim_ids in person_claims.items():
        claim_ids = _filter_claim_ids(conn, list(claim_ids))
        if len(claim_ids) < min_claims:
            continue

        entity_name = _get_entity_name(conn, entity_id)
        has_ev = _has_evidence(conn, list(claim_ids))

        existing = conn.execute(
            "SELECT id FROM cases WHERE title LIKE ?", (f"%{entity_name}%",)
        ).fetchone()
        if existing:
            case_id = existing[0]
            for cid in claim_ids:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO case_claims(case_id, claim_id) VALUES(?,?)",
                        (case_id, cid),
                    )
                except Exception:
                    pass
            continue

        claim_list = list(claim_ids)
        rows = conn.execute(
            f"SELECT claim_type, COUNT(*) FROM claims WHERE id IN ({','.join('?' * len(claim_list))}) GROUP BY claim_type ORDER BY COUNT(*) DESC",
            claim_list,
        ).fetchall()

        case_type = rows[0][0] if rows else "unknown"
        case_type = CASE_TYPE_MAP.get(case_type, case_type)

        evidence_str = " (с доказательствами)" if has_ev else ""
        title = f"Дело: {entity_name}{evidence_str}"

        description_parts = []
        for r in rows[:5]:
            description_parts.append(f"{r[0]}: {r[1]} утверждений")
        description = "; ".join(description_parts)

        related_ents = _find_related_entities(conn, claim_list[:20])

        region_row = conn.execute(
            """
            SELECT ct.tag_name
            FROM content_tags ct
            JOIN claims c ON c.content_item_id = ct.content_item_id
            WHERE c.id IN ({}) AND ct.tag_level = 2
            LIMIT 1
            """.format(",".join("?" * min(5, len(claim_list)))),
            claim_list[:5],
        ).fetchone()
        region = region_row[0] if region_row else None

        cur = conn.execute(
            """INSERT INTO cases(title, description, case_type, status, region, started_at)
               VALUES(?,?,?,?,?,?)""",
            (title, description, case_type, "open" if has_ev else "draft", region, datetime.now().isoformat()),
        )
        case_id = cur.lastrowid

        for cid in claim_ids:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO case_claims(case_id, claim_id, role) VALUES(?,?,?)",
                    (case_id, cid, "central" if has_ev else "allegation"),
                ),
            except Exception:
                pass

        cases_created += 1

    conn.commit()
    log.info("Built %d entity-based cases", cases_created)

    topic_clusters = _find_topic_clusters(conn)
    topic_cases = 0

    for cluster in topic_clusters:
        claim_ids = _filter_claim_ids(conn, list(cluster["claim_ids"]))
        if len(claim_ids) < 5:
            continue

        tag = cluster["tag"]
        case_type = CASE_TYPE_MAP.get(tag, tag)

        existing = conn.execute(
            "SELECT id FROM cases WHERE case_type=? AND status != 'closed' LIMIT 1",
            (case_type,),
        ).fetchone()
        if existing:
            for cid in claim_ids:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO case_claims(case_id, claim_id) VALUES(?,?)",
                        (existing[0], cid),
                    )
                except Exception:
                    pass
            continue

        has_ev = _has_evidence(conn, claim_ids)
        title = f"Тематическое дело: {tag} ({len(claim_ids)} утверждений)"

        cur = conn.execute(
            """INSERT INTO cases(title, description, case_type, status, started_at)
               VALUES(?,?,?,?,?)""",
            (title, f"Автоматически сгруппировано по тегу {tag}", case_type, "open" if has_ev else "draft", datetime.now().isoformat()),
        )
        case_id = cur.lastrowid

        for cid in claim_ids:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO case_claims(case_id, claim_id) VALUES(?,?)",
                    (case_id, cid),
                )
            except Exception:
                pass

        topic_cases += 1

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
    log.info("Cases: %d entity-based, %d topic-based, %d total", cases_created, topic_cases, total)
    conn.close()
    return total


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    build_cases_from_entities()


if __name__ == "__main__":
    main()
