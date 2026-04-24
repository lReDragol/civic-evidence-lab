import logging
import re
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

NEGATION_WORDS = [
    "не ", "нет ", "ни ", "никогда не ", "нельзя ", "отрицает", "опроверг",
    "не может быть", "не является", "не было", "не будет", "не существует",
    "вовсе не", "далеко не", "отнюдь не", "ничего не", "никак не",
    "не удалось", "не произошло", "не подтверд", "не согласен",
    "не признал", "не допускает", "не допускается", "запрещено не",
    "никто не", "нигде не", "нисколько не",
]

NEGATION_CONTEXT_WINDOW = 40

CLAIM_TYPE_NEGATION_MAP = {
    "detention": ["не арестован", "не задержан", "не взят под стражу", "не помещен в сизо", "не обыск"],
    "court_decision": ["не осужден", "не приговорен", "не осудил", "не лишен свободы"],
    "censorship_action": ["не заблокирован", "не запрещен", "не ограничен", "не признан экстремист", "не внесен в реестр"],
    "corruption_claim": ["не украл", "не похитил", "не растратил", "не освоил", "не распилил", "не было откат", "не было взятк"],
    "vote_record": ["не голосовал за", "не голосовал против", "не поддержал", "не отклонил"],
    "mobilization_claim": ["не мобилизован", "не призван", "не уклоняется от"],
    "public_statement": ["не заявлял", "не говорил", "не выступал"],
    "procurement_claim": ["не выиграл закупк", "не получил контракт"],
    "ownership_claim": ["не владеет", "не принадлежит"],
}

REBUTTAL_PATTERNS = [
    re.compile(r"(?:официально\s+)?опроверг(?:ает|ли|нул|ла|ло)?\s*(?:информац|заявлен|утвержд|слух|сообщ)", re.I),
    re.compile(r"не\s+соответствует\s+действительност", re.I),
    re.compile(r"(?:фейк|ложн|враньё|дезинформац)", re.I),
    re.compile(r"опроверг(?:ает|ли)?\s*(?:заявлен|утвержд|информац|сообщ)", re.I),
    re.compile(r"никакого\s+\w+\s+не\s+было", re.I),
    re.compile(r"никто\s+не\s+\w+(?:ал|ил|ел)", re.I),
]


def detect_negation(text: str, keyword_start: int, keyword_end: int) -> Tuple[bool, Optional[str]]:
    if keyword_start < 0 or not text:
        return False, None

    window_start = max(0, keyword_start - NEGATION_CONTEXT_WINDOW)
    prefix = text[window_start:keyword_start].lower()

    for neg_word in NEGATION_WORDS:
        neg_lower = neg_word.lower().strip()
        idx = prefix.rfind(neg_lower)
        if idx >= 0:
            gap = len(prefix) - idx - len(neg_lower)
            if gap <= 8:
                return True, neg_lower

    return False, None


def check_claim_type_negation(text: str, claim_type: str) -> Tuple[bool, Optional[str]]:
    patterns = CLAIM_TYPE_NEGATION_MAP.get(claim_type, [])
    text_lower = text.lower()
    for pat in patterns:
        if pat.lower() in text_lower:
            return True, pat
    return False, None


def detect_rebuttal(text: str) -> Tuple[bool, Optional[str]]:
    for pat in REBUTTAL_PATTERNS:
        m = pat.search(text)
        if m:
            return True, m.group(0)
    return False, None


def process_claims_for_negation(settings=None, limit: int = 2000):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT cl.id, cl.claim_text, cl.claim_type, cl.status, c.id AS content_item_id, c.body_text, c.title
        FROM claims cl
        JOIN content_items c ON c.id = cl.content_item_id
        WHERE cl.status NOT IN ('disproved', 'manipulation')
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    log.info("Processing %d claims for negation detection", len(rows))

    negated = 0
    rebutted = 0
    updated = 0

    for row in rows:
        claim_id = row["id"]
        claim_text = row["claim_text"]
        claim_type = row["claim_type"]
        status = row["status"]
        full_text = f"{row['title'] or ''}\n{row['body_text'] or ''}"

        is_negated = False
        negation_source = None

        type_neg, type_neg_word = check_claim_type_negation(full_text, claim_type)
        if type_neg:
            is_negated = True
            negation_source = f"type_negation:{type_neg_word}"

        if not is_negated:
            from verification.engine import CLAIM_PATTERNS
            for pattern, ct in CLAIM_PATTERNS:
                if ct != claim_type:
                    continue
                m = pattern.search(claim_text)
                if m:
                    has_neg, neg_word = detect_negation(claim_text, m.start(), m.end())
                    if has_neg:
                        is_negated = True
                        negation_source = f"prefix_negation:{neg_word}"
                        break

        is_rebuttal = False
        rebuttal_source = None
        if not is_negated:
            is_rebuttal, rebuttal_word = detect_rebuttal(full_text)
            if is_rebuttal:
                rebuttal_source = f"rebuttal:{rebuttal_word}"

        if is_negated:
            negated += 1
            new_status = "likely_false" if status in ("unverified", "likely_false") else status
            conn.execute(
                "UPDATE claims SET status=?, manipulation_risk=COALESCE(manipulation_risk,0)+0.3 WHERE id=?",
                (new_status, claim_id),
            )
            try:
                conn.execute(
                    "INSERT INTO verifications(claim_id, verifier_type, old_status, new_status, notes, verified_at) VALUES(?,?,?,?,?,?)",
                    (claim_id, "negation_detector", status, new_status,
                     f"Negation detected: {negation_source}",
                     datetime.now().isoformat()),
                )
            except Exception:
                from datetime import datetime
                pass

            for tag_row in conn.execute(
                "SELECT id FROM content_tags WHERE content_item_id=? AND tag_name=?",
                (row["content_item_id"], claim_type),
            ).fetchall():
                try:
                    conn.execute(
                        "INSERT INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source) VALUES(?,1,?,0.3,'negation')",
                        (row["content_item_id"], f"{claim_type}:negated"),
                    )
                except Exception:
                    pass

            updated += 1

        if is_rebuttal and not is_negated:
            rebutted += 1
            conn.execute(
                "UPDATE claims SET manipulation_risk=COALESCE(manipulation_risk,0)+0.2 WHERE id=?",
                (claim_id,),
            )

    conn.commit()
    log.info("Negation detection: %d negated, %d rebutted, %d status updates", negated, rebutted, updated)
    conn.close()
    return {"negated_claims": negated, "rebutted_claims": rebutted, "status_updates": updated}


def process_tags_for_negation(settings=None, limit: int = 5000):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    conn.row_factory = sqlite3.Row

    event_tags = conn.execute(
        """
        SELECT ct.id, ct.content_item_id, ct.tag_name, ct.confidence, c.body_text, c.title
        FROM content_tags ct
        JOIN content_items c ON c.id = ct.content_item_id
        WHERE ct.tag_level = 1
        AND ct.tag_name IN ('detention','court_decision','censorship_action','corruption_claim',
                            'mobilization_claim','vote_record','procurement_claim','ownership_claim')
        AND c.body_text IS NOT NULL
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    log.info("Checking %d event tags for negation", len(event_tags))

    adjusted = 0
    for row in event_tags:
        full_text = f"{row['title'] or ''}\n{row['body_text'] or ''}"
        tag_name = row["tag_name"]

        type_neg, neg_word = check_claim_type_negation(full_text, tag_name)
        if type_neg:
            conn.execute(
                "UPDATE content_tags SET confidence = confidence * 0.3 WHERE id=?",
                (row["id"],),
            )
            try:
                conn.execute(
                    "INSERT INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source) VALUES(?,1,?,0.3,'negation')",
                    (row["content_item_id"], f"{tag_name}:negated"),
                )
            except Exception:
                pass
            try:
                conn.execute(
                    "INSERT INTO tag_explanations(content_tag_id, trigger_text, trigger_rule, matched_pattern, confidence_raw) VALUES(?,?,?,?,?)",
                    (row["id"], neg_word[:300], "negation_detector", f"negated:{tag_name}", -0.7),
                )
            except Exception:
                pass
            adjusted += 1
            continue

        from verification.engine import CLAIM_PATTERNS
        for pattern, ct in CLAIM_PATTERNS:
            if ct != tag_name:
                continue
            m = pattern.search(full_text)
            if m:
                has_neg, neg_word = detect_negation(full_text, m.start(), m.end())
                if has_neg:
                    conn.execute(
                        "UPDATE content_tags SET confidence = confidence * 0.3 WHERE id=?",
                        (row["id"],),
                    )
                    try:
                        conn.execute(
                            "INSERT INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source) VALUES(?,1,?,0.3,'negation')",
                            (row["content_item_id"], f"{tag_name}:negated"),
                        )
                    except Exception:
                        pass
                    adjusted += 1
                    break

    conn.commit()
    log.info("Negation: adjusted %d tags (confidence reduced, :negated tags added)", adjusted)
    conn.close()
    return {"tags_adjusted": adjusted}


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Detect negation in claims and tags")
    parser.add_argument("--claims", action="store_true", help="Process claims for negation")
    parser.add_argument("--tags", action="store_true", help="Process tags for negation")
    parser.add_argument("--all", action="store_true", help="Process both")
    parser.add_argument("--limit", type=int, default=2000)
    args = parser.parse_args()

    if not args.claims and not args.tags and not args.all:
        args.all = True

    results = {}
    if args.all or args.claims:
        results["claims"] = process_claims_for_negation(limit=args.limit)
    if args.all or args.tags:
        results["tags"] = process_tags_for_negation(limit=args.limit)

    import json
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
