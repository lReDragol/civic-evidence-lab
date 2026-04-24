import json
import logging
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

CORRUPTION_KEYWORDS = [
    r"\bкоррупц",
    r"\bворовств",
    r"\bвзятк",
    r"\bоткат",
    r"\bраспил",
    r"\bхищен",
    r"\bмошенничеств",
    r"\bпревышен.*полномоч",
    r"\bнезаконн.*вознагражд",
    r"\bкоммерческ.*подкуп",
    r"\bприсвоен",
    r"\bрастрат",
    r"\bнецелев.*использован",
    r"\bаффилирован",
    r"\bконфликт.*интерес",
    r"\bдекларац.*недостов",
    r"\bсокрыт.*доход",
    r"\bнедвижимост.*неуказан",
    r"\bсчет.*заграниц",
    r"\bиностр.*счет",
    r"\bсанкцион.*наруш",
    r"\bблокиров.*счет",
]

COERCION_KEYWORDS = [
    r"\bдавлен",
    r"\bпринужден",
    r"\bшантаж",
    r"\bзапугива",
    r"\bугроз",
    r"\bвынужден",
    r"\bподавлен",
    r"\bзапрет.*говорить",
    r"\bцензур",
    r"\bблокиров.*доступ",
    r"\bприостанов.*деятельност",
    r"\bликвидац.*организац",
    r"\bпризнан.*иноагент",
    r"\bмаркировк",
    r"\bнежелательн.*организац",
    r"\bэкстремистск",
]

CONTRADICTION_KEYWORDS = [
    r"\bопроверг",
    r"\bдезинформац",
    r"\bфэйк",
    r"\bфейк",
    r"\bложн.*утвержд",
    r"\bнесоответств.*факт",
    r"\bпротиворечи",
    r"\bне.*совпадает.*с.*данн",
    r"\bискажён",
    r"\bискажен",
    r"\bвведён.*в.*заблужден",
    r"\bманипуляц",
    r"\bпропаганд",
]

LAW_SUPPRESSION_KEYWORDS = [
    r"\bзаблокирован.*сайт",
    r"\bРКН.*заблокир",
    r"\bРоскомнадзор.*заблокир",
    r"\bблокиров.*без.*решен",
    r"\bограничен.*доступ.*информац",
    r"\bфильтрац.*контент",
    r"\bТСПУ",
    r"\bСЗВ",
    r"\bсистем.*оперативн.*розыскн",
    r"\bСОРМ",
    r"\bперехват.*коммуникац",
    r"\bзакон.*Яровай",
    r"\bпакет.*Яровай",
    r"\bобязательн.*хранен.*трафик",
    r"\bдеперсонализац",
    r"\bVPN.*запрещ",
    r"\bанонимайзер.*запрещ",
]


def _match_keywords(text: str, patterns: List[str]) -> List[str]:
    matched = []
    for pat in patterns:
        if re.search(pat, text, re.I):
            matched.append(pat.replace(r"\b", "").split("*")[0].split("[")[0][:20])
    return matched


def _compute_risk_level(score: int) -> str:
    if score >= 5:
        return "critical"
    elif score >= 3:
        return "high"
    elif score >= 2:
        return "medium"
    return "low"


def detect_corruption_patterns(settings: dict = None) -> Dict:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    log.info("Detecting corruption risk patterns...")

    tagged_items = conn.execute(
        """
        SELECT ct.content_item_id, ct.tag_name
        FROM content_tags ct
        WHERE ct.tag_name IN (
            'коррупция', 'воровство', 'взятка', 'откат', 'распил',
            'хищение', 'мошенничество', 'аффилированность', 'конфликт интересов',
            'недостоверная декларация', 'нецелевое использование', 'иностранные счета'
        )
        """
    ).fetchall()

    item_keyword_hits = defaultdict(set)
    for item_id, tag in tagged_items:
        item_keyword_hits[item_id].add(tag)

    rows = conn.execute(
        """
        SELECT c.id, c.body_text, c.title
        FROM content_items c
        WHERE length(c.body_text) > 100
        LIMIT 30000
        """
    ).fetchall()

    for row in rows:
        text = f"{row[1] or ''} {row[2] or ''}"
        hits = _match_keywords(text, CORRUPTION_KEYWORDS)
        if hits:
            item_keyword_hits[row[0]].update(hits)

    person_items = defaultdict(set)
    for item_id in item_keyword_hits:
        entities = conn.execute(
            "SELECT entity_id, e.entity_type FROM entity_mentions em JOIN entities e ON e.id = em.entity_id WHERE em.content_item_id = ? AND e.entity_type = 'person'",
            (item_id,),
        ).fetchall()
        for eid, etype in entities:
            person_items[eid].add(item_id)

    patterns_found = 0
    for person_id, item_ids in person_items.items():
        if len(item_ids) < 2:
            continue

        person_name = conn.execute("SELECT canonical_name FROM entities WHERE id = ?", (person_id,)).fetchone()
        if not person_name:
            continue

        all_keywords = set()
        for iid in item_ids:
            all_keywords.update(item_keyword_hits.get(iid, set()))

        has_relations = conn.execute(
            "SELECT COUNT(*) FROM entity_relations WHERE (from_entity_id = ? OR to_entity_id = ?) AND relation_type IN ('head_of', 'founder_of', 'member_of')",
            (person_id, person_id),
        ).fetchone()[0]

        has_flagged = conn.execute(
            """SELECT COUNT(*) FROM quotes q
               JOIN entity_mentions em ON em.content_item_id = q.content_item_id
               WHERE em.entity_id = ? AND q.is_flagged = 1""",
            (person_id,),
        ).fetchone()[0]

        risk_score = 0
        if len(item_ids) >= 5:
            risk_score += 2
        elif len(item_ids) >= 3:
            risk_score += 1
        if len(all_keywords) >= 3:
            risk_score += 2
        elif len(all_keywords) >= 2:
            risk_score += 1
        if has_relations > 0:
            risk_score += 1
        if has_flagged > 0:
            risk_score += 1

        if risk_score < 2:
            continue

        risk_level = _compute_risk_level(risk_score)
        entity_ids_str = json.dumps([person_id], ensure_ascii=False)
        evidence_ids_str = json.dumps(list(item_ids)[:50], ensure_ascii=False)

        case_id = None
        existing_cases = conn.execute(
            """
            SELECT cs.id FROM cases cs
            JOIN case_claims ccl ON ccl.case_id = cs.id
            JOIN claims cl ON cl.id = ccl.claim_id
            WHERE cl.content_item_id IN ({}) AND cs.case_type = 'corruption_risk'
            LIMIT 1
            """.format(",".join("?" * min(len(item_ids), 20))),
            list(item_ids)[:20],
        ).fetchall()
        if existing_cases:
            case_id = existing_cases[0][0]

        existing_pattern = conn.execute(
            "SELECT id FROM risk_patterns WHERE pattern_type = 'corruption_risk' AND entity_ids = ?",
            (entity_ids_str,),
        ).fetchone()

        if existing_pattern:
            conn.execute(
                "UPDATE risk_patterns SET risk_level = ?, evidence_ids = ?, case_id = ? WHERE id = ?",
                (risk_level, evidence_ids_str, case_id, existing_pattern[0]),
            )
        else:
            conn.execute(
                """INSERT INTO risk_patterns(pattern_type, description, entity_ids, evidence_ids, risk_level, case_id, needs_review)
                   VALUES(?,?,?,?,?,?,1)""",
                (
                    "corruption_risk",
                    f"{person_name[0]}: коррупционные риски ({len(item_ids)} упоминаний, {len(all_keywords)} маркеров: {', '.join(list(all_keywords)[:5])})",
                    entity_ids_str,
                    evidence_ids_str,
                    risk_level,
                    case_id,
                ),
            )
        patterns_found += 1

        if patterns_found % 100 == 0:
            conn.commit()

    conn.commit()
    log.info("Corruption risk patterns: %d detected", patterns_found)

    conn.close()
    return {"corruption_patterns": patterns_found}


def detect_rhetoric_patterns(settings: dict = None) -> Dict:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    log.info("Detecting rhetoric/coercion risk patterns...")

    flagged_quotes = conn.execute(
        """
        SELECT q.id, q.content_item_id, q.entity_id, q.rhetoric_class, q.quote_text
        FROM quotes q
        WHERE q.is_flagged = 1
        """
    ).fetchall()

    person_rhetoric = defaultdict(lambda: defaultdict(list))
    for q in flagged_quotes:
        entity_id = q[2]
        if entity_id is None:
            em_rows = conn.execute(
                "SELECT entity_id FROM entity_mentions em JOIN entities e ON e.id = em.entity_id WHERE em.content_item_id = ? AND e.entity_type = 'person' LIMIT 1",
                (q[1],),
            ).fetchall()
            if em_rows:
                entity_id = em_rows[0][0]
                conn.execute("UPDATE quotes SET entity_id = ? WHERE id = ?", (entity_id, q[0]))
            else:
                continue

        rhetoric = q[3] or "unknown"
        person_rhetoric[entity_id][rhetoric].append({
            "quote_id": q[0], "content_item_id": q[1], "text": q[4][:100] if q[4] else ""
        })

    patterns_found = 0
    for entity_id, rhetoric_map in person_rhetoric.items():
        total_flagged = sum(len(v) for v in rhetoric_map.values())
        unique_rhetoric_types = len(rhetoric_map)

        if total_flagged < 2 and unique_rhetoric_types < 2:
            continue

        person_name = conn.execute("SELECT canonical_name FROM entities WHERE id = ?", (entity_id,)).fetchone()
        if not person_name:
            continue

        is_deputy = conn.execute(
            "SELECT id FROM deputy_profiles WHERE entity_id = ?", (entity_id,)
        ).fetchone()

        risk_score = 0
        if total_flagged >= 5:
            risk_score += 2
        elif total_flagged >= 3:
            risk_score += 1
        if unique_rhetoric_types >= 3:
            risk_score += 2
        elif unique_rhetoric_types >= 2:
            risk_score += 1
        if is_deputy:
            risk_score += 1

        coercion_hits = set()
        for q_data in rhetoric_map.get("hostile_labeling", []) + rhetoric_map.get("pressure", []):
            text = q_data.get("text", "")
            hits = _match_keywords(text, COERCION_KEYWORDS)
            coercion_hits.update(hits)
        if coercion_hits:
            risk_score += 1

        if risk_score < 2:
            continue

        risk_level = _compute_risk_level(risk_score)
        entity_ids_str = json.dumps([entity_id], ensure_ascii=False)
        evidence_ids_str = json.dumps([q_data["content_item_id"] for q_data_list in rhetoric_map.values() for q_data in q_data_list][:50], ensure_ascii=False)

        rhetoric_summary = ", ".join(f"{k}({len(v)})" for k, v in rhetoric_map.items())

        existing = conn.execute(
            "SELECT id FROM risk_patterns WHERE pattern_type = 'rhetoric_risk' AND entity_ids = ?",
            (entity_ids_str,),
        ).fetchone()

        description = f"{person_name[0]}: риторика давления ({total_flagged} флагов, {unique_rhetoric_types} типов: {rhetoric_summary})"

        if existing:
            conn.execute(
                "UPDATE risk_patterns SET risk_level = ?, description = ?, evidence_ids = ? WHERE id = ?",
                (risk_level, description, evidence_ids_str, existing[0]),
            )
        else:
            conn.execute(
                """INSERT INTO risk_patterns(pattern_type, description, entity_ids, evidence_ids, risk_level, needs_review)
                   VALUES(?,?,?,?,?,1)""",
                ("rhetoric_risk", description, entity_ids_str, evidence_ids_str, risk_level),
            )
        patterns_found += 1

    conn.commit()
    log.info("Rhetoric risk patterns: %d detected", patterns_found)

    conn.close()
    return {"rhetoric_patterns": patterns_found}


def detect_contradiction_patterns(settings: dict = None) -> Dict:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    log.info("Detecting contradiction patterns...")

    person_quotes = defaultdict(list)
    quotes = conn.execute(
        """
        SELECT q.id, q.content_item_id, q.entity_id, q.quote_text, q.rhetoric_class, c.published_at
        FROM quotes q
        JOIN content_items c ON c.id = q.content_item_id
        WHERE q.entity_id IS NOT NULL AND q.quote_text IS NOT NULL AND length(q.quote_text) > 20
        ORDER BY q.entity_id, c.published_at
        """
    ).fetchall()

    for q in quotes:
        person_quotes[q[2]].append({
            "quote_id": q[0], "content_item_id": q[1], "text": q[3],
            "rhetoric": q[4], "published": q[5],
        })

    patterns_found = 0
    for entity_id, quote_list in person_quotes.items():
        if len(quote_list) < 3:
            continue

        person_name = conn.execute("SELECT canonical_name FROM entities WHERE id = ?", (entity_id,)).fetchone()
        if not person_name:
            continue

        promises = [q for q in quote_list if q["rhetoric"] and "promise" in (q["rhetoric"] or "")]
        dismissals = [q for q in quote_list if q["rhetoric"] and "dismissal" in (q["rhetoric"] or "")]
        hostile = [q for q in quote_list if q["rhetoric"] and "hostile" in (q["rhetoric"] or "")]
        external_blame = [q for q in quote_list if q["rhetoric"] and "external_blame" in (q["rhetoric"] or "")]

        contradiction_indicators = 0
        if promises and dismissals:
            contradiction_indicators += 2
        if promises and hostile:
            contradiction_indicators += 1
        if external_blame and promises:
            contradiction_indicators += 1

        body_keyword_hits = defaultdict(set)
        for q in quote_list:
            text = q["text"]
            hits = _match_keywords(text, CONTRADICTION_KEYWORDS)
            for h in hits:
                body_keyword_hits[h].add(q["content_item_id"])

        if body_keyword_hits:
            contradiction_indicators += min(3, len(body_keyword_hits))

        if contradiction_indicators < 2:
            continue

        risk_level = _compute_risk_level(contradiction_indicators)
        entity_ids_str = json.dumps([entity_id], ensure_ascii=False)
        all_content_ids = list(set(q["content_item_id"] for q in quote_list))[:50]
        evidence_ids_str = json.dumps(all_content_ids, ensure_ascii=False)

        description = f"{person_name[0]}: противоречия ({len(promises)} обещаний, {len(dismissals)} обесцениваний, {len(hostile)} агрессии, {len(external_blame)} внешних обвинений)"

        existing = conn.execute(
            "SELECT id FROM risk_patterns WHERE pattern_type = 'contradiction_risk' AND entity_ids = ?",
            (entity_ids_str,),
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE risk_patterns SET risk_level = ?, description = ?, evidence_ids = ? WHERE id = ?",
                (risk_level, description, evidence_ids_str, existing[0]),
            )
        else:
            conn.execute(
                """INSERT INTO risk_patterns(pattern_type, description, entity_ids, evidence_ids, risk_level, needs_review)
                   VALUES(?,?,?,?,?,1)""",
                ("contradiction_risk", description, entity_ids_str, evidence_ids_str, risk_level),
            )
        patterns_found += 1

    conn.commit()
    log.info("Contradiction patterns: %d detected", patterns_found)
    conn.close()
    return {"contradiction_patterns": patterns_found}


def detect_suppression_patterns(settings: dict = None) -> Dict:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    log.info("Detecting law/suppression risk patterns...")

    tagged_items = conn.execute(
        """
        SELECT ct.content_item_id, ct.tag_name
        FROM content_tags ct
        WHERE ct.tag_name IN (
            'блокировка', 'ркн', 'фсб', 'сорм', 'яровая', 'цензура',
            'иноагент', 'нежелательная организация', 'экстремизм',
            'блокировка сайтов', 'тспу', 'деперсонализация', 'закон о цензуре'
        )
        """
    ).fetchall()

    item_keyword_hits = defaultdict(set)
    for item_id, tag in tagged_items:
        item_keyword_hits[item_id].add(tag)

    rows = conn.execute(
        """
        SELECT c.id, c.body_text
        FROM content_items c
        WHERE length(c.body_text) > 100
        LIMIT 30000
        """
    ).fetchall()

    for row in rows:
        hits = _match_keywords(row[1] or "", LAW_SUPPRESSION_KEYWORDS)
        if hits:
            item_keyword_hits[row[0]].update(hits)

    entity_hits = defaultdict(lambda: defaultdict(set))
    for item_id, keywords in item_keyword_hits.items():
        if not keywords:
            continue
        entities = conn.execute(
            "SELECT entity_id FROM entity_mentions WHERE content_item_id = ? AND entity_id IN (SELECT id FROM entities WHERE entity_type = 'person')",
            (item_id,),
        ).fetchall()
        for eid in entities:
            for kw in keywords:
                entity_hits[eid[0]][kw].add(item_id)

    patterns_found = 0
    for entity_id, keyword_map in entity_hits.items():
        total_items = set()
        for kw_items in keyword_map.values():
            total_items.update(kw_items)

        if len(total_items) < 2 or len(keyword_map) < 2:
            continue

        person_name = conn.execute("SELECT canonical_name FROM entities WHERE id = ?", (entity_id,)).fetchone()
        if not person_name:
            continue

        is_deputy = conn.execute(
            "SELECT id FROM deputy_profiles WHERE entity_id = ?", (entity_id,)
        ).fetchone()

        risk_score = 0
        if len(total_items) >= 5:
            risk_score += 2
        elif len(total_items) >= 3:
            risk_score += 1
        if len(keyword_map) >= 4:
            risk_score += 2
        elif len(keyword_map) >= 2:
            risk_score += 1
        if is_deputy:
            risk_score += 1

        if risk_score < 2:
            continue

        risk_level = _compute_risk_level(risk_score)
        entity_ids_str = json.dumps([entity_id], ensure_ascii=False)
        evidence_ids_str = json.dumps(list(total_items)[:50], ensure_ascii=False)

        kw_summary = ", ".join(f"{kw}({len(items)})" for kw, items in keyword_map.items())

        description = f"{person_name[0]}: подавление/цензура ({len(total_items)} упоминаний, маркеры: {kw_summary})"

        existing = conn.execute(
            "SELECT id FROM risk_patterns WHERE pattern_type = 'suppression_risk' AND entity_ids = ?",
            (entity_ids_str,),
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE risk_patterns SET risk_level = ?, description = ?, evidence_ids = ? WHERE id = ?",
                (risk_level, description, evidence_ids_str, existing[0]),
            )
        else:
            conn.execute(
                """INSERT INTO risk_patterns(pattern_type, description, entity_ids, evidence_ids, risk_level, needs_review)
                   VALUES(?,?,?,?,?,1)""",
                ("suppression_risk", description, entity_ids_str, evidence_ids_str, risk_level),
            )
        patterns_found += 1

    conn.commit()
    log.info("Suppression patterns: %d detected", patterns_found)
    conn.close()
    return {"suppression_patterns": patterns_found}


def detect_all_patterns(settings: dict = None) -> Dict:
    if settings is None:
        settings = load_settings()

    results = {}
    for name, func in [
        ("corruption", detect_corruption_patterns),
        ("rhetoric", detect_rhetoric_patterns),
        ("contradiction", detect_contradiction_patterns),
        ("suppression", detect_suppression_patterns),
    ]:
        try:
            r = func(settings)
            results[name] = r
        except Exception as e:
            log.error("Pattern detection %s failed: %s", name, e)
            results[name] = {"error": str(e)}

    conn = get_db(settings)
    total = conn.execute("SELECT COUNT(*) FROM risk_patterns").fetchone()[0]
    by_type = conn.execute("SELECT pattern_type, COUNT(*), risk_level FROM risk_patterns GROUP BY pattern_type, risk_level ORDER BY pattern_type, risk_level").fetchall()
    conn.close()

    summary = {"total_patterns": total, "by_type": [dict(zip(["type", "count", "level"], r)) for r in by_type]}
    results["summary"] = summary
    log.info("Pattern detection summary: %s", summary)
    return results


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--corruption", action="store_true")
    parser.add_argument("--rhetoric", action="store_true")
    parser.add_argument("--contradiction", action="store_true")
    parser.add_argument("--suppression", action="store_true")
    parser.add_argument("--all", action="store_true", default=True)
    args = parser.parse_args()

    result = detect_all_patterns()
    print(json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
