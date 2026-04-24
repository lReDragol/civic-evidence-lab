import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    os.sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

try:
    from collectors.site_search import targeted_search, _detect_query_type, _extract_search_queries
    SITE_SEARCH_AVAILABLE = True
except ImportError:
    SITE_SEARCH_AVAILABLE = False
    log.warning("site_search module not available — targeted site verification disabled")

CREDIBILITY_TIER_SCORES = {"A": 0.5, "B": 0.3, "C": 0.15, "D": 0.05}

CLAIM_PATTERNS = [
    (re.compile(r"(арестован|задержан|взят\s+под\s+стражу|помещен\s+в\s+сизо|обыск\s+у|допрос[аи]?)", re.I), "detention"),
    (re.compile(r"(осужден|приговорен|приговорил|осудил|лишен\s+свободы|штраф\s+в\s+рамках)", re.I), "court_decision"),
    (re.compile(r"(заявил|сказал|выступил|обратился|пригрозил|пообещал|обещал|оскорбил|шантажировал)", re.I), "public_statement"),
    (re.compile(r"(заблокирован|запрещен|ограничен|признан\s+экстремист|внесен\s+в\s+реестр)", re.I), "censorship_action"),
    (re.compile(r"(голосовал\s+за|проголосовал\s+против|поддержал\s+закон|отклонил)", re.I), "vote_record"),
    (re.compile(r"(выиграл\s+закупк|получил\s+контракт|подрядчик|заказчик|аффилирован)", re.I), "procurement_claim"),
    (re.compile(r"(владеет|собственник|принадлежит|купил|продал)", re.I), "ownership_claim"),
    (re.compile(r"(украл|похитил|растратил|освоил|распилил|откат|взятк|коррупц|мошенничеств|хищен)", re.I), "corruption_claim"),
    (re.compile(r"(мобилизован|повестк|призван|уклон[аи]ется\s+от)", re.I), "mobilization_claim"),
    (re.compile(r"(обман[ау]?|враньё|ложь|воровств|шантаж|крышеван)", re.I), "abuse_claim"),
]

CLAIM_CONTEXT_RE = re.compile(
    r"\b("
    r"госдум|совфед|суд|верховн|конституционн|к[сc]\s*рф|"
    r"росреестр|ркн|роскомнадзор|минюст|мвд|фсб|прокуратур|"
    r"правительств|белый\s+дом|сша|канада|китай|мексик|"
    r"гис\s+жкх|жкх|законопроект|закон|контракт|заказчик|поставщик|"
    r"налог|ндс|ук|навальн|путин|карлсон|воронеж|вкс"
    r")\b",
    re.I,
)
LOW_SIGNAL_CLAIM_RE = re.compile(
    r"^(ложь[,! ]|коррупция\s+\w+|собственников\.?$|но\s+я\s+уже\s+пообещал|я\s+уже\s+говорил)",
    re.I,
)


def _claim_has_context(text: str) -> bool:
    if _person_names_from_text(text):
        return True
    if _inn_from_text(text) or _case_numbers_from_text(text):
        return True
    if re.search(r"\b\d{1,2}[./]\d{1,2}[./]\d{2,4}\b", text):
        return True
    if re.search(r"\b[А-ЯЁ]{2,}\b", text):
        return True
    return bool(CLAIM_CONTEXT_RE.search(text))


def _is_informative_claim(claim_text: str, claim_type: str) -> bool:
    normalized = re.sub(r"\s+", " ", claim_text or "").strip()
    if len(normalized) < 18:
        return False

    words = re.findall(r"\b[\w\-]+\b", normalized, flags=re.UNICODE)
    if len(words) < 3:
        return False

    lowered = normalized.lower()
    if LOW_SIGNAL_CLAIM_RE.search(lowered):
        return False

    has_context = _claim_has_context(normalized)
    if claim_type in {"ownership_claim", "abuse_claim", "corruption_claim", "public_statement"} and not has_context:
        return False
    if claim_type in {"ownership_claim", "abuse_claim"} and len(words) < 6 and not re.search(r"\d", normalized):
        return False
    if claim_type == "public_statement" and re.search(r"\bя\b", lowered) and not has_context:
        return False

    return True


def extract_claims_from_text(text: str) -> List[Dict]:
    if not text:
        return []
    sentences = [
        part.strip()
        for part in re.split(r"(?<=[.!?…])\s+|\n+", text)
        if part and len(part.strip()) > 10
    ]
    claims = []
    seen = set()
    for sentence in sentences:
        for pattern, claim_type in CLAIM_PATTERNS:
            match = pattern.search(sentence)
            if not match:
                continue
            claim_text = re.sub(r"\s+", " ", sentence).strip()
            if len(claim_text) > 700:
                start = max(0, match.start() - 260)
                end = min(len(sentence), match.end() + 360)
                claim_text = sentence[start:end].strip()
            if not _is_informative_claim(claim_text, claim_type):
                continue
            key = (claim_type, claim_text.lower())
            if key in seen:
                continue
            seen.add(key)
            claims.append({
                "text": claim_text,
                "claim_type": claim_type,
                "start": match.start(),
                "end": match.end(),
            })
    return claims


def _person_names_from_text(text: str) -> List[str]:
    pattern = re.compile(r'\b([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){1,3})\b')
    names = []
    for m in pattern.finditer(text):
        name = m.group(1).strip()
        parts = name.split()
        if 2 <= len(parts) <= 3:
            names.append(name)
    return names


def _inn_from_text(text: str) -> List[str]:
    return re.findall(r'\b(\d{10}|\d{12})\b', text)


def _case_numbers_from_text(text: str) -> List[str]:
    patterns = [
        r'дел[аоу]?\s+(?:номер\s+)?(\d+[\-/]\d+[\-/]\d+)',
        r'(\d{1,2}[\-/]\d{1,2}[\-/]\d{2,8})',
        r'номер\s+дел[аоу]?\s+(\S+)',
    ]
    results = []
    for p in patterns:
        results.extend(re.findall(p, text, re.I))
    return results


def _important_search_terms(text: str) -> List[str]:
    terms = []
    terms.extend(_inn_from_text(text))
    terms.extend(_case_numbers_from_text(text))
    terms.extend(_person_names_from_text(text))

    quoted = re.findall(r"[\"«]([^\"»]{4,80})[\"»]", text)
    terms.extend(quoted[:3])

    for token in re.findall(r"\b[А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z0-9\-]{4,}\b", text):
        if token.lower() not in {"россии", "россия", "москва", "telegram", "youtube"}:
            terms.append(token)

    cleaned = []
    seen = set()
    for term in terms:
        term = re.sub(r"\s+", " ", term).strip()
        if len(term) < 4:
            continue
        key = term.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(term)
    return cleaned[:8]


def search_local_official_evidence(
    conn: sqlite3.Connection,
    claim_text: str,
    source_content_id: int,
    limit: int = 8,
) -> List[Dict]:
    terms = _important_search_terms(claim_text)
    if not terms:
        return []

    scored = {}
    for term in terms:
        like = f"%{term}%"
        rows = conn.execute(
            """
            SELECT c.id, c.content_type, c.title, c.url, s.name AS source_name,
                   s.category AS source_category, s.credibility_tier
            FROM content_items c
            JOIN sources s ON s.id = c.source_id
            WHERE c.id != ?
              AND (s.category IN ('official_registry', 'official_site')
                   OR c.content_type IN ('registry_record', 'court_record', 'enforcement', 'procurement', 'bill', 'transcript'))
              AND (c.title LIKE ? OR c.body_text LIKE ? OR c.url LIKE ?)
            LIMIT 30
            """,
            (source_content_id, like, like, like),
        ).fetchall()
        for row in rows:
            item = scored.setdefault(
                row["id"],
                {
                    "content_item_id": row["id"],
                    "content_type": row["content_type"],
                    "title": row["title"] or "",
                    "url": row["url"] or "",
                    "source_name": row["source_name"] or "",
                    "source_category": row["source_category"] or "",
                    "credibility_tier": row["credibility_tier"] or "",
                    "matched_terms": set(),
                    "score": 0.0,
                },
            )
            item["matched_terms"].add(term)
            item["score"] += 3.0 if row["source_category"] in {"official_registry", "official_site"} else 1.0
            if row["credibility_tier"] == "A":
                item["score"] += 1.0

    evidence = sorted(scored.values(), key=lambda x: (-x["score"], x["content_item_id"]))[:limit]
    for item in evidence:
        item["matched_terms"] = sorted(item["matched_terms"])
    return evidence


def link_local_evidence_for_claim(
    conn: sqlite3.Connection,
    claim_id: int,
    claim_text: str,
    source_content_id: int,
) -> int:
    evidence_items = search_local_official_evidence(conn, claim_text, source_content_id)
    links = 0
    for item in evidence_items:
        existing = conn.execute(
            "SELECT id FROM evidence_links WHERE claim_id=? AND evidence_item_id=?",
            (claim_id, item["content_item_id"]),
        ).fetchone()
        if existing:
            continue
        strength = "strong" if item["score"] >= 8 else "moderate" if item["score"] >= 4 else "weak"
        notes = json.dumps(
            {
                "strategy": "local_official_document_search",
                "matched_terms": item["matched_terms"],
                "source": item["source_name"],
                "title": item["title"],
                "score": item["score"],
            },
            ensure_ascii=False,
        )
        conn.execute(
            """
            INSERT INTO evidence_links(claim_id, evidence_item_id, evidence_type, strength, notes)
            VALUES(?,?,?,?,?)
            """,
            (claim_id, item["content_item_id"], "local_official_document", strength, notes),
        )
        links += 1
    return links


def check_egrul(inn: str, settings: dict = None) -> Optional[Dict]:
    try:
        import requests
        url = f"https://egrul.nalog.ru/api/v1/organization/{inn}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                return {
                    "source": "egrul.nalog.ru",
                    "inn": inn,
                    "name": data.get("name", ""),
                    "address": data.get("address", ""),
                    "status": data.get("status", ""),
                    "directors": data.get("directors", []),
                    "found": True,
                }
    except Exception as e:
        log.warning("EGRUL check failed for INN %s: %s", inn, e)
    return None


def check_kad_arbitr(case_number: str, settings: dict = None) -> Optional[Dict]:
    try:
        import requests
        url = "https://kad.arbitr.ru/Kad/Search"
        params = {"SimpleSearch": case_number}
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.post(url, data=params, headers=headers, timeout=15)
        if resp.status_code == 200:
            return {
                "source": "kad.arbitr.ru",
                "case_number": case_number,
                "found": resp.text and len(resp.text) > 100,
            }
    except Exception as e:
        log.warning("Kad.Arbitr check failed for %s: %s", case_number, e)
    return None


def check_fssp(person_name: str, settings: dict = None) -> Optional[Dict]:
    try:
        import requests
        url = "https://is.fssp.gov.ru/is/ajax_search"
        parts = person_name.split()
        data = {
            "is[extended]": "0",
            "is[region_id][0]": "-1",
            "is[lastname]": parts[0] if parts else "",
            "is[firstname]": parts[1] if len(parts) > 1 else "",
            "is[secondname]": parts[2] if len(parts) > 2 else "",
        }
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.post(url, data=data, headers=headers, timeout=15)
        if resp.status_code == 200:
            result = resp.json()
            items = result.get("data", {}).get("result", [])
            return {
                "source": "fssp.gov.ru",
                "person": person_name,
                "found": bool(items),
                "count": len(items) if items else 0,
            }
    except Exception as e:
        log.warning("FSSP check failed for %s: %s", person_name, e)
    return None


def compute_claim_confidence(claim_row: Dict, evidence_count: int, source_tier: str, has_primary_doc: bool) -> str:
    source_score = CREDIBILITY_TIER_SCORES.get(source_tier, 0.15)
    doc_score = 0.8 if has_primary_doc else (0.2 if evidence_count > 0 else 0)
    corroboration = min(0.3, evidence_count * 0.1) if evidence_count > 1 else 0

    total = source_score + doc_score + corroboration
    if total >= 1.0:
        return "confirmed"
    if total >= 0.6:
        return "partially_confirmed"
    if total >= 0.2:
        return "unverified"
    return "raw_signal"


def verify_claim_with_site_search(claim_id: int, claim_text: str, settings: dict = None) -> int:
    if not SITE_SEARCH_AVAILABLE:
        return 0
    try:
        queries = _extract_search_queries(claim_text)
        if not queries:
            return 0
        total_stored = 0
        for q in queries[:3]:
            results = targeted_search(q, query_type="auto", settings=settings)
            for site, count in results.items():
                if count and count > 0:
                    total_stored += count
            time.sleep(1)
        if total_stored > 0:
            conn = get_db(settings)
            try:
                conn.execute(
                    "UPDATE claims SET status='partially_confirmed', needs_review=1 WHERE id=? AND status='unverified'",
                    (claim_id,),
                )
                conn.commit()
            finally:
                conn.close()
        return total_stored
    except Exception as e:
        log.warning("Site search verification failed for claim %d: %s", claim_id, e)
        return 0


def process_claims_for_content(
    settings: dict = None,
    content_limit: int = 3000,
    verification_limit: int = 200,
    external_checks: bool = True,
) -> Dict:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT c.id, c.body_text, c.title, s.credibility_tier, s.category
        FROM content_items c
        JOIN sources s ON s.id = c.source_id
        WHERE (length(c.body_text) > 5 OR length(c.title) > 10)
        LIMIT ?
        """,
        (content_limit,),
    ).fetchall()

    if not rows:
        log.info("No content items to extract claims from")
        conn.close()
        return {"content_processed": 0, "claims_added": 0, "verified": 0, "local_evidence_links": 0}

    log.info("Processing %d content items for claims", len(rows))
    total_claims = 0
    verified = 0
    local_evidence_links = 0
    for row in rows:
        content_id = row["id"]
        text = f"{row['title'] or ''}\n{row['body_text'] or ''}"
        tier = row["credibility_tier"]

        claims = extract_claims_from_text(text)
        if not claims:
            continue

        for claim in claims:
            try:
                existing = conn.execute(
                    "SELECT id FROM claims WHERE content_item_id=? AND claim_text=? AND claim_type=?",
                    (content_id, claim["text"][:500], claim["claim_type"]),
                ).fetchone()
                if existing:
                    claim_id = existing[0]
                else:
                    cur = conn.execute(
                        """INSERT INTO claims(content_item_id, claim_text, claim_type, status, source_score, needs_review)
                           VALUES(?,?,?,?,?,1)""",
                        (content_id, claim["text"][:500], claim["claim_type"], "unverified", CREDIBILITY_TIER_SCORES.get(tier, 0.15)),
                    )
                    claim_id = cur.lastrowid
                    total_claims += 1
                local_evidence_links += link_local_evidence_for_claim(conn, claim_id, claim["text"], content_id)
            except Exception as e:
                log.warning("Failed to insert claim for item %d: %s", content_id, e)

    conn.commit()

    unverified = conn.execute(
        """
        SELECT id, content_item_id, claim_text, claim_type
        FROM claims
        WHERE status='unverified'
          AND claim_type IN ('detention','court_decision','corruption_claim','procurement_claim')
        LIMIT ?
        """
        ,
        (verification_limit,),
    ).fetchall()

    for claim_row in unverified:
        claim_id = claim_row["id"]
        content_id = claim_row["content_item_id"]
        claim_text = claim_row["claim_text"]
        claim_type = claim_row["claim_type"]

        names = _person_names_from_text(claim_text)
        inns = _inn_from_text(claim_text)
        case_nums = _case_numbers_from_text(claim_text)

        evidence_found = False
        local_links = link_local_evidence_for_claim(conn, claim_id, claim_text, content_id)
        if local_links:
            evidence_found = True

        if external_checks:
            for inn in inns[:3]:
                result = check_egrul(inn, settings)
                if result and result.get("found"):
                    evidence_found = True
                    try:
                        conn.execute(
                            """INSERT INTO evidence_links(claim_id, evidence_item_id, evidence_type, strength, notes)
                               VALUES(?,NULL,'registry_record','strong',?)""",
                            (claim_id, json.dumps(result, ensure_ascii=False)),
                        )
                    except Exception:
                        pass

            for case_num in case_nums[:3]:
                result = check_kad_arbitr(case_num, settings)
                if result and result.get("found"):
                    evidence_found = True
                    try:
                        conn.execute(
                            """INSERT INTO evidence_links(claim_id, evidence_item_id, evidence_type, strength, notes)
                               VALUES(?,NULL,'court_record','strong',?)""",
                            (claim_id, json.dumps(result, ensure_ascii=False)),
                        )
                    except Exception:
                        pass

            for name in names[:2]:
                result = check_fssp(name, settings)
                if result and result.get("found"):
                    evidence_found = True
                    try:
                        conn.execute(
                            """INSERT INTO evidence_links(claim_id, evidence_item_id, evidence_type, strength, notes)
                               VALUES(?,NULL,'fssp_record','moderate',?)""",
                            (claim_id, json.dumps(result, ensure_ascii=False)),
                        )
                    except Exception:
                        pass

        if evidence_found:
            conn.execute("UPDATE claims SET status='partially_confirmed', needs_review=1 WHERE id=?", (claim_id,))
            verified += 1
        else:
            if external_checks and SITE_SEARCH_AVAILABLE:
                site_results = verify_claim_with_site_search(claim_id, claim_text, settings)
                if site_results > 0:
                    verified += 1
                    continue
            conn.execute("UPDATE claims SET status='unverified', needs_review=1 WHERE id=?", (claim_id,))

    conn.commit()

    claim_count = conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
    confirmed = conn.execute("SELECT COUNT(*) FROM claims WHERE status='partially_confirmed'").fetchone()[0]
    unverified_count = conn.execute("SELECT COUNT(*) FROM claims WHERE status='unverified'").fetchone()[0]

    log.info("Claims: %d total, %d partially confirmed, %d unverified", claim_count, confirmed, unverified_count)
    conn.close()
    return {
        "content_processed": len(rows),
        "claims_added": total_claims,
        "verified": verified,
        "local_evidence_links": local_evidence_links,
        "claims_total": claim_count,
        "partially_confirmed": confirmed,
        "unverified": unverified_count,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--content-limit", type=int, default=3000)
    parser.add_argument("--verification-limit", type=int, default=200)
    parser.add_argument("--local-only", action="store_true", help="Skip external registry HTTP checks")
    args = parser.parse_args()
    result = process_claims_for_content(
        content_limit=args.content_limit,
        verification_limit=args.verification_limit,
        external_checks=not args.local_only,
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
