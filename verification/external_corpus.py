import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from verification.engine import _important_search_terms, _person_names_from_text

OFFICIAL_CATEGORIES = {"official_registry", "official_site"}
EVIDENCE_CATEGORIES = {"official_registry", "official_site", "media"}
OFFICIAL_TYPES = {"registry_record", "court_record", "enforcement", "procurement", "bill", "transcript", "official_page"}
GENERIC_TERMS = {
    "владимир",
    "путин",
    "россия",
    "россии",
    "российский",
    "российская",
    "российской",
    "российские",
    "российских",
    "рф",
    "государственная",
    "государственное",
    "государственной",
    "государственную",
    "государственный",
    "государственных",
    "президент",
    "президента",
    "президентом",
    "государства",
    "заявил",
    "заявила",
    "заявили",
    "сообщил",
    "сообщила",
    "сообщили",
    "рассказал",
    "рассказала",
    "сказал",
    "сказала",
}
VERIFIER_LINKED_BY = "external_corpus_verifier"
VERIFIER_EVIDENCE_TYPE = "external_db_corroboration"

MONTHS = (
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
)
DATE_RE = re.compile(
    rf"\b(?:\d{{1,2}}\.\d{{1,2}}\.\d{{2,4}}|\d{{1,2}}\s+(?:{'|'.join(MONTHS)})(?:\s+\d{{4}})?|\d{{4}})\b",
    re.I,
)
LAW_RE = re.compile(
    r"\b(?:\d{1,4}-ФЗ|ЖК\s*РФ|НК\s*РФ|КоАП\s*РФ|УК\s*РФ|ГК\s*РФ|"
    r"ст\.?\s*\d+(?:\.\d+)?|ч\.?\s*\d+(?:\.\d+)?|НДС|"
    r"законопроект\s*№?\s*[\d-]+|№\s*[\d-]{3,})\b",
    re.I,
)
ORG_PATTERNS = {
    "president": [r"\bпутин\w*\b", r"\bпесков\w*\b", r"\bкремл[ьяе]\b", r"\bпрезидент\w*\b"],
    "government": [r"\bправительств\w*\b", r"\bкабинет\w*\b", r"\bбелый\s+дом\b"],
    "duma": [r"\bгосдум\w*\b", r"\bдум[аеуы]\b", r"\bволодин\w*\b", r"\bдепутат\w*\b"],
    "council": [r"\bсовет\s+федерации\b", r"\bсенатор\w*\b"],
    "rkn": [r"\bроскомнадзор\w*\b", r"\bркн\b"],
    "rosreestr": [r"\bросреестр\w*\b", r"\bегрн\b"],
    "gis_gkh": [r"\bгис\s+жкх\b", r"\bдом\.госуслуг\w*\b"],
    "court": [r"\bконституционн\w+\s+суд\w*\b", r"\bкс\s+рф\b", r"\bсуд\w*\b"],
    "minjust": [r"\bминюст\w*\b", r"\bиноагент\w*\b"],
    "prosecutor": [r"\bгенпрокуратур\w*\b", r"\bпрокуратур\w*\b"],
    "military": [r"\bминобороны\b", r"\bвоенн\w*\b"],
}
TOPIC_KEYWORDS = {
    "housing": ["жкх", "жилищ", "дом", "квартир", "управляющ", "собственник", "помещени"],
    "property_registry": ["росреестр", "егрн", "кадастр", "недвижим", "реестр собствен"],
    "legislation": ["законопроект", "закон", "поправк", "жк рф", "ст.", "ч."],
    "taxes": ["ндс", "налог", "пошлин", "тариф"],
    "courts": ["суд", "конституционн", "приговор", "дело", "исков"],
    "procurement": ["закупк", "контракт", "тендер", "поставщик"],
    "censorship": ["ркн", "роскомнадзор", "блокиров", "огранич"],
    "foreign_agent": ["иноагент", "минюст", "реестр иностранных"],
    "government_action": ["правительств", "постановлен", "распоряж", "пакет мер"],
    "presidential_speech": ["путин", "песков", "президент", "стенограмм", "заявил"],
    "detention": ["задерж", "арест", "обыск", "уголовн"],
    "military": ["минобороны", "мобилизац", "сво", "военн"],
    "international": ["сша", "белый дом", "санкц", "иностран"],
}
PERSON_PHRASE_BLOCKLIST = (
    "государствен",
    "госдум",
    "дум",
    "правительств",
    "единой россии",
    "россии",
    "союз",
    "совет",
    "совбеза",
    "госсовет",
    "фракц",
    "парт",
)


def open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def _dedupe_terms(values: Iterable[str]) -> List[str]:
    cleaned: List[str] = []
    seen = set()
    for value in values:
        term = " ".join(str(value or "").split()).strip()
        if len(term) < 4:
            continue
        key = term.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(term)
    return cleaned


def _normalize_feature(value: str) -> str:
    return " ".join(str(value or "").lower().replace("ё", "е").split())


def _extract_dates(text: str) -> List[str]:
    return _dedupe_terms(_normalize_feature(match.group(0)) for match in DATE_RE.finditer(text or ""))


def _extract_law_refs(text: str) -> List[str]:
    return _dedupe_terms(_normalize_feature(match.group(0)) for match in LAW_RE.finditer(text or ""))


def _extract_orgs(text: str) -> List[str]:
    lowered = _normalize_feature(text)
    found = []
    for label, patterns in ORG_PATTERNS.items():
        if any(re.search(pattern, lowered, re.I) for pattern in patterns):
            found.append(label)
    return found


def _extract_topics(text: str) -> List[str]:
    lowered = _normalize_feature(text)
    found = []
    for label, keywords in TOPIC_KEYWORDS.items():
        if any(_topic_keyword_matches(lowered, keyword) for keyword in keywords):
            found.append(label)
    return found


def _is_probable_person_phrase(phrase: str) -> bool:
    lowered = _normalize_feature(phrase)
    return not any(block in lowered for block in PERSON_PHRASE_BLOCKLIST)


def _topic_keyword_matches(text: str, keyword: str) -> bool:
    keyword = _normalize_feature(keyword)
    if not keyword:
        return False
    if " " in keyword or "." in keyword:
        return keyword in text
    return bool(re.search(rf"(?<![а-яёa-z0-9]){re.escape(keyword)}[а-яёa-z0-9-]*", text, re.I))


def _feature_intersection(left: Sequence[str], right: Sequence[str]) -> List[str]:
    right_set: Set[str] = {_normalize_feature(value) for value in right}
    return [value for value in left if _normalize_feature(value) in right_set]


def _has_precise_date(values: Sequence[str]) -> bool:
    return any(not re.fullmatch(r"\d{4}", _normalize_feature(value)) for value in values)


def _has_strong_numeric(values: Sequence[str]) -> bool:
    return any(not re.fullmatch(r"\d{4}", _normalize_feature(value)) for value in values)


def _claim_terms(claim_text: str) -> Dict[str, List[str]]:
    person_phrases = _dedupe_terms(
        phrase for phrase in _person_names_from_text(claim_text) if _is_probable_person_phrase(phrase)
    )
    person_tokens = _dedupe_terms(
        part
        for phrase in person_phrases
        for part in phrase.split()
        if len(part) >= 4
    )
    quoted_phrases = _dedupe_terms(
        part
        for part in re.findall(r"[\"«]([^\"»]{4,80})[\"»]", claim_text)
    )

    person_token_keys = {x.lower() for x in person_tokens}
    person_phrase_keys = {x.lower() for x in person_phrases}
    keywords = _dedupe_terms(
        term
        for term in _important_search_terms(claim_text)
        if term.lower() not in GENERIC_TERMS
        and term.lower() not in person_token_keys
        and term.lower() not in person_phrase_keys
    )
    numeric_terms = _dedupe_terms(term for term in keywords if any(ch.isdigit() for ch in term))
    keywords = [term for term in keywords if term not in numeric_terms]
    dates = _extract_dates(claim_text)
    law_refs = _extract_law_refs(claim_text)
    orgs = _extract_orgs(claim_text)
    topics = _extract_topics(claim_text)

    search_terms = _dedupe_terms(
        person_phrases
        + person_tokens
        + quoted_phrases
        + numeric_terms
        + law_refs
        + dates
        + keywords
    )[:18]
    return {
        "person_phrases": person_phrases,
        "person_tokens": person_tokens,
        "quoted_phrases": quoted_phrases,
        "numeric_terms": numeric_terms,
        "keywords": keywords[:10],
        "dates": dates,
        "law_refs": law_refs,
        "orgs": orgs,
        "topics": topics,
        "search_terms": search_terms,
    }


def _candidate_rows(evidence_conn: sqlite3.Connection, search_terms: Sequence[str], limit: int = 60) -> List[sqlite3.Row]:
    if not search_terms:
        return []
    filters = []
    params: List[object] = []
    for term in search_terms:
        like = f"%{term}%"
        filters.append("(c.title LIKE ? OR c.body_text LIKE ? OR c.url LIKE ?)")
        params.extend([like, like, like])
    params.append(limit)
    sql = f"""
        SELECT c.id, c.content_type, c.title, c.body_text, c.url, s.name AS source_name,
               s.category AS source_category, s.credibility_tier
        FROM content_items c
        JOIN sources s ON s.id = c.source_id
        WHERE s.category IN ('official_registry', 'official_site', 'media')
          AND ({' OR '.join(filters)})
        LIMIT ?
    """
    return evidence_conn.execute(sql, params).fetchall()


def _match_terms(text: str, terms: Sequence[str]) -> List[str]:
    lowered = text.lower()
    return [term for term in terms if term.lower() in lowered]


def _candidate_features(text: str) -> Dict[str, List[str]]:
    return {
        "dates": _extract_dates(text),
        "law_refs": _extract_law_refs(text),
        "orgs": _extract_orgs(text),
        "topics": _extract_topics(text),
    }


def _is_reliable_match(
    *,
    is_official: bool,
    matched_keywords: Sequence[str],
    matched_numeric: Sequence[str],
    matched_quotes: Sequence[str],
    matched_dates: Sequence[str],
    matched_law_refs: Sequence[str],
    matched_orgs: Sequence[str],
    matched_topics: Sequence[str],
) -> bool:
    has_precise_date = _has_precise_date(matched_dates)
    has_strong_numeric = _has_strong_numeric(matched_numeric)
    has_specific = bool(matched_quotes or matched_law_refs or has_precise_date or has_strong_numeric)
    if matched_law_refs and (matched_orgs or matched_topics or matched_keywords or is_official):
        return True
    if matched_quotes and matched_topics and (matched_orgs or is_official):
        return True
    if not is_official:
        return False
    if matched_orgs and matched_topics and (has_specific or len(matched_keywords) >= 2):
        return True
    if has_precise_date and matched_topics and len(matched_keywords) >= 2:
        return True
    if has_strong_numeric and matched_topics and len(matched_keywords) >= 2:
        return True
    if len(matched_keywords) >= 4 and matched_topics and (matched_orgs or has_specific):
        return True
    return False


def _score_candidate(row: sqlite3.Row, claim_terms: Dict[str, List[str]]) -> Dict | None:
    text = " ".join(
        part
        for part in [row["title"] or "", row["body_text"] or "", row["url"] or "", row["source_name"] or ""]
        if part
    )
    candidate_features = _candidate_features(text)
    matched_numeric = _match_terms(text, claim_terms["numeric_terms"])
    matched_quotes = _match_terms(text, claim_terms["quoted_phrases"])
    matched_keywords = _match_terms(text, claim_terms["keywords"])
    matched_person_phrases = _match_terms(text, claim_terms["person_phrases"])
    matched_person_tokens = _match_terms(text, claim_terms["person_tokens"])
    matched_dates = _feature_intersection(claim_terms["dates"], candidate_features["dates"])
    matched_law_refs = _feature_intersection(claim_terms["law_refs"], candidate_features["law_refs"])
    matched_orgs = _feature_intersection(claim_terms["orgs"], candidate_features["orgs"])
    matched_topics = _feature_intersection(claim_terms["topics"], candidate_features["topics"])

    special_matches = _dedupe_terms(matched_numeric + matched_quotes + matched_dates + matched_law_refs)
    if not matched_keywords and not special_matches:
        return None
    if len(matched_keywords) < 2 and not special_matches:
        return None

    is_official = row["source_category"] in OFFICIAL_CATEGORIES or row["content_type"] in OFFICIAL_TYPES
    is_reliable = _is_reliable_match(
        is_official=is_official,
        matched_keywords=matched_keywords,
        matched_numeric=matched_numeric,
        matched_quotes=matched_quotes,
        matched_dates=matched_dates,
        matched_law_refs=matched_law_refs,
        matched_orgs=matched_orgs,
        matched_topics=matched_topics,
    )
    is_media_reviewable = (
        row["source_category"] == "media"
        and len(matched_keywords) >= 3
        and bool(matched_orgs or matched_topics or special_matches)
    )
    if not is_reliable and not is_media_reviewable:
        return None

    score = 0.0
    score += len(matched_keywords) * 2.5
    score += len(matched_numeric) * 4.0
    score += len(matched_quotes) * 4.0
    score += len(matched_dates) * 3.0
    score += len(matched_law_refs) * 5.0
    score += len(matched_orgs) * 2.0
    score += len(matched_topics) * 1.5
    score += min(len(matched_person_phrases), 1) * 1.0
    score += min(len(matched_person_tokens), 2) * 0.25

    if row["source_category"] in OFFICIAL_CATEGORIES:
        score += 4.0
    elif row["source_category"] == "media":
        score += 1.5

    if row["content_type"] in OFFICIAL_TYPES:
        score += 2.0
    if row["credibility_tier"] == "A":
        score += 1.0
    elif row["credibility_tier"] == "B":
        score += 0.5

    if is_reliable:
        score += 3.0

    matched_terms = _dedupe_terms(
        matched_numeric
        + matched_quotes
        + matched_dates
        + matched_law_refs
        + matched_keywords
        + matched_person_phrases
        + matched_person_tokens
    )
    match_reason = []
    if matched_keywords:
        match_reason.append(f"keyword_overlap:{', '.join(matched_keywords[:6])}")
    if matched_dates:
        match_reason.append(f"date_overlap:{', '.join(matched_dates[:4])}")
    if matched_law_refs:
        match_reason.append(f"law_ref_overlap:{', '.join(matched_law_refs[:4])}")
    if matched_orgs:
        match_reason.append(f"org_overlap:{', '.join(matched_orgs[:4])}")
    if matched_topics:
        match_reason.append(f"topic_overlap:{', '.join(matched_topics[:4])}")
    if row["source_category"] in OFFICIAL_CATEGORIES:
        match_reason.append("official_source")
    if row["content_type"] in OFFICIAL_TYPES:
        match_reason.append(f"official_content_type:{row['content_type']}")
    if is_reliable:
        match_reason.append("reliable_threshold_passed")

    return {
        "content_id": row["id"],
        "content_type": row["content_type"] or "",
        "title": row["title"] or "",
        "url": row["url"] or "",
        "source_name": row["source_name"] or "",
        "source_category": row["source_category"] or "",
        "credibility_tier": row["credibility_tier"] or "",
        "matched_terms": matched_terms,
        "keyword_matches": matched_keywords,
        "special_matches": special_matches,
        "person_matches": _dedupe_terms(matched_person_phrases + matched_person_tokens),
        "date_matches": matched_dates,
        "law_ref_matches": matched_law_refs,
        "org_matches": matched_orgs,
        "topic_matches": matched_topics,
        "match_reason": match_reason,
        "is_reliable": is_reliable,
        "score": round(score, 2),
    }


def search_external_corpus(evidence_conn: sqlite3.Connection, claim_text: str, limit: int = 8) -> List[Dict]:
    claim_terms = _claim_terms(claim_text)
    rows = _candidate_rows(evidence_conn, claim_terms["search_terms"])
    scored: List[Dict] = []
    for row in rows:
        item = _score_candidate(row, claim_terms)
        if not item:
            continue
        scored.append(item)
    scored.sort(key=lambda x: (not x["is_reliable"], -x["score"], -len(x["keyword_matches"]), x["content_id"]))
    return scored[:limit]


def _clear_previous_external_links(target_conn: sqlite3.Connection) -> Dict[str, int]:
    claim_ids = [
        row[0]
        for row in target_conn.execute(
            """
            SELECT DISTINCT claim_id
            FROM evidence_links
            WHERE evidence_type=? OR linked_by=?
            """,
            (VERIFIER_EVIDENCE_TYPE, VERIFIER_LINKED_BY),
        ).fetchall()
    ]
    removed = target_conn.execute(
        "DELETE FROM evidence_links WHERE evidence_type=? OR linked_by=?",
        (VERIFIER_EVIDENCE_TYPE, VERIFIER_LINKED_BY),
    ).rowcount
    status_reset = 0
    for claim_id in claim_ids:
        remaining = target_conn.execute(
            "SELECT COUNT(*) FROM evidence_links WHERE claim_id=?",
            (claim_id,),
        ).fetchone()[0]
        if remaining == 0:
            target_conn.execute(
                """
                UPDATE claims
                SET status='unverified',
                    corroboration_score=0,
                    needs_review=1
                WHERE id=?
                """,
                (claim_id,),
            )
            status_reset += 1
    return {"links_removed": removed, "claims_reset": status_reset}


def verify_claims_against_external_corpus(
    target_db: Path,
    evidence_db: Path,
    claim_limit: int = 200,
) -> Dict[str, int]:
    if not target_db.exists():
        raise FileNotFoundError(f"Target DB not found: {target_db}")
    if not evidence_db.exists():
        raise FileNotFoundError(f"Evidence DB not found: {evidence_db}")
    if target_db.resolve() == evidence_db.resolve():
        raise RuntimeError("Target DB and evidence DB must be different for external corpus verification")

    target_conn = open_db(target_db)
    evidence_conn = open_db(evidence_db)
    try:
        reset_info = _clear_previous_external_links(target_conn)
        claims = target_conn.execute(
            """
            SELECT id, claim_text, status
            FROM claims
            WHERE status IN ('unverified', 'raw_signal')
            ORDER BY id DESC
            LIMIT ?
            """,
            (claim_limit,),
        ).fetchall()

        linked = 0
        claims_with_hits = 0
        partially_confirmed = 0
        claims_with_official_hits = 0
        claims_with_media_hits = 0

        for claim in claims:
            evidence_items = search_external_corpus(evidence_conn, claim["claim_text"])
            if not evidence_items:
                continue

            claims_with_hits += 1
            official_hits = 0
            official_reliable_hits = 0
            media_hits = 0

            for item in evidence_items:
                if item["source_category"] in OFFICIAL_CATEGORIES or item["content_type"] in OFFICIAL_TYPES:
                    if item["is_reliable"] and item["score"] >= 14:
                        strength = "strong"
                    elif item["is_reliable"]:
                        strength = "moderate"
                    else:
                        strength = "weak"
                    official_hits += 1
                    if item["is_reliable"]:
                        official_reliable_hits += 1
                else:
                    strength = "moderate" if item["is_reliable"] and len(item["keyword_matches"]) >= 3 and item["score"] >= 8 else "weak"
                    media_hits += 1

                notes = json.dumps(
                    {
                        "db": str(evidence_db),
                        "content_id": item["content_id"],
                        "source_category": item["source_category"],
                        "source_name": item["source_name"],
                        "content_type": item["content_type"],
                        "title": item["title"],
                        "url": item["url"],
                        "matched_terms": item["matched_terms"],
                        "keyword_matches": item["keyword_matches"],
                        "special_matches": item["special_matches"],
                        "date_matches": item["date_matches"],
                        "law_ref_matches": item["law_ref_matches"],
                        "org_matches": item["org_matches"],
                        "topic_matches": item["topic_matches"],
                        "match_reason": item["match_reason"],
                        "is_reliable": item["is_reliable"],
                        "score": item["score"],
                    },
                    ensure_ascii=False,
                )
                target_conn.execute(
                    """
                    INSERT INTO evidence_links(claim_id, evidence_item_id, evidence_type, strength, notes, linked_by)
                    VALUES(?,NULL,?,?,?,?)
                    """,
                    (claim["id"], VERIFIER_EVIDENCE_TYPE, strength, notes, VERIFIER_LINKED_BY),
                )
                linked += 1

            if official_reliable_hits > 0:
                target_conn.execute(
                    """
                    UPDATE claims
                    SET status='partially_confirmed',
                        needs_review=1,
                        corroboration_score=COALESCE(corroboration_score, 0) + 0.4
                    WHERE id=?
                    """,
                    (claim["id"],),
                )
                partially_confirmed += 1
                claims_with_official_hits += 1
            elif media_hits > 0:
                target_conn.execute(
                    """
                    UPDATE claims
                    SET corroboration_score=COALESCE(corroboration_score, 0) + ?
                    WHERE id=?
                    """,
                    (min(0.3, media_hits * 0.1), claim["id"]),
                )
                claims_with_media_hits += 1

        target_conn.commit()
        return {
            "claims_checked": len(claims),
            "claims_with_hits": claims_with_hits,
            "claims_with_official_hits": claims_with_official_hits,
            "claims_with_media_hits": claims_with_media_hits,
            "evidence_links_added": linked,
            "claims_partially_confirmed": partially_confirmed,
            "links_removed_before_run": reset_info["links_removed"],
            "claims_reset_before_run": reset_info["claims_reset"],
        }
    finally:
        evidence_conn.close()
        target_conn.close()
