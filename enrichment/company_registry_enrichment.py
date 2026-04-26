from __future__ import annotations

import re
from typing import Any

from enrichment.common import (
    clean_text,
    ensure_review_task,
    find_person_entity,
    json_dumps,
    normalize_text,
    open_db,
    parse_json,
)


COMPANY_ALIAS_MAP: dict[str, tuple[str, ...]] = {
    "Сбербанк": ("сбербанк", "пао сбербанк", "оао сбербанк"),
    "ВТБ": ("банк втб", "пао втб"),
    "ВТБ Капитал": ("втб капитал",),
    "Ренессанс Капитал": ("ренессанс капитал", "ренессанс капитал финансовый консультант"),
    "Росбанк": ("росбанк", "оао акб росбанк"),
    "БНП Париба": ("бнп париба", "банк париба"),
    "ЮБиЭс Банк": ("юбиэс банк", "юби эс банк", "ubs bank"),
    "Универсальная электронная карта": ("универсальная электронная карта",),
    "Ростелеком": ("ростелеком",),
    "Росатом": ("росатом", "госкорпорация росатом"),
    "Газпром": ("газпром",),
    "Роснефть": ("роснефть",),
    "Ростех": ("ростех",),
    "РЖД": ("ржд", "российские железные дороги"),
    "АвтоВАЗ": ("автоваз",),
}

COMPANY_PATTERN = re.compile(
    r"(?:\b(?:ПАО|ОАО|АО|ООО|ЗАО|НАО|Банк|Госкорпорация|Инвестиционный\s+банк)\b\s*[«\"]?[^,.;:\n]{2,90}[»\"]?)",
    flags=re.IGNORECASE | re.UNICODE,
)
PERIOD_PATTERN = re.compile(
    r"(?P<start>(?:19|20)\d{2})\s*[–-]\s*(?P<end>(?:19|20)\d{2}|по\s+н\.\s*в\.|н\.\s*в\.)",
    flags=re.IGNORECASE | re.UNICODE,
)
ROLE_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"председател[ья]\s+совета\s+директоров", "board_chair"),
    (r"член\s+совета\s+директоров", "board_member"),
    (r"член\s+правления", "board_member"),
    (r"генеральн(?:ый|ого)\s+директор", "director"),
    (r"исполнительн(?:ый|ого)\s+директор", "director"),
    (r"перв(?:ый|ого)\s+вице-президент", "vice_president"),
    (r"вице-президент", "vice_president"),
    (r"советник\s+президента", "advisor"),
    (r"советник", "advisor"),
    (r"со-директор", "director"),
    (r"директор(?:\s+по\s+[^,.;:\n]{2,80})?", "director"),
    (r"начальник\s+управления", "manager"),
    (r"начальник\s+отдела", "manager"),
    (r"аналитик", "employee"),
    (r"экономист", "employee"),
    (r"менеджер(?:\s+по\s+[^,.;:\n]{2,80})?", "employee"),
)
COMPANY_STOPWORDS = (
    "министерств",
    "федеральн",
    "академ",
    "университет",
    "институт",
    "департамент",
    "служб",
    "правительств",
    "казначей",
    "налогов",
    "государственн",
    "комитет",
    "администрац",
)
SEGMENT_SPLIT_RE = re.compile(r"[;\n]+|(?<=\.)\s+")
COMPANY_TAIL_SPLIT_RE = re.compile(
    r"\s+(?:(?:19|20)\d{2}\s*[–-]\s*(?:19|20)\d{2}|по\s+н\.\s*в\.|н\.\s*в\.|"
    r"заместител|директор|начальник|советник|экономист|аналитик|менеджер)\b.*$",
    flags=re.IGNORECASE | re.UNICODE,
)


def _extract_people(payload: dict[str, Any]) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    director = payload.get("director")
    if isinstance(director, dict):
        fio = clean_text(director.get("fio") or director.get("name"))
        if fio:
            results.append((fio, "director"))
    elif clean_text(director):
        results.append((clean_text(director), "director"))

    founders = payload.get("founders")
    if isinstance(founders, list):
        for founder in founders:
            if isinstance(founder, dict):
                fio = clean_text(founder.get("fio") or founder.get("name"))
            else:
                fio = clean_text(founder)
            if fio:
                results.append((fio, "founder"))
    return results


def _ensure_person_entity(conn, person_name: str) -> int:
    person_entity_id = find_person_entity(conn, person_name)
    if person_entity_id is not None:
        return person_entity_id
    person_row = conn.execute(
        "SELECT id FROM entities WHERE entity_type='person' AND canonical_name=? LIMIT 1",
        (person_name,),
    ).fetchone()
    if person_row:
        return int(person_row[0])
    cur = conn.execute(
        "INSERT INTO entities(entity_type, canonical_name, description) VALUES('person', ?, ?)",
        (person_name, "Figure imported from company affiliation enrichment"),
    )
    return int(cur.lastrowid)


def _canonical_company_name(raw_name: str) -> str:
    cleaned = clean_text(raw_name).strip(" .,:;")
    cleaned = COMPANY_TAIL_SPLIT_RE.sub("", cleaned).strip(" .,:;")
    normalized = normalize_text(cleaned)
    for canonical, aliases in COMPANY_ALIAS_MAP.items():
        if any(alias in normalized for alias in aliases):
            return canonical
    cleaned = re.sub(r"^(?:ПАО|ОАО|АО|ООО|ЗАО|НАО|Банк|Госкорпорация|Инвестиционный\s+банк)\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip("«»\"' ")
    return clean_text(cleaned)


def _looks_like_company(name: str) -> bool:
    normalized = normalize_text(name)
    if not normalized or len(normalized) < 3:
        return False
    if any(stopword in normalized for stopword in COMPANY_STOPWORDS):
        return False
    if any(alias in normalized for aliases in COMPANY_ALIAS_MAP.values() for alias in aliases):
        return True
    if any(token in normalized for token in ("капитал", "банк", "холдинг", "корпорац", "акционер", "компани", "предприят")):
        return True
    return bool(COMPANY_PATTERN.search(name))


def _ensure_company_entity(conn, company_name: str) -> int:
    canonical_name = _canonical_company_name(company_name)
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type='organization' AND canonical_name=? LIMIT 1",
        (canonical_name,),
    ).fetchone()
    if row:
        return int(row[0]), canonical_name
    cur = conn.execute(
        "INSERT INTO entities(entity_type, canonical_name, description) VALUES('organization', ?, ?)",
        (canonical_name, "Company affiliation extracted from public profile or registry"),
    )
    return int(cur.lastrowid), canonical_name


def _extract_period(segment: str) -> tuple[str | None, str | None]:
    match = PERIOD_PATTERN.search(segment)
    if not match:
        return None, None
    start = clean_text(match.group("start")) or None
    end = clean_text(match.group("end")) or None
    return start, end


def _extract_role_near_match(before_text: str, after_text: str) -> tuple[str, str]:
    prefix_hits: list[tuple[int, str, str]] = []
    suffix_hits: list[tuple[int, str, str]] = []
    for pattern, role_type in ROLE_PATTERNS:
        for match in re.finditer(pattern, before_text, flags=re.IGNORECASE | re.UNICODE):
            prefix_hits.append((match.end(), role_type, clean_text(match.group(0))))
        for match in re.finditer(pattern, after_text, flags=re.IGNORECASE | re.UNICODE):
            suffix_hits.append((match.start(), role_type, clean_text(match.group(0))))
    if prefix_hits:
        prefix_hits.sort(key=lambda item: item[0], reverse=True)
        _pos, role_type, role_title = prefix_hits[0]
        return role_type, role_title
    if suffix_hits:
        suffix_hits.sort(key=lambda item: item[0])
        _pos, role_type, role_title = suffix_hits[0]
        return role_type, role_title
    return "", ""


def _extract_period_near_match(before_text: str, after_text: str) -> tuple[str | None, str | None]:
    prefix_hits = list(PERIOD_PATTERN.finditer(before_text))
    if prefix_hits:
        match = prefix_hits[-1]
        return clean_text(match.group("start")) or None, clean_text(match.group("end")) or None
    suffix_hits = list(PERIOD_PATTERN.finditer(after_text))
    if suffix_hits:
        match = suffix_hits[0]
        return clean_text(match.group("start")) or None, clean_text(match.group("end")) or None
    return None, None


def _company_mentions_from_segment(segment: str) -> list[tuple[str, int, int]]:
    results: list[tuple[str, int, int]] = []
    for canonical, aliases in COMPANY_ALIAS_MAP.items():
        for alias in sorted(aliases, key=len, reverse=True):
            pattern = re.compile(re.escape(alias), flags=re.IGNORECASE | re.UNICODE)
            for match in pattern.finditer(segment):
                results.append((canonical, match.start(), match.end()))
    for match in COMPANY_PATTERN.finditer(segment):
        canonical = _canonical_company_name(match.group(0))
        if canonical and _looks_like_company(canonical):
            results.append((canonical, match.start(), match.end()))

    deduped: list[tuple[str, int, int]] = []
    for canonical, start, end in sorted(results, key=lambda item: (item[1], item[2] - item[1], item[0])):
        if len(canonical) > 90:
            continue
        if any(
            existing_canonical == canonical and not (end <= existing_start or start >= existing_end)
            for existing_canonical, existing_start, existing_end in deduped
        ):
            continue
        deduped.append((canonical, start, end))
    return deduped


def _purge_profile_affiliations(conn) -> None:
    rows = conn.execute(
        """
        SELECT id
        FROM company_affiliations
        WHERE metadata_json LIKE '%"origin": "profile_biography"%'
        """
    ).fetchall()
    if not rows:
        return
    affiliation_ids = [int(row[0]) for row in rows]
    placeholders = ",".join("?" for _ in affiliation_ids)
    conn.execute(
        f"""
        DELETE FROM review_tasks
        WHERE task_key LIKE 'affiliation:profile:%'
           OR (subject_type='company_affiliation' AND subject_id IN ({placeholders}))
        """,
        tuple(affiliation_ids),
    )
    conn.execute(
        f"DELETE FROM company_affiliations WHERE id IN ({placeholders})",
        tuple(affiliation_ids),
    )


def _profile_rows(conn, limit: int):
    return conn.execute(
        """
        SELECT
            c.id,
            c.content_type,
            c.title,
            c.body_text,
            c.url,
            rs.raw_payload
        FROM content_items c
        LEFT JOIN raw_source_items rs ON rs.id = c.raw_item_id
        WHERE c.content_type IN ('official_profile', 'deputy_profile')
          AND COALESCE(c.body_text, '') <> ''
        ORDER BY c.id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def _insert_or_update_affiliation(
    conn,
    *,
    person_entity_id: int,
    company_entity_id: int,
    company_name: str,
    role_type: str,
    role_title: str,
    period_start: str | None,
    period_end: str | None,
    source_content_id: int | None,
    source_url: str | None,
    evidence_class: str,
    metadata: dict[str, Any],
) -> tuple[bool, int]:
    existing = conn.execute(
        """
        SELECT id
        FROM company_affiliations
        WHERE entity_id=? AND company_entity_id=? AND role_type=? AND COALESCE(source_content_id, 0)=COALESCE(?, 0)
        LIMIT 1
        """,
        (person_entity_id, company_entity_id, role_type, source_content_id),
    ).fetchone()
    payload = (
        company_name,
        role_title or None,
        period_start,
        period_end,
        source_content_id,
        clean_text(source_url) or None,
        evidence_class,
        json_dumps(metadata),
    )
    if existing:
        conn.execute(
            """
            UPDATE company_affiliations
            SET company_name=?, role_title=?, period_start=?, period_end=?, source_content_id=?,
                source_url=?, evidence_class=?, metadata_json=?, updated_at=datetime('now')
            WHERE id=?
            """,
            payload + (int(existing[0]),),
        )
        return False, int(existing[0])
    cur = conn.execute(
        """
        INSERT INTO company_affiliations(
            entity_id, company_entity_id, company_name, role_type, role_title,
            period_start, period_end, source_content_id, source_url, evidence_class, metadata_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            person_entity_id,
            company_entity_id,
            company_name,
            role_type,
            role_title or None,
            period_start,
            period_end,
            source_content_id,
            clean_text(source_url) or None,
            evidence_class,
            json_dumps(metadata),
        ),
    )
    return True, int(cur.lastrowid)


def _extract_affiliations_from_profiles(conn, *, limit: int) -> tuple[int, int]:
    created = 0
    updated = 0
    _purge_profile_affiliations(conn)
    for row in _profile_rows(conn, limit):
        payload = parse_json(row["raw_payload"], {})
        entity_id = None
        if isinstance(payload, dict):
            raw_entity_id = payload.get("entity_id")
            if isinstance(raw_entity_id, int):
                entity_id = raw_entity_id
            elif isinstance(raw_entity_id, str) and raw_entity_id.isdigit():
                entity_id = int(raw_entity_id)
        if entity_id is None:
            entity_id = find_person_entity(conn, clean_text(row["title"]))
        if entity_id is None:
            continue
        body_text = clean_text(row["body_text"])
        if not body_text:
            continue
        for segment in SEGMENT_SPLIT_RE.split(body_text):
            segment = clean_text(segment)
            if len(segment) < 12:
                continue
            mentions = _company_mentions_from_segment(segment)
            if not mentions:
                continue
            for company_name, start, end in mentions:
                before_text = segment[max(0, start - 160):start]
                after_text = segment[end:min(len(segment), end + 140)]
                role_type, role_title = _extract_role_near_match(before_text, after_text)
                if not role_type:
                    continue
                period_start, period_end = _extract_period_near_match(before_text, after_text)
                company_entity_id, canonical_company_name = _ensure_company_entity(conn, company_name)
                inserted, affiliation_id = _insert_or_update_affiliation(
                    conn,
                    person_entity_id=entity_id,
                    company_entity_id=company_entity_id,
                    company_name=canonical_company_name,
                    role_type=role_type,
                    role_title=role_title or role_type,
                    period_start=period_start,
                    period_end=period_end,
                    source_content_id=int(row["id"]),
                    source_url=clean_text(row["url"]) or None,
                    evidence_class="support",
                    metadata={"origin": "profile_biography", "content_type": row["content_type"], "segment": segment},
                )
                if inserted:
                    created += 1
                else:
                    updated += 1
                ensure_review_task(
                    conn,
                    task_key=f"affiliation:profile:{entity_id}:{company_entity_id}:{role_type}:{row['id']}",
                    queue_key="assets_affiliations",
                    subject_type="company_affiliation",
                    subject_id=affiliation_id,
                    related_id=company_entity_id,
                    candidate_payload={
                        "entity_id": entity_id,
                        "company_name": canonical_company_name,
                        "role_type": role_type,
                        "role_title": role_title or role_type,
                        "period_start": period_start,
                        "period_end": period_end,
                        "source_content_id": int(row["id"]),
                    },
                    suggested_action="promote",
                    confidence=0.78,
                    machine_reason="Public profile biography indicates company affiliation",
                    source_links=[row["url"]] if clean_text(row["url"]) else [],
                )
    return created, updated


def run_company_registry_enrichment(settings: dict[str, Any] | None = None, *, limit: int = 200) -> dict[str, Any]:
    settings = settings or {}
    warnings: list[str] = []
    fresh = 0
    try:
        module = __import__("collectors.official_scraper", fromlist=["egrul_collect_by_inn_list"])
        conn = open_db(settings)
        try:
            inns = [
                row[0]
                for row in conn.execute(
                    """
                    SELECT DISTINCT inn
                    FROM entities
                    WHERE entity_type='organization'
                      AND inn IS NOT NULL
                      AND length(inn) IN (10, 12)
                    ORDER BY id
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            ]
        finally:
            conn.close()
        fresh = module.egrul_collect_by_inn_list(settings, inn_list=inns) or 0
    except Exception as error:
        warnings.append(f"egrul_collect:{error}")

    conn = open_db(settings)
    created = 0
    updated = 0
    registry_rows_seen = 0
    profile_rows_seen = 0
    try:
        rows = conn.execute(
            """
            SELECT e.id AS company_entity_id, e.canonical_name, e.inn, e.extra_data
            FROM entities e
            WHERE e.entity_type='organization' AND COALESCE(e.extra_data, '') <> ''
            ORDER BY e.id
            LIMIT ?
            """,
            (max(limit, 200),),
        ).fetchall()
        registry_rows_seen = len(rows)
        for row in rows:
            payload = parse_json(row["extra_data"], {})
            if not isinstance(payload, dict):
                continue
            for person_name, role_type in _extract_people(payload):
                person_entity_id = _ensure_person_entity(conn, person_name)
                source_url = f"https://egrul.nalog.ru/entity/{clean_text(row['inn'])}" if clean_text(row["inn"]) else None
                inserted, affiliation_id = _insert_or_update_affiliation(
                    conn,
                    person_entity_id=person_entity_id,
                    company_entity_id=int(row["company_entity_id"]),
                    company_name=row["canonical_name"],
                    role_type=role_type,
                    role_title=role_type,
                    period_start=None,
                    period_end=None,
                    source_content_id=None,
                    source_url=source_url,
                    evidence_class="hard",
                    metadata={"registry_inn": row["inn"], "registry_payload": payload},
                )
                if inserted:
                    created += 1
                else:
                    updated += 1
                ensure_review_task(
                    conn,
                    task_key=f"affiliation:registry:{person_entity_id}:{row['company_entity_id']}:{role_type}",
                    queue_key="assets_affiliations",
                    subject_type="company_affiliation",
                    subject_id=affiliation_id,
                    related_id=int(row["company_entity_id"]),
                    candidate_payload={
                        "company_name": row["canonical_name"],
                        "role_type": role_type,
                        "inn": row["inn"],
                    },
                    suggested_action="promote",
                    confidence=0.92,
                    machine_reason="EGRUL registry role extracted",
                    source_links=[source_url] if source_url else [],
                )

        profile_limit = int(settings.get("company_affiliations_profile_limit", max(limit * 6, 600)) or max(limit * 6, 600))
        profile_rows_seen = len(_profile_rows(conn, profile_limit))
        profile_created, profile_updated = _extract_affiliations_from_profiles(conn, limit=profile_limit)
        created += profile_created
        updated += profile_updated
        conn.commit()
        return {
            "ok": True,
            "items_seen": registry_rows_seen + profile_rows_seen,
            "items_new": created,
            "items_updated": updated + int(fresh or 0),
            "warnings": warnings,
            "artifacts": {
                "registry_rows_seen": registry_rows_seen,
                "profile_rows_seen": profile_rows_seen,
                "egrul_refresh_items": int(fresh or 0),
            },
        }
    finally:
        conn.close()
