from __future__ import annotations

import re
from typing import Any

import requests
from bs4 import BeautifulSoup

from enrichment.common import (
    clean_text,
    ensure_content_item,
    ensure_raw_item,
    ensure_review_task,
    find_person_entity,
    json_dumps,
    now_iso,
    open_db,
    parse_money_amount,
    resolve_source_for_url,
    stable_hash,
)

WAYBACK_CDX = "https://web.archive.org/cdx/search/cdx"
DUMA_PROPERTY_URL_TEMPLATE = "http://duma.gov.ru/duma/persons/properties/{year}/"
DEFAULT_DUMA_YEARS = (2024, 2023, 2022, 2021, 2020)
SPOUSE_PREFIXES = ("супруга", "супруг", "несовершеннолетний", "ребенок", "ребёнок")
TRANSPORT_RE = re.compile(r"\bавтомобил|транспорт|мотоцикл|судно\b", re.IGNORECASE)
ARCHIVE_FALLBACKS: dict[int, tuple[str, ...]] = {
    2021: ("https://web.archive.org/web/20220415143334/http://duma.gov.ru/duma/persons/properties/2021/",),
    2020: ("https://web.archive.org/web/20210418195118/http://duma.gov.ru/duma/persons/properties/2020/",),
}


def _find_or_create_disclosure_content(
    conn,
    *,
    source_id: int,
    year: int,
    page_url: str,
    row_count: int,
) -> int:
    title = f"Сведения о доходах депутатов Государственной Думы за {year} год"
    raw_item_id = ensure_raw_item(
        conn,
        source_id=source_id,
        external_id=f"duma-declaration:{year}",
        raw_payload={"year": year, "page_url": page_url, "row_count": row_count},
    )
    return ensure_content_item(
        conn,
        source_id=source_id,
        raw_item_id=raw_item_id,
        external_id=f"duma-declaration:{year}",
        content_type="anticorruption_declaration",
        title=title,
        body_text=f"Архивная страница деклараций Госдумы за {year} год.\nИсточник: {page_url}",
        published_at=f"{year}-12-31",
        url=page_url,
    )


def _insert_asset(conn, *, disclosure_id: int, entity_id: int | None, owner_role: str, asset_type: str, ownership_type: str, area_text: str, country: str, usage_type: str = "", source_url: str = "") -> int:
    cur = conn.execute(
        """
        INSERT INTO declared_assets(
            disclosure_id, entity_id, owner_role, asset_type, ownership_type,
            area_text, area_value, country, usage_type, source_url, metadata_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            disclosure_id,
            entity_id,
            owner_role,
            clean_text(asset_type),
            clean_text(ownership_type) or None,
            clean_text(area_text) or None,
            parse_money_amount(area_text),
            clean_text(country) or None,
            clean_text(usage_type) or None,
            clean_text(source_url) or None,
            json_dumps({"imported_at": now_iso()}),
        ),
    )
    return int(cur.lastrowid)


def ingest_duma_property_html(
    conn,
    *,
    source_id: int,
    html: str,
    year: int,
    page_url: str,
) -> dict[str, int]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if table is None:
        return {"ok": False, "disclosures_created": 0, "assets_created": 0}

    rows = table.find_all("tr")
    if len(rows) <= 1:
        return {"ok": True, "disclosures_created": 0, "assets_created": 0}

    content_id = _find_or_create_disclosure_content(conn, source_id=source_id, year=year, page_url=page_url, row_count=max(len(rows) - 1, 0))
    disclosures_created = 0
    assets_created = 0
    current_disclosure_id: int | None = None
    current_entity_id: int | None = None

    for row in rows[1:]:
        cells = [clean_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["td", "th"])]
        if len(cells) < 12:
            continue
        name = cells[1]
        own_asset = cells[3]
        own_kind = cells[4]
        own_area = cells[5]
        own_country = cells[6]
        use_asset = cells[7]
        use_area = cells[8]
        use_country = cells[9]
        income_text = cells[11]

        lower_name = name.lower()
        is_relative = any(lower_name.startswith(prefix) for prefix in SPOUSE_PREFIXES)
        if not is_relative:
            entity_id = find_person_entity(conn, name)
            if not entity_id:
                continue
            current_entity_id = entity_id
            disclosure_row = conn.execute(
                """
                SELECT id
                FROM person_disclosures
                WHERE entity_id=? AND disclosure_year=? AND source_url=?
                LIMIT 1
                """,
                (entity_id, year, page_url),
            ).fetchone()
            if disclosure_row:
                current_disclosure_id = int(disclosure_row[0])
            else:
                cur = conn.execute(
                    """
                    INSERT INTO person_disclosures(
                        entity_id, disclosure_year, source_content_id, source_url, source_type,
                        income_amount, raw_income_text, source_scope, evidence_class, metadata_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        entity_id,
                        year,
                        content_id,
                        page_url,
                        "official_archive",
                        parse_money_amount(income_text),
                        income_text or None,
                        "deputy_property_page",
                        "hard",
                        json_dumps({"full_name": name, "position": cells[2]}),
                    ),
                )
                current_disclosure_id = int(cur.lastrowid)
                disclosures_created += 1
            if income_text:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO compensation_facts(
                        entity_id, compensation_year, amount, amount_text, role_title,
                        fact_type, source_content_id, source_url, evidence_class, metadata_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        entity_id,
                        year,
                        parse_money_amount(income_text),
                        income_text,
                        clean_text(cells[2]) or None,
                        "income",
                        content_id,
                        page_url,
                        "hard",
                        json_dumps({"source": "duma_property_archive"}),
                    ),
                )
                conn.execute(
                    "UPDATE deputy_profiles SET income_latest=? WHERE entity_id=?",
                    (income_text, entity_id),
                )
        if not current_disclosure_id:
            continue

        owner_role = "spouse" if is_relative else "self"
        target_entity_id = current_entity_id if owner_role == "self" else None
        if own_asset:
            assets_created += _insert_asset(
                conn,
                disclosure_id=current_disclosure_id,
                entity_id=target_entity_id,
                owner_role=owner_role,
                asset_type=own_asset,
                ownership_type=own_kind,
                area_text=own_area,
                country=own_country,
                source_url=page_url,
            ) and 1 or 0
        if use_asset and not is_relative:
            assets_created += _insert_asset(
                conn,
                disclosure_id=current_disclosure_id,
                entity_id=target_entity_id,
                owner_role=owner_role,
                asset_type=use_asset,
                ownership_type="use",
                area_text=use_area,
                country=use_country,
                usage_type="use",
                source_url=page_url,
            ) and 1 or 0
        if cells[10] and not is_relative and not TRANSPORT_RE.search(cells[10] or ""):
            assets_created += _insert_asset(
                conn,
                disclosure_id=current_disclosure_id,
                entity_id=target_entity_id,
                owner_role=owner_role,
                asset_type=cells[10],
                ownership_type="transport",
                area_text="",
                country="",
                source_url=page_url,
            ) and 1 or 0

        ensure_review_task(
            conn,
            task_key=f"assets:{current_disclosure_id}",
            queue_key="assets_affiliations",
            subject_type="person_disclosure",
            subject_id=current_disclosure_id,
            candidate_payload={
                "disclosure_year": year,
                "page_url": page_url,
                "entity_id": current_entity_id,
            },
            suggested_action="promote",
            confidence=0.93,
            machine_reason="Official anti-corruption declaration page imported",
            source_links=[page_url],
        )

    return {
        "ok": True,
        "disclosures_created": disclosures_created,
        "assets_created": assets_created,
        "content_items_created": 1 if content_id else 0,
    }


def _wayback_snapshot(url: str, year: int) -> str | None:
    params = {
        "url": url,
        "from": str(year),
        "to": str(year + 2),
        "output": "json",
        "fl": "timestamp,original,statuscode",
        "filter": "statuscode:200",
        "limit": "1",
    }
    try:
        response = requests.get(WAYBACK_CDX, params=params, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        payload = response.json()
        if len(payload) >= 2:
            timestamp = payload[1][0]
            return f"https://web.archive.org/web/{timestamp}/{url}"
    except Exception:
        pass
    fallbacks = ARCHIVE_FALLBACKS.get(year) or ()
    return next(iter(fallbacks), None)


def run_anticorruption_disclosures(settings: dict[str, Any] | None = None, *, years: tuple[int, ...] = DEFAULT_DUMA_YEARS) -> dict[str, Any]:
    settings = settings or {}
    conn = open_db(settings)
    warnings: list[str] = []
    disclosures_created = 0
    assets_created = 0
    source_id = resolve_source_for_url(
        conn,
        url="https://duma.gov.ru/anticorruption/",
        fallback_name="Сведения о доходах депутатов",
        fallback_category="official_site",
        fallback_subcategory="anticorruption",
        is_official=1,
    )
    try:
        for year in years:
            page_url = DUMA_PROPERTY_URL_TEMPLATE.format(year=year)
            try:
                snapshot_url = _wayback_snapshot(page_url, year)
                if not snapshot_url:
                    warnings.append(f"duma:{year}:snapshot_not_found")
                    continue
                response = requests.get(snapshot_url, timeout=40, headers={"User-Agent": "Mozilla/5.0"})
                response.raise_for_status()
                stats = ingest_duma_property_html(conn, source_id=source_id, html=response.text, year=year, page_url=snapshot_url)
                disclosures_created += int(stats.get("disclosures_created") or 0)
                assets_created += int(stats.get("assets_created") or 0)
            except Exception as error:
                warnings.append(f"duma:{year}:{error}")
        conn.commit()
        return {
            "ok": True,
            "items_seen": len(tuple(years)),
            "items_new": disclosures_created,
            "items_updated": assets_created,
            "disclosures_created": disclosures_created,
            "assets_created": assets_created,
            "warnings": warnings,
        }
    finally:
        conn.close()
