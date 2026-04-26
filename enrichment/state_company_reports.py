from __future__ import annotations

from typing import Any

import requests
import urllib3
from bs4 import BeautifulSoup
from requests import exceptions as requests_exceptions

from enrichment.common import clean_text, ensure_content_item, ensure_raw_item, open_db, resolve_source_for_url

STATE_COMPANY_TARGETS = [
    {"name": "АвтоВАЗ", "url": "https://www.avtovaz.ru/company/management", "organization": "АвтоВАЗ"},
    {"name": "Роснефть", "url": "https://www.rosneft.ru/governance/corpboard/", "organization": "Роснефть"},
    {"name": "Газпром", "url": "https://www.gazprom.ru/about/management/", "organization": "Газпром", "allow_insecure_tls": True},
    {"name": "Ростех", "url": "https://rostec.ru/about/management/", "organization": "Ростех"},
    {"name": "Росатом", "url": "https://www.rosatom.ru/about/management/", "organization": "Росатом"},
    {"name": "РЖД", "url": "https://company.rzd.ru/ru/9353/page/105104?id=1745", "organization": "РЖД"},
    {"name": "Сбер", "url": "https://www.sberbank.com/ru/investor-relations/corporate-governance/management-board", "organization": "Сбер"},
    {"name": "ВТБ", "url": "https://www.vtb.ru/ir/governance/management/", "organization": "ВТБ"},
    {"name": "Ростелеком", "url": "https://www.company.rt.ru/ir/corporate_governance/management/", "organization": "Ростелеком"},
]


def _fetch_company_page(session: requests.Session, item: dict[str, str]) -> requests.Response:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = session.get(item["url"], timeout=30, headers=headers)
        response.raise_for_status()
        return response
    except requests_exceptions.SSLError:
        if item.get("allow_insecure_tls"):
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            response = session.get(item["url"], timeout=30, headers=headers, verify=False)
            response.raise_for_status()
            return response
        raise


def _page_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.select("script, style, noscript, svg"):
        tag.decompose()
    text = "\n".join(part.strip() for part in soup.get_text("\n").splitlines() if part.strip())
    return text[:25000]


def run_state_company_reports(settings: dict[str, Any] | None = None, *, targets: list[dict[str, str]] | None = None) -> dict[str, Any]:
    settings = settings or {}
    targets = targets or STATE_COMPANY_TARGETS
    session = requests.Session()
    conn = open_db(settings)
    created = 0
    warnings: list[str] = []
    try:
        for item in targets:
            try:
                response = _fetch_company_page(session, item)
                body_text = _page_text(response.text)
                source_id = resolve_source_for_url(
                    conn,
                    url=response.url or item["url"],
                    fallback_name=f"{item['name']} — управление и отчёты",
                    fallback_category="official_site",
                    fallback_subcategory="state_company",
                    is_official=1,
                )
                external_id = f"state-company:{item['organization']}"
                raw_item_id = ensure_raw_item(
                    conn,
                    source_id=source_id,
                    external_id=external_id,
                    raw_payload={
                        "organization": item["organization"],
                        "url": response.url or item["url"],
                        "title": item["name"],
                    },
                )
                existing = conn.execute(
                    "SELECT id FROM content_items WHERE source_id=? AND external_id=? LIMIT 1",
                    (source_id, external_id),
                ).fetchone()
                ensure_content_item(
                    conn,
                    source_id=source_id,
                    raw_item_id=raw_item_id,
                    external_id=external_id,
                    content_type="state_company_report",
                    title=f"{item['name']} — руководство и отчёты",
                    body_text=body_text,
                    published_at=None,
                    url=response.url or item["url"],
                    status="official_document",
                )
                if not existing:
                    created += 1
            except Exception as error:
                warnings.append(f"{item['name']}: {error}")
        conn.commit()
        return {"ok": True, "items_seen": len(targets), "items_new": created, "warnings": warnings}
    finally:
        conn.close()
