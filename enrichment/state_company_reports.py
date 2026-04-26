from __future__ import annotations

from typing import Any

import requests
import urllib3
from bs4 import BeautifulSoup
from requests import exceptions as requests_exceptions

from config.source_health import (
    fallback_urls,
    find_fixture_path,
    load_source_health_manifest,
    manifest_entry,
    primary_urls,
)
from enrichment.common import clean_text, ensure_content_item, ensure_raw_item, open_db, resolve_source_for_url
from runtime.state import register_source_fixture, update_source_sync_state

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


def _fetch_company_page_candidates(
    session: requests.Session,
    item: dict[str, str],
    entry: dict[str, Any],
) -> requests.Response:
    urls = []
    urls.extend(primary_urls(entry))
    urls.append(item["url"])
    urls.extend(fallback_urls(entry))
    last_error: Exception | None = None
    for candidate in list(dict.fromkeys(url for url in urls if url)):
        candidate_item = dict(item)
        candidate_item["url"] = candidate
        try:
            return _fetch_company_page(session, candidate_item)
        except Exception as error:  # noqa: BLE001
            last_error = error
            continue
    if last_error:
        raise last_error
    raise RuntimeError(f"missing_source_urls:{item.get('name')}")


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
    manifest = load_source_health_manifest(settings)
    try:
        for item in targets:
            source_key = item.get("source_key") or f"state_company_reports:{item['name']}"
            entry = manifest_entry(source_key, settings=settings, manifest=manifest)
            try:
                response = _fetch_company_page_candidates(session, item, entry)
                body_text = _page_text(response.text)
                fallback_used = "archive" if response.url and "web.archive.org" in response.url else None
                archive_derived = bool(fallback_used)
                fixture_id = None
                final_url = response.url or item["url"]
            except Exception as error:
                fixture_path = find_fixture_path(source_key, settings=settings, manifest=manifest)
                if fixture_path:
                    body_text = _page_text(fixture_path.read_text(encoding="utf-8", errors="ignore"))
                    final_url = str(fixture_path)
                    fallback_used = "fixture"
                    archive_derived = True
                    fixture_id = register_source_fixture(
                        conn,
                        source_key=source_key,
                        fixture_kind="archive_fixture" if entry.get("acceptance_mode") == "archive_ok" else "local_fixture",
                        origin_url=item["url"],
                        local_path=str(fixture_path),
                        metadata={"acceptance_mode": entry.get("acceptance_mode"), "quality_expectations": entry.get("quality_expectations")},
                    )
                else:
                    warnings.append(f"{item['name']}: {error}")
                    update_source_sync_state(
                        conn,
                        source_key=source_key,
                        success=False,
                        state="degraded",
                        transport_mode="state_company_reports",
                        last_error=str(error),
                        metadata={
                            "acceptance_mode": entry.get("acceptance_mode") or "direct_only",
                            "organization": item["organization"],
                            "primary_urls": primary_urls(entry) or [item["url"]],
                            "fallback_urls": fallback_urls(entry),
                        },
                    )
                    continue
            try:
                source_id = resolve_source_for_url(
                    conn,
                    url=item["url"],
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
                        "url": final_url,
                        "title": item["name"],
                        "archive_derived": archive_derived,
                        "fallback_used": fallback_used,
                        "fixture_id": fixture_id,
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
                    url=item["url"],
                    status="official_document",
                )
                if not existing:
                    created += 1
                update_source_sync_state(
                    conn,
                    source_key=source_key,
                    source_id=source_id,
                    success=True,
                    state="ok",
                    transport_mode="state_company_reports",
                    quality_state="ok",
                    metadata={
                        "acceptance_mode": entry.get("acceptance_mode") or "direct_only",
                        "organization": item["organization"],
                        "primary_urls": primary_urls(entry) or [item["url"]],
                        "fallback_urls": fallback_urls(entry),
                        "fallback_used": fallback_used,
                        "archive_derived": archive_derived,
                        "fixture_id": fixture_id,
                        "resolved_url": final_url,
                    },
                )
            except Exception as error:
                warnings.append(f"{item['name']}: {error}")
        conn.commit()
        return {"ok": True, "items_seen": len(targets), "items_new": created, "warnings": warnings}
    finally:
        conn.close()
