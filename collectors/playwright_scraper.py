import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)


def _get_proxy_settings(settings: dict = None) -> Optional[Dict]:
    if settings is None:
        return None
    proxy_url = settings.get("http_proxy") or settings.get("https_proxy")
    if proxy_url:
        return {"server": proxy_url}
    return None


def _get_source_id(conn: sqlite3.Connection, url_contains: str) -> Optional[int]:
    row = conn.execute(
        "SELECT id FROM sources WHERE url LIKE ? AND is_active=1 LIMIT 1",
        (f"%{url_contains}%",),
    ).fetchone()
    return row[0] if row else None


def _store_raw_and_content(conn: sqlite3.Connection, source_id: int, ext_id: str,
                           raw_json: str, content_type: str, title: str,
                           body: str, published: str, url: str) -> int:
    raw_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
    existing = conn.execute(
        "SELECT id FROM raw_source_items WHERE source_id=? AND external_id=?",
        (source_id, ext_id),
    ).fetchone()
    if existing:
        return existing[0]

    cur = conn.execute(
        """INSERT INTO raw_source_items(source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed)
           VALUES(?,?,?,?,?,1)""",
        (source_id, ext_id, raw_json, datetime.now().isoformat(), raw_hash),
    )
    raw_id = cur.lastrowid

    conn.execute(
        """INSERT INTO content_items(source_id, raw_item_id, external_id, content_type, title, body_text, published_at, collected_at, url, status)
           VALUES(?,?,?,?,?,?,?,?,?,'raw_signal')""",
        (source_id, raw_id, ext_id, content_type, title, body[:50000], published, datetime.now().isoformat(), url),
    )
    return raw_id


def _launch_browser(proxy: Optional[Dict] = None):
    from playwright.sync_api import sync_playwright
    pw = sync_playwright().start()
    launch_opts = {"headless": True}
    context_opts = {
        "locale": "ru-RU",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    if proxy:
        context_opts["proxy"] = proxy
    browser = pw.chromium.launch(**launch_opts)
    context = browser.new_context(**context_opts)
    return pw, browser, context


def _cleanup(pw, browser):
    try:
        browser.close()
    except Exception:
        pass
    try:
        pw.stop()
    except Exception:
        pass


# ============================================================
# FSSP — is.fssp.gov.ru (JS-rendered search)
# ============================================================

def fssp_playwright_search(lastname: str, firstname: str, secondname: str = "",
                           region_id: int = -1, proxy: Optional[Dict] = None,
                           timeout_ms: int = 15000) -> List[Dict]:
    pw, browser, context = None, None, None
    results = []
    try:
        pw, browser, context = _launch_browser(proxy)
        page = context.new_page()
        page.goto("https://is.fssp.gov.ru/", timeout=timeout_ms, wait_until="networkidle")
        page.wait_for_timeout(2000)

        lastname_input = page.query_selector("input[name='is[lastname]'], #lastname, input[placeholder*='амили']")
        firstname_input = page.query_selector("input[name='is[firstname]'], #firstname, input[placeholder*='мя']")
        secondname_input = page.query_selector("input[name='is[secondname]'], #secondname, input[placeholder*='тчеств']")

        if lastname_input:
            lastname_input.fill(lastname)
        if firstname_input:
            firstname_input.fill(firstname)
        if secondname_input and secondname:
            secondname_input.fill(secondname)

        region_select = page.query_selector("select[name='is[region_id][0]'], #region_id")
        if region_select and region_id >= 0:
            region_select.select_option(value=str(region_id))

        submit_btn = page.query_selector("button[type='submit'], input[type='submit'], .btn-search, .search-btn")
        if not submit_btn:
            submit_btn = page.query_selector("button:has-text('Найти'), button:has-text('Искать')")
        if submit_btn:
            submit_btn.click()
        else:
            page.keyboard.press("Enter")

        page.wait_for_timeout(5000)
        page.wait_for_load_state("networkidle", timeout=timeout_ms)

        rows = page.query_selector_all("table tbody tr, .result-item, .search-result-row, .executive-row")
        for row in rows:
            cells = row.query_selector_all("td, .cell, .result-cell")
            if len(cells) < 2:
                continue
            row_text = row.inner_text()
            cols = [c.inner_text().strip() for c in cells]
            results.append({
                "raw_text": row_text,
                "cols": cols,
                "detail": cols[0] if cols else "",
                "amount": cols[1] if len(cols) > 1 else "",
                "status": cols[2] if len(cols) > 2 else "",
            })

        if not rows:
            body_text = page.inner_text("body")
            if "исполнительн" in body_text.lower() or "производств" in body_text.lower():
                results.append({"raw_text": body_text[:2000], "cols": [], "detail": "", "amount": "", "status": ""})

    except Exception as e:
        log.warning("FSSP Playwright search failed for %s %s: %s", lastname, firstname, e)
    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass
        _cleanup(pw, browser)
    return results


def fssp_playwright_collect(settings: dict = None, names: List[Dict] = None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "fssp.gov.ru")
    if not source_id:
        log.warning("No FSSP source in DB")
        conn.close()
        return 0

    if names is None:
        rows = conn.execute(
            """SELECT canonical_name FROM entities
               WHERE entity_type='person' AND canonical_name LIKE '% %' LIMIT 50"""
        ).fetchall()
        names = []
        for r in rows:
            parts = r[0].split()
            if len(parts) >= 2:
                names.append({"last": parts[0], "first": parts[1], "middle": parts[2] if len(parts) > 2 else ""})

    if not names:
        log.info("No person names for FSSP Playwright search")
        conn.close()
        return 0

    proxy = _get_proxy_settings(settings)
    log.info("FSSP Playwright: searching %d names", len(names))
    collected = 0

    for n in names[:30]:
        results = fssp_playwright_search(n["last"], n["first"], n.get("middle", ""), proxy=proxy)
        if not results:
            continue

        raw_json = json.dumps(results, ensure_ascii=False, default=str)
        body_parts = [f"ФИО: {n['last']} {n['first']} {n.get('middle', '')}"]
        body_parts.append(f"Производств: {len(results)}")
        for item in results[:10]:
            body_parts.append(f"  {item.get('detail', '')} — {item.get('amount', '')}")
        body = "\n".join(body_parts)

        ext_id = f"fssp_pw_{hashlib.sha256((n['last']+n['first']).encode()).hexdigest()[:16]}"
        _store_raw_and_content(
            conn, source_id, ext_id, raw_json, "enforcement",
            f"Исп. производства (FSSP): {n['last']} {n['first']}", body, "",
            "https://is.fssp.gov.ru/",
        )
        collected += 1
        time.sleep(2)

    conn.commit()
    log.info("FSSP Playwright: collected %d results", collected)
    conn.close()
    return collected


# ============================================================
# MINJUST — иноагенты (JS-rendered table)
# ============================================================

def minjust_playwright_collect(settings: dict = None, pages: int = 5):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "minjust.gov.ru")
    if not source_id:
        log.warning("No Minjust source in DB")
        conn.close()
        return 0

    proxy = _get_proxy_settings(settings)
    pw, browser, context = None, None, None
    collected = 0

    try:
        pw, browser, context = _launch_browser(proxy)
        page = context.new_page()

        for pg in range(1, pages + 1):
            try:
                url = "https://minjust.gov.ru/ru/activity/directions/reestr-inostrannykh-agentov/"
                if pg > 1:
                    url += f"?page={pg}"

                page.goto(url, timeout=30000, wait_until="networkidle")
                page.wait_for_timeout(3000)

                rows = page.query_selector_all("table tbody tr, .agent-row, .registry-item, .reestr-item")
                if not rows:
                    rows = page.query_selector_all("tr")

                for row in rows:
                    cells = row.query_selector_all("td, .cell")
                    if len(cells) < 2:
                        continue
                    cols = [c.inner_text().strip() for c in cells]
                    name = cols[0] if cols else ""
                    reg_date = cols[1] if len(cols) > 1 else ""
                    reason = cols[2] if len(cols) > 2 else ""

                    if not name or len(name) < 2:
                        continue

                    raw = {"name": name, "reg_date": reg_date, "reason": reason, "page": pg}
                    raw_json = json.dumps(raw, ensure_ascii=False)
                    ext_id = f"minoagent_pw_{hashlib.sha256(name.encode()).hexdigest()[:16]}"

                    _store_raw_and_content(
                        conn, source_id, ext_id, raw_json, "registry_record",
                        f"Иноагент: {name}", f"Дата: {reg_date}\nОснование: {reason}",
                        reg_date, url,
                    )
                    collected += 1

                next_btn = page.query_selector(".pagination .next a, a.next, .page-next, li.next a")
                if not next_btn:
                    break
            except Exception as e:
                log.warning("Minjust Playwright page %d failed: %s", pg, e)
                continue

    except Exception as e:
        log.error("Minjust Playwright scraper failed: %s", e)
    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass
        _cleanup(pw, browser)
        conn.commit()
        log.info("Minjust Playwright: collected %d records", collected)
        conn.close()
    return collected


# ============================================================
# DUMA — sozd.duma.gov.ru (JS SPA)
# ============================================================

def duma_playwright_bills_collect(settings: dict = None, pages: int = 5):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "sozd.duma.gov.ru")
    if not source_id:
        source_id = _get_source_id(conn, "duma.gov.ru")
    if not source_id:
        log.warning("No Duma source in DB")
        conn.close()
        return 0

    proxy = _get_proxy_settings(settings)
    pw, browser, context = None, None, None
    collected = 0

    try:
        pw, browser, context = _launch_browser(proxy)
        page = context.new_page()

        for pg in range(1, pages + 1):
            try:
                url = f"https://sozd.duma.gov.ru/bills/?page={pg}"
                page.goto(url, timeout=30000, wait_until="networkidle")
                page.wait_for_timeout(4000)

                rows = page.query_selector_all("table tbody tr, .bill-row, .search-result-item, .bill-item")
                if not rows:
                    rows = page.query_selector_all("tr")

                for row in rows:
                    cells = row.query_selector_all("td, .cell")
                    if len(cells) < 2:
                        link_el = row.query_selector("a[href*='bill'], a[href*='law']")
                        if link_el:
                            href = link_el.get_attribute("href") or ""
                            title = link_el.inner_text().strip()
                            if title and len(title) > 3:
                                full_url = f"https://sozd.duma.gov.ru{href}" if href.startswith("/") else href
                                raw = {"title": title, "url": full_url, "page": pg}
                                raw_json = json.dumps(raw, ensure_ascii=False)
                                ext_id = f"duma_pw_{hashlib.sha256((title+str(pg)).encode()).hexdigest()[:16]}"
                                _store_raw_and_content(
                                    conn, source_id, ext_id, raw_json, "bill",
                                    f"Законопроект: {title[:100]}", f"URL: {full_url}", "", full_url,
                                )
                                collected += 1
                        continue

                    cols = [c.inner_text().strip() for c in cells]
                    number = cols[0] if cols else ""
                    name = cols[1] if len(cols) > 1 else ""
                    status = cols[2] if len(cols) > 2 else ""

                    link_el = cells[0].query_selector("a") or cells[1].query_selector("a") if len(cells) > 1 else None
                    link = ""
                    if link_el:
                        href = link_el.get_attribute("href") or ""
                        link = f"https://sozd.duma.gov.ru{href}" if href.startswith("/") else href

                    if not number and not name:
                        continue

                    raw = {"number": number, "name": name, "status": status, "link": link, "page": pg}
                    raw_json = json.dumps(raw, ensure_ascii=False)
                    ext_id = f"duma_pw_{hashlib.sha256((number+name[:30]).encode()).hexdigest()[:16]}"

                    _store_raw_and_content(
                        conn, source_id, ext_id, raw_json, "bill",
                        f"Законопроект {number}: {name[:100]}",
                        f"Номер: {number}\nНазвание: {name}\nСтатус: {status}",
                        "", link or "https://sozd.duma.gov.ru/",
                    )
                    collected += 1

                next_btn = page.query_selector(".pagination .next a, a.next, .page-next, li.next a")
                if not next_btn:
                    break
            except Exception as e:
                log.warning("Duma Playwright page %d failed: %s", pg, e)
                continue

    except Exception as e:
        log.error("Duma Playwright scraper failed: %s", e)
    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass
        _cleanup(pw, browser)
        conn.commit()
        log.info("Duma Playwright: collected %d bills", collected)
        conn.close()
    return collected


def duma_playwright_deputies_collect(settings: dict = None, pages: int = 3):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "duma.gov.ru")
    if not source_id:
        source_id = _get_source_id(conn, "sozd.duma.gov.ru")
    if not source_id:
        log.warning("No Duma source in DB")
        conn.close()
        return 0

    proxy = _get_proxy_settings(settings)
    pw, browser, context = None, None, None
    collected = 0

    try:
        pw, browser, context = _launch_browser(proxy)
        page = context.new_page()

        for pg in range(1, pages + 1):
            try:
                url = f"https://duma.gov.ru/duma/deputies/?page={pg}"
                page.goto(url, timeout=30000, wait_until="networkidle")
                page.wait_for_timeout(3000)

                cards = page.query_selector_all(".deputy-card, .deputy-item, .person-card, .deputy-list-item, .card")
                if not cards:
                    rows = page.query_selector_all("table tbody tr, .deputy-row, .person-row")
                    cards = rows

                for card in cards:
                    name_el = card.query_selector("a[href*='deputy'], .deputy-name, .name, h3, h4")
                    faction_el = card.query_selector(".faction, .party, .fraction, .deputy-faction")
                    region_el = card.query_selector(".region, .deputy-region, .area")
                    committee_el = card.query_selector(".committee, .deputy-committee")
                    photo_el = card.query_selector("img[src*='deputy'], img[src*='photo']")

                    name = name_el.inner_text().strip() if name_el else ""
                    faction = faction_el.inner_text().strip() if faction_el else ""
                    region = region_el.inner_text().strip() if region_el else ""
                    committee = committee_el.inner_text().strip() if committee_el else ""
                    photo_url = photo_el.get_attribute("src") if photo_el else ""
                    link = name_el.get_attribute("href") if name_el else ""
                    if link and not link.startswith("http"):
                        link = f"https://duma.gov.ru{link}"

                    if not name or len(name) < 3:
                        continue

                    raw = {
                        "name": name, "faction": faction, "region": region,
                        "committee": committee, "photo_url": photo_url, "url": link, "page": pg,
                    }
                    raw_json = json.dumps(raw, ensure_ascii=False)
                    ext_id = f"duma_dep_pw_{hashlib.sha256(name.encode()).hexdigest()[:16]}"

                    body = f"Фракция: {faction}\nРегион: {region}\nКомитет: {committee}"
                    _store_raw_and_content(
                        conn, source_id, ext_id, raw_json, "deputy_profile",
                        f"Депутат: {name}", body, "", link or "https://duma.gov.ru/duma/deputies/",
                    )

                    from collectors.deputy_importer import _get_or_create_entity, _import_deputy
                    entity_id = _get_or_create_entity(conn, "person", name, {"faction": faction, "region": region})
                    existing = conn.execute("SELECT id FROM deputy_profiles WHERE entity_id=?", (entity_id,)).fetchone()
                    if not existing:
                        conn.execute(
                            """INSERT INTO deputy_profiles(entity_id, full_name, position, faction, region, committee, biography_url, photo_url, is_active)
                               VALUES(?,?,?,?,?,?,?,?,1)""",
                            (entity_id, name, "депутат ГД", faction, region, committee, link, photo_url),
                        )
                    collected += 1

                next_btn = page.query_selector(".pagination .next a, a.next, li.next a")
                if not next_btn:
                    break
            except Exception as e:
                log.warning("Duma deputies page %d failed: %s", pg, e)
                continue

    except Exception as e:
        log.error("Duma deputies Playwright scraper failed: %s", e)
    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass
        _cleanup(pw, browser)
        conn.commit()
        log.info("Duma deputies Playwright: collected %d profiles", collected)
        conn.close()
    return collected


# ============================================================
# EGRUL — egrul.nalog.ru (Playwright fallback)
# ============================================================

def egrul_playwright_search(query: str, proxy: Optional[Dict] = None,
                            timeout_ms: int = 20000) -> Optional[Dict]:
    pw, browser, context = None, None, None
    try:
        pw, browser, context = _launch_browser(proxy)
        page = context.new_page()
        page.goto("https://egrul.nalog.ru/", timeout=timeout_ms, wait_until="networkidle")
        page.wait_for_timeout(2000)

        search_input = page.query_selector("input[name='query'], input[type='text'], #query, input[placeholder*='ИНН'], input[placeholder*='ОГРН']")
        if search_input:
            search_input.fill(query)
            search_btn = page.query_selector("button[type='submit'], .btn-search, .search-btn, button:has-text('Найти')")
            if search_btn:
                search_btn.click()
            else:
                page.keyboard.press("Enter")

            page.wait_for_timeout(5000)
            page.wait_for_load_state("networkidle", timeout=timeout_ms)

            body_text = page.inner_text("body")
            results = []

            rows = page.query_selector_all("table tbody tr, .result-row, .search-result, .org-item")
            for row in rows:
                cols = [c.inner_text().strip() for c in row.query_selector_all("td, .cell")]
                if cols:
                    results.append({"cols": cols, "text": row.inner_text().strip()})

            if not results and len(body_text) > 100:
                return {"raw_html_text": body_text[:10000], "query": query, "found": len(body_text) > 500}

            return {"results": results, "query": query, "found": len(results) > 0}

    except Exception as e:
        log.warning("EGRUL Playwright search failed for %s: %s", query[:30], e)
    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass
        _cleanup(pw, browser)
    return None


# ============================================================
# SUDRF — sudrf.ru (JS-rendered court search)
# ============================================================

def sudrf_playwright_search(participant: str = None, case_number: str = None,
                            proxy: Optional[Dict] = None, timeout_ms: int = 20000) -> List[Dict]:
    pw, browser, context = None, None, None
    results = []
    try:
        pw, browser, context = _launch_browser(proxy)
        page = context.new_page()
        page.goto("https://sudrf.ru/", timeout=timeout_ms, wait_until="networkidle")
        page.wait_for_timeout(2000)

        search_link = page.query_selector("a[href*='search'], a[href*='case'], .search-link")
        if search_link:
            search_link.click()
            page.wait_for_timeout(2000)

        if participant:
            part_input = page.query_selector("input[name*='participant'], input[placeholder*='Участник'], #participant")
            if part_input:
                part_input.fill(participant)
        if case_number:
            case_input = page.query_selector("input[name*='case_number'], input[placeholder*='омер'], #case_number")
            if case_input:
                case_input.fill(case_number)

        search_btn = page.query_selector("button[type='submit'], .btn-search, button:has-text('Найти')")
        if search_btn:
            search_btn.click()
        else:
            page.keyboard.press("Enter")

        page.wait_for_timeout(5000)
        page.wait_for_load_state("networkidle", timeout=timeout_ms)

        rows = page.query_selector_all("table tbody tr, .case-row, .search-result")
        for row in rows:
            cols = [c.inner_text().strip() for c in row.query_selector_all("td, .cell")]
            if len(cols) >= 2:
                results.append({
                    "case_number": cols[0],
                    "category": cols[1] if len(cols) > 1 else "",
                    "court": cols[2] if len(cols) > 2 else "",
                    "status": cols[3] if len(cols) > 3 else "",
                    "date": cols[4] if len(cols) > 4 else "",
                })

    except Exception as e:
        log.warning("Sudrf Playwright search failed: %s", e)
    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass
        _cleanup(pw, browser)
    return results


def sudrf_playwright_collect(settings: dict = None, names: List[str] = None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "sudrf.ru")
    if not source_id:
        log.warning("No Sudrf source in DB")
        conn.close()
        return 0

    if names is None:
        rows = conn.execute(
            "SELECT DISTINCT canonical_name FROM entities WHERE entity_type='person' AND length(canonical_name) > 5 LIMIT 30"
        ).fetchall()
        names = [r[0] for r in rows]

    proxy = _get_proxy_settings(settings)
    log.info("Sudrf Playwright: searching %d names", len(names))
    collected = 0

    for name in names[:20]:
        results = sudrf_playwright_search(participant=name, proxy=proxy)
        for item in results:
            raw = {"participant": name, **item}
            raw_json = json.dumps(raw, ensure_ascii=False)
            ext_id = f"sudrf_pw_{hashlib.sha256((item.get('case_number','')+name).encode()).hexdigest()[:16]}"

            _store_raw_and_content(
                conn, source_id, ext_id, raw_json, "court_record",
                f"Суд. дело: {item.get('case_number', '')}",
                f"Номер: {item.get('case_number', '')}\nКатегория: {item.get('category', '')}\nСуд: {item.get('court', '')}\nСтатус: {item.get('status', '')}",
                item.get("date", ""), "https://sudrf.ru/",
            )
            collected += 1
        time.sleep(3)

    conn.commit()
    log.info("Sudrf Playwright: collected %d records", collected)
    conn.close()
    return collected


# ============================================================
# KAD.ARBITR — kad.arbitr.ru (JS-rendered)
# ============================================================

def kad_arbitr_playwright_search(query: str, proxy: Optional[Dict] = None,
                                  timeout_ms: int = 20000) -> List[Dict]:
    pw, browser, context = None, None, None
    results = []
    try:
        pw, browser, context = _launch_browser(proxy)
        page = context.new_page()
        page.goto("https://kad.arbitr.ru/", timeout=timeout_ms, wait_until="networkidle")
        page.wait_for_timeout(2000)

        search_input = page.query_selector("input[type='text'], input[name*='search'], #SimpleSearch, .search-input")
        if search_input:
            search_input.fill(query)
            search_btn = page.query_selector("button[type='submit'], .btn-search, button:has-text('Найти')")
            if search_btn:
                search_btn.click()
            else:
                page.keyboard.press("Enter")

            page.wait_for_timeout(5000)
            page.wait_for_load_state("networkidle", timeout=timeout_ms)

            rows = page.query_selector_all("table tbody tr, .case-row, .search-result-row, .row-case")
            for row in rows:
                cols = [c.inner_text().strip() for c in row.query_selector_all("td, .cell")]
                link_el = row.query_selector("a[href*='case'], a")
                link = link_el.get_attribute("href") if link_el else ""
                if cols:
                    results.append({
                        "case_number": cols[0],
                        "category": cols[1] if len(cols) > 1 else "",
                        "court": cols[2] if len(cols) > 2 else "",
                        "date": cols[3] if len(cols) > 3 else "",
                        "link": link,
                    })

    except Exception as e:
        log.warning("Kad.Arbitr Playwright search failed: %s", e)
    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass
        _cleanup(pw, browser)
    return results


def kad_arbitr_playwright_collect(settings: dict = None, names: List[str] = None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "kad.arbitr.ru")
    if not source_id:
        log.warning("No Kad.Arbitr source in DB")
        conn.close()
        return 0

    if names is None:
        rows = conn.execute(
            "SELECT DISTINCT canonical_name FROM entities WHERE entity_type='person' AND length(canonical_name) > 5 LIMIT 30"
        ).fetchall()
        names = [r[0] for r in rows]

    proxy = _get_proxy_settings(settings)
    log.info("Kad.Arbitr Playwright: searching %d names", len(names))
    collected = 0

    for name in names[:20]:
        results = kad_arbitr_playwright_search(name, proxy=proxy)
        for item in results:
            raw = {"query": name, **item}
            raw_json = json.dumps(raw, ensure_ascii=False)
            ext_id = f"kad_pw_{hashlib.sha256((item.get('case_number','')+name).encode()).hexdigest()[:16]}"

            _store_raw_and_content(
                conn, source_id, ext_id, raw_json, "court_record",
                f"Арбитраж: {item.get('case_number', '')}",
                f"Номер: {item.get('case_number', '')}\nКатегория: {item.get('category', '')}\nСуд: {item.get('court', '')}",
                item.get("date", ""), "https://kad.arbitr.ru/",
            )
            collected += 1
        time.sleep(3)

    conn.commit()
    log.info("Kad.Arbitr Playwright: collected %d records", collected)
    conn.close()
    return collected


# ============================================================
# MASTER
# ============================================================

def collect_all_playwright(settings: dict = None):
    if settings is None:
        settings = load_settings()

    results = {}

    for name, func in [
        ("fssp", lambda: fssp_playwright_collect(settings)),
        ("minjust", lambda: minjust_playwright_collect(settings, pages=3)),
        ("duma_bills", lambda: duma_playwright_bills_collect(settings, pages=3)),
        ("duma_deputies", lambda: duma_playwright_deputies_collect(settings, pages=3)),
        ("sudrf", lambda: sudrf_playwright_collect(settings)),
        ("kad_arbitr", lambda: kad_arbitr_playwright_collect(settings)),
    ]:
        try:
            r = func()
            results[name] = r or 0
        except Exception as e:
            log.error("Playwright %s failed: %s", name, e)
            results[name] = -1

    log.info("Playwright collection complete: %s", results)
    return results


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import argparse
    parser = argparse.ArgumentParser(description="Playwright-based scraper for JS-heavy government sites")
    parser.add_argument("--source", choices=[
        "fssp", "minjust", "duma_bills", "duma_deputies", "egrul",
        "sudrf", "kad", "all"
    ], default="all")
    parser.add_argument("--pages", type=int, default=3)
    args = parser.parse_args()

    settings = load_settings()

    if args.source == "all":
        collect_all_playwright(settings)
    elif args.source == "fssp":
        fssp_playwright_collect(settings)
    elif args.source == "minjust":
        minjust_playwright_collect(settings, pages=args.pages)
    elif args.source == "duma_bills":
        duma_playwright_bills_collect(settings, pages=args.pages)
    elif args.source == "duma_deputies":
        duma_playwright_deputies_collect(settings, pages=args.pages)
    elif args.source == "egrul":
        egrul_playwright_search("test", proxy=_get_proxy_settings(settings))
    elif args.source == "sudrf":
        sudrf_playwright_collect(settings)
    elif args.source == "kad":
        kad_arbitr_playwright_collect(settings)


if __name__ == "__main__":
    main()
