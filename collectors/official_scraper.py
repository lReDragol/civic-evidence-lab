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
from urllib.parse import parse_qs, urljoin, urlparse

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.5",
}

MINJUST_IA_PAGE = "https://minjust.gov.ru/ru/pages/reestr-inostryannykh-agentov/"
MINJUST_REESTRS_BASE = "https://reestrs.minjust.gov.ru"
MINJUST_REGISTRY_ID_RE = re.compile(r"let id = '([^']+)'")


def _get_proxies(settings: dict = None) -> Optional[Dict]:
    return None


def _get_source_id(conn: sqlite3.Connection, url_contains: str) -> Optional[int]:
    row = conn.execute(
        "SELECT id FROM sources WHERE url LIKE ? AND is_active=1 LIMIT 1",
        (f"%{url_contains}%",),
    ).fetchone()
    return row[0] if row else None


def _get_or_create_source(
    conn: sqlite3.Connection,
    *,
    name: str,
    category: str,
    subcategory: str,
    url: str,
    access_method: str,
    is_official: int = 1,
    credibility_tier: str = "A",
    update_frequency: str = "daily",
    notes: str = "",
) -> int:
    source_id = _get_source_id(conn, url)
    if source_id:
        return source_id

    row = conn.execute(
        "SELECT id FROM sources WHERE url=? AND category=? LIMIT 1",
        (url, category),
    ).fetchone()
    if row:
        return row[0]

    conn.execute(
        """
        INSERT OR IGNORE INTO sources(
            name, category, subcategory, url, access_method, is_official,
            credibility_tier, update_frequency, notes, is_active
        )
        VALUES(?,?,?,?,?,?,?,?,?,1)
        """,
        (
            name,
            category,
            subcategory,
            url,
            access_method,
            is_official,
            credibility_tier,
            update_frequency,
            notes,
        ),
    )
    row = conn.execute(
        "SELECT id FROM sources WHERE url=? AND category=? LIMIT 1",
        (url, category),
    ).fetchone()
    if not row:
        raise RuntimeError(f"Failed to create source: {name} ({url})")
    return row[0]


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

    cur = conn.execute(
        """INSERT INTO content_items(source_id, raw_item_id, external_id, content_type, title, body_text, published_at, collected_at, url, status)
           VALUES(?,?,?,?,?,?,?,?,?,'raw_signal')""",
        (source_id, raw_id, ext_id, content_type, title, body[:50000], published, datetime.now().isoformat(), url),
    )
    content_id = cur.lastrowid
    conn.execute(
        """
        INSERT INTO content_search(rowid, title, body_text)
        VALUES(?,?,?)
        """,
        (content_id, title or "", body[:50000]),
    )
    return raw_id


def _requests_session(verify: bool = True):
    import requests

    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = verify
    if not verify:
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
    return session


def _request_timeout(timeout: int):
    return (min(4, timeout), timeout)


def _page_title(soup) -> str:
    if soup.title and soup.title.get_text(strip=True):
        return soup.title.get_text(" ", strip=True)
    h1 = soup.find("h1")
    return h1.get_text(" ", strip=True) if h1 else ""


def _clean_page_text(soup, limit: int = 20000) -> str:
    for tag in soup.select("script, style, noscript, svg"):
        tag.decompose()
    text = "\n".join(part.strip() for part in soup.get_text("\n").splitlines() if part.strip())
    return text[:limit]


def _extract_links(soup, base_url: str, keywords: Optional[List[str]] = None, limit: int = 60) -> List[Dict[str, str]]:
    keyword_lowers = [kw.lower() for kw in (keywords or [])]
    links: List[Dict[str, str]] = []
    seen = set()
    for anchor in soup.find_all("a"):
        href = (anchor.get("href") or "").strip()
        text = " ".join(anchor.get_text(" ", strip=True).split())
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        full_url = urljoin(base_url, href)
        if not full_url.startswith(("http://", "https://")):
            continue
        key = full_url.split("#", 1)[0]
        if key in seen:
            continue
        haystack = f"{text} {full_url}".lower()
        if keyword_lowers and not any(kw in haystack for kw in keyword_lowers):
            continue
        seen.add(key)
        links.append({"text": text[:300], "url": full_url})
        if len(links) >= limit:
            break
    return links


def _store_page_snapshot(
    conn: sqlite3.Connection,
    source_id: int,
    *,
    ext_id: str,
    content_type: str,
    title: str,
    text: str,
    url: str,
    links: Optional[List[Dict[str, str]]] = None,
    published: str = "",
    extra: Optional[Dict] = None,
) -> None:
    body_parts = [text]
    if links:
        body_parts.append("Ссылки страницы:")
        body_parts.extend(f"- {item.get('text') or item.get('url')}: {item.get('url')}" for item in links[:80])
    body = "\n".join(part for part in body_parts if part)
    raw = {
        "url": url,
        "title": title,
        "links": links or [],
        **(extra or {}),
    }
    _store_raw_and_content(
        conn,
        source_id,
        ext_id,
        json.dumps(raw, ensure_ascii=False),
        content_type,
        title,
        body,
        published,
        url,
    )


def _collect_index_page(
    conn: sqlite3.Connection,
    source_id: int,
    *,
    url: str,
    ext_prefix: str,
    content_type: str = "article",
    timeout: int = 12,
    keywords: Optional[List[str]] = None,
    fetch_details: bool = False,
    detail_limit: int = 20,
    verify: bool = True,
) -> int:
    from bs4 import BeautifulSoup

    session = _requests_session(verify=verify)
    resp = session.get(url, timeout=_request_timeout(timeout))
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    title = _page_title(soup) or url
    links = _extract_links(soup, resp.url, keywords=keywords, limit=80)
    _store_page_snapshot(
        conn,
        source_id,
        ext_id=f"{ext_prefix}_index_{hashlib.sha256(resp.url.encode('utf-8')).hexdigest()[:12]}",
        content_type=content_type,
        title=title,
        text=_clean_page_text(soup),
        url=resp.url,
        links=links,
        extra={"status_code": resp.status_code},
    )
    collected = 1

    if fetch_details:
        for link in links[:detail_limit]:
            try:
                detail_resp = session.get(link["url"], timeout=_request_timeout(timeout))
                if detail_resp.status_code != 200 or "text/html" not in detail_resp.headers.get("content-type", ""):
                    continue
                detail_soup = BeautifulSoup(detail_resp.text, "lxml")
                detail_title = _page_title(detail_soup) or link["text"] or link["url"]
                detail_text = _clean_page_text(detail_soup)
                if len(detail_text) < 200:
                    continue
                _store_page_snapshot(
                    conn,
                    source_id,
                    ext_id=f"{ext_prefix}_{hashlib.sha256(link['url'].encode('utf-8')).hexdigest()[:16]}",
                    content_type=content_type,
                    title=detail_title,
                    text=detail_text,
                    url=detail_resp.url,
                    links=[],
                    extra={"index_url": url, "link_text": link["text"]},
                )
                collected += 1
                time.sleep(0.4)
            except Exception as e:
                log.warning("%s detail failed for %s: %s", ext_prefix, link["url"], e)
                continue
    return collected


# ============================================================
# EGRUL — egrul.nalog.ru
# ============================================================

def egrul_search_by_inn(inn: str, timeout: int = 15) -> Optional[Dict]:
    try:
        import requests
        resp = requests.post(
            "https://egrul.nalog.ru/api/v1/search",
            json={"query": inn, "region": ""},
            headers={**HEADERS, "Content-Type": "application/json"},
            timeout=timeout,
        )
        if resp.status_code == 200:
            data = resp.json()
            token = data.get("token") or data.get("id")
            if token:
                time.sleep(1)
                result_resp = requests.get(
                    f"https://egrul.nalog.ru/api/v1/result/{token}",
                    headers=HEADERS, timeout=timeout,
                )
                if result_resp.status_code == 200:
                    return result_resp.json()
            return data
    except Exception as e:
        log.warning("EGRUL request failed for INN %s: %s", inn, e)
    return None


def egrul_search_by_name(query: str, page: int = 1, per_page: int = 20) -> Optional[Dict]:
    try:
        import requests
        resp = requests.post(
            "https://egrul.nalog.ru/api/v1/search",
            json={"query": query, "region": "", "page": page, "perPage": per_page},
            headers={**HEADERS, "Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            token = data.get("token") or data.get("id")
            if token:
                time.sleep(1)
                result_resp = requests.get(
                    f"https://egrul.nalog.ru/api/v1/result/{token}",
                    headers=HEADERS, timeout=15,
                )
                if result_resp.status_code == 200:
                    return result_resp.json()
            return data
    except Exception as e:
        log.warning("EGRUL search failed for %s: %s", query[:30], e)
    return None


def egrul_collect_by_inn_list(settings: dict = None, inn_list: List[str] = None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "egrul.nalog.ru")
    if not source_id:
        log.warning("No EGRUL source in DB")
        conn.close()
        return

    if inn_list is None:
        inn_rows = conn.execute(
            "SELECT DISTINCT canonical_name FROM entities WHERE entity_type='inn' LIMIT 200"
        ).fetchall()
        inn_list = [r[0] for r in inn_rows if r[0] and len(r[0]) in (10, 12)]

    if not inn_list:
        inn_rows2 = conn.execute(
            "SELECT DISTINCT inn FROM entities WHERE inn IS NOT NULL AND length(inn) IN (10,12) LIMIT 200"
        ).fetchall()
        inn_list = [r[0] for r in inn_rows2]

    if not inn_list:
        log.info("No INNs found in DB to query EGRUL")
        conn.close()
        return

    log.info("EGRUL: querying %d INNs", len(inn_list))
    collected = 0
    for inn in inn_list:
        data = egrul_search_by_inn(inn)
        if not data:
            continue

        raw_json = json.dumps(data, ensure_ascii=False, default=str)
        name = data.get("name", data.get("fullName", ""))
        address = data.get("address", "")
        status = data.get("status", "")
        director = ""
        founders = []
        if data.get("director"):
            director = data["director"].get("fio", str(data["director"]))
        if data.get("founders"):
            founders = [f.get("fio", f.get("name", "")) for f in data["founders"]]

        body_parts = [f"Статус: {status}", f"Адрес: {address}"]
        if director:
            body_parts.append(f"Руководитель: {director}")
        if founders:
            body_parts.append(f"Учредители: {', '.join(founders[:10])}")
        body = "\n".join(body_parts)

        ext_id = f"egrul_{inn}"
        _store_raw_and_content(conn, source_id, ext_id, raw_json, "registry_record",
                               name or f"Организация ИНН {inn}", body, "", f"https://egrul.nalog.ru/entity/{inn}")

        entity_row = conn.execute("SELECT id FROM entities WHERE inn=?", (inn,)).fetchone()
        if entity_row:
            eid = entity_row[0]
            conn.execute("UPDATE entities SET extra_data=?, ogrn=?, description=? WHERE id=?",
                         (raw_json[:30000], data.get("ogrn"), f"{status}; {address}", eid))
        collected += 1

        if collected % 20 == 0:
            conn.commit()
        time.sleep(0.3)

    conn.commit()
    log.info("EGRUL: collected %d records", collected)
    conn.close()
    return collected


# ============================================================
# KAD.ARBIRR — kad.arbitr.ru
# ============================================================

def kad_arbitr_search(case_number: str = None, participant_name: str = None,
                      page: int = 1, per_page: int = 25) -> Optional[Dict]:
    try:
        import requests
        if case_number:
            params = {"SimpleSearch": case_number, "Page": page}
        elif participant_name:
            params = {"SimpleSearch": participant_name, "Page": page}
        else:
            return None

        headers = {**HEADERS, "Referer": "https://kad.arbitr.ru/"}
        resp = requests.post(
            "https://kad.arbitr.ru/Kad/Search",
            data=params, headers=headers, timeout=20,
        )
        if resp.status_code == 200:
            try:
                return resp.json()
            except json.JSONDecodeError:
                return {"html": resp.text, "found": len(resp.text) > 500}
    except Exception as e:
        log.warning("Kad.Arbitr search failed: %s", e)
    return None


def kad_arbitr_collect(settings: dict = None, names: List[str] = None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "kad.arbitr.ru")
    if not source_id:
        log.warning("No Kad.Arbitr source in DB")
        conn.close()
        return

    if names is None:
        name_rows = conn.execute(
            "SELECT DISTINCT canonical_name FROM entities WHERE entity_type='person' AND length(canonical_name) > 5 LIMIT 100"
        ).fetchall()
        names = [r[0] for r in name_rows]

    if not names:
        log.info("No names to search in Kad.Arbitr")
        conn.close()
        return

    log.info("Kad.Arbitr: searching %d names", len(names))
    collected = 0
    for name in names[:50]:
        result = kad_arbitr_search(participant_name=name)
        if not result:
            continue

        raw_json = json.dumps(result, ensure_ascii=False, default=str)
        found = result.get("found", False)
        cases = result.get("data", {}).get("result", []) if isinstance(result.get("data"), dict) else []

        body_parts = [f"Поиск по: {name}", f"Найдено: {'да' if found else 'нет'}"]
        if cases:
            for c in cases[:10]:
                case_num = c.get("caseNumber", c.get("number", ""))
                category = c.get("category", "")
                court = c.get("court", "")
                body_parts.append(f"Дело {case_num} — {category} ({court})")
        body = "\n".join(body_parts)

        ext_id = f"kad_{hashlib.sha256(name.encode()).hexdigest()[:16]}"
        _store_raw_and_content(conn, source_id, ext_id, raw_json, "court_record",
                               f"Арбитражные дела: {name}", body, "", "https://kad.arbitr.ru/")
        collected += 1
        time.sleep(1)

    conn.commit()
    log.info("Kad.Arbitr: collected %d search results", collected)
    conn.close()
    return collected


# ============================================================
# FSSP — fssp.gov.ru
# ============================================================

def fssp_search(lastname: str, firstname: str, secondname: str = "", region_id: int = -1) -> Optional[Dict]:
    try:
        import requests
        data = {
            "is[extended]": "0",
            "is[region_id][0]": str(region_id),
            "is[lastname]": lastname,
            "is[firstname]": firstname,
            "is[secondname]": secondname or "",
        }
        headers = {**HEADERS, "X-Requested-With": "XMLHttpRequest"}
        resp = requests.post(
            "https://is.fssp.gov.ru/is/ajax_search",
            data=data, headers=headers, timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        log.warning("FSSP search failed for %s: %s", lastname, e)
    return None


def fssp_collect(settings: dict = None, names: List[Dict] = None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "fssp.gov.ru")
    if not source_id:
        log.warning("No FSSP source in DB")
        conn.close()
        return

    if names is None:
        rows = conn.execute(
            """SELECT canonical_name FROM entities
               WHERE entity_type='person' AND canonical_name LIKE '% %' LIMIT 100"""
        ).fetchall()
        names = []
        for r in rows:
            parts = r[0].split()
            if len(parts) >= 2:
                names.append({"last": parts[0], "first": parts[1], "middle": parts[2] if len(parts) > 2 else ""})

    if not names:
        log.info("No person names for FSSP search")
        conn.close()
        return

    log.info("FSSP: searching %d names", len(names))
    collected = 0
    for n in names[:50]:
        result = fssp_search(n["last"], n["first"], n.get("middle", ""))
        if not result:
            continue

        raw_json = json.dumps(result, ensure_ascii=False, default=str)
        items = result.get("data", {}).get("result", []) if isinstance(result.get("data"), dict) else []
        body_parts = [f"ФИО: {n['last']} {n['first']} {n.get('middle', '')}"]
        body_parts.append(f"Производств: {len(items)}")
        for item in items[:10]:
            detail = item.get("detail", item.get("name", ""))
            amount = item.get("amount", "")
            body_parts.append(f"  {detail} — {amount}")
        body = "\n".join(body_parts)

        ext_id = f"fssp_{hashlib.sha256((n['last']+n['first']).encode()).hexdigest()[:16]}"
        _store_raw_and_content(conn, source_id, ext_id, raw_json, "enforcement",
                               f"Исполнительные производства: {n['last']} {n['first']}", body, "",
                               "https://is.fssp.gov.ru/")
        collected += 1
        time.sleep(1.5)

    conn.commit()
    log.info("FSSP: collected %d results", collected)
    conn.close()
    return collected


# ============================================================
# MINJUST — реестр иноагентов
# ============================================================

def minjust_inoagents_collect(settings: dict = None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "minjust.gov.ru/ru/pages/reestr-inostryannykh-agentov/")
    if not source_id:
        source_id = _get_source_id(conn, "minjust.gov.ru")
    if not source_id:
        log.warning("No Minjust source in DB")
        conn.close()
        return

    collected = 0
    try:
        import requests

        session = _requests_session()
        page_resp = session.get(MINJUST_IA_PAGE, timeout=30)
        if page_resp.status_code != 200:
            raise RuntimeError(f"Minjust page returned {page_resp.status_code}")

        match = MINJUST_REGISTRY_ID_RE.search(page_resp.text)
        registry_id = match.group(1) if match else "39b95df9-9a68-6b6d-e1e3-e6388507067e"

        api_session = _requests_session(verify=False)
        info_resp = api_session.get(f"{MINJUST_REESTRS_BASE}/rest/registry/{registry_id}/info", timeout=30)
        info_resp.raise_for_status()
        info = info_resp.json()
        columns = {
            col.get("name"): col.get("title")
            for col in info.get("columns", [])
            if col.get("name")
        }

        offset = 0
        limit = 300
        total = None
        while total is None or offset < total:
            values_resp = api_session.post(
                f"{MINJUST_REESTRS_BASE}/rest/registry/{registry_id}/values",
                json={"offset": offset, "limit": limit},
                headers={"Content-Type": "application/json;charset=utf-8"},
                timeout=30,
            )
            values_resp.raise_for_status()
            payload = values_resp.json()
            total = int(payload.get("size") or 0)
            rows = payload.get("values") or []
            if not rows:
                break

            for item in rows:
                name = item.get("field_2_s", "").strip()
                if not name:
                    continue

                included_at = item.get("field_4_dt") or item.get("field_4_s") or ""
                excluded_at = item.get("field_5_dt") or item.get("field_5_s") or ""
                reasons = item.get("field_3_s", "").strip()
                agent_type = item.get("field_7_s", "").strip()
                reg_number = item.get("field_8_s", "").strip()
                inn = item.get("field_9_s", "").strip()
                ogrn = item.get("field_10_s", "").strip()
                domain = item.get("field_6_s", "").strip()

                body_parts = [
                    f"Тип: {agent_type}" if agent_type else "",
                    f"Дата включения: {included_at}" if included_at else "",
                    f"Дата исключения: {excluded_at}" if excluded_at else "",
                    f"Основания: {reasons}" if reasons else "",
                    f"Регистрационный номер: {reg_number}" if reg_number else "",
                    f"ИНН: {inn}" if inn else "",
                    f"ОГРН: {ogrn}" if ogrn else "",
                    f"Домен: {domain}" if domain else "",
                ]

                extra_fields = []
                for key, value in item.items():
                    if not key.startswith("field_"):
                        continue
                    if key in {"field_2_s", "field_3_s", "field_4_s", "field_4_dt", "field_5_s", "field_5_dt", "field_6_s", "field_7_s", "field_8_s", "field_9_s", "field_10_s"}:
                        continue
                    if value in ("", None, []):
                        continue
                    label = columns.get(key, key)
                    extra_fields.append(f"{label}: {value}")

                body = "\n".join(part for part in body_parts + extra_fields if part)
                raw = {
                    "registry_id": registry_id,
                    "source_page": MINJUST_IA_PAGE,
                    "registry_api_base": MINJUST_REESTRS_BASE,
                    "columns": columns,
                    "item": item,
                }
                raw_json = json.dumps(raw, ensure_ascii=False)
                ext_key = item.get("id") or reg_number or name
                ext_id = f"minoagent_{hashlib.sha256(str(ext_key).encode('utf-8')).hexdigest()[:16]}"

                _store_raw_and_content(
                    conn,
                    source_id,
                    ext_id,
                    raw_json,
                    "registry_record",
                    f"Иноагент: {name}",
                    body,
                    included_at,
                    MINJUST_IA_PAGE,
                )
                collected += 1

            conn.commit()
            offset += len(rows)

        log.info("Minjust: collected %d inoagent records from registry API", collected)
    except Exception as e:
        log.error("Minjust scraper failed: %s", e)
    finally:
        conn.close()
    return collected


# ============================================================
# RKN — единый реестр запрещённой информации
# ============================================================

def rkn_blocked_collect(settings: dict = None, pages: int = 5):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "eais.rkn.gov.ru")
    if not source_id:
        log.warning("No RKN source in DB")
        conn.close()
        return

    try:
        import requests
        from bs4 import BeautifulSoup

        collected = 0
        for page in range(1, pages + 1):
            try:
                url = f"https://eais.rkn.gov.ru/{'page/' + str(page) if page > 1 else ''}"
                resp = requests.get(url, headers=HEADERS, timeout=30)
                if resp.status_code != 200:
                    continue

                soup = BeautifulSoup(resp.text, "lxml")
                records = soup.select("table.table tbody tr, .registry-item, .reestr-item")
                if not records:
                    records = soup.select("tr")

                for rec in records:
                    cells = rec.find_all("td")
                    if len(cells) < 2:
                        continue
                    url_val = cells[0].get_text(strip=True)
                    date_val = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    reason = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                    org = cells[3].get_text(strip=True) if len(cells) > 3 else ""

                    raw = {"url": url_val, "date": date_val, "reason": reason, "org": org, "page": page}
                    raw_json = json.dumps(raw, ensure_ascii=False)
                    ext_id = f"rkn_{hashlib.sha256((url_val+date_val).encode()).hexdigest()[:16]}"

                    _store_raw_and_content(
                        conn, source_id, ext_id, raw_json, "registry_record",
                        f"Заблокировано: {url_val[:80]}",
                        f"URL: {url_val}\nДата: {date_val}\nОснование: {reason}\nОрган: {org}",
                        date_val, "https://eais.rkn.gov.ru/",
                    )
                    collected += 1
            except Exception as e:
                log.warning("RKN page %d failed: %s", page, e)
                continue

        conn.commit()
        log.info("RKN: collected %d blocked records", collected)
    except Exception as e:
        log.error("RKN scraper failed: %s", e)
    finally:
        conn.close()
    return collected


# ============================================================
# ZAKUPKI.GOVRU — госзакупки
# ============================================================

def zakupki_search(query: str = None, registry_type: str = "contracts",
                  page: int = 1, per_page: int = 50) -> Optional[Dict]:
    try:
        import requests
        base = "https://zakupki.gov.ru/api"
        if registry_type == "contracts":
            url = f"{base}/epz/contract/extendedSearch"
            params = {"searchString": query or "", "pageNumber": page, "pageSize": per_page}
        else:
            url = f"{base}/epz/order/extendedSearch"
            params = {"searchString": query or "", "page": page, "limit": per_page}

        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        log.warning("Zakupki search failed: %s", e)
    return None


def zakupki_html_scrape(query: str = None, page: int = 1) -> List[Dict]:
    try:
        import requests
        from bs4 import BeautifulSoup

        url = "https://zakupki.gov.ru/epz/contract/search/results.html"
        params = {"searchString": query or "", "pageNumber": str(page)}
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        results = []
        items = soup.select(".search-registry-entry-block, .card-wrapper, .row.record")
        if not items:
            items = soup.select("div[data-object-id]")

        for item in items:
            number_el = item.select_one(".registry-entry__header-mid__number a, .link-wrap a")
            price_el = item.select_one(".price-block__value, .cost")
            customer_el = item.select_one(".registry-entry__body-href a, .link-wrap.bold")

            number = number_el.get_text(strip=True) if number_el else ""
            price = price_el.get_text(strip=True) if price_el else ""
            customer = customer_el.get_text(strip=True) if customer_el else ""
            link = number_el.get("href", "") if number_el else ""
            full_link = f"https://zakupki.gov.ru{link}" if link.startswith("/") else link
            parsed = parse_qs(urlparse(full_link).query) if full_link else {}
            registry_number = (parsed.get("reestrNumber") or [""])[0]

            if number or price:
                results.append({
                    "contract_number": number,
                    "registry_number": registry_number,
                    "price": price,
                    "customer": customer,
                    "link": full_link,
                })
        return results
    except Exception as e:
        log.warning("Zakupki HTML scrape failed: %s", e)
        return []


def zakupki_collect(settings: dict = None, queries: List[str] = None, pages: int = 3):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "zakupki.gov.ru")
    if not source_id:
        log.warning("No Zakupki source in DB")
        conn.close()
        return

    if queries is None:
        queries = [None]

    collected = 0
    for query in queries:
        for page in range(1, pages + 1):
            results = zakupki_html_scrape(query=query, page=page)
            if not results:
                break

            for item in results:
                raw = {"query": query, "page": page, **item}
                raw_json = json.dumps(raw, ensure_ascii=False)
                registry_number = item.get("registry_number", "")
                ext_base = registry_number or item.get("contract_number", "") or item.get("link", "")
                ext_id = f"zakupki_{hashlib.sha256(ext_base.encode('utf-8')).hexdigest()[:16]}"

                title_number = registry_number or item.get("contract_number", "—")
                title = f"Контракт: {title_number}"
                body_parts = [
                    f"Реестровый номер: {registry_number}" if registry_number else "",
                    f"Номер: {item.get('contract_number', '')}" if item.get("contract_number") else "",
                    f"Цена: {item.get('price', '')}" if item.get("price") else "",
                    f"Заказчик: {item.get('customer', '')}" if item.get("customer") else "",
                    f"Поисковый запрос: {query}" if query else "Режим: последние контракты",
                ]
                body = "\n".join(part for part in body_parts if part)

                _store_raw_and_content(
                    conn, source_id, ext_id, raw_json, "procurement",
                    title, body, "", item.get("link", "https://zakupki.gov.ru/"),
                )
                collected += 1

            time.sleep(2)

    conn.commit()
    log.info("Zakupki: collected %d records", collected)
    conn.close()
    return collected


# ============================================================
# DUMA — законопроекты + депутаты
# ============================================================

def duma_bills_collect(settings: dict = None, pages: int = 5, queries: Optional[List[str]] = None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "sozd.duma.gov.ru")
    if not source_id:
        source_id = _get_source_id(conn, "duma.gov.ru")
    if not source_id:
        log.warning("No Duma source in DB")
        conn.close()
        return

    try:
        import requests
        from bs4 import BeautifulSoup

        if queries is None:
            queries = ["жкх", "собствен", "жиль", "реестр", "ндс", "налог", "бюджет", "суд", "иностран", "штраф", "закуп"]
        collected = 0
        session = _requests_session()

        for query in queries:
            for page in range(1, pages + 1):
                try:
                    resp = session.get(
                        "https://sozd.duma.gov.ru/oz/b",
                        params={"b[Annotation]": query, "page": page},
                        timeout=30,
                    )
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.text, "lxml")
                    tables = soup.select("table.tbl_search_results")
                    result_table = None
                    for table in tables:
                        if table.select_one("a[href*='/bill/']"):
                            result_table = table
                            break
                    if result_table is None:
                        break

                    page_rows = 0
                    for row in result_table.select("tr"):
                        cells = row.find_all("td")
                        if len(cells) < 6:
                            continue
                        link_el = cells[1].select_one("a[href*='/bill/']")
                        if not link_el:
                            continue
                        number = link_el.get_text(strip=True)
                        href = link_el.get("href", "")
                        if not number:
                            continue
                        info_text = " ".join(cells[1].get_text(" ", strip=True).split())
                        status = ""
                        title_text = re.sub(rf"^{re.escape(number)}\s*", "", info_text).strip()
                        for known_status in ("На рассмотрении", "В архиве"):
                            if title_text.startswith(known_status):
                                status = known_status
                                title_text = title_text[len(known_status):].strip()
                                break
                        reg_date = cells[2].get_text(" ", strip=True)
                        sponsor = cells[3].get_text(" ", strip=True)
                        last_event = cells[4].get_text(" ", strip=True)
                        last_event_date = cells[5].get_text(" ", strip=True)
                        link = f"https://sozd.duma.gov.ru{href}" if href.startswith("/") else href

                        raw = {
                            "query": query,
                            "page": page,
                            "number": number,
                            "status": status,
                            "name": title_text,
                            "registration_date": reg_date,
                            "sponsor": sponsor,
                            "last_event": last_event,
                            "last_event_date": last_event_date,
                            "link": link,
                        }
                        raw_json = json.dumps(raw, ensure_ascii=False)
                        ext_id = f"duma_bill_{hashlib.sha256(number.encode('utf-8')).hexdigest()[:16]}"
                        body = "\n".join(
                            part for part in [
                                f"Номер: {number}",
                                f"Название: {title_text}" if title_text else "",
                                f"Статус: {status}" if status else "",
                                f"Дата регистрации: {reg_date}" if reg_date else "",
                                f"СПЗИ: {sponsor}" if sponsor else "",
                                f"Последнее событие: {last_event}" if last_event else "",
                                f"Дата последнего события: {last_event_date}" if last_event_date else "",
                                f"Поисковый запрос: {query}",
                            ]
                            if part
                        )

                        _store_raw_and_content(
                            conn,
                            source_id,
                            ext_id,
                            raw_json,
                            "bill",
                            f"Законопроект {number}: {title_text[:100]}",
                            body,
                            reg_date,
                            link or "https://sozd.duma.gov.ru/oz/b",
                        )
                        collected += 1
                        page_rows += 1

                    if page_rows == 0:
                        break
                except Exception as e:
                    log.warning("Duma bills query=%s page=%d failed: %s", query, page, e)
                    continue

        conn.commit()
        log.info("Duma bills: collected %d records", collected)
    except Exception as e:
        log.error("Duma bills scraper failed: %s", e)
    finally:
        conn.close()
    return collected


# ============================================================
# GIS GKH — дом.госуслуги.ру
# ============================================================

def gis_gkh_collect(settings: dict = None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_or_create_source(
        conn,
        name="ГИС ЖКХ",
        category="official_registry",
        subcategory="housing",
        url="dom.gosuslugi.ru",
        access_method="html",
        notes="Официальная государственная информационная система жилищно-коммунального хозяйства",
    )

    collected = 0
    try:
        keywords = [
            "гис",
            "жкх",
            "жилищ",
            "коммун",
            "собствен",
            "реестр",
            "дом",
            "помещ",
            "голос",
            "управ",
        ]
        collected += _collect_index_page(
            conn,
            source_id,
            url="https://dom.gosuslugi.ru/",
            ext_prefix="gis_gkh",
            content_type="registry_record",
            timeout=12,
            keywords=keywords,
            fetch_details=False,
        )
        conn.commit()
        log.info("GIS GKH: collected %d page snapshots", collected)
    except Exception as e:
        log.error("GIS GKH scraper failed: %s", e)
    finally:
        conn.close()
    return collected


# ============================================================
# GOVERNMENT.RU — новости и документы правительства
# ============================================================

def government_collect(settings: dict = None, pages: int = 2):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_or_create_source(
        conn,
        name="Правительство РФ",
        category="official_site",
        subcategory="government",
        url="government.ru",
        access_method="html",
        notes="Новости, решения и документы Правительства РФ (НЕДОСТУПЕН извне — нужен российский IP/Playwright)",
    )

    log.warning("Government.ru is unreachable from current network — skipping. Needs Playwright on Russian IP.")
    conn.close()
    return 0


# ============================================================
# PRAVO — официальное опубликование правовых актов
# ============================================================

def pravo_collect(settings: dict = None, pages: int = 2):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_or_create_source(
        conn,
        name="Официальное опубликование правовых актов",
        category="official_registry",
        subcategory="laws",
        url="publication.pravo.gov.ru",
        access_method="html",
        notes="Официальный портал опубликования правовых актов",
    )

    collected = 0
    try:
        keywords = ["документ", "закон", "постанов", "указ", "распоряж", "жкх", "налог", "ндс", "реестр"]
        urls = [
            "http://publication.pravo.gov.ru/",
            "http://pravo.gov.ru/",
        ]
        for base_url in urls:
            try:
                collected += _collect_index_page(
                    conn,
                    source_id,
                    url=base_url,
                    ext_prefix="pravo",
                    content_type="registry_record",
                    timeout=10,
                    keywords=keywords,
                    fetch_details=False,
                    detail_limit=max(5, pages * 6),
                    verify=False,
                )
                conn.commit()
                if collected:
                    break
            except Exception as e:
                log.warning("Pravo index failed for %s: %s", base_url, e)
                continue
        log.info("Pravo: collected %d records", collected)
    except Exception as e:
        log.error("Pravo scraper failed: %s", e)
    finally:
        conn.close()
    return collected


# ============================================================
# ROSREESTR — fallback по публичным страницам
# ============================================================

def rosreestr_collect(settings: dict = None, pages: int = 2):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_or_create_source(
        conn,
        name="Росреестр",
        category="official_registry",
        subcategory="property",
        url="rosreestr.gov.ru",
        access_method="html",
        notes="Публичные страницы и пресс-архив Росреестра; отдельные endpoints могут требовать сетевого workaround",
    )

    collected = 0
    try:
        keywords = ["реестр", "собствен", "недвиж", "кадастр", "егрн", "гис", "жкх", "жиль"]
        urls = [
            "http://rosreestr.gov.ru/press/archive/",
            "http://rosreestr.gov.ru/site/press/news/",
        ]
        for base_url in urls:
            try:
                collected += _collect_index_page(
                    conn,
                    source_id,
                    url=base_url,
                    ext_prefix="rosreestr",
                    content_type="registry_record",
                    timeout=10,
                    keywords=keywords,
                    fetch_details=False,
                    detail_limit=max(5, pages * 6),
                    verify=False,
                )
                conn.commit()
            except Exception as e:
                log.warning("Rosreestr index failed for %s: %s", base_url, e)
                continue
        log.info("Rosreestr: collected %d records", collected)
    except Exception as e:
        log.error("Rosreestr scraper failed: %s", e)
    finally:
        conn.close()
    return collected


# ============================================================
# FEDRESURS — банкротства
# ============================================================

def fedresurs_bankruptcy_collect(settings: dict = None, pages: int = 3):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "bankrot.fedresurs.ru")
    if not source_id:
        log.warning("No Fedresurs source in DB")
        conn.close()
        return 0

    log.warning("Fedresurs (bankrot.fedresurs.ru) requires authentication — skipping. Needs API key or credentials.")
    conn.close()
    return 0


# ============================================================
# KREMLIN — стенограммы
# ============================================================

def kremlin_transcripts_collect(settings: dict = None, pages: int = 3):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_or_create_source(
        conn,
        name="Стенограммы Президента",
        category="official_site",
        subcategory="president",
        url="kremlin.ru/events/president/transcripts/",
        access_method="html",
        notes="Стенограммы и публичные выступления Президента РФ (НЕДОСТУПЕН извне — нужен российский IP/Playwright)",
    )

    log.warning("Kremlin.ru is unreachable from current network — skipping. Needs Playwright on Russian IP.")
    conn.close()
    return 0


# ============================================================
# SUDRF — ГАС Правосудие
# ============================================================

def sudrf_search(court_code: str = None, case_number: str = None,
                 participant: str = None, page: int = 1) -> List[Dict]:
    try:
        import requests
        from bs4 import BeautifulSoup

        url = "https://sudrf.ru/blocks/sr_court/search"
        params = {
            "case_number": case_number or "",
            "participant_name": participant or "",
            "court_code": court_code or "",
            "page": page,
        }
        resp = requests.post(url, data=params, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        results = []
        rows = soup.select("table tbody tr, .case-row, .search-result")
        if not rows:
            rows = soup.select("tr")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            case_num = cells[0].get_text(strip=True)
            category = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            court_name = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            status_val = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            date_val = cells[4].get_text(strip=True) if len(cells) > 4 else ""

            if case_num:
                results.append({
                    "case_number": case_num,
                    "category": category,
                    "court": court_name,
                    "status": status_val,
                    "date": date_val,
                })
        return results
    except Exception as e:
        log.warning("Sudrf search failed: %s", e)
        return []


def sudrf_collect(settings: dict = None, names: List[str] = None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    source_id = _get_source_id(conn, "sudrf.ru")
    if not source_id:
        log.warning("No Sudrf source in DB")
        conn.close()
        return

    if names is None:
        rows = conn.execute(
            "SELECT DISTINCT canonical_name FROM entities WHERE entity_type='person' AND length(canonical_name) > 5 LIMIT 50"
        ).fetchall()
        names = [r[0] for r in rows]

    collected = 0
    for name in names[:30]:
        results = sudrf_search(participant=name)
        for item in results:
            raw = {"participant": name, **item}
            raw_json = json.dumps(raw, ensure_ascii=False)
            ext_id = f"sudrf_{hashlib.sha256((item.get('case_number','')+name).encode()).hexdigest()[:16]}"

            _store_raw_and_content(
                conn, source_id, ext_id, raw_json, "court_record",
                f"Суд. дело: {item.get('case_number', '')}",
                f"Номер: {item.get('case_number', '')}\nКатегория: {item.get('category', '')}\nСуд: {item.get('court', '')}\nСтатус: {item.get('status', '')}",
                item.get("date", ""), "https://sudrf.ru/",
            )
            collected += 1
        time.sleep(1.5)

    conn.commit()
    log.info("Sudrf: collected %d records", collected)
    conn.close()
    return collected


# ============================================================
# MASTER: запуск всех парсеров
# ============================================================

def collect_all_official(settings: dict = None):
    if settings is None:
        settings = load_settings()

    results = {}

    try:
        r = egrul_collect_by_inn_list(settings)
        results["egrul"] = r or 0
    except Exception as e:
        log.error("EGRUL failed: %s", e)
        results["egrul"] = -1

    try:
        r = minjust_inoagents_collect(settings)
        results["minjust"] = r or 0
    except Exception as e:
        log.error("Minjust failed: %s", e)
        results["minjust"] = -1

    try:
        r = rkn_blocked_collect(settings, pages=3)
        results["rkn"] = r or 0
    except Exception as e:
        log.error("RKN failed: %s", e)
        results["rkn"] = -1

    try:
        r = zakupki_collect(settings)
        results["zakupki"] = r or 0
    except Exception as e:
        log.error("Zakupki failed: %s", e)
        results["zakupki"] = -1

    try:
        r = duma_bills_collect(settings, pages=3)
        results["duma_bills"] = r or 0
    except Exception as e:
        log.error("Duma bills failed: %s", e)
        results["duma_bills"] = -1

    try:
        r = gis_gkh_collect(settings)
        results["gis_gkh"] = r or 0
    except Exception as e:
        log.error("GIS GKH failed: %s", e)
        results["gis_gkh"] = -1

    try:
        r = government_collect(settings, pages=2)
        results["government"] = r or 0
    except Exception as e:
        log.error("Government failed: %s", e)
        results["government"] = -1

    try:
        r = pravo_collect(settings, pages=2)
        results["pravo"] = r or 0
    except Exception as e:
        log.error("Pravo failed: %s", e)
        results["pravo"] = -1

    try:
        r = rosreestr_collect(settings, pages=2)
        results["rosreestr"] = r or 0
    except Exception as e:
        log.error("Rosreestr failed: %s", e)
        results["rosreestr"] = -1

    try:
        r = fedresurs_bankruptcy_collect(settings, pages=2)
        results["fedresurs"] = r or 0
    except Exception as e:
        log.error("Fedresurs failed: %s", e)
        results["fedresurs"] = -1

    try:
        r = kremlin_transcripts_collect(settings, pages=2)
        results["kremlin"] = r or 0
    except Exception as e:
        log.error("Kremlin failed: %s", e)
        results["kremlin"] = -1

    try:
        r = kad_arbitr_collect(settings)
        results["kad_arbitr"] = r or 0
    except Exception as e:
        log.error("Kad.Arbitr failed: %s", e)
        results["kad_arbitr"] = -1

    try:
        r = fssp_collect(settings)
        results["fssp"] = r or 0
    except Exception as e:
        log.error("FSSP failed: %s", e)
        results["fssp"] = -1

    try:
        r = sudrf_collect(settings)
        results["sudrf"] = r or 0
    except Exception as e:
        log.error("Sudrf failed: %s", e)
        results["sudrf"] = -1

    log.info("Official collection complete: %s", results)
    return results


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import argparse
    parser = argparse.ArgumentParser(description="Collect data from official Russian government sources")
    parser.add_argument("--source", choices=[
        "egrul", "kad", "fssp", "minjust", "rkn", "zakupki",
        "duma", "fedresurs", "kremlin", "sudrf", "all"
    ], default="all", help="Which source to scrape")
    parser.add_argument("--pages", type=int, default=3, help="Number of pages per source")
    args = parser.parse_args()

    settings = load_settings()

    if args.source == "all":
        collect_all_official(settings)
    elif args.source == "egrul":
        egrul_collect_by_inn_list(settings)
    elif args.source == "kad":
        kad_arbitr_collect(settings)
    elif args.source == "fssp":
        fssp_collect(settings)
    elif args.source == "minjust":
        minjust_inoagents_collect(settings)
    elif args.source == "rkn":
        rkn_blocked_collect(settings, pages=args.pages)
    elif args.source == "zakupki":
        zakupki_collect(settings)
    elif args.source == "duma":
        duma_bills_collect(settings, pages=args.pages)
    elif args.source == "fedresurs":
        fedresurs_bankruptcy_collect(settings, pages=args.pages)
    elif args.source == "kremlin":
        kremlin_transcripts_collect(settings, pages=args.pages)
    elif args.source == "sudrf":
        sudrf_collect(settings)


if __name__ == "__main__":
    main()
