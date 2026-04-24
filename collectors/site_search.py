import hashlib
import json
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
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

SITE_STATUS = {
    "egrul": "needs_vpn",
    "minjust": "needs_vpn",
    "duma": "needs_vpn",
    "zakupki": "needs_vpn",
    "gis_gkh": "needs_vpn",
    "pravo": "needs_vpn",
    "kad_arbitr": "needs_vpn",
    "fssp": "needs_vpn",
    "sudrf": "needs_vpn",
    "government": "unreachable",
    "kremlin": "unreachable",
    "rosreestr": "ssl_error",
    "fedresurs": "auth_required",
    "rkn": "unknown",
}


def _session(verify: bool = True):
    import requests
    s = requests.Session()
    s.headers.update(HEADERS)
    s.verify = verify
    if not verify:
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
    return s


def _timeout(t: int = 15):
    return (min(4, t), t)


def _store_result(conn, source_id, ext_id, raw_json, content_type, title, body, published, url):
    raw_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
    existing = conn.execute(
        "SELECT id FROM raw_source_items WHERE source_id=? AND external_id=?",
        (source_id, ext_id),
    ).fetchone()
    if existing:
        return existing[0]
    cur = conn.execute(
        "INSERT INTO raw_source_items(source_id,external_id,raw_payload,collected_at,hash_sha256,is_processed)"
        " VALUES(?,?,?,?,?,1)",
        (source_id, ext_id, raw_json, datetime.now().isoformat(), raw_hash),
    )
    raw_id = cur.lastrowid
    cur = conn.execute(
        "INSERT INTO content_items(source_id,raw_item_id,external_id,content_type,title,body_text,published_at,collected_at,url,status)"
        " VALUES(?,?,?,?,?,?,?,?,?,'evidence')",
        (source_id, raw_id, ext_id, content_type, title, body[:50000], published, datetime.now().isoformat(), url),
    )
    content_id = cur.lastrowid
    try:
        conn.execute("INSERT INTO content_search(rowid,title,body_text) VALUES(?,?,?)",
                     (content_id, title or "", body[:50000]))
    except Exception:
        pass
    return raw_id


def _get_source_id(conn, url_contains: str) -> Optional[int]:
    row = conn.execute(
        "SELECT id FROM sources WHERE url LIKE ? AND is_active=1 LIMIT 1",
        (f"%{url_contains}%",),
    ).fetchone()
    return row[0] if row else None


def _get_or_create_source(conn, *, name, category, subcategory, url, access_method,
                          is_official=1, credibility_tier="A", update_frequency="daily", notes=""):
    sid = _get_source_id(conn, url)
    if sid:
        return sid
    row = conn.execute("SELECT id FROM sources WHERE url=? AND category=? LIMIT 1", (url, category)).fetchone()
    if row:
        return row[0]
    conn.execute(
        "INSERT OR IGNORE INTO sources(name,category,subcategory,url,access_method,is_official,"
        "credibility_tier,update_frequency,notes,is_active) VALUES(?,?,?,?,?,?,?,?,?,?,1)",
        (name, category, subcategory, url, access_method, is_official, credibility_tier, update_frequency, notes),
    )
    row = conn.execute("SELECT id FROM sources WHERE url=? AND category=? LIMIT 1", (url, category)).fetchone()
    return row[0] if row else None


# ============================================================
# EGRUL — egrul.nalog.ru
# ============================================================

def search_egrul(query: str, query_type: str = "inn", timeout: int = 15) -> List[Dict]:
    import requests
    results = []
    try:
        payload = {"query": query, "region": ""}
        if query_type == "inn" and len(query) in (10, 12):
            payload["query"] = query
        resp = requests.post(
            "https://egrul.nalog.ru/api/v1/search",
            json=payload, headers={**HEADERS, "Content-Type": "application/json"},
            timeout=timeout,
        )
        if resp.status_code != 200:
            return results
        data = resp.json()
        token = data.get("token") or data.get("id")
        if not token:
            return results
        time.sleep(1)
        result_resp = requests.get(
            f"https://egrul.nalog.ru/api/v1/result/{token}",
            headers=HEADERS, timeout=timeout,
        )
        if result_resp.status_code != 200:
            return results
        result_data = result_resp.json()
        entries = result_data.get("rows", result_data.get("data", []))
        if isinstance(entries, list):
            for entry in entries[:20]:
                name = entry.get("name", entry.get("fullName", entry.get("n", "")))
                inn_val = entry.get("inn", entry.get("i", ""))
                ogrn = entry.get("ogrn", entry.get("o", ""))
                address = entry.get("address", entry.get("a", ""))
                status = entry.get("status", entry.get("s", ""))
                results.append({
                    "source": "egrul", "query": query, "query_type": query_type,
                    "name": name, "inn": inn_val, "ogrn": ogrn,
                    "address": address, "status": status, "raw": entry,
                })
    except Exception as e:
        log.warning("EGRUL search failed for %s: %s", query[:30], e)
    return results


def search_egrul_and_store(conn, source_id, query, query_type="inn"):
    results = search_egrul(query, query_type)
    stored = 0
    for r in results:
        name = r.get("name", "")
        inn_val = r.get("inn", "")
        body_parts = [
            f"Статус: {r.get('status', '')}" if r.get("status") else "",
            f"Адрес: {r.get('address', '')}" if r.get("address") else "",
            f"ОГРН: {r.get('ogrn', '')}" if r.get("ogrn") else "",
            f"ИНН: {inn_val}" if inn_val else "",
        ]
        body = "\n".join(p for p in body_parts if p)
        ext_id = f"egrul_search_{hashlib.sha256((query + inn_val).encode()).hexdigest()[:16]}"
        _store_result(conn, source_id, ext_id,
                      json.dumps(r, ensure_ascii=False, default=str),
                      "registry_record", name or f"ЕГРЮЛ: {query}",
                      body, "", f"https://egrul.nalog.ru/entity/{inn_val}" if inn_val else "https://egrul.nalog.ru/")
        stored += 1
    return stored


# ============================================================
# MINJUST — reestrs.minjust.gov.ru API
# ============================================================

MINJUST_REESTRS_BASE = "https://reestrs.minjust.gov.ru"
MINJUST_REGISTRY_ID_RE = re.compile(r"let id = '([^']+)'")


def _minjust_get_registry_id(session) -> str:
    resp = session.get("https://minjust.gov.ru/ru/pages/reestr-inostryannykh-agentov/", timeout=30)
    if resp.status_code == 200:
        match = MINJUST_REGISTRY_ID_RE.search(resp.text)
        if match:
            return match.group(1)
    return "39b95df9-9a68-6b6d-e1e3-e6388507067e"


def search_minjust(query: str, field: str = "name", timeout: int = 30) -> List[Dict]:
    import requests
    results = []
    try:
        session = _session(verify=False)
        registry_id = _minjust_get_registry_id(session)
        info_resp = session.get(f"{MINJUST_REESTRS_BASE}/rest/registry/{registry_id}/info", timeout=timeout)
        info_resp.raise_for_status()
        columns = {col.get("name"): col.get("title") for col in info_resp.json().get("columns", []) if col.get("name")}

        offset = 0
        limit = 300
        total = None
        query_lower = query.lower()
        while total is None or offset < total:
            values_resp = session.post(
                f"{MINJUST_REESTRS_BASE}/rest/registry/{registry_id}/values",
                json={"offset": offset, "limit": limit},
                headers={"Content-Type": "application/json;charset=utf-8"},
                timeout=timeout,
            )
            values_resp.raise_for_status()
            payload = values_resp.json()
            total = int(payload.get("size") or 0)
            rows = payload.get("values") or []
            if not rows:
                break
            for item in rows:
                name = item.get("field_2_s", "").strip()
                inn_val = item.get("field_9_s", "").strip()
                domain = item.get("field_6_s", "").strip()
                if field == "inn" and inn_val != query:
                    continue
                if field == "domain" and domain != query:
                    continue
                if field == "name":
                    if query_lower not in name.lower():
                        continue
                if not name:
                    continue
                results.append({
                    "source": "minjust", "query": query, "field": field,
                    "name": name,
                    "type": item.get("field_7_s", ""),
                    "included_at": item.get("field_4_dt") or item.get("field_4_s", ""),
                    "excluded_at": item.get("field_5_dt") or item.get("field_5_s", ""),
                    "reasons": item.get("field_3_s", ""),
                    "inn": inn_val,
                    "ogrn": item.get("field_10_s", ""),
                    "domain": domain,
                    "reg_number": item.get("field_8_s", ""),
                    "raw": item,
                })
            offset += len(rows)
            if len(results) >= 50:
                break
    except Exception as e:
        log.warning("Minjust search failed for %s: %s", query[:30], e)
    return results


def search_minjust_and_store(conn, source_id, query, field="name"):
    results = search_minjust(query, field)
    stored = 0
    for r in results:
        body_parts = [
            f"Тип: {r['type']}" if r.get("type") else "",
            f"Дата включения: {r['included_at']}" if r.get("included_at") else "",
            f"Дата исключения: {r['excluded_at']}" if r.get("excluded_at") else "",
            f"Основания: {r['reasons']}" if r.get("reasons") else "",
            f"ИНН: {r['inn']}" if r.get("inn") else "",
            f"ОГРН: {r['ogrn']}" if r.get("ogrn") else "",
        ]
        body = "\n".join(p for p in body_parts if p)
        ext_id = f"minjust_search_{hashlib.sha256((query + r.get('inn', '')).encode()).hexdigest()[:16]}"
        _store_result(conn, source_id, ext_id,
                      json.dumps(r, ensure_ascii=False, default=str),
                      "registry_record", f"Иноагент: {r['name']}",
                      body, r.get("included_at", ""),
                      "https://minjust.gov.ru/ru/pages/reestr-inostryannykh-agentov/")
        stored += 1
    return stored


# ============================================================
# DUMA — sozd.duma.gov.ru
# ============================================================

def search_duma(query: str, pages: int = 3, timeout: int = 30) -> List[Dict]:
    from bs4 import BeautifulSoup
    results = []
    try:
        session = _session()
        for page in range(1, pages + 1):
            resp = session.get(
                "https://sozd.duma.gov.ru/oz/b",
                params={"b[Annotation]": query, "page": page},
                timeout=timeout,
            )
            if resp.status_code != 200:
                break
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
                results.append({
                    "source": "duma", "query": query,
                    "number": number, "title": title_text, "status": status,
                    "registration_date": reg_date, "sponsor": sponsor,
                    "last_event": last_event, "last_event_date": last_event_date,
                    "url": link,
                })
                page_rows += 1
            if page_rows == 0:
                break
    except Exception as e:
        log.warning("Duma search failed for %s: %s", query[:30], e)
    return results


def search_duma_and_store(conn, source_id, query, pages=3):
    results = search_duma(query, pages)
    stored = 0
    for r in results:
        body_parts = [
            f"Номер: {r['number']}",
            f"Название: {r['title']}" if r.get("title") else "",
            f"Статус: {r['status']}" if r.get("status") else "",
            f"Дата регистрации: {r['registration_date']}" if r.get("registration_date") else "",
            f"СПЗИ: {r['sponsor']}" if r.get("sponsor") else "",
            f"Последнее событие: {r['last_event']}" if r.get("last_event") else "",
            f"Поисковый запрос: {query}",
        ]
        body = "\n".join(p for p in body_parts if p)
        ext_id = f"duma_search_{hashlib.sha256((query + r['number']).encode()).hexdigest()[:16]}"
        _store_result(conn, source_id, ext_id,
                      json.dumps(r, ensure_ascii=False),
                      "bill", f"Законопроект {r['number']}: {r.get('title', '')[:100]}",
                      body, r.get("registration_date", ""), r.get("url", "https://sozd.duma.gov.ru/"))
        stored += 1
    return stored


# ============================================================
# ZAKUPKI — zakupki.gov.ru
# ============================================================

def search_zakupki(query: str, pages: int = 3, timeout: int = 30) -> List[Dict]:
    from bs4 import BeautifulSoup
    results = []
    try:
        session = _session()
        for page in range(1, pages + 1):
            url = "https://zakupki.gov.ru/epz/contract/search/results.html"
            resp = session.get(url, params={"searchString": query, "pageNumber": str(page)}, timeout=timeout)
            if resp.status_code != 200:
                break
            soup = BeautifulSoup(resp.text, "lxml")
            items = soup.select(".search-registry-entry-block, .card-wrapper, .row.record, div[data-object-id]")
            if not items:
                break
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
                        "source": "zakupki", "query": query,
                        "contract_number": number, "registry_number": registry_number,
                        "price": price, "customer": customer, "url": full_link,
                    })
            time.sleep(1)
    except Exception as e:
        log.warning("Zakupki search failed for %s: %s", query[:30], e)
    return results


def search_zakupki_and_store(conn, source_id, query, pages=3):
    results = search_zakupki(query, pages)
    stored = 0
    for r in results:
        body_parts = [
            f"Реестровый номер: {r['registry_number']}" if r.get("registry_number") else "",
            f"Номер: {r['contract_number']}" if r.get("contract_number") else "",
            f"Цена: {r['price']}" if r.get("price") else "",
            f"Заказчик: {r['customer']}" if r.get("customer") else "",
            f"Запрос: {query}",
        ]
        body = "\n".join(p for p in body_parts if p)
        ext_id = f"zakupki_search_{hashlib.sha256((query + (r.get('registry_number') or r.get('contract_number') or '')).encode()).hexdigest()[:16]}"
        _store_result(conn, source_id, ext_id,
                      json.dumps(r, ensure_ascii=False),
                      "procurement", f"Контракт: {r.get('registry_number') or r.get('contract_number', '—')}",
                      body, "", r.get("url", "https://zakupki.gov.ru/"))
        stored += 1
    return stored


# ============================================================
# KAD.ARBIRR — kad.arbitr.ru
# ============================================================

def search_kad_arbitr(query: str, query_type: str = "name", page: int = 1, timeout: int = 20) -> List[Dict]:
    import requests
    results = []
    try:
        params = {"SimpleSearch": query, "Page": page}
        headers = {**HEADERS, "Referer": "https://kad.arbitr.ru/"}
        resp = requests.post("https://kad.arbitr.ru/Kad/Search", data=params, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            return results
        try:
            data = resp.json()
        except json.JSONDecodeError:
            return results
        cases = data.get("data", {}).get("result", []) if isinstance(data.get("data"), dict) else []
        if isinstance(data.get("Result"), list):
            cases = data["Result"]
        for c in (cases or [])[:25]:
            results.append({
                "source": "kad_arbitr", "query": query, "query_type": query_type,
                "case_number": c.get("caseNumber", c.get("number", c.get("CaseNumber", ""))),
                "category": c.get("category", c.get("Category", "")),
                "court": c.get("court", c.get("Court", "")),
                "status": c.get("status", c.get("Status", "")),
                "date": c.get("date", c.get("Date", "")),
                "url": c.get("url", c.get("Href", "https://kad.arbitr.ru/")),
                "raw": c,
            })
    except Exception as e:
        log.warning("Kad.Arbitr search failed for %s: %s", query[:30], e)
    return results


def search_kad_arbitr_and_store(conn, source_id, query, query_type="name"):
    results = search_kad_arbitr(query, query_type)
    stored = 0
    for r in results:
        body_parts = [
            f"Дело: {r['case_number']}" if r.get("case_number") else "",
            f"Категория: {r['category']}" if r.get("category") else "",
            f"Суд: {r['court']}" if r.get("court") else "",
            f"Статус: {r['status']}" if r.get("status") else "",
            f"Запрос: {query}",
        ]
        body = "\n".join(p for p in body_parts if p)
        ext_id = f"kad_search_{hashlib.sha256((query + r.get('case_number', '')).encode()).hexdigest()[:16]}"
        _store_result(conn, source_id, ext_id,
                      json.dumps(r, ensure_ascii=False, default=str),
                      "court_record", f"Арбитраж: {r.get('case_number', query)}",
                      body, r.get("date", ""), r.get("url", "https://kad.arbitr.ru/"))
        stored += 1
    return stored


# ============================================================
# FSSP — is.fssp.gov.ru
# ============================================================

def search_fssp(lastname: str, firstname: str, secondname: str = "", region_id: int = -1) -> List[Dict]:
    import requests
    results = []
    try:
        data = {
            "is[extended]": "0",
            "is[region_id][0]": str(region_id),
            "is[lastname]": lastname,
            "is[firstname]": firstname,
            "is[secondname]": secondname or "",
        }
        headers = {**HEADERS, "X-Requested-With": "XMLHttpRequest"}
        resp = requests.post("https://is.fssp.gov.ru/is/ajax_search", data=data, headers=headers, timeout=15)
        if resp.status_code != 200:
            return results
        result = resp.json()
        items = result.get("data", {}).get("result", []) if isinstance(result.get("data"), dict) else []
        for item in items[:25]:
            results.append({
                "source": "fssp", "lastname": lastname, "firstname": firstname,
                "detail": item.get("detail", item.get("name", "")),
                "amount": item.get("amount", ""),
                "raw": item,
            })
    except Exception as e:
        log.warning("FSSP search failed for %s %s: %s", lastname, firstname, e)
    return results


def _parse_fio(fio: str) -> Tuple[str, str, str]:
    parts = fio.strip().split()
    if len(parts) >= 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        return parts[0], parts[1], ""
    elif len(parts) == 1:
        return parts[0], "", ""
    return "", "", ""


def search_fssp_and_store(conn, source_id, query):
    lastname, firstname, secondname = _parse_fio(query)
    if not lastname or not firstname:
        return 0
    results = search_fssp(lastname, firstname, secondname)
    stored = 0
    for r in results:
        body_parts = [
            f"ФИО: {lastname} {firstname} {secondname}".strip(),
            f"Производство: {r['detail']}" if r.get("detail") else "",
            f"Сумма: {r['amount']}" if r.get("amount") else "",
        ]
        body = "\n".join(p for p in body_parts if p)
        ext_id = f"fssp_search_{hashlib.sha256((query + (r.get('detail') or '')).encode()).hexdigest()[:16]}"
        _store_result(conn, source_id, ext_id,
                      json.dumps(r, ensure_ascii=False, default=str),
                      "enforcement", f"ФССП: {lastname} {firstname}",
                      body, "", "https://is.fssp.gov.ru/")
        stored += 1
    return stored


# ============================================================
# SUDRF — sudrf.ru
# ============================================================

def search_sudrf(participant: str, court_code: str = "", case_number: str = "", page: int = 1) -> List[Dict]:
    import requests
    from bs4 import BeautifulSoup
    results = []
    try:
        params = {
            "case_number": case_number or "",
            "participant_name": participant or "",
            "court_code": court_code or "",
            "page": page,
        }
        resp = requests.post("https://sudrf.ru/blocks/sr_court/search", data=params, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return results
        soup = BeautifulSoup(resp.text, "lxml")
        rows = soup.select("table tbody tr, .case-row, .search-result, tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            case_num = cells[0].get_text(strip=True)
            if case_num:
                results.append({
                    "source": "sudrf", "participant": participant,
                    "case_number": case_num,
                    "category": cells[1].get_text(strip=True) if len(cells) > 1 else "",
                    "court": cells[2].get_text(strip=True) if len(cells) > 2 else "",
                    "status": cells[3].get_text(strip=True) if len(cells) > 3 else "",
                    "date": cells[4].get_text(strip=True) if len(cells) > 4 else "",
                })
    except Exception as e:
        log.warning("Sudrf search failed for %s: %s", participant[:30], e)
    return results


def search_sudrf_and_store(conn, source_id, query):
    results = search_sudrf(participant=query)
    stored = 0
    for r in results:
        body_parts = [
            f"Дело: {r['case_number']}" if r.get("case_number") else "",
            f"Категория: {r['category']}" if r.get("category") else "",
            f"Суд: {r['court']}" if r.get("court") else "",
            f"Статус: {r['status']}" if r.get("status") else "",
            f"Запрос: {query}",
        ]
        body = "\n".join(p for p in body_parts if p)
        ext_id = f"sudrf_search_{hashlib.sha256((query + r.get('case_number', '')).encode()).hexdigest()[:16]}"
        _store_result(conn, source_id, ext_id,
                      json.dumps(r, ensure_ascii=False),
                      "court_record", f"Суд. дело: {r.get('case_number', query)}",
                      body, r.get("date", ""), "https://sudrf.ru/")
        stored += 1
    return stored


# ============================================================
# PRAVO — publication.pravo.gov.ru (HTTP only)
# ============================================================

def search_pravo(query: str, timeout: int = 15) -> List[Dict]:
    from bs4 import BeautifulSoup
    results = []
    try:
        session = _session(verify=False)
        base_url = "http://pravo.gov.ru"
        search_url = f"{base_url}/search/"
        params = {"q": query}
        try:
            resp = session.get(search_url, params=params, timeout=_timeout(timeout))
        except Exception:
            try:
                resp = session.get("http://publication.pravo.gov.ru/", params={"q": query}, timeout=_timeout(timeout))
            except Exception:
                return results
        if resp.status_code != 200:
            return results
        soup = BeautifulSoup(resp.text, "lxml")
        items = soup.select(".search-result, .result-item, .document-item, .law-item, .search-item")
        if not items:
            anchors = soup.select("a[href*='document'], a[href*='law'], a[href*='ukaz'], a[href*='postanov']")
            for a in anchors[:30]:
                text = a.get_text(strip=True)
                href = a.get("href", "")
                if text and len(text) > 10:
                    items.append(a)
        for item in items[:30]:
            if item.name == "a":
                text = item.get_text(strip=True)
                href = item.get("href", "")
            else:
                a_el = item.select_one("a")
                text = item.get_text(" ", strip=True)
                href = a_el.get("href", "") if a_el else ""
            full_url = urljoin(base_url, href)
            if text and len(text) > 5:
                results.append({
                    "source": "pravo", "query": query,
                    "title": text[:300], "url": full_url,
                })
    except Exception as e:
        log.warning("Pravo search failed for %s: %s", query[:30], e)
    return results


def search_pravo_and_store(conn, source_id, query):
    results = search_pravo(query)
    stored = 0
    for r in results:
        ext_id = f"pravo_search_{hashlib.sha256((query + r['url']).encode()).hexdigest()[:16]}"
        _store_result(conn, source_id, ext_id,
                      json.dumps(r, ensure_ascii=False),
                      "registry_record", r.get("title", f"Право: {query}"),
                      r.get("title", ""), "", r.get("url", "http://pravo.gov.ru/"))
        stored += 1
    return stored


# ============================================================
# GIS ЖКХ — dom.gosuslugi.ru
# ============================================================

def search_gis_gkh(query: str, timeout: int = 15) -> List[Dict]:
    from bs4 import BeautifulSoup
    results = []
    try:
        session = _session()
        search_url = "https://dom.gosuslugi.ru/search"
        try:
            resp = session.get(search_url, params={"q": query}, timeout=_timeout(timeout))
        except Exception:
            resp = session.get("https://dom.gosuslugi.ru/", timeout=_timeout(timeout))
        if resp.status_code != 200:
            return results
        soup = BeautifulSoup(resp.text, "lxml")
        for a in soup.select("a[href]")[:40]:
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if text and len(text) > 5 and href:
                full_url = urljoin("https://dom.gosuslugi.ru", href)
                results.append({
                    "source": "gis_gkh", "query": query,
                    "title": text[:300], "url": full_url,
                })
    except Exception as e:
        log.warning("GIS GKH search failed for %s: %s", query[:30], e)
    return results


# ============================================================
# UNIFIED SEARCH INTERFACE
# ============================================================

SEARCH_FUNCTIONS = {
    "egrul": {"func": search_egrul_and_store, "query_types": ["inn", "name", "ogrn"],
              "url_contains": "egrul.nalog.ru",
              "source_meta": {"name": "ЕГРЮЛ", "category": "official_registry", "subcategory": "business",
                              "url": "egrul.nalog.ru", "access_method": "api"}},
    "minjust": {"func": search_minjust_and_store, "query_types": ["name", "inn", "domain"],
                "url_contains": "minjust.gov.ru",
                "source_meta": {"name": "Минюст иноагенты", "category": "official_registry",
                                "subcategory": "foreign_agents", "url": "minjust.gov.ru", "access_method": "api"}},
    "duma": {"func": search_duma_and_store, "query_types": ["keyword"],
             "url_contains": "sozd.duma.gov.ru",
             "source_meta": {"name": "Госдума законопроекты", "category": "official_site",
                             "subcategory": "legislation", "url": "sozd.duma.gov.ru", "access_method": "html"}},
    "zakupki": {"func": search_zakupki_and_store, "query_types": ["keyword", "inn"],
                "url_contains": "zakupki.gov.ru",
                "source_meta": {"name": "Госзакупки", "category": "official_registry",
                                "subcategory": "procurement", "url": "zakupki.gov.ru", "access_method": "html"}},
    "kad_arbitr": {"func": search_kad_arbitr_and_store, "query_types": ["name", "case_number"],
                   "url_contains": "kad.arbitr.ru",
                   "source_meta": {"name": "Кад.Арбитр", "category": "official_registry",
                                   "subcategory": "courts", "url": "kad.arbitr.ru", "access_method": "html"}},
    "fssp": {"func": search_fssp_and_store, "query_types": ["name"],
             "url_contains": "fssp.gov.ru",
             "source_meta": {"name": "ФССП", "category": "official_registry",
                             "subcategory": "enforcement", "url": "fssp.gov.ru", "access_method": "html"}},
    "sudrf": {"func": search_sudrf_and_store, "query_types": ["name"],
              "url_contains": "sudrf.ru",
              "source_meta": {"name": "Суд.РФ", "category": "official_registry",
                              "subcategory": "courts", "url": "sudrf.ru", "access_method": "html"}},
    "pravo": {"func": search_pravo_and_store, "query_types": ["keyword"],
              "url_contains": "pravo.gov.ru",
              "source_meta": {"name": "Право.Гослуслуги", "category": "official_registry",
                              "subcategory": "laws", "url": "pravo.gov.ru", "access_method": "html"}},
}


def targeted_search(
    query: str,
    query_type: str = "auto",
    sites: Optional[List[str]] = None,
    settings: dict = None,
) -> Dict[str, int]:
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    results = {}

    if sites is None:
        sites = [s for s in SEARCH_FUNCTIONS if SITE_STATUS.get(s) not in ("unreachable", "auth_required")]

    if query_type == "auto":
        query_type = _detect_query_type(query)

    for site_name in sites:
        site_cfg = SEARCH_FUNCTIONS.get(site_name)
        if not site_cfg:
            log.warning("Unknown site: %s", site_name)
            continue
        if SITE_STATUS.get(site_name) == "unreachable":
            log.info("Skipping unreachable site: %s", site_name)
            continue

        source_id = _get_or_create_source(conn, **site_cfg["source_meta"])
        try:
            search_func = site_cfg["func"]
            if site_name == "egrul":
                stored = search_func(conn, source_id, query, query_type=query_type)
            elif site_name == "minjust":
                field = "inn" if query_type == "inn" else "name"
                stored = search_func(conn, source_id, query, field=field)
            elif site_name == "kad_arbitr":
                stored = search_func(conn, source_id, query, query_type=query_type)
            else:
                stored = search_func(conn, source_id, query)
            results[site_name] = stored or 0
            log.info("Site search %s: %d results for '%s'", site_name, stored or 0, query[:50])
        except Exception as e:
            log.error("Site search %s failed for '%s': %s", site_name, query[:30], e)
            results[site_name] = -1
        time.sleep(0.5)

    conn.commit()
    conn.close()
    return results


def _detect_query_type(query: str) -> str:
    digits = re.sub(r"\D", "", query)
    if len(digits) == 10 or len(digits) == 12:
        return "inn"
    if re.match(r"^[А-ЯЁA-Z]\d{2,}[А-ЯЁA-Z]?\d?-\d", query, re.I):
        return "case_number"
    parts = query.strip().split()
    if len(parts) >= 2 and all(len(p) > 1 for p in parts[:2]):
        if re.match(r"^[А-ЯЁ]", parts[0], re.I):
            return "name"
    return "keyword"


def search_for_entity(entity_id: int, settings: dict = None) -> Dict[str, int]:
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    row = conn.execute(
        "SELECT canonical_name, entity_type, inn FROM entities WHERE id=?",
        (entity_id,),
    ).fetchone()
    conn.close()
    if not row:
        return {}
    name, etype, inn = row
    results = {}
    if inn and len(inn) in (10, 12):
        r = targeted_search(inn, query_type="inn", settings=settings)
        for k, v in r.items():
            results.setdefault(k, 0)
            results[k] += v if v > 0 else 0
    if name and len(name) > 3:
        qt = "name" if etype == "person" else "keyword"
        r = targeted_search(name, query_type=qt, settings=settings)
        for k, v in r.items():
            results.setdefault(k, 0)
            results[k] += v if v > 0 else 0
    return results


def search_for_claim(claim_id: int, settings: dict = None) -> Dict[str, int]:
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    row = conn.execute(
        "SELECT claim_text, claim_type FROM claims WHERE id=?",
        (claim_id,),
    ).fetchone()
    conn.close()
    if not row:
        return {}
    claim_text, claim_type = row
    queries = _extract_search_queries(claim_text)
    results = {}
    for q in queries[:5]:
        r = targeted_search(q, query_type="auto", settings=settings)
        for k, v in r.items():
            results.setdefault(k, 0)
            results[k] += v if v > 0 else 0
        time.sleep(1)
    return results


def _extract_search_queries(text: str) -> List[str]:
    queries = []
    inn_matches = re.findall(r"\b(\d{10}|\d{12})\b", text)
    queries.extend(inn_matches[:3])
    name_patterns = [
        r"([А-ЯЁ][а-яё]+ [А-ЯЁ][а-яё]+(?: [А-ЯЁ][а-яё]+)?)",
        r"(депутат|сенатор|министр|губернатор|мэр|прокурор)\s+([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)",
    ]
    for pat in name_patterns:
        for m in re.finditer(pat, text):
            name = m.group(1) if m.lastindex == 1 else m.group(2)
            if len(name) > 5 and name not in queries:
                queries.append(name)
    keyword_patterns = [
        r"(законопроект|закон|постановление|указ)\s+[N№]\s*([\d-]+)",
        r"(дело|иск)\s+[N№]?\s*([\w-]+)",
    ]
    for pat in keyword_patterns:
        for m in re.finditer(pat, text, re.I):
            kw = f"{m.group(1)} {m.group(2)}"
            if kw not in queries:
                queries.append(kw)
    if not queries:
        words = re.findall(r"[а-яё]{4,}", text.lower())
        from collections import Counter
        common = [w for w, _ in Counter(words).most_common(5)]
        if common:
            queries.append(" ".join(common))
    return queries[:8]


def batch_search_entities(limit: int = 50, entity_type: str = "person", settings: dict = None) -> Dict:
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    rows = conn.execute(
        "SELECT id, canonical_name, inn FROM entities WHERE entity_type=? AND length(canonical_name) > 5 LIMIT ?",
        (entity_type, limit),
    ).fetchall()
    conn.close()
    total = {"entities": 0, "total_results": 0, "by_site": {}}
    for eid, name, inn in rows:
        log.info("Batch search entity %d: %s (ИНН=%s)", eid, name, inn or "—")
        results = search_for_entity(eid, settings)
        total["entities"] += 1
        for site, count in results.items():
            total["by_site"].setdefault(site, 0)
            total["by_site"][site] += count
            total["total_results"] += count if count > 0 else 0
        time.sleep(2)
    return total


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Smart targeted search on official Russian sites")
    parser.add_argument("query", nargs="?", help="Search query (name, INN, keyword)")
    parser.add_argument("--type", choices=["inn", "name", "keyword", "case_number", "auto"], default="auto")
    parser.add_argument("--sites", nargs="+", help="Specific sites to search")
    parser.add_argument("--entity-id", type=int, help="Search for a specific entity from DB")
    parser.add_argument("--claim-id", type=int, help="Search for evidence for a specific claim")
    parser.add_argument("--batch-entities", type=int, help="Batch search N entities")
    parser.add_argument("--entity-type", default="person", choices=["person", "organization"])
    args = parser.parse_args()

    settings = load_settings()

    if args.entity_id:
        r = search_for_entity(args.entity_id, settings)
        print(f"Entity {args.entity_id}: {r}")
    elif args.claim_id:
        r = search_for_claim(args.claim_id, settings)
        print(f"Claim {args.claim_id}: {r}")
    elif args.batch_entities:
        r = batch_search_entities(args.batch_entities, args.entity_type, settings)
        print(f"Batch: {r}")
    elif args.query:
        r = targeted_search(args.query, query_type=args.type, sites=args.sites, settings=settings)
        print(f"Query '{args.query}': {r}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
