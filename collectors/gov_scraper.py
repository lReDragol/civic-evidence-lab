import json
import logging
import re
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

BASE_HTTP = "http://kremlin.ru"
BASE_HTTPS = "https://kremlin.ru"
GOV_HTTP = "http://government.ru"
GOV_HTTPS = "https://government.ru"
PRAVO_HTTP = "http://publication.pravo.gov.ru"
PRAVO_HTTPS = "https://publication.pravo.gov.ru"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}


def _session():
    import requests
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _get_with_fallback(session, url_https: str, url_http: str, timeout: int = 20):
    try:
        r = session.get(url_https, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return r
    except Exception:
        pass
    try:
        r = session.get(url_http, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return r
    except Exception:
        pass
    return None


def _ensure_source(conn, name: str, url: str, category: str, credibility_tier: str = "A"):
    row = conn.execute("SELECT id FROM sources WHERE url=? AND category=?", (url, category)).fetchone()
    if row:
        conn.execute("UPDATE sources SET last_checked_at=datetime('now') WHERE id=?", (row[0],))
        return row[0]
    cur = conn.execute(
        "INSERT INTO sources(name, category, url, is_official, credibility_tier, is_active) VALUES(?,?,?,?,?,1)",
        (name, category, url, 1, credibility_tier),
    )
    return cur.lastrowid


def _get_or_create_entity(conn, entity_type, canonical_name, description=None):
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type=? AND canonical_name=?",
        (entity_type, canonical_name),
    ).fetchone()
    if row:
        eid = row[0]
        if description:
            conn.execute("UPDATE entities SET description=? WHERE id=?", (description, eid))
        return eid
    cur = conn.execute(
        "INSERT INTO entities(entity_type, canonical_name, description) VALUES(?,?,?)",
        (entity_type, canonical_name, description),
    )
    return cur.lastrowid


MONTHS_RU = {
    "января": "01", "февраля": "02", "марта": "03", "апреля": "04",
    "мая": "05", "июня": "06", "июля": "07", "августа": "08",
    "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12",
}


def _parse_ru_date(text: str) -> str:
    m = re.search(r"от\s+(\d{1,2})\s+(\w+)\s+(\d{4})\s*г", text)
    if m:
        day = m.group(1).zfill(2)
        month_name = m.group(2).lower()
        year = m.group(3)
        month = MONTHS_RU.get(month_name)
        if month:
            return f"{day}.{month}.{year}"
    m2 = re.search(r"от\s+(\d{2}\.\d{2}\.\d{4})\s*г", text)
    if m2:
        return m2.group(1)
    return ""


def search_acts_bank(session, query: str = "", pages: int = 5) -> List[Dict]:
    from bs4 import BeautifulSoup
    acts = []
    for page in range(1, pages + 1):
        try:
            params = {}
            if query:
                params["query"] = query
                params["title"] = query
            url_https = f"{BASE_HTTPS}/acts/bank/page/{page}"
            url_http = f"{BASE_HTTP}/acts/bank/page/{page}"
            if page == 1 and not query:
                url_https = f"{BASE_HTTPS}/acts/bank"
                url_http = f"{BASE_HTTP}/acts/bank"
            elif page == 1 and query:
                url_https = f"{BASE_HTTPS}/acts/bank/search"
                url_http = f"{BASE_HTTP}/acts/bank/search"

            r = _get_with_fallback(session, url_https, url_http)
            if not r or r.status_code != 200:
                break

            soup = BeautifulSoup(r.text, "lxml")
            page_acts = []

            for link in soup.select("h2 a[href*='/acts/bank/'], h3 a[href*='/acts/bank/'], .entry_title a[href*='/acts/bank/']"):
                title = link.get_text(strip=True)
                href = link.get("href", "")
                if not title or not href:
                    continue
                full_url = href if href.startswith("http") else BASE_HTTPS + href

                date = _parse_ru_date(title)

                number_m = re.search(r"(?:№\s*|N\s*)([\d\-ФКЗФ]+(?:-ФЗ?)?)", title)
                number = number_m.group(1) if number_m else ""

                act_type = ""
                if "Указ" in title:
                    act_type = "Указ Президента РФ"
                elif "Федеральный конституционный закон" in title:
                    act_type = "ФКЗ"
                elif "Федеральный закон" in title:
                    act_type = "ФЗ"
                elif "Распоряжение" in title:
                    act_type = "Распоряжение"
                elif "Поручение" in title:
                    act_type = "Поручение"

                page_acts.append({
                    "title": title,
                    "url": full_url,
                    "date": date,
                    "number": number,
                    "act_type": act_type,
                    "source": "kremlin.ru",
                })

            if not page_acts:
                for link in soup.select("a[href*='/acts/bank/']"):
                    title = link.get_text(strip=True)
                    href = link.get("href", "")
                    if title and href and "/acts/bank/" in href and len(title) > 20:
                        full_url = href if href.startswith("http") else BASE_HTTPS + href
                        if full_url not in [a["url"] for a in acts + page_acts]:
                            page_acts.append({
                                "title": title,
                                "url": full_url,
                                "date": "",
                                "number": "",
                                "act_type": "",
                                "source": "kremlin.ru",
                            })

            acts.extend(page_acts)
            log.info("Kremlin acts page %d: found %d", page, len(page_acts))
            if not page_acts:
                break
            time.sleep(0.3)
        except Exception as e:
            log.warning("Kremlin acts page %d failed: %s", page, e)
            continue
    return acts


def scrape_act_detail(session, url: str) -> Dict:
    from bs4 import BeautifulSoup
    detail = {"url": url}
    try:
        url_https = url.replace(BASE_HTTP, BASE_HTTPS)
        url_http = url.replace(BASE_HTTPS, BASE_HTTP) if url_https != url else url
        r = _get_with_fallback(session, url_https, url_http)
        if not r or r.status_code != 200:
            return detail
        soup = BeautifulSoup(r.text, "lxml")

        h1 = soup.select_one("h1, .title, .act-title")
        if h1:
            detail["title"] = h1.get_text(strip=True)

        for meta in soup.select("meta"):
            name = meta.get("name", "").lower()
            content = meta.get("content", "")
            if "description" in name and content:
                detail["description"] = content
            elif "date" in name and content:
                detail["meta_date"] = content

        signers_el = soup.select_one(".signers, .signed-by, [class*='signer']")
        if signers_el:
            detail["signers"] = signers_el.get_text(strip=True)

        pravo_url = None
        for a in soup.select("a[href*='pravo.gov.ru']"):
            href = a.get("href", "")
            if "pravo.gov.ru/proxy/ips" in href or "pravo.gov.ru" in href:
                if not href.startswith("https://"):
                    href = href.replace("http://", "https://")
                pravo_url = href
                break

        if pravo_url:
            try:
                pr = session.get(pravo_url, timeout=20, allow_redirects=True)
                if pr.status_code == 200 and len(pr.text) > 500:
                    pr_soup = BeautifulSoup(pr.text, "lxml")
                    content_el = pr_soup.select_one("#content, .content, article, .document-text, .text")
                    if not content_el:
                        content_el = pr_soup.select_one("body")
                    if content_el:
                        text = content_el.get_text("\n", strip=True)
                        if len(text) > 100:
                            detail["full_text"] = text[:50000]
            except Exception as e:
                log.warning("Pravo.gov.ru detail fetch failed for %s: %s", pravo_url, e)

        print_url = None
        for a in soup.select("a[href*='/print']"):
            href = a.get("href", "")
            if "/print" in href:
                print_url = href if href.startswith("http") else BASE_HTTPS + href
                break

        if not detail.get("full_text") and print_url:
            try:
                pr = session.get(print_url, timeout=20, allow_redirects=True)
                if pr.status_code == 200 and len(pr.text) > 500:
                    pr_soup = BeautifulSoup(pr.text, "lxml")
                    content_el = pr_soup.select_one("article, .read__text, .text, #content, main")
                    if not content_el:
                        content_el = pr_soup.select_one("body")
                    if content_el:
                        text = content_el.get_text("\n", strip=True)
                        if len(text) > 100:
                            detail["full_text"] = text[:50000]
            except Exception as e:
                log.warning("Print version fetch failed for %s: %s", print_url, e)

        law_refs = []
        full_text = detail.get("full_text", "")
        title_text = detail.get("title", "")
        if full_text or title_text:
            search_text = (title_text + " " + full_text)[:10000]
            for m in re.finditer(r"Федеральн[а-яё]+\s+закон[^№]*?(?:от\s*\d{1,2}\s+\w+\s+\d{4}\s*г\.?\s*)?№\s*([\d\-]+[-–]ФЗ)", search_text, re.IGNORECASE):
                law_refs.append({"law_type": "ФЗ", "law_number": m.group(1).replace("–", "-")})
            for m in re.finditer(r"ст(?:ать[а-яё]*)?[\.\s]*(\d[\d.]*)\s*(?:ч\.\s*(\d))?\s*(?:УК\s*РФ|КоАП)", search_text, re.IGNORECASE):
                law_type = "УК РФ" if "УК" in m.group(0) else "КоАП"
                law_refs.append({"law_type": law_type, "article": m.group(1), "part": m.group(2) or ""})
            for m in re.finditer(r"(?:Указ|Распоряжение|Поручение)[^№]*№\s*([\d\-]+)", search_text, re.IGNORECASE):
                if "Указ" in m.group(0):
                    law_refs.append({"law_type": "Указ", "law_number": m.group(1)})
                elif "Распоряжение" in m.group(0):
                    law_refs.append({"law_type": "Распоряжение", "law_number": m.group(1)})
        if law_refs:
            detail["law_references"] = law_refs

        doc_date = _parse_ru_date(title_text + " " + full_text[:500])
        if doc_date:
            detail["document_date"] = doc_date

        number_m = re.search(r"№\s*([\d\-]+(?:-ФЗ|ФКЗ)?)", title_text)
        if number_m:
            detail["document_number"] = number_m.group(1)

    except Exception as e:
        log.warning("Kremlin act detail failed for %s: %s", url, e)
    return detail


def store_act(conn, act: Dict, detail: Optional[Dict] = None) -> Optional[int]:
    title = act.get("title", "")
    url = act.get("url", "")
    if not title and not url:
        return None

    existing = conn.execute(
        "SELECT id FROM investigative_materials WHERE url=? AND material_type=?",
        (url, "presidential_act"),
    ).fetchone()
    if existing:
        if detail and detail.get("full_text"):
            conn.execute(
                "UPDATE investigative_materials SET summary=?, raw_data=? WHERE id=?",
                (detail.get("description", "") or detail.get("full_text", "")[:500],
                 json.dumps({**act, **(detail or {})}, ensure_ascii=False, default=str),
                 existing[0]),
            )
        return existing[0]

    full_text = ""
    if detail and detail.get("full_text"):
        full_text = detail["full_text"][:50000]

    description = ""
    if detail and detail.get("description"):
        description = detail["description"]
    elif detail and detail.get("full_text"):
        description = detail["full_text"][:500]

    raw_data = json.dumps({**act, **(detail or {})}, ensure_ascii=False, default=str)

    date_val = act.get("date", "")
    if detail and detail.get("document_date"):
        date_val = detail["document_date"]

    involved_json = None
    if detail and detail.get("signers"):
        involved = [{"name": detail["signers"], "role": "подписавший"}]
        involved_json = json.dumps(involved, ensure_ascii=False)

    laws_json = None
    if detail and detail.get("law_references"):
        laws_json = json.dumps(detail["law_references"], ensure_ascii=False)

    cur = conn.execute(
        "INSERT INTO investigative_materials(title, material_type, url, source_org, "
        "publication_date, raw_data, verification_status, summary, involved_entities, referenced_laws) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            title[:500],
            "presidential_act",
            url,
            "Президент РФ (kremlin.ru)",
            date_val,
            raw_data,
            "confirmed",
            description,
            involved_json,
            laws_json,
        ),
    )
    return cur.lastrowid


def collect_kremlin_acts(settings=None, pages: int = 10, fetch_details: bool = False,
                          detail_limit: int = 10, queries: List[str] = None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    _ensure_source(conn, "Президент РФ (kremlin.ru)", BASE_HTTPS, "acts")
    conn.commit()

    total = 0

    if queries:
        for q in queries:
            acts = search_acts_bank(session, query=q, pages=2)
            log.info("Kremlin query=%s: found %d acts", q, len(acts))
            for act in acts:
                detail = None
                if fetch_details and act.get("url") and total < detail_limit:
                    detail = scrape_act_detail(session, act["url"])
                    time.sleep(0.5)
                aid = store_act(conn, act, detail)
                if aid:
                    total += 1
            conn.commit()
    else:
        acts = search_acts_bank(session, pages=pages)
        log.info("Kremlin: found %d acts total", len(acts))
        for act in acts:
            detail = None
            if fetch_details and act.get("url") and total < detail_limit:
                detail = scrape_act_detail(session, act["url"])
                time.sleep(0.5)
            aid = store_act(conn, act, detail)
            if aid:
                total += 1
        conn.commit()

    conn.close()
    log.info("Kremlin: %d acts stored", total)
    return total


def collect_government_news(settings=None, pages: int = 5):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    _ensure_source(conn, "Правительство РФ (government.ru)", GOV_HTTPS, "news")
    conn.commit()

    from bs4 import BeautifulSoup
    total = 0

    for page in range(1, pages + 1):
        try:
            url_https = f"{GOV_HTTPS}/news/{page}/" if page > 1 else f"{GOV_HTTPS}/news/"
            url_http = f"{GOV_HTTP}/news/{page}/" if page > 1 else f"{GOV_HTTP}/news/"
            r = _get_with_fallback(session, url_https, url_http)
            if not r or r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, "lxml")

            items = []
            for link in soup.select("h2 a, h3 a, .news-item a, .entry-title a, a[href*='/news/']"):
                title = link.get_text(strip=True)
                href = link.get("href", "")
                if title and len(title) > 10 and href:
                    full_url = href if href.startswith("http") else GOV_HTTPS + href
                    if "/news/" in full_url and full_url not in [i["url"] for i in items]:
                        items.append({"title": title, "url": full_url})

            for item in items:
                existing = conn.execute(
                    "SELECT id FROM investigative_materials WHERE url=? AND material_type=?",
                    (item["url"], "government_decision"),
                ).fetchone()
                if existing:
                    continue

                conn.execute(
                    "INSERT INTO investigative_materials(title, material_type, url, source_org, verification_status) VALUES(?,?,?,?,?)",
                    (item["title"][:500], "government_decision", item["url"],
                     "Правительство РФ (government.ru)", "confirmed"),
                )
                total += 1

            log.info("Government news page %d: found %d", page, len(items))
            if not items:
                break
            time.sleep(0.3)
        except Exception as e:
            log.warning("Government news page %d failed: %s", page, e)
            continue

    conn.commit()
    conn.close()
    log.info("Government: %d news stored", total)
    return total


def collect_pravo_acts(settings=None, pages: int = 5):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    _ensure_source(conn, "pravo.gov.ru", PRAVO_HTTPS, "acts")
    conn.commit()

    from bs4 import BeautifulSoup
    total = 0

    try:
        r = _get_with_fallback(session, PRAVO_HTTPS, PRAVO_HTTP)
        if not r or r.status_code != 200:
            log.warning("pravo.gov.ru returned %s", "no response" if not r else r.status_code)
            conn.close()
            return 0

        soup = BeautifulSoup(r.text, "lxml")
        items = []

        for link in soup.select("a[href]"):
            href = link.get("href", "")
            title = link.get_text(strip=True)
            if href and title and len(title) > 10:
                full_url = href if href.startswith("http") else PRAVO_HTTPS + href
                if "document" in href.lower() or "act" in href.lower() or "signature" in href.lower():
                    items.append({"title": title, "url": full_url})

        for item in items:
            existing = conn.execute(
                "SELECT id FROM investigative_materials WHERE url=? AND material_type=?",
                (item["url"], "legal_act_publication"),
            ).fetchone()
            if existing:
                continue

            conn.execute(
                "INSERT INTO investigative_materials(title, material_type, url, source_org, verification_status) VALUES(?,?,?,?,?)",
                (item["title"][:500], "legal_act_publication", item["url"],
                 "pravo.gov.ru", "confirmed"),
            )
            total += 1

    except Exception as e:
        log.warning("pravo.gov.ru failed: %s", e)

    conn.commit()
    conn.close()
    log.info("Pravo: %d acts stored", total)
    return total


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Collect from government sites via HTTP")
    parser.add_argument("--kremlin", action="store_true")
    parser.add_argument("--government", action="store_true")
    parser.add_argument("--pravo", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--pages", type=int, default=10)
    parser.add_argument("--fetch-details", action="store_true", help="Fetch full text of acts")
    parser.add_argument("--detail-limit", type=int, default=20, help="Max detail pages to fetch")
    args = parser.parse_args()

    if not args.kremlin and not args.government and not args.pravo:
        args.all = True

    if args.all or args.kremlin:
        count = collect_kremlin_acts(pages=args.pages, fetch_details=args.fetch_details,
                                      detail_limit=args.detail_limit)
        print(f"Kremlin: {count} acts")

    if args.all or args.government:
        count = collect_government_news(pages=args.pages)
        print(f"Government: {count} news")

    if args.all or args.pravo:
        count = collect_pravo_acts(pages=args.pages)
        print(f"Pravo: {count} acts")


if __name__ == "__main__":
    main()
