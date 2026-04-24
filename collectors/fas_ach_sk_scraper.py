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

FAS_BASE = "https://fas.gov.ru"
ACH_BASE = "https://ach.gov.ru"
SK_BASE = "https://sledcom.ru"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}


def _session():
    import requests
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _ensure_entity(conn, entity_type: str, name: str, description: str = "") -> int:
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type=? AND canonical_name=?",
        (entity_type, name),
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO entities(entity_type, canonical_name, description) VALUES(?,?,?)",
        (entity_type, name, description),
    )
    return cur.lastrowid


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


def _store_material(conn, title: str, material_type: str, url: str,
                     source_org: str, summary: str = "", publication_date: str = "",
                     involved_entities: str = "", referenced_laws: str = "",
                     source_credibility: str = "A", verification_status: str = "confirmed",
                     raw_data: str = "") -> Optional[int]:
    existing = conn.execute(
        "SELECT id FROM investigative_materials WHERE url=? AND material_type=?",
        (url, material_type),
    ).fetchone()
    if existing:
        if summary or raw_data:
            conn.execute(
                "UPDATE investigative_materials SET summary=?, raw_data=?, publication_date=? WHERE id=?",
                (summary or "", raw_data, publication_date, existing[0]),
            )
        return existing[0]

    cur = conn.execute(
        "INSERT INTO investigative_materials(title, material_type, summary, url, source_org, "
        "source_credibility, verification_status, publication_date, involved_entities, "
        "referenced_laws, raw_data) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (title[:500], material_type, summary[:2000], url, source_org,
         source_credibility, verification_status, publication_date,
         involved_entities, referenced_laws, raw_data),
    )
    return cur.lastrowid


def _extract_inn(text: str) -> List[str]:
    return re.findall(r"ИНН[:\s]*(\d{10}|\d{12})", text)


def _extract_articles(text: str) -> List[str]:
    articles = []
    for m in re.finditer(r"ст(?:ать[а-яё]*)?[\.\s]*(\d[\d.]*)\s*(?:ч\.\s*(\d))?\s*(?:УК\s*РФ|КоАП\s*РФ|КоАП)", text, re.IGNORECASE):
        articles.append(m.group(0).strip())
    return articles


def _extract_persons(text: str) -> List[str]:
    persons = []
    for m in re.finditer(r"([А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.[А-ЯЁ]\.(?:\s+[А-ЯЁ][а-яё]+)?)", text):
        persons.append(m.group(1))
    for m in re.finditer(r"([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+)", text):
        if m.group(1) not in persons:
            persons.append(m.group(1))
    return persons


# ===== FAS =====

def scrape_fas_news(session, pages: int = 5) -> List[Dict]:
    from bs4 import BeautifulSoup
    items = []
    for page in range(1, pages + 1):
        try:
            url = f"{FAS_BASE}/p/news" if page == 1 else f"{FAS_BASE}/p/news?page={page}"
            r = session.get(url, timeout=20)
            if r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, "lxml")

            for a in soup.select("a[href*='/news/']"):
                href = a.get("href", "")
                text = a.get_text(strip=True)
                if not text or len(text) < 10 or not href:
                    continue
                full_url = href if href.startswith("http") else FAS_BASE + href
                if full_url not in [i["url"] for i in items]:
                    items.append({"title": text, "url": full_url, "source": "fas"})

            if not items:
                break
            time.sleep(0.3)
        except Exception as e:
            log.warning("FAS news page %d failed: %s", page, e)
    return items


def scrape_fas_news_detail(session, url: str) -> Dict:
    from bs4 import BeautifulSoup
    detail = {"url": url}
    try:
        r = session.get(url, timeout=15)
        if r.status_code != 200:
            return detail
        soup = BeautifulSoup(r.text, "lxml")

        content = soup.select_one("article, .news-content, .content, .text, main")
        if not content:
            content = soup.select_one("body")
        if content:
            text = content.get_text("\n", strip=True)
            detail["full_text"] = text[:30000]

        date_el = soup.select_one("time, .date, .news-date, [class*='date']")
        if date_el:
            detail["date"] = date_el.get_text(strip=True)[:30]

        detail["inn"] = _extract_inn(detail.get("full_text", ""))
        detail["articles"] = _extract_articles(detail.get("full_text", ""))
        detail["persons"] = _extract_persons(detail.get("full_text", ""))

    except Exception as e:
        log.warning("FAS detail failed for %s: %s", url, e)
    return detail


def collect_fas(settings=None, pages: int = 5, fetch_details: bool = True, detail_limit: int = 30):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    _ensure_source(conn, "ФАС России (fas.gov.ru)", FAS_BASE, "fas_decisions")
    conn.commit()

    news = scrape_fas_news(session, pages=pages)
    log.info("FAS: found %d news items", len(news))

    stored = 0
    for i, item in enumerate(news):
        detail = {}
        if fetch_details and i < detail_limit:
            detail = scrape_fas_news_detail(session, item["url"])
            time.sleep(0.5)

        inn_list = detail.get("inn", [])
        articles = detail.get("articles", [])
        persons = detail.get("persons", [])

        involved = json.dumps(persons, ensure_ascii=False) if persons else ""
        laws = json.dumps(articles, ensure_ascii=False) if articles else ""

        mid = _store_material(
            conn, title=item["title"], material_type="fas_decision",
            url=item["url"], source_org="ФАС России (fas.gov.ru)",
            summary=detail.get("full_text", "")[:2000],
            publication_date=detail.get("date", ""),
            involved_entities=involved, referenced_laws=laws,
            source_credibility="A", verification_status="confirmed",
            raw_data=json.dumps({**item, **detail}, ensure_ascii=False, default=str),
        )
        if mid:
            stored += 1

        for inn in inn_list:
            org_eid = _ensure_entity(conn, "organization", f"Организация ИНН {inn}", f"Организация ИНН {inn}")
            conn.execute("UPDATE entities SET inn=? WHERE id=?", (inn, org_eid))

    conn.commit()
    conn.close()
    log.info("FAS: %d materials stored", stored)
    return stored


# ===== Счётная палата =====

def scrape_ach_audits(session, pages: int = 3) -> List[Dict]:
    from bs4 import BeautifulSoup
    items = []
    for url in [f"{ACH_BASE}/audit/", f"{ACH_BASE}/checks/"]:
        try:
            r = session.get(url, timeout=20)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "lxml")

            for a in soup.select("a[href*='/audit/'], a[href*='/checks/']"):
                href = a.get("href", "")
                text = a.get_text(strip=True)
                if not text or len(text) < 10 or not href:
                    continue
                if any(skip in text.lower() for skip in ["все проверки", "все аудиты", "операционный"]):
                    continue
                full_url = href if href.startswith("http") else ACH_BASE + href
                if full_url not in [i["url"] for i in items]:
                    items.append({"title": text, "url": full_url, "source": "ach"})

            time.sleep(0.3)
        except Exception as e:
            log.warning("ACH page failed: %s", e)

    news_items = []
    try:
        r = session.get(f"{ACH_BASE}/news/", timeout=20)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.select("a[href*='/news/']"):
                href = a.get("href", "")
                text = a.get_text(strip=True)
                if text and len(text) > 10 and href:
                    full_url = href if href.startswith("http") else ACH_BASE + href
                    if full_url not in [i["url"] for i in items + news_items]:
                        news_items.append({"title": text, "url": full_url, "source": "ach_news"})
    except Exception:
        pass

    items.extend(news_items[:20])
    return items


def scrape_ach_detail(session, url: str) -> Dict:
    from bs4 import BeautifulSoup
    detail = {"url": url}
    try:
        r = session.get(url, timeout=15)
        if r.status_code != 200:
            return detail
        soup = BeautifulSoup(r.text, "lxml")

        content = soup.select_one("article, .content, .text, main")
        if not content:
            content = soup.select_one("body")
        if content:
            text = content.get_text("\n", strip=True)
            detail["full_text"] = text[:30000]

        detail["persons"] = _extract_persons(detail.get("full_text", ""))
        detail["articles"] = _extract_articles(detail.get("full_text", ""))

    except Exception as e:
        log.warning("ACH detail failed for %s: %s", url, e)
    return detail


def collect_ach(settings=None, fetch_details: bool = True, detail_limit: int = 20):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    _ensure_source(conn, "Счётная палата (ach.gov.ru)", ACH_BASE, "audit_reports")
    conn.commit()

    items = scrape_ach_audits(session)
    log.info("ACH: found %d items", len(items))

    stored = 0
    for i, item in enumerate(items):
        detail = {}
        if fetch_details and i < detail_limit:
            detail = scrape_ach_detail(session, item["url"])
            time.sleep(0.5)

        persons = detail.get("persons", [])
        articles = detail.get("articles", [])
        involved = json.dumps(persons, ensure_ascii=False) if persons else ""
        laws = json.dumps(articles, ensure_ascii=False) if articles else ""

        mat_type = "audit_report" if item["source"] == "ach" else "ach_news"
        mid = _store_material(
            conn, title=item["title"], material_type=mat_type,
            url=item["url"], source_org="Счётная палата РФ (ach.gov.ru)",
            summary=detail.get("full_text", "")[:2000],
            involved_entities=involved, referenced_laws=laws,
            source_credibility="A", verification_status="confirmed",
            raw_data=json.dumps({**item, **detail}, ensure_ascii=False, default=str),
        )
        if mid:
            stored += 1

    conn.commit()
    conn.close()
    log.info("ACH: %d materials stored", stored)
    return stored


# ===== СК РФ =====

def scrape_sk_news(session, pages: int = 3, news_type: str = "") -> List[Dict]:
    from bs4 import BeautifulSoup
    items = []
    for page in range(1, pages + 1):
        try:
            url = f"{SK_BASE}/news/"
            params = {}
            if news_type:
                params["type"] = news_type
            if page > 1:
                params["page"] = page
            r = session.get(url, params=params, timeout=20)
            if r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, "lxml")

            page_items = []
            for a in soup.select("a[href*='/news/item/'], a[href*='/news/?']"):
                href = a.get("href", "")
                text = a.get_text(strip=True)
                if not text or len(text) < 10:
                    continue
                full_url = href if href.startswith("http") else SK_BASE + href
                if "/news/item/" in full_url and full_url not in [i["url"] for i in items]:
                    page_items.append({"title": text, "url": full_url, "source": "sk"})

            if not page_items:
                break
            items.extend(page_items)
            time.sleep(0.3)
        except Exception as e:
            log.warning("SK news page %d failed: %s", page, e)
    return items


def scrape_sk_detail(session, url: str) -> Dict:
    from bs4 import BeautifulSoup
    detail = {"url": url}
    try:
        r = session.get(url, timeout=15)
        if r.status_code != 200:
            return detail
        soup = BeautifulSoup(r.text, "lxml")

        content = soup.select_one("article, .news-content, .content, main")
        if not content:
            content = soup.select_one("body")
        if content:
            text = content.get_text("\n", strip=True)
            detail["full_text"] = text[:30000]

        date_el = soup.select_one("time, .date, [class*='date']")
        if date_el:
            detail["date"] = date_el.get_text(strip=True)[:30]

        detail["persons"] = _extract_persons(detail.get("full_text", ""))
        detail["articles"] = _extract_articles(detail.get("full_text", ""))

    except Exception as e:
        log.warning("SK detail failed for %s: %s", url, e)
    return detail


def collect_sk(settings=None, pages: int = 3, fetch_details: bool = True, detail_limit: int = 20):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    _ensure_source(conn, "Следственный комитет (sledcom.ru)", SK_BASE, "investigation_reports")
    conn.commit()

    items = scrape_sk_news(session, pages=pages, news_type="corrupt")
    if not items:
        items = scrape_sk_news(session, pages=pages)
    log.info("СК: found %d news items", len(items))

    stored = 0
    for i, item in enumerate(items):
        detail = {}
        if fetch_details and i < detail_limit:
            detail = scrape_sk_detail(session, item["url"])
            time.sleep(0.5)

        persons = detail.get("persons", [])
        articles = detail.get("articles", [])
        involved = json.dumps(persons, ensure_ascii=False) if persons else ""
        laws = json.dumps(articles, ensure_ascii=False) if articles else ""

        mid = _store_material(
            conn, title=item["title"], material_type="investigation_report",
            url=item["url"], source_org="Следственный комитет РФ (sledcom.ru)",
            summary=detail.get("full_text", "")[:2000],
            publication_date=detail.get("date", ""),
            involved_entities=involved, referenced_laws=laws,
            source_credibility="A", verification_status="confirmed",
            raw_data=json.dumps({**item, **detail}, ensure_ascii=False, default=str),
        )
        if mid:
            stored += 1

    conn.commit()
    conn.close()
    log.info("СК: %d materials stored", stored)
    return stored


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Collect from FAS, Accounts Chamber, Investigative Committee")
    parser.add_argument("--fas", action="store_true")
    parser.add_argument("--ach", action="store_true")
    parser.add_argument("--sk", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--pages", type=int, default=5)
    parser.add_argument("--no-details", action="store_true")
    args = parser.parse_args()

    if not args.fas and not args.ach and not args.sk:
        args.all = True

    fetch = not args.no_details

    if args.all or args.fas:
        count = collect_fas(pages=args.pages, fetch_details=fetch)
        print(f"FAS: {count}")

    if args.all or args.ach:
        count = collect_ach(fetch_details=fetch)
        print(f"ACH: {count}")

    if args.all or args.sk:
        count = collect_sk(pages=args.pages, fetch_details=fetch)
        print(f"SK: {count}")


if __name__ == "__main__":
    main()
