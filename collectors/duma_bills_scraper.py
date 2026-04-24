import json
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

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

DEFAULT_QUERIES = [
    "жкх", "собствен", "жиль", "реестр", "ндс", "налог", "бюджет",
    "суд", "иностран", "штраф", "закуп", "коррупц", "мошенничеств",
    "арест", "конфиск", "ликвид", "банкрот", "цензур", "блокир",
    "иноагент", "экстремист", "оппозицион", "протест", "мирн",
    "собрани", "выбор", "депутат", "законопроект", "поправк",
]


def _session():
    import requests
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


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


def _find_entity_by_name(conn, name: str) -> Optional[int]:
    if not name or len(name) < 3:
        return None
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type='person' AND canonical_name LIKE ? LIMIT 1",
        (f"%{name[:30]}%",),
    ).fetchone()
    if row:
        return row[0]
    words = name.strip().split()
    if len(words) >= 2:
        row = conn.execute(
            "SELECT entity_id FROM entity_aliases WHERE alias LIKE ? LIMIT 1",
            (f"{words[0]} {words[1][:1]}%",),
        ).fetchone()
        if row:
            return row[0]
    return None


def search_bills_list(session, query: str, pages: int = 3, timeout: int = 30) -> List[Dict]:
    from bs4 import BeautifulSoup
    bills = []
    for page in range(1, pages + 1):
        try:
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
                for known_status in ("На рассмотрении", "В архиве", "Снят с рассмотрения"):
                    if title_text.startswith(known_status):
                        status = known_status
                        title_text = title_text[len(known_status):].strip()
                        break
                reg_date = cells[2].get_text(" ", strip=True)
                sponsor = cells[3].get_text(" ", strip=True)
                last_event = cells[4].get_text(" ", strip=True)
                last_event_date = cells[5].get_text(" ", strip=True)
                link = f"https://sozd.duma.gov.ru{href}" if href.startswith("/") else href

                bills.append({
                    "number": number,
                    "title": title_text,
                    "status": status,
                    "registration_date": reg_date,
                    "sponsor_text": sponsor,
                    "last_event": last_event,
                    "last_event_date": last_event_date,
                    "duma_url": link,
                    "query": query,
                })
                page_rows += 1

            if page_rows == 0:
                break
            time.sleep(0.3)
        except Exception as e:
            log.warning("Bill search query=%s page=%d failed: %s", query, page, e)
            continue
    return bills


def scrape_bill_detail(session, duma_url: str, timeout: int = 30) -> Dict:
    from bs4 import BeautifulSoup
    detail = {"duma_url": duma_url}
    try:
        resp = session.get(duma_url, timeout=timeout)
        if resp.status_code != 200:
            return detail
        soup = BeautifulSoup(resp.text, "lxml")

        h1 = soup.select_one("h1, .bill-title, .document-title")
        if h1:
            detail["title"] = h1.get_text(strip=True)

        annotation_el = soup.select_one(".annotation, .bill-annotation, .document-annotation, .description")
        if annotation_el:
            detail["annotation"] = annotation_el.get_text("\n", strip=True)

        committee_el = soup.select_one(".committee, .bill-committee, .responsible-committee")
        if committee_el:
            detail["committee"] = committee_el.get_text(strip=True)

        sponsors = []
        sponsor_sections = soup.select(".sponsors, .bill-sponsors, .initiators, .authors")
        for section in sponsor_sections:
            for a in section.select("a[href]"):
                name = a.get_text(strip=True)
                href = a.get("href", "")
                if name and len(name) > 3:
                    faction = ""
                    parent = a.parent
                    if parent:
                        faction_el = parent.select_one(".faction, .party, .fraction")
                        if faction_el:
                            faction = faction_el.get_text(strip=True)
                    sponsors.append({
                        "name": name,
                        "faction": faction,
                        "profile_url": urljoin("https://sozd.duma.gov.ru", href),
                    })

        if not sponsors:
            for a in soup.select("a[href*='/deputies/'], a[href*='/persons/']"):
                name = a.get_text(strip=True)
                if name and len(name) > 5 and " " in name:
                    sponsors.append({"name": name, "faction": "", "profile_url": a.get("href", "")})

        if not sponsors:
            sponsor_text_el = soup.select_one(".initiators-text, .sponsors-text, .authors-text")
            if sponsor_text_el:
                text = sponsor_text_el.get_text(strip=True)
                for name_match in re.finditer(r"([А-ЯЁ][а-яё]+ [А-ЯЁ]\.[А-ЯЁ]\.)", text):
                    sponsors.append({"name": name_match.group(1), "faction": "", "profile_url": ""})

        detail["sponsors"] = sponsors

        stages = []
        for row in soup.select(".stages tr, .bill-stages tr, .passage-stages tr, .stage-row"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                stage_name = cells[0].get_text(strip=True)
                stage_date = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                stages.append({"stage": stage_name, "date": stage_date})
        detail["stages"] = stages

        keywords = []
        for tag_el in soup.select(".keywords a, .bill-tags a, .tags a, .tag"):
            kw = tag_el.get_text(strip=True)
            if kw:
                keywords.append(kw)
        detail["keywords"] = keywords

        number_match = re.search(r"(\d+[-–]\d+[-–]\d+|\d{5,})", duma_url)
        if number_match:
            detail["bill_number"] = number_match.group(1)

    except Exception as e:
        log.warning("Bill detail failed for %s: %s", duma_url, e)
    return detail


def store_bill(conn, bill_data: Dict, detail: Optional[Dict] = None) -> int:
    number = bill_data.get("bill_number") or bill_data.get("number", "")
    if not number:
        return 0

    title = bill_data.get("title", "")
    if detail and detail.get("title") and len(detail["title"]) > len(title):
        title = detail["title"]

    annotation = ""
    if detail:
        annotation = detail.get("annotation", "")

    committee = bill_data.get("committee", "")
    if detail and detail.get("committee"):
        committee = detail["committee"]

    keywords_json = None
    if detail and detail.get("keywords"):
        keywords_json = json.dumps(detail["keywords"], ensure_ascii=False)

    stages_json = None
    if detail and detail.get("stages"):
        stages_json = json.dumps(detail["stages"], ensure_ascii=False)

    raw_data = json.dumps({**bill_data, **(detail or {})}, ensure_ascii=False, default=str)

    existing = conn.execute("SELECT id FROM bills WHERE number=?", (number,)).fetchone()
    if existing:
        bill_id = existing[0]
        conn.execute(
            "UPDATE bills SET title=?, status=?, registration_date=?, duma_url=?, committee=?, "
            "keywords=?, annotation=?, raw_data=?, updated_at=? WHERE id=?",
            (title, bill_data.get("status", ""), bill_data.get("registration_date", ""),
             bill_data.get("duma_url", ""), committee, keywords_json, annotation,
             raw_data, datetime.now().isoformat(), bill_id),
        )
    else:
        cur = conn.execute(
            "INSERT INTO bills(number, title, bill_type, status, registration_date, duma_url, "
            "committee, keywords, annotation, raw_data) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (number, title, "федеральный закон", bill_data.get("status", ""),
             bill_data.get("registration_date", ""), bill_data.get("duma_url", ""),
             committee, keywords_json, annotation, raw_data),
        )
        bill_id = cur.lastrowid

    sponsors = []
    if detail and detail.get("sponsors"):
        sponsors = detail["sponsors"]
    elif bill_data.get("sponsor_text"):
        clean_text = re.sub(r"Показать еще\s*\d*", "", bill_data["sponsor_text"])
        clean_text = re.sub(r"Скрыть", "", clean_text)
        for m in re.finditer(r"([А-ЯЁ]\.[А-ЯЁ]\.[А-ЯЁ][а-яё]+)", clean_text):
            sponsors.append({"name": m.group(1).strip(), "faction": "", "profile_url": ""})
        if not sponsors:
            for m in re.finditer(r"([А-ЯЁ][а-яё]+ [А-ЯЁ]\.[А-ЯЁ]\.(?:\s+[А-ЯЁ][а-яё]+)?)", clean_text):
                sponsors.append({"name": m.group(1).strip(), "faction": "", "profile_url": ""})
        if not sponsors:
            for m in re.finditer(r"([А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+)?)", clean_text):
                sponsors.append({"name": m.group(1).strip(), "faction": "", "profile_url": ""})

    for sp in sponsors:
        name = sp.get("name", "").strip()
        if not name or len(name) < 5:
            continue
        if any(bad in name for bad in ("Скрыть", "Показать", "еще")):
            continue
        entity_id = _find_entity_by_name(conn, name)
        faction = sp.get("faction", "")
        is_collective = 1 if any(kw in name for kw in ("Правительство", "Президент", "Совет Фед", "Группа депутатов")) else 0

        existing_sp = conn.execute(
            "SELECT id FROM bill_sponsors WHERE bill_id=? AND sponsor_name=?",
            (bill_id, name),
        ).fetchone()
        if not existing_sp:
            conn.execute(
                "INSERT INTO bill_sponsors(bill_id, entity_id, sponsor_name, sponsor_role, faction, is_collective) VALUES(?,?,?,?,?,?)",
                (bill_id, entity_id, name, "sponsor" if not is_collective else "collective_sponsor", faction, is_collective),
            )

    return bill_id


def collect_bills(settings: dict = None, queries: Optional[List[str]] = None,
                  pages: int = 3, fetch_details: bool = True, detail_limit: int = 50):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    _ensure_source(conn, "Государственная Дума — Законопроекты", "https://sozd.duma.gov.ru", "bills")
    conn.commit()

    if queries is None:
        queries = DEFAULT_QUERIES

    total_bills = 0
    total_details = 0
    all_bill_numbers = set()

    for query in queries:
        try:
            bills = search_bills_list(session, query, pages=pages)
            log.info("Query '%s': found %d bills", query, len(bills))

            for bill in bills:
                number = bill.get("number", "")
                if number in all_bill_numbers:
                    continue
                all_bill_numbers.add(number)

                detail = None
                duma_url = bill.get("duma_url", "")
                if fetch_details and duma_url and total_details < detail_limit:
                    detail = scrape_bill_detail(session, duma_url)
                    total_details += 1
                    time.sleep(0.3)

                bill_id = store_bill(conn, bill, detail)
                if bill_id:
                    total_bills += 1

            if total_bills % 20 == 0:
                conn.commit()
        except Exception as e:
            log.warning("Bill collection failed for query '%s': %s", query, e)
            continue

    conn.commit()
    log.info("Bills: %d stored, %d details fetched", total_bills, total_details)
    conn.close()
    return total_bills


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Collect bills from sozd.duma.gov.ru")
    parser.add_argument("--queries", nargs="+", help="Search queries")
    parser.add_argument("--pages", type=int, default=3)
    parser.add_argument("--no-details", action="store_true", help="Skip detail page fetching")
    parser.add_argument("--detail-limit", type=int, default=50, help="Max detail pages to fetch")
    args = parser.parse_args()

    settings = load_settings()
    count = collect_bills(
        settings,
        queries=args.queries,
        pages=args.pages,
        fetch_details=not args.no_details,
        detail_limit=args.detail_limit,
    )
    print(f"Collected: {count} bills")


if __name__ == "__main__":
    main()
