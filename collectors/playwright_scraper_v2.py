import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}


def _pw_fetch_page(url: str, wait_seconds: int = 5, selector: str = None,
                    headless: bool = True) -> Optional[Dict]:
    try:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=HEADERS["User-Agent"],
            locale="ru-RU",
            ignore_https_errors=True,
        )
        page = context.new_page()
        page.route("**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2,ttf,eot,css}",
                    lambda route: route.abort())

        page.goto(url, wait_until="domcontentloaded", timeout=30000)

        if selector:
            try:
                page.wait_for_selector(selector, timeout=wait_seconds * 1000)
            except Exception:
                log.warning("Selector %s not found within %ds for %s", selector, wait_seconds, url)
        else:
            time.sleep(wait_seconds)

        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            log.warning("networkidle timeout for %s — continuing with current DOM", url)

        html = page.content()
        title = page.title()

        browser.close()
        pw.stop()

        return {"html": html, "title": title, "url": url}
    except Exception as e:
        log.error("Playwright fetch failed for %s: %s", url, e)
        try:
            browser.close()
            pw.stop()
        except Exception:
            pass
        return None


def scrape_duma_bills_playwright(queries: List[str] = None, pages: int = 3,
                                  detail_limit: int = 30, headless: bool = True) -> List[Dict]:
    from bs4 import BeautifulSoup

    if queries is None:
        queries = ["жкх", "собствен", "жиль", "реестр", "ндс", "налог", "бюджет",
                    "коррупц", "мошенничеств", "иноагент", "цензур", "блокир"]

    all_bills = []
    seen_numbers = set()

    for query in queries:
        for page_num in range(1, pages + 1):
            url = f"https://sozd.duma.gov.ru/oz/b?b%5BAnnotation%5D={query}&page={page_num}"
            log.info("PW fetching Duma bills: query=%s page=%d", query, page_num)

            result = _pw_fetch_page(url, wait_seconds=5, selector="table.tbl_search_results a")
            if not result:
                continue

            soup = BeautifulSoup(result["html"], "lxml")
            tables = soup.select("table.tbl_search_results")

            page_bills = 0
            for table in tables:
                for row in table.select("tr"):
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

                    if number in seen_numbers:
                        continue
                    seen_numbers.add(number)

                    info_text = " ".join(cells[1].get_text(" ", strip=True).split())
                    status = ""
                    title_text = re.sub(rf"^{re.escape(number)}\s*", "", info_text).strip()
                    for known_status in ("На рассмотрении", "В архиве", "Снят с рассмотрения", "Принят"):
                        if title_text.startswith(known_status):
                            status = known_status
                            title_text = title_text[len(known_status):].strip()
                            break

                    reg_date = cells[2].get_text(" ", strip=True)
                    sponsor = cells[3].get_text(" ", strip=True)
                    last_event = cells[4].get_text(" ", strip=True)
                    last_event_date = cells[5].get_text(" ", strip=True)
                    link = f"https://sozd.duma.gov.ru{href}" if href.startswith("/") else href

                    all_bills.append({
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
                    page_bills += 1

            log.info("Duma PW: query=%s page=%d found=%d", query, page_num, page_bills)
            if page_bills == 0:
                break
            time.sleep(1)

    return all_bills


def scrape_duma_bill_detail_pw(duma_url: str, headless: bool = True) -> Dict:
    from bs4 import BeautifulSoup

    result = _pw_fetch_page(duma_url, wait_seconds=5, selector="table.table-hover, span.oz_naimen")
    if not result:
        return {"duma_url": duma_url}

    soup = BeautifulSoup(result["html"], "lxml")
    detail = {"duma_url": duma_url}

    title_el = soup.select_one("span.oz_naimen")
    if title_el:
        detail["title"] = title_el.get_text(strip=True)
    else:
        p_desc = soup.select_one("p.text-justif")
        if p_desc:
            detail["title"] = p_desc.get_text(strip=True)

    annotation_el = soup.select_one("p.text-justif")
    if annotation_el and annotation_el != title_el:
        text = annotation_el.get_text(strip=True)
        if len(text) > 50:
            detail["annotation"] = text[:5000]

    committee_el = soup.select_one("[class*='committee']")
    if committee_el:
        detail["committee"] = committee_el.get_text(strip=True)

    sponsors = []
    pass_table = soup.select_one("table.table-hover, table.table-striped")
    if pass_table:
        for row in pass_table.select("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)
                if "субъект" in label and "законодательн" in label:
                    for m in re.finditer(r"([А-ЯЁ]\.[А-ЯЁ]\.[А-ЯЁ][а-яё]+)", value):
                        sponsors.append({"name": m.group(1).strip(), "faction": "", "profile_url": ""})
                    if not sponsors:
                        for m in re.finditer(r"([А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.[А-ЯЁ]\.(?:\s+[А-ЯЁ][а-яё]+)?)", value):
                            sponsors.append({"name": m.group(1).strip(), "faction": "", "profile_url": ""})
                    if not sponsors:
                        is_collective = any(kw in value for kw in ("Правительство", "Президент", "Совет Фед", "Группа депутатов"))
                        sponsors.append({"name": value, "faction": "", "profile_url": "", "is_collective": is_collective})
                elif "форма" in label:
                    detail["bill_type"] = value

    if not sponsors:
        opch_r = soup.select_one("div.opch_r")
        if opch_r:
            text = opch_r.get_text(strip=True)
            for m in re.finditer(r"([А-ЯЁ]\.[А-ЯЁ]\.[А-ЯЁ][а-яё]+)", text):
                sponsors.append({"name": m.group(1).strip(), "faction": "", "profile_url": ""})

    if not sponsors:
        for a in soup.select("a[href*='/deputies/'], a[href*='/persons/']"):
            name = a.get_text(strip=True)
            if name and len(name) > 5 and " " in name:
                sponsors.append({"name": name, "faction": "", "profile_url": a.get("href", "")})

    detail["sponsors"] = sponsors

    stages = []
    seen_stage_names = set()
    first_tab = soup.select_one("div.tab-pane.active, div.bh_histras")
    stage_container = first_tab if first_tab else soup
    for stage_el in stage_container.select("div.root-stage"):
        ttl_el = stage_el.select_one("div.ttl")
        stage_name = ttl_el.get_text(strip=True) if ttl_el else ""
        if stage_name in seen_stage_names:
            continue
        seen_stage_names.add(stage_name)
        events = []
        for ev in stage_el.select("div.oz_event"):
            ev_text = ev.get_text(" ", strip=True)
            date_match = re.search(r"(\d{2}\.\d{2}\.\d{4})", ev_text)
            ev_date = date_match.group(1) if date_match else ""
            desc_el = ev.select_one(".bh_etap_txt, [class*='etap_txt']")
            ev_desc = desc_el.get_text(strip=True) if desc_el else ev_text[:200]
            events.append({"event": ev_desc, "date": ev_date})
        if stage_name:
            stages.append({"stage": stage_name, "events": events})

    if not stages:
        for ev in soup.select("div.oz_event"):
            ev_text = ev.get_text(" ", strip=True)
            date_match = re.search(r"(\d{2}\.\d{2}\.\d{4})", ev_text)
            ev_date = date_match.group(1) if date_match else ""
            stages.append({"stage": ev_text[:200], "date": ev_date})

    detail["stages"] = stages

    keywords = []
    kw_section = soup.select_one("div.bill-keywords, div.keywords-section, [class*='keyword-list']")
    if kw_section:
        for tag_el in kw_section.select("a, span"):
            kw = tag_el.get_text(strip=True)
            if kw and len(kw) < 50:
                keywords.append(kw)
    detail["keywords"] = keywords

    return detail


def scrape_deputies_list_pw(headless: bool = True, max_pages: int = 10) -> List[Dict]:
    from bs4 import BeautifulSoup

    deputies = []
    for page in range(1, max_pages + 1):
        url = f"https://duma.gov.ru/deputies/page/{page}/"
        log.info("PW fetching deputies page %d", page)

        result = _pw_fetch_page(url, wait_seconds=3)
        if not result:
            break

        soup = BeautifulSoup(result["html"], "lxml")
        items = soup.select(".deputy-card, .deputy-item, .person-card, article, .person")

        if not items:
            items = soup.select("a[href*='/deputies/']")

        page_deps = 0
        for item in items:
            name = ""
            href = ""
            faction = ""
            region = ""

            link_el = item if item.name == "a" else item.find("a")
            if link_el:
                name = link_el.get_text(strip=True)
                href = link_el.get("href", "")

            if not name:
                h = item.select_one("h2, h3, h4, .name, .title")
                if h:
                    name = h.get_text(strip=True)

            faction_el = item.select_one("[class*='faction'], [class*='party'], [class*='fraction']")
            if faction_el:
                faction = faction_el.get_text(strip=True)

            region_el = item.select_one("[class*='region'], [class*='okrug']")
            if region_el:
                region = region_el.get_text(strip=True)

            if name and len(name) > 3 and "Депутаты" not in name:
                deputies.append({
                    "name": name,
                    "faction": faction,
                    "region": region,
                    "profile_url": f"https://duma.gov.ru{href}" if href.startswith("/") else href,
                })
                page_deps += 1

        log.info("PW deputies page %d: found %d", page, page_deps)
        if page_deps == 0 and page > 1:
            break
        time.sleep(1)

    return deputies


def collect_bills_playwright(settings=None, queries=None, pages=3, detail_limit=30, headless=True):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)

    bills = scrape_duma_bills_playwright(queries=queries, pages=pages, detail_limit=detail_limit, headless=headless)
    log.info("PW: found %d bills total", len(bills))

    from collectors.duma_bills_scraper import store_bill
    stored = 0
    for bill in bills:
        detail = None
        duma_url = bill.get("duma_url", "")
        if duma_url and stored < detail_limit:
            detail = scrape_duma_bill_detail_pw(duma_url, headless=headless)
            time.sleep(0.5)

        bill_id = store_bill(conn, bill, detail)
        if bill_id:
            stored += 1

        if stored % 20 == 0:
            conn.commit()

    conn.commit()
    log.info("PW bills: %d stored", stored)
    conn.close()
    return stored


def collect_deputies_playwright(settings=None, headless=True, max_pages=10):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)

    deputies = scrape_deputies_list_pw(headless=headless, max_pages=max_pages)
    log.info("PW: found %d deputies", len(deputies))

    from collectors.deputy_profiles_scraper import ingest_deputies
    count = ingest_deputies(deputies, conn, fetch_details=False)
    conn.commit()
    conn.close()
    return count


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--bills", action="store_true")
    parser.add_argument("--deputies", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--pages", type=int, default=3)
    parser.add_argument("--no-headless", action="store_true")
    args = parser.parse_args()

    if not args.bills and not args.deputies and not args.all:
        args.all = True

    headless = not args.no_headless

    if args.all or args.bills:
        count = collect_bills_playwright(pages=args.pages, headless=headless)
        print(f"Bills: {count}")

    if args.all or args.deputies:
        count = collect_deputies_playwright(headless=headless)
        print(f"Deputies: {count}")


if __name__ == "__main__":
    main()
