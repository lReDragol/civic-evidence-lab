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

DUMA_API_BASE = "https://api.duma.gov.ru/api/{token}"


def _session(verify=True):
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


def _find_entity_by_name(conn, name: str) -> Optional[int]:
    if not name or len(name) < 3:
        return None
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type='person' AND canonical_name=? LIMIT 1",
        (name,),
    ).fetchone()
    if row:
        return row[0]
    clean = re.sub(r"\s+", " ", name.strip())
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type='person' AND canonical_name LIKE ? LIMIT 1",
        (f"%{clean[:20]}%",),
    ).fetchone()
    if row:
        return row[0]
    parts = clean.split()
    if len(parts) >= 2:
        alias_row = conn.execute(
            "SELECT entity_id FROM entity_aliases WHERE alias LIKE ? LIMIT 1",
            (f"{parts[0]} {parts[1][:1]}%",),
        ).fetchone()
        if alias_row:
            return alias_row[0]
    return None


def fetch_vote_sessions_api(token: str, bill_number: str = None,
                            convocation: str = "VIII") -> List[Dict]:
    import requests
    results = []
    base = DUMA_API_BASE.format(token=token)
    try:
        if bill_number:
            url = f"{base}/vote.xml"
            params = {"bill_number": bill_number}
        else:
            url = f"{base}/vote.xml"
            params = {"convocation": convocation}

        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            log.warning("Duma vote API returned %d", resp.status_code)
            return results

        import xml.etree.ElementTree as ET
        root = ET.fromstring(resp.text)

        for vote_el in root.findall(".//vote"):
            session = {
                "vote_id": vote_el.get("id", ""),
                "vote_date": vote_el.findtext("date", ""),
                "vote_subject": vote_el.findtext("subject", ""),
                "vote_stage": vote_el.findtext("stage", ""),
                "bill_number": vote_el.findtext("billNumber", bill_number or ""),
                "total_for": int(vote_el.findtext("totalFor", "0") or "0"),
                "total_against": int(vote_el.findtext("totalAgainst", "0") or "0"),
                "total_abstained": int(vote_el.findtext("totalAbstained", "0") or "0"),
                "total_absent": int(vote_el.findtext("totalAbsent", "0") or "0"),
                "total_present": int(vote_el.findtext("totalPresent", "0") or "0"),
                "result": vote_el.findtext("result", ""),
                "deputy_votes": [],
            }

            for deputy_el in vote_el.findall(".//deputy"):
                dep = {
                    "deputy_name": deputy_el.findtext("name", ""),
                    "faction": deputy_el.findtext("faction", ""),
                    "vote_result": deputy_el.findtext("result", ""),
                    "duma_id": deputy_el.get("id", ""),
                }
                if dep["deputy_name"]:
                    session["deputy_votes"].append(dep)

            results.append(session)

    except Exception as e:
        log.warning("Duma vote API failed: %s", e)
    return results


def scrape_vote_page(session, bill_url: str, timeout: int = 30) -> List[Dict]:
    from bs4 import BeautifulSoup
    results = []
    try:
        resp = session.get(bill_url, timeout=timeout)
        if resp.status_code != 200:
            return results
        soup = BeautifulSoup(resp.text, "lxml")

        vote_links = []
        for a in soup.select("a[href*='vote'], a[href*='result'], a[href*='voting']"):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if href and ("голосован" in text.lower() or "результат" in text.lower() or "принят" in text.lower()):
                vote_links.append(urljoin(bill_url, href))

        if not vote_links:
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                text = a.get_text(strip=True).lower()
                if "vote" in href.lower() and text and len(text) > 5:
                    vote_links.append(urljoin(bill_url, href))

        for vlink in vote_links[:5]:
            try:
                vresp = session.get(vlink, timeout=timeout)
                if vresp.status_code != 200:
                    continue
                vsoup = BeautifulSoup(vresp.text, "lxml")

                vote_date = ""
                date_el = vsoup.select_one(".vote-date, .date, time")
                if date_el:
                    vote_date = date_el.get_text(strip=True)
                if not vote_date:
                    dm = re.search(r"(\d{2}[./]\d{2}[./]\d{4})", vresp.text)
                    if dm:
                        vote_date = dm.group(1).replace("/", ".")

                vote_stage = ""
                for el in vsoup.select("h2, h3, h4, .vote-stage, .stage"):
                    txt = el.get_text(strip=True)
                    for stage_name in ("Первое чтение", "Второе чтение", "Третье чтение",
                                       "В целом", "Поправки", "В первом чтении",
                                       "Во втором чтении", "В третьем чтении"):
                        if stage_name.lower() in txt.lower():
                            vote_stage = stage_name
                            break
                    if vote_stage:
                        break

                totals = {"for": 0, "against": 0, "abstained": 0, "absent": 0, "present": 0}
                for row in vsoup.select(".vote-results tr, .results-table tr, .result-row"):
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).lower()
                        try:
                            val = int(re.sub(r"\D", "", cells[1].get_text(strip=True)) or "0")
                        except ValueError:
                            val = 0
                        if "за" in label:
                            totals["for"] = val
                        elif "против" in label:
                            totals["against"] = val
                        elif "воздерж" in label:
                            totals["abstained"] = val
                        elif "отсутств" in label:
                            totals["absent"] = val
                        elif "присутств" in label:
                            totals["present"] = val

                deputy_votes = []
                vote_tables = vsoup.select("table")
                for vtable in vote_tables:
                    for row in vtable.select("tr"):
                        cells = row.find_all("td")
                        if len(cells) >= 3:
                            name = cells[0].get_text(strip=True)
                            faction = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                            vote_res = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                            if name and len(name) > 3 and vote_res:
                                normalized = vote_res.lower().strip()
                                if "за" in normalized:
                                    vote_res = "за"
                                elif "против" in normalized:
                                    vote_res = "против"
                                elif "воздерж" in normalized:
                                    vote_res = "воздержался"
                                elif "не голос" in normalized:
                                    vote_res = "не голосовал"
                                elif "отсутств" in normalized:
                                    vote_res = "отсутствовал"
                                deputy_votes.append({
                                    "deputy_name": name,
                                    "faction": faction,
                                    "vote_result": vote_res,
                                })

                results.append({
                    "vote_date": vote_date,
                    "vote_stage": vote_stage,
                    "vote_url": vlink,
                    "total_for": totals["for"],
                    "total_against": totals["against"],
                    "total_abstained": totals["abstained"],
                    "total_absent": totals["absent"],
                    "total_present": totals["present"],
                    "deputy_votes": deputy_votes,
                })
                time.sleep(0.3)
            except Exception as e:
                log.warning("Vote page %s failed: %s", vlink, e)
                continue

    except Exception as e:
        log.warning("Vote scraping failed for %s: %s", bill_url, e)
    return results


def store_vote_session(conn, bill_id: int, session_data: Dict) -> int:
    vote_date = session_data.get("vote_date", "")
    vote_stage = session_data.get("vote_stage", "")
    deputy_votes = session_data.get("deputy_votes", [])

    existing = conn.execute(
        "SELECT id FROM bill_vote_sessions WHERE bill_id=? AND vote_date=? AND vote_stage=?",
        (bill_id, vote_date, vote_stage),
    ).fetchone()
    if existing:
        vs_id = existing[0]
    else:
        raw_data = json.dumps(session_data, ensure_ascii=False, default=str)
        cur = conn.execute(
            "INSERT INTO bill_vote_sessions(bill_id, vote_date, vote_stage, total_for, total_against, "
            "total_abstained, total_absent, total_present, result, raw_data) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (bill_id, vote_date, vote_stage,
             session_data.get("total_for", 0), session_data.get("total_against", 0),
             session_data.get("total_abstained", 0), session_data.get("total_absent", 0),
             session_data.get("total_present", 0), session_data.get("result", ""),
             raw_data),
        )
        vs_id = cur.lastrowid

    stored_votes = 0
    for dv in deputy_votes:
        name = dv.get("deputy_name", "").strip()
        vote_result = dv.get("vote_result", "").strip()
        faction = dv.get("faction", "").strip()
        if not name or not vote_result:
            continue

        entity_id = _find_entity_by_name(conn, name)

        existing_vote = conn.execute(
            "SELECT id FROM bill_votes WHERE vote_session_id=? AND entity_id=?",
            (vs_id, entity_id),
        ).fetchone() if entity_id else None

        if not existing_vote:
            raw = json.dumps(dv, ensure_ascii=False)
            try:
                conn.execute(
                    "INSERT INTO bill_votes(vote_session_id, entity_id, deputy_name, faction, vote_result, raw_data) VALUES(?,?,?,?,?,?)",
                    (vs_id, entity_id, name, faction, vote_result, raw),
                )
                stored_votes += 1
            except Exception:
                pass

    return stored_votes


def collect_votes_for_bills(settings: dict = None, bill_limit: int = 100,
                            use_api: bool = False, fetch_details: bool = True):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    token = settings.get("duma_api_token")
    if use_api and token:
        log.info("Using Duma API for votes")

    bills = conn.execute(
        "SELECT id, number, duma_url FROM bills WHERE duma_url IS NOT NULL AND duma_url != '' LIMIT ?",
        (bill_limit,),
    ).fetchall()

    if not bills:
        log.warning("No bills with URLs found in DB — run duma_bills_scraper first")
        conn.close()
        return 0

    total_sessions = 0
    total_votes = 0

    for bill_id, number, duma_url in bills:
        log.info("Collecting votes for bill %s (id=%d)", number, bill_id)

        if use_api and token:
            api_sessions = fetch_vote_sessions_api(token, bill_number=number)
            for vs in api_sessions:
                stored = store_vote_session(conn, bill_id, vs)
                total_votes += stored
                total_sessions += 1
        elif fetch_details and duma_url:
            html_sessions = scrape_vote_page(session, duma_url)
            for vs in html_sessions:
                stored = store_vote_session(conn, bill_id, vs)
                total_votes += stored
                total_sessions += 1
            time.sleep(0.5)

        if total_sessions % 5 == 0:
            conn.commit()

    conn.commit()
    log.info("Votes: %d sessions, %d individual votes stored", total_sessions, total_votes)
    conn.close()
    return total_votes


def collect_recent_votes(settings: dict = None, limit: int = 50):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    log.info("Scraping recent voting results from duma.gov.ru")
    vote_sessions = []

    try:
        for page in range(1, 6):
            url = f"https://duma.gov.ru/news/votes/page/{page}/"
            try:
                resp = session.get(url, timeout=20)
                if resp.status_code != 200:
                    break
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "lxml")

                items = soup.select(".vote-card, .vote-item, article, .news-item")
                if not items:
                    items = soup.select("a[href*='vote']")

                for item in items[:20]:
                    link_el = item.find("a") if item.name != "a" else item
                    if not link_el:
                        continue
                    href = link_el.get("href", "")
                    text = link_el.get_text(strip=True)
                    if href and text:
                        vote_sessions.append({
                            "subject": text,
                            "url": urljoin("https://duma.gov.ru", href),
                        })
                time.sleep(0.3)
            except Exception as e:
                log.warning("Recent votes page %d failed: %s", page, e)
                continue
    except Exception as e:
        log.error("Recent votes scraping failed: %s", e)

    total_votes = 0
    for vs_info in vote_sessions[:limit]:
        vote_url = vs_info.get("url", "")
        if not vote_url:
            continue
        try:
            vs_list = scrape_vote_page(session, vote_url)
            for vs in vs_list:
                bill_number = ""
                for m in re.finditer(r"(\d{5,}[-–]\d+[-–]\d+|\d{5,})", vs_info.get("subject", "")):
                    bill_number = m.group(1)
                    break
                bill_id = None
                if bill_number:
                    row = conn.execute("SELECT id FROM bills WHERE number=?", (bill_number,)).fetchone()
                    if row:
                        bill_id = row[0]
                if bill_id:
                    total_votes += store_vote_session(conn, bill_id, vs)
            time.sleep(0.5)
        except Exception as e:
            log.warning("Failed to process vote %s: %s", vote_url, e)
            continue

    conn.commit()
    log.info("Recent votes: %d individual votes stored", total_votes)
    conn.close()
    return total_votes


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Collect Duma voting records")
    parser.add_argument("--bill-limit", type=int, default=100, help="Max bills to fetch votes for")
    parser.add_argument("--api", action="store_true", help="Use Duma API (requires token)")
    parser.add_argument("--no-details", action="store_true", help="Skip detail page fetching")
    parser.add_argument("--recent", action="store_true", help="Collect recent votes from duma.gov.ru/news/votes/")
    args = parser.parse_args()

    settings = load_settings()

    if args.recent:
        count = collect_recent_votes(settings)
    else:
        count = collect_votes_for_bills(
            settings,
            bill_limit=args.bill_limit,
            use_api=args.api,
            fetch_details=not args.no_details,
        )
    print(f"Collected: {count} votes")


if __name__ == "__main__":
    main()
