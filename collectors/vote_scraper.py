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

BASE_URL = "https://vote.duma.gov.ru"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}


FACTION_MAP = {
    "72100024": "ЕР",
    "72100004": "КПРФ",
    "72100008": "ЛДПР",
    "72100019": "СР",
    "72100028": "НЛ",
    "72100012": "Родина",
    "72100032": "Партия роста",
    "72100036": "Гражданская платформа",
    "0": "Без фракции",
}

RESULT_MAP = {
    "for": "за",
    "against": "против",
    "abstained": "воздержался",
    "absent": "не голосовал",
    "no_vote": "не голосовал",
    "none": "отсутствовал",
}


def _parse_deputies_data(html_text: str) -> List[Dict]:
    match = re.search(r'deputiesData\s*=\s*(\[.*?\]);', html_text, re.DOTALL)
    if not match:
        return []
    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return []
    deputies = []
    for d in data:
        name = d.get("sortName", "")
        faction = d.get("faction", "")
        result_raw = d.get("result", "")
        result = RESULT_MAP.get(result_raw, result_raw)
        url = d.get("url", "")
        duma_id = None
        id_m = re.search(r'deputy=(\d+)', url)
        if id_m:
            duma_id = int(id_m.group(1))
        deputies.append({
            "name": name,
            "faction": faction,
            "result": result,
            "duma_id": duma_id,
        })
    return deputies


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
        "SELECT entity_id FROM entity_aliases WHERE alias LIKE ? LIMIT 1",
        (f"{name[:20]}%",),
    ).fetchone()
    if row:
        return row[0]
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type='person' AND canonical_name LIKE ? LIMIT 1",
        (f"%{name[:20]}%",),
    ).fetchone()
    if row:
        return row[0]
    return None


def _find_bill_by_number(conn, vote_subject: str) -> Optional[int]:
    bill_number = ""
    for m in re.finditer(r"№\s*(\d{5,}[-–]\d+[-–]\d+|\d{5,})", vote_subject):
        bill_number = m.group(1).replace("–", "-")
        break
    if not bill_number:
        return None
    row = conn.execute("SELECT id FROM bills WHERE number=?", (bill_number,)).fetchone()
    return row[0] if row else None


def scrape_vote_list(session, page: int = 1, convocation: str = "VIII") -> List[Dict]:
    from bs4 import BeautifulSoup
    votes = []
    try:
        params = {}
        if page > 1:
            params["page"] = page
        r = session.get(f"{BASE_URL}/", params=params, timeout=20)
        if r.status_code != 200:
            return votes

        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.select("a[href*='/vote/']"):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if not href or not text or len(text) < 10:
                continue
            vote_id_m = re.search(r"/vote/(\d+)", href)
            if not vote_id_m:
                continue
            vote_id = vote_id_m.group(1)
            stage = ""
            stage_m = re.match(r"\(([^)]+)\)", text)
            if stage_m:
                stage = stage_m.group(1)
                text = re.sub(r"^\([^)]+\)\s*", "", text)

            bill_number = ""
            for m in re.finditer(r"№\s*(\d{5,}[-–]\d+[-–]\d+|\d{5,})", text):
                bill_number = m.group(1).replace("–", "-")
                break

            votes.append({
                "vote_id": vote_id,
                "subject": text,
                "stage": stage,
                "bill_number": bill_number,
                "url": f"{BASE_URL}/vote/{vote_id}",
            })

    except Exception as e:
        log.warning("Vote list page %d failed: %s", page, e)
    return votes


def scrape_vote_detail(session, vote_url: str) -> Dict:
    from bs4 import BeautifulSoup
    detail = {"url": vote_url}
    try:
        r = session.get(vote_url, timeout=20)
        if r.status_code != 200:
            return detail
        soup = BeautifulSoup(r.text, "lxml")

        h1 = soup.select_one("h1")
        if h1:
            detail["subject"] = h1.get_text(strip=True)

        date_el = soup.select_one(".date-p")
        if date_el:
            date_text = date_el.get_text(strip=True)
            date_m = re.search(r"(\d{2}\.\d{2}\.\d{4})", date_text)
            detail["vote_date"] = date_m.group(1) if date_m else date_text[:20]

        stat_el = soup.select_one(".statis")
        if stat_el:
            stat_text = stat_el.get_text(strip=True)
            for m in re.finditer(r"За:\s*(\d+)", stat_text):
                detail["total_for"] = int(m.group(1))
            for m in re.finditer(r"Против:\s*(\d+)", stat_text):
                detail["total_against"] = int(m.group(1))
            for m in re.finditer(r"Воздержалось:\s*(\d+)", stat_text):
                detail["total_abstained"] = int(m.group(1))
            for m in re.finditer(r"Не голосовало:\s*(\d+)", stat_text):
                detail["total_absent"] = int(m.group(1))
            for m in re.finditer(r"Кворум:\s*(\d+)", stat_text):
                detail["quorum"] = int(m.group(1))

        faction_results = []
        for box in soup.select(".e-pers-box"):
            frac_el = box.select_one(".ld-frac-n")
            fname = frac_el.get_text(strip=True) if frac_el else ""
            if not fname:
                continue
            fr = {"faction": fname}
            for cls_name, label in [("ep-green","za"),("ep-red","protiv"),("ep-blue","vozderzhan"),("ep-gray","otsutstvoval")]:
                el = box.select_one("." + cls_name)
                if el:
                    val_text = el.get_text(strip=True)
                    num_m = re.search(r"(\d+)\s*гол", val_text)
                    fr[label] = int(num_m.group(1)) if num_m else 0
                else:
                    fr[label] = 0
            faction_results.append(fr)
        detail["faction_results"] = faction_results

        bill_number = ""
        for m in re.finditer(r"№\s*(\d{5,}[-–]\d+[-–]\d+|\d{5,})", detail.get("subject", "")):
            bill_number = m.group(1).replace("–", "-")
            break
        detail["bill_number"] = bill_number

        stage = ""
        if detail.get("subject"):
            stage_m = re.search(r"\(([^)]*(?:чтени|поправк|основу|целом)[^)]*)\)", detail["subject"])
            if stage_m:
                stage = stage_m.group(1)
        detail["stage"] = stage

        result = ""
        stat_text = stat_el.get_text(strip=True) if stat_el else ""
        if "Принят" in stat_text:
            result = "принят"
        elif "Отклонен" in stat_text:
            result = "отклонен"
        detail["result"] = result

        individual = _parse_deputies_data(r.text)
        if individual:
            detail["individual_votes"] = individual

    except Exception as e:
        log.warning("Vote detail failed for %s: %s", vote_url, e)
    return detail


def store_vote_session(conn, bill_id: Optional[int], vote_data: Dict) -> Optional[int]:
    vote_date = vote_data.get("vote_date", "")
    stage = vote_data.get("stage", "")
    url = vote_data.get("url", "")

    existing = conn.execute(
        "SELECT id FROM bill_vote_sessions WHERE bill_id=? AND vote_date=? AND vote_stage=?",
        (bill_id, vote_date, stage),
    ).fetchone() if bill_id else None

    if not existing and not bill_id:
        existing = conn.execute(
            "SELECT id FROM bill_vote_sessions WHERE vote_date=? AND vote_stage=? AND bill_id IS NULL",
            (vote_date, stage),
        ).fetchone()

    if existing:
        return existing[0]

    total_present = vote_data.get("total_for", 0) + vote_data.get("total_against", 0) + vote_data.get("total_abstained", 0)
    raw_data = json.dumps(vote_data, ensure_ascii=False, default=str)
    cur = conn.execute(
        "INSERT INTO bill_vote_sessions(bill_id, vote_date, vote_stage, total_for, total_against, "
        "total_abstained, total_absent, total_present, result, raw_data) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (bill_id, vote_date, stage,
         vote_data.get("total_for", 0), vote_data.get("total_against", 0),
         vote_data.get("total_abstained", 0), vote_data.get("total_absent", 0),
         total_present, vote_data.get("result", ""),
         raw_data),
    )
    vs_id = cur.lastrowid

    for fr in vote_data.get("faction_results", []):
        faction = fr.get("faction", "")
        if not faction:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO bill_votes(vote_session_id, deputy_name, faction, vote_result, raw_data) VALUES(?,?,?,?,?)",
            (vs_id, f"Фракция: {faction}", faction,
             f"за={fr.get('za',0)} против={fr.get('protiv',0)} воздерж={fr.get('vozderzhan',0)} отсутств={fr.get('otsutstvoval',0)}",
             json.dumps(fr, ensure_ascii=False)),
        )

    for dv in vote_data.get("individual_votes", []):
        name = dv.get("name", "")
        faction = dv.get("faction", "")
        result = dv.get("result", "")
        if not name or not result:
            continue
        entity_id = _find_entity_by_name(conn, name)
        if not entity_id and dv.get("duma_id"):
            row = conn.execute(
                "SELECT entity_id FROM deputy_profiles WHERE duma_id=?",
                (dv["duma_id"],),
            ).fetchone()
            if row:
                entity_id = row[0]
        try:
            conn.execute(
                "INSERT OR IGNORE INTO bill_votes(vote_session_id, entity_id, deputy_name, faction, vote_result, raw_data) VALUES(?,?,?,?,?,?)",
                (vs_id, entity_id, name, faction, result,
                 json.dumps(dv, ensure_ascii=False)),
            )
        except Exception as e:
            log.warning("Failed to store individual vote for %s: %s", name, e)

    return vs_id


def collect_votes(settings=None, pages: int = 10, fetch_details: bool = True):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    _ensure_source(conn, "Голосования ГД (vote.duma.gov.ru)", BASE_URL, "votes")
    conn.commit()

    total = 0
    for page in range(1, pages + 1):
        votes = scrape_vote_list(session, page=page)
        log.info("Vote list page %d: found %d votes", page, len(votes))
        if not votes:
            break

        for v in votes:
            bill_id = _find_bill_by_number(conn, v.get("subject", "")) if v.get("bill_number") else None
            if not bill_id and v.get("bill_number"):
                row = conn.execute("SELECT id FROM bills WHERE number=?", (v["bill_number"],)).fetchone()
                bill_id = row[0] if row else None

            if fetch_details:
                detail = scrape_vote_detail(session, v["url"])
                time.sleep(0.3)
            else:
                detail = v

            vs_id = store_vote_session(conn, bill_id, detail)
            if vs_id:
                total += 1

        conn.commit()
        time.sleep(0.5)

    conn.commit()
    conn.close()
    log.info("Vote sessions: %d stored", total)
    return total


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Collect Duma votes from vote.duma.gov.ru")
    parser.add_argument("--pages", type=int, default=10)
    parser.add_argument("--no-details", action="store_true")
    args = parser.parse_args()

    count = collect_votes(pages=args.pages, fetch_details=not args.no_details)
    print(f"Collected: {count} vote sessions")


if __name__ == "__main__":
    main()
