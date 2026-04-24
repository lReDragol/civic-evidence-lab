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

BASE_URL = "https://council.gov.ru"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}


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


def _ensure_entity(conn, name: str, entity_type: str = "person") -> int:
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type=? AND canonical_name=?",
        (entity_type, name),
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO entities(entity_type, canonical_name) VALUES(?,?)",
        (entity_type, name),
    )
    return cur.lastrowid


def _ensure_alias(conn, entity_id: int, alias: str, alias_type: str = "spelling"):
    if not alias or len(alias) < 3:
        return
    existing = conn.execute(
        "SELECT id FROM entity_aliases WHERE entity_id=? AND alias=?",
        (entity_id, alias),
    ).fetchone()
    if existing:
        return
    conn.execute(
        "INSERT OR IGNORE INTO entity_aliases(entity_id, alias, alias_type) VALUES(?,?,?)",
        (entity_id, alias, alias_type),
    )


def scrape_senators_list(session) -> List[Dict]:
    from bs4 import BeautifulSoup
    senators = []
    try:
        r = session.get(f"{BASE_URL}/structure/members/", timeout=20)
        if r.status_code != 200:
            log.error("Senators list page returned %d", r.status_code)
            return senators

        soup = BeautifulSoup(r.text, "lxml")

        for a in soup.select("a.group__persons__item"):
            name_el = a.select_one("span.group__persons__item__title")
            name = name_el.get_text(strip=True) if name_el else a.get_text(strip=True)
            href = a.get("href", "")
            data_title = a.get("data-title", "")
            region_id = a.get("data-region-id", "")
            region_title = a.get("data-region-title", "")
            photo_el = a.select_one("img.group__persons__item__photo")
            photo_url = photo_el.get("src", "") if photo_el else ""

            if not name or not href:
                continue

            senators.append({
                "name": name,
                "data_title": data_title,
                "href": href,
                "region_id": region_id,
                "region_title": region_title,
                "photo_url": photo_url,
                "profile_url": f"{BASE_URL}{href}" if href.startswith("/") else href,
            })

        log.info("Senators list: found %d entries", len(senators))
    except Exception as e:
        log.error("Failed to fetch senators list: %s", e)
    return senators


def scrape_senator_profile(session, profile_url: str, retries: int = 3) -> Dict:
    from bs4 import BeautifulSoup
    detail = {"url": profile_url}
    r = None
    for attempt in range(retries):
        try:
            r = session.get(profile_url, timeout=15)
            if r.status_code == 200:
                break
            if r.status_code == 403 and attempt < retries - 1:
                wait = 3 * (attempt + 1)
                log.warning("Senator profile %s returned 403, retry %d/%d in %ds", profile_url, attempt + 1, retries, wait)
                time.sleep(wait)
                continue
            log.warning("Senator profile %s returned %d", profile_url, r.status_code)
            return detail
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(3)
                continue
            log.warning("Senator profile %s failed: %s", profile_url, e)
            return detail

    if r is None or r.status_code != 200:
        return detail

    try:
        soup = BeautifulSoup(r.text, "lxml")

        h1 = soup.select_one("h1")
        if h1:
            h1_text = h1.get_text(" ", strip=True)
            detail["h1"] = h1_text

            if "Председатель Совета Федерации" in h1_text:
                detail["position"] = "Председатель Совета Федерации"
            elif "Заместитель Председателя Совета Федерации" in h1_text:
                detail["position"] = "Заместитель Председателя Совета Федерации"
            elif "Председатель" in h1_text and "Совета Федерации" in h1_text:
                detail["position"] = "Председатель комитета Совета Федерации"
            elif "Сенатор" in h1_text or "сенатор" in h1_text:
                detail["position"] = "Сенатор Российской Федерации"
            elif "Представитель" in h1_text:
                detail["position"] = "Представитель от субъекта РФ"
            else:
                detail["position"] = h1_text[:100]

        photo = soup.select_one("img[src*='/media/persons/']")
        if photo:
            src = photo.get("src", "")
            if src.startswith("//"):
                src = "https:" + src
            elif src.startswith("/"):
                src = f"{BASE_URL}{src}"
            detail["photo_url"] = src

        committee_links = []
        for a in soup.select("a[href*='/structure/committees/']"):
            text = a.get_text(strip=True)
            href = a.get("href", "")
            if text:
                committee_links.append({"name": text, "url": f"{BASE_URL}{href}" if href.startswith("/") else href})
        if committee_links:
            detail["committees"] = committee_links
            detail["committee"] = committee_links[0]["name"]

        commission_links = []
        for a in soup.select("a[href*='/structure/commissions/']"):
            text = a.get_text(strip=True)
            href = a.get("href", "")
            if text:
                commission_links.append({"name": text, "url": f"{BASE_URL}{href}" if href.startswith("/") else href})
        if commission_links:
            detail["commissions"] = commission_links

        main_content = soup.select_one("main, .main__content_wrapper")
        if main_content:
            main_text = main_content.get_text(" ", strip=True)

            party = ""
            for party_name in ["Единая Россия", "КПРФ", "ЛДПР", "Справедливая Россия",
                               "Новые люди", "Родина", "Партия роста", "Гражданская платформа",
                               "Яблоко", "Коммунисты России", "Зеленые", "Пенсионеры"]:
                if party_name in main_text:
                    party = party_name
                    break
            detail["party"] = party

            term_m = re.search(r"срок\s*полномочий[^а-яё]*?(?:с|от)?\s*(\d{1,2}[\.\s]\w+[\.\s]\d{4})\s*(?:по|до)?\s*(\d{1,2}[\.\s]\w+[\.\s]\d{4})?",
                               main_text, re.IGNORECASE)
            if term_m:
                detail["term_start"] = term_m.group(1) if term_m.group(1) else ""
                detail["term_end"] = term_m.group(2) if term_m.group(2) else ""

            date_m = re.search(r"(?:назначен|избран|надел[ёе]н)\s*(?:Указом\s*Президента)?\s*(?:РФ)?\s*(?:от|от\s*\d{2})\s*(\d{1,2}\s+\w+\s+\d{4})",
                               main_text, re.IGNORECASE)
            if date_m:
                detail["date_appointed"] = date_m.group(1)

            decree_m = re.search(r"Указ\s*Президента\s*РФ\s*(?:от\s*)?(\d{1,2}\s+\w+\s+\d{4})\s*[№N]\s*([\d\-]+)",
                                 main_text, re.IGNORECASE)
            if decree_m:
                detail["appointment_decree_date"] = decree_m.group(1)
                detail["appointment_decree_number"] = decree_m.group(2)

            income_m = re.search(r"доход[^)]*?(\d[\d\s,.]*?(?:руб|тыс|млн))", main_text, re.IGNORECASE)
            if income_m:
                detail["income_mention"] = income_m.group(1).strip()

        for h in soup.select("h2, h3, h4"):
            h_text = h.get_text(strip=True).lower()
            if "биограф" in h_text:
                next_el = h.find_next_sibling()
                if next_el:
                    detail["biography"] = next_el.get_text(" ", strip=True)[:2000]
                break

    except Exception as e:
        log.warning("Failed to parse senator profile %s: %s", profile_url, e)
    return detail


def store_senator(conn, senator: Dict, profile: Dict) -> Optional[int]:
    name = senator.get("name", "")
    if not name:
        return None

    full_name = senator.get("data_title", "") or name
    entity_id = _ensure_entity(conn, full_name, "person")

    _ensure_alias(conn, entity_id, name, "short_name")
    if full_name != name:
        _ensure_alias(conn, entity_id, full_name, "full_name")

    parts = full_name.split()
    if len(parts) >= 3:
        short = f"{parts[0]} {parts[1][0]}.{parts[2][0]}."
        _ensure_alias(conn, entity_id, short, "initials")
    elif len(parts) == 2:
        short = f"{parts[0]} {parts[1][0]}."
        _ensure_alias(conn, entity_id, short, "initials")

    region = senator.get("region_title", "") or profile.get("region", "")
    position = profile.get("position", "Сенатор Российской Федерации")
    committee = profile.get("committee", "")
    party = profile.get("party", "")
    photo_url = profile.get("photo_url", "") or senator.get("photo_url", "")
    profile_url = senator.get("profile_url", "") or profile.get("url", "")

    row = conn.execute(
        "SELECT id, entity_id FROM deputy_profiles WHERE entity_id=?", (entity_id,)
    ).fetchone()
    if row:
        dp_id = row[0]
        conn.execute(
            "UPDATE deputy_profiles SET full_name=?, position=?, faction=?, region=?, "
            "committee=?, biography_url=?, photo_url=?, is_active=1 WHERE id=?",
            (full_name, position, party, region, committee,
             profile_url, photo_url, dp_id),
        )
    else:
        cur = conn.execute(
            "INSERT INTO deputy_profiles(entity_id, full_name, position, faction, region, "
            "committee, biography_url, photo_url, is_active) VALUES(?,?,?,?,?,?,?,?,1)",
            (entity_id, full_name, position, party, region,
             committee, profile_url, photo_url),
        )
        dp_id = cur.lastrowid

    conn.execute(
        "DELETE FROM official_positions WHERE entity_id=? AND position_title=? AND organization='Совет Федерации'",
        (entity_id, position),
    )
    conn.execute(
        "INSERT INTO official_positions(entity_id, position_title, organization, region, faction, "
        "started_at, source_url, source_type, is_active) VALUES(?,?,?,?,?,?,?,?,1)",
        (entity_id, position, "Совет Федерации", region, party,
         profile.get("date_appointed", ""), profile_url, "council_gov_ru"),
    )

    if party:
        existing = conn.execute(
            "SELECT id FROM party_memberships WHERE entity_id=? AND party_name=? AND is_current=1",
            (entity_id, party),
        ).fetchone()
        if not existing:
            conn.execute(
                "UPDATE party_memberships SET is_current=0, ended_at=datetime('now') WHERE entity_id=? AND is_current=1",
                (entity_id,),
            )
            conn.execute(
                "INSERT INTO party_memberships(entity_id, party_name, role, source_url, is_current) "
                "VALUES(?,?,?,?,1)",
                (entity_id, party, "член", profile_url),
            )

    return dp_id


def collect_senators(settings=None, fetch_profiles: bool = True, profile_limit: int = 0):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    _ensure_source(conn, "Совет Федерации (council.gov.ru)", BASE_URL, "senators")
    conn.commit()

    senators = scrape_senators_list(session)
    log.info("Found %d senators on list page", len(senators))

    stored = 0
    for i, sen in enumerate(senators):
        profile = {}
        if fetch_profiles:
            profile = scrape_senator_profile(session, sen["profile_url"])
            time.sleep(1.5)

        dp_id = store_senator(conn, sen, profile)
        if dp_id:
            stored += 1

        if profile_limit > 0 and i + 1 >= profile_limit:
            break

        if stored % 20 == 0:
            conn.commit()
            log.info("Senators: %d/%d stored", stored, len(senators))

    conn.commit()
    conn.close()
    log.info("Senators: %d stored (of %d found)", stored, len(senators))
    return stored


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Collect senators from council.gov.ru")
    parser.add_argument("--no-profiles", action="store_true", help="Skip profile detail pages")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of profiles to fetch")
    args = parser.parse_args()

    count = collect_senators(
        fetch_profiles=not args.no_profiles,
        profile_limit=args.limit,
    )
    print(f"Collected: {count} senators")


if __name__ == "__main__":
    main()
