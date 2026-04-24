import hashlib
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

DUMA_DEPUTIES_URL = "https://duma.gov.ru/deputies/"
DUMA_API_DEPUTIES = "https://api.duma.gov.ru/api/{token}/deputies.xml"

FACTION_MAP = {
    "Единая Россия": "ЕР",
    "КПРФ": "КПРФ",
    "ЛДПР": "ЛДПР",
    "Справедливая Россия — За правду": "СР",
    "Справедливая Россия": "СР",
    "Новые люди": "НЛ",
    "Родина": "Родина",
    "Партия роста": "Партия роста",
    "Гражданская платформа": "Гражданская платформа",
}

FACTION_MAP_REVERSE = {v: k for k, v in FACTION_MAP.items() if k not in ("КПРФ", "ЛДПР", "Родина", "Партия роста", "Гражданская платформа")}
FACTION_MAP_REVERSE["КПРФ"] = "КПРФ"
FACTION_MAP_REVERSE["ЛДПР"] = "ЛДПР"
FACTION_MAP_REVERSE["Родина"] = "Родина"


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


def _get_or_create_entity(conn, entity_type, canonical_name, inn=None, ogrn=None, description=None, extra_data=None):
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type=? AND canonical_name=?",
        (entity_type, canonical_name),
    ).fetchone()
    if row:
        eid = row[0]
        if inn or ogrn or description or extra_data:
            sets = []
            vals = []
            if inn:
                sets.append("inn=?"); vals.append(inn)
            if ogrn:
                sets.append("ogrn=?"); vals.append(ogrn)
            if description:
                sets.append("description=?"); vals.append(description)
            if extra_data:
                sets.append("extra_data=?"); vals.append(extra_data if isinstance(extra_data, str) else json.dumps(extra_data, ensure_ascii=False))
            if sets:
                vals.append(eid)
                conn.execute(f"UPDATE entities SET {','.join(sets)} WHERE id=?", vals)
        return eid

    cur = conn.execute(
        "INSERT INTO entities(entity_type, canonical_name, inn, ogrn, description, extra_data) VALUES(?,?,?,?,?,?)",
        (entity_type, canonical_name, inn, ogrn, description,
         extra_data if isinstance(extra_data, str) or extra_data is None else json.dumps(extra_data, ensure_ascii=False)),
    )
    return cur.lastrowid


def _add_alias(conn, entity_id, alias, alias_type="spelling"):
    if not alias or not entity_id:
        return
    try:
        conn.execute(
            "INSERT OR IGNORE INTO entity_aliases(entity_id, alias, alias_type) VALUES(?,?,?)",
            (entity_id, alias, alias_type),
        )
    except Exception:
        pass


def _add_party_membership(conn, entity_id, party_name, role=None, started_at=None, source_url=None):
    existing = conn.execute(
        "SELECT id FROM party_memberships WHERE entity_id=? AND party_name=? AND is_current=1",
        (entity_id, party_name),
    ).fetchone()
    if existing:
        return existing[0]
    conn.execute(
        "UPDATE party_memberships SET is_current=0, ended_at=? WHERE entity_id=? AND is_current=1 AND party_name!=?",
        (datetime.now().isoformat()[:10], entity_id, party_name),
    )
    cur = conn.execute(
        "INSERT INTO party_memberships(entity_id, party_name, role, started_at, source_url, is_current) VALUES(?,?,?,?,?,1)",
        (entity_id, party_name, role, started_at or datetime.now().isoformat()[:10], source_url),
    )
    return cur.lastrowid


def _add_official_position(conn, entity_id, position_title, organization, region=None,
                           faction=None, started_at=None, source_url=None, source_type=None):
    existing = conn.execute(
        "SELECT id FROM official_positions WHERE entity_id=? AND position_title=? AND organization=? AND is_active=1",
        (entity_id, position_title, organization),
    ).fetchone()
    if existing:
        row = conn.execute("SELECT faction, region FROM official_positions WHERE id=?", (existing[0],)).fetchone()
        updates = {}
        if faction and row[0] != faction:
            updates["faction"] = faction
        if region and row[1] != region:
            updates["region"] = region
        if updates:
            sets = ",".join(f"{k}=?" for k in updates)
            conn.execute(f"UPDATE official_positions SET {sets} WHERE id=?", list(updates.values()) + [existing[0]])
        return existing[0]

    cur = conn.execute(
        "INSERT INTO official_positions(entity_id, position_title, organization, region, faction, started_at, source_url, source_type, is_active) VALUES(?,?,?,?,?,?,?,?,1)",
        (entity_id, position_title, organization, region, faction, started_at, source_url, source_type),
    )
    return cur.lastrowid


def _add_deputy_profile(conn, entity_id, full_name, position=None, faction=None,
                        region=None, committee=None, duma_id=None, date_elected=None,
                        income_latest=None, biography_url=None, photo_url=None, is_active=1):
    existing = conn.execute("SELECT id FROM deputy_profiles WHERE entity_id=?", (entity_id,)).fetchone()
    if existing:
        dp_id = existing[0]
        conn.execute(
            "UPDATE deputy_profiles SET full_name=?, position=?, faction=?, region=?, committee=?, "
            "duma_id=?, date_elected=?, income_latest=?, biography_url=?, photo_url=?, is_active=? WHERE id=?",
            (full_name, position, faction, region, committee, duma_id, date_elected,
             income_latest, biography_url, photo_url, is_active, dp_id),
        )
        return dp_id

    cur = conn.execute(
        "INSERT INTO deputy_profiles(entity_id, full_name, position, faction, region, committee, "
        "duma_id, date_elected, income_latest, biography_url, photo_url, is_active) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (entity_id, full_name, position, faction, region, committee, duma_id,
         date_elected, income_latest, biography_url, photo_url, is_active),
    )
    return cur.lastrowid


def scrape_deputies_list(session, pages: int = 20) -> List[Dict]:
    from bs4 import BeautifulSoup
    deputies = []
    for page in range(1, pages + 1):
        try:
            url = f"https://duma.gov.ru/deputies/page/{page}/"
            resp = session.get(url, timeout=20)
            if resp.status_code != 200:
                log.warning("Deputies page %d returned %d", page, resp.status_code)
                break
            soup = BeautifulSoup(resp.text, "lxml")

            items = soup.select(".deputy-card, .deputy-item, .person-card, article.deputy")
            if not items:
                items = soup.select("a[href*='/deputies/']")
            if not items:
                rows = soup.select("table tbody tr")
                if rows:
                    for row in rows:
                        cells = row.find_all("td")
                        if len(cells) >= 3:
                            link = cells[0].find("a")
                            name = link.get_text(strip=True) if link else cells[0].get_text(strip=True)
                            href = link.get("href", "") if link else ""
                            faction = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                            region = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                            if name:
                                deputies.append({
                                    "name": name,
                                    "faction": faction,
                                    "region": region,
                                    "profile_url": urljoin("https://duma.gov.ru", href) if href else "",
                                })
                    continue
                if page == 1:
                    log.warning("No deputy items found on page 1 — structure may have changed")
                    for a in soup.select("a[href]"):
                        href = a.get("href", "")
                        text = a.get_text(strip=True)
                        if "/deputies/" in href and text and len(text) > 5 and " " in text:
                            deputies.append({
                                "name": text,
                                "faction": "",
                                "region": "",
                                "profile_url": urljoin("https://duma.gov.ru", href),
                            })
                if not deputies and page > 1:
                    break
                continue

            for item in items:
                name = ""
                href = ""
                faction = ""
                region = ""

                link_el = item.find("a") or (item if item.name == "a" else None)
                if link_el:
                    name = link_el.get_text(strip=True)
                    href = link_el.get("href", "")

                faction_el = item.select_one(".faction, .party, .fraction, .deputy-faction")
                if faction_el:
                    faction = faction_el.get_text(strip=True)

                region_el = item.select_one(".region, .deputy-region, .okrug")
                if region_el:
                    region = region_el.get_text(strip=True)

                if not name:
                    h = item.select_one("h2, h3, h4, .name, .title")
                    if h:
                        name = h.get_text(strip=True)

                if not faction:
                    for el in item.select("span, div, p"):
                        txt = el.get_text(strip=True)
                        for known in FACTION_MAP:
                            if known in txt:
                                faction = known
                                break
                        if faction:
                            break

                if name and len(name) > 3:
                    deputies.append({
                        "name": name,
                        "faction": faction,
                        "region": region,
                        "profile_url": urljoin("https://duma.gov.ru", href) if href else "",
                    })

            time.sleep(0.5)
        except Exception as e:
            log.warning("Deputies page %d failed: %s", page, e)
            continue

    return deputies


def scrape_deputy_detail(session, profile_url: str) -> Dict:
    from bs4 import BeautifulSoup
    detail = {"profile_url": profile_url}
    try:
        resp = session.get(profile_url, timeout=20)
        if resp.status_code != 200:
            return detail
        soup = BeautifulSoup(resp.text, "lxml")

        h1 = soup.select_one("h1, .deputy-name, .person-name")
        if h1:
            detail["full_name"] = h1.get_text(strip=True)

        for row in soup.select(".info-row, .detail-row, .deputy-info tr, dl dt, .field"):
            label_el = row.select_one(".label, dt, .field-label, .info-label")
            value_el = row.select_one(".value, dd, .field-value, .info-value")
            if not label_el or not value_el:
                continue
            label = label_el.get_text(strip=True).lower()
            value = value_el.get_text(strip=True)
            if "фракци" in label:
                detail["faction"] = value
            elif "регион" in label or "округ" in label:
                detail["region"] = value
            elif "комитет" in label:
                detail["committee"] = value
            elif "дата" in label or "избран" in label:
                detail["date_elected"] = value
            elif "доход" in label or "деклар" in label:
                detail["income_latest"] = value
            elif "должност" in label or "позиция" in label:
                detail["position"] = value

        img = soup.select_one("img.deputy-photo, .person-photo img, img[alt*='депутат']")
        if img:
            detail["photo_url"] = img.get("src", "")

        bio_link = soup.select_one("a[href*='biography'], a[href*='bio']")
        if bio_link:
            detail["biography_url"] = urljoin(profile_url, bio_link.get("href", ""))

        duma_id_match = re.search(r"/deputies/(\d+)", profile_url)
        if duma_id_match:
            detail["duma_id"] = int(duma_id_match.group(1))

    except Exception as e:
        log.warning("Deputy detail failed for %s: %s", profile_url, e)
    return detail


def fetch_deputies_api(token: str, session=None) -> List[Dict]:
    import requests
    deputies = []
    url = DUMA_API_DEPUTIES.format(token=token)
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            log.warning("Duma API returned %d", resp.status_code)
            return deputies

        import xml.etree.ElementTree as ET
        root = ET.fromstring(resp.text)
        for deputy_el in root.findall(".//deputy"):
            d = {
                "duma_id": int(deputy_el.get("id", 0)),
                "full_name": deputy_el.findtext("name", ""),
                "faction": deputy_el.findtext("faction", ""),
                "region": deputy_el.findtext("region", ""),
                "position": deputy_el.findtext("position", ""),
                "date_elected": deputy_el.findtext("dateElected", ""),
                "is_active": int(deputy_el.get("isCurrent", "1")),
                "profile_url": deputy_el.findtext("link", ""),
            }
            if d["full_name"]:
                deputies.append(d)
    except Exception as e:
        log.warning("Duma API fetch failed: %s", e)
    return deputies


def ingest_deputies(deputies: List[Dict], conn, fetch_details: bool = True):
    session = _session()
    ingested = 0
    for dep in deputies:
        try:
            name = dep.get("full_name") or dep.get("name", "")
            if not name or len(name) < 5:
                continue

            faction = dep.get("faction", "")
            region = dep.get("region", "")
            profile_url = dep.get("profile_url", "")
            duma_id = dep.get("duma_id")
            committee = dep.get("committee", "")
            position = dep.get("position", "Депутат Государственной Думы")
            date_elected = dep.get("date_elected", "")
            income_latest = dep.get("income_latest")
            is_active = dep.get("is_active", 1)

            if fetch_details and profile_url:
                detail = scrape_deputy_detail(session, profile_url)
                if detail.get("full_name"):
                    name = detail["full_name"]
                faction = detail.get("faction", faction)
                region = detail.get("region", region)
                committee = detail.get("committee", committee)
                position = detail.get("position", position)
                date_elected = detail.get("date_elected", date_elected)
                income_latest = detail.get("income_latest", income_latest)
                if detail.get("duma_id"):
                    duma_id = detail["duma_id"]
                if detail.get("biography_url"):
                    profile_url = detail["biography_url"]
                if detail.get("photo_url"):
                    dep["photo_url"] = detail["photo_url"]
                time.sleep(0.3)

            entity_id = _get_or_create_entity(
                conn, "person", name,
                description=f"Депутат ГД. Фракция: {faction}. Регион: {region}" if faction or region else "Депутат Государственной Думы",
            )

            parts = name.split()
            if len(parts) >= 2:
                _add_alias(conn, entity_id, f"{parts[0]} {parts[1][:1]}.", "short_form")
                if len(parts) >= 3:
                    _add_alias(conn, entity_id, f"{parts[0]} {parts[1][:1]}.{parts[2][:1]}.", "short_form")
                    _add_alias(conn, entity_id, parts[0], "surname_only")

            _add_deputy_profile(
                conn, entity_id, name,
                position=position, faction=faction, region=region,
                committee=committee, duma_id=duma_id,
                date_elected=date_elected, income_latest=income_latest,
                biography_url=profile_url,
                photo_url=dep.get("photo_url"),
                is_active=is_active,
            )

            if faction:
                faction_full = FACTION_MAP_REVERSE.get(faction, faction)
                _add_party_membership(
                    conn, entity_id, faction_full,
                    role="член фракции",
                    started_at=date_elected or datetime.now().isoformat()[:10],
                    source_url=profile_url,
                )

            _add_official_position(
                conn, entity_id, position or "Депутат Государственной Думы",
                "Государственная Дума РФ", region=region, faction=faction,
                started_at=date_elected, source_url=profile_url,
                source_type="duma_profile",
            )

            ingested += 1
            if ingested % 50 == 0:
                conn.commit()

        except Exception as e:
            log.warning("Failed to ingest deputy %s: %s", dep.get("name", "?"), e)
            continue

    conn.commit()
    log.info("Ingested %d deputies", ingested)
    return ingested


def collect_deputies_html(settings: dict = None, fetch_details: bool = True, max_pages: int = 20):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)

    _ensure_source(conn, "Государственная Дума (duma.gov.ru)", DUMA_DEPUTIES_URL, "deputies")
    conn.commit()

    try:
        session = _session()
        log.info("Scraping deputies list from duma.gov.ru...")
        deputies = scrape_deputies_list(session, pages=max_pages)
        log.info("Found %d deputies on list pages", len(deputies))

        if deputies:
            count = ingest_deputies(deputies, conn, fetch_details=fetch_details)
            return count
        else:
            log.warning("No deputies found — site may be unreachable or structure changed")
            return 0
    except Exception as e:
        log.error("Deputies collection failed: %s", e)
        return 0
    finally:
        conn.close()


def collect_deputies_api(settings: dict = None):
    if settings is None:
        settings = load_settings()
    token = settings.get("duma_api_token")
    if not token:
        log.warning("No duma_api_token in settings — cannot use API")
        return 0

    conn = get_db(settings)
    _ensure_source(conn, "Государственная Дума API (api.duma.gov.ru)", "https://api.duma.gov.ru", "deputies")
    conn.commit()

    try:
        deputies = fetch_deputies_api(token)
        if not deputies:
            log.warning("Duma API returned 0 deputies")
            return 0
        log.info("Duma API: %d deputies", len(deputies))
        count = ingest_deputies(deputies, conn, fetch_details=False)
        return count
    except Exception as e:
        log.error("Duma API collection failed: %s", e)
        return 0
    finally:
        conn.close()


def collect_deputies(settings: dict = None, fetch_details: bool = True):
    if settings is None:
        settings = load_settings()
    token = settings.get("duma_api_token")
    if token:
        log.info("Using Duma API (token found)")
        return collect_deputies_api(settings)
    else:
        log.info("No API token — using HTML scraping")
        return collect_deputies_html(settings, fetch_details=fetch_details)


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Collect deputy profiles from duma.gov.ru")
    parser.add_argument("--api", action="store_true", help="Use API (requires duma_api_token in settings)")
    parser.add_argument("--html", action="store_true", help="Force HTML scraping")
    parser.add_argument("--no-details", action="store_true", help="Skip detail page fetching")
    parser.add_argument("--pages", type=int, default=20, help="Max pages to scrape")
    args = parser.parse_args()

    settings = load_settings()

    if args.api:
        count = collect_deputies_api(settings)
    elif args.html:
        count = collect_deputies_html(settings, fetch_details=not args.no_details, max_pages=args.pages)
    else:
        count = collect_deputies(settings, fetch_details=not args.no_details)

    print(f"Ingested: {count} deputies")


if __name__ == "__main__":
    main()
