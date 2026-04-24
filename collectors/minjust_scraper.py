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

BASE_URL = "https://reestrs.minjust.gov.ru/rest/registry"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

REGISTRY_IDS = {
    "foreign_agents": "39b95df9-9a68-6b6d-e1e3-e6388507067e",
    "undesirable_orgs": "c2d1692e-a9f6-5a79-1",
}

FIELD_MAP_FOREIGN_AGENTS = {
    "field_1_i": "row_number",
    "field_2_s": "full_name",
    "field_3_s": "inclusion_reasons",
    "field_4_s": "inclusion_date",
    "field_5_s": "exclusion_date",
    "field_6_s": "domain_name",
    "field_7_s": "agent_type",
    "field_8_s": "registration_number",
    "field_9_s": "inn",
}

FIELD_MAP_UNDESIRABLE = {
    "field_1_i": "row_number",
    "field_2_s": "full_name",
    "field_3_s": "country",
    "field_4_s": "inclusion_date",
    "field_5_s": "exclusion_date",
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


def _map_fields(raw_entry: Dict, field_map: Dict) -> Dict:
    mapped = {}
    for raw_key, friendly_key in field_map.items():
        if raw_key in raw_entry:
            mapped[friendly_key] = raw_entry[raw_key]
    return mapped


def _get_or_create_entity(conn, entity_type, canonical_name, inn=None, description=None):
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type=? AND canonical_name=?",
        (entity_type, canonical_name),
    ).fetchone()
    if row:
        eid = row[0]
        if inn or description:
            sets = []
            vals = []
            if inn:
                sets.append("inn=?")
                vals.append(inn)
            if description:
                sets.append("description=?")
                vals.append(description)
            if sets:
                vals.append(eid)
                conn.execute(f"UPDATE entities SET {','.join(sets)} WHERE id=?", vals)
        return eid
    cur = conn.execute(
        "INSERT INTO entities(entity_type, canonical_name, inn, description) VALUES(?,?,?,?)",
        (entity_type, canonical_name, inn, description),
    )
    return cur.lastrowid


def fetch_registries(session) -> List[Dict]:
    r = session.post(f"{BASE_URL}/all", json={"offset": 0, "limit": 50, "search": ""}, timeout=20)
    if r.status_code != 200:
        log.warning("Failed to fetch registry list: %d", r.status_code)
        return []
    data = r.json()
    return data.get("values", [])


def fetch_registry_data(session, registry_id: str, offset: int = 0, limit: int = 100,
                        search: str = "", facets: Dict = None, sort: List = None) -> Dict:
    payload = {
        "offset": offset,
        "limit": limit,
        "search": search,
        "facets": facets or {},
        "sort": sort or [],
    }
    r = session.post(f"{BASE_URL}/{registry_id}/values", json=payload, timeout=30)
    if r.status_code != 200:
        log.warning("Failed to fetch registry %s data: %d", registry_id, r.status_code)
        return {"size": 0, "values": []}
    return r.json()


def _store_foreign_agent(conn, entry: Dict, field_map: Dict) -> Optional[int]:
    mapped = _map_fields(entry, field_map)
    name = mapped.get("full_name", "")
    if not name:
        return None

    inn = mapped.get("inn", "")
    agent_type = mapped.get("agent_type", "")
    inclusion_date = mapped.get("inclusion_date", "")
    exclusion_date = mapped.get("exclusion_date", "")
    reasons = mapped.get("inclusion_reasons", "")
    domain = mapped.get("domain_name", "")
    is_active = not bool(exclusion_date)

    entity_type = "organization" if ("Юридическ" in agent_type or "организаци" in name.lower()) else "person"
    desc = f"Иностранный агент ({agent_type})"
    if reasons:
        desc += f". Основания: {reasons[:200]}"
    if exclusion_date:
        desc += f". Исключён: {exclusion_date}"
    if domain:
        desc += f". Сайт: {domain}"

    entity_id = _get_or_create_entity(conn, entity_type, name, inn=inn, description=desc)

    if entity_type == "organization" and inn:
        conn.execute("UPDATE entities SET inn=? WHERE id=?", (inn, entity_id))

    title = f"Иностранный агент: {name}"
    existing = conn.execute(
        "SELECT id FROM investigative_materials WHERE title=? AND material_type=?",
        (title, "foreign_agent"),
    ).fetchone()
    if existing:
        return existing[0]

    raw_data = json.dumps(mapped, ensure_ascii=False, default=str)
    involved = json.dumps([{"entity_id": entity_id, "name": name, "role": "иностранный агент", "type": entity_type}], ensure_ascii=False)

    cur = conn.execute(
        "INSERT INTO investigative_materials(title, material_type, url, source_org, publication_date, "
        "raw_data, verification_status, involved_entities, summary) VALUES(?,?,?,?,?,?,?,?,?)",
        (
            title,
            "foreign_agent",
            "https://reestrs.minjust.gov.ru/",
            "Минюст России",
            inclusion_date,
            raw_data,
            "confirmed" if is_active else "archived",
            involved,
            desc,
        ),
    )
    return cur.lastrowid


def _store_undesirable_org(conn, entry: Dict, field_map: Dict) -> Optional[int]:
    mapped = _map_fields(entry, field_map)
    name = mapped.get("full_name", "")
    if not name:
        return None

    country = mapped.get("country", "")
    inclusion_date = mapped.get("inclusion_date", "")
    exclusion_date = mapped.get("exclusion_date", "")
    is_active = not bool(exclusion_date)

    desc = "Нежелательная организация"
    if country:
        desc += f". Страна: {country}"
    if exclusion_date:
        desc += f". Исключена: {exclusion_date}"

    entity_id = _get_or_create_entity(conn, "organization", name, description=desc)

    title = f"Нежелательная организация: {name}"
    existing = conn.execute(
        "SELECT id FROM investigative_materials WHERE title=? AND material_type=?",
        (title, "undesirable_org"),
    ).fetchone()
    if existing:
        return existing[0]

    raw_data = json.dumps(mapped, ensure_ascii=False, default=str)
    involved = json.dumps([{"entity_id": entity_id, "name": name, "role": "нежелательная организация", "type": "organization"}], ensure_ascii=False)

    cur = conn.execute(
        "INSERT INTO investigative_materials(title, material_type, url, source_org, publication_date, "
        "raw_data, verification_status, involved_entities, summary) VALUES(?,?,?,?,?,?,?,?,?)",
        (
            title,
            "undesirable_org",
            "https://reestrs.minjust.gov.ru/",
            "Минюст России",
            inclusion_date,
            raw_data,
            "confirmed" if is_active else "archived",
            involved,
            desc,
        ),
    )
    return cur.lastrowid


def collect_foreign_agents(settings=None, batch_size: int = 100, search: str = ""):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    _ensure_source(conn, "Минюст России (Иностранные агенты)", BASE_URL, "foreign_agents")
    conn.commit()

    registry_id = REGISTRY_IDS["foreign_agents"]
    total_stored = 0
    offset = 0

    while True:
        data = fetch_registry_data(session, registry_id, offset=offset, limit=batch_size, search=search)
        values = data.get("values", [])
        total_size = data.get("size", 0)

        if not values:
            break

        for entry in values:
            eid = _store_foreign_agent(conn, entry, FIELD_MAP_FOREIGN_AGENTS)
            if eid:
                total_stored += 1

        conn.commit()
        offset += batch_size
        log.info("Foreign agents: %d/%d processed", min(offset, total_size), total_size)

        if offset >= total_size:
            break
        time.sleep(0.3)

    conn.close()
    log.info("Foreign agents: %d stored", total_stored)
    return total_stored


def collect_undesirable_orgs(settings=None, batch_size: int = 100):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    _ensure_source(conn, "Минюст России (Нежелательные организации)", BASE_URL, "undesirable_orgs")
    conn.commit()

    registries = fetch_registries(session)
    undesirable_id = None
    undesirable_columns = {}
    for reg in registries:
        if "нежелательн" in reg.get("title", "").lower():
            undesirable_id = reg.get("id")
            for col in reg.get("columns", []):
                name = col.get("name", "")
                title = col.get("title", "").lower()
                if "наименован" in title:
                    undesirable_columns[name] = "full_name"
                elif "стран" in title:
                    undesirable_columns[name] = "country"
                elif "включен" in title and "решен" in title:
                    undesirable_columns[name] = "inclusion_date"
                elif "исключен" in title:
                    undesirable_columns[name] = "exclusion_date"
                elif "п/п" in title:
                    undesirable_columns[name] = "row_number"
            break

    if not undesirable_id:
        log.warning("Undesirable organizations registry not found")
        conn.close()
        return 0

    total_stored = 0
    offset = 0

    while True:
        data = fetch_registry_data(session, undesirable_id, offset=offset, limit=batch_size)
        values = data.get("values", [])
        total_size = data.get("size", 0)

        if not values:
            break

        for entry in values:
            eid = _store_undesirable_org(conn, entry, undesirable_columns)
            if eid:
                total_stored += 1

        conn.commit()
        offset += batch_size
        log.info("Undesirable orgs: %d/%d processed", min(offset, total_size), total_size)

        if offset >= total_size:
            break
        time.sleep(0.3)

    conn.close()
    log.info("Undesirable orgs: %d stored", total_stored)
    return total_stored


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Collect data from reestrs.minjust.gov.ru")
    parser.add_argument("--foreign-agents", action="store_true")
    parser.add_argument("--undesirable", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--search", type=str, default="")
    args = parser.parse_args()

    if not args.foreign_agents and not args.undesirable and not args.all:
        args.all = True

    if args.all or args.foreign_agents:
        count = collect_foreign_agents(search=args.search)
        print(f"Foreign agents: {count}")

    if args.all or args.undesirable:
        count = collect_undesirable_orgs()
        print(f"Undesirable orgs: {count}")


if __name__ == "__main__":
    main()
