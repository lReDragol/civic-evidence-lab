import json
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    os.sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

DUMA_API_BASE = "https://api.duma.gov.ru/api"


def _get_duma_token(settings: dict) -> Optional[str]:
    return settings.get("duma_api_token")


def fetch_deputies_api(token: str, period_id: int = 8) -> Optional[List[Dict]]:
    try:
        import requests
        url = f"{DUMA_API_BASE}/{token}/deputies.json"
        params = {"position": "deputy", "period_id": period_id}
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        log.warning("Duma API returned %d", resp.status_code)
    except Exception as e:
        log.warning("Duma API fetch failed: %s", e)
    return None


def fetch_deputy_detail_api(token: str, deputy_id: int) -> Optional[Dict]:
    try:
        import requests
        url = f"{DUMA_API_BASE}/{token}/deputy.json"
        params = {"id": deputy_id}
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        log.warning("Duma deputy detail fetch failed: %s", e)
    return None


def fetch_periods_api(token: str) -> Optional[List[Dict]]:
    try:
        import requests
        url = f"{DUMA_API_BASE}/{token}/periods.json"
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        log.warning("Duma periods fetch failed: %s", e)
    return None


FACTION_MAP = {
    "Единая Россия": "er",
    "КПРФ": "kprf",
    "ЛДПР": "ldpr",
    "Справедливая Россия": "sr",
    "Новые люди": "nl",
    "Партия роста": "pg",
    "Гражданская платформа": "gp",
    "Родина": "rodina",
    "Патриоты России": "pr",
}


def _get_or_create_entity(conn: sqlite3.Connection, entity_type: str, name: str, extra: dict = None) -> int:
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type=? AND canonical_name=?",
        (entity_type, name),
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO entities(entity_type, canonical_name, inn, ogrn, description, extra_data) VALUES(?,?,?,?,?,?)",
        (
            entity_type,
            name,
            extra.get("inn") if extra else None,
            extra.get("ogrn") if extra else None,
            extra.get("description") if extra else None,
            json.dumps(extra, ensure_ascii=False) if extra else None,
        ),
    )
    return cur.lastrowid


def _import_deputy(conn: sqlite3.Connection, deputy_data: Dict) -> Optional[int]:
    full_name = deputy_data.get("name", "")
    if not full_name:
        first = deputy_data.get("firstName", "")
        last = deputy_data.get("lastName", "")
        middle = deputy_data.get("middleName", "")
        parts = [p for p in [last, first, middle] if p]
        full_name = " ".join(parts)
    if not full_name:
        return None

    entity_id = _get_or_create_entity(conn, "person", full_name, {
        "duma_id": deputy_data.get("id"),
        "faction": deputy_data.get("factionName", deputy_data.get("faction")),
        "position": deputy_data.get("position"),
    })

    existing = conn.execute(
        "SELECT id FROM deputy_profiles WHERE entity_id=?", (entity_id,)
    ).fetchone()
    if existing:
        conn.execute(
            """UPDATE deputy_profiles SET
               full_name=?, position=?, faction=?, region=?, committee=?,
               duma_id=?, date_elected=?, income_latest=?, biography_url=?, photo_url=?, is_active=?
               WHERE entity_id=?""",
            (
                full_name,
                deputy_data.get("position", "депутат ГД"),
                deputy_data.get("factionName", deputy_data.get("faction", "")),
                deputy_data.get("regionName", deputy_data.get("region", "")),
                deputy_data.get("committeeName", deputy_data.get("committee", "")),
                deputy_data.get("id"),
                deputy_data.get("dateElected", deputy_data.get("dateStart", "")),
                deputy_data.get("income", ""),
                deputy_data.get("biographyUrl", deputy_data.get("url", "")),
                deputy_data.get("photoUrl", deputy_data.get("photo", "")),
                1 if deputy_data.get("isCurrent", True) else 0,
                entity_id,
            ),
        )
    else:
        conn.execute(
            """INSERT INTO deputy_profiles(
                entity_id, full_name, position, faction, region, committee,
                duma_id, date_elected, income_latest, biography_url, photo_url, is_active
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                entity_id,
                full_name,
                deputy_data.get("position", "депутат ГД"),
                deputy_data.get("factionName", deputy_data.get("faction", "")),
                deputy_data.get("regionName", deputy_data.get("region", "")),
                deputy_data.get("committeeName", deputy_data.get("committee", "")),
                deputy_data.get("id"),
                deputy_data.get("dateElected", deputy_data.get("dateStart", "")),
                deputy_data.get("income", ""),
                deputy_data.get("biographyUrl", deputy_data.get("url", "")),
                deputy_data.get("photoUrl", deputy_data.get("photo", "")),
                1 if deputy_data.get("isCurrent", True) else 0,
            ),
        )

    return entity_id


def import_from_api(settings: dict = None, period_id: int = 8) -> int:
    if settings is None:
        settings = load_settings()

    token = _get_duma_token(settings)
    if not token:
        log.warning("No duma_api_token in settings — cannot fetch from API")
        return 0

    conn = get_db(settings)
    deputies = fetch_deputies_api(token, period_id=period_id)

    if not deputies:
        log.warning("No deputies returned from API")
        conn.close()
        return 0

    imported = 0
    for dep in deputies:
        try:
            dep_id = dep.get("id")
            detail = None
            if dep_id:
                detail = fetch_deputy_detail_api(token, dep_id)
            data = detail or dep
            entity_id = _import_deputy(conn, data)
            if entity_id:
                imported += 1
        except Exception as e:
            log.warning("Failed to import deputy %s: %s", dep.get("name", "?"), e)

    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM deputy_profiles").fetchone()[0]
    log.info("Imported %d deputies from API, total in DB: %d", imported, count)
    conn.close()
    return imported


def import_from_manual_list(settings: dict = None) -> int:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    manual_deputies = [
        {"name": "Вячеслав Володин", "faction": "Единая Россия", "position": "Председатель ГД", "duma_id": 1},
        {"name": "Владимир Васильев", "faction": "Единая Россия", "position": "депутат ГД", "duma_id": 2},
        {"name": "Ирина Яровая", "faction": "Единая Россия", "position": "депутат ГД", "duma_id": 3},
        {"name": "Павел Крашенинников", "faction": "Единая Россия", "position": "депутат ГД", "duma_id": 4},
        {"name": "Елена Мизулина", "faction": "Единая Россия", "position": "депутат ГД", "duma_id": 5},
        {"name": "Геннадий Зюганов", "faction": "КПРФ", "position": "депутат ГД", "duma_id": 6},
        {"name": "Валентин Терентьев", "faction": "КПРФ", "position": "депутат ГД", "duma_id": 7},
        {"name": "Леонид Слуцкий", "faction": "ЛДПР", "position": "депутат ГД", "duma_id": 8},
        {"name": "Михаил Дегтярёв", "faction": "ЛДПР", "position": "депутат ГД", "duma_id": 9},
        {"name": "Сергей Миронов", "faction": "Справедливая Россия", "position": "депутат ГД", "duma_id": 10},
        {"name": "Ольга Епифанова", "faction": "Справедливая Россия", "position": "депутат ГД", "duma_id": 11},
        {"name": "Алексей Нечаев", "faction": "Новые люди", "position": "депутат ГД", "duma_id": 12},
        {"name": "Андрей Луговой", "faction": "ЛДПР", "position": "депутат ГД", "duma_id": 13},
        {"name": "Рафаэль Марданшин", "faction": "Единая Россия", "position": "депутат ГД", "duma_id": 14},
        {"name": "Василий Пискарёв", "faction": "Единая Россия", "position": "депутат ГД", "duma_id": 15},
        {"name": "Дмитрий Саблин", "faction": "Единая Россия", "position": "депутат ГД", "duma_id": 16},
        {"name": "Андрей Турчак", "faction": "Единая Россия", "position": "депутат ГД", "duma_id": 17},
        {"name": "Мария Бутина", "faction": "Единая Россия", "position": "депутат ГД", "duma_id": 18},
        {"name": "Константин Затулин", "faction": "Единая Россия", "position": "депутат ГД", "duma_id": 19},
        {"name": "Сергей Гаврилов", "faction": "КПРФ", "position": "депутат ГД", "duma_id": 20},
        {"name": "Денис Парфёнов", "faction": "КПРФ", "position": "депутат ГД", "duma_id": 21},
        {"name": "Михаил Матвеев", "faction": "КПРФ", "position": "депутат ГД", "duma_id": 22},
        {"name": "Ольга Тимофеева", "faction": "Единая Россия", "position": "депутат ГД", "duma_id": 23},
        {"name": "Александр Хинштейн", "faction": "Единая Россия", "position": "депутат ГД", "duma_id": 24},
        {"name": "Яна Лантратова", "faction": "Справедливая Россия", "position": "депутат ГД", "duma_id": 25},
    ]

    imported = 0
    for dep in manual_deputies:
        try:
            entity_id = _import_deputy(conn, dep)
            if entity_id:
                imported += 1
        except Exception as e:
            log.warning("Failed to import %s: %s", dep.get("name", "?"), e)

    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM deputy_profiles").fetchone()[0]
    log.info("Imported %d deputies from manual list, total in DB: %d", imported, count)
    conn.close()
    return imported


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", action="store_true", help="Use Duma API (requires token)")
    parser.add_argument("--period", type=int, default=8, help="Duma period ID (default: 8 = VIII созыв)")
    args = parser.parse_args()

    if args.api:
        import_from_api(period_id=args.period)
    else:
        import_from_manual_list()


if __name__ == "__main__":
    main()
