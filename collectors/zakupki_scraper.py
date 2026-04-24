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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}

ZAKUPKI_BASE = "https://zakupki.gov.ru/epz/contract/search/results.html"
ZAKUPKI_HTTPS = "https://zakupki.gov.ru"
ZAKUPKI_HTTP = "http://zakupki.gov.ru"


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


def _parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    text = text.replace("\xa0", "").replace(" ", "").replace("₽", "").replace(",", ".").strip()
    try:
        return float(text)
    except ValueError:
        return None


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
                sets.append("inn=?"); vals.append(inn)
            if description:
                sets.append("description=?"); vals.append(description)
            if sets:
                vals.append(eid)
                conn.execute(f"UPDATE entities SET {','.join(sets)} WHERE id=?", vals)
        return eid
    cur = conn.execute(
        "INSERT INTO entities(entity_type, canonical_name, inn, description) VALUES(?,?,?,?)",
        (entity_type, canonical_name, inn, description),
    )
    return cur.lastrowid


def search_contracts(session, page: int = 1, per_page: int = 10,
                     fz44: bool = True, fz223: bool = True,
                     keyword: str = "") -> List[Dict]:
    from bs4 import BeautifulSoup
    contracts = []

    params = {
        "morphology": "on",
        "search-filter": "Дате+размещения",
        "pageNumber": str(page),
        "sortDirection": "false",
        "recordsPerPage": f"_{per_page}",
        "showLotsInfoHidden": "false",
        "fz44": "on" if fz44 else "",
        "fz223": "on" if fz223 else "",
        "pc": "on",
    }
    if keyword:
        params["searchString"] = keyword

    try:
        resp = session.get(ZAKUPKI_BASE, params=params, timeout=30)
        if resp.status_code != 200:
            log.warning("Zakupki returned %d", resp.status_code)
            return contracts

        soup = BeautifulSoup(resp.text, "lxml")
        entries = soup.select(".search-registry-entry-block")

        for entry in entries:
            contract = {}

            num_el = entry.select_one(".registry-entry__header-mid__number a, .registry-entry__header-mid__number")
            if num_el:
                contract["contract_number"] = num_el.get_text(strip=True).replace("№", "").strip()
                href = num_el.get("href", "")
                if href:
                    contract["detail_url"] = href if href.startswith("http") else f"https://zakupki.gov.ru{href}"

            status_el = entry.select_one(".registry-entry__header-mid__title")
            if status_el:
                contract["status"] = status_el.get_text(strip=True)

            customer_el = entry.select_one(".registry-entry__body-href")
            if customer_el:
                contract["customer"] = customer_el.get_text(strip=True)
                customer_href = customer_el.get("href", "")
                inn_m = re.search(r"inn[=/](\d{10,12})", customer_href or "")
                if inn_m:
                    contract["customer_inn"] = inn_m.group(1)

            subject_el = entry.select_one(".lots-wrap-content__body__val")
            if subject_el:
                contract["subject"] = subject_el.get_text(strip=True)

            price_el = entry.select_one(".price-block__value")
            if price_el:
                price_text = price_el.get_text(strip=True)
                contract["price_text"] = price_text
                contract["price"] = _parse_price(price_text)

            date_blocks = entry.select(".data-block")
            for db in date_blocks:
                title_el = db.select_one(".data-block__title")
                value_el = db.select_one(".data-block__value")
                if title_el and value_el:
                    title = title_el.get_text(strip=True).lower()
                    value = value_el.get_text(strip=True)
                    if "заключен" in title:
                        contract["contract_date"] = value
                    elif "срок исполнен" in title:
                        contract["deadline"] = value
                    elif "размещен" in title:
                        contract["published_date"] = value
                    elif "обновлен" in title:
                        contract["updated_date"] = value

            procurement_el = entry.select_one(".lots-wrap-content__body__val")
            if procurement_el:
                proc_text = procurement_el.get_text(strip=True)
                proc_type_m = re.match(r"^(Электронный\s+аукцион|Открытый\s+конкурс|Запрос\s+котировок|Закупка\s+у\s+ед\.?\s+поставщика|Аукцион|Конкурс)", proc_text)
                if proc_type_m:
                    contract["procurement_type"] = proc_type_m.group(1)

                lot_num_m = re.search(r"№(\d{18,})", proc_text)
                if lot_num_m:
                    contract["lot_number"] = lot_num_m.group(1)

            if contract.get("contract_number"):
                contracts.append(contract)

    except Exception as e:
        log.warning("Zakupki search failed page=%d: %s", page, e)

    return contracts


def store_contract(conn, contract: Dict) -> Optional[int]:
    number = contract.get("contract_number", "")
    if not number:
        return None

    customer = contract.get("customer", "")
    customer_inn = contract.get("customer_inn", "")
    customer_id = None
    if customer:
        customer_id = _get_or_create_entity(
            conn, "organization", customer,
            inn=customer_inn or None,
            description="Заказчик по государственному контракту"
        )
        if customer_inn:
            conn.execute("UPDATE entities SET inn=? WHERE id=?", (customer_inn, customer_id))

    price = contract.get("price")
    subject = contract.get("subject", "")
    raw_data = json.dumps(contract, ensure_ascii=False, default=str)

    url = contract.get("detail_url", "")
    existing = conn.execute("SELECT id FROM investigative_materials WHERE title LIKE ?", (f"Контракт {number}:%",)).fetchone()
    if existing:
        return existing[0]

    involved = []
    if customer_id:
        involved.append({"entity_id": customer_id, "name": customer, "role": "заказчик", "type": "organization"})
    involved_json = json.dumps(involved, ensure_ascii=False) if involved else None

    summary_parts = []
    if subject:
        summary_parts.append(subject[:300])
    if price:
        summary_parts.append(f"Сумма: {price:,.2f} руб.")
    if contract.get("contract_date"):
        summary_parts.append(f"Заключён: {contract['contract_date']}")
    if contract.get("procurement_type"):
        summary_parts.append(f"Тип: {contract['procurement_type']}")
    summary = " | ".join(summary_parts)

    cur = conn.execute(
        "INSERT INTO investigative_materials(title, material_type, url, source_org, publication_date, "
        "raw_data, verification_status, involved_entities, summary) VALUES(?,?,?,?,?,?,?,?,?)",
        (
            f"Контракт {number}: {subject[:200]}",
            "government_contract",
            url,
            "zakupki.gov.ru",
            contract.get("contract_date", contract.get("published_date", "")),
            raw_data,
            "confirmed",
            involved_json,
            summary,
        ),
    )
    return cur.lastrowid


def collect_contracts(settings=None, pages: int = 5, per_page: int = 10,
                      keywords: List[str] = None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    _ensure_source(conn, "zakupki.gov.ru", ZAKUPKI_HTTPS, "contracts")
    conn.commit()

    if keywords is None:
        keywords = ["строительство", "медицин", "образован", "информац", "связь",
                     "энерг", "транспорт", "безопасн", "консультац", "услуг"]

    total = 0
    for kw in keywords:
        for page in range(1, pages + 1):
            contracts = search_contracts(session, page=page, per_page=per_page, keyword=kw)
            log.info("Zakupki kw=%s page=%d found=%d", kw, page, len(contracts))

            for c in contracts:
                cid = store_contract(conn, c)
                if cid:
                    total += 1

            if not contracts:
                break
            conn.commit()
            time.sleep(0.5)

    conn.commit()
    log.info("Zakupki: %d contracts stored", total)
    conn.close()
    return total


def collect_contracts_recent(settings=None, pages: int = 3, per_page: int = 20):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    _ensure_source(conn, "zakupki.gov.ru", ZAKUPKI_HTTPS, "contracts")
    conn.commit()

    total = 0
    for page in range(1, pages + 1):
        contracts = search_contracts(session, page=page, per_page=per_page)
        log.info("Zakupki recent page=%d found=%d", page, len(contracts))

        for c in contracts:
            cid = store_contract(conn, c)
            if cid:
                total += 1

        if not contracts:
            break
        conn.commit()
        time.sleep(0.3)

    conn.commit()
    log.info("Zakupki recent: %d contracts stored", total)
    conn.close()
    return total


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Collect contracts from zakupki.gov.ru")
    parser.add_argument("--recent", action="store_true", help="Collect recent contracts (no keyword search)")
    parser.add_argument("--pages", type=int, default=3)
    parser.add_argument("--keywords", nargs="+", default=None)
    args = parser.parse_args()

    if args.recent:
        count = collect_contracts_recent(pages=args.pages)
    else:
        count = collect_contracts(pages=args.pages, keywords=args.keywords)
    print(f"Collected: {count} contracts")


if __name__ == "__main__":
    main()
