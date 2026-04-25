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


def _collapse_space(text: str) -> str:
    return " ".join((text or "").replace("\xa0", " ").split())


def _extract_inn(text: str) -> str:
    match = re.search(r"\b(\d{10,12})\b", text or "")
    return match.group(1) if match else ""


def parse_contract_detail_html(html: str) -> Dict:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    detail: Dict = {"suppliers": []}

    customer_header = soup.find(["h1", "h2", "h3"], string=lambda s: s and "Информация о заказчике" in s)
    if customer_header:
        customer_block = customer_header.find_parent(class_="blockInfo") or customer_header.parent
        if customer_block:
            for section in customer_block.select("section"):
                title_el = section.select_one(".section__title")
                info_el = section.select_one(".section__info")
                title = _collapse_space(title_el.get_text(" ", strip=True) if title_el else "")
                info = _collapse_space(info_el.get_text(" ", strip=True) if info_el else section.get_text(" ", strip=True))
                if not title or not info:
                    continue
                if "Полное наименование заказчика" in title and not detail.get("customer"):
                    detail["customer"] = info
                elif title == "ИНН" and not detail.get("customer_inn"):
                    detail["customer_inn"] = _extract_inn(info)

    suppliers_header = soup.find(["h1", "h2", "h3"], string=lambda s: s and "Информация о поставщиках" in s)
    if suppliers_header:
        suppliers_block = suppliers_header.find_parent(class_="blockInfo") or suppliers_header.parent
        if suppliers_block:
            for row in suppliers_block.select("tbody tr"):
                cells = row.select("td")
                if not cells:
                    continue
                first_cell_text = cells[0].get_text("\n", strip=True)
                lines = [_collapse_space(line) for line in first_cell_text.splitlines() if _collapse_space(line)]
                if not lines:
                    continue
                supplier_name = lines[0]
                supplier_inn = _extract_inn(first_cell_text)
                supplier = {
                    "name": supplier_name,
                    "inn": supplier_inn,
                }
                detail["suppliers"].append(supplier)

    if detail["suppliers"]:
        detail["supplier"] = detail["suppliers"][0]["name"]
        if detail["suppliers"][0]["inn"]:
            detail["supplier_inn"] = detail["suppliers"][0]["inn"]

    return detail


def fetch_contract_detail(session, detail_url: str) -> Dict:
    if not detail_url:
        return {}
    try:
        resp = session.get(detail_url, timeout=40)
        if resp.status_code != 200:
            log.warning("Zakupki detail returned %d for %s", resp.status_code, detail_url)
            return {}
        detail = parse_contract_detail_html(resp.text)
        detail["detail_url"] = detail_url
        return detail
    except Exception as e:
        log.warning("Zakupki detail fetch failed for %s: %s", detail_url, e)
        return {}


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


def _build_involved_entities(customer_id, customer, customer_inn, supplier_id, supplier, supplier_inn):
    involved = []
    if customer_id or customer:
        involved.append({
            "entity_id": customer_id,
            "name": customer,
            "role": "заказчик",
            "type": "organization",
            "inn": customer_inn or "",
        })
    if supplier_id or supplier:
        involved.append({
            "entity_id": supplier_id,
            "name": supplier,
            "role": "поставщик",
            "type": "organization",
            "inn": supplier_inn or "",
        })
    return involved


def _contract_summary(contract: Dict) -> str:
    price = contract.get("price")
    subject = contract.get("subject", "")
    summary_parts = []
    if subject:
        summary_parts.append(subject[:300])
    if price:
        summary_parts.append(f"Сумма: {price:,.2f} руб.")
    if contract.get("contract_date"):
        summary_parts.append(f"Заключён: {contract['contract_date']}")
    if contract.get("procurement_type"):
        summary_parts.append(f"Тип: {contract['procurement_type']}")
    if contract.get("supplier"):
        summary_parts.append(f"Поставщик: {contract['supplier'][:160]}")
    return " | ".join(summary_parts)


def parse_search_results_html(html: str) -> List[Dict]:
    from bs4 import BeautifulSoup

    contracts: List[Dict] = []
    soup = BeautifulSoup(html, "lxml")
    entries = soup.select(".search-registry-entry-block")

    for entry in entries:
        contract = {}

        num_el = entry.select_one(".registry-entry__header-mid__number a")
        if num_el is None:
            num_el = entry.select_one(".registry-entry__header-mid__number")
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

    return contracts


def search_contracts(session, page: int = 1, per_page: int = 10,
                     fz44: bool = True, fz223: bool = True,
                     keyword: str = "") -> List[Dict]:
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

        contracts = parse_search_results_html(resp.text)

    except Exception as e:
        log.warning("Zakupki search failed page=%d: %s", page, e)

    return contracts


def store_contract(conn, contract: Dict) -> Optional[int]:
    number = contract.get("contract_number", "")
    if not number:
        return None

    customer = contract.get("customer", "")
    customer_inn = contract.get("customer_inn", "")
    supplier = contract.get("supplier", "")
    supplier_inn = contract.get("supplier_inn", "")
    customer_id = None
    if customer:
        customer_id = _get_or_create_entity(
            conn, "organization", customer,
            inn=customer_inn or None,
            description="Заказчик по государственному контракту"
        )
        if customer_inn:
            conn.execute("UPDATE entities SET inn=? WHERE id=?", (customer_inn, customer_id))

    supplier_id = None
    if supplier:
        supplier_id = _get_or_create_entity(
            conn, "organization", supplier,
            inn=supplier_inn or None,
            description="Поставщик по государственному контракту"
        )
        if supplier_inn:
            conn.execute("UPDATE entities SET inn=? WHERE id=?", (supplier_inn, supplier_id))

    subject = contract.get("subject", "")
    url = contract.get("detail_url", "")
    title = f"Контракт {number}: {subject[:200]}"
    summary = _contract_summary(contract)
    involved = _build_involved_entities(
        customer_id, customer, customer_inn, supplier_id, supplier, supplier_inn
    )
    raw_data = json.dumps(contract, ensure_ascii=False, default=str)

    existing = conn.execute(
        "SELECT id, raw_data FROM investigative_materials WHERE title LIKE ?",
        (f"Контракт {number}:%",),
    ).fetchone()
    if existing:
        existing_id = existing["id"] if hasattr(existing, "keys") else existing[0]
        existing_raw = existing["raw_data"] if hasattr(existing, "keys") else existing[1]
        old_raw = {}
        if existing_raw:
            try:
                old_raw = json.loads(existing_raw)
            except json.JSONDecodeError:
                old_raw = {}
        merged_contract = {**old_raw, **contract}
        if old_raw.get("suppliers") and not merged_contract.get("suppliers"):
            merged_contract["suppliers"] = old_raw["suppliers"]
        involved_json = json.dumps(involved, ensure_ascii=False) if involved else None
        conn.execute(
            """
            UPDATE investigative_materials
            SET title=?, url=COALESCE(NULLIF(?, ''), url), source_org=?, publication_date=?,
                raw_data=?, verification_status='confirmed', involved_entities=?, summary=?
            WHERE id=?
            """,
            (
                title,
                url,
                "zakupki.gov.ru",
                contract.get("contract_date", contract.get("published_date", "")),
                json.dumps(merged_contract, ensure_ascii=False, default=str),
                involved_json,
                summary,
                existing_id,
            ),
        )
        return existing_id

    involved_json = json.dumps(involved, ensure_ascii=False) if involved else None

    cur = conn.execute(
        "INSERT INTO investigative_materials(title, material_type, url, source_org, publication_date, "
        "raw_data, verification_status, involved_entities, summary) VALUES(?,?,?,?,?,?,?,?,?)",
        (
            title,
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
                      keywords: List[str] = None, fetch_details: bool = True,
                      detail_limit: int = 12):
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
    detail_fetches = 0
    for kw in keywords:
        for page in range(1, pages + 1):
            contracts = search_contracts(session, page=page, per_page=per_page, keyword=kw)
            log.info("Zakupki kw=%s page=%d found=%d", kw, page, len(contracts))

            for c in contracts:
                if fetch_details and c.get("detail_url") and detail_fetches < detail_limit:
                    detail = fetch_contract_detail(session, c["detail_url"])
                    if detail:
                        c.update(detail)
                    detail_fetches += 1
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


def collect_contracts_recent(settings=None, pages: int = 3, per_page: int = 20, detail_limit: int | None = None):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()

    _ensure_source(conn, "zakupki.gov.ru", ZAKUPKI_HTTPS, "contracts")
    conn.commit()

    total = 0
    detail_fetches = 0
    if detail_limit is None:
        detail_limit = max(1, pages * per_page)
    for page in range(1, pages + 1):
        contracts = search_contracts(session, page=page, per_page=per_page)
        log.info("Zakupki recent page=%d found=%d", page, len(contracts))

        for c in contracts:
            if c.get("detail_url") and detail_fetches < detail_limit:
                detail = fetch_contract_detail(session, c["detail_url"])
                if detail:
                    c.update(detail)
                detail_fetches += 1
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
