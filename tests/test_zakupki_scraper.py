import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from collectors.zakupki_scraper import parse_contract_detail_html, store_contract
from collectors.zakupki_scraper import parse_search_results_html


DETAIL_HTML = """
<div id="ajax-group">
  <div class="container">
    <div class="row blockInfo">
      <div class="col">
        <h2 class="blockInfo__title">Информация о заказчике</h2>
        <section class="blockInfo__section section">
          <span class="section__title">Полное наименование заказчика</span>
          <span class="section__info">ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ "ЗАКАЗЧИК"</span>
        </section>
        <section class="blockInfo__section section">
          <span class="section__title">ИНН</span>
          <span class="section__info">1234567890</span>
        </section>
      </div>
    </div>
  </div>
  <div class="container">
    <div class="row blockInfo">
      <div class="col">
        <h2 class="blockInfo__title">Информация о поставщиках</h2>
        <div class="participantsInnerHtml">
          <table class="blockInfo__table tableBlock grayBorderBottom">
            <tbody class="tableBlock__body">
              <tr class="tableBlock__row">
                <td class="tableBlock__col tableBlock__col_first text-break">
                  ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ПОСТАВЩИК"
                  <section class="section">
                    <span class="grey-main-light">ИНН:</span>
                    <span>0987654321</span>
                  </section>
                </td>
                <td class="tableBlock__col">Российская Федерация<br/>643</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>
</div>
"""


SEARCH_RESULTS_HTML = """
<div class="search-registry-entry-block">
  <div class="registry-entry__header-mid__number">
    <a href="/epz/contract/contractCard/common-info.html?reestrNumber=2616410011825000592">
      № 2616410011825000592
    </a>
  </div>
  <div class="registry-entry__header-mid__title">Исполнение</div>
  <a class="registry-entry__body-href" href="/epz/organization/view/info.html?inn=6164100118">
    ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ "ЗАКАЗЧИК"
  </a>
  <div class="lots-wrap-content__body__val">Поставка тестового товара №123456789012345678</div>
  <div class="price-block__value">1 234,56 ₽</div>
  <div class="data-block">
    <span class="data-block__title">Заключен</span>
    <span class="data-block__value">01.04.2026</span>
  </div>
</div>
"""


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                canonical_name TEXT NOT NULL,
                inn TEXT,
                description TEXT
            );
            CREATE TABLE investigative_materials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content_item_id INTEGER,
                material_type TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT,
                involved_entities TEXT,
                publication_date TEXT,
                source_org TEXT,
                verification_status TEXT,
                raw_data TEXT,
                url TEXT
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


class ZakupkiScraperTests(unittest.TestCase):
    def test_parse_search_results_html_extracts_detail_url_from_nested_anchor(self):
        contracts = parse_search_results_html(SEARCH_RESULTS_HTML)

        self.assertEqual(len(contracts), 1)
        self.assertEqual(contracts[0]["contract_number"], "2616410011825000592")
        self.assertEqual(
            contracts[0]["detail_url"],
            "https://zakupki.gov.ru/epz/contract/contractCard/common-info.html?reestrNumber=2616410011825000592",
        )
        self.assertEqual(contracts[0]["customer_inn"], "6164100118")

    def test_parse_contract_detail_html_extracts_customer_and_supplier(self):
        detail = parse_contract_detail_html(DETAIL_HTML)
        self.assertEqual(detail["customer"], 'ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ "ЗАКАЗЧИК"')
        self.assertEqual(detail["customer_inn"], "1234567890")
        self.assertEqual(detail["supplier"], 'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ПОСТАВЩИК"')
        self.assertEqual(detail["supplier_inn"], "0987654321")
        self.assertTrue(detail["suppliers"])

    def test_store_contract_updates_existing_material_with_enriched_supplier_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "sample.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                base_contract = {
                    "contract_number": "CN-77",
                    "customer": 'ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ "ЗАКАЗЧИК"',
                    "subject": "Поставка тестового товара",
                    "contract_date": "2026-04-01",
                    "detail_url": "https://zakupki.test/contract/CN-77",
                }
                first_id = store_contract(conn, base_contract)
                self.assertIsNotNone(first_id)

                enriched_contract = {
                    **base_contract,
                    "customer_inn": "1234567890",
                    "supplier": 'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ПОСТАВЩИК"',
                    "supplier_inn": "0987654321",
                }
                second_id = store_contract(conn, enriched_contract)
                self.assertEqual(first_id, second_id)

                row = conn.execute(
                    "SELECT title, involved_entities, raw_data FROM investigative_materials WHERE id=?",
                    (first_id,),
                ).fetchone()
                self.assertIsNotNone(row)
                involved_entities = json.loads(row[1])
                raw_data = json.loads(row[2])
                roles = {item["role"] for item in involved_entities}

                self.assertIn("заказчик", roles)
                self.assertIn("поставщик", roles)
                self.assertEqual(raw_data["supplier"], enriched_contract["supplier"])
                self.assertEqual(raw_data["supplier_inn"], "0987654321")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
