import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from config.db_utils import SCHEMA_PATH


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


class FakeResponse:
    def __init__(self, *, status_code=200, text="", content=b"", headers=None, url="https://example.test/file"):
        self.status_code = status_code
        self.text = text
        self.content = content
        self.headers = headers or {}
        self.url = url

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")


class EnrichmentRolloutTests(unittest.TestCase):
    def test_find_person_entity_matches_surname_first_disclosure_name(self):
        from enrichment.common import find_person_entity

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "enrichment.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO entities(id, entity_type, canonical_name, description)
                    VALUES(401, 'person', 'Вячеслав Володин', 'Председатель Государственной Думы');

                    INSERT INTO deputy_profiles(entity_id, full_name, position, is_active)
                    VALUES(401, 'Вячеслав Володин', 'Председатель Государственной Думы', 1);
                    """
                )
                conn.commit()
                matched = find_person_entity(conn, "ВОЛОДИН Вячеслав Викторович")
            finally:
                conn.close()

            self.assertEqual(matched, 401)

    def test_content_dedupe_clusters_duplicate_titles_and_creates_review_task(self):
        from enrichment.content_dedupe import run_content_dedupe

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "enrichment.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO sources(id, name, category, url, is_active)
                    VALUES
                        (1, 'Source A', 'telegram', 'https://t.me/a', 1),
                        (2, 'Source B', 'media', 'https://example.test/b', 1);

                    INSERT INTO content_items(id, source_id, external_id, content_type, title, body_text, published_at, url, status)
                    VALUES
                        (11, 1, 'dup-1', 'post', 'Москва подпишитесь на наш канал!', 'Повторяющийся сигнал про Москву и рекламу канала.', '2026-04-20', 'https://t.me/a/11', 'raw_signal'),
                        (12, 2, 'dup-2', 'article', 'Москва. Подпишитесь на наш канал', 'Повторяющийся сигнал про Москву и рекламу канала', '2026-04-21', 'https://example.test/b/12', 'raw_signal'),
                        (13, 2, 'unique-1', 'article', 'Уникальная новость', 'Совершенно другой материал.', '2026-04-21', 'https://example.test/b/13', 'raw_signal');
                    """
                )
                conn.commit()
            finally:
                conn.close()

            result = run_content_dedupe({"db_path": str(db_path), "ensure_schema_on_connect": True}, min_cluster_size=2)

            conn = sqlite3.connect(db_path)
            try:
                clusters = conn.execute("SELECT cluster_key, canonical_content_id, item_count FROM content_clusters").fetchall()
                cluster_items = conn.execute(
                    "SELECT cluster_id, content_item_id, is_canonical FROM content_cluster_items ORDER BY content_item_id"
                ).fetchall()
                review_rows = conn.execute(
                    "SELECT queue_key, subject_type, suggested_action, status FROM review_tasks ORDER BY id"
                ).fetchall()
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertEqual(result["clusters_created"], 1)
            self.assertEqual(len(clusters), 1)
            self.assertEqual(clusters[0][2], 2)
            self.assertEqual([(row[1], row[2]) for row in cluster_items], [(11, 1), (12, 0)])
            self.assertEqual(len(review_rows), 1)
            self.assertEqual(review_rows[0][0], "content_duplicates")
            self.assertEqual(review_rows[0][1], "content_cluster")
            self.assertEqual(review_rows[0][2], "merge")
            self.assertEqual(review_rows[0][3], "open")

    def test_photo_backfill_materializes_entity_media_and_profile_content(self):
        from enrichment.photo_backfill import run_photo_backfill

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "enrichment.db"
            create_db(db_path)
            processed_documents = Path(tmp) / "processed" / "documents"
            processed_documents.mkdir(parents=True, exist_ok=True)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO sources(id, name, category, subcategory, url, is_active, is_official)
                    VALUES(1, 'Карточки депутатов', 'official_site', 'parliament', 'https://duma.gov.ru/duma/deputies/', 1, 1);

                    INSERT INTO entities(id, entity_type, canonical_name, description)
                    VALUES(101, 'person', 'Иванов Иван Иванович', 'Депутат Государственной Думы');

                    INSERT INTO deputy_profiles(
                        entity_id, full_name, position, biography_url, photo_url, is_active
                    ) VALUES(
                        101,
                        'Иванов Иван Иванович',
                        'Депутат Государственной Думы',
                        'https://duma.gov.ru/deputies/101/',
                        'https://cdn.example.test/photos/ivanov.jpg',
                        1
                    );
                    """
                )
                conn.commit()
            finally:
                conn.close()

            def fake_get(url, timeout=None, headers=None, **kwargs):
                if url.endswith(".jpg"):
                    return FakeResponse(
                        status_code=200,
                        content=b"\x89PNG\r\nfake-image",
                        headers={"Content-Type": "image/jpeg"},
                        url=url,
                    )
                return FakeResponse(status_code=200, text="<html><h1>Иванов</h1></html>", url=url)

            with patch("enrichment.photo_backfill.requests.Session.get", side_effect=fake_get):
                result = run_photo_backfill(
                    {
                        "db_path": str(db_path),
                        "ensure_schema_on_connect": True,
                        "processed_documents": str(processed_documents),
                    },
                    limit=20,
                )

            conn = sqlite3.connect(db_path)
            try:
                entity_media = conn.execute(
                    "SELECT entity_id, media_kind, is_primary FROM entity_media"
                ).fetchall()
                attachments = conn.execute(
                    "SELECT attachment_type, mime_type, file_path FROM attachments"
                ).fetchall()
                content_rows = conn.execute(
                    "SELECT content_type, title FROM content_items ORDER BY id"
                ).fetchall()
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertEqual(result["items_new"], 1)
            self.assertEqual(len(entity_media), 1)
            self.assertEqual(entity_media[0][0], 101)
            self.assertEqual(entity_media[0][1], "photo")
            self.assertEqual(entity_media[0][2], 1)
            self.assertEqual(len(attachments), 1)
            self.assertEqual(attachments[0][0], "photo")
            self.assertEqual(attachments[0][1], "image/jpeg")
            self.assertTrue(Path(attachments[0][2]).exists())
            self.assertEqual(content_rows[0][0], "deputy_profile")

    def test_photo_backfill_resolves_photo_from_profile_page_when_photo_url_missing(self):
        from enrichment.photo_backfill import run_photo_backfill

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "enrichment.db"
            create_db(db_path)
            processed_documents = Path(tmp) / "processed" / "documents"
            processed_documents.mkdir(parents=True, exist_ok=True)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO sources(id, name, category, subcategory, url, is_active, is_official)
                    VALUES(1, 'Руководство Минфина', 'official_site', 'executive_directory', 'https://example.test/minfin', 1, 1);

                    INSERT INTO entities(id, entity_type, canonical_name, description)
                    VALUES(111, 'person', 'Петров Пётр Петрович', 'Заместитель министра');

                    INSERT INTO deputy_profiles(
                        entity_id, full_name, position, biography_url, photo_url, is_active
                    ) VALUES(
                        111,
                        'Петров Пётр Петрович',
                        'Заместитель министра',
                        'https://example.test/profiles/petrov',
                        '',
                        1
                    );
                    """
                )
                conn.commit()
            finally:
                conn.close()

            profile_html = """
            <html>
              <body>
                <h1>Петров Пётр Петрович</h1>
                <div>Заместитель министра</div>
                <img alt="Петров Пётр Петрович" src="/media/petrov.jpg" />
              </body>
            </html>
            """

            def fake_get(url, timeout=None, headers=None, **kwargs):
                if url.endswith("/media/petrov.jpg"):
                    return FakeResponse(
                        status_code=200,
                        content=b"\x89PNG\r\npetrov-image",
                        headers={"Content-Type": "image/jpeg"},
                        url=url,
                    )
                return FakeResponse(status_code=200, text=profile_html, url=url)

            with patch("enrichment.photo_backfill.requests.Session.get", side_effect=fake_get):
                result = run_photo_backfill(
                    {
                        "db_path": str(db_path),
                        "ensure_schema_on_connect": True,
                        "processed_documents": str(processed_documents),
                    },
                    limit=20,
                )

            conn = sqlite3.connect(db_path)
            try:
                media_row = conn.execute(
                    "SELECT entity_id, media_kind FROM entity_media WHERE entity_id=111"
                ).fetchone()
                photo_url = conn.execute(
                    "SELECT photo_url FROM deputy_profiles WHERE entity_id=111"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertEqual(result["items_new"], 1)
            self.assertIsNotNone(media_row)
            self.assertEqual(media_row[0], 111)
            self.assertEqual(media_row[1], "photo")
            self.assertTrue(photo_url.endswith("/media/petrov.jpg"))

    def test_anticorruption_ingest_duma_archive_creates_disclosures_assets_and_income_fact(self):
        from enrichment.anticorruption_scraper import ingest_duma_property_html

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "enrichment.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO sources(id, name, category, subcategory, url, is_active, is_official)
                    VALUES(31, 'Сведения о доходах депутатов', 'official_site', 'anticorruption', 'https://duma.gov.ru/anticorruption/', 1, 1);

                    INSERT INTO entities(id, entity_type, canonical_name, description)
                    VALUES(201, 'person', 'АВДЕЕВ Александр Александрович', 'Депутат Государственной Думы');

                    INSERT INTO deputy_profiles(entity_id, full_name, position, faction, is_active)
                    VALUES(201, 'АВДЕЕВ Александр Александрович', 'член комитета Государственной Думы', 'ЕР', 1);
                    """
                )
                conn.commit()

                html = """
                <html><body>
                <table>
                  <tr>
                    <th>№</th><th>ФИО</th><th>Должность</th>
                    <th>Собственность</th><th>Вид собственности</th><th>Площадь</th><th>Страна</th>
                    <th>Пользование</th><th>Площадь</th><th>Страна</th>
                    <th>Транспорт</th><th>Доход</th>
                  </tr>
                  <tr>
                    <td>1</td>
                    <td>АВДЕЕВ Александр Александрович</td>
                    <td>член комитета Государственной Думы</td>
                    <td>квартира</td>
                    <td>индивидуальная</td>
                    <td>48,60</td>
                    <td>Россия</td>
                    <td>квартира (наём)</td>
                    <td>119,00</td>
                    <td>Россия</td>
                    <td>автомобили легковые: Chevrolet Niva</td>
                    <td>5611777,86</td>
                  </tr>
                  <tr>
                    <td></td>
                    <td>супруга</td>
                    <td></td>
                    <td>земельный участок</td>
                    <td>индивидуальная</td>
                    <td>1085,00</td>
                    <td>Россия</td>
                    <td></td>
                    <td></td>
                    <td></td>
                    <td>автомобили легковые: Volkswagen Golf Plus</td>
                    <td>3621084,87</td>
                  </tr>
                </table>
                </body></html>
                """
                result = ingest_duma_property_html(
                    conn,
                    source_id=31,
                    html=html,
                    year=2020,
                    page_url="https://web.archive.org/web/20210418195118/http://duma.gov.ru/duma/persons/properties/2020/",
                )
                conn.commit()

                disclosures = conn.execute(
                    "SELECT entity_id, disclosure_year, income_amount, raw_income_text FROM person_disclosures"
                ).fetchall()
                assets = conn.execute(
                    "SELECT owner_role, asset_type, country FROM declared_assets ORDER BY id"
                ).fetchall()
                compensation = conn.execute(
                    "SELECT entity_id, compensation_year, amount, fact_type FROM compensation_facts"
                ).fetchall()
                review = conn.execute(
                    "SELECT queue_key, suggested_action FROM review_tasks WHERE queue_key='assets_affiliations'"
                ).fetchall()
                content_rows = conn.execute(
                    "SELECT content_type, title FROM content_items WHERE content_type='anticorruption_declaration'"
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(result["disclosures_created"], 1)
            self.assertEqual(result["assets_created"], 3)
            self.assertEqual(len(disclosures), 1)
            self.assertEqual(disclosures[0][0], 201)
            self.assertEqual(disclosures[0][1], 2020)
            self.assertAlmostEqual(disclosures[0][2], 5611777.86, places=2)
            self.assertEqual(disclosures[0][3], "5611777,86")
            self.assertEqual(len(assets), 3)
            self.assertEqual(assets[0][0], "self")
            self.assertEqual(assets[1][0], "self")
            self.assertEqual(assets[2][0], "spouse")
            self.assertTrue(any(item[1] == 2020 and item[3] == "income" for item in compensation))
            self.assertTrue(review)
            self.assertEqual(len(content_rows), 1)

    def test_company_registry_enrichment_extracts_affiliation_from_profile_biography(self):
        from enrichment.company_registry_enrichment import run_company_registry_enrichment

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "enrichment.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO sources(id, name, category, subcategory, url, is_active, is_official)
                    VALUES(1, 'Карточки Минфина', 'official_site', 'executive_directory', 'https://example.test/minfin', 1, 1);

                    INSERT INTO entities(id, entity_type, canonical_name, description)
                    VALUES
                        (301, 'person', 'Чебесков Иван Александрович', 'Заместитель Министра'),
                        (302, 'organization', 'ВТБ Капитал', 'Финансовая организация');

                    INSERT INTO raw_source_items(id, source_id, external_id, raw_payload, hash_sha256)
                    VALUES(
                        401, 1, 'dossier:official_profile:301',
                        '{"entity_id":301,"full_name":"Чебесков Иван Александрович"}',
                        'hash-profile-301'
                    );

                    INSERT INTO content_items(
                        id, source_id, raw_item_id, external_id, content_type, title, body_text, published_at, url, status
                    ) VALUES(
                        402, 1, 401, 'dossier:official_profile:301', 'official_profile',
                        'Чебесков Иван Александрович',
                        '2009–2013 ВТБ Капитал, директор по продажам акций. 2023–по н.в. Заместитель Министра финансов.',
                        '2026-04-25',
                        'https://example.test/minfin/chebeskov',
                        'official_document'
                    );
                    """
                )
                conn.commit()
            finally:
                conn.close()

            with patch("collectors.official_scraper.egrul_collect_by_inn_list", return_value=0):
                result = run_company_registry_enrichment(
                    {"db_path": str(db_path), "ensure_schema_on_connect": True},
                    limit=20,
                )

            conn = sqlite3.connect(db_path)
            try:
                affiliations = conn.execute(
                    """
                    SELECT entity_id, company_entity_id, company_name, role_type, role_title, source_content_id, evidence_class
                    FROM company_affiliations
                    ORDER BY id
                    """
                ).fetchall()
                tasks = conn.execute(
                    "SELECT queue_key, subject_type, suggested_action FROM review_tasks WHERE queue_key='assets_affiliations'"
                ).fetchall()
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertEqual(len(affiliations), 1)
            self.assertEqual(affiliations[0][0], 301)
            self.assertEqual(affiliations[0][1], 302)
            self.assertEqual(affiliations[0][2], "ВТБ Капитал")
            self.assertEqual(affiliations[0][3], "director")
            self.assertIn("директор", affiliations[0][4])
            self.assertEqual(affiliations[0][5], 402)
            self.assertEqual(affiliations[0][6], "support")
            self.assertTrue(tasks)

    def test_company_registry_enrichment_trims_overlapping_company_tail_noise(self):
        from enrichment.company_registry_enrichment import run_company_registry_enrichment

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "enrichment.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO sources(id, name, category, subcategory, url, is_active, is_official)
                    VALUES(1, 'Карточки Минфина', 'official_site', 'executive_directory', 'https://example.test/minfin', 1, 1);

                    INSERT INTO entities(id, entity_type, canonical_name, description)
                    VALUES(311, 'person', 'Моисеев Алексей Владимирович', 'Заместитель министра');

                    INSERT INTO raw_source_items(id, source_id, external_id, raw_payload, hash_sha256)
                    VALUES(
                        411, 1, 'dossier:official_profile:311',
                        '{"entity_id":311,"full_name":"Моисеев Алексей Владимирович"}',
                        'hash-profile-311'
                    );

                    INSERT INTO content_items(
                        id, source_id, raw_item_id, external_id, content_type, title, body_text, published_at, url, status
                    ) VALUES(
                        412, 1, 411, 'dossier:official_profile:311', 'official_profile',
                        'Моисеев Алексей Владимирович',
                        '2001–2010 Ренессанс Капитал – Финансовый Консультант, экономист. 2010–2012 Заместитель руководителя Аналитического департамента.',
                        '2026-04-25',
                        'https://example.test/minfin/moiseev',
                        'official_document'
                    );
                    """
                )
                conn.commit()
            finally:
                conn.close()

            with patch("collectors.official_scraper.egrul_collect_by_inn_list", return_value=0):
                result = run_company_registry_enrichment(
                    {"db_path": str(db_path), "ensure_schema_on_connect": True},
                    limit=20,
                )

            conn = sqlite3.connect(db_path)
            try:
                affiliations = conn.execute(
                    """
                    SELECT company_name, role_type, role_title, period_start, period_end
                    FROM company_affiliations
                    ORDER BY id
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertEqual(len(affiliations), 1)
            self.assertEqual(affiliations[0][0], "Ренессанс Капитал")
            self.assertEqual(affiliations[0][1], "employee")
            self.assertEqual(affiliations[0][2], "экономист")
            self.assertEqual(affiliations[0][3], "2001")
            self.assertEqual(affiliations[0][4], "2010")

    def test_review_pack_roundtrip_updates_review_tasks(self):
        from enrichment.review_packs import export_review_pack, import_review_pack

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "enrichment.db"
            create_db(db_path)
            pack_path = Path(tmp) / "review_pack.csv"

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO review_tasks(
                        task_key, queue_key, subject_type, subject_id, candidate_payload,
                        suggested_action, confidence, machine_reason, source_links_json, status
                    ) VALUES(
                        'content-cluster-1',
                        'content_duplicates',
                        'content_cluster',
                        1,
                        '{"cluster_key":"dup-title","items":[11,12]}',
                        'merge',
                        0.91,
                        'Exact normalized title duplicate',
                        '["https://example.test/a","https://example.test/b"]',
                        'open'
                    );
                    """
                )
                conn.commit()
            finally:
                conn.close()

            export_result = export_review_pack(
                {"db_path": str(db_path), "ensure_schema_on_connect": True},
                queue_key="content_duplicates",
                csv_path=pack_path,
            )
            self.assertTrue(export_result["ok"])
            self.assertTrue(pack_path.exists())

            rows = list(csv.DictReader(pack_path.read_text(encoding="utf-8").splitlines()))
            self.assertEqual(len(rows), 1)
            rows[0]["status"] = "resolved"
            rows[0]["reviewer"] = "editor-1"
            rows[0]["resolution_notes"] = "Merged duplicate Telegram repost"
            with pack_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

            import_result = import_review_pack(
                {"db_path": str(db_path), "ensure_schema_on_connect": True},
                csv_path=pack_path,
            )
            self.assertTrue(import_result["ok"])

            conn = sqlite3.connect(db_path)
            try:
                task_row = conn.execute(
                    "SELECT status, reviewer, resolution_notes, review_pack_id FROM review_tasks WHERE task_key='content-cluster-1'"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(task_row[0], "resolved")
            self.assertEqual(task_row[1], "editor-1")
            self.assertIn("Merged duplicate", task_row[2])
            self.assertTrue(task_row[3])

    def test_relation_rebuild_enriched_is_idempotent_for_affiliations_and_restrictions(self):
        from enrichment.relation_rebuild import run_relation_rebuild_enriched

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "enrichment.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO entities(id, entity_type, canonical_name, description)
                    VALUES
                        (1001, 'person', 'Иван Иванов', 'Чиновник'),
                        (1002, 'organization', 'Госкомпания', 'Организация'),
                        (1003, 'organization', 'Ведомство', 'Орган власти'),
                        (1004, 'person', 'Петров Петр', 'Цель ограничения');

                    INSERT INTO company_affiliations(
                        entity_id, company_entity_id, company_name, role_type, role_title, source_content_id, evidence_class
                    ) VALUES(
                        1001, 1002, 'Госкомпания', 'director', 'Генеральный директор', NULL, 'support'
                    );

                    INSERT INTO restriction_events(
                        issuer_entity_id, target_entity_id, restriction_type, source_content_id, evidence_class
                    ) VALUES(
                        1003, 1004, 'internet_block', NULL, 'hard'
                    );
                    """
                )
                conn.commit()
            finally:
                conn.close()

            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}
            first = run_relation_rebuild_enriched(settings)
            second = run_relation_rebuild_enriched(settings)

            conn = sqlite3.connect(db_path)
            try:
                relation_rows = conn.execute(
                    """
                    SELECT from_entity_id, to_entity_id, relation_type, detected_by, COUNT(*)
                    FROM entity_relations
                    GROUP BY from_entity_id, to_entity_id, relation_type, detected_by
                    ORDER BY relation_type, detected_by
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertTrue(first["ok"])
            self.assertTrue(second["ok"])
            self.assertEqual(first["items_new"], 2)
            self.assertEqual(second["items_new"], 2)
            self.assertEqual(
                relation_rows,
                [
                    (1001, 1002, "head_of", "company_affiliations", 1),
                    (1003, 1004, "restricted", "restriction_events", 1),
                ],
            )

    def test_restriction_corpus_resolves_target_entity_and_review_subject(self):
        from enrichment.restriction_corpus import build_restriction_corpus

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "enrichment.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO sources(id, name, category, subcategory, url, is_active, is_official)
                    VALUES(71, 'Реестр иноагентов', 'official_site', 'restrictions', 'https://minjust.gov.ru/ru/pages/reestr-inostryannykh-agentov/', 1, 1);

                    INSERT INTO entities(id, entity_type, canonical_name, description)
                    VALUES
                        (801, 'organization', 'Министерство юстиции Российской Федерации', 'Орган власти'),
                        (802, 'person', 'Савва Михаил', 'Фигура из реестра');

                    INSERT INTO content_items(id, source_id, external_id, content_type, title, body_text, published_at, url, status)
                    VALUES(
                        900, 71, 'restr-1', 'restriction_record',
                        'Иноагент: Савва Михаил Валентинович',
                        'Минюст включил в реестр иноагентов.', '2026-04-25',
                        'https://minjust.gov.ru/ru/pages/reestr-inostryannykh-agentov/', 'official_document'
                    );
                    """
                )
                conn.commit()
            finally:
                conn.close()

            result = build_restriction_corpus(
                {"db_path": str(db_path), "ensure_schema_on_connect": True},
                limit=20,
            )

            conn = sqlite3.connect(db_path)
            try:
                event_row = conn.execute(
                    "SELECT issuer_entity_id, target_entity_id, restriction_type FROM restriction_events WHERE source_content_id=900"
                ).fetchone()
                review_row = conn.execute(
                    "SELECT subject_type, subject_id FROM review_tasks WHERE task_key='restriction:900'"
                ).fetchone()
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertEqual(event_row[2], "foreign_agent_registry")
            self.assertIsNotNone(event_row[1])
            self.assertEqual(review_row[0], "restriction_event")
            self.assertGreater(review_row[1], 0)


if __name__ == "__main__":
    unittest.main()
