import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from enrichment.anticorruption_scraper import run_anticorruption_disclosures
from enrichment.state_company_reports import run_state_company_reports
from quality.pipeline_gate import build_quality_gate
from runtime.state import set_runtime_metadata
from tools.check_official_sources import check_sources


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


class FakeResponse:
    def __init__(self, *, status_code=200, text="", content=b"", headers=None, url="https://example.test/", json_data=None):
        self.status_code = status_code
        self.text = text
        self.content = content or text.encode("utf-8")
        self.headers = headers or {"content-type": "text/html; charset=utf-8"}
        self.url = url
        self._json_data = json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            error = requests.HTTPError(f"{self.status_code} Client Error: Not Found for url: {self.url}")
            error.response = self
            raise error

    def json(self):
        if self._json_data is None:
            raise ValueError("no json payload")
        return self._json_data


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


class SourceHealthFallbackTests(unittest.TestCase):
    def test_quality_gate_accepts_fixture_backed_degraded_source_from_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "quality.db"
            report_path = tmp_path / "qa_quality_latest.json"
            fixture_path = tmp_path / "government_news.html"
            manifest_path = tmp_path / "source_health_manifest.json"
            create_db(db_path)
            fixture_path.write_text(
                "<html><head><title>Правительство РФ — новости</title></head><body><a href='/news/1'>Новость</a></body></html>",
                encoding="utf-8",
            )
            manifest_path.write_text(
                json.dumps(
                    {
                        "government_news": {
                            "acceptance_mode": "fixture_ok",
                            "required_for_gate": True,
                            "primary_urls": ["https://government.ru/news/"],
                            "fallback_urls": ["http://government.ru/news/"],
                            "fixture_paths": [str(fixture_path)],
                            "fixture_strategy": "local_html",
                            "expected_cadence": "daily",
                            "quality_expectations": "html_listing",
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO source_sync_state(
                        source_key, state, quality_state, failure_class, last_error, metadata_json
                    ) VALUES(
                        'government_news', 'degraded', 'degraded', 'timeout',
                        'ConnectTimeout: https://government.ru/news/',
                        '{"fixture_sample":"https://government.ru/news/"}'
                    );

                    INSERT INTO source_fixtures(
                        source_key, fixture_kind, origin_url, local_path, checksum, captured_at, is_active, metadata_json
                    ) VALUES(
                        'government_news', 'local_fixture', 'https://government.ru/news/',
                        '"""
                    + str(fixture_path).replace("\\", "\\\\")
                    + """', 'checksum-government-news', '2026-04-26T12:00:00', 1,
                        '{"quality_expectations":"html_listing"}'
                    );
                    """
                )
                set_runtime_metadata(conn, "classifier_audit_last_status", "ok")
                set_runtime_metadata(conn, "classifier_audit_last_report", {"reviewed_baseline_ready": True})
                conn.commit()
            finally:
                conn.close()

            result = build_quality_gate(
                {
                    "db_path": str(db_path),
                    "ensure_schema_on_connect": True,
                    "source_health_manifest_path": str(manifest_path),
                    "quality_gate": {
                        "report_path": str(report_path),
                        "strict_gate": True,
                    },
                }
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["fatal_errors"], [])
            acceptance_rows = result["artifacts"]["source_acceptance"]["rows"]
            self.assertEqual(len(acceptance_rows), 1)
            self.assertEqual(acceptance_rows[0]["source_key"], "government_news")
            self.assertEqual(acceptance_rows[0]["effective_state"], "healthy_fixture")
            self.assertEqual(result["artifacts"]["fixture_backed_sources"], ["government_news"])
            self.assertEqual(result["artifacts"]["unresolved_blockers"], [])

    def test_check_sources_reports_fixture_smoke_for_degraded_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            fixture_path = tmp_path / "government_docs.html"
            manifest_path = tmp_path / "source_health_manifest.json"
            fixture_path.write_text(
                "<html><head><title>Документы Правительства РФ</title></head><body><a href='/docs/1'>Документ</a></body></html>",
                encoding="utf-8",
            )
            manifest_path.write_text(
                json.dumps(
                    {
                        "government_docs": {
                            "acceptance_mode": "fixture_ok",
                            "primary_urls": ["https://government.ru/docs/"],
                            "fallback_urls": ["http://government.ru/docs/"],
                            "fixture_paths": [str(fixture_path)],
                            "fixture_strategy": "local_html",
                            "expected_cadence": "daily",
                            "quality_expectations": "document_listing",
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            with patch("tools.check_official_sources.requests.get", side_effect=Exception("timeout")):
                report = check_sources(
                    timeout=1,
                    probes=[{"source": "government_docs", "url": "https://government.ru/docs/", "kind": "html"}],
                    settings={"source_health_manifest_path": str(manifest_path)},
                )

            self.assertEqual(report["failed"], 1)
            self.assertTrue(report["items"][0]["fixture_smoke"]["ok"])
            self.assertEqual(report["items"][0]["fixture_smoke"]["effective_state"], "healthy_fixture")

    def test_run_anticorruption_disclosures_uses_local_fixture_when_archive_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "enrichment.db"
            manifest_path = tmp_path / "source_health_manifest.json"
            fixture_path = tmp_path / "duma_2024.html"
            create_db(db_path)
            fixture_path.write_text(
                """
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
                    <td>ИВАНОВ Иван Иванович</td>
                    <td>депутат Государственной Думы</td>
                    <td>квартира</td>
                    <td>индивидуальная</td>
                    <td>42,0</td>
                    <td>Россия</td>
                    <td></td><td></td><td></td>
                    <td></td>
                    <td>123456,78</td>
                  </tr>
                </table>
                </body></html>
                """,
                encoding="utf-8",
            )
            manifest_path.write_text(
                json.dumps(
                    {
                        "duma_disclosures": {
                            "acceptance_mode": "archive_ok",
                            "required_for_gate": True,
                            "primary_urls": ["http://duma.gov.ru/duma/persons/properties/{year}/"],
                            "fallback_urls": {"2024": []},
                            "fixture_paths": {"2024": [str(fixture_path)]},
                            "fixture_strategy": "by_year",
                            "expected_cadence": "yearly",
                            "quality_expectations": "declaration_table",
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO entities(id, entity_type, canonical_name, description)
                    VALUES(201, 'person', 'ИВАНОВ Иван Иванович', 'Депутат');

                    INSERT INTO deputy_profiles(entity_id, full_name, position, is_active)
                    VALUES(201, 'ИВАНОВ Иван Иванович', 'депутат Государственной Думы', 1);
                    """
                )
                conn.commit()
            finally:
                conn.close()

            def fake_get(url, *args, **kwargs):
                if "web.archive.org/cdx" in url:
                    return FakeResponse(json_data=[["timestamp", "original", "statuscode"]], url=url)
                raise AssertionError(f"unexpected network fetch: {url}")

            with patch("enrichment.anticorruption_scraper.requests.get", side_effect=fake_get):
                result = run_anticorruption_disclosures(
                    {
                        "db_path": str(db_path),
                        "ensure_schema_on_connect": True,
                        "source_health_manifest_path": str(manifest_path),
                    },
                    years=(2024,),
                )

            conn = sqlite3.connect(db_path)
            try:
                disclosure = conn.execute(
                    """
                    SELECT source_type, source_url, metadata_json
                    FROM person_disclosures
                    WHERE entity_id=201 AND disclosure_year=2024
                    """
                ).fetchone()
                source_state = conn.execute(
                    """
                    SELECT state, quality_state, metadata_json
                    FROM source_sync_state
                    WHERE source_key='duma_disclosures:2024'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertEqual(result["warnings"], [])
            self.assertEqual(result["disclosures_created"], 1)
            self.assertEqual(disclosure[0], "official_archive")
            self.assertIn("archive_derived", disclosure[2])
            self.assertEqual(source_state[0], "ok")
            self.assertEqual(source_state[1], "ok")
            self.assertIn("fallback_used", source_state[2])

    def test_state_company_reports_uses_manifest_fixture_when_live_url_404(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "enrichment.db"
            manifest_path = tmp_path / "source_health_manifest.json"
            fixture_path = tmp_path / "rostec_management.html"
            create_db(db_path)
            fixture_path.write_text(
                """
                <html><head><title>Ростех — руководство</title></head>
                <body><main><h1>Руководство Ростех</h1><p>Сергей Чемезов — генеральный директор.</p></main></body></html>
                """,
                encoding="utf-8",
            )
            manifest_path.write_text(
                json.dumps(
                    {
                        "state_company_reports:Ростех": {
                            "acceptance_mode": "archive_ok",
                            "primary_urls": ["https://rostec.ru/about/management/"],
                            "fallback_urls": ["https://web.archive.org/web/20260301000000/https://rostec.ru/about/management/"],
                            "fixture_paths": [str(fixture_path)],
                            "fixture_strategy": "local_html",
                            "expected_cadence": "weekly",
                            "quality_expectations": "management_page",
                            "warning_match": ["Ростех:"],
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            def fake_get(url, *args, **kwargs):
                raise FakeResponse(status_code=404, url=url)

            with patch("enrichment.state_company_reports.requests.Session.get", side_effect=fake_get):
                result = run_state_company_reports(
                    {
                        "db_path": str(db_path),
                        "ensure_schema_on_connect": True,
                        "source_health_manifest_path": str(manifest_path),
                    },
                    targets=[
                        {
                            "name": "Ростех",
                            "url": "https://rostec.ru/about/management/",
                            "organization": "Ростех",
                            "source_key": "state_company_reports:Ростех",
                        }
                    ],
                )

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT content_type, title, body_text, status
                    FROM content_items
                    WHERE external_id='state-company:Ростех'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertEqual(result["warnings"], [])
            self.assertEqual(result["items_new"], 1)
            self.assertEqual(row[0], "state_company_report")
            self.assertIn("Чемезов", row[2])
            self.assertEqual(row[3], "official_document")


if __name__ == "__main__":
    unittest.main()
