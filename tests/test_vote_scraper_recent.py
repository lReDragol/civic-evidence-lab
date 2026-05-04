import sqlite3
import tempfile
import unittest
import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

from collectors import vote_scraper
from runtime import registry


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


class FakeResponse:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code


class FakeSession:
    def get(self, url, params=None, timeout=20):
        if url == vote_scraper.BASE_URL + "/":
            page = int((params or {}).get("page") or 1)
            if page == 1:
                return FakeResponse(
                    """
                    <html><body>
                    <a href="/vote/100">(первое чтение) О проекте № 1209693-8</a>
                    <a href="/vote/99">(третье чтение) Старый проект № 111111-8</a>
                    </body></html>
                    """
                )
            return FakeResponse("")
        if url.endswith("/vote/100"):
            return FakeResponse(
                """
                <html><body>
                  <h1>О проекте № 1209693-8 о дополнительных ограничениях</h1>
                  <div class="date-p">30.03.2026</div>
                  <div class="statis">За: 300 Против: 100 Воздержалось: 1 Не голосовало: 49 Кворум: 226 Принят</div>
                  <script>
                    deputiesData = [
                      {"sortName":"Иванов Иван Иванович","faction":"КПРФ","result":"against","url":"/deputy?deputy=777"}
                    ];
                  </script>
                </body></html>
                """
            )
        if url.endswith("/vote/99"):
            return FakeResponse(
                """
                <html><body>
                  <h1>О старом проекте № 111111-8</h1>
                  <div class="date-p">10.01.2020</div>
                  <div class="statis">За: 300 Против: 0 Воздержалось: 0 Не голосовало: 150 Принят</div>
                </body></html>
                """
            )
        return FakeResponse("", 404)


class FailingListSession:
    def get(self, url, params=None, timeout=20):
        if url == vote_scraper.BASE_URL + "/":
            raise TimeoutError("vote list timeout")
        return FakeResponse("", 404)


class VoteScraperRecentTests(unittest.TestCase):
    def _make_db(self, tmp_path: Path) -> Path:
        db_path = tmp_path / "votes.db"
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
            conn.execute(
                "INSERT INTO bills(id, number, title, status) VALUES(1, '1209693-8', 'О дополнительных ограничениях', 'registered')"
            )
            conn.execute(
                "INSERT INTO entities(id, entity_type, canonical_name) VALUES(10, 'person', 'Иванов Иван Иванович')"
            )
            conn.execute(
                "INSERT INTO deputy_profiles(entity_id, full_name, duma_id, faction, is_active) VALUES(10, 'Иванов Иван Иванович', 777, 'КПРФ', 1)"
            )
            conn.commit()
        finally:
            conn.close()
        return db_path

    def test_collect_votes_since_is_bounded_and_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._make_db(Path(tmp))
            settings = {"db_path": str(db_path), "schema_path": str(SCHEMA_PATH), "ensure_schema_on_connect": True}
            with patch.object(vote_scraper, "_session", return_value=FakeSession()):
                first = vote_scraper.collect_votes_since(
                    settings,
                    start_date=date(2024, 5, 2),
                    end_date=date(2026, 5, 2),
                    max_pages=1,
                )
                second = vote_scraper.collect_votes_since(
                    settings,
                    start_date=date(2024, 5, 2),
                    end_date=date(2026, 5, 2),
                    max_pages=1,
                )

            self.assertEqual(first["items_new"], 1)
            self.assertEqual(second["items_new"], 0)
            self.assertEqual(second["items_updated"], 1)

            conn = sqlite3.connect(db_path)
            try:
                sessions = conn.execute("SELECT COUNT(*) FROM bill_vote_sessions").fetchone()[0]
                votes = conn.execute("SELECT COUNT(*) FROM bill_votes WHERE deputy_name NOT LIKE 'Фракция:%'").fetchone()[0]
                stored = conn.execute(
                    """
                    SELECT bvs.external_vote_id, bvs.source_url, bv.external_vote_id, bv.source_url, bv.entity_id, bv.vote_result
                    FROM bill_vote_sessions bvs
                    JOIN bill_votes bv ON bv.vote_session_id=bvs.id
                    WHERE bv.deputy_name='Иванов Иван Иванович'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(sessions, 1)
            self.assertEqual(votes, 1)
            self.assertEqual(stored[0], "100")
            self.assertEqual(stored[1], "https://vote.duma.gov.ru/vote/100")
            self.assertEqual(stored[2], "100")
            self.assertEqual(stored[3], "https://vote.duma.gov.ru/vote/100")
            self.assertEqual(stored[4], 10)
            self.assertEqual(stored[5], "против")

    def test_collect_votes_since_reports_first_page_fetch_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._make_db(Path(tmp))
            settings = {"db_path": str(db_path), "schema_path": str(SCHEMA_PATH), "ensure_schema_on_connect": True}
            with patch.object(vote_scraper, "_session", return_value=FailingListSession()):
                result = vote_scraper.collect_votes_since(
                    settings,
                    start_date=date(2024, 5, 2),
                    end_date=date(2026, 5, 2),
                    max_pages=1,
                )

            self.assertFalse(result["ok"])
            self.assertIn("vote_list_fetch_failed:1", result["retriable_errors"][0])
            self.assertEqual(result["items_seen"], 0)

    def test_recent_vote_job_enqueues_source_review_when_live_and_fallback_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._make_db(Path(tmp))
            settings = {"db_path": str(db_path), "schema_path": str(SCHEMA_PATH), "ensure_schema_on_connect": True}
            primary = {
                "ok": False,
                "items_seen": 0,
                "items_new": 0,
                "items_updated": 0,
                "warnings": ["vote_list_fetch_failed:1:timeout"],
                "retriable_errors": ["vote_list_fetch_failed:1:timeout"],
                "artifacts": {"window_start": "2024-05-03", "window_end": "2026-05-03"},
            }
            with patch("collectors.vote_scraper.collect_votes_last_years", return_value=primary), patch(
                "collectors.duma_votes_scraper.collect_votes_for_bills",
                return_value=0,
            ):
                result = registry._duma_votes_2y(settings)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                task = conn.execute(
                    "SELECT queue_key, subject_type, suggested_action, candidate_payload FROM review_tasks WHERE task_key='source:votes:fallback_unresolved'"
                ).fetchone()
            finally:
                conn.close()

            self.assertFalse(result["ok"])
            self.assertIsNotNone(task)
            self.assertEqual(task["queue_key"], "sources")
            self.assertEqual(task["subject_type"], "source")
            self.assertEqual(task["suggested_action"], "add_vote_archive_or_fixture")
            payload = json.loads(task["candidate_payload"])
            self.assertEqual(payload["failure_class"], "timeout")
            self.assertEqual(payload["fallback_result"], 0)


if __name__ == "__main__":
    unittest.main()
