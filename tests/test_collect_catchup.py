import sqlite3
import tempfile
import unittest
import json
from pathlib import Path
from unittest.mock import patch

from runtime import catchup
from runtime.registry import get_job_spec


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


class CollectCatchupTests(unittest.TestCase):
    def test_registry_exposes_manual_catchup_and_two_year_vote_job(self):
        collect_spec = get_job_spec("collect_catchup")
        votes_spec = get_job_spec("duma_votes_2y")

        self.assertIsNotNone(collect_spec)
        self.assertIsNotNone(votes_spec)
        self.assertFalse(collect_spec.scheduled)
        self.assertFalse(votes_spec.scheduled)
        self.assertEqual(collect_spec.name, "Сбор новых данных")

    def test_catchup_runs_configured_jobs_once_and_summarizes_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "catchup.db"
            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
                conn.commit()
            finally:
                conn.close()

            settings = {
                "db_path": str(db_path),
                "schema_path": str(SCHEMA_PATH),
                "ensure_schema_on_connect": True,
                "reports_dir": str(Path(tmp) / "reports"),
                "catchup_job_ids": ["telegram", "duma_votes_2y"],
            }
            calls = []

            def fake_run_job_once(job_id, **kwargs):
                calls.append((job_id, kwargs.get("trigger_mode"), kwargs.get("requested_by")))
                return {
                    "ok": True,
                    "items_seen": 10,
                    "items_new": 2 if job_id == "telegram" else 1,
                    "items_updated": 3,
                    "warnings": [],
                }

            with patch.object(catchup, "run_job_once", side_effect=fake_run_job_once):
                result = catchup.run_collect_catchup(settings)

            self.assertTrue(result["ok"])
            self.assertEqual([call[0] for call in calls], ["telegram", "duma_votes_2y"])
            self.assertTrue(all(call[1] == "catchup" for call in calls))
            self.assertEqual(result["items_seen"], 20)
            self.assertEqual(result["items_new"], 3)
            self.assertEqual(result["items_updated"], 6)

    def test_catchup_writes_report_and_fails_on_required_job_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "catchup.db"
            reports_dir = tmp_path / "reports"
            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
                conn.commit()
            finally:
                conn.close()

            settings = {
                "db_path": str(db_path),
                "schema_path": str(SCHEMA_PATH),
                "ensure_schema_on_connect": True,
                "reports_dir": str(reports_dir),
                "catchup_job_ids": ["telegram_public_fallback", "quality_gate"],
                "catchup_required_job_ids": ["telegram_public_fallback", "quality_gate"],
            }

            def fake_run_job_once(job_id, **kwargs):
                if job_id == "telegram_public_fallback":
                    return {
                        "ok": False,
                        "items_seen": 1,
                        "items_new": 0,
                        "items_updated": 0,
                        "fatal_errors": ["public_fallback_failed"],
                    }
                return {
                    "ok": True,
                    "items_seen": 0,
                    "items_new": 0,
                    "items_updated": 0,
                    "warnings": [],
                    "artifacts": {
                        "relation_quality": {
                            "promoted_same_case_cluster": 0,
                            "promoted_with_location_entity": 0,
                        }
                    },
                }

            with patch.object(catchup, "run_job_once", side_effect=fake_run_job_once):
                result = catchup.run_collect_catchup(settings)

            report_path = reports_dir / "collect_catchup_latest.json"
            self.assertFalse(result["ok"])
            self.assertIn("telegram_public_fallback", result["fatal_errors"][0])
            self.assertTrue(report_path.exists())
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertFalse(report["ok"])
            self.assertEqual(report["critical_failures"][0]["job_id"], "telegram_public_fallback")
            self.assertEqual(report["quality_summary"]["relation_quality"]["promoted_same_case_cluster"], 0)


if __name__ == "__main__":
    unittest.main()
