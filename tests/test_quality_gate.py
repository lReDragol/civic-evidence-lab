import sqlite3
import tempfile
import unittest
from pathlib import Path

from quality.pipeline_gate import build_quality_gate
from runtime.state import set_runtime_metadata


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_quality_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


class QualityGateTests(unittest.TestCase):
    def test_quality_gate_fails_on_zero_support_duplicates_and_degraded_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "quality.db"
            report_path = Path(tmp) / "qa_quality_latest.json"
            create_quality_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO sources(id, name, category, url, is_active) VALUES
                        (1, 'Official', 'official_site', 'https://official.example.test', 1);

                    INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                        (101, 1, 'restriction_record', 'Restriction', 'Restriction body', 'raw_signal'),
                        (102, 1, 'post', 'CTA', 'Подпишитесь на наш канал', 'suppressed_template');

                    INSERT INTO content_tags(
                        id, content_item_id, tag_level, tag_name, namespace, normalized_tag,
                        confidence, confidence_calibrated, tag_source, decision_source
                    ) VALUES
                        (301, 101, 0, 'technology', 'topic', 'technology', 0.9, 0.9, 'classifier_v3', 'classifier_v3');

                    INSERT INTO claims(id, content_item_id, claim_text, status) VALUES
                        (401, 102, 'Политический лозунг', 'unverified');

                    INSERT INTO relation_candidates(
                        id, entity_a_id, entity_b_id, candidate_type, origin, score,
                        support_items, support_sources, support_domains, candidate_state, promotion_state
                    ) VALUES
                        (501, 1, 2, 'same_case_cluster', 'candidate_builder:hybrid', 0.55, 0, 0, 0, 'review', 'review');

                    INSERT INTO source_sync_state(source_key, state, quality_state, failure_class, last_error)
                    VALUES('government_news', 'degraded', 'degraded', 'timeout', 'ConnectTimeout: https://government.ru/news/');

                    INSERT INTO job_runs(job_id, trigger_mode, requested_by, owner, attempt_no, status, started_at, warnings_json)
                    VALUES('photo_backfill', 'manual', 'test', 'owner', 1, 'ok', '2026-04-26T00:00:00', '[\"map.svg\"]');
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
                    "quality_gate": {
                        "report_path": str(report_path),
                        "max_degraded_sources": 0,
                        "max_zero_support_review_candidates": 0,
                        "max_duplicate_leakage": 0,
                        "strict_gate": True,
                    },
                }
            )

            self.assertFalse(result["ok"])
            self.assertIn("relation_quality_gate_failed", result["fatal_errors"])
            self.assertIn("dedupe_leak_gate_failed", result["fatal_errors"])
            self.assertIn("source_health_gate_failed", result["fatal_errors"])
            self.assertTrue(report_path.exists())
            self.assertEqual(result["artifacts"]["relation_quality"]["zero_support_review_candidates"], 1)
            self.assertEqual(result["artifacts"]["dedupe_leakage"]["total"], 1)
            self.assertEqual(result["artifacts"]["source_health"]["degraded_count"], 1)
            self.assertEqual(result["artifacts"]["top_false_positive_tags"][0]["tag_name"], "technology")

    def test_quality_gate_passes_when_quality_signals_are_clean(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "quality.db"
            report_path = Path(tmp) / "qa_quality_latest.json"
            create_quality_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO job_runs(job_id, trigger_mode, requested_by, owner, attempt_no, status, started_at, warnings_json)
                    VALUES('tagger', 'manual', 'test', 'owner', 1, 'ok', '2026-04-26T00:00:00', '[]');
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
                    "quality_gate": {
                        "report_path": str(report_path),
                        "strict_gate": True,
                    },
                }
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["fatal_errors"], [])
            self.assertEqual(result["artifacts"]["relation_quality"]["zero_support_review_candidates"], 0)
            self.assertEqual(result["artifacts"]["dedupe_leakage"]["total"], 0)
            self.assertEqual(result["artifacts"]["source_health"]["degraded_count"], 0)

    def test_quality_gate_fails_on_latest_critical_warning_job_even_without_degraded_source_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "quality.db"
            report_path = Path(tmp) / "qa_quality_latest.json"
            create_quality_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO job_runs(job_id, trigger_mode, requested_by, owner, attempt_no, status, started_at, warnings_json)
                    VALUES
                        ('photo_backfill', 'manual', 'test', 'owner', 1, 'ok', '2026-04-26T00:00:00', '[\"map.svg\"]'),
                        ('tagger', 'manual', 'test', 'owner', 1, 'ok', '2026-04-26T00:00:00', '[]');
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
                    "quality_gate": {
                        "report_path": str(report_path),
                        "strict_gate": True,
                    },
                }
            )

            self.assertFalse(result["ok"])
            self.assertIn("source_health_gate_failed", result["fatal_errors"])
            self.assertEqual(len(result["artifacts"]["critical_warning_jobs"]), 1)
            self.assertEqual(result["artifacts"]["critical_warning_jobs"][0]["failure_class"], "bad_asset")


if __name__ == "__main__":
    unittest.main()
