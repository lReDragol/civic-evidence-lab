import io
import json
import logging
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from config.db_utils import exec_schema, get_db, setup_logging
from runtime.state import record_dead_letter, start_job_run


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        exec_schema(conn, SCHEMA_PATH)
        conn.commit()
    finally:
        conn.close()


class ObservabilityMonitoringTests(unittest.TestCase):
    def test_observability_schema_adds_runtime_columns_and_tables(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "obs.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
                self.assertIn("runtime_events", tables)
                self.assertIn("processing_skips", tables)

                job_cols = {row[1] for row in conn.execute("PRAGMA table_info(job_runs)")}
                self.assertIn("duration_ms", job_cols)
                self.assertIn("items_skipped", job_cols)
                self.assertIn("duplicate_items", job_cols)

                source_cols = {row[1] for row in conn.execute("PRAGMA table_info(source_sync_state)")}
                self.assertIn("current_job_id", source_cols)
                self.assertIn("current_telegram_session", source_cols)

                session_cols = {row[1] for row in conn.execute("PRAGMA table_info(telegram_sessions)")}
                self.assertIn("current_channel", session_cols)
                self.assertIn("collecting_now", session_cols)

                item_cols = {row[1] for row in conn.execute("PRAGMA table_info(content_items)")}
                self.assertIn("body_hash", item_cols)
            finally:
                conn.close()

    def test_setup_logging_is_idempotent_and_includes_context_fields(self):
        from runtime.logging_context import clear_log_context, log_context

        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "log.log"
            settings = {"log_file": str(log_path), "log_level": "INFO"}
            setup_logging(settings)
            setup_logging(settings)
            root = logging.getLogger()
            managed = [h for h in root.handlers if getattr(h, "_news_archive_handler", False)]
            self.assertEqual(len(managed), 2)

            with log_context(job_run_id=42, source_key="telegram:1", telegram_session="s1", model="m1"):
                logging.getLogger("tests.observability").info("context check")
            clear_log_context()

            text = log_path.read_text(encoding="utf-8")
            self.assertEqual(text.count("context check"), 1)
            self.assertIn("job_run_id=42", text)
            self.assertIn("source_key=telegram:1", text)
            self.assertIn("telegram_session=s1", text)
            self.assertIn("model=m1", text)

    def test_record_dead_letter_also_records_runtime_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "obs.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                dead_id = record_dead_letter(
                    conn,
                    failure_stage="ocr_missing_attachment",
                    source_key="telegram:1",
                    content_item_id=10,
                    error_type="FileNotFoundError",
                    error_message="missing file",
                )
                row = conn.execute(
                    """
                    SELECT level, event_type, stage, source_key, content_item_id, error_type, message
                    FROM runtime_events
                    ORDER BY id DESC LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertGreater(dead_id, 0)
            self.assertEqual(row["level"], "error")
            self.assertEqual(row["event_type"], "dead_letter")
            self.assertEqual(row["stage"], "ocr_missing_attachment")
            self.assertEqual(row["source_key"], "telegram:1")
            self.assertEqual(row["content_item_id"], 10)
            self.assertEqual(row["error_type"], "FileNotFoundError")
            self.assertIn("missing file", row["message"])

    def test_monitoring_payload_reports_funnel_models_sources_duplicates_and_coverage(self):
        from runtime.monitoring import get_full_monitoring_payload

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "obs.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                conn.executescript(
                    """
                    INSERT INTO sources(id, name, category, url, is_active)
                    VALUES
                        (1, 'YEP', 'telegram', 'https://t.me/yep_news', 1),
                        (2, 'RSS', 'rss', 'https://example.test/rss', 1);
                    INSERT INTO raw_source_items(source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed)
                    VALUES
                        (1, '100', '{}', '2026-05-14T10:00:00', 'h1', 1),
                        (1, '101', '{}', '2026-05-16T10:00:00', 'h2', 1);
                    INSERT INTO content_items(source_id, raw_item_id, external_id, title, body_text, body_hash, published_at, collected_at, status, ner_processed, llm_processed, classification_v3_processed)
                    VALUES
                        (1, 1, '100', 'A', 'same text', 'dup-hash', '2026-05-14T10:00:00', '2026-05-14T10:01:00', 'active', 1, 1, 1),
                        (1, 2, '101', 'B', 'same text', 'dup-hash', '2026-05-16T10:00:00', '2026-05-16T10:01:00', 'active', 0, 1, 0);
                    INSERT INTO telegram_sessions(session_key, client_type, session_path, status, assigned_count, collecting_now, current_channel, collected_current_run, collected_today, duplicates_skipped, heartbeat_at)
                    VALUES('232354072', 'telethon', 's.session', 'active', 1, 1, 'yep_news', 2, 2, 1, '2026-05-16T10:03:00');
                    INSERT INTO ai_task_attempts(provider, model_name, status, failure_kind, task_type, latency_ms, started_at, finished_at)
                    VALUES
                        ('mistral', 'mistral-medium', 'ok', NULL, 'structured_extract', 100, '2026-05-16T10:00:00', '2026-05-16T10:00:01'),
                        ('mistral', 'mistral-medium', 'failed', 'timeout', 'structured_extract', 300, '2026-05-16T10:01:00', '2026-05-16T10:01:01');
                    INSERT INTO job_runs(job_id, status, started_at, finished_at, items_seen, items_new, items_skipped, items_failed, duplicate_items)
                    VALUES('telegram_telethon_pool', 'ok', '2026-05-16T10:00:00', '2026-05-16T10:01:00', 2, 2, 0, 0, 1);
                    """
                )
                conn.commit()
                payload = get_full_monitoring_payload(conn)
            finally:
                conn.close()

            self.assertEqual(payload["overview"]["collected_total"], 2)
            self.assertEqual(payload["coverage"]["earliest_item_date"], "2026-05-14T10:00:00")
            self.assertGreaterEqual(payload["coverage"]["coverage_days"], 2)
            self.assertEqual(payload["telegram_sessions"][0]["current_channel"], "yep_news")
            self.assertEqual(payload["models"][0]["provider"], "mistral")
            self.assertEqual(payload["funnel"]["raw_collected"]["count"], 2)
            self.assertGreater(payload["duplicates"]["duplicate_body_hashes"], 0)

    def test_start247_dry_run_does_not_write_runtime_metadata(self):
        from runtime.start247 import ensure_247

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "obs.db"
            create_db(db_path)
            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}

            with patch("runtime.start247.task_scheduler.install_task") as install_task, patch(
                "runtime.start247._spawn_detached"
            ) as spawn:
                result = ensure_247(settings, dry_run=True)

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    "SELECT value_text FROM runtime_metadata WHERE key='mode_247_enabled'"
                ).fetchone()
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertTrue(result["dry_run"])
            self.assertIsNone(row)
            install_task.assert_called_once()
            spawn.assert_not_called()


if __name__ == "__main__":
    unittest.main()
