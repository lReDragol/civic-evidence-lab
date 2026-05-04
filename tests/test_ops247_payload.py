import sqlite3
import tempfile
import unittest
from pathlib import Path

from ui.web_bridge import DashboardDataService


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO runtime_metadata(key, value_text, updated_at)
            VALUES
                ('mode_247_enabled', 'True', '2026-05-03T10:00:00'),
                ('mode_247_autostart_status', 'ok', '2026-05-03T10:00:00'),
                ('last_collect_catchup_finished_at', '2026-05-03T10:05:00', '2026-05-03T10:05:00');
            INSERT INTO job_leases(job_id, lease_owner, expires_at, heartbeat_at, payload_json)
            VALUES('__daemon__', 'daemon:test', '2999-01-01T00:00:00', '2026-05-03T10:00:00', '{}');
            INSERT INTO job_runs(job_id, status, started_at, finished_at, items_new, error_summary)
            VALUES
                ('telegram_telethon_pool', 'ok', '2026-05-03T10:00:00', '2026-05-03T10:01:00', 12, NULL),
                ('quality_gate', 'failed', '2026-05-03T10:02:00', '2026-05-03T10:03:00', 0, 'source blocked');
            INSERT INTO telegram_sessions(session_key, client_type, session_path, status, assigned_count, failure_class)
            VALUES
                ('232354072', 'telethon', 'config/telegram_test_sessions/232354072_telethon.session', 'active', 4, NULL),
                ('news_collector', 'pyrogram', 'config/news_collector.session', 'failed', 0, 'unauthorized_session');
            INSERT INTO llm_keys(provider, api_key, key_hash, status)
            VALUES
                ('mistral', 'a', 'h1', 'active'),
                ('mistral', 'b', 'h2', 'active'),
                ('perplexity', 'c', 'h3', 'cooldown'),
                ('groq', 'd', 'h4', 'removed');
            INSERT INTO llm_provider_health(provider, status, active_key_count)
            VALUES('mistral', 'ok', 2), ('perplexity', 'warning', 0);
            INSERT INTO ai_work_items(id, unit_kind, unit_key, stage, status)
            VALUES(1, 'content_item', 'content:1', 'structured_extract', 'completed');
            INSERT INTO ai_task_attempts(work_item_id, provider, status, failure_kind)
            VALUES
                (1, 'mistral', 'ok', NULL),
                (1, 'perplexity', 'failed', 'timeout');
            """
        )
        conn.commit()
    finally:
        conn.close()


class Ops247PayloadTests(unittest.TestCase):
    def test_ops247_payload_exposes_runtime_telegram_ai_quality_and_error_logs(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ops.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                payload = DashboardDataService(conn, {}).screen_payload("ops247")
            finally:
                conn.close()

            self.assertTrue(payload["runtime"]["enabled"])
            self.assertTrue(payload["runtime"]["daemon_running"])
            self.assertEqual(payload["telegram"]["active_sessions"], 1)
            self.assertEqual(payload["telegram"]["failed_sessions"], 1)
            self.assertEqual(payload["ai"]["keys"]["active"], 2)
            self.assertEqual(payload["ai"]["failure_kinds"]["timeout"], 1)
            self.assertTrue(any(log["level"] == "error" for log in payload["logs"]))
