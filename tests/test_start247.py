import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from config.db_utils import get_db
from runtime.state import DAEMON_JOB_ID, acquire_job_lease, get_runtime_metadata


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


class Start247Tests(unittest.TestCase):
    def test_start247_bootstraps_autostart_daemon_sessions_and_catchup(self):
        from runtime.start247 import ensure_247

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "news.db"
            create_db(db_path)
            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}
            started = []

            with patch("runtime.start247.task_scheduler.install_task", return_value={"ok": True, "install_mode": "task_scheduler"}), \
                patch("runtime.start247._spawn_detached", side_effect=lambda command: started.append(command) or {"ok": True, "pid": 1234}), \
                patch("runtime.start247.import_telegram_sessions", return_value={"active_count": 1, "sessions": []}), \
                patch("runtime.start247.assign_telegram_sources", return_value={"assigned_sources": 0, "assignments": {}}):
                result = ensure_247(settings, start_catchup=True)

            conn = get_db(settings)
            try:
                self.assertEqual(get_runtime_metadata(conn, "mode_247_enabled"), "True")
                self.assertEqual(get_runtime_metadata(conn, "mode_247_autostart_status"), "ok")
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertTrue(result["daemon"]["started"])
            self.assertTrue(result["catchup"]["started"])
            self.assertTrue(any("runtime.daemon" in json.dumps(command) for command in started))
            self.assertTrue(any("collect_catchup" in json.dumps(command) for command in started))

    def test_start247_does_not_spawn_second_daemon_when_lease_is_active(self):
        from runtime.start247 import ensure_247

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "news.db"
            create_db(db_path)
            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}
            conn = get_db(settings)
            try:
                acquire_job_lease(conn, DAEMON_JOB_ID, "daemon:test", ttl_seconds=3600)
            finally:
                conn.close()

            with patch("runtime.start247.task_scheduler.install_task", return_value={"ok": True}), \
                patch("runtime.start247._spawn_detached") as spawn, \
                patch("runtime.start247.import_telegram_sessions", return_value={"active_count": 0, "sessions": []}), \
                patch("runtime.start247.assign_telegram_sources", return_value={"assigned_sources": 0, "assignments": {}}):
                result = ensure_247(settings, start_catchup=False)

            spawn.assert_not_called()
            self.assertFalse(result["daemon"]["started"])
            self.assertEqual(result["daemon"]["status"], "already_running")

