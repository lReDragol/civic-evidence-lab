import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from runtime.runner import run_job_once
from runtime.state import (
    acquire_job_lease,
    active_job_lease,
    finish_job_run,
    now_iso,
    parse_iso,
    recover_abandoned_runs,
    release_job_lease,
    request_daemon_stop,
    runtime_summary,
    set_runtime_metadata,
    start_job_run,
    get_runtime_metadata,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


class RuntimeStateTests(unittest.TestCase):
    def test_now_iso_returns_naive_utc_string_and_roundtrips(self):
        stamp = now_iso()
        self.assertNotIn("+", stamp)
        parsed = parse_iso(stamp)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.utcoffset().total_seconds(), 0)

    def test_job_lease_acquire_release_cycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                self.assertTrue(acquire_job_lease(conn, "telegram", "owner-a", ttl_seconds=60))
                lease = active_job_lease(conn, "telegram")
                self.assertIsNotNone(lease)
                self.assertEqual(lease["lease_owner"], "owner-a")
                self.assertFalse(acquire_job_lease(conn, "telegram", "owner-b", ttl_seconds=60))
                release_job_lease(conn, "telegram", "owner-a")
                self.assertIsNone(active_job_lease(conn, "telegram"))
            finally:
                conn.close()

    def test_recover_abandoned_running_job_marks_run_and_releases_stale_lease(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.row_factory = sqlite3.Row
                run_id = start_job_run(
                    conn,
                    job_id="rss",
                    trigger_mode="scheduled",
                    requested_by="test",
                    owner="owner-a",
                )
                conn.execute(
                    "UPDATE job_runs SET started_at='2000-01-01T00:00:00' WHERE id=?",
                    (run_id,),
                )
                conn.execute(
                    """
                    INSERT INTO job_leases(job_id, lease_owner, started_at, heartbeat_at, expires_at)
                    VALUES('rss', 'owner-a', '2000-01-01T00:00:00', '2000-01-01T00:00:00', '2000-01-01T00:00:01')
                    """
                )
                conn.commit()

                stats = recover_abandoned_runs(conn, stale_seconds=10)
                status = conn.execute("SELECT status FROM job_runs WHERE id=?", (run_id,)).fetchone()[0]
                lease_count = conn.execute("SELECT COUNT(*) FROM job_leases WHERE job_id='rss'").fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(stats["abandoned_runs"], 1)
            self.assertEqual(stats["released_leases"], 1)
            self.assertEqual(status, "abandoned")
            self.assertEqual(lease_count, 0)

    def test_active_job_lease_ignores_expired_lease_and_runtime_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.row_factory = sqlite3.Row
                conn.execute(
                    """
                    INSERT INTO job_leases(job_id, lease_owner, started_at, heartbeat_at, expires_at)
                    VALUES('__daemon__', 'daemon:test', '2000-01-01T00:00:00', '2000-01-01T00:00:00', '2000-01-01T00:00:01')
                    """
                )
                conn.commit()
                self.assertIsNone(active_job_lease(conn, "__daemon__"))

                summary = runtime_summary(conn)
            finally:
                conn.close()

            self.assertFalse(summary["daemon_running"])
            self.assertEqual(summary["running_jobs"], 0)

    def test_runtime_metadata_roundtrip_and_daemon_stop_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                set_runtime_metadata(conn, "current_pipeline_version", "nightly-20260425")
                request_daemon_stop(conn, True)
                version = get_runtime_metadata(conn, "current_pipeline_version")
                stop_flag = get_runtime_metadata(conn, "daemon_stop_requested")
            finally:
                conn.close()

            self.assertEqual(version, "nightly-20260425")
            self.assertTrue(stop_flag)

    def test_run_job_once_returns_active_lease_failure_without_crash(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            create_db(db_path)
            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}

            conn = sqlite3.connect(db_path)
            try:
                self.assertTrue(acquire_job_lease(conn, "dummy", "owner-a", ttl_seconds=300))
            finally:
                conn.close()

            spec = SimpleNamespace(id="dummy", timeout_seconds=60, source_keys=())
            with patch("runtime.runner.get_job_spec", return_value=spec):
                result = run_job_once("dummy", settings=settings, owner="owner-b")

            self.assertFalse(result["ok"])
            self.assertEqual(result["job_id"], "dummy")
            self.assertIn("active_lease:dummy", result["retriable_errors"])


if __name__ == "__main__":
    unittest.main()
