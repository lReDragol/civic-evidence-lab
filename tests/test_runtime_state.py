import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from runtime.runner import _finalize_job_state, _heartbeat_loop, run_job_once
from runtime.state import (
    acquire_job_lease,
    active_job_lease,
    force_recover_job,
    finish_job_run,
    record_source_health_report,
    now_iso,
    parse_iso,
    recover_abandoned_runs,
    release_job_lease,
    request_daemon_stop,
    runtime_summary,
    set_runtime_metadata,
    start_job_run,
    get_runtime_metadata,
    update_source_sync_state,
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

    def test_force_recover_job_abandons_running_run_and_releases_lease(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                run_id = start_job_run(
                    conn,
                    job_id="telegram",
                    trigger_mode="manual",
                    requested_by="test",
                    owner="owner-a",
                )
                acquire_job_lease(conn, "telegram", "owner-a", ttl_seconds=600)
                stats = force_recover_job(conn, "telegram", reason="manual cleanup")
                row = conn.execute(
                    "SELECT status, error_summary FROM job_runs WHERE id=?",
                    (run_id,),
                ).fetchone()
                remaining = conn.execute(
                    "SELECT COUNT(*) FROM job_leases WHERE job_id='telegram'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(stats["abandoned_runs"], 1)
            self.assertEqual(stats["released_leases"], 1)
            self.assertEqual(row[0], "abandoned")
            self.assertIn("manual cleanup", row[1])
            self.assertEqual(remaining, 0)

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

    def test_heartbeat_loop_ignores_retriable_database_lock(self):
        class FakeConn:
            def close(self):
                return None

        class FakeStopEvent:
            def __init__(self):
                self.calls = 0

            def wait(self, _interval):
                self.calls += 1
                return self.calls > 1

        fake_event = FakeStopEvent()
        with patch("runtime.runner.get_db", return_value=FakeConn()), patch(
            "runtime.runner.heartbeat_job_lease",
            side_effect=sqlite3.OperationalError("database is locked"),
        ) as heartbeat_mock:
            _heartbeat_loop(fake_event, {}, "telegram", "owner-a", 60)

        heartbeat_mock.assert_called_once()

    def test_update_source_sync_state_persists_quality_state_and_failure_class(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                update_source_sync_state(
                    conn,
                    source_key="government_news",
                    success=False,
                    state="degraded",
                    transport_mode="healthcheck",
                    last_error="ConnectTimeout: example",
                    metadata={"fixture_sample": "https://government.ru/news/"},
                    quality_state="degraded",
                    quality_issue="timeout on primary transport",
                    failure_class="timeout",
                )
                row = conn.execute(
                    """
                    SELECT state, quality_state, quality_issue, failure_class, transport_mode, metadata_json
                    FROM source_sync_state
                    WHERE source_key='government_news'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(row[0], "degraded")
            self.assertEqual(row[1], "degraded")
            self.assertIn("timeout", row[2])
            self.assertEqual(row[3], "timeout")
            self.assertEqual(row[4], "healthcheck")
            self.assertIn("fixture_sample", row[5])

    def test_record_source_health_report_classifies_failure_and_quality_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    "INSERT INTO sources(id, name, category, url, is_active) VALUES(1, 'Gov', 'official_site', 'https://government.ru/news/', 1)"
                )
                conn.commit()

                stats = record_source_health_report(
                    conn,
                    {
                        "items": [
                            {
                                "source": "government_news",
                                "url": "https://government.ru/news/",
                                "ok": False,
                                "status": None,
                                "error": "ConnectTimeout: HTTPSConnectionPool(...)",
                                "checked_at": "2026-04-26T12:00:00",
                            }
                        ]
                    },
                    transport_mode="healthcheck",
                )
                row = conn.execute(
                    """
                    SELECT state, quality_state, failure_class, metadata_json
                    FROM source_sync_state
                    WHERE source_key='government_news'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(stats, {"inserted": 1, "degraded": 1})
            self.assertEqual(row[0], "degraded")
            self.assertEqual(row[1], "degraded")
            self.assertEqual(row[2], "timeout")
            self.assertIn("fixture_sample", row[3])

    def test_finalize_job_state_retries_when_runtime_metadata_hits_database_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "runtime.db"
            create_db(db_path)
            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}

            conn = sqlite3.connect(db_path)
            try:
                run_id = start_job_run(
                    conn,
                    job_id="dummy",
                    trigger_mode="manual",
                    requested_by="test",
                    owner="owner-a",
                )
                acquire_job_lease(conn, "dummy", "owner-a", ttl_seconds=300)
            finally:
                conn.close()

            spec = SimpleNamespace(id="dummy", source_keys=())
            result = {
                "ok": True,
                "job_id": "dummy",
                "started_at": "2026-04-26T12:00:00",
                "finished_at": "2026-04-26T12:00:01",
                "items_seen": 1,
                "items_new": 0,
                "items_updated": 0,
                "warnings": [],
                "retriable_errors": [],
                "fatal_errors": [],
                "artifacts": {},
            }

            from runtime import state as runtime_state_module

            call_state = {"calls": 0}

            def flaky_set_runtime_metadata(conn, key, value):
                call_state["calls"] += 1
                if call_state["calls"] == 1:
                    raise sqlite3.OperationalError("database is locked")
                return runtime_state_module.set_runtime_metadata(conn, key, value)

            with patch("runtime.runner.set_runtime_metadata", side_effect=flaky_set_runtime_metadata):
                _finalize_job_state(
                    settings,
                    job_id="dummy",
                    owner="owner-a",
                    run_id=run_id,
                    spec=spec,
                    result=result,
                    pipeline_version=None,
                )

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute("SELECT status, finished_at FROM job_runs WHERE id=?", (run_id,)).fetchone()
                lease_count = conn.execute("SELECT COUNT(*) FROM job_leases WHERE job_id='dummy'").fetchone()[0]
                last_finished = get_runtime_metadata(conn, "last_job_finished:dummy")
            finally:
                conn.close()

            self.assertEqual(row[0], "ok")
            self.assertEqual(lease_count, 0)
            self.assertEqual(last_finished, "2026-04-26T12:00:01")


if __name__ == "__main__":
    unittest.main()
