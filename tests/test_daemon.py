"""Smoke tests for runtime/daemon.py startup and shutdown.

These tests mock the scheduler and DB layer to verify lease management
and lifecycle without a real SQLite contention surface.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from tests.conftest import PROJECT_ROOT, SCHEMA_PATH, create_db


class FakeScheduler:
    """Minimal in-memory stand-in for APScheduler BackgroundScheduler."""

    def __init__(self, *args, **kwargs):
        self.jobs: list[dict] = []
        self._started = False
        self.job_defaults = kwargs.get("job_defaults", {})

    def add_job(self, func, trigger, *, kwargs=None, id=None, replace_existing=True, name=None):
        self.jobs.append({"func": func, "trigger": trigger, "kwargs": kwargs, "id": id, "name": name})

    def start(self):
        self._started = True

    def shutdown(self, *, wait=False):
        self._started = False


def _make_fake_get_db(db_path: Path):
    def _get_db(_settings=None):
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        return conn
    return _get_db


class TestDaemonLifecycle(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / "test.db"
        create_db(self.db_path)

        self.settings = {
            "db_path": str(self.db_path),
            "analysis_db_path": str(Path(self.tmp.name) / "analysis.db"),
            "log_level": "WARNING",
            "telegram_api_id": None,
            "telegram_api_hash": None,
            "ensure_schema_on_connect": False,
        }

    def _common_patches(self):
        """Return a list of patch() context managers for safe daemon invocation.

        IMPORTANT: we must patch names as they appear in runtime.daemon
        because most state functions are imported via
        ``from runtime.state import x`` (local binding).
        """
        fake_get_db = _make_fake_get_db(self.db_path)
        return [
            patch("runtime.daemon.BackgroundScheduler", FakeScheduler),
            patch("runtime.daemon.get_db", fake_get_db),
            patch("config.db_utils.load_settings", lambda: self.settings),
            patch("config.db_utils.setup_logging", lambda _s=None: None),
            patch("config.db_utils.ensure_dirs", lambda _s=None: None),
            patch("config.db_utils.exec_schema", lambda conn, schema_path=None: None),
            patch("db.migrate_v3.migrate", lambda conn: None),
            patch("runtime.daemon.recover_abandoned_runs", lambda conn: {"abandoned_runs": 0, "released_leases": 0}),
            # Local imports inside runtime.daemon
            patch("runtime.daemon.daemon_stop_requested", return_value=True),
            patch("runtime.daemon.request_daemon_stop", lambda conn, value=True: None),
            patch("runtime.daemon.acquire_job_lease", lambda conn, job_id, owner, **kw: True),
            patch("runtime.daemon.active_job_lease", lambda conn, job_id: None),
            patch("runtime.daemon.release_job_lease", lambda conn, job_id, owner: None),
            patch("time.sleep", lambda _s: None),
            patch("runtime.daemon.run_job_once", lambda *args, **kwargs: {"ok": True, "items_new": 0}),
        ]

    def _enter_patches(self):
        self._cm_stack = list(self._common_patches())
        for cm in self._cm_stack:
            cm.__enter__()

    def _exit_patches(self):
        for cm in reversed(self._cm_stack):
            cm.__exit__(None, None, None)

    def test_daemon_acquires_lease_and_returns_ok(self):
        from runtime.daemon import run_daemon
        self._enter_patches()
        try:
            result = run_daemon(no_preflight=True)
            self.assertTrue(result.get("ok"))
            self.assertIn("daemon_owner", result)
        finally:
            self._exit_patches()

    def test_daemon_preflight_runs_source_health(self):
        from runtime.daemon import run_daemon
        calls = []
        def _capture_run_job_once(job_id, *, settings=None, **kwargs):
            calls.append(job_id)
            return {"ok": True, "items_new": 0}

        patches = self._common_patches()
        # Replace the run_job_once patch with our capturing version
        # The run_job_once patch is the last one in _common_patches().
        patches[-1] = patch("runtime.daemon.run_job_once", side_effect=_capture_run_job_once)
        stack = [cm.__enter__() for cm in patches]
        try:
            result = run_daemon(no_preflight=False)
            self.assertTrue(result.get("ok"))
            self.assertIn("source_health", calls)
        finally:
            for cm in reversed(patches):
                cm.__exit__(None, None, None)

    def test_daemon_second_instance_refuses_lease(self):
        from runtime.daemon import run_daemon
        call_count = [0]
        def _counted_acquire(conn, job_id, owner, **kw):
            call_count[0] += 1
            return call_count[0] == 1

        patches = self._common_patches()
        # Replace acquire_job_lease patch (index -4 before sleep, -5 before release)
        # Order: ..., acquire_job_lease, active_job_lease, release_job_lease, sleep, run_job_once
        # So acquire is at index -5.
        patches[-5] = patch("runtime.daemon.acquire_job_lease", side_effect=_counted_acquire)
        # Also need active_job_lease to return something for second call
        patches[-4] = patch(
            "runtime.daemon.active_job_lease",
            return_value={"lease_owner": "other:123", "expires_at": "2099-01-01T00:00:00"},
        )
        stack = [cm.__enter__() for cm in patches]
        try:
            first = run_daemon(no_preflight=True)
            self.assertTrue(first.get("ok"))

            second = run_daemon(no_preflight=True)
            self.assertFalse(second.get("ok"))
            self.assertEqual(second.get("error"), "daemon_already_running")
        finally:
            for cm in reversed(patches):
                cm.__exit__(None, None, None)

    def test_daemon_main_exits_zero_on_success(self):
        from runtime.daemon import main
        import io

        self._enter_patches()
        try:
            with patch("sys.argv", ["daemon", "--no-preflight"]):
                with patch("sys.stdout", new_callable=io.StringIO) as stdout:
                    with self.assertRaises(SystemExit) as cm:
                        main()
                    self.assertEqual(cm.exception.code, 0)
                    output = stdout.getvalue()
                    parsed = json.loads(output)
                    self.assertTrue(parsed.get("ok"))
        finally:
            self._exit_patches()


if __name__ == "__main__":
    unittest.main()
