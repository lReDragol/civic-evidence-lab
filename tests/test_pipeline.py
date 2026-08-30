"""Tests for runtime/pipeline.py.

Mocks run_job_once to verify dependency resolution, stage ordering,
and pipeline result aggregation without real job side-effects.
"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.conftest import create_db


class TestPipeline(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / "test.db"
        create_db(self.db_path)

        self.settings = {
            "db_path": str(self.db_path),
            "ensure_schema_on_connect": False,
            "telegram_api_id": None,
            "telegram_api_hash": None,
        }

    def test_run_pipeline_unknown_mode_fails(self):
        from runtime.pipeline import run_pipeline
        result = run_pipeline("nonexistent_mode", settings=self.settings)
        self.assertFalse(result.get("ok"))
        self.assertIn("unknown_pipeline_mode:nonexistent_mode", result.get("fatal_errors", []))

    def test_run_pipeline_skips_dependent_jobs_when_dependency_fails(self):
        from runtime.pipeline import run_pipeline

        call_order = []
        def _fake_run_job_once(job_id, *, settings=None, **kwargs):
            call_order.append(job_id)
            if job_id == "tagger":
                return {"ok": False, "fatal_errors": ["tagger_broken"]}
            return {"ok": True, "items_new": 1}

        patches = [
            patch("runtime.pipeline.get_db", lambda _s=None: sqlite3.connect(str(self.db_path))),
            patch("runtime.pipeline.run_job_once", side_effect=_fake_run_job_once),
        ]
        stack = [cm.__enter__() for cm in patches]
        try:
            result = run_pipeline("nightly", settings=self.settings)
        finally:
            for cm in reversed(patches):
                cm.__exit__(None, None, None)

        # tagger failed, so semantic_index (depends_on tagger) should be skipped
        self.assertIn("tagger", call_order)
        self.assertNotIn("semantic_index", call_order)
        self.assertFalse(result.get("ok"))

    def test_run_pipeline_succeeds_when_all_stages_ok(self):
        from runtime.pipeline import run_pipeline

        def _fake_run_job_once(job_id, *, settings=None, **kwargs):
            return {"ok": True, "items_new": 5, "items_seen": 10}

        patches = [
            patch("runtime.pipeline.get_db", lambda _s=None: sqlite3.connect(str(self.db_path))),
            patch("runtime.pipeline.run_job_once", side_effect=_fake_run_job_once),
        ]
        stack = [cm.__enter__() for cm in patches]
        try:
            result = run_pipeline("nightly", settings=self.settings)
        finally:
            for cm in reversed(patches):
                cm.__exit__(None, None, None)

        self.assertTrue(result.get("ok"))
        self.assertGreater(result.get("items_new", 0), 0)
        self.assertIn("pipeline_version", result.get("artifacts", {}))

    def test_run_pipeline_records_pipeline_run_in_db(self):
        from runtime.pipeline import run_pipeline

        def _fake_run_job_once(job_id, *, settings=None, **kwargs):
            return {"ok": True, "items_new": 1}

        patches = [
            patch("runtime.pipeline.get_db", lambda _s=None: sqlite3.connect(str(self.db_path))),
            patch("runtime.pipeline.run_job_once", side_effect=_fake_run_job_once),
        ]
        stack = [cm.__enter__() for cm in patches]
        try:
            result = run_pipeline("weekly_maintenance", settings=self.settings)
        finally:
            for cm in reversed(patches):
                cm.__exit__(None, None, None)

        self.assertTrue(result.get("ok"))
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute("SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 1").fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["mode"], "weekly_maintenance")
            self.assertEqual(row["status"], "ok")
        finally:
            conn.close()

    def test_run_pipeline_generates_version(self):
        from runtime.pipeline import generate_pipeline_version
        version = generate_pipeline_version("nightly")
        self.assertTrue(version.startswith("nightly-"))
        self.assertGreater(len(version), len("nightly-"))


if __name__ == "__main__":
    unittest.main()
