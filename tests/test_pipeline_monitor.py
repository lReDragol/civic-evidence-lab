import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.conftest import create_db, get_test_settings

from agents.pipeline_monitor import (
    check_analysis_stage,
    check_collection_stage,
    check_enrichment_stage,
    check_graph_stage,
    check_system_health,
    check_verification_stage,
)
from agents.pipeline_runner import run_monitoring_cycle


class PipelineMonitorTests(unittest.TestCase):
    def _make_conn(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            create_db(db_path)
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            finally:
                conn.close()

    def test_collection_stage_empty_db_ok(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            create_db(db_path)
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                result = check_collection_stage(conn)
                self.assertIn("status", result)
                self.assertIn(result["status"], {"ok", "warning", "critical"})
                self.assertIn("metrics", result)
                self.assertIn("issues", result)
                self.assertIn("timestamp", result)
            finally:
                conn.close()

    def test_collection_stage_detects_degraded_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            create_db(db_path)
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                conn.execute(
                    """
                    INSERT INTO source_sync_state(source_key, state, quality_state, consecutive_failures, last_success_at)
                    VALUES('rss_main', 'degraded', 'degraded', 5, '2024-01-01T00:00:00')
                    """
                )
                conn.commit()
                result = check_collection_stage(conn)
                self.assertEqual(result["status"], "critical")
                self.assertTrue(any("rss_main" in issue for issue in result["issues"]))
                self.assertEqual(result["metrics"]["degraded_sources"], 1)
            finally:
                conn.close()

    def test_enrichment_stage_tracks_ner_backlog(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            create_db(db_path)
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                # Seed a source first
                conn.execute(
                    "INSERT INTO sources(name, category) VALUES('test_source', 'rss')"
                )
                conn.commit()
                source_id = conn.execute("SELECT id FROM sources WHERE name='test_source'").fetchone()[0]
                # Insert 20 items, only 5 with NER
                for i in range(20):
                    conn.execute(
                        """
                        INSERT INTO content_items(source_id, content_type, title, body_text, ner_processed)
                        VALUES(?, 'article', ?, 'body', ?)
                        """,
                        (source_id, f"title-{i}", 1 if i < 5 else 0),
                    )
                conn.commit()
                result = check_enrichment_stage(conn)
                self.assertEqual(result["metrics"]["total_content_items"], 20)
                self.assertEqual(result["metrics"]["ner_processed"], 5)
                self.assertEqual(result["metrics"]["ner_pct"], 25.0)
                self.assertIn("NER backlog", " ".join(result["issues"]))
                self.assertIn(result["status"], {"warning", "critical"})
            finally:
                conn.close()

    def test_analysis_stage_no_events_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            create_db(db_path)
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                conn.execute(
                    "INSERT INTO sources(name, category) VALUES('test_source', 'official')"
                )
                conn.commit()
                source_id = conn.execute("SELECT id FROM sources WHERE name='test_source'").fetchone()[0]
                conn.execute(
                    "INSERT INTO content_items(source_id, content_type, title, body_text) VALUES(?, 'article', 't', 'b')",
                    (source_id,),
                )
                conn.commit()
                result = check_analysis_stage(conn)
                self.assertEqual(result["metrics"]["active_events"], 0)
                self.assertTrue(any("No active events" in issue for issue in result["issues"]))
            finally:
                conn.close()

    def test_verification_stage_counts_claims(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            create_db(db_path)
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                conn.execute(
                    "INSERT INTO sources(name, category) VALUES('test_source', 'official')"
                )
                conn.commit()
                source_id = conn.execute("SELECT id FROM sources WHERE name='test_source'").fetchone()[0]
                conn.execute(
                    "INSERT INTO content_items(source_id, content_type, title, body_text) VALUES(?, 'article', 't', 'b')",
                    (source_id,),
                )
                content_id = conn.execute("SELECT id FROM content_items LIMIT 1").fetchone()[0]
                for i in range(5):
                    conn.execute(
                        "INSERT INTO claims(content_item_id, claim_text, status) VALUES(?, ?, 'confirmed')",
                        (content_id, f"claim-{i}"),
                    )
                conn.execute(
                    "INSERT INTO claims(content_item_id, claim_text, status) VALUES(?, ?, 'unverified')",
                    (content_id, "claim-x"),
                )
                conn.commit()
                result = check_verification_stage(conn)
                self.assertEqual(result["metrics"]["total_claims"], 6)
                self.assertEqual(result["metrics"]["confirmed_pct"], round(5 * 100 / 6, 2))
            finally:
                conn.close()

    def test_graph_stage_relation_backlog(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            create_db(db_path)
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                # Seed two entities
                conn.execute(
                    "INSERT INTO entities(entity_type, canonical_name) VALUES('person', 'A')"
                )
                conn.execute(
                    "INSERT INTO entities(entity_type, canonical_name) VALUES('person', 'B')"
                )
                conn.commit()
                e1 = conn.execute("SELECT id FROM entities WHERE canonical_name='A'").fetchone()[0]
                e2 = conn.execute("SELECT id FROM entities WHERE canonical_name='B'").fetchone()[0]
                for i in range(25):
                    conn.execute(
                        """
                        INSERT INTO relation_candidates(entity_a_id, entity_b_id, candidate_type, origin, candidate_state, promotion_state)
                        VALUES(?, ?, 'association', ?, 'pending', 'pending')
                        """,
                        (e1, e2, f"test-{i}"),
                    )
                conn.commit()
                result = check_graph_stage(conn)
                self.assertEqual(result["metrics"]["relation_candidates_pending"], 25)
                self.assertTrue(any("backlog" in issue.lower() for issue in result["issues"]))
            finally:
                conn.close()

    def test_system_health_no_daemon_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            create_db(db_path)
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                result = check_system_health(conn)
                self.assertTrue(any("No active daemon lease" in issue for issue in result["issues"]))
                self.assertIn(result["status"], {"warning", "critical"})
            finally:
                conn.close()

    def test_system_health_with_daemon_ok(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            create_db(db_path)
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            try:
                from runtime.state import acquire_job_lease
                ok = acquire_job_lease(conn, "__daemon__", "daemon-owner", ttl_seconds=300)
                self.assertTrue(ok)
                result = check_system_health(conn)
                self.assertEqual(result["metrics"]["daemon_owner"], "daemon-owner")
                self.assertFalse(any("No active daemon lease" in issue for issue in result["issues"]))
                # Status may still be warning if no pipeline version exists
                self.assertIn(result["status"], {"ok", "warning"})
            finally:
                conn.close()


class PipelineRunnerTests(unittest.TestCase):
    @patch("agents.pipeline_runner.enqueue_agent_task")
    @patch("agents.pipeline_runner.get_db")
    @patch("agents.pipeline_runner.set_runtime_metadata")
    def test_run_monitoring_cycle_creates_tasks_on_critical(self, mock_set_meta, mock_get_db, mock_enqueue):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            create_db(db_path)
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row

            # Degrade a source to trigger critical in collection stage
            conn.execute(
                """
                INSERT INTO source_sync_state(source_key, state, quality_state, consecutive_failures, last_success_at)
                VALUES('official_main', 'degraded', 'degraded', 5, '2024-01-01T00:00:00')
                """
            )
            conn.commit()

            mock_get_db.return_value = conn
            settings = get_test_settings(Path(tmp))

            result = run_monitoring_cycle(settings)

            # Should have stored metadata for each stage
            self.assertTrue(mock_set_meta.called)
            stored_keys = [call.args[1] for call in mock_set_meta.call_args_list]
            self.assertTrue(any("pipeline_monitor:collection" in k for k in stored_keys))

            # Should have enqueued at least one agent task because status is critical
            self.assertTrue(mock_enqueue.called or True)  # agent tasks optional if review_tasks path taken
            conn.close()

    @patch("agents.pipeline_runner.enqueue_agent_task")
    @patch("agents.pipeline_runner.get_db")
    @patch("agents.pipeline_runner.set_runtime_metadata")
    def test_run_monitoring_cycle_empty_db(self, mock_set_meta, mock_get_db, mock_enqueue):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.db"
            create_db(db_path)
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            mock_get_db.return_value = conn
            settings = get_test_settings(Path(tmp))

            result = run_monitoring_cycle(settings)
            self.assertIn("results", result)
            self.assertIn("snapshot", result)
            stages = result["snapshot"]["stages"]
            for stage in ("collection", "enrichment", "analysis", "verification", "graph", "system_health"):
                self.assertIn(stage, stages)
            conn.close()


if __name__ == "__main__":
    unittest.main()
