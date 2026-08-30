import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tests.conftest import create_db


class MetricsCollectorTests(unittest.TestCase):
    def test_collect_metrics_uses_ai_task_attempt_started_at(self):
        from runtime.metrics_collector import collect_metrics_snapshot

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "metrics.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    """
                    INSERT INTO ai_work_items(unit_kind, unit_key, stage, prompt_version, input_hash, status)
                    VALUES('content_item', 'content:1', 'structured_extract', 'v-test', 'hash-test', 'completed')
                    """
                )
                work_item_id = conn.execute("SELECT id FROM ai_work_items LIMIT 1").fetchone()[0]
                conn.execute(
                    """
                    INSERT INTO ai_task_attempts(work_item_id, provider, model_name, status, started_at)
                    VALUES(?, 'perplexity', 'sonar-pro', 'ok', datetime('now'))
                    """,
                    (work_item_id,),
                )
                conn.commit()
            finally:
                conn.close()

            result = collect_metrics_snapshot({"db_path": str(db_path), "ensure_schema_on_connect": True})
            self.assertTrue(result["ok"])

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT metric_value_json
                    FROM autonomy_test_metrics
                    WHERE metric_group='llm' AND metric_key='attempts_last_hour'
                    ORDER BY id DESC LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertIsNotNone(row)
            value = json.loads(row[0])
            self.assertEqual(value["perplexity/sonar-pro"]["ok"], 1)


if __name__ == "__main__":
    unittest.main()
