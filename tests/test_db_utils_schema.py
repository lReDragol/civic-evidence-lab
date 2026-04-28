import sqlite3
import tempfile
import unittest
from pathlib import Path

from config.db_utils import exec_schema


class DbUtilsSchemaTests(unittest.TestCase):
    def test_exec_schema_upgrades_legacy_db_before_new_indexes(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "legacy.db"
            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    PRAGMA foreign_keys = OFF;

                    CREATE TABLE sources (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        category TEXT NOT NULL
                    );

                    CREATE TABLE content_items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source_id INTEGER NOT NULL,
                        title TEXT,
                        body_text TEXT,
                        status TEXT DEFAULT 'raw_signal',
                        granular_processed INTEGER DEFAULT 0
                    );

                    CREATE TABLE claims (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        content_item_id INTEGER NOT NULL,
                        claim_text TEXT NOT NULL,
                        claim_type TEXT,
                        status TEXT DEFAULT 'unverified'
                    );

                    CREATE TABLE evidence_links (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        claim_id INTEGER NOT NULL,
                        evidence_item_id INTEGER,
                        evidence_type TEXT NOT NULL
                    );

                    CREATE TABLE content_tags (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        content_item_id INTEGER NOT NULL,
                        tag_level INTEGER NOT NULL,
                        tag_name TEXT NOT NULL,
                        confidence REAL DEFAULT 1.0,
                        tag_source TEXT DEFAULT 'rule'
                    );

                    CREATE TABLE relation_candidates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        entity_a_id INTEGER NOT NULL,
                        entity_b_id INTEGER NOT NULL,
                        candidate_type TEXT NOT NULL,
                        score REAL DEFAULT 0,
                        support_items INTEGER DEFAULT 0,
                        support_sources INTEGER DEFAULT 0,
                        support_domains INTEGER DEFAULT 0,
                        promotion_state TEXT DEFAULT 'pending'
                    );
                    """
                )
                conn.commit()

                exec_schema(conn)

                def columns(table_name: str) -> set[str]:
                    return {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}

                self.assertIn("canonical_hash", columns("claims"))
                self.assertIn("claim_cluster_id", columns("claims"))
                self.assertIn("evidence_class", columns("evidence_links"))
                self.assertIn("decision_source", columns("content_tags"))
                self.assertIn("candidate_state", columns("relation_candidates"))
                self.assertIn("promotion_block_reason", columns("relation_candidates"))
                self.assertIn("evidence_mix_json", columns("relation_candidates"))
                self.assertIn("support_class", columns("relation_support"))
                self.assertIn("quality_state", columns("source_sync_state"))
                self.assertIn("suppression_reason", columns("content_clusters"))
                self.assertIn("entity_quality_score", columns("relation_features"))
                self.assertIn("dedupe_support_score", columns("relation_features"))
                self.assertIn("real_host_diversity_score", columns("relation_features"))
                self.assertIn("bridge_diversity_score", columns("relation_features"))
                self.assertIn("campaign_id", columns("ai_work_items"))
                self.assertIn("prompt_version", columns("ai_work_items"))
                self.assertIn("input_hash", columns("ai_work_items"))
                self.assertIn("sample_bucket", columns("ai_work_items"))
                self.assertIn("campaign_id", columns("content_derivations"))
                self.assertIn("work_item_id", columns("content_derivations"))
                self.assertIn("is_current", columns("content_derivations"))
                self.assertIn("failure_kind", columns("ai_task_attempts"))
                self.assertIn("event_consistency_score", columns("relation_features"))
                self.assertIn("fact_support_score", columns("relation_features"))
                self.assertIn("official_bridge_score", columns("relation_features"))
                self.assertIn("telegram_penalty", columns("relation_features"))
                self.assertIn("event_id", columns("relation_support"))
                self.assertIn("fact_id", columns("relation_support"))

                self.assertTrue(
                    conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='content_tag_votes'").fetchone()
                )
                self.assertTrue(
                    conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='semantic_neighbors'").fetchone()
                )
                self.assertTrue(
                    conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='claim_clusters'").fetchone()
                )
                self.assertTrue(
                    conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='relation_features'").fetchone()
                )
                self.assertTrue(
                    conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='llm_keys'").fetchone()
                )
                self.assertTrue(
                    conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='llm_key_failures'").fetchone()
                )
                self.assertTrue(
                    conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='llm_provider_models'").fetchone()
                )
                self.assertTrue(
                    conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='llm_provider_health'").fetchone()
                )
                self.assertTrue(
                    conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ai_sweep_campaigns'").fetchone()
                )
                self.assertTrue(
                    conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ai_work_items'").fetchone()
                )
                self.assertTrue(
                    conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ai_task_attempts'").fetchone()
                )
                self.assertTrue(
                    conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_candidates'").fetchone()
                )
                self.assertTrue(
                    conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_merge_reviews'").fetchone()
                )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
