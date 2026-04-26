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
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
