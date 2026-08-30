from __future__ import annotations

import sqlite3
import unittest

from db.migration_runner import apply_pending_migrations
from knowledge.projection import activate_generation, active_generation_id, mark_generation_validated, start_generation
from knowledge.revisions import record_source_revision, store_raw_item_with_revision


BASE_SCHEMA = """
CREATE TABLE sources(id INTEGER PRIMARY KEY, name TEXT, category TEXT);
CREATE TABLE raw_source_items(
    id INTEGER PRIMARY KEY, source_id INTEGER, external_id TEXT, raw_payload TEXT,
    collected_at TEXT, hash_sha256 TEXT, is_processed INTEGER DEFAULT 0,
    UNIQUE(source_id, external_id)
);
CREATE TABLE content_items(id INTEGER PRIMARY KEY, source_id INTEGER, raw_item_id INTEGER, body_text TEXT);
CREATE TABLE entities(id INTEGER PRIMARY KEY, entity_type TEXT, canonical_name TEXT);
CREATE TABLE events(id INTEGER PRIMARY KEY, superseded_at TEXT);
CREATE TABLE event_facts(id INTEGER PRIMARY KEY, event_id INTEGER, superseded_at TEXT);
CREATE TABLE event_entities(id INTEGER PRIMARY KEY, event_id INTEGER, superseded_at TEXT);
CREATE TABLE fact_evidence(id INTEGER PRIMARY KEY, fact_id INTEGER, content_item_id INTEGER);
CREATE TABLE entity_relations(id INTEGER PRIMARY KEY);
"""


class KnowledgeSpineTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(BASE_SCHEMA)
        apply_pending_migrations(self.conn)
        self.conn.execute("INSERT INTO sources(id,name,category) VALUES(1,'Source','media')")

    def tearDown(self):
        self.conn.close()

    def test_identical_payload_reuses_revision_and_changed_payload_appends(self):
        first = record_source_revision(
            self.conn, source_id=1, external_id="post:1", raw_payload={"text": "one"}
        )
        same = record_source_revision(
            self.conn, source_id=1, external_id="post:1", raw_payload={"text": "one"}
        )
        changed = record_source_revision(
            self.conn, source_id=1, external_id="post:1", raw_payload={"text": "two"}
        )

        self.assertTrue(first["created"])
        self.assertFalse(same["created"])
        self.assertEqual(2, changed["revision_no"])
        rows = self.conn.execute(
            "SELECT revision_no,is_current,supersedes_revision_id FROM source_revisions ORDER BY revision_no"
        ).fetchall()
        self.assertEqual([(1, 0, None), (2, 1, first["revision_id"])], rows)

    def test_changed_raw_item_is_marked_for_reprocessing(self):
        first = store_raw_item_with_revision(
            self.conn, source_id=1, external_id="post:2", raw_payload={"text": "one"}
        )
        self.conn.execute(
            "UPDATE raw_source_items SET is_processed=1 WHERE id=?", (first["raw_item_id"],)
        )
        second = store_raw_item_with_revision(
            self.conn, source_id=1, external_id="post:2", raw_payload={"text": "two"}
        )
        self.assertTrue(second["updated"])
        self.assertEqual(
            0,
            self.conn.execute(
                "SELECT is_processed FROM raw_source_items WHERE id=?", (first["raw_item_id"],)
            ).fetchone()[0],
        )

    def test_projection_activation_is_atomic_and_single_active(self):
        first = start_generation(self.conn, projection_type="relations", generation_key="r1")
        mark_generation_validated(self.conn, first, metrics={"assertions": 1})
        activate_generation(self.conn, first)
        second = start_generation(self.conn, projection_type="relations", generation_key="r2")
        mark_generation_validated(self.conn, second, metrics={"assertions": 2})
        activate_generation(self.conn, second)

        self.assertEqual(second, active_generation_id(self.conn, "relations"))
        states = self.conn.execute(
            "SELECT generation_key,status FROM projection_generations ORDER BY id"
        ).fetchall()
        self.assertEqual([("r1", "superseded"), ("r2", "active")], states)


if __name__ == "__main__":
    unittest.main()
