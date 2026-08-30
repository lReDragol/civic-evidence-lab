"""End-to-end pipeline integration test.

Simulates a full pipeline run across collection → enrichment → analysis →
verification → graph stages using mocked job runners and verifies that:
- content_items are created
- entities and mentions are extracted
- claims are generated
- events are built
- relation candidates appear
- cases are constructed
"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.conftest import create_db


class EndToEndPipelineTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / "test.db"
        create_db(self.db_path)

        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            # Ensure relation_candidates exists (not always in base schema)
            conn.execute("DROP TABLE IF EXISTS relation_candidates")
            conn.execute(
                """
                CREATE TABLE relation_candidates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_a_id INTEGER NOT NULL,
                    entity_b_id INTEGER NOT NULL,
                    candidate_type TEXT NOT NULL,
                    candidate_state TEXT DEFAULT 'pending',
                    origin TEXT NOT NULL DEFAULT 'seed',
                    score REAL DEFAULT 0,
                    support_items INTEGER DEFAULT 0,
                    support_sources INTEGER DEFAULT 0,
                    support_domains INTEGER DEFAULT 0,
                    promotion_state TEXT DEFAULT 'pending'
                )
                """
            )
            # Seed source
            conn.execute("INSERT INTO sources(name, category, url, is_active) VALUES(?,?,?,?)",
                         ("TestRSS", "rss", "http://example.com/feed", 1))
            # Seed content items
            for i in range(3):
                conn.execute(
                    """INSERT INTO content_items(source_id, content_type, title, body_text, url, language, ner_processed, llm_processed, quotes_processed)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (1, "text", f"Title {i}", f"Body text about corruption and courts for item {i}.", f"http://ex/{i}", "ru", 0, 0, 0),
                )
            conn.commit()
        finally:
            conn.close()

        self.settings = {"db_path": str(self.db_path), "ensure_schema_on_connect": False}

    def _fake_run_job_once(self, job_id, *, settings=None, **kwargs):
        """Mock runner that directly manipulates the DB for key stages."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            if job_id == "watch_folder":
                return {"ok": True, "items_new": 0}
            if job_id == "rss":
                # Already seeded; pretend we collected
                return {"ok": True, "items_new": 3, "items_seen": 3}
            if job_id == "tagger":
                # Tag content items
                for row in conn.execute("SELECT id FROM content_items WHERE status='raw_signal'").fetchall():
                    conn.execute(
                        "INSERT OR IGNORE INTO content_tags(content_item_id, tag_name, tag_level, confidence, tag_source) VALUES(?,?,?,?,?)",
                        (row["id"], "corruption_claim", 1, 0.9, "rule"),
                    )
                conn.commit()
                return {"ok": True, "items_new": 3}
            if job_id == "ner":
                # Extract fake entities
                for row in conn.execute("SELECT id, title, body_text FROM content_items WHERE ner_processed=0").fetchall():
                    text = f"{row['title']} {row['body_text']}"
                    # Create two distinct entities so relation building has pairs
                    for ent_name in ["Иванов Иван", "Минфин"]:
                        ent_type = "person" if "Иванов" in ent_name else "organization"
                        conn.execute("INSERT OR IGNORE INTO entities(entity_type, canonical_name) VALUES(?,?)", (ent_type, ent_name))
                    eid = conn.execute("SELECT id FROM entities WHERE entity_type=? AND canonical_name=?", ("person", "Иванов Иван")).fetchone()["id"]
                    conn.execute(
                        "INSERT OR IGNORE INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES(?,?,?,?)",
                        (eid, row["id"], "person", 1.0),
                    )
                    conn.execute("UPDATE content_items SET ner_processed=1 WHERE id=?", (row["id"],))
                conn.commit()
                return {"ok": True, "items_new": 3}
            if job_id == "claims":
                # Generate claims from content items
                for row in conn.execute("SELECT id, body_text FROM content_items").fetchall():
                    conn.execute(
                        "INSERT INTO claims(content_item_id, claim_text, claim_type, status) VALUES(?,?,?,?)",
                        (row["id"], f"Claim about {row['body_text'][:50]}", "corruption", "unverified"),
                    )
                conn.commit()
                return {"ok": True, "items_new": 3}
            if job_id == "event_pipeline":
                # Build an event
                cur = conn.execute(
                    "INSERT INTO events(canonical_title, event_type, status) VALUES(?,?,?)",
                    ("Corruption case investigation", "corruption", "active"),
                )
                event_id = cur.lastrowid
                for row in conn.execute("SELECT id FROM content_items").fetchall():
                    conn.execute(
                        "INSERT OR IGNORE INTO event_items(event_id, content_item_id, item_role) VALUES(?,?,?)",
                        (event_id, row["id"], "origin"),
                    )
                conn.commit()
                return {"ok": True, "items_new": 1}
            if job_id == "relations":
                # Build relation candidates
                entities = conn.execute("SELECT id FROM entities").fetchall()
                if len(entities) >= 2:
                    conn.execute(
                        "INSERT INTO relation_candidates(entity_a_id, entity_b_id, candidate_type, candidate_state) VALUES(?,?,?,?)",
                        (entities[0]["id"], entities[1]["id"] if len(entities) > 1 else entities[0]["id"], "affiliation", "pending"),
                    )
                conn.commit()
                return {"ok": True, "items_new": 1}
            if job_id == "cases":
                # Build cases
                persons = conn.execute("SELECT id FROM entities WHERE entity_type='person'").fetchall()
                claims = conn.execute("SELECT id FROM claims").fetchall()
                if persons and claims:
                    cur = conn.execute(
                        "INSERT INTO cases(title, description, case_type, status) VALUES(?,?,?,?)",
                        ("Case: Иванов", "Auto-generated case", "corruption", "open"),
                    )
                    case_id = cur.lastrowid
                    for c in claims:
                        conn.execute(
                            "INSERT OR IGNORE INTO case_claims(case_id, claim_id) VALUES(?,?)",
                            (case_id, c["id"]),
                        )
                conn.commit()
                return {"ok": True, "items_new": 1}
            # Default pass-through for any other job
            return {"ok": True, "items_new": 0}
        finally:
            conn.close()

    def test_full_pipeline_creates_entities_events_cases(self):
        from runtime.pipeline import run_pipeline

        with patch("runtime.pipeline.get_db", lambda _s=None: sqlite3.connect(str(self.db_path))):
            with patch("runtime.pipeline.run_job_once", side_effect=self._fake_run_job_once):
                result = run_pipeline("nightly", settings=self.settings)

        self.assertTrue(result.get("ok"))

        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            entities = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
            mentions = conn.execute("SELECT COUNT(*) FROM entity_mentions").fetchone()[0]
            claims = conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
            events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            rels = conn.execute("SELECT COUNT(*) FROM relation_candidates").fetchone()[0]
            cases = conn.execute("SELECT COUNT(*) FROM cases").fetchone()[0]

            self.assertGreaterEqual(entities, 1, "Expected at least 1 entity")
            self.assertGreaterEqual(mentions, 1, "Expected at least 1 mention")
            self.assertGreaterEqual(claims, 1, "Expected at least 1 claim")
            self.assertGreaterEqual(events, 1, "Expected at least 1 event")
            self.assertGreaterEqual(rels, 1, "Expected at least 1 relation candidate")
            self.assertGreaterEqual(cases, 1, "Expected at least 1 case")
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
