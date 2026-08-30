"""Tests for cases/builder.py.

Creates synthetic entities, claims, and mentions, then verifies
that build_cases_from_entities groups them into cases.
"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.conftest import create_db


class TestCasesBuilder(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / "test.db"
        create_db(self.db_path)

        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            # Source
            conn.execute("INSERT INTO sources(name, category) VALUES(?,?)", ("Test", "rss"))
            # Content items
            conn.execute(
                "INSERT INTO content_items(source_id, content_type, title, body_text, url, language) VALUES(?,?,?,?,?,?)",
                (1, "text", "Title 1", "Body one", "http://a", "ru"),
            )
            conn.execute(
                "INSERT INTO content_items(source_id, content_type, title, body_text, url, language) VALUES(?,?,?,?,?,?)",
                (1, "text", "Title 2", "Body two", "http://b", "ru"),
            )
            # Claims linked to content items
            conn.execute(
                "INSERT INTO claims(content_item_id, claim_type, claim_text, status) VALUES(?,?,?,?)",
                (1, "corruption", "Иванов получил взятку в размере 500000 рублей", "unverified"),
            )
            conn.execute(
                "INSERT INTO claims(content_item_id, claim_type, claim_text, status) VALUES(?,?,?,?)",
                (2, "corruption", "Иванов незаконно передал документы в 2023 году", "unverified"),
            )
            # Entity
            conn.execute(
                "INSERT INTO entities(entity_type, canonical_name) VALUES(?,?)",
                ("person", "Иванов Иван"),
            )
            # Mentions linking entity to content items
            conn.execute(
                "INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES(?,?,?,?)",
                (1, 1, "person", 1.0),
            )
            conn.execute(
                "INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES(?,?,?,?)",
                (1, 2, "person", 1.0),
            )
            conn.commit()
        finally:
            conn.close()

        self.settings = {"db_path": str(self.db_path), "ensure_schema_on_connect": False}

    def test_build_cases_creates_case_with_two_claims(self):
        from cases.builder import build_cases_from_entities

        def _fake_get_db(_s=None):
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            return conn

        patches = [
            patch("cases.builder.get_db", _fake_get_db),
            patch("cases.builder._find_topic_clusters", return_value=[]),
        ]
        stack = [cm.__enter__() for cm in patches]
        try:
            total = build_cases_from_entities(self.settings, min_claims=2)
        finally:
            for cm in reversed(patches):
                cm.__exit__(None, None, None)

        self.assertEqual(total, 1)

        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            case = conn.execute("SELECT * FROM cases LIMIT 1").fetchone()
            self.assertIsNotNone(case)
            self.assertIn("Иванов Иван", case["title"])
            cc = conn.execute("SELECT COUNT(*) FROM case_claims WHERE case_id=?", (case["id"],)).fetchone()[0]
            self.assertEqual(cc, 2)
        finally:
            conn.close()

    def test_build_cases_empty_when_min_claims_not_met(self):
        from cases.builder import build_cases_from_entities

        def _fake_get_db(_s=None):
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            return conn

        patches = [
            patch("cases.builder.get_db", _fake_get_db),
            patch("cases.builder._find_topic_clusters", return_value=[]),
        ]
        stack = [cm.__enter__() for cm in patches]
        try:
            total = build_cases_from_entities(self.settings, min_claims=10)
        finally:
            for cm in reversed(patches):
                cm.__exit__(None, None, None)

        self.assertEqual(total, 0)


if __name__ == "__main__":
    unittest.main()
