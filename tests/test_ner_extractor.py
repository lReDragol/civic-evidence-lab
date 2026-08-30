"""Tests for ner/extractor.py.

Mocks the Natasha pipeline so tests run without the heavy NLP dependency.
"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.conftest import create_db


class TestNERExtractor(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / "test.db"
        create_db(self.db_path)

        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(
                "INSERT INTO sources(name, category) VALUES(?,?)",
                ("TestSource", "telegram"),
            )
            conn.execute(
                "INSERT INTO content_items(source_id, content_type, title, body_text, url, language, ner_processed) VALUES(?,?,?,?,?,?,?)",
                (1, "text", "Иванов и Правительство", "Сергей Иванов встретился с представителями Минфина.", "http://test", "ru", 0),
            )
            conn.execute(
                "INSERT INTO content_items(source_id, content_type, title, body_text, url, language, ner_processed) VALUES(?,?,?,?,?,?,?)",
                (1, "text", "Short", "A", "http://test2", "ru", 0),
            )
            conn.commit()
        finally:
            conn.close()

        self.settings = {"db_path": str(self.db_path), "ensure_schema_on_connect": False}

    def test_process_content_entities_creates_entities_and_mentions(self):
        from ner.extractor import process_content_entities

        fake_entities = [
            {"name": "Сергей Иванов", "entity_type": "person", "source": "natasha"},
            {"name": "Минфин", "entity_type": "organization", "source": "natasha"},
        ]

        def _fake_extract(text):
            return fake_entities

        def _fake_get_db(_s=None):
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            return conn

        patches = [
            patch("ner.extractor.get_db", _fake_get_db),
            patch("ner.extractor.extract_entities", _fake_extract),
            patch("ner.extractor._init_natasha", return_value=True),
        ]
        stack = [cm.__enter__() for cm in patches]
        try:
            stats = process_content_entities(self.settings, batch_size=10)
        finally:
            for cm in reversed(patches):
                cm.__exit__(None, None, None)

        self.assertIsNotNone(stats)
        self.assertGreaterEqual(stats["mentions_total"], 2)
        self.assertGreaterEqual(stats["entities_total"], 2)

        # Verify content_items were marked processed
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            processed = conn.execute("SELECT COUNT(*) FROM content_items WHERE ner_processed=1").fetchone()[0]
            self.assertEqual(processed, 1)  # only first row had real text
        finally:
            conn.close()

    def test_process_content_entities_empty_when_no_rows(self):
        from ner.extractor import process_content_entities

        empty_db = Path(self.tmp.name) / "empty.db"
        create_db(empty_db)
        settings = {"db_path": str(empty_db), "ensure_schema_on_connect": False}

        patches = [
            patch("ner.extractor.get_db", lambda _s=None: sqlite3.connect(str(empty_db))),
        ]
        stack = [cm.__enter__() for cm in patches]
        try:
            result = process_content_entities(settings, batch_size=10)
        finally:
            for cm in reversed(patches):
                cm.__exit__(None, None, None)

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
