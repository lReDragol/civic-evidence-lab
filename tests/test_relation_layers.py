import sqlite3
import tempfile
import unittest
from pathlib import Path

from analysis.entity_relation_builder import build_co_occurrence_from_mentions
from ner.relation_extractor import extract_co_occurrence_relations


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_relation_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Source A', 'media', 'https://example.test/a', 1),
                (2, 'Source B', 'media', 'https://example.test/b', 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (1, 'person', 'Иван Иванов'),
                (2, 'organization', 'Министерство тестирования'),
                (3, 'location', 'Москва');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (101, 1, 'article', 'A1', 'A1', 'raw_signal'),
                (102, 1, 'article', 'A2', 'A2', 'raw_signal'),
                (103, 2, 'article', 'B1', 'B1', 'raw_signal');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (1, 101, 'subject', 1.0),
                (2, 101, 'organization', 1.0),
                (3, 101, 'location', 1.0),
                (1, 102, 'subject', 1.0),
                (2, 102, 'organization', 1.0),
                (3, 102, 'location', 1.0),
                (1, 103, 'subject', 1.0),
                (2, 103, 'organization', 1.0);

            INSERT INTO entity_relations(id, from_entity_id, to_entity_id, relation_type, strength, detected_by)
            VALUES (900, 1, 3, 'mentioned_together', 'weak', 'co_occurrence:2');
            """
        )
        conn.commit()
    finally:
        conn.close()


class RelationLayerTests(unittest.TestCase):
    def test_relation_extractor_requires_independent_sources_and_cleans_old_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_relation_db(db_path)

            result = extract_co_occurrence_relations({"db_path": str(db_path)})
            self.assertEqual(result["relations_inserted"], 1)

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT from_entity_id, to_entity_id, relation_type, detected_by
                    FROM entity_relations
                    WHERE COALESCE(detected_by, '') LIKE 'co_occurrence:%'
                    ORDER BY id
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0:3], (1, 2, "mentioned_together"))
            self.assertEqual(rows[0][3], "co_occurrence:items=3:sources=2")

    def test_entity_relation_builder_skips_same_source_only_pair(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_relation_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                created = build_co_occurrence_from_mentions(conn)
                conn.commit()
                rows = conn.execute(
                    """
                    SELECT from_entity_id, to_entity_id, relation_type, detected_by
                    FROM entity_relations
                    WHERE relation_type='mentioned_together'
                    ORDER BY id
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(created, 1)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0:3], (1, 2, "mentioned_together"))
            self.assertEqual(rows[0][3], "co_occurrence:items=3:sources=2")


if __name__ == "__main__":
    unittest.main()
