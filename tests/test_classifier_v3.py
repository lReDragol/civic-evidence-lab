import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from classifier.llm_classifier_v2 import classify_content as classify_content_llm_v2
from classifier.tagger_granular import infer_granular_tags
from classifier.tagger_v3 import classify_content_items


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_classifier_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active, credibility_tier) VALUES
                (1, 'Official', 'official_site', 'https://official.example.test', 1, 'A'),
                (2, 'Media', 'media', 'https://media.example.test', 1, 'B');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (
                    101,
                    1,
                    'executive_profile',
                    'Первый заместитель Руководителя Аппарата Правительства Российской Федерации',
                    'Первый заместитель Руководителя Аппарата Правительства Российской Федерации. Координирует деятельность департаментов аппарата правительства.',
                    'raw_signal'
                ),
                (
                    102,
                    2,
                    'article',
                    'Иноагент: Распутин Ярослав Иванович',
                    'Минюст включил в реестр иноагентов Распутина Ярослава Ивановича.',
                    'raw_signal'
                ),
                (
                    103,
                    1,
                    'restriction_record',
                    'РКН ограничил доступ к интернет-ресурсу',
                    'Роскомнадзор ограничил доступ к интернет-ресурсу за нарушение требований законодательства.',
                    'raw_signal'
                );
            """
        )
        conn.commit()
    finally:
        conn.close()


class ClassifierV3Tests(unittest.TestCase):
    def test_granular_tagger_does_not_match_short_ai_token_inside_words(self):
        tags = infer_granular_tags(
            "Первый заместитель Руководителя Аппарата Правительства Российской Федерации"
        )
        self.assertNotIn("искусственный интеллект", tags["keyword"])
        self.assertNotIn("технологии", tags["keyword"])

    def test_granular_tagger_does_not_match_putin_inside_rasputin(self):
        tags = infer_granular_tags("Минюст включил Распутина Ярослава Ивановича в реестр иноагентов.")
        self.assertFalse(any(tag.endswith("путин") for tag in tags["deputy"]))

    def test_classifier_v3_records_votes_and_filters_false_positive_tags(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "classifier.db"
            create_classifier_db(db_path)
            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}

            result = classify_content_items(settings=settings, batch_size=20)
            self.assertTrue(result["ok"])

            conn = sqlite3.connect(db_path)
            try:
                conn.row_factory = sqlite3.Row
                tags_101 = {
                    row["tag_name"]: row
                    for row in conn.execute(
                        """
                        SELECT tag_name, namespace, normalized_tag, confidence_calibrated, decision_source
                        FROM content_tags
                        WHERE content_item_id=101
                        """
                    ).fetchall()
                }
                tags_102 = {
                    row["tag_name"]: row
                    for row in conn.execute(
                        """
                        SELECT tag_name, namespace, normalized_tag, confidence_calibrated, decision_source
                        FROM content_tags
                        WHERE content_item_id=102
                        """
                    ).fetchall()
                }
                tags_103 = {
                    row["tag_name"]: row
                    for row in conn.execute(
                        """
                        SELECT tag_name, namespace, normalized_tag, confidence_calibrated, decision_source
                        FROM content_tags
                        WHERE content_item_id=103
                        """
                    ).fetchall()
                }
                votes_101 = conn.execute(
                    "SELECT COUNT(*) FROM content_tag_votes WHERE content_item_id=101"
                ).fetchone()[0]
                processed_flags = conn.execute(
                    "SELECT id, classification_v3_processed FROM content_items ORDER BY id"
                ).fetchall()
                ai_support_votes = conn.execute(
                    """
                    SELECT COUNT(*) FROM content_tag_votes
                    WHERE content_item_id=101
                      AND normalized_tag='искусственный интеллект'
                      AND vote_value='support'
                    """
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertGreater(votes_101, 0)
            self.assertEqual(ai_support_votes, 0)
            self.assertNotIn("искусственный интеллект", tags_101)
            self.assertNotIn("технологии", tags_101)
            self.assertNotIn("депутат:путин", tags_102)
            self.assertIn("иноагент", tags_102)
            self.assertEqual(tags_102["иноагент"]["namespace"], "event")
            self.assertEqual(tags_102["иноагент"]["decision_source"], "classifier_v3")
            self.assertIsNotNone(tags_102["иноагент"]["confidence_calibrated"])
            self.assertNotIn("технологии", tags_103)
            self.assertNotIn("technology", tags_103)
            self.assertNotIn("искусственный интеллект", tags_103)
            self.assertEqual([tuple(row) for row in processed_flags], [(101, 1), (102, 1), (103, 1)])

    def test_classifier_v3_cleans_legacy_generic_tags_on_strict_content_types(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "classifier.db"
            create_classifier_db(db_path)
            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}

            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    """
                    INSERT INTO content_tags(
                        content_item_id, tag_level, tag_name, namespace, normalized_tag,
                        confidence, confidence_calibrated, tag_source, decision_source
                    ) VALUES(103, 0, 'technology', 'topic', 'technology', 0.9, 0.9, 'granular', NULL)
                    """
                )
                conn.execute(
                    "UPDATE content_items SET classification_v3_processed=1 WHERE id=103"
                )
                conn.commit()
            finally:
                conn.close()

            result = classify_content_items(settings=settings, batch_size=20)
            self.assertTrue(result["ok"])
            self.assertEqual(result["cleanup_deleted"], 1)

            conn = sqlite3.connect(db_path)
            try:
                remaining = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM content_tags
                    WHERE content_item_id=103
                      AND lower(COALESCE(normalized_tag, tag_name, ''))='technology'
                    """
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(remaining, 0)

    def test_failed_llm_v2_pass_does_not_mark_item_processed(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "classifier.db"
            create_classifier_db(db_path)
            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}

            with patch("classifier.llm_classifier_v2._call_ollama", return_value=None):
                result = classify_content_llm_v2(settings=settings, batch_size=20)

            self.assertEqual(result["classified"], 0)
            self.assertEqual(result["failed"], 3)

            conn = sqlite3.connect(db_path)
            try:
                processed_flags = conn.execute(
                    "SELECT llm_processed FROM content_items ORDER BY id"
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(processed_flags, [(0,), (0,), (0,)])


if __name__ == "__main__":
    unittest.main()
