import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from classifier.negation_handler import process_negations
from verification.engine import verify_claim_with_site_search


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_verification_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active, credibility_tier) VALUES
                (1, 'Media', 'media', 'https://media.example.test', 1, 'B');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (101, 1, 'post', 'Проверка', 'Госдума проголосовала за закон.', 'raw_signal');

            INSERT INTO claims(id, content_item_id, claim_text, canonical_text, canonical_hash, claim_type, status, source_score, needs_review)
            VALUES(201, 101, 'Госдума проголосовала за закон.', 'Госдума проголосовала за закон.', 'hash-201', 'vote_record', 'unverified', 0.3, 1);
            """
        )
        conn.commit()
    finally:
        conn.close()


class VerificationRuntimeRegressionTests(unittest.TestCase):
    def test_process_negations_exists_and_runs_on_empty_signal(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "verification.db"
            create_verification_db(db_path)

            result = process_negations(settings={"db_path": str(db_path), "ensure_schema_on_connect": True}, limit=10)

            self.assertTrue(result["ok"])
            self.assertIn("claims", result["artifacts"])
            self.assertIn("tags", result["artifacts"])

    def test_verify_claim_with_site_search_uses_time_and_updates_claim(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "verification.db"
            create_verification_db(db_path)
            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}

            with patch("verification.engine.SITE_SEARCH_AVAILABLE", True), patch(
                "verification.engine.targeted_search",
                return_value={"pravo": 1, "zakupki": 0},
            ):
                stored = verify_claim_with_site_search(201, "Госдума проголосовала за закон.", settings)

            self.assertEqual(stored, 1)
            conn = sqlite3.connect(db_path)
            try:
                status = conn.execute("SELECT status, needs_review FROM claims WHERE id=201").fetchone()
            finally:
                conn.close()
            self.assertEqual(status, ("partially_confirmed", 1))


if __name__ == "__main__":
    unittest.main()
