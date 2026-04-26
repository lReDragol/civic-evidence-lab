import sqlite3
import tempfile
import unittest
from pathlib import Path

from verification.authenticity_model import reverify_all_claims
from verification.claim_normalizer import canonicalize_claim_text, sync_claim_clusters


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_claim_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active, credibility_tier) VALUES
                (1, 'Media A', 'media', 'https://a.example.test', 1, 'B'),
                (2, 'Media B', 'media', 'https://b.example.test', 1, 'B');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (101, 1, 'article', 'A', 'A', 'raw_signal'),
                (102, 2, 'article', 'B', 'B', 'raw_signal'),
                (103, 1, 'article', 'C', 'C', 'raw_signal');

            INSERT INTO claims(id, content_item_id, claim_text, claim_type, status, needs_review) VALUES
                (201, 101, 'Депутат заявил о росте цен на бензин в регионе.', 'public_statement', 'unverified', 1),
                (202, 102, 'Депутат заявил о росте цен на бензин в регионе', 'public_statement', 'unverified', 1),
                (203, 103, 'заявил', 'public_statement', 'unverified', 1);
            """
        )
        conn.commit()
    finally:
        conn.close()


class ClaimNormalizationTests(unittest.TestCase):
    def test_canonicalize_claim_text_rejects_single_verb_low_signal_claim(self):
        self.assertIsNone(canonicalize_claim_text("заявил", "public_statement"))
        self.assertIsNone(canonicalize_claim_text("сказал", "public_statement"))

    def test_sync_claim_clusters_groups_duplicates_and_archives_low_signal(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "claims.db"
            create_claim_db(db_path)
            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}

            result = sync_claim_clusters(settings=settings)
            self.assertTrue(result["ok"])

            conn = sqlite3.connect(db_path)
            try:
                clusters = conn.execute(
                    "SELECT canonical_text, support_count FROM claim_clusters ORDER BY id"
                ).fetchall()
                claims = conn.execute(
                    """
                    SELECT id, canonical_text, claim_cluster_id, status
                    FROM claims
                    ORDER BY id
                    """
                ).fetchall()
                occurrences = conn.execute(
                    """
                    SELECT claim_cluster_id, claim_id, content_item_id
                    FROM claim_occurrences
                    ORDER BY claim_id
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(len(clusters), 1)
            self.assertEqual(clusters[0][1], 2)
            self.assertEqual(claims[0][1], claims[1][1])
            self.assertEqual(claims[0][2], claims[1][2])
            self.assertEqual(claims[2][3], "archived_low_signal")
            self.assertEqual(len(occurrences), 2)

    def test_reverify_all_claims_skips_archived_low_signal(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "claims.db"
            create_claim_db(db_path)
            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}

            sync_claim_clusters(settings=settings)
            result = reverify_all_claims(settings=settings, limit=10)

            conn = sqlite3.connect(db_path)
            try:
                archived_status = conn.execute(
                    "SELECT status FROM claims WHERE id=203"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(result["claims_reverified"], 2)
            self.assertEqual(archived_status, "archived_low_signal")


if __name__ == "__main__":
    unittest.main()
