import sqlite3
import tempfile
import unittest
from pathlib import Path

from verification.authenticity_model import compute_document_evidence
from verification.evidence_linker import backfill_evidence_classes


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_evidence_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active, credibility_tier, is_official) VALUES
                (1, 'Official Registry', 'official_registry', 'https://official.example.test', 1, 'A', 1),
                (2, 'Media', 'media', 'https://media.example.test', 1, 'B', 0);

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (101, 2, 'article', 'Claim source', 'Body', 'raw_signal'),
                (102, 1, 'registry_record', 'Registry', 'Registry body', 'raw_signal'),
                (103, 2, 'article', 'Mention', 'Mention body', 'raw_signal');

            INSERT INTO claims(id, content_item_id, claim_text, claim_type, status) VALUES
                (201, 101, 'Claim 201', 'fact', 'unverified');

            INSERT INTO evidence_links(id, claim_id, evidence_item_id, evidence_type, evidence_class, strength, notes) VALUES
                (301, 201, 102, 'registry_record', 'hard', 'strong', 'hard evidence'),
                (302, 201, 103, 'cross_source_corroboration', 'support', 'moderate', 'support evidence'),
                (303, 201, 103, 'official_document', 'seed', 'weak', 'seed evidence');
            """
        )
        conn.commit()
    finally:
        conn.close()


class EvidenceWeightingTests(unittest.TestCase):
    def test_hard_evidence_outweighs_seed_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "evidence.db"
            create_evidence_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                score = compute_document_evidence(conn, 201)
            finally:
                conn.close()

            self.assertGreater(score, 0.75)

    def test_backfill_evidence_classes_reclassifies_legacy_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "evidence.db"
            create_evidence_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                conn.execute("UPDATE evidence_links SET evidence_class='support' WHERE id=301")
                conn.execute("UPDATE evidence_links SET evidence_type='co_entity', evidence_class='support' WHERE id=303")
                conn.commit()
            finally:
                conn.close()

            result = backfill_evidence_classes({"db_path": str(db_path), "ensure_schema_on_connect": True})
            self.assertGreaterEqual(result["evidence_classes_updated"], 2)

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    "SELECT id, evidence_class FROM evidence_links ORDER BY id"
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(dict(rows)[301], "hard")
            self.assertEqual(dict(rows)[303], "seed")


if __name__ == "__main__":
    unittest.main()
