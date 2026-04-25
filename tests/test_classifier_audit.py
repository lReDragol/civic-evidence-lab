import sqlite3
import tempfile
import unittest
from pathlib import Path

from classifier.audit import build_classifier_audit
from runtime.state import get_runtime_metadata


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_audit_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Audit Source', 'media', 'https://audit.example.test', 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (1, 'person', 'Персона 1'),
                (2, 'person', 'Персона 2'),
                (3, 'organization', 'Орг 3');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, published_at, status) VALUES
                (101, 1, 'article', 'Материал 101', 'Текст 101', '2026-04-20T12:00:00', 'raw_signal'),
                (102, 1, 'article', 'Материал 102', 'Текст 102', '2026-04-21T12:00:00', 'raw_signal'),
                (103, 1, 'article', 'Материал 103', 'Текст 103', '2026-04-22T12:00:00', 'raw_signal');

            INSERT INTO claims(id, content_item_id, claim_text, claim_type, status, confidence_final) VALUES
                (201, 101, 'Claim 201', 'fact', 'verified', 0.9),
                (202, 102, 'Claim 202', 'fact', 'unverified', 0.5),
                (203, 103, 'Claim 203', 'fact', 'partially_confirmed', 0.7);

            INSERT INTO content_tags(id, content_item_id, tag_level, tag_name, confidence, tag_source) VALUES
                (301, 101, 2, 'topic/corruption', 0.9, 'rule'),
                (302, 102, 2, 'topic/corruption', 0.8, 'rule'),
                (303, 103, 2, 'risk/fraud', 0.85, 'rule');

            INSERT INTO relation_candidates(
                id, entity_a_id, entity_b_id, candidate_type, origin, score,
                source_independence, evidence_overlap, temporal_proximity, role_compatibility,
                tag_overlap, text_specificity, support_items, support_sources, support_domains,
                support_categories, promotion_state, metadata_json
            ) VALUES
                (401, 1, 2, 'same_bill_cluster', 'candidate_builder:hybrid', 0.61, 0.4, 0.5, 0.6, 0.85, 0.1, 0.2, 0, 0, 0, 0, 'review', '{}'),
                (402, 1, 3, 'same_contract_cluster', 'candidate_builder:hybrid', 0.59, 0.4, 0.45, 0.55, 1.0, 0.1, 0.2, 0, 0, 0, 0, 'review', '{}');
            """
        )
        conn.commit()
    finally:
        conn.close()


class ClassifierAuditTests(unittest.TestCase):
    def test_build_classifier_audit_creates_samples_and_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "audit.db"
            report_path = Path(tmp) / "classifier_audit_latest.json"
            create_audit_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "classifier_audit": {
                    "gold_claims_target": 2,
                    "gold_relations_target": 2,
                    "gold_tags_target": 2,
                    "report_path": str(report_path),
                },
            }

            result = build_classifier_audit(settings)

            self.assertTrue(result["ok"])
            self.assertTrue(report_path.exists())

            conn = sqlite3.connect(db_path)
            try:
                sample_counts = conn.execute(
                    """
                    SELECT sample_kind, COUNT(*)
                    FROM classifier_audit_samples
                    GROUP BY sample_kind
                    ORDER BY sample_kind
                    """
                ).fetchall()
                baseline = get_runtime_metadata(conn, "classifier_audit_baseline")
                last_status = get_runtime_metadata(conn, "classifier_audit_last_status")
            finally:
                conn.close()

            self.assertEqual(sample_counts, [("claim", 2), ("relation_candidate", 2), ("tag_assignment", 2)])
            self.assertIsInstance(baseline, dict)
            self.assertEqual(last_status, "ok")

    def test_build_classifier_audit_fails_strict_gate_on_drift(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "audit.db"
            report_path = Path(tmp) / "classifier_audit_latest.json"
            create_audit_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "classifier_audit": {
                    "gold_claims_target": 2,
                    "gold_relations_target": 2,
                    "gold_tags_target": 2,
                    "report_path": str(report_path),
                    "claim_status_drift_threshold": 0.05,
                    "relation_drift_threshold": 0.05,
                    "tag_drift_threshold": 0.05,
                    "strict_gate": True,
                },
            }

            first_result = build_classifier_audit(settings)
            self.assertTrue(first_result["ok"])

            conn = sqlite3.connect(db_path)
            try:
                conn.execute("DELETE FROM claims")
                conn.execute("DELETE FROM content_tags")
                conn.execute("DELETE FROM relation_candidates")
                conn.executescript(
                    """
                    INSERT INTO claims(id, content_item_id, claim_text, claim_type, status, confidence_final) VALUES
                        (204, 101, 'Claim 204', 'fact', 'disputed', 0.1),
                        (205, 102, 'Claim 205', 'fact', 'disputed', 0.1),
                        (206, 103, 'Claim 206', 'fact', 'disputed', 0.1);

                    INSERT INTO content_tags(id, content_item_id, tag_level, tag_name, confidence, tag_source) VALUES
                        (304, 101, 2, 'topic/censorship', 0.95, 'rule'),
                        (305, 102, 2, 'topic/censorship', 0.95, 'rule'),
                        (306, 103, 2, 'topic/censorship', 0.95, 'rule');

                    INSERT INTO relation_candidates(
                        id, entity_a_id, entity_b_id, candidate_type, origin, score,
                        source_independence, evidence_overlap, temporal_proximity, role_compatibility,
                        tag_overlap, text_specificity, support_items, support_sources, support_domains,
                        support_categories, promotion_state, metadata_json
                    ) VALUES
                        (403, 1, 2, 'same_case_cluster', 'candidate_builder:hybrid', 0.52, 0.4, 0.5, 0.6, 0.85, 0.1, 0.2, 0, 0, 0, 0, 'review', '{}'),
                        (404, 2, 3, 'same_case_cluster', 'candidate_builder:hybrid', 0.51, 0.4, 0.5, 0.6, 0.85, 0.1, 0.2, 0, 0, 0, 0, 'review', '{}');
                    """
                )
                conn.commit()
            finally:
                conn.close()

            second_result = build_classifier_audit(settings)

            self.assertFalse(second_result["ok"])
            self.assertIn("classifier_drift_gate_failed", second_result["fatal_errors"])
            self.assertTrue(any("drift>" in item for item in second_result["warnings"]))


if __name__ == "__main__":
    unittest.main()
