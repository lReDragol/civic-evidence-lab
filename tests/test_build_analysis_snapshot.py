import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools.build_analysis_snapshot import build_analysis_snapshot


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_source_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS bills (
                id INTEGER PRIMARY KEY,
                number TEXT NOT NULL,
                title TEXT NOT NULL,
                registration_date TEXT
            );
            CREATE TABLE IF NOT EXISTS bill_vote_sessions (
                id INTEGER PRIMARY KEY,
                bill_id INTEGER,
                vote_date TEXT NOT NULL,
                vote_stage TEXT
            );
            CREATE TABLE IF NOT EXISTS investigative_materials (
                id INTEGER PRIMARY KEY,
                content_item_id INTEGER,
                material_type TEXT NOT NULL,
                title TEXT NOT NULL,
                involved_entities TEXT,
                raw_data TEXT
            );
            CREATE TABLE IF NOT EXISTS contracts (
                id INTEGER PRIMARY KEY,
                material_id INTEGER,
                content_item_id INTEGER,
                contract_number TEXT,
                title TEXT NOT NULL,
                summary TEXT,
                publication_date TEXT,
                source_org TEXT,
                customer_inn TEXT,
                supplier_inn TEXT,
                raw_data TEXT
            );
            CREATE TABLE IF NOT EXISTS contract_parties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_id INTEGER NOT NULL,
                entity_id INTEGER,
                party_name TEXT,
                party_role TEXT NOT NULL,
                inn TEXT,
                metadata_json TEXT
            );
            """
        )
        conn.execute(
            "INSERT INTO sources(id, name, category, url, access_method, credibility_tier, is_active) VALUES(1,'Src','telegram','https://t.me/test','telethon','B',1)"
        )
        conn.executemany(
            "INSERT INTO entities(id, entity_type, canonical_name) VALUES(?,?,?)",
            [
                (1, "person", "Entity One"),
                (2, "organization", "Entity Two"),
                (3, "organization", "Entity Three"),
            ],
        )
        conn.execute("UPDATE entities SET inn='111' WHERE id=2")
        conn.execute("UPDATE entities SET inn='222' WHERE id=3")
        conn.execute(
            "INSERT INTO content_items(id, source_id, content_type, title, body_text, published_at, status) VALUES(1,1,'post','T1','Body','2026-04-01','verified')"
        )
        conn.execute(
            "INSERT INTO claims(id, content_item_id, claim_text, claim_type, status, needs_review) VALUES(1,1,'Claim text','public_statement','verified',0)"
        )
        conn.execute(
            "INSERT INTO cases(id, title, case_type, status, started_at) VALUES(1,'Case 1','type','open','2026-04-01')"
        )
        conn.execute("INSERT INTO case_claims(case_id, claim_id, role) VALUES(1,1,'central')")
        conn.executemany(
            """
            INSERT INTO entity_relations(from_entity_id, to_entity_id, relation_type, evidence_item_id, strength, detected_by)
            VALUES(?,?,?,?,?,?)
            """,
            [
                (1, 2, "works_at", 1, "strong", "official_positions"),
                (1, 3, "mentioned_together", None, "weak", "co_occurrence:2"),
            ],
        )
        conn.execute(
            "INSERT INTO risk_patterns(id, pattern_type, description, entity_ids, evidence_ids, risk_level, case_id, needs_review) VALUES(1,'corruption_risk','desc','[1]','[1]','critical',1,0)"
        )
        conn.execute(
            "INSERT INTO evidence_links(claim_id, evidence_item_id, evidence_type, strength, notes) VALUES(1,1,'official_document','strong','note')"
        )
        conn.execute(
            "INSERT INTO bills(id, number, title, registration_date) VALUES(1,'B-1','Bill one','2026-04-01')"
        )
        conn.execute(
            "INSERT INTO bill_vote_sessions(id, bill_id, vote_date, vote_stage) VALUES(1,1,'2026-04-02','first reading')"
        )
        conn.execute(
            """
            INSERT INTO investigative_materials(id, content_item_id, material_type, title, involved_entities, raw_data)
            VALUES(
                1, 1, 'government_contract', 'Contract 1',
                '[{\"entity_id\":2,\"role\":\"customer\"},{\"entity_id\":3,\"role\":\"supplier\"}]',
                '{\"contract_number\":\"CN-1\",\"customer_inn\":\"111\",\"supplier_inn\":\"222\"}'
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


class BuildAnalysisSnapshotTests(unittest.TestCase):
    def test_snapshot_builder_copies_db_and_writes_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source_db = tmp_path / "source.db"
            target_db = tmp_path / "snapshot.db"
            report_path = tmp_path / "report.json"
            create_source_db(source_db)

            with mock.patch("tools.build_analysis_snapshot.run_all_structural_links", return_value={"ok": 1}), \
                 mock.patch("tools.build_analysis_snapshot.run_entity_relation_builder", return_value={"ok": 2}), \
                 mock.patch("tools.build_analysis_snapshot.extract_co_occurrence_relations", return_value={"ok": 3}), \
                 mock.patch("tools.build_analysis_snapshot.extract_head_role_relations", return_value={"ok": 4}), \
                 mock.patch("tools.build_analysis_snapshot.run_contradiction_detection", return_value={"ok": 5}), \
                 mock.patch("tools.build_analysis_snapshot.auto_link_evidence", return_value={"ok": 6}), \
                 mock.patch("tools.build_analysis_snapshot.auto_link_by_content_type", return_value={"ok": 7}), \
                 mock.patch("tools.build_analysis_snapshot.detect_all_patterns", return_value={"ok": 8}):
                report = build_analysis_snapshot(
                    source_db=source_db,
                    target_db=target_db,
                    report_path=report_path,
                )

            self.assertTrue(target_db.exists())
            self.assertTrue(report_path.exists())
            self.assertEqual(report["db"]["source_db"], str(source_db))
            self.assertEqual(report["db"]["target_db"], str(target_db))
            self.assertIn("pipeline", report)
            self.assertIn("summary", report)
            self.assertIn("relation_layers", report["summary"])
            self.assertEqual(report["summary"]["relation_layers"]["structural"], 1)
            self.assertEqual(report["summary"]["relation_layers"]["weak_similarity"], 1)
            self.assertEqual(report["summary"]["evidence_backed_relations"], 1)
            self.assertEqual(report["summary"]["counts"]["claims"], 1)
            self.assertEqual(report["summary"]["counts"]["cases"], 1)
            self.assertEqual(report["summary"]["counts"]["bills"], 1)
            self.assertEqual(report["summary"]["counts"]["vote_sessions"], 1)
            self.assertEqual(report["summary"]["counts"]["contracts"], 1)
            self.assertEqual(report["summary"]["counts"]["risks"], 1)
            self.assertTrue(report["top_hubs"])
            self.assertIn("pipeline", json.loads(report_path.read_text(encoding="utf-8")))

            conn = sqlite3.connect(target_db)
            try:
                contracts_count = conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0]
                parties = conn.execute(
                    "SELECT entity_id, party_role, inn FROM contract_parties ORDER BY entity_id"
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(contracts_count, 1)
            self.assertEqual(parties, [(2, "customer", "111"), (3, "supplier", "222")])


if __name__ == "__main__":
    unittest.main()
