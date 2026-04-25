import sqlite3
import tempfile
import unittest
from pathlib import Path

from ui.web_bridge import DashboardDataService


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active, is_official, credibility_tier)
            VALUES
                (1, 'Минфин России — руководство', 'official_site', 'https://example.test/minfin', 1, 1, 'A'),
                (2, 'Тестовый Telegram', 'telegram', 'https://t.me/test', 1, 0, 'B');

            INSERT INTO entities(id, entity_type, canonical_name, description)
            VALUES
                (1, 'person', 'Иванов Иван Иванович', 'Министр тестирования'),
                (2, 'organization', 'Министерство тестирования', 'Орган государственной власти'),
                (3, 'person', 'Петров Пётр Петрович', 'Смежная персона');

            INSERT INTO official_positions(
                entity_id, position_title, organization, source_type, is_active, source_url
            ) VALUES(
                1, 'Министр тестирования Российской Федерации',
                'Министерство тестирования', 'executive_directory:minfin', 1,
                'https://example.test/minfin/person/1'
            );

            INSERT INTO content_items(
                id, source_id, external_id, content_type, title, body_text, published_at, status, url
            ) VALUES(
                10, 1, 'exec:1', 'profile', 'Иванов Иван Иванович — Министр тестирования',
                'Министр тестирования Российской Федерации',
                '2026-04-25', 'raw_signal', 'https://example.test/minfin/person/1'
            );

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence)
            VALUES
                (1, 10, 'subject', 1.0),
                (2, 10, 'organization', 1.0);

            INSERT INTO claims(id, content_item_id, claim_text, status, needs_review)
            VALUES(21, 10, 'Иванов Иван Иванович занимает должность министра', 'verified', 0);

            INSERT INTO cases(id, title, case_type, status, started_at)
            VALUES(31, 'Кейс по назначению', 'oversight', 'open', '2026-04-24');

            INSERT INTO case_claims(case_id, claim_id, role) VALUES(31, 21, 'central');

            INSERT INTO entity_relations(
                id, from_entity_id, to_entity_id, relation_type, evidence_item_id, strength, detected_by
            ) VALUES
                (41, 1, 2, 'works_at', 10, 'strong', 'official_positions'),
                (42, 1, 3, 'mentioned_together', NULL, 'weak', 'co_occurrence:3');
            """
        )
        conn.commit()
    finally:
        conn.close()


class DashboardDataServiceTests(unittest.TestCase):
    def test_bootstrap_payload_contains_navigation_counts_and_jobs(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bridge.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = DashboardDataService(conn, {"pinned_sources": [1]})
                payload = service.bootstrap_payload(
                    running_jobs={"executive_directory"},
                    scheduler_running=True,
                )
            finally:
                conn.close()

            self.assertEqual(payload["summary"]["counts"]["content"], 1)
            self.assertEqual(payload["summary"]["counts"]["claims"], 1)
            self.assertEqual(payload["summary"]["counts"]["officials"], 1)
            self.assertIn("secondary_counts", payload["summary"])
            self.assertIn("graph_health", payload["summary"])
            self.assertEqual(payload["sources"]["groups"][0]["key"], "pinned")
            self.assertTrue(payload["jobs"]["scheduler_running"])
            self.assertEqual(payload["jobs"]["items"][0]["id"], "watch_folder")
            self.assertTrue(any(group["key"] == "analytics" for group in payload["navigation"]))

    def test_screen_payload_for_officials_returns_list_and_detail(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bridge.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = DashboardDataService(conn, {"pinned_sources": [1]})
                payload = service.screen_payload(
                    "officials",
                    {"query": "Иванов", "selected_id": 1},
                )
            finally:
                conn.close()

            self.assertEqual(len(payload["items"]), 1)
            self.assertEqual(payload["items"][0]["full_name"], "Иванов Иван Иванович")
            self.assertEqual(payload["detail"]["entity_id"], 1)
            self.assertEqual(
                payload["detail"]["positions"][0]["position_title"],
                "Министр тестирования Российской Федерации",
            )
            self.assertEqual(payload["detail"]["content"][0]["id"], 10)

    def test_screen_payload_for_relations_supports_layer_filter(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bridge.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = DashboardDataService(conn, {})
                payload = service.screen_payload(
                    "relations",
                    {"layer": "evidence"},
                )
            finally:
                conn.close()

            self.assertEqual(len(payload["items"]), 1)
            self.assertEqual(payload["items"][0]["relation_type"], "works_at")
            self.assertEqual(payload["items"][0]["layer"], "evidence")
            self.assertEqual(payload["detail"]["id"], 41)

    def test_relation_layer_treats_evidence_backed_structural_edge_as_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bridge.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = DashboardDataService(conn, {})
                self.assertEqual(service.relation_layer("works_at", "official_positions", 10), "evidence")
                self.assertEqual(service.relation_layer("works_at", "official_positions", None), "structural")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
