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
            ),
            (
                11, 2, 'exec:2', 'post', 'Повторное сообщение об Иванове',
                'Иванов Иван Иванович занимает должность министра',
                '2026-04-26', 'raw_signal', 'https://example.test/post/11'
            ),
            (
                12, 1, 'exec:3', 'document', 'Приказ о назначении Иванова',
                'Назначить Иванова Ивана Ивановича министром тестирования.',
                '2026-04-24', 'official_document', 'https://example.test/minfin/order/12'
            );

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence)
            VALUES
                (1, 10, 'subject', 1.0),
                (2, 10, 'organization', 1.0),
                (1, 11, 'subject', 1.0),
                (3, 11, 'subject', 1.0),
                (1, 12, 'subject', 1.0),
                (2, 12, 'organization', 1.0);

            INSERT INTO claims(id, content_item_id, claim_text, status, needs_review)
            VALUES
                (21, 10, 'Иванов Иван Иванович занимает должность министра', 'verified', 0),
                (22, 11, 'Иванов Иван Иванович занимает должность министра', 'verified', 0),
                (23, 11, 'заявил', 'unverified', 1);

            INSERT INTO cases(id, title, case_type, status, started_at)
            VALUES(31, 'Кейс по назначению', 'oversight', 'open', '2026-04-24');

            INSERT INTO case_claims(case_id, claim_id, role) VALUES
                (31, 21, 'central'),
                (31, 22, 'central'),
                (31, 23, 'central');

            INSERT INTO evidence_links(id, claim_id, evidence_item_id, evidence_type, strength, notes)
            VALUES
                (51, 21, 12, 'official_document', 'strong', 'Приказ о назначении');

            INSERT INTO entity_relations(
                id, from_entity_id, to_entity_id, relation_type, evidence_item_id, strength, detected_by
            ) VALUES
                (41, 1, 2, 'works_at', 10, 'strong', 'official_positions'),
                (42, 1, 3, 'mentioned_together', NULL, 'weak', 'co_occurrence:3');

            INSERT INTO bills(id, number, title, status, registration_date, duma_url)
            VALUES(71, '901048-8', 'О проекте федерального закона о тестировании', 'registered', '2026-04-20', 'https://example.test/duma/bill/71');

            INSERT INTO bill_sponsors(bill_id, entity_id, sponsor_name)
            VALUES
                (71, 1, 'Иванов Иван Иванович'),
                (71, 3, 'Петров Пётр Петрович');

            INSERT INTO contracts(id, contract_number, title, publication_date, customer_inn, supplier_inn)
            VALUES(81, 'T-2026-81', 'Контракт на тестовую поставку', '2026-04-22', '1002003004', '5006007008');

            INSERT INTO contract_parties(contract_id, entity_id, party_name, party_role, inn)
            VALUES
                (81, 2, 'Министерство тестирования', 'customer', '1002003004'),
                (81, 3, 'Петров Пётр Петрович', 'supplier', '5006007008');

            INSERT INTO raw_source_items(id, source_id, external_id, raw_payload, hash_sha256)
            VALUES(90, 1, 'blob:ivanov-photo', '{}', 'hash-raw-ivanov-photo');

            INSERT INTO raw_blobs(id, raw_item_id, blob_type, file_path, original_filename, mime_type, hash_sha256)
            VALUES(91, 90, 'entity_media', 'processed/documents/entity_media/photos/ivanov.jpg', 'ivanov.jpg', 'image/jpeg', 'hash-ivanov-photo');

            INSERT INTO attachments(id, content_item_id, blob_id, file_path, attachment_type, hash_sha256, mime_type)
            VALUES(91, 10, 91, 'processed/documents/entity_media/photos/ivanov.jpg', 'image', 'hash-ivanov-photo', 'image/jpeg');

            INSERT INTO entity_media(entity_id, attachment_id, media_kind, source_url, is_primary)
            VALUES(1, 91, 'photo', 'https://example.test/minfin/person/1/photo.jpg', 1);

            INSERT INTO person_disclosures(
                id, entity_id, disclosure_year, income_amount, raw_income_text, source_url, source_content_id
            ) VALUES(
                101, 1, 2024, 1234567.89, '1 234 567,89 руб.', 'https://example.test/disclosure/101', 12
            );

            INSERT INTO declared_assets(
                id, disclosure_id, owner_role, asset_type, ownership_type, area_text, country, usage_type
            ) VALUES(
                102, 101, 'self', 'apartment', 'shared', '67.8', 'Россия', 'residential'
            );

            INSERT INTO company_affiliations(
                id, entity_id, company_entity_id, company_name, role_type, source_url, evidence_class
            ) VALUES(
                103, 1, 2, 'Министерство тестирования', 'board_member', 'https://example.test/egrul/103', 'support'
            );

            INSERT INTO restriction_events(
                id, issuer_entity_id, target_entity_id, target_name, restriction_type,
                right_category, stated_justification, source_content_id, evidence_class
            ) VALUES(
                104, 1, 2, 'Министерство тестирования', 'internet_block', 'internet',
                'по соображениям безопасности', 12, 'hard'
            );

            INSERT INTO review_tasks(
                id, task_key, queue_key, subject_type, subject_id, related_id,
                suggested_action, confidence, machine_reason, candidate_payload,
                source_links_json, status, review_pack_id
            ) VALUES(
                201, 'dup:11:12', 'content_duplicates', 'content_cluster', 301, NULL,
                'merge', 0.91, 'Normalized duplicate title',
                '{"items":[11,12],"canonical_title":"Повторное сообщение об Иванове"}',
                '["https://example.test/post/11","https://example.test/minfin/order/12"]',
                'open', 'pack-1'
            );

            INSERT INTO content_clusters(id, cluster_key, cluster_type, canonical_content_id, canonical_title, item_count, similarity_score)
            VALUES(301, 'cluster:ivanov', 'document_dedupe', 11, 'Повторное сообщение об Иванове', 2, 0.98);
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

            self.assertEqual(payload["summary"]["counts"]["content"], 3)
            self.assertEqual(payload["summary"]["counts"]["claims"], 3)
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
            self.assertEqual(payload["detail"]["content"][0]["id"], 11)

    def test_entity_detail_exposes_enrichment_sections(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bridge.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = DashboardDataService(conn, {})
                payload = service.screen_payload("entities", {"selected_id": 1})
            finally:
                conn.close()

            detail = payload["detail"]
            self.assertEqual(detail["media"][0]["media_kind"], "photo")
            self.assertEqual(detail["disclosures"][0]["disclosure_year"], 2024)
            self.assertEqual(detail["affiliations"][0]["role_type"], "board_member")
            self.assertEqual(detail["restrictions"][0]["restriction_type"], "internet_block")

    def test_review_ops_screen_returns_queue_detail_and_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bridge.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = DashboardDataService(conn, {})
                payload = service.screen_payload("review_ops", {"selected_id": 201})
            finally:
                conn.close()

            self.assertEqual(len(payload["items"]), 1)
            self.assertEqual(payload["queues"][0]["queue_key"], "content_duplicates")
            self.assertEqual(payload["detail"]["suggested_action"], "merge")
            self.assertIn("Повторное сообщение", payload["detail"]["subject_summary"])
            self.assertEqual(len(payload["detail"]["source_links"]), 2)
            self.assertIn('"items"', payload["detail"]["candidate_payload_pretty"])

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
            self.assertEqual(payload["items"][0]["relation_label"], "Работает в")
            self.assertEqual(payload["items"][0]["detected_label"], "официальные должности")
            self.assertEqual(payload["detail"]["id"], 41)
            self.assertEqual(payload["map_graph"]["kind"], "relation_map")
            self.assertTrue(payload["map_graph"]["edges"])
            self.assertTrue(any(edge["kind"] == "evidence" for edge in payload["map_graph"]["edges"]))

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

    def test_cases_screen_deduplicates_low_signal_claims(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bridge.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = DashboardDataService(conn, {})
                payload = service.screen_payload("cases", {"selected_id": 31})
            finally:
                conn.close()

            self.assertEqual(payload["detail"]["claims_total"], 3)
            self.assertEqual(len(payload["detail"]["claims"]), 1)
            self.assertEqual(payload["detail"]["claims_hidden_count"], 2)
            self.assertEqual(payload["detail"]["claims"][0]["support_count"], 2)
            self.assertEqual(
                payload["detail"]["claims"][0]["claim_text"],
                "Иванов Иван Иванович занимает должность министра",
            )

    def test_claim_detail_contains_evidence_graph(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bridge.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = DashboardDataService(conn, {})
                payload = service.screen_payload("claims", {"selected_id": 21})
            finally:
                conn.close()

            graph = payload["detail"]["evidence_graph"]
            self.assertEqual(graph["kind"], "claim")
            node_roles = {node["role"] for node in graph["nodes"]}
            self.assertIn("claim", node_roles)
            self.assertIn("content_origin", node_roles)
            self.assertIn("evidence", node_roles)
            self.assertIn("case", node_roles)
            edge_labels = {edge["label"] for edge in graph["edges"]}
            self.assertIn("источник", edge_labels)
            self.assertIn("official_document", edge_labels)
            self.assertTrue(all(node.get("description") for node in graph["nodes"]))

    def test_relation_detail_contains_evidence_graph(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bridge.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = DashboardDataService(conn, {})
                payload = service.screen_payload("relations", {"selected_id": 41})
            finally:
                conn.close()

            graph = payload["detail"]["evidence_graph"]
            self.assertEqual(graph["kind"], "relation")
            node_roles = {node["role"] for node in graph["nodes"]}
            self.assertIn("relation", node_roles)
            self.assertIn("entity_from", node_roles)
            self.assertIn("entity_to", node_roles)
            self.assertIn("evidence", node_roles)
            edge_labels = {edge["label"] for edge in graph["edges"]}
            self.assertIn("Работает в", edge_labels)
            self.assertIn("доказательство", edge_labels)
            self.assertTrue(all(node.get("description") for node in graph["nodes"]))

    def test_relation_map_includes_review_candidates(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bridge.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                conn.execute(
                    """
                    INSERT INTO relation_candidates(
                        id, entity_a_id, entity_b_id, candidate_type, origin, score,
                        support_items, support_sources, support_domains, promotion_state, metadata_json
                    ) VALUES(
                        81, 1, 3, 'same_case_cluster', 'candidate_builder:hybrid', 0.44,
                        4, 2, 2, 'review',
                        '{"case_overlap": 2, "support_items": 4}'
                    )
                    """
                )
                conn.commit()
                service = DashboardDataService(conn, {})
                payload = service.screen_payload("relations", {})
            finally:
                conn.close()

            map_graph = payload["map_graph"]
            self.assertEqual(map_graph["kind"], "relation_map")
            edge_kinds = {edge["kind"] for edge in map_graph["edges"]}
            self.assertIn("weak_similarity", edge_kinds)
            self.assertTrue(any("общие дела" in (edge.get("summary") or "") for edge in map_graph["edges"]))

    def test_relation_map_contains_bridge_nodes_and_detail_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bridge.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = DashboardDataService(conn, {})
                payload = service.screen_payload("relations", {"selected_id": 42})
            finally:
                conn.close()

            map_graph = payload["map_graph"]
            node_roles = {node["role"] for node in map_graph["nodes"]}
            self.assertIn("bridge_claim", node_roles)
            self.assertIn("bridge_case", node_roles)
            self.assertIn("bridge_content", node_roles)
            self.assertIn("bridge_evidence", node_roles)
            self.assertIn("bridge_bill", node_roles)
            self.assertIn("bridge_contract", node_roles)
            self.assertIn("bridge_affiliation", node_roles)
            self.assertIn("bridge_restriction", node_roles)

            bridge_paths = payload["detail"].get("bridge_paths") or []
            self.assertTrue(bridge_paths)
            self.assertTrue(any("Claim" in path["label"] for path in bridge_paths))
            self.assertTrue(any("Законопроект" in path["label"] or "Контракт" in path["label"] for path in bridge_paths))

    def test_relation_map_contains_affiliation_bridge_for_enriched_relation(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bridge.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                conn.execute(
                    """
                    INSERT INTO entity_relations(
                        id, from_entity_id, to_entity_id, relation_type, evidence_item_id, strength, detected_by
                    ) VALUES(
                        43, 1, 2, 'member_of', 10, 'strong', 'company_affiliations'
                    )
                    """
                )
                conn.commit()
                service = DashboardDataService(conn, {})
                payload = service.screen_payload("relations", {"selected_id": 43})
            finally:
                conn.close()

            node_roles = {node["role"] for node in payload["map_graph"]["nodes"]}
            self.assertIn("bridge_affiliation", node_roles)
            bridge_paths = payload["detail"].get("bridge_paths") or []
            self.assertTrue(any("Аффилиация" in path["label"] for path in bridge_paths))

    def test_settings_screen_contains_workspace_detail(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "bridge.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = DashboardDataService(
                    conn,
                    {
                        "db_path": "db/news_unified.db",
                        "obsidian_export_dir": "obsidian_export_graph",
                        "executive_directory_interval_seconds": 604800,
                    },
                )
                payload = service.screen_payload("settings", {})
            finally:
                conn.close()

            self.assertIn("project_root", payload["detail"])
            keys = {item["key"] for item in payload["items"]}
            self.assertIn("db_path", keys)
            self.assertIn("obsidian_export_dir", keys)


if __name__ == "__main__":
    unittest.main()
