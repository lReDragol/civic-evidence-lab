import sqlite3
import tempfile
import unittest
from pathlib import Path

from investigation.engine import InvestigationEngine
from investigation.models import Confidence, NodeType


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url) VALUES(1, 'Fixture Source', 'telegram', 'https://example.test/source');
            CREATE TABLE IF NOT EXISTS official_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id INTEGER NOT NULL,
                position_title TEXT,
                organization TEXT,
                region TEXT,
                faction TEXT,
                started_at TEXT,
                ended_at TEXT,
                source_url TEXT,
                source_type TEXT,
                is_active INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS bills (
                id INTEGER PRIMARY KEY,
                number TEXT NOT NULL,
                title TEXT NOT NULL,
                bill_type TEXT,
                status TEXT,
                registration_date TEXT,
                duma_url TEXT
            );
            CREATE TABLE IF NOT EXISTS bill_vote_sessions (
                id INTEGER PRIMARY KEY,
                bill_id INTEGER,
                vote_date TEXT NOT NULL,
                vote_stage TEXT,
                total_for INTEGER DEFAULT 0,
                total_against INTEGER DEFAULT 0,
                total_absent INTEGER DEFAULT 0,
                result TEXT
            );
            CREATE TABLE IF NOT EXISTS bill_sponsors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bill_id INTEGER NOT NULL,
                entity_id INTEGER,
                sponsor_name TEXT,
                sponsor_role TEXT,
                faction TEXT
            );
            CREATE TABLE IF NOT EXISTS bill_votes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vote_session_id INTEGER NOT NULL,
                entity_id INTEGER,
                deputy_name TEXT NOT NULL,
                faction TEXT,
                vote_result TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS investigative_materials (
                id INTEGER PRIMARY KEY,
                content_item_id INTEGER,
                material_type TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT,
                involved_entities TEXT,
                publication_date TEXT,
                source_org TEXT,
                verification_status TEXT,
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

        conn.executemany(
            "INSERT INTO entities(id, entity_type, canonical_name, inn) VALUES(?,?,?,?)",
            [
                (1, "organization", "Customer Org", None),
                (2, "organization", "Supplier Org", None),
                (3, "organization", "Agency Without INN", None),
                (4, "organization", "Vendor Without INN", None),
                (10, "person", "Ivan Ivanov", None),
            ],
        )
        conn.execute(
            """
            INSERT INTO official_positions(
                entity_id, position_title, organization, is_active
            ) VALUES(10, 'Deputy', 'Customer Org', 1)
            """
        )

        conn.execute(
            """
            INSERT INTO bills(
                id, number, title, bill_type, status, registration_date, duma_url
            ) VALUES(
                1, '123-FZ', 'Bill 123', 'federal', 'introduced', '2026-04-01', 'https://sozd.duma.gov.ru/bill/123'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO bill_vote_sessions(
                id, bill_id, vote_date, vote_stage, total_for, total_against, total_absent, result
            ) VALUES(
                1, 1, '2026-04-02', 'first reading', 300, 10, 40, 'accepted'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO bill_votes(
                vote_session_id, entity_id, deputy_name, faction, vote_result
            ) VALUES(
                1, 10, 'Ivan Ivanov', 'Party X', 'за'
            )
            """
        )

        conn.execute(
            """
            INSERT INTO investigative_materials(
                id, content_item_id, material_type, title, summary, involved_entities, publication_date, source_org, raw_data
            ) VALUES(
                7, NULL, 'government_contract', 'Contract CN-77', 'Customer Org hired Supplier Org',
                '[{"entity_id":1,"role":"customer"},{"entity_id":2,"role":"supplier"}]',
                '2026-04-03', 'EIS',
                '{"contract_number":"CN-77","customer_inn":"111","supplier_inn":"222"}'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO contracts(
                id, material_id, content_item_id, contract_number, title, summary, publication_date, source_org, customer_inn, supplier_inn, raw_data
            ) VALUES(
                7, 7, NULL, 'CN-77', 'Contract CN-77', 'Customer Org hired Supplier Org', '2026-04-03', 'EIS', '111', '222',
                '{"contract_number":"CN-77","customer_inn":"111","supplier_inn":"222"}'
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO contract_parties(
                contract_id, entity_id, party_name, party_role, inn
            ) VALUES(?,?,?,?,?)
            """,
            [
                (7, 1, "Customer Org", "customer", "111"),
                (7, 2, "Supplier Org", "supplier", "222"),
            ],
        )
        conn.execute(
            """
            INSERT INTO investigative_materials(
                id, content_item_id, material_type, title, summary, involved_entities, publication_date, source_org, raw_data
            ) VALUES(
                8, NULL, 'government_contract', 'Contract CN-88', 'Agency Without INN hired Vendor Without INN',
                NULL, '2026-04-04', 'EIS',
                '{"contract_number":"CN-88"}'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO contracts(
                id, material_id, content_item_id, contract_number, title, summary, publication_date, source_org, customer_inn, supplier_inn, raw_data
            ) VALUES(
                8, 8, NULL, 'CN-88', 'Contract CN-88', 'Agency Without INN hired Vendor Without INN',
                '2026-04-04', 'EIS', '', '',
                '{"contract_number":"CN-88"}'
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO contract_parties(
                contract_id, entity_id, party_name, party_role, inn
            ) VALUES(?,?,?,?,?)
            """,
            [
                (8, 3, "Agency Without INN", "customer", ""),
                (8, 4, "Vendor Without INN", "supplier", ""),
            ],
        )
        conn.executemany(
            """
            INSERT INTO content_items(
                id, source_id, content_type, title, body_text, published_at, status
            ) VALUES(?,?,?,?,?,?,?)
            """,
            [
                (
                    100,
                    1,
                    "post",
                    "Customer Org accuses Supplier Org",
                    "Customer Org says Supplier Org received a questionable contract.",
                    "2026-04-05T10:00:00",
                    "classified",
                ),
                (
                    101,
                    1,
                    "document",
                    "Official contract evidence",
                    "Supplier Org appears in the supporting procurement document.",
                    "2026-04-06T10:00:00",
                    "verified",
                ),
            ],
        )
        conn.executemany(
            """
            INSERT INTO entity_mentions(
                entity_id, content_item_id, mention_type, confidence
            ) VALUES(?,?,?,?)
            """,
            [
                (1, 100, "organization", 1.0),
                (2, 100, "organization", 1.0),
                (2, 101, "organization", 1.0),
            ],
        )
        conn.execute(
            """
            INSERT INTO claims(
                id, content_item_id, claim_text, claim_type, status, confidence_final, needs_review
            ) VALUES(
                50, 100, 'Supplier Org received a questionable contract', 'government_contract',
                'partially_confirmed', 0.8, 0
            )
            """
        )
        conn.execute(
            """
            INSERT INTO evidence_links(
                claim_id, evidence_item_id, evidence_type, strength, notes, linked_by
            ) VALUES(
                50, 101, 'official_document', 'strong', 'fixture evidence', 'unit-test'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO cases(
                id, title, description, case_type, status, started_at
            ) VALUES(
                9, 'Questionable procurement case', 'Fixture case linked to the questionable contract claim',
                'procurement', 'open', '2026-04-07'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO case_claims(
                case_id, claim_id, role
            ) VALUES(
                9, 50, 'central'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO case_events(
                case_id, event_date, event_title, event_description, content_item_id, event_order
            ) VALUES(
                9, '2026-04-08', 'Official evidence added',
                'A supporting procurement document was attached to the case.',
                101, 1
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


class InvestigationEvidenceGraphTests(unittest.TestCase):
    def test_engine_expands_contract_and_vote_session_virtual_nodes(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "sample.db"
            create_db(db_path)

            engine = InvestigationEngine(str(db_path))
            try:
                org_result = engine.investigate(1, max_hops=2, min_confidence=Confidence.LIKELY)
                contract_nodes = [
                    node for node in org_result.nodes.values() if node.node_type == NodeType.CONTRACT
                ]
                self.assertTrue(contract_nodes)
                self.assertIn(2, org_result.nodes)
                self.assertTrue(
                    any(
                        edge.relation_type == "government_contract"
                        and {edge.from_id, edge.to_id} == {1, contract_nodes[0].entity_id}
                        for edge in org_result.edges
                    )
                )
                self.assertTrue(
                    any(
                        edge.relation_type == "government_contract"
                        and {edge.from_id, edge.to_id} == {2, contract_nodes[0].entity_id}
                        for edge in org_result.edges
                    )
                )
                self.assertTrue(org_result.evidence_chains)
                self.assertTrue(
                    any(
                        contract_nodes[0].entity_id in chain.entity_path and 2 in chain.entity_path
                        for chain in org_result.evidence_chains
                    )
                )

                person_result = engine.investigate(10, max_hops=2, min_confidence=Confidence.LIKELY)
                vote_nodes = [
                    node for node in person_result.nodes.values() if node.node_type == NodeType.VOTE_SESSION
                ]
                bill_nodes = [
                    node for node in person_result.nodes.values() if node.node_type == NodeType.BILL
                ]
                self.assertTrue(vote_nodes)
                self.assertTrue(bill_nodes)
                self.assertTrue(
                    any(
                        edge.relation_type == "about_bill"
                        and {edge.from_id, edge.to_id} == {vote_nodes[0].entity_id, bill_nodes[0].entity_id}
                        for edge in person_result.edges
                    )
                )
                self.assertTrue(person_result.evidence_chains)
                self.assertEqual(person_result.evidence_chains[0].entity_path[-1], bill_nodes[0].entity_id)
                self.assertGreater(person_result.evidence_chains[0].score, 0.0)
            finally:
                engine.close()

    def test_engine_finds_contracts_by_contract_parties_entity_id_without_inn(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "sample.db"
            create_db(db_path)

            engine = InvestigationEngine(str(db_path))
            try:
                org_result = engine.investigate(3, max_hops=2, min_confidence=Confidence.LIKELY)
                contract_nodes = [
                    node for node in org_result.nodes.values() if node.node_type == NodeType.CONTRACT
                ]

                self.assertTrue(contract_nodes)
                self.assertIn(4, org_result.nodes)
                self.assertTrue(
                    any(
                        edge.relation_type == "government_contract"
                        and contract_nodes[0].entity_id in {edge.from_id, edge.to_id}
                        for edge in org_result.edges
                    )
                )
                self.assertTrue(
                    any(
                        chain.entity_path == [3, contract_nodes[0].entity_id, 4]
                        for chain in org_result.evidence_chains
                    )
                )
            finally:
                engine.close()

    def test_engine_builds_claim_content_evidence_chain(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "sample.db"
            create_db(db_path)

            engine = InvestigationEngine(str(db_path))
            try:
                result = engine.investigate(1, max_hops=3, min_confidence=Confidence.LIKELY)
                claim_nodes = [node for node in result.nodes.values() if node.node_type == NodeType.CLAIM]
                content_nodes = [node for node in result.nodes.values() if node.node_type == NodeType.CONTENT]

                self.assertTrue(claim_nodes)
                self.assertTrue(content_nodes)
                self.assertTrue(
                    any(
                        edge.relation_type == "has_claim"
                        and {edge.from_id, edge.to_id} == {1, claim_nodes[0].entity_id}
                        for edge in result.edges
                    )
                )
                self.assertTrue(
                    any(
                        edge.relation_type == "reported_in"
                        and claim_nodes[0].entity_id in {edge.from_id, edge.to_id}
                        for edge in result.edges
                    )
                )
                self.assertTrue(
                    any(
                        edge.relation_type == "supported_by"
                        and claim_nodes[0].entity_id in {edge.from_id, edge.to_id}
                        for edge in result.edges
                    )
                )
                self.assertTrue(
                    any(
                        edge.relation_type == "mentions_entity"
                        and edge.to_id == 2
                        for edge in result.edges
                    )
                )
                self.assertTrue(
                    any(
                        chain.entity_path[0] == 1
                        and claim_nodes[0].entity_id in chain.entity_path
                        and 2 in chain.entity_path
                        for chain in result.evidence_chains
                    )
                )
            finally:
                engine.close()

    def test_engine_builds_case_nodes_from_claims_and_case_events(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "sample.db"
            create_db(db_path)

            engine = InvestigationEngine(str(db_path))
            try:
                result = engine.investigate(1, max_hops=4, min_confidence=Confidence.LIKELY)
                claim_nodes = [node for node in result.nodes.values() if node.node_type == NodeType.CLAIM]
                case_nodes = [node for node in result.nodes.values() if node.node_type == NodeType.CASE]
                content_nodes = [node for node in result.nodes.values() if node.node_type == NodeType.CONTENT]

                self.assertTrue(claim_nodes)
                self.assertTrue(case_nodes)
                self.assertTrue(content_nodes)
                self.assertTrue(
                    any(
                        edge.relation_type == "part_of_case"
                        and claim_nodes[0].entity_id in {edge.from_id, edge.to_id}
                        and case_nodes[0].entity_id in {edge.from_id, edge.to_id}
                        for edge in result.edges
                    )
                )
                self.assertTrue(
                    any(
                        edge.relation_type == "documents_case"
                        and case_nodes[0].entity_id in {edge.from_id, edge.to_id}
                        and 101 in {result.nodes[edge.from_id].extra.get("content_item_id"), result.nodes[edge.to_id].extra.get("content_item_id")}
                        for edge in result.edges
                    )
                )
                self.assertTrue(
                    any(
                        case_nodes[0].entity_id in chain.entity_path and claim_nodes[0].entity_id in chain.entity_path
                        for chain in result.evidence_chains
                    )
                )
            finally:
                engine.close()

    def test_engine_respects_max_nodes_limit_for_virtual_evidence_graph(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "sample.db"
            create_db(db_path)

            engine = InvestigationEngine(str(db_path), max_nodes=4, max_edges=16)
            try:
                result = engine.investigate(1, max_hops=3, min_confidence=Confidence.LIKELY)
                self.assertLessEqual(len(result.nodes), 4)
                self.assertLessEqual(len(result.edges), 16)
                self.assertTrue(
                    all(edge.from_id in result.nodes and edge.to_id in result.nodes for edge in result.edges)
                )
            finally:
                engine.close()


if __name__ == "__main__":
    unittest.main()
