import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tools.export_obsidian import export_obsidian


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        schema = SCHEMA_PATH.read_text(encoding="utf-8")
        conn.executescript(schema)
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS bills (
                id INTEGER PRIMARY KEY,
                number TEXT NOT NULL,
                title TEXT NOT NULL,
                bill_type TEXT,
                status TEXT,
                registration_date TEXT,
                duma_url TEXT
            );
            CREATE TABLE IF NOT EXISTS bill_sponsors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bill_id INTEGER NOT NULL,
                entity_id INTEGER,
                sponsor_name TEXT,
                sponsor_role TEXT,
                faction TEXT
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
            "INSERT INTO sources(id, name, category, url, access_method, credibility_tier, is_active) VALUES(1,'Telegram Feed','telegram','https://t.me/test','telethon','B',1)"
        )
        conn.execute(
            "INSERT INTO sources(id, name, category, url, access_method, credibility_tier, is_official, is_active) VALUES(2,'Official Registry','official_registry','https://example.org/registry','http','A',1,1)"
        )

        entities = [
            (1, "person", "Ivan Ivanov", None, None, "Deputy"),
            (2, "organization", "Party X", None, None, "Political party"),
            (3, "person", "Weak Peer", None, None, "Secondary mention"),
            (4, "organization", "State Agency", "111", None, "Customer"),
            (5, "organization", "Vendor LLC", "222", None, "Supplier"),
            (6, "organization", "Budget Hospital", None, None, "Customer without INN"),
            (7, "organization", "MedTech Supplier", None, None, "Supplier without INN"),
        ]
        conn.executemany(
            "INSERT INTO entities(id, entity_type, canonical_name, inn, ogrn, description) VALUES(?,?,?,?,?,?)",
            entities,
        )

        conn.execute(
            """
            INSERT INTO content_items(
                id, source_id, external_id, content_type, title, body_text, published_at, url, status
            ) VALUES(
                1, 1, 'msg-1', 'post', 'Deputy voted for bill 123',
                'Ivan Ivanov from Party X supported bill 123 and became central in the case.',
                '2026-04-01T10:00:00', 'https://t.me/test/1', 'verified'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO content_items(
                id, source_id, external_id, content_type, title, body_text, published_at, url, status
            ) VALUES(
                2, 2, 'doc-2', 'bill', 'Official bill 123',
                'Official document for bill 123',
                '2026-04-02T09:00:00', 'https://example.org/bill/123', 'official'
            )
            """
        )

        conn.executemany(
            "INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES(?,?,?,?)",
            [
                (1, 1, "subject", 0.99),
                (2, 1, "organization", 0.95),
                (3, 1, "co_mention", 0.80),
                (4, 2, "customer", 0.90),
                (5, 2, "supplier", 0.90),
            ],
        )
        conn.executemany(
            "INSERT INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source) VALUES(?,?,?,?,?)",
            [
                (1, 1, "Corruption / Procurement", 0.95, "rule"),
                (1, 2, "Negative / Harm", 0.90, "rule"),
            ],
        )

        conn.execute(
            """
            INSERT INTO claims(
                id, content_item_id, claim_text, claim_type, confidence_auto, confidence_final, status, needs_review
            ) VALUES(
                1, 1, 'Ivan Ivanov voted for bill 123', 'vote_record', 0.9, 0.9, 'verified', 0
            )
            """
        )
        conn.execute(
            "INSERT INTO evidence_links(claim_id, evidence_item_id, evidence_type, strength, notes) VALUES(1,2,'legislative_document','strong','Official bill reference')"
        )

        conn.execute(
            """
            INSERT INTO cases(
                id, title, description, case_type, status, region, started_at, updated_at
            ) VALUES(
                1, 'Main Case', 'Case around bill 123', 'legislative_impact', 'open', 'Moscow', '2026-04-01', '2026-04-02'
            )
            """
        )
        conn.execute("INSERT INTO case_claims(case_id, claim_id, role) VALUES(1,1,'central')")
        conn.execute(
            """
            INSERT INTO case_events(case_id, event_date, event_title, event_description, content_item_id, event_order)
            VALUES(1, '2026-04-01', 'Initial publication', 'Telegram post created the case', 1, 0)
            """
        )

        conn.executemany(
            """
            INSERT INTO entity_relations(
                from_entity_id, to_entity_id, relation_type, evidence_item_id, strength, detected_by
            ) VALUES(?,?,?,?,?,?)
            """,
            [
                (1, 2, "works_at", 2, "strong", "official_positions"),
                (1, 3, "mentioned_together", None, "weak", "co_occurrence:2"),
            ],
        )

        conn.execute(
            """
            INSERT INTO risk_patterns(
                id, pattern_type, description, entity_ids, evidence_ids, risk_level, case_id, needs_review
            ) VALUES(
                1, 'corruption_risk', 'Ivan Ivanov procurement risk', '[1,2]', '[1,2]', 'critical', 1, 0
            )
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
            "INSERT INTO bill_sponsors(bill_id, entity_id, sponsor_name, sponsor_role, faction) VALUES(1,1,'Ivan Ivanov','author','Party X')"
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
            "INSERT INTO bill_votes(vote_session_id, entity_id, deputy_name, faction, vote_result) VALUES(1,1,'Ivan Ivanov','Party X','за')"
        )

        conn.execute(
            """
            INSERT INTO investigative_materials(
                id, content_item_id, material_type, title, summary, involved_entities, publication_date, source_org, raw_data
            ) VALUES(
                1, 2, 'government_contract', 'Contract CN-1', 'State Agency hired Vendor LLC',
                NULL,
                '2026-04-03', 'EIS',
                '{\"customer_inn\":\"111\",\"supplier_inn\":\"222\",\"contract_number\":\"CN-1\"}'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO investigative_materials(
                id, content_item_id, material_type, title, summary, involved_entities, publication_date, source_org, raw_data
            ) VALUES(
                2, NULL, 'government_contract', 'Contract CN-2', 'Budget Hospital hired MedTech Supplier',
                NULL,
                '2026-04-04', 'EIS',
                '{\"contract_number\":\"CN-2\"}'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO contracts(
                id, material_id, content_item_id, contract_number, title, summary, publication_date, source_org, customer_inn, supplier_inn, raw_data
            ) VALUES(
                1, 1, 2, 'CN-1', 'Contract CN-1', 'State Agency hired Vendor LLC', '2026-04-03', 'EIS', '111', '222',
                '{\"customer_inn\":\"111\",\"supplier_inn\":\"222\",\"contract_number\":\"CN-1\"}'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO contracts(
                id, material_id, content_item_id, contract_number, title, summary, publication_date, source_org, customer_inn, supplier_inn, raw_data
            ) VALUES(
                2, 2, NULL, 'CN-2', 'Contract CN-2', 'Budget Hospital hired MedTech Supplier', '2026-04-04', 'EIS', NULL, NULL,
                '{\"contract_number\":\"CN-2\"}'
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO contract_parties(contract_id, entity_id, party_name, party_role, inn)
            VALUES(?,?,?,?,?)
            """,
            [
                (1, 4, "State Agency", "customer", "111"),
                (1, 5, "Vendor LLC", "supplier", "222"),
                (2, 6, "Budget Hospital", "customer", None),
                (2, 7, "MedTech Supplier", "supplier", None),
            ],
        )
        conn.commit()
    finally:
        conn.close()


class ObsidianGraphExportTests(unittest.TestCase):
    def test_graph_export_creates_linked_notes_and_separates_weak_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "sample.db"
            vault = tmp_path / "vault"
            create_db(db_path)

            export_obsidian(db_path=db_path, vault=vault, mode="graph", copy_media=False)

            entity_note = vault / "Entities" / "person" / "1-Ivan-Ivanov.md"
            content_note = vault / "Content" / "2026-04" / "1-Deputy-voted-for-bill-123.md"
            claim_note = vault / "Claims" / "1-Ivan-Ivanov-voted-for-bill-123.md"
            case_note = vault / "Cases" / "1-Main-Case.md"
            risk_note = vault / "Risks" / "1-corruption-risk-Ivan-Ivanov-procurement-risk.md"
            bill_note = vault / "Bills" / "1-123-FZ.md"
            vote_note = vault / "VoteSessions" / "1-2026-04-02-123-FZ-first-reading.md"
            contract_note = vault / "Contracts" / "1-CN-1-Contract-CN-1.md"
            contract_note_2 = vault / "Contracts" / "2-CN-2-Contract-CN-2.md"
            weak_note = vault / "WeakLinks" / "person" / "1-Ivan-Ivanov.md"
            tag_index = vault / "Tags" / "index.md"

            for path in [entity_note, content_note, claim_note, case_note, risk_note, bill_note, vote_note, contract_note, contract_note_2, weak_note, tag_index]:
                self.assertTrue(path.exists(), path)

            entity_text = entity_note.read_text(encoding="utf-8")
            self.assertIn("## Strong links", entity_text)
            self.assertIn("[[Entities/organization/2-Party-X|Party X]]", entity_text)
            self.assertIn("## Claims", entity_text)
            self.assertIn("[[Claims/1-Ivan-Ivanov-voted-for-bill-123|Claim 1]]", entity_text)
            self.assertIn("## Cases", entity_text)
            self.assertIn("[[Cases/1-Main-Case|Main Case]]", entity_text)
            self.assertIn("## Risks", entity_text)
            self.assertIn("[[Risks/1-corruption-risk-Ivan-Ivanov-procurement-risk|corruption_risk #1]]", entity_text)
            self.assertIn("## Related content", entity_text)
            self.assertIn("[[Content/2026-04/1-Deputy-voted-for-bill-123|Deputy voted for bill 123]]", entity_text)
            self.assertIn("## Bills", entity_text)
            self.assertIn("[[Bills/1-123-FZ|123-FZ]]", entity_text)
            self.assertIn("## Vote sessions", entity_text)
            self.assertIn("[[VoteSessions/1-2026-04-02-123-FZ-first-reading|2026-04-02 123-FZ]]", entity_text)
            self.assertNotIn("[[Entities/person/3-Weak-Peer|Weak Peer]]", entity_text)
            self.assertIn("[[WeakLinks/person/1-Ivan-Ivanov|Weak similarity layer]]", entity_text)

            content_text = content_note.read_text(encoding="utf-8")
            self.assertIn("[[Entities/person/1-Ivan-Ivanov|Ivan Ivanov]]", content_text)
            self.assertIn("[[Entities/organization/2-Party-X|Party X]]", content_text)
            self.assertIn("[[Claims/1-Ivan-Ivanov-voted-for-bill-123|Claim 1]]", content_text)
            self.assertIn("[[Cases/1-Main-Case|Main Case]]", content_text)
            self.assertIn("[[Risks/1-corruption-risk-Ivan-Ivanov-procurement-risk|corruption_risk #1]]", content_text)
            self.assertIn("#corruption/procurement", content_text)
            self.assertIn("#negative/harm", content_text)
            self.assertNotIn("[[Sources/", content_text)

            weak_text = weak_note.read_text(encoding="utf-8")
            self.assertIn("[[Entities/person/3-Weak-Peer|Weak Peer]]", weak_text)
            self.assertIn("support_count", weak_text)

            contract_text = contract_note.read_text(encoding="utf-8")
            self.assertIn("[[Entities/organization/4-State-Agency|State Agency]]", contract_text)
            self.assertIn("[[Entities/organization/5-Vendor-LLC|Vendor LLC]]", contract_text)

            contract_text_2 = contract_note_2.read_text(encoding="utf-8")
            self.assertIn("[[Entities/organization/6-Budget-Hospital|Budget Hospital]]", contract_text_2)
            self.assertIn("[[Entities/organization/7-MedTech-Supplier|MedTech Supplier]]", contract_text_2)

            vote_text = vote_note.read_text(encoding="utf-8")
            self.assertIn("[[Bills/1-123-FZ|123-FZ]]", vote_text)
            self.assertIn("[[Entities/person/1-Ivan-Ivanov|Ivan Ivanov]]", vote_text)

    def test_graph_export_exposes_promoted_official_overlay_in_strong_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "sample.db"
            vault = tmp_path / "vault"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    """
                    INSERT INTO relation_candidates(
                        entity_a_id, entity_b_id, candidate_type, origin, score,
                        support_items, support_sources, support_domains, support_hard_evidence_count,
                        candidate_state, promotion_state, evidence_mix_json, explain_path_json
                    ) VALUES(
                        1, 2, 'likely_association', 'candidate_builder:co_occurrence', 0.92,
                        1, 1, 1, 1,
                        'promoted', 'promoted',
                        '{"bridge_types":["Content","Disclosure","OfficialDocument"],"official_content_types":["anticorruption_declaration"]}',
                        '[{"node_type":"Content","ids":[1]},{"node_type":"Disclosure","ids":[1]},{"node_type":"OfficialDocument","ids":[1]}]'
                    )
                    """
                )
                conn.commit()
            finally:
                conn.close()

            export_obsidian(db_path=db_path, vault=vault, mode="graph", copy_media=False)

            entity_note = vault / "Entities" / "person" / "1-Ivan-Ivanov.md"
            entity_text = entity_note.read_text(encoding="utf-8")
            self.assertIn("promoted official bridge", entity_text)
            self.assertIn("bridges Content, Disclosure, OfficialDocument", entity_text)


if __name__ == "__main__":
    unittest.main()
