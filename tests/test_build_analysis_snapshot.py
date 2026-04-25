import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tools.build_analysis_snapshot import normalize_contracts, semantic_relation_layer


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE entities (
                id INTEGER PRIMARY KEY,
                entity_type TEXT NOT NULL,
                canonical_name TEXT NOT NULL,
                inn TEXT,
                description TEXT
            );

            CREATE TABLE investigative_materials (
                id INTEGER PRIMARY KEY,
                content_item_id INTEGER,
                title TEXT NOT NULL,
                summary TEXT,
                material_type TEXT NOT NULL,
                involved_entities TEXT,
                publication_date TEXT,
                source_org TEXT,
                raw_data TEXT
            );
            """
        )
        conn.executemany(
            "INSERT INTO entities(id, entity_type, canonical_name, inn) VALUES(?,?,?,?)",
            [
                (1, "organization", 'ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ "ЗАКАЗЧИК"', "1234567890"),
                (2, "organization", 'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ПОСТАВЩИК"', "0987654321"),
            ],
        )
        conn.execute(
            """
            INSERT INTO investigative_materials(
                id, content_item_id, title, summary, material_type, involved_entities,
                publication_date, source_org, raw_data
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                7,
                70,
                "Контракт CN-77",
                "Поставка тестового товара",
                "government_contract",
                json.dumps(
                    [
                        {
                            "entity_id": 1,
                            "name": 'ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ "ЗАКАЗЧИК"',
                            "role": "заказчик",
                            "inn": "1234567890",
                        },
                        {
                            "entity_id": 2,
                            "name": 'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ПОСТАВЩИК"',
                            "role": "поставщик",
                            "inn": "0987654321",
                        },
                    ],
                    ensure_ascii=False,
                ),
                "2026-04-25",
                "zakupki.gov.ru",
                json.dumps(
                    {
                        "contract_number": "CN-77",
                        "customer": 'ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ "ЗАКАЗЧИК"',
                        "customer_inn": "1234567890",
                        "supplier": 'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ПОСТАВЩИК"',
                        "supplier_inn": "0987654321",
                    },
                    ensure_ascii=False,
                ),
            ),
        )
        conn.commit()
    finally:
        conn.close()


class BuildAnalysisSnapshotTests(unittest.TestCase):
    def test_normalize_contracts_deduplicates_russian_and_english_party_roles(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "snapshot.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                stats = normalize_contracts(conn)
                parties = [
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT contract_id, entity_id, party_role, inn
                        FROM contract_parties
                        ORDER BY id
                        """
                    ).fetchall()
                ]
            finally:
                conn.close()

            self.assertEqual(stats["contracts"], 1)
            self.assertEqual(stats["parties"], 2)
            self.assertEqual(
                parties,
                [
                    (7, 1, "customer", "1234567890"),
                    (7, 2, "supplier", "0987654321"),
                ],
            )

    def test_semantic_relation_layer_marks_evidence_backed_structural_relations_as_evidence(self):
        self.assertEqual(
            semantic_relation_layer("works_at", "official_positions", 101),
            "evidence",
        )
        self.assertEqual(
            semantic_relation_layer("mentioned_together", "co_occurrence:4", None),
            "weak_similarity",
        )
        self.assertEqual(
            semantic_relation_layer("works_at", "official_positions", None),
            "structural",
        )


if __name__ == "__main__":
    unittest.main()
