import sqlite3
import tempfile
import unittest
from pathlib import Path

from analysis.entity_relation_builder import build_co_occurrence_from_mentions
from graph.relation_candidates import rebuild_relation_candidates
from ner.relation_extractor import extract_co_occurrence_relations


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_relation_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Source A', 'media', 'https://a.example.test/a', 1),
                (2, 'Source B', 'media', 'https://b.example.test/b', 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (1, 'person', 'Иван Иванов'),
                (2, 'organization', 'Министерство тестирования'),
                (3, 'location', 'Москва');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (101, 1, 'article', 'A1', 'A1', 'raw_signal'),
                (102, 1, 'article', 'A2', 'A2', 'raw_signal'),
                (103, 2, 'article', 'B1', 'B1', 'raw_signal');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (1, 101, 'subject', 1.0),
                (2, 101, 'organization', 1.0),
                (3, 101, 'location', 1.0),
                (1, 102, 'subject', 1.0),
                (2, 102, 'organization', 1.0),
                (3, 102, 'location', 1.0),
                (1, 103, 'subject', 1.0),
                (2, 103, 'organization', 1.0);

            INSERT INTO entity_relations(id, from_entity_id, to_entity_id, relation_type, strength, detected_by)
            VALUES (900, 1, 3, 'mentioned_together', 'weak', 'co_occurrence:2');
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_structural_relation_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (10, 'organization', 'Заказчик'),
                (11, 'organization', 'Поставщик');

            INSERT INTO contracts(id, title, contract_number) VALUES
                (501, 'Контракт 501', '501');

            INSERT INTO contract_parties(contract_id, entity_id, party_name, party_role, inn) VALUES
                (501, 10, 'Заказчик', 'customer', '1000000000'),
                (501, 11, 'Поставщик', 'supplier', '2000000000');
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_vote_pattern_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (20, 'person', 'Депутат А'),
                (21, 'person', 'Депутат Б');
            """
        )
        for session_id in range(1, 21):
            conn.execute(
                """
                INSERT INTO bill_vote_sessions(id, vote_date, vote_stage, result)
                VALUES(?, '2026-01-01', ?, 'accepted')
                """,
                (session_id, f'Vote {session_id}'),
            )
            conn.execute(
                """
                INSERT INTO bill_votes(vote_session_id, entity_id, deputy_name, vote_result)
                VALUES(?, 20, 'Депутат А', 'за')
                """,
                (session_id,),
            )
            conn.execute(
                """
                INSERT INTO bill_votes(vote_session_id, entity_id, deputy_name, vote_result)
                VALUES(?, 21, 'Депутат Б', 'за')
                """,
                (session_id,),
            )
        conn.commit()
    finally:
        conn.close()


def create_bill_cluster_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (30, 'person', 'Депутат В'),
                (31, 'person', 'Депутат Г');

            INSERT INTO bills(id, number, title) VALUES
                (601, '601', 'Bill 601'),
                (602, '602', 'Bill 602'),
                (603, '603', 'Bill 603');

            INSERT INTO bill_sponsors(bill_id, entity_id, sponsor_name, sponsor_role) VALUES
                (601, 30, 'Депутат В', 'sponsor'),
                (601, 31, 'Депутат Г', 'sponsor'),
                (602, 30, 'Депутат В', 'sponsor'),
                (602, 31, 'Депутат Г', 'sponsor'),
                (603, 30, 'Депутат В', 'sponsor'),
                (603, 31, 'Депутат Г', 'sponsor');
            """
        )
        conn.commit()
    finally:
        conn.close()


class RelationLayerTests(unittest.TestCase):
    def test_relation_extractor_requires_independent_sources_and_cleans_old_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_relation_db(db_path)

            result = extract_co_occurrence_relations({"db_path": str(db_path)})
            self.assertEqual(result["relations_inserted"], 0)
            self.assertEqual(result["relation_candidates_created"], 1)

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, support_items, support_sources, support_domains, promotion_state
                    FROM relation_candidates
                    ORDER BY id
                    """
                ).fetchall()
                promoted = conn.execute(
                    "SELECT COUNT(*) FROM entity_relations WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0:3], (1, 2, "likely_association"))
            self.assertEqual(rows[0][3:6], (3, 2, 2))
            self.assertEqual(rows[0][6], "pending")
            self.assertEqual(promoted, 0)

    def test_entity_relation_builder_skips_same_source_only_pair(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_relation_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                created = build_co_occurrence_from_mentions(conn)
                conn.commit()
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, support_items, support_sources, support_domains, promotion_state
                    FROM relation_candidates
                    ORDER BY id
                    """
                ).fetchall()
                weak_edges = conn.execute(
                    "SELECT COUNT(*) FROM entity_relations WHERE relation_type='mentioned_together'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(created, 1)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0:3], (1, 2, "likely_association"))
            self.assertEqual(rows[0][3:6], (3, 2, 2))
            self.assertEqual(rows[0][6], "pending")
            self.assertEqual(weak_edges, 0)

    def test_relation_candidate_builder_keeps_structural_only_contract_pairs_for_review(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_structural_relation_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state, support_items, support_sources, support_domains
                    FROM relation_candidates
                    ORDER BY id
                    """
                ).fetchall()
                promoted = conn.execute(
                    "SELECT COUNT(*) FROM entity_relations WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(result["relation_candidates_created"], 1)
            self.assertEqual(result["promoted_relations"], 0)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0:3], (10, 11, "same_contract_cluster"))
            self.assertEqual(rows[0][3], "review")
            self.assertEqual(rows[0][4:7], (0, 0, 0))
            self.assertEqual(promoted, 0)

    def test_relation_candidate_builder_promotes_structural_vote_pattern(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_vote_pattern_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state
                    FROM relation_candidates
                    ORDER BY id
                    """
                ).fetchall()
                promoted = conn.execute(
                    """
                    SELECT from_entity_id, to_entity_id, relation_type
                    FROM entity_relations
                    WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(result["relation_candidates_created"], 1)
            self.assertEqual(result["promoted_relations"], 1)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0], (20, 21, "same_vote_pattern", "promoted"))
            self.assertEqual(promoted, [(20, 21, "same_vote_pattern")])

    def test_relation_candidate_builder_reviews_structural_bill_cluster(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_bill_cluster_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state
                    FROM relation_candidates
                    ORDER BY id
                    """
                ).fetchall()
                promoted = conn.execute(
                    "SELECT COUNT(*) FROM entity_relations WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(result["relation_candidates_created"], 1)
            self.assertEqual(result["promoted_relations"], 0)
            self.assertEqual(rows, [(30, 31, "same_bill_cluster", "review")])
            self.assertEqual(promoted, 0)


if __name__ == "__main__":
    unittest.main()
