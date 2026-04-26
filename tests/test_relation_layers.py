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


def create_case_cluster_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Case Source A', 'media', 'https://case-a.example.test', 1),
                (2, 'Case Source B', 'media', 'https://case-b.example.test', 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (40, 'person', 'Фигурант А'),
                (41, 'organization', 'Компания Б');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (301, 1, 'article', 'Case A', 'Case A body', 'raw_signal'),
                (302, 2, 'article', 'Case B', 'Case B body', 'raw_signal');

            INSERT INTO claims(id, content_item_id, claim_text, status) VALUES
                (701, 301, 'Claim A', 'pending'),
                (702, 302, 'Claim B', 'pending');

            INSERT INTO cases(id, title, description, case_type, status) VALUES
                (801, 'Кейс 801', 'Расследование', 'investigation', 'open');

            INSERT INTO case_claims(case_id, claim_id, role) VALUES
                (801, 701, 'central'),
                (801, 702, 'supporting');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (40, 301, 'subject', 1.0),
                (41, 302, 'organization', 1.0);
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_case_claim_only_seed_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Seed Source', 'media', 'https://seed.example.test', 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (50, 'person', 'Фигурант В'),
                (51, 'organization', 'Компания Г');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status)
            VALUES
                (401, 1, 'article', 'Claim A', 'Claim body A', 'raw_signal'),
                (402, 1, 'article', 'Claim B', 'Claim body B', 'raw_signal');

            INSERT INTO claims(id, content_item_id, claim_text, claim_type, canonical_text, canonical_hash, claim_cluster_id, status)
            VALUES
                (901, 401, 'Совместное утверждение', 'fact', 'совместное утверждение', 'hash-1', 77, 'unverified'),
                (902, 402, 'Совместное утверждение', 'fact', 'совместное утверждение', 'hash-1', 77, 'unverified');

            INSERT INTO cases(id, title, description, case_type, status) VALUES
                (950, 'Кейс 950', 'Case cluster only', 'investigation', 'open');

            INSERT INTO case_claims(case_id, claim_id, role) VALUES
                (950, 901, 'central'),
                (950, 902, 'supporting');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (50, 401, 'subject', 1.0),
                (51, 402, 'organization', 1.0);
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
            self.assertEqual(rows[0][6], "review")
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
            self.assertEqual(rows[0][6], "review")
            self.assertEqual(weak_edges, 0)

    def test_relation_candidate_builder_keeps_structural_only_contract_pairs_as_seed_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_structural_relation_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state, candidate_state, support_items, support_sources, support_domains
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
            self.assertEqual(rows[0][3:5], ("seed_only", "seed_only"))
            self.assertEqual(rows[0][5:8], (0, 0, 0))
            self.assertEqual(promoted, 0)

    def test_relation_candidate_builder_keeps_structural_seed_only_when_only_semantic_support_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_structural_relation_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO semantic_neighbors(source_kind, source_id, neighbor_kind, neighbor_id, score, method)
                    VALUES
                        ('entity', 10, 'entity', 11, 0.71, 'tfidf'),
                        ('entity', 11, 'entity', 10, 0.71, 'tfidf');
                    """
                )
                conn.commit()
            finally:
                conn.close()

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT candidate_type, promotion_state, candidate_state, semantic_score
                    FROM relation_candidates
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
                support = conn.execute(
                    """
                    SELECT support_kind, metric_value
                    FROM relation_support
                    WHERE support_kind='semantic_neighbor'
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(result["relation_candidates_created"], 1)
            self.assertEqual(row[0], "same_contract_cluster")
            self.assertEqual(row[1:3], ("seed_only", "seed_only"))
            self.assertGreaterEqual(float(row[3] or 0.0), 0.71)
            self.assertTrue(support)

    def test_relation_candidate_builder_keeps_structural_vote_pattern_as_seed_only_without_support(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_vote_pattern_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state, candidate_state
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
            self.assertEqual(result["promoted_relations"], 0)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0], (20, 21, "same_vote_pattern", "seed_only", "seed_only"))
            self.assertEqual(promoted, [])

    def test_relation_candidate_builder_marks_structural_bill_cluster_as_seed_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_bill_cluster_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state, candidate_state
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
            self.assertEqual(rows, [(30, 31, "same_bill_cluster", "seed_only", "seed_only")])
            self.assertEqual(promoted, 0)

    def test_relation_candidate_builder_marks_structural_case_cluster_as_seed_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_case_cluster_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state, candidate_state
                    FROM relation_candidates
                    ORDER BY id
                    """
                ).fetchall()
                support = conn.execute(
                    """
                    SELECT support_kind, metadata_json
                    FROM relation_support
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
            self.assertEqual(rows, [(40, 41, "same_case_cluster", "seed_only", "seed_only")])
            self.assertTrue(any(kind == "case" and '"case_id": 801' in metadata for kind, metadata in support))
            self.assertEqual(promoted, 0)

    def test_relation_candidate_builder_does_not_move_case_seed_to_review_without_evidence_items(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_case_claim_only_seed_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state, candidate_state,
                           support_items, support_sources, support_domains, support_claim_cluster_count
                    FROM relation_candidates
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
                support_rows = conn.execute(
                    """
                    SELECT support_kind, metadata_json
                    FROM relation_support
                    ORDER BY id
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(result["relation_candidates_created"], 1)
            self.assertEqual(row[0:5], (50, 51, "same_case_cluster", "seed_only", "seed_only"))
            self.assertEqual(row[5:8], (0, 0, 0))
            self.assertEqual(row[8], 1)
            self.assertTrue(any(kind == "claim_cluster" for kind, _ in support_rows))


if __name__ == "__main__":
    unittest.main()
