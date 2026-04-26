import sqlite3
import tempfile
import unittest
from pathlib import Path

from classifier.semantic_index import build_semantic_index


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_semantic_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Semantic Source', 'media', 'https://semantic.example.test', 1);

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (101, 1, 'article', 'Коррупция в закупках школы', 'Школьные закупки сопровождались завышением цен, фиктивной конкуренцией и откатами на государственных тендерах.', 'raw_signal'),
                (102, 1, 'article', 'Фиктивные тендеры и завышение цен', 'Государственные тендеры сопровождались завышением цен, фиктивной конкуренцией и откатами при школьных закупках.', 'raw_signal'),
                (103, 1, 'article', 'Спортивные результаты района', 'Команда района выиграла турнир и получила новые награды.', 'raw_signal');

            INSERT INTO claims(id, content_item_id, claim_text, canonical_text, status) VALUES
                (201, 101, 'На тендерах была фиктивная конкуренция и завышение цен', 'на тендерах была фиктивная конкуренция и завышение цен', 'unverified'),
                (202, 102, 'На тендерах была фиктивная конкуренция и завышение цен в закупках', 'на тендерах была фиктивная конкуренция и завышение цен в закупках', 'unverified'),
                (203, 103, 'Команда района выиграла турнир', 'команда района выиграла турнир', 'unverified');

            INSERT INTO entities(id, entity_type, canonical_name, description) VALUES
                (301, 'organization', 'Министерство финансов', 'Министерство финансов Российской Федерации, федеральный орган исполнительной власти в сфере бюджетной и финансовой политики'),
                (302, 'organization', 'Минфин России', 'Минфин России, Министерство финансов Российской Федерации, федеральный орган в сфере бюджетной и финансовой политики'),
                (303, 'organization', 'Спортивный комитет', 'Организация, отвечающая за районные спортивные мероприятия');
            """
        )
        conn.commit()
    finally:
        conn.close()


class SemanticIndexTests(unittest.TestCase):
    def test_build_semantic_index_creates_neighbors_for_similar_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "semantic.db"
            create_semantic_db(db_path)
            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}

            result = build_semantic_index(settings=settings, top_k=2, limit_per_kind=100)

            self.assertTrue(result["ok"])
            self.assertGreater(result["neighbors_indexed"], 0)

            conn = sqlite3.connect(db_path)
            try:
                content_neighbors = conn.execute(
                    """
                    SELECT source_id, neighbor_id, score
                    FROM semantic_neighbors
                    WHERE source_kind='content' AND source_id=101
                    ORDER BY score DESC, neighbor_id
                    """
                ).fetchall()
                claim_neighbors = conn.execute(
                    """
                    SELECT source_id, neighbor_id, score
                    FROM semantic_neighbors
                    WHERE source_kind='claim' AND source_id=201
                    ORDER BY score DESC, neighbor_id
                    """
                ).fetchall()
                entity_neighbors = conn.execute(
                    """
                    SELECT source_id, neighbor_id, score
                    FROM semantic_neighbors
                    WHERE source_kind='entity' AND source_id=301
                    ORDER BY score DESC, neighbor_id
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertTrue(any(row[1] == 102 for row in content_neighbors))
            self.assertTrue(any(row[1] == 202 for row in claim_neighbors))
            self.assertTrue(any(row[1] == 302 for row in entity_neighbors))


if __name__ == "__main__":
    unittest.main()
