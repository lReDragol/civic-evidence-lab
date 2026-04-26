import sqlite3
import tempfile
import unittest
from pathlib import Path

from config.db_utils import SCHEMA_PATH


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


class StructuralCaseTitleTests(unittest.TestCase):
    def test_dedupe_duplicate_case_titles_suffixes_by_case_type(self):
        from cases.structural_links import dedupe_duplicate_case_titles

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "cases.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO cases(id, title, description, case_type, status, started_at)
                    VALUES
                        (1, 'Закон 1191451-8: О внесении изменений в Налоговый кодекс', 'A', 'legislative_corruption', 'open', '2026-04-26'),
                        (2, 'Закон 1191451-8: О внесении изменений в Налоговый кодекс', 'B', 'legislative_impact', 'open', '2026-04-26');
                    """
                )
                conn.commit()

                updated = dedupe_duplicate_case_titles(conn)
                conn.commit()

                rows = conn.execute(
                    "SELECT id, title FROM cases ORDER BY id"
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(updated, 2)
            self.assertEqual(
                rows,
                [
                    (1, "Закон 1191451-8: О внесении изменений в Налоговый кодекс · коррупционный контур"),
                    (2, "Закон 1191451-8: О внесении изменений в Налоговый кодекс · общественное влияние"),
                ],
            )


if __name__ == "__main__":
    unittest.main()
