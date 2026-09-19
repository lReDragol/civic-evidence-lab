import sqlite3
import tempfile
import unittest
from pathlib import Path
from db.migration_runner import apply_pending_migrations


class MigrationAtomicityTests(unittest.TestCase):
    def test_ledger_failure_rolls_back_schema_and_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            conn = sqlite3.connect(":memory:")
            apply_pending_migrations(conn, migrations_dir=root)
            conn.execute("CREATE TRIGGER ledger_failure BEFORE INSERT ON schema_migrations BEGIN SELECT RAISE(ABORT,'crash'); END")
            conn.commit()
            (root / "0001_sample.sql").write_text("CREATE TABLE sample(id INTEGER); INSERT INTO sample VALUES(1);", encoding="utf-8")
            with self.assertRaises(sqlite3.IntegrityError):
                apply_pending_migrations(conn, migrations_dir=root)
            self.assertIsNone(conn.execute("SELECT name FROM sqlite_master WHERE name='sample'").fetchone())
            conn.execute("DROP TRIGGER ledger_failure")
            apply_pending_migrations(conn, migrations_dir=root)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM sample").fetchone()[0], 1)
            apply_pending_migrations(conn, migrations_dir=root)
            conn.close()

    def test_trigger_semicolons_and_forbidden_commit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            conn = sqlite3.connect(":memory:")
            (root / "0001_sample.sql").write_text("CREATE TABLE sample(id INTEGER); CREATE TRIGGER t AFTER INSERT ON sample BEGIN UPDATE sample SET id=id+1; END;", encoding="utf-8")
            apply_pending_migrations(conn, migrations_dir=root)
            (root / "0002_bad.sql").write_text("CREATE TABLE bad(id); COMMIT;", encoding="utf-8")
            with self.assertRaises(sqlite3.DatabaseError):
                apply_pending_migrations(conn, migrations_dir=root)
            self.assertIsNone(conn.execute("SELECT name FROM sqlite_master WHERE name='bad'").fetchone())
            conn.close()
