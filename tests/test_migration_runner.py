from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from db.migration_runner import apply_pending_migrations


class MigrationRunnerTests(unittest.TestCase):
    def test_applies_once_and_records_checksum(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            migration = directory / "0001_create_demo.sql"
            migration.write_text("CREATE TABLE demo(id INTEGER PRIMARY KEY);", encoding="utf-8")
            conn = sqlite3.connect(":memory:")

            first = apply_pending_migrations(conn, migrations_dir=directory)
            second = apply_pending_migrations(conn, migrations_dir=directory)

            self.assertEqual(["0001"], [row["version"] for row in first["applied"]])
            self.assertEqual(["0001"], second["skipped"])
            self.assertIsNotNone(
                conn.execute("SELECT 1 FROM sqlite_master WHERE name='demo'").fetchone()
            )

    def test_rejects_changed_applied_migration(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            migration = directory / "0001_create_demo.sql"
            migration.write_text("CREATE TABLE demo(id INTEGER PRIMARY KEY);", encoding="utf-8")
            conn = sqlite3.connect(":memory:")
            apply_pending_migrations(conn, migrations_dir=directory)
            migration.write_text("CREATE TABLE demo(id INTEGER, name TEXT);", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                apply_pending_migrations(conn, migrations_dir=directory)


if __name__ == "__main__":
    unittest.main()
