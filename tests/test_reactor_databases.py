from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from db.reactor import bootstrap_reactor_databases, open_reactor_db, reactor_paths


class ReactorDatabasesTests(unittest.TestCase):
    def test_bootstrap_creates_three_isolated_databases_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            legacy = Path(tmp) / "legacy.db"
            legacy.write_bytes(b"legacy-unchanged")
            settings = {"reactor_db_dir": tmp}

            first = bootstrap_reactor_databases(settings)
            second = bootstrap_reactor_databases(settings)

            self.assertTrue(first["ok"])
            self.assertEqual(b"legacy-unchanged", legacy.read_bytes())
            paths = reactor_paths(settings)
            self.assertTrue(paths.knowledge.exists())
            self.assertTrue(paths.ops.exists())
            self.assertTrue(paths.search.exists())
            self.assertTrue(second["paths"]["knowledge"]["migration"]["skipped"])

    def test_knowledge_schema_enforces_one_current_revision(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = {"reactor_db_dir": tmp}
            bootstrap_reactor_databases(settings)
            conn = open_reactor_db("knowledge", settings=settings)
            try:
                conn.execute(
                    "INSERT INTO source_systems(source_key,source_type,title) VALUES('s','rss','S')"
                )
                conn.execute(
                    """
                    INSERT INTO source_objects(
                        source_system_id,external_id,first_seen_at,last_seen_at
                    ) VALUES(1,'x','2026-01-01','2026-01-01')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO source_revisions(
                        source_object_id,revision_no,payload_hash,payload_json,fetched_at,is_current
                    ) VALUES(1,1,'a','{}','2026-01-01',1)
                    """
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(
                        """
                        INSERT INTO source_revisions(
                            source_object_id,revision_no,payload_hash,payload_json,fetched_at,is_current
                        ) VALUES(1,2,'b','{}','2026-01-02',1)
                        """
                    )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
