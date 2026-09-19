from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

from db.reactor import open_reactor_db
from knowledge.reactor_import import replay_legacy_revisions


def _legacy_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE sources(
                id INTEGER PRIMARY KEY, name TEXT, category TEXT, url TEXT,
                is_official INTEGER, credibility_tier TEXT, owner TEXT, is_active INTEGER
            );
            CREATE TABLE raw_source_items(
                id INTEGER PRIMARY KEY, source_id INTEGER, external_id TEXT,
                raw_payload TEXT, collected_at TEXT
            );
            CREATE TABLE content_items(
                id INTEGER PRIMARY KEY, source_id INTEGER, raw_item_id INTEGER,
                content_type TEXT, title TEXT, body_text TEXT, published_at TEXT,
                url TEXT, status TEXT
            );
            CREATE TABLE raw_blobs(
                id INTEGER PRIMARY KEY, raw_item_id INTEGER, hash_sha256 TEXT,
                mime_type TEXT, file_size INTEGER, storage_rel_path TEXT,
                file_path TEXT, metadata_json TEXT
            );
            INSERT INTO sources VALUES(1,'YEP','telegram','https://t.me/yep_news',0,'C',NULL,1);
            INSERT INTO raw_source_items VALUES(1,1,'post:1','{"text":"alpha"}','2026-08-01');
            INSERT INTO content_items VALUES(
                1,1,1,'telegram_post','Alpha','alpha','2026-08-01',
                'https://t.me/yep_news/1','raw_signal'
            );
            INSERT INTO raw_blobs VALUES(1,1,'blobhash','image/png',100,'blob.png',NULL,NULL);
            """
        )
        conn.commit()
    finally:
        conn.close()


class ReactorImportTests(unittest.TestCase):
    def test_replay_is_read_only_and_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            legacy = Path(tmp) / "legacy.db"
            _legacy_db(legacy)
            before = hashlib.sha256(legacy.read_bytes()).hexdigest()
            settings = {"reactor_db_dir": str(Path(tmp) / "reactor")}

            first = replay_legacy_revisions(legacy, settings=settings)
            second = replay_legacy_revisions(legacy, settings=settings)

            self.assertEqual(before, hashlib.sha256(legacy.read_bytes()).hexdigest())
            self.assertEqual(1, first["revisions_created"])
            self.assertEqual(1, second["revisions_reused"])
            conn = open_reactor_db("knowledge", settings=settings)
            try:
                self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM source_revisions").fetchone()[0])
                self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM revision_blobs").fetchone()[0])
                self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM relation_assertions").fetchone()[0])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
