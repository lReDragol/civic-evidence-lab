import json
import sqlite3
import tempfile
import threading
import time
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


class AiKeyPoolTests(unittest.TestCase):
    def test_import_keys_from_malformed_key_json_is_tolerant(self):
        from llm.key_pool import bootstrap_provider_catalog, import_keys_from_file

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            keys_path = Path(tmp) / "key.json"
            create_db(db_path)
            keys_path.write_text(
                "{\n"
                "  }\n"
                "\"provider_counts\": {\"openai\": 1, \"mistral\": 2},\n"
                "\"keys\": [\n"
                "  {\"provider\": \"openai\", \"api_key\": \"fake-openai-key-1\", \"status\": \"active\"},\n"
                "  {\"provider\": \"mistral\", \"api_key\": \"fake-mistral-key-1\", \"status\": \"active\"},\n"
                "  {\"provider\": \"groq\", \"api_key\": \"fake-groq-key-1\", \"status\": \"active\"}\n"
                "]\n"
                "}\n",
                encoding="utf-8",
            )

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                bootstrap_provider_catalog(conn)
                result = import_keys_from_file(conn, keys_path)
                rows = conn.execute(
                    "SELECT provider, status, failure_count FROM llm_keys ORDER BY provider"
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(result["inserted"], 3)
            self.assertEqual(result["updated"], 0)
            self.assertEqual([tuple(row) for row in rows], [
                ("groq", "active", 0),
                ("mistral", "active", 0),
                ("openai", "active", 0),
            ])

    def test_record_key_failure_hard_removes_after_threshold(self):
        from llm.key_pool import record_key_failure

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                cur = conn.execute(
                    """
                    INSERT INTO llm_keys(provider, api_key, key_hash, status, failure_count)
                    VALUES('perplexity', 'fake-perplexity-key-1', 'hash-1', 'active', 0)
                    """
                )
                key_id = int(cur.lastrowid)
                conn.commit()

                record_key_failure(conn, key_id, failure_kind="auth", error_text="401 invalid", remove_threshold=3)
                record_key_failure(conn, key_id, failure_kind="auth", error_text="401 invalid", remove_threshold=3)
                result = record_key_failure(conn, key_id, failure_kind="auth", error_text="401 invalid", remove_threshold=3)

                row = conn.execute(
                    "SELECT status, failure_count, removed_at, last_error FROM llm_keys WHERE id=?",
                    (key_id,),
                ).fetchone()
            finally:
                conn.close()

            self.assertTrue(result["removed"])
            self.assertEqual(row[0], "removed")
            self.assertEqual(row[1], 3)
            self.assertIsNotNone(row[2])
            self.assertIn("401", row[3])

    def test_removed_key_is_not_reactivated_by_reimport_from_key_file(self):
        from llm.key_pool import _hash_key, bootstrap_provider_catalog, import_keys_from_file

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            keys_path = Path(tmp) / "key.json"
            create_db(db_path)
            keys_path.write_text(
                json.dumps(
                    {
                        "keys": [
                            {"provider": "perplexity", "api_key": "fake-perplexity-removed", "status": "active"},
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                bootstrap_provider_catalog(conn)
                conn.execute(
                    """
                    INSERT INTO llm_keys(provider, api_key, key_hash, status, failure_count, removed_at)
                    VALUES('perplexity', 'fake-perplexity-removed', ?, 'removed', 3, '2026-04-27T00:00:00')
                    """
                    ,
                    (_hash_key("perplexity", "fake-perplexity-removed"),),
                )
                conn.commit()

                result = import_keys_from_file(conn, keys_path)
                row = conn.execute(
                    "SELECT status, failure_count, removed_at FROM llm_keys WHERE provider='perplexity' AND api_key='fake-perplexity-removed'"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(result["updated"], 1)
            self.assertEqual(row[0], "removed")
            self.assertEqual(row[1], 3)
            self.assertIsNotNone(row[2])

    def test_bootstrap_provider_catalog_registers_search_capable_models(self):
        from llm.key_pool import bootstrap_provider_catalog

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                result = bootstrap_provider_catalog(conn)
                providers = conn.execute(
                    """
                    SELECT provider, COUNT(*)
                    FROM llm_provider_models
                    WHERE is_active=1 AND supports_web_search=1
                    GROUP BY provider
                    ORDER BY provider
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertGreaterEqual(result["inserted"] + result["updated"], 5)
            self.assertEqual([provider for provider, _count in providers], [
                "groq",
                "mistral",
                "openai",
                "openrouter",
                "perplexity",
            ])

    def test_reactivate_recoverable_keys_restores_provider_and_rate_removed_keys(self):
        from llm.key_pool import reactivate_recoverable_keys

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    """
                    INSERT INTO llm_keys(provider, api_key, key_hash, status, failure_count, removed_at, last_failure_kind, last_error)
                    VALUES('mistral', 'm-key', 'm-hash', 'removed', 3, '2026-04-27T00:00:00', 'provider', '400 invalid_tools')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO llm_keys(provider, api_key, key_hash, status, failure_count, removed_at, last_failure_kind, last_error)
                    VALUES('groq', 'g-key', 'g-hash', 'removed', 1, '2026-04-27T00:00:00', 'rate', '429 Rate limit reached')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO llm_keys(provider, api_key, key_hash, status, failure_count, removed_at, last_failure_kind, last_error)
                    VALUES('groq', 'g-auth-mislabel', 'g-auth-mislabel-hash', 'removed', 1, '2026-04-27T00:00:00', 'auth', '429 Rate limit reached')
                    """
                )
                conn.commit()

                result = reactivate_recoverable_keys(conn)
                rows = conn.execute(
                    "SELECT provider, status, failure_count, removed_at FROM llm_keys ORDER BY provider"
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(result["reactivated"], 3)
            self.assertEqual([tuple(row) for row in rows], [
                ("groq", "active", 0, None),
                ("groq", "active", 0, None),
                ("mistral", "active", 0, None),
            ])

    def test_record_key_success_waits_out_short_sqlite_write_lock(self):
        from llm.key_pool import record_key_success

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)

            setup = sqlite3.connect(db_path)
            try:
                cur = setup.execute(
                    """
                    INSERT INTO llm_keys(provider, api_key, key_hash, status, failure_count)
                    VALUES('mistral', 'm-key', 'm-hash', 'active', 2)
                    """
                )
                key_id = int(cur.lastrowid)
                setup.commit()
            finally:
                setup.close()

            locker = sqlite3.connect(db_path, check_same_thread=False)
            locker.execute("BEGIN IMMEDIATE")
            locker.execute("UPDATE llm_keys SET updated_at='locked' WHERE id=?", (key_id,))

            def release_lock():
                time.sleep(0.15)
                locker.commit()
                locker.close()

            releaser = threading.Thread(target=release_lock)
            releaser.start()
            worker = sqlite3.connect(db_path)
            worker.execute("PRAGMA busy_timeout = 20")
            try:
                record_key_success(worker, key_id)
                row = worker.execute(
                    "SELECT status, failure_count, last_used_at FROM llm_keys WHERE id=?",
                    (key_id,),
                ).fetchone()
            finally:
                worker.close()
                releaser.join(timeout=2)

            self.assertEqual(row[0], "active")
            self.assertEqual(row[1], 0)
            self.assertIsNotNone(row[2])

    def test_record_key_failure_waits_out_short_sqlite_write_lock(self):
        from llm.key_pool import record_key_failure

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)

            setup = sqlite3.connect(db_path)
            try:
                cur = setup.execute(
                    """
                    INSERT INTO llm_keys(provider, api_key, key_hash, status, failure_count)
                    VALUES('groq', 'g-key', 'g-hash', 'active', 0)
                    """
                )
                key_id = int(cur.lastrowid)
                setup.commit()
            finally:
                setup.close()

            locker = sqlite3.connect(db_path, check_same_thread=False)
            locker.execute("BEGIN IMMEDIATE")
            locker.execute("UPDATE llm_keys SET updated_at='locked' WHERE id=?", (key_id,))

            def release_lock():
                time.sleep(0.15)
                locker.commit()
                locker.close()

            releaser = threading.Thread(target=release_lock)
            releaser.start()
            worker = sqlite3.connect(db_path)
            worker.execute("PRAGMA busy_timeout = 20")
            try:
                result = record_key_failure(
                    worker,
                    key_id,
                    failure_kind="timeout",
                    error_text="request timed out",
                    remove_threshold=3,
                )
                row = worker.execute(
                    "SELECT status, failure_count, last_failure_kind FROM llm_keys WHERE id=?",
                    (key_id,),
                ).fetchone()
            finally:
                worker.close()
                releaser.join(timeout=2)

            self.assertFalse(result["removed"])
            self.assertEqual(row[0], "active")
            self.assertEqual(row[1], 1)
            self.assertEqual(row[2], "timeout")


if __name__ == "__main__":
    unittest.main()
