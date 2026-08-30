import sqlite3
import tempfile
import unittest
from pathlib import Path

from config.db_utils import get_db


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


def create_telethon_session(path: Path, *, with_auth: bool = True):
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE sessions (
                dc_id INTEGER PRIMARY KEY,
                server_address TEXT,
                port INTEGER,
                auth_key BLOB,
                takeout_id INTEGER
            );
            CREATE TABLE entities (
                id INTEGER PRIMARY KEY,
                hash INTEGER NOT NULL,
                username TEXT,
                phone INTEGER,
                name TEXT,
                date INTEGER
            );
            """
        )
        conn.execute(
            """
            INSERT INTO sessions(dc_id, server_address, port, auth_key, takeout_id)
            VALUES(2, '149.154.167.50', 443, ?, NULL)
            """,
            (b"auth-key" if with_auth else None,),
        )
        conn.commit()
    finally:
        conn.close()


class TelegramSessionPoolTests(unittest.TestCase):
    def test_imports_valid_telethon_session_as_active(self):
        from collectors.telegram_session_pool import import_telegram_sessions

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "news.db"
            session_path = root / "232354072_telethon.session"
            create_db(db_path)
            create_telethon_session(session_path)

            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "telegram_telethon_session_paths": [str(session_path)],
            }
            conn = get_db(settings)
            try:
                result = import_telegram_sessions(conn, settings)
                row = conn.execute(
                    """
                    SELECT session_key, client_type, session_path, status, failure_class
                    FROM telegram_sessions
                    WHERE session_key='232354072'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(result["active_count"], 1)
            self.assertEqual(row["client_type"], "telethon")
            self.assertEqual(row["session_path"], str(session_path))
            self.assertEqual(row["status"], "active")
            self.assertIsNone(row["failure_class"])

    def test_imports_broken_telethon_session_as_failed(self):
        from collectors.telegram_session_pool import import_telegram_sessions

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "news.db"
            session_path = root / "broken_telethon.session"
            create_db(db_path)
            create_telethon_session(session_path, with_auth=False)

            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "telegram_telethon_session_paths": [str(session_path)],
            }
            conn = get_db(settings)
            try:
                result = import_telegram_sessions(conn, settings)
                row = conn.execute(
                    """
                    SELECT session_key, status, failure_class
                    FROM telegram_sessions
                    WHERE session_key='broken'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(result["active_count"], 0)
            self.assertEqual(row["status"], "failed")
            self.assertEqual(row["failure_class"], "unauthorized_session")

    def test_assigns_sources_deterministically_and_skips_cooldown_sessions(self):
        from collectors.telegram_session_pool import assign_telegram_sources

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "news.db"
            create_db(db_path)
            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}
            conn = get_db(settings)
            try:
                conn.executescript(
                    """
                    INSERT INTO sources(id, name, category, url, access_method, is_active)
                    VALUES
                        (1, 'A', 'telegram', 'https://t.me/a', 'telegram_tdlib', 1),
                        (2, 'B', 'telegram', 'https://t.me/b', 'telegram_tdlib', 1),
                        (3, 'C', 'telegram', 'https://t.me/c', 'telegram_tdlib', 1),
                        (4, 'D', 'telegram', 'https://t.me/d', 'telegram_tdlib', 1);
                    INSERT INTO telegram_sessions(session_key, client_type, session_path, status)
                    VALUES
                        ('s1', 'telethon', 's1.session', 'active'),
                        ('s2', 'telethon', 's2.session', 'active');
                    """
                )
                first = assign_telegram_sources(conn, assignment_version="test-v1")
                second = assign_telegram_sources(conn, assignment_version="test-v1")

                conn.execute(
                    """
                    UPDATE telegram_sessions
                    SET status='cooldown', cooldown_until='2999-01-01T00:00:00'
                    WHERE session_key='s1'
                    """
                )
                reassigned = assign_telegram_sources(conn, assignment_version="test-v2")
            finally:
                conn.close()

            self.assertEqual(first["assigned_sources"], 4)
            self.assertEqual(first["assignments"], second["assignments"])
            self.assertEqual(reassigned["assigned_sources"], 4)
            self.assertEqual(set(reassigned["assignments"].values()), {"s2"})

    def test_session_progress_records_current_channel_and_counters(self):
        from collectors.telegram_session_pool import mark_session_progress, mark_session_result

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "news.db"
            create_db(db_path)
            settings = {"db_path": str(db_path), "ensure_schema_on_connect": True}
            conn = get_db(settings)
            try:
                conn.executescript(
                    """
                    INSERT INTO sources(id, name, category, url, access_method, is_active)
                    VALUES(1, 'YEP', 'telegram', 'https://t.me/yep_news', 'telegram_tdlib', 1);
                    INSERT INTO telegram_sessions(session_key, client_type, session_path, status)
                    VALUES('s1', 'telethon', 's1.session', 'active');
                    """
                )
                mark_session_progress(
                    conn,
                    "s1",
                    source_id=1,
                    channel="yep_news",
                    collecting_now=True,
                    current_job_id="telegram_telethon_pool",
                    last_message_id="200",
                    last_message_date="2026-05-16T10:00:00",
                    collected_delta=3,
                    duplicate_delta=1,
                    failed_delta=2,
                )
                mid = conn.execute(
                    """
                    SELECT collecting_now, current_channel, current_source_id, current_job_id,
                           last_message_id, last_message_date, collected_current_run,
                           duplicates_skipped, failed_items
                    FROM telegram_sessions WHERE session_key='s1'
                    """
                ).fetchone()
                mark_session_result(conn, "s1", success=True, metadata={"done": True})
                final = conn.execute(
                    "SELECT collecting_now, current_channel, failure_class FROM telegram_sessions WHERE session_key='s1'"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(mid["collecting_now"], 1)
            self.assertEqual(mid["current_channel"], "yep_news")
            self.assertEqual(mid["current_source_id"], 1)
            self.assertEqual(mid["current_job_id"], "telegram_telethon_pool")
            self.assertEqual(mid["last_message_id"], "200")
            self.assertEqual(mid["last_message_date"], "2026-05-16T10:00:00")
            self.assertEqual(mid["collected_current_run"], 3)
            self.assertEqual(mid["duplicates_skipped"], 1)
            self.assertEqual(mid["failed_items"], 2)
            self.assertEqual(final["collecting_now"], 0)
            self.assertEqual(final["current_channel"], "yep_news")
            self.assertIsNone(final["failure_class"])
