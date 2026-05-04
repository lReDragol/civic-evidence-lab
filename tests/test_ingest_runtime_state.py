import asyncio
import json
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from collectors import telegram_collector, watch_folder
from config.db_utils import get_db
from media_pipeline import ocr


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


class IngestRuntimeStateTests(unittest.TestCase):
    def test_telegram_relevance_keeps_russian_negative_documents_and_drops_global_noise(self):
        source = {
            "name": "YEP",
            "url": "https://t.me/yep_news",
            "subcategory": "media",
            "political_alignment": "media",
            "notes": "Telegram news channel",
        }

        rkn = telegram_collector.classify_message_relevance(
            "Штрафы до 15 млн рублей начал выписывать РКН владельцам сайтов зоны .RU. "
            "В требовании Роскомнадзора говорится о политике конфиденциальности, Google Analytics "
            "и трансграничной передаче персональных данных.",
            has_media=True,
            source=source,
            store_mode="negative_only",
        )
        self.assertTrue(rkn["keep"])
        self.assertTrue(rkn["is_document_like"])
        self.assertIn("restriction/censorship", rkn["navigation_tags"])

        vpn = telegram_collector.classify_message_relevance(
            "Депутат Госдумы Денис Парфенов направил главе Минцифры Максуту Шадаеву "
            "официальный запрос после сообщений о планах ввести отдельную плату за VPN-трафик.",
            has_media=True,
            source=source,
            store_mode="negative_only",
        )
        self.assertTrue(vpn["keep"])
        self.assertTrue(vpn["is_document_like"])
        self.assertIn("restriction/internet", vpn["navigation_tags"])

        eu_google = telegram_collector.classify_message_relevance(
            "Google should allow third-party search engines access to data, EU says. "
            "Reuters reports that Brussels wants Google to share search data with competitors.",
            has_media=True,
            source=source,
            store_mode="negative_only",
        )
        self.assertFalse(eu_google["keep"])
        self.assertIn("not_russia_related", eu_google["reasons"])

    def test_watch_folder_success_updates_item_level_source_sync_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "news.db"
            processed_dir = tmp_path / "processed-docs"
            processed_dir.mkdir(parents=True, exist_ok=True)
            create_db(db_path)

            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "processed_documents": str(processed_dir),
            }
            conn = get_db(settings)
            try:
                conn.execute(
                    """
                    INSERT INTO sources(id, name, category, url, is_active)
                    VALUES(1, 'Uploads', 'user_upload', 'watch://documents', 1)
                    """
                )
                conn.commit()

                src = tmp_path / "report.txt"
                src.write_text("test payload", encoding="utf-8")

                status = watch_folder.process_document_file(src, conn, settings)

                state_row = conn.execute(
                    """
                    SELECT source_key, source_id, state, last_external_id, last_hash, transport_mode, metadata_json
                    FROM source_sync_state
                    WHERE source_key='watch_folder:documents:1'
                    """
                ).fetchone()
                dead_letters = conn.execute("SELECT COUNT(*) FROM dead_letter_items").fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(status, "processed")
            self.assertIsNotNone(state_row)
            self.assertEqual(state_row["source_key"], "watch_folder:documents:1")
            self.assertEqual(state_row["source_id"], 1)
            self.assertEqual(state_row["state"], "ok")
            self.assertTrue(state_row["last_external_id"])
            self.assertTrue(state_row["last_hash"])
            self.assertEqual(state_row["transport_mode"], "watch_folder:documents")
            self.assertEqual(dead_letters, 0)
            metadata = json.loads(state_row["metadata_json"])
            self.assertEqual(metadata["last_filename"], "report.txt")
            self.assertEqual(metadata["status"], "processed")

    def test_watch_folder_copy_failure_records_dead_letter_and_warning_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "news.db"
            processed_dir = tmp_path / "processed-docs"
            processed_dir.mkdir(parents=True, exist_ok=True)
            create_db(db_path)

            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "processed_documents": str(processed_dir),
            }
            conn = get_db(settings)
            try:
                conn.execute(
                    """
                    INSERT INTO sources(id, name, category, url, is_active)
                    VALUES(1, 'Uploads', 'user_upload', 'watch://documents', 1)
                    """
                )
                conn.commit()

                src = tmp_path / "broken.txt"
                src.write_text("broken payload", encoding="utf-8")

                with patch("collectors.watch_folder.shutil.copy2", side_effect=OSError("disk full")):
                    status = watch_folder.process_document_file(src, conn, settings)

                state_row = conn.execute(
                    """
                    SELECT state, last_error, transport_mode
                    FROM source_sync_state
                    WHERE source_key='watch_folder:documents:1'
                    """
                ).fetchone()
                dead_letter = conn.execute(
                    """
                    SELECT failure_stage, error_type, error_message
                    FROM dead_letter_items
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(status, "failed")
            self.assertEqual(state_row["state"], "warning")
            self.assertIn("disk full", state_row["last_error"])
            self.assertEqual(state_row["transport_mode"], "watch_folder:documents")
            self.assertEqual(dead_letter["failure_stage"], "watch_folder_copy")
            self.assertEqual(dead_letter["error_type"], "OSError")
            self.assertIn("disk full", dead_letter["error_message"])

    def test_collect_channel_uses_source_sync_cursor_and_updates_state(self):
        class FakeApp:
            def __init__(self):
                self.offset_id = None

            async def get_chat(self, handle):
                return SimpleNamespace(id=77, title="Channel Title")

            def get_chat_history(self, peer_id, limit=100, offset_id=0):
                self.offset_id = offset_id

                async def _gen():
                    yield SimpleNamespace(
                        id=101,
                        text="hello world",
                        caption=None,
                        date=datetime(2026, 4, 25, 12, 0, 0),
                        views=10,
                        forwards=1,
                        post_author=None,
                        grouped_id=None,
                        reply_to_message=None,
                        media=None,
                        photo=None,
                        video=None,
                        document=None,
                    )

                return _gen()

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "news.db"
            telegram_dir = tmp_path / "telegram"
            create_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "processed_telegram": str(telegram_dir),
            }
            conn = get_db(settings)
            try:
                conn.execute(
                    """
                    INSERT INTO sources(id, name, category, url, access_method, is_active)
                    VALUES(1, 'Test channel', 'telegram', 'https://t.me/testchan', 'telegram', 1)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO source_sync_state(source_key, source_id, state, last_external_id, transport_mode)
                    VALUES('telegram:1', 1, 'ok', '99', 'telegram')
                    """
                )
                conn.commit()

                fake_app = FakeApp()
                collected = asyncio.run(
                    telegram_collector.collect_channel(
                        fake_app,
                        "https://t.me/testchan",
                        1,
                        conn,
                        settings,
                        limit=10,
                    )
                )
                state_row = conn.execute(
                    """
                    SELECT state, last_external_id, last_cursor, transport_mode, metadata_json
                    FROM source_sync_state
                    WHERE source_key='telegram:1'
                    """
                ).fetchone()
                raw_count = conn.execute("SELECT COUNT(*) FROM raw_source_items WHERE source_id=1").fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(fake_app.offset_id, 99)
            self.assertEqual(collected, 1)
            self.assertEqual(raw_count, 1)
            self.assertEqual(state_row["state"], "ok")
            self.assertEqual(state_row["last_external_id"], "101")
            self.assertEqual(state_row["last_cursor"], "101")
            self.assertEqual(state_row["transport_mode"], "telegram")
            metadata = json.loads(state_row["metadata_json"])
            self.assertEqual(metadata["channel_handle"], "@testchan")
            self.assertEqual(metadata["collected"], 1)

    def test_collect_channel_filters_noise_and_queues_document_review(self):
        class FakeApp:
            async def get_chat(self, handle):
                return SimpleNamespace(id=77, title="YEP")

            def get_chat_history(self, peer_id, limit=100, offset_id=0):
                async def _gen():
                    yield SimpleNamespace(
                        id=102,
                        text=(
                            "Google should allow third-party search engines access to data, EU says. "
                            "Reuters reports on Brussels and Google."
                        ),
                        caption=None,
                        date=datetime(2026, 4, 30, 12, 5, 0),
                        views=10,
                        forwards=1,
                        post_author=None,
                        grouped_id=None,
                        reply_to_message=None,
                        media=True,
                        photo=True,
                        video=None,
                        document=None,
                    )
                    yield SimpleNamespace(
                        id=101,
                        text=(
                            "Штрафы до 15 млн рублей начал выписывать РКН владельцам сайтов зоны .RU. "
                            "На скриншоте требование Роскомнадзора по политике конфиденциальности "
                            "и Google Analytics."
                        ),
                        caption=None,
                        date=datetime(2026, 4, 30, 12, 0, 0),
                        views=20,
                        forwards=2,
                        post_author=None,
                        grouped_id=None,
                        reply_to_message=None,
                        media=True,
                        photo=True,
                        video=None,
                        document=None,
                    )

                return _gen()

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "news.db"
            telegram_dir = tmp_path / "telegram"
            create_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "processed_telegram": str(telegram_dir),
                "telegram_store_mode": "negative_only",
            }
            conn = get_db(settings)
            try:
                conn.execute(
                    """
                    INSERT INTO sources(id, name, category, subcategory, url, access_method, is_active, credibility_tier)
                    VALUES(1, 'YEP', 'telegram', 'media', 'https://t.me/yep_news', 'telegram', 1, 'B')
                    """
                )
                conn.commit()

                collected = asyncio.run(
                    telegram_collector.collect_channel(
                        FakeApp(),
                        "https://t.me/yep_news",
                        1,
                        conn,
                        settings,
                        limit=10,
                    )
                )
                content_rows = conn.execute(
                    "SELECT id, external_id, title FROM content_items WHERE source_id=1 ORDER BY external_id"
                ).fetchall()
                tag_votes = conn.execute(
                    "SELECT tag_name, vote_value, signal_layer FROM content_tag_votes ORDER BY tag_name"
                ).fetchall()
                review_rows = conn.execute(
                    "SELECT queue_key, subject_type, suggested_action, status FROM review_tasks ORDER BY id"
                ).fetchall()
                attachments = conn.execute("SELECT attachment_type FROM attachments").fetchall()
            finally:
                conn.close()

        self.assertEqual(collected, 1)
        self.assertEqual([row["external_id"] for row in content_rows], ["101"])
        self.assertEqual([row["attachment_type"] for row in attachments], ["photo"])
        self.assertTrue(any(row["tag_name"] == "document/screenshot" and row["vote_value"] == "support" for row in tag_votes))
        self.assertIn(("documents", "content_item", "verify_document_screenshot", "open"), [tuple(row) for row in review_rows])

    def test_telegram_run_collect_requires_existing_session_without_prompting(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "news.db"
            session_dir = tmp_path / "telegram-session"
            create_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "telegram_api_id": 123,
                "telegram_api_hash": "hash",
                "telegram_session_dir": str(session_dir),
                "telegram_require_existing_session": True,
            }
            conn = get_db(settings)
            try:
                conn.execute(
                    """
                    INSERT INTO sources(id, name, category, url, access_method, is_active)
                    VALUES(1, 'YEP', 'telegram', 'https://t.me/yep_news', 'telegram', 1)
                    """
                )
                conn.commit()
            finally:
                conn.close()

            with patch.object(telegram_collector, "HAVE_PYROGRAM", True), patch.object(telegram_collector, "Client") as client:
                result = asyncio.run(telegram_collector.run_collect(settings))

            self.assertFalse(result["ok"])
            self.assertIn("telegram_session_missing", result["fatal_errors"])
            client.assert_not_called()

            conn = get_db(settings)
            try:
                state = conn.execute(
                    "SELECT state, failure_class, last_error, transport_mode FROM source_sync_state WHERE source_key='telegram'"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(state["state"], "warning")
            self.assertEqual(state["failure_class"], "auth")
            self.assertIn("telegram_session_missing", state["last_error"])
            self.assertEqual(state["transport_mode"], "telegram")

    def test_telegram_run_collect_rejects_unauthorized_session_without_prompting(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "news.db"
            session_dir = tmp_path / "telegram-session"
            session_dir.mkdir()
            session_db = session_dir / "news_collector.session"
            session_conn = sqlite3.connect(session_db)
            try:
                session_conn.execute(
                    """
                    CREATE TABLE sessions (
                        dc_id INTEGER PRIMARY KEY,
                        api_id INTEGER,
                        test_mode INTEGER,
                        auth_key BLOB,
                        date INTEGER NOT NULL,
                        user_id INTEGER,
                        is_bot INTEGER
                    )
                    """
                )
                session_conn.execute(
                    "INSERT INTO sessions(dc_id, api_id, test_mode, auth_key, date, user_id, is_bot) VALUES(2, 123, 0, ?, 0, NULL, NULL)",
                    (b"auth-key",),
                )
                session_conn.commit()
            finally:
                session_conn.close()

            create_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "telegram_api_id": 123,
                "telegram_api_hash": "hash",
                "telegram_session_dir": str(session_dir),
                "telegram_require_existing_session": True,
            }
            conn = get_db(settings)
            try:
                conn.execute(
                    """
                    INSERT INTO sources(id, name, category, url, access_method, is_active)
                    VALUES(1, 'YEP', 'telegram', 'https://t.me/yep_news', 'telegram', 1)
                    """
                )
                conn.commit()
            finally:
                conn.close()

            with patch.object(telegram_collector, "HAVE_PYROGRAM", True), patch.object(telegram_collector, "Client") as client:
                result = asyncio.run(telegram_collector.run_collect(settings))

            self.assertFalse(result["ok"])
            self.assertIn("telegram_session_unauthorized", result["fatal_errors"])
            client.assert_not_called()

    def test_telegram_run_collect_skips_legacy_pyrogram_when_telethon_pool_is_active(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "news.db"
            session_dir = tmp_path / "telegram-session"
            session_dir.mkdir()
            session_db = session_dir / "news_collector.session"
            session_conn = sqlite3.connect(session_db)
            try:
                session_conn.execute(
                    """
                    CREATE TABLE sessions (
                        dc_id INTEGER PRIMARY KEY,
                        api_id INTEGER,
                        test_mode INTEGER,
                        auth_key BLOB,
                        date INTEGER NOT NULL,
                        user_id INTEGER,
                        is_bot INTEGER
                    )
                    """
                )
                session_conn.execute(
                    "INSERT INTO sessions(dc_id, api_id, test_mode, auth_key, date, user_id, is_bot) VALUES(2, 123, 0, ?, 0, NULL, NULL)",
                    (b"auth-key",),
                )
                session_conn.commit()
            finally:
                session_conn.close()

            create_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "telegram_api_id": 123,
                "telegram_api_hash": "hash",
                "telegram_session_dir": str(session_dir),
                "telegram_require_existing_session": True,
            }
            conn = get_db(settings)
            try:
                conn.execute(
                    """
                    INSERT INTO sources(id, name, category, url, access_method, is_active)
                    VALUES(1, 'YEP', 'telegram', 'https://t.me/yep_news', 'telegram', 1)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO telegram_sessions(session_key, client_type, session_path, status, assigned_count)
                    VALUES('232354072', 'telethon', 'config/telegram_test_sessions/232354072_telethon.session', 'active', 4)
                    """
                )
                conn.commit()
            finally:
                conn.close()

            with patch.object(telegram_collector, "HAVE_PYROGRAM", True), patch.object(telegram_collector, "Client") as client:
                result = asyncio.run(telegram_collector.run_collect(settings))

            self.assertTrue(result["ok"])
            self.assertIn("telethon_pool_active", " ".join(result.get("warnings") or []))
            client.assert_not_called()

            conn = get_db(settings)
            try:
                state = conn.execute(
                    "SELECT state, failure_class, transport_mode, last_error FROM source_sync_state WHERE source_key='telegram'"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(state["state"], "warning")
            self.assertIsNone(state["failure_class"])
            self.assertEqual(state["transport_mode"], "pyrogram")
            self.assertIn("telethon_pool_active", state["last_error"])

    def test_ocr_missing_file_records_dead_letter_and_source_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "news.db"
            create_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
            }
            conn = get_db(settings)
            try:
                conn.execute(
                    """
                    INSERT INTO sources(id, name, category, url, is_active)
                    VALUES(1, 'Uploads', 'user_upload', 'watch://documents', 1)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO raw_source_items(id, source_id, external_id, raw_payload, hash_sha256, is_processed)
                    VALUES(1, 1, 'abc123', '{}', 'abc123', 1)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO content_items(id, source_id, raw_item_id, external_id, content_type, title, body_text, status)
                    VALUES(1, 1, 1, 'abc123', 'document', 'Broken scan', '', 'raw_signal')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO attachments(id, content_item_id, file_path, attachment_type, hash_sha256, file_size, mime_type)
                    VALUES(1, 1, ?, 'photo', 'abc123', 0, 'image/jpeg')
                    """,
                    (str(tmp_path / "missing.jpg"),),
                )
                conn.commit()
            finally:
                conn.close()

            result = ocr.process_unprocessed_ocr(settings)

            conn = get_db(settings)
            try:
                dead_letter = conn.execute(
                    """
                    SELECT failure_stage, error_type, attachment_id, content_item_id
                    FROM dead_letter_items
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ).fetchone()
                state_row = conn.execute(
                    """
                    SELECT state, last_external_id, last_cursor, transport_mode
                    FROM source_sync_state
                    WHERE source_key='ocr:1'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(result["dead_letters"], 1)
            self.assertEqual(dead_letter["failure_stage"], "ocr_missing_attachment")
            self.assertEqual(dead_letter["error_type"], "FileNotFoundError")
            self.assertEqual(dead_letter["attachment_id"], 1)
            self.assertEqual(dead_letter["content_item_id"], 1)
            self.assertEqual(state_row["state"], "warning")
            self.assertEqual(state_row["last_external_id"], "1")
            self.assertEqual(state_row["last_cursor"], "1")
            self.assertEqual(state_row["transport_mode"], "ocr")

    def test_ocr_runtime_failure_creates_single_dead_letter_and_skips_retries(self):
        class BrokenEngine:
            def ocr(self, image_path, **kwargs):
                raise RuntimeError("engine exploded")

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "news.db"
            image_path = tmp_path / "scan.jpg"
            image_path.write_bytes(b"jpeg-bytes")
            create_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
            }
            conn = get_db(settings)
            try:
                conn.execute(
                    """
                    INSERT INTO sources(id, name, category, url, is_active)
                    VALUES(1, 'Uploads', 'user_upload', 'watch://documents', 1)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO raw_source_items(id, source_id, external_id, raw_payload, hash_sha256, is_processed)
                    VALUES(1, 1, 'abc123', '{}', 'abc123', 1)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO content_items(id, source_id, raw_item_id, external_id, content_type, title, body_text, status)
                    VALUES(1, 1, 1, 'abc123', 'document', 'Broken scan', '', 'raw_signal')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO attachments(id, content_item_id, file_path, attachment_type, hash_sha256, file_size, mime_type)
                    VALUES(1, 1, ?, 'photo', 'abc123', 10, 'image/jpeg')
                    """,
                    (str(image_path),),
                )
                conn.commit()
            finally:
                conn.close()

            with patch("media_pipeline.ocr.get_ocr_engine", return_value=BrokenEngine()):
                first = ocr.process_unprocessed_ocr(settings)
                second = ocr.process_unprocessed_ocr(settings)

            conn = get_db(settings)
            try:
                dead_letters = conn.execute(
                    """
                    SELECT failure_stage, error_type, attachment_id
                    FROM dead_letter_items
                    ORDER BY id
                    """
                ).fetchall()
                state_row = conn.execute(
                    """
                    SELECT state, last_external_id, transport_mode
                    FROM source_sync_state
                    WHERE source_key='ocr:1'
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(first["dead_letters"], 1)
            self.assertEqual(first["items_updated"], 0)
            self.assertEqual(second["items_seen"], 0)
            self.assertEqual(len(dead_letters), 1)
            self.assertEqual(dead_letters[0]["failure_stage"], "ocr_runtime")
            self.assertEqual(dead_letters[0]["error_type"], "RuntimeError")
            self.assertEqual(dead_letters[0]["attachment_id"], 1)
            self.assertEqual(state_row["state"], "warning")
            self.assertEqual(state_row["last_external_id"], "1")
            self.assertEqual(state_row["transport_mode"], "ocr")

    def test_ocr_prioritizes_document_review_attachments(self):
        class FakeEngine:
            def ocr(self, image_path, **kwargs):
                return [[[None, (f"ocr:{Path(image_path).name}", 0.99)]]]

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "news.db"
            create_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
            }
            conn = get_db(settings)
            try:
                conn.execute("INSERT INTO sources(id, name, category, url, is_active) VALUES(1, 'Telegram', 'telegram', 'https://t.me/a', 1)")
                for i in range(1, 101):
                    image_path = tmp_path / f"normal-{i}.jpg"
                    image_path.write_bytes(b"image")
                    conn.execute(
                        """
                        INSERT INTO content_items(id, source_id, external_id, content_type, title, body_text, status)
                        VALUES(?,?,?,?,?,?,?)
                        """,
                        (i, 1, str(i), "post", f"normal {i}", "", "raw_signal"),
                    )
                    conn.execute(
                        """
                        INSERT INTO attachments(content_item_id, file_path, attachment_type, hash_sha256, file_size, mime_type)
                        VALUES(?,?,?,?,?,?)
                        """,
                        (i, str(image_path), "photo", f"h{i}", 5, "image/jpeg"),
                    )

                document_path = tmp_path / "document.jpg"
                document_path.write_bytes(b"image")
                conn.execute(
                    """
                    INSERT INTO content_items(id, source_id, external_id, content_type, title, body_text, status)
                    VALUES(?,?,?,?,?,?,?)
                    """,
                    (200, 1, "200", "post", "document", "", "raw_signal"),
                )
                conn.execute(
                    """
                    INSERT INTO attachments(content_item_id, file_path, attachment_type, hash_sha256, file_size, mime_type)
                    VALUES(?,?,?,?,?,?)
                    """,
                    (200, str(document_path), "photo", "doc-hash", 5, "image/jpeg"),
                )
                conn.execute(
                    """
                    INSERT INTO review_tasks(task_key, queue_key, subject_type, subject_id, suggested_action, status)
                    VALUES('telegram-document:1:200', 'documents', 'content_item', 200, 'verify_document_screenshot', 'open')
                    """
                )
                conn.commit()
            finally:
                conn.close()

            with patch("media_pipeline.ocr.get_ocr_engine", return_value=FakeEngine()):
                previous_backend = getattr(ocr, "_ocr_backend", None)
                ocr._ocr_backend = "paddleocr"
                try:
                    result = ocr.process_unprocessed_ocr(settings)
                finally:
                    ocr._ocr_backend = previous_backend

            conn = get_db(settings)
            try:
                doc_ocr = conn.execute(
                    "SELECT ocr_text FROM attachments WHERE content_item_id=200"
                ).fetchone()["ocr_text"]
                remaining_normals = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM attachments
                    WHERE content_item_id BETWEEN 1 AND 100
                      AND (ocr_text IS NULL OR ocr_text='')
                    """
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(result["items_seen"], 100)
            self.assertEqual(doc_ocr, "ocr:document.jpg")
            self.assertGreaterEqual(remaining_normals, 1)

    def test_ocr_updates_document_review_payload_for_authenticity_followup(self):
        class FakeEngine:
            def ocr(self, image_path, **kwargs):
                return [[[None, ("РОСКОМНАДЗОР Требование № 207 от 30.03.2026 по 152-ФЗ", 0.99)]]]

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "news.db"
            image_path = tmp_path / "document.jpg"
            image_path.write_bytes(b"image")
            create_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
            }
            conn = get_db(settings)
            try:
                conn.execute("INSERT INTO sources(id, name, category, url, is_active) VALUES(1, 'YEP', 'telegram', 'https://t.me/yep_news', 1)")
                conn.execute(
                    """
                    INSERT INTO content_items(id, source_id, external_id, content_type, title, body_text, status)
                    VALUES(300, 1, '300', 'post', 'Скриншот требования РКН', '', 'raw_signal')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO attachments(id, content_item_id, file_path, attachment_type, hash_sha256, file_size, mime_type)
                    VALUES(300, 300, ?, 'photo', 'doc-hash', 5, 'image/jpeg')
                    """,
                    (str(image_path),),
                )
                conn.execute(
                    """
                    INSERT INTO review_tasks(
                        task_key, queue_key, subject_type, subject_id, candidate_payload,
                        suggested_action, confidence, machine_reason, status
                    ) VALUES(
                        'telegram-document:1:300', 'documents', 'content_item', 300,
                        '{"document_reasons":["требование"]}', 'verify_document_screenshot',
                        0.9, 'needs OCR', 'open'
                    )
                    """
                )
                conn.commit()
            finally:
                conn.close()

            with patch("media_pipeline.ocr.get_ocr_engine", return_value=FakeEngine()):
                previous_backend = getattr(ocr, "_ocr_backend", None)
                ocr._ocr_backend = "paddleocr"
                try:
                    result = ocr.process_unprocessed_ocr(settings)
                finally:
                    ocr._ocr_backend = previous_backend

            conn = get_db(settings)
            try:
                review_row = conn.execute(
                    """
                    SELECT candidate_payload, suggested_action, machine_reason
                    FROM review_tasks
                    WHERE task_key='telegram-document:1:300'
                    """
                ).fetchone()
                attachment_row = conn.execute(
                    "SELECT ocr_text FROM attachments WHERE id=300"
                ).fetchone()
            finally:
                conn.close()

            payload = json.loads(review_row["candidate_payload"])
            self.assertEqual(result["items_updated"], 1)
            self.assertIn("РОСКОМНАДЗОР", attachment_row["ocr_text"])
            self.assertEqual(payload["ocr_status"], "ready")
            self.assertEqual(payload["verification_stage"], "official_source_search")
            self.assertEqual(payload["authenticity_verdict"], "needs_review")
            self.assertIn("document_identifiers", payload)
            self.assertIn("provenance_signals", payload)
            self.assertEqual(review_row["suggested_action"], "official_source_search")
            self.assertIn("official-source search", review_row["machine_reason"])

    def test_get_ocr_engine_retries_without_show_log(self):
        class FakePaddleOCR:
            calls = []

            def __init__(self, **kwargs):
                FakePaddleOCR.calls.append(kwargs)
                if "show_log" in kwargs:
                    raise TypeError("Unknown argument: show_log")

        fake_module = SimpleNamespace(PaddleOCR=FakePaddleOCR)
        previous_engine = ocr._ocr_engine
        previous_backend = getattr(ocr, "_ocr_backend", None)
        previous_failed = set(getattr(ocr, "_ocr_failed_backends", set()))
        ocr._ocr_engine = None
        ocr._ocr_backend = None
        ocr._ocr_failed_backends = set()
        try:
            with patch.dict(sys.modules, {"paddleocr": fake_module}):
                engine = ocr.get_ocr_engine({"ocr_allow_fallback": False})
        finally:
            ocr._ocr_engine = previous_engine
            ocr._ocr_backend = previous_backend
            ocr._ocr_failed_backends = previous_failed
            sys.modules.pop("paddleocr", None)

        self.assertIsNotNone(engine)
        self.assertEqual(len(FakePaddleOCR.calls), 2)
        self.assertIn("show_log", FakePaddleOCR.calls[0])
        self.assertNotIn("show_log", FakePaddleOCR.calls[1])

    def test_get_ocr_engine_caches_terminal_failure(self):
        class AlwaysFailOCR:
            calls = 0

            def __init__(self, **kwargs):
                AlwaysFailOCR.calls += 1
                raise RuntimeError("broken runtime")

        fake_module = SimpleNamespace(PaddleOCR=AlwaysFailOCR)
        previous_engine = ocr._ocr_engine
        previous_backend = getattr(ocr, "_ocr_backend", None)
        previous_failed = set(getattr(ocr, "_ocr_failed_backends", set()))
        ocr._ocr_engine = None
        ocr._ocr_backend = None
        ocr._ocr_failed_backends = set()
        try:
            with patch.dict(sys.modules, {"paddleocr": fake_module}):
                self.assertIsNone(ocr.get_ocr_engine({"ocr_allow_fallback": False}))
                self.assertIsNone(ocr.get_ocr_engine({"ocr_allow_fallback": False}))
        finally:
            ocr._ocr_engine = previous_engine
            ocr._ocr_backend = previous_backend
            ocr._ocr_failed_backends = previous_failed
            sys.modules.pop("paddleocr", None)

        self.assertEqual(AlwaysFailOCR.calls, 1)

    def test_ocr_image_retries_without_cls_argument(self):
        class FakeEngine:
            def __init__(self):
                self.calls = []

            def ocr(self, image_path, **kwargs):
                self.calls.append({"image_path": image_path, **kwargs})
                if "cls" in kwargs:
                    raise TypeError("predict() got an unexpected keyword argument 'cls'")
                return [[("box", ("decoded text", 0.99))]]

        fake_engine = FakeEngine()
        with patch("media_pipeline.ocr.get_ocr_engine", return_value=fake_engine):
            text = ocr.ocr_image("dummy-path.jpg", {})

        self.assertEqual(text, "decoded text")
        self.assertEqual(len(fake_engine.calls), 2)
        self.assertTrue(fake_engine.calls[0]["cls"])
        self.assertNotIn("cls", fake_engine.calls[1])

    def test_ocr_image_switches_to_rapidocr_after_paddle_runtime_failure(self):
        class BrokenPaddle:
            def ocr(self, image_path, **kwargs):
                raise NotImplementedError("ConvertPirAttribute2RuntimeAttribute")

        class FakeRapidOCR:
            def __call__(self, image_path, **kwargs):
                return ([[[], "fallback text", 0.99]], [0.1, 0.1, 0.1])

        previous_engine = ocr._ocr_engine
        previous_backend = getattr(ocr, "_ocr_backend", None)
        previous_failed = set(getattr(ocr, "_ocr_failed_backends", set()))
        ocr._ocr_engine = BrokenPaddle()
        ocr._ocr_backend = "paddleocr"
        ocr._ocr_failed_backends = set()
        try:
            with patch("media_pipeline.ocr._load_rapidocr_engine", return_value=FakeRapidOCR()):
                text = ocr.ocr_image("dummy-path.jpg", {})
        finally:
            ocr._ocr_engine = previous_engine
            ocr._ocr_backend = previous_backend
            ocr._ocr_failed_backends = previous_failed

        self.assertEqual(text, "fallback text")

    def test_effective_ocr_settings_prefers_rapidocr_after_successful_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "news.db"
            create_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "ocr_engine": "auto",
            }
            conn = get_db(settings)
            try:
                conn.execute(
                    """
                    INSERT INTO sources(id, name, category, url, is_active)
                    VALUES(1, 'Uploads', 'user_upload', 'watch://documents', 1)
                    """
                )
                conn.execute(
                    """
                    INSERT INTO source_sync_state(
                        source_key, source_id, state, last_success_at, last_attempt_at,
                        transport_mode, metadata_json
                    ) VALUES('ocr:1', 1, 'ok', '2026-04-25T12:00:00', '2026-04-25T12:00:00', 'ocr', ?)
                    """,
                    (json.dumps({"backend": "rapidocr"}),),
                )
                conn.commit()
                effective = ocr._effective_ocr_settings(conn, settings)
            finally:
                conn.close()

        self.assertEqual(effective["ocr_engine"], "rapidocr")


if __name__ == "__main__":
    unittest.main()
