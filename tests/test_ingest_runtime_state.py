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

    def test_get_ocr_engine_retries_without_show_log(self):
        class FakePaddleOCR:
            calls = []

            def __init__(self, **kwargs):
                FakePaddleOCR.calls.append(kwargs)
                if "show_log" in kwargs:
                    raise TypeError("Unknown argument: show_log")

        fake_module = SimpleNamespace(PaddleOCR=FakePaddleOCR)
        previous_engine = ocr._ocr_engine
        previous_failed = getattr(ocr, "_ocr_engine_failed", False)
        ocr._ocr_engine = None
        ocr._ocr_engine_failed = False
        try:
            with patch.dict(sys.modules, {"paddleocr": fake_module}):
                engine = ocr.get_ocr_engine({})
        finally:
            ocr._ocr_engine = previous_engine
            ocr._ocr_engine_failed = previous_failed
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
        previous_failed = getattr(ocr, "_ocr_engine_failed", False)
        ocr._ocr_engine = None
        ocr._ocr_engine_failed = False
        try:
            with patch.dict(sys.modules, {"paddleocr": fake_module}):
                self.assertIsNone(ocr.get_ocr_engine({}))
                self.assertIsNone(ocr.get_ocr_engine({}))
        finally:
            ocr._ocr_engine = previous_engine
            ocr._ocr_engine_failed = previous_failed
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


if __name__ == "__main__":
    unittest.main()
