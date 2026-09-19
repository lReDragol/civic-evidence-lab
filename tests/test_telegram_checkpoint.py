from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from collectors import telegram_telethon_collector as collector
from tests.conftest import create_db
from tests.test_telegram_evidence_archive import DownloadClient, message


class FakeClient(DownloadClient):
    def __init__(self, messages=(), *, resolve_error=None, fail_id=None, iteration_error=None, **kwargs):
        super().__init__(**kwargs)
        self.messages = list(messages)
        self.resolve_error = resolve_error
        self.fail_id = fail_id
        self.iteration_error = iteration_error
        self.pages = []
        self.edit_queries = []

    async def get_entity(self, handle):
        if self.resolve_error:
            raise self.resolve_error
        return handle

    async def iter_messages(self, entity, *, limit, min_id, reverse=False):
        self.pages.append((min_id, limit, reverse))
        selected = sorted((m for m in self.messages if m.id > min_id), key=lambda m: m.id, reverse=not reverse)
        for msg in selected[:limit]:
            yield msg
        if self.iteration_error:
            raise self.iteration_error

    async def get_messages(self, entity, *, ids):
        self.edit_queries.append(ids)
        return [next((m for m in self.messages if m.id == i), None) for i in ids]

    async def download_media(self, msg, *, file):
        if msg.id == self.fail_id:
            file.write(b"bad")
            raise OSError("SECRET session path auth_key api_hash")
        return await super().download_media(msg, file=file)


class CheckpointTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        path = Path(self.tmp.name) / "test.db"
        create_db(path)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.addCleanup(self.conn.close)
        self.conn.execute("INSERT INTO sources(name,url,category) VALUES('Test','https://t.me/test','telegram')")
        self.conn.execute("INSERT INTO telegram_sessions(session_key,session_path) VALUES('test-session','never-open.session')")
        self.conn.commit()
        self.source = self.conn.execute("SELECT * FROM sources").fetchone()
        self.settings = {
            "telegram_posts_per_channel": 2, "telegram_edit_scan_limit": 0, "telegram_store_mode": "all",
            "telegram_recent_capture_limit": 0,
            "evidence_archive_root": str(Path(self.tmp.name) / "archive"),
            "evidence_archive_max_bytes": 100000, "evidence_archive_reserve_bytes": 0,
        }

    async def collect(self, client):
        return await collector._collect_source(client, self.source, self.conn, self.settings, session_key="test-session")

    def state(self):
        return self.conn.execute("SELECT * FROM source_sync_state WHERE source_id=?", (self.source["id"],)).fetchone()

    def count(self, table):
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def raw_ids(self):
        return [int(row[0]) for row in self.conn.execute("SELECT external_id FROM raw_source_items ORDER BY CAST(external_id AS INTEGER)")]

    async def test_bounded_oldest_first_pages_do_not_skip_backlog(self):
        client = FakeClient([message(i, media=False) for i in range(1, 8)])
        for expected in (2, 4, 6, 7):
            await self.collect(client)
            self.assertEqual(int(self.state()["last_external_id"]), expected)
            self.assertEqual(self.raw_ids(), list(range(1, expected + 1)))
        self.assertEqual(client.pages, [(0, 2, True), (2, 2, True), (4, 2, True), (6, 2, True)])
        self.assertEqual(await self.collect(client), (0, 0))

    async def test_deleted_ids_are_not_mistaken_for_pagination_gaps(self):
        client = FakeClient([message(i, media=False) for i in (2, 8, 14)])
        await self.collect(client)
        self.assertEqual(self.state()["last_external_id"], "8")
        await self.collect(client)
        self.assertEqual(self.raw_ids(), [2, 8, 14])

    async def test_recent_capture_does_not_skip_contiguous_history(self):
        self.settings["telegram_recent_capture_limit"] = 2
        client = FakeClient([message(i, media=False) for i in range(1, 8)])
        await self.collect(client)
        self.assertEqual(self.raw_ids(), [1, 2, 6, 7])
        self.assertEqual(self.state()["last_external_id"], "2")
        self.assertEqual(client.pages, [(0, 2, False), (0, 2, True)])
        await self.collect(client)
        self.assertEqual(self.raw_ids(), [1, 2, 3, 4, 6, 7])
        self.assertEqual(self.state()["last_external_id"], "4")
        await self.collect(client)
        self.assertEqual(self.raw_ids(), list(range(1, 8)))

    async def test_recent_failure_never_claims_historical_coverage(self):
        self.settings["telegram_recent_capture_limit"] = 2
        client = FakeClient([message(1), message(2), message(10)], fail_id=2)
        with self.assertRaises(collector.TelegramSourceError):
            await self.collect(client)
        self.assertEqual(self.raw_ids(), [10])
        self.assertEqual(self.state()["last_external_id"], "0")

    async def test_filtered_posts_have_durable_reasons_and_checkpoint(self):
        client = FakeClient([message(i) for i in (1, 2, 3)])
        with patch.object(collector, "classify_message_relevance", return_value={"keep": False, "reasons": ["promo_or_ad"]}):
            self.assertEqual(await self.collect(client), (2, 0))
        self.assertEqual(self.state()["last_external_id"], "2")
        self.assertEqual(client.calls, [])
        self.assertEqual(self.count("content_items"), 0)
        for row in self.conn.execute("SELECT raw_payload,is_processed FROM raw_source_items"):
            decision = json.loads(row[0])["collection_decision"]
            self.assertEqual(decision, {"status": "skipped", "reasons": ["promo_or_ad"]})
            self.assertEqual(row[1], 1)
        self.assertEqual(json.loads(self.state()["metadata_json"])["skipped"], 2)

    async def test_missing_state_does_not_infer_cursor_from_max_raw_id(self):
        self.conn.execute("INSERT INTO raw_source_items(source_id,external_id,raw_payload) VALUES(1,'4','{}')")
        self.conn.commit()
        await self.collect(FakeClient([message(i, media=False) for i in range(1, 5)]))
        self.assertEqual(self.state()["last_external_id"], "2")
        self.assertEqual(self.raw_ids(), [1, 2, 4])

    async def test_duplicates_advance_checkpoint_without_new_rows_or_download(self):
        client = FakeClient([message(1), message(2)])
        await self.collect(client)
        self.conn.execute("UPDATE source_sync_state SET last_external_id='0',last_cursor='0'")
        self.conn.commit()
        self.assertEqual(await self.collect(client), (2, 0))
        self.assertEqual(self.state()["last_external_id"], "2")
        self.assertEqual(self.state()["duplicates_current_run"], 2)
        self.assertEqual(client.calls, [1, 2])
        self.assertEqual(self.count("raw_source_items"), 2)
        self.assertEqual(self.count("attachments"), 2)
        self.assertEqual(len(list(Path(self.settings["evidence_archive_root"]).glob("sha256/*/*/*"))), 1)

    async def test_download_is_required_before_attachment_and_checkpoint(self):
        client = FakeClient([message(i) for i in (1, 2, 3)], fail_id=2)
        with self.assertRaises(collector.TelegramSourceError) as failure:
            await self.collect(client)
        self.assertNotIn("SECRET", str(failure.exception))
        self.assertEqual(self.raw_ids(), [1])
        self.assertEqual(self.state()["last_external_id"], "1")
        self.assertEqual(self.state()["is_collecting"], 0)
        self.assertEqual(self.count("attachments"), 1)
        self.assertNotIn("SECRET", json.dumps(dict(self.state())))
        client.fail_id = None
        self.assertEqual(await self.collect(client), (2, 2))
        self.assertEqual(self.raw_ids(), [1, 2, 3])
        self.assertEqual(self.state()["last_external_id"], "3")

    async def test_sql_failure_rolls_back_raw_content_votes_and_checkpoint(self):
        self.conn.execute("CREATE TRIGGER reject_attachment BEFORE INSERT ON attachments BEGIN SELECT RAISE(ABORT,'attachment rejected'); END")
        with self.assertRaises(collector.TelegramSourceError):
            await self.collect(FakeClient([message(1)]))
        for table in ("raw_source_items", "content_items", "raw_blobs", "attachments", "content_tag_votes"):
            self.assertEqual(self.count(table), 0, table)
        self.assertEqual(self.state()["last_external_id"], "0")
        # Published originals survive SQL failure and are reused by retry.
        self.assertEqual(len(list(Path(self.settings["evidence_archive_root"]).glob("sha256/*/*/*"))), 1)
        self.conn.execute("DROP TRIGGER reject_attachment")
        self.assertEqual(await self.collect(FakeClient([message(1)])), (1, 1))

    async def test_checkpoint_failure_rolls_back_message_not_prior_progress(self):
        self.conn.execute("CREATE TRIGGER reject_cursor BEFORE UPDATE ON source_sync_state WHEN NEW.last_external_id='2' BEGIN SELECT RAISE(ABORT,'cursor rejected'); END")
        with self.assertRaises(collector.TelegramSourceError):
            await self.collect(FakeClient([message(1, media=False), message(2, media=False)]))
        self.assertEqual(self.raw_ids(), [1])
        self.assertEqual(self.state()["last_external_id"], "1")
        self.assertEqual(self.state()["items_current_run"], 1)

    async def test_quota_failure_keeps_checkpoint_before_failed_media(self):
        self.settings["evidence_archive_max_bytes"] = 2
        with self.assertRaises(collector.TelegramSourceError):
            await self.collect(FakeClient([message(1)]))
        self.assertEqual(self.state()["last_external_id"], "0")
        self.assertEqual(self.state()["failure_class"], "archive_failed")
        self.assertEqual(self.count("attachments"), 0)
        self.assertEqual(self.count("raw_source_items"), 0)

    async def test_iterator_failure_preserves_successful_prefix(self):
        with self.assertRaises(collector.TelegramSourceError):
            await self.collect(FakeClient([message(1, media=False)], iteration_error=OSError("SECRET")))
        self.assertEqual(self.raw_ids(), [1])
        self.assertEqual(self.state()["last_external_id"], "1")
        self.assertEqual(self.state()["is_collecting"], 0)

    async def test_flood_wait_is_not_resolve_failed_and_is_reraised(self):
        class FloodWait(Exception):
            seconds = 37
        error = FloodWait("SECRET")
        with patch.object(collector, "errors", SimpleNamespace(FloodWaitError=FloodWait)):
            with self.assertRaises(FloodWait):
                await self.collect(FakeClient(resolve_error=error))
        self.assertEqual(self.state()["failure_class"], "flood_wait")
        self.assertEqual(self.state()["is_collecting"], 0)
        self.assertNotIn("SECRET", json.dumps(dict(self.state())))

    async def test_resolve_failure_is_sanitized_and_checkpoint_unchanged(self):
        with self.assertRaises(collector.TelegramSourceError):
            await self.collect(FakeClient(resolve_error=ValueError("SECRET")))
        self.assertEqual(self.state()["failure_class"], "resolve_failed")
        self.assertEqual(self.state()["last_external_id"], "0")
        self.assertNotIn("SECRET", json.dumps(dict(self.state())))

    async def test_cancelled_download_leaves_no_partial_database_rows(self):
        with self.assertRaises(asyncio.CancelledError):
            await self.collect(FakeClient([message(1)], error=asyncio.CancelledError()))
        self.assertEqual(self.count("raw_source_items"), 0)
        self.assertEqual(self.state()["last_external_id"], "0")
        self.assertEqual(self.state()["failure_class"], "cancelled")

    async def test_edit_scan_archives_revision_without_overwriting_user_text(self):
        self.settings["telegram_edit_scan_limit"] = 1
        client = FakeClient([message(1, media=False)])
        await self.collect(client)
        original = self.conn.execute("SELECT raw_payload FROM raw_source_items").fetchone()[0]
        self.conn.execute("UPDATE content_items SET body_text='User correction',status='reviewed'")
        self.conn.commit()
        client.messages[0].message = "Edited at source"
        client.messages[0].edit_date = datetime(2026, 2, 1, tzinfo=timezone.utc)
        await self.collect(client)
        row = self.conn.execute("SELECT * FROM raw_blobs WHERE blob_type='telegram_revision'").fetchone()
        self.assertEqual(json.loads(Path(row["file_path"]).read_text(encoding="utf-8"))["text"], "Edited at source")
        self.assertEqual(self.conn.execute("SELECT raw_payload FROM raw_source_items").fetchone()[0], original)
        content = self.conn.execute("SELECT body_text,status FROM content_items").fetchone()
        self.assertEqual(tuple(content), ("User correction", "reviewed"))
        self.assertEqual(self.state()["last_external_id"], "1")
        self.assertEqual(json.loads(self.state()["metadata_json"])["items_updated"], 1)
        await self.collect(client)
        self.assertEqual(self.count("raw_blobs"), 1)
        self.assertEqual(client.edit_queries, [[1], [1]])

    async def test_views_are_not_edits_and_deleted_tail_does_not_regress_cursor(self):
        self.settings["telegram_edit_scan_limit"] = 2
        client = FakeClient([message(1, media=False), message(2, media=False)])
        await self.collect(client)
        client.messages.pop()
        client.messages[0].views += 100
        await self.collect(client)
        self.assertEqual(self.count("raw_blobs"), 0)
        self.assertEqual(self.state()["last_external_id"], "2")
        self.assertEqual(client.edit_queries, [[2, 1]])

    async def test_non_downloadable_media_has_explicit_status_not_placeholder(self):
        msg = message(1)
        msg.document = None
        await self.collect(FakeClient([msg]))
        payload = json.loads(self.conn.execute("SELECT raw_payload FROM raw_source_items").fetchone()[0])
        self.assertEqual(payload["media_archive"]["status"], "not_downloadable")
        self.assertEqual(self.count("attachments"), 0)

    async def test_edit_media_retains_both_originals_and_deduplicates_repeat(self):
        self.settings["telegram_edit_scan_limit"] = 1
        client = FakeClient([message(1)])
        await self.collect(client)
        original = self.conn.execute("SELECT file_path FROM attachments").fetchone()[0]
        client.data = b"replacement bytes"
        client.messages[0].document.id = 43
        client.messages[0].file.size = len(client.data)
        await self.collect(client)
        self.assertEqual(self.count("attachments"), 2)
        self.assertEqual(Path(original).read_bytes(), b"original bytes")
        self.assertEqual(self.count("raw_blobs"), 3)
        await self.collect(client)
        self.assertEqual(self.count("attachments"), 2)
        self.assertEqual(client.calls, [1, 1])

    async def test_edit_reversions_are_observed_in_order(self):
        self.settings["telegram_edit_scan_limit"] = 1
        client = FakeClient([message(1, text="A", media=False)])
        await self.collect(client)
        for value in ("B", "C", "B", "A"):
            client.messages[0].message = value
            await self.collect(client)
        rows = self.conn.execute("SELECT file_path FROM raw_blobs ORDER BY id").fetchall()
        self.assertEqual([json.loads(Path(row[0]).read_text(encoding="utf-8"))["text"] for row in rows], ["B", "C", "B", "A"])
        await self.collect(client)
        self.assertEqual(self.count("raw_blobs"), 4)

    async def test_missing_latest_media_is_not_masked_by_older_original(self):
        self.settings["telegram_edit_scan_limit"] = 1
        client = FakeClient([message(1)])
        await self.collect(client)
        client.data = b"replacement bytes"
        client.messages[0].document.id = 43
        client.messages[0].file.size = len(client.data)
        await self.collect(client)
        latest = Path(self.conn.execute("SELECT file_path FROM attachments ORDER BY id DESC LIMIT 1").fetchone()[0])
        moved = Path(self.tmp.name) / "moved-original"
        latest.rename(moved)
        await self.collect(client)
        self.assertEqual(client.calls, [1, 1, 1])
        self.assertEqual(latest.read_bytes(), client.data)
        self.assertEqual(moved.read_bytes(), client.data)
        self.assertEqual(self.count("attachments"), 2)

    async def run_pool(self, client):
        class PoolClient:
            async def connect(self):
                pass

            async def is_user_authorized(self):
                return True

            async def disconnect(self):
                pass

            def __getattr__(self, key):
                return getattr(client, key)

        class Connection:
            def close(self):
                pass

            def __getattr__(proxy, key):
                return getattr(self.conn, key)

        session = {"session_key": "test-session", "session_path": "SECRET.session", "client_type": "telethon"}
        with ExitStack() as stack:
            stack.enter_context(patch.object(collector, "get_db", return_value=Connection()))
            stack.enter_context(patch.object(collector, "_load_api_credentials", return_value=(12345, "SECRET")))
            factory = stack.enter_context(patch.object(collector, "TelegramClient", return_value=PoolClient()))
            stack.enter_context(patch.object(collector, "import_telegram_sessions", return_value={"ok": True, "sessions": [session], "active_count": 1, "failed_count": 0}))
            stack.enter_context(patch.object(collector, "assign_telegram_sources", return_value={"assignment_version": "test"}))
            stack.enter_context(patch.object(collector, "active_telegram_sessions", return_value=[session]))
            stack.enter_context(patch.object(collector, "_source_rows_for_session", return_value=[self.source]))
            result = await collector._collect_with_sessions(self.settings)
            self.assertEqual(factory.call_args.kwargs["flood_sleep_threshold"], 0)
        self.assertNotIn("SECRET", json.dumps(result))
        pool_state = self.conn.execute("SELECT metadata_json FROM source_sync_state WHERE source_key='telegram_telethon_pool'").fetchone()[0]
        self.assertNotIn("SECRET", pool_state)
        return result

    async def test_pool_flood_wait_sets_cooldown_and_does_not_report_success(self):
        class FloodWait(Exception):
            seconds = 37
        with patch.object(collector, "errors", SimpleNamespace(FloodWaitError=FloodWait)):
            result = await self.run_pool(FakeClient(resolve_error=FloodWait("SECRET")))
        self.assertFalse(result["ok"])
        row = self.conn.execute("SELECT status,failure_class,cooldown_until,metadata_json FROM telegram_sessions").fetchone()
        self.assertEqual(row[0:2], ("cooldown", "flood_wait"))
        remaining = (datetime.fromisoformat(row[2]) - datetime.now(timezone.utc).replace(tzinfo=None)).total_seconds()
        self.assertGreater(remaining, 25)
        self.assertLessEqual(remaining, 37)
        self.assertNotIn("SECRET", row[3])

    async def test_pool_failure_reports_committed_prefix_counts(self):
        result = await self.run_pool(FakeClient([message(1), message(2)], fail_id=2))
        self.assertFalse(result["ok"])
        self.assertEqual(result["items_new"], 1)
        self.assertEqual(result["items_seen"], 2)
        self.assertEqual(self.state()["last_external_id"], "1")

    async def test_pool_reports_observed_edit_count(self):
        self.settings["telegram_edit_scan_limit"] = 1
        client = FakeClient([message(1, media=False)])
        self.assertEqual((await self.run_pool(client))["items_new"], 1)
        client.messages[0].message = "Edited"
        result = await self.run_pool(client)
        self.assertTrue(result["ok"])
        self.assertEqual(result["items_updated"], 1)
        self.assertEqual(result["items_new"], 0)


if __name__ == "__main__":
    unittest.main()
