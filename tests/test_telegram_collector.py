"""Tests for collectors/telegram_collector.py.

Mocks Pyrogram client, DB layer, and session helpers to verify
happy-path and error-path behaviour without real Telegram credentials.
"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.conftest import create_db


class FakePyrogramClient:
    """Stand-in for pyrogram.Client that yields no messages."""

    def __init__(self, *args, **kwargs):
        self.name = kwargs.get("name") or args[0] if args else "fake"
        self.workdir = kwargs.get("workdir", ".")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def get_chat_history(self, chat_id, *, limit=100):
        return iter([])

    async def download_media(self, message, file_name=None):
        return None


class FakeUnauthorizedClient(FakePyrogramClient):
    """Simulates auth failure on context entry."""

    async def __aenter__(self):
        raise EOFError("Session missing")


class TestTelegramCollector(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / "test.db"
        create_db(self.db_path)
        conn = sqlite3.connect(str(self.db_path))
        try:
            conn.execute("INSERT INTO sources(name, url, category, is_active, access_method) VALUES(?,?,?,?,?)",
                         ("TestChannel", "https://t.me/testchannel", "telegram", 1, "pyrogram"))
            conn.commit()
        finally:
            conn.close()

        self.settings = {
            "db_path": str(self.db_path),
            "telegram_api_id": 123456,
            "telegram_api_hash": "deadbeef",
            "telegram_session_dir": str(Path(self.tmp.name)),
            "telegram_posts_per_channel": 10,
            "telegram_require_existing_session": True,
        }

    def _patch_session_and_db(self):
        def _fake_get_db(_s=None):
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            return conn
        return [
            patch("collectors.telegram_collector.get_db", _fake_get_db),
            patch("collectors.telegram_collector._existing_telegram_session_file", return_value=Path(self.tmp.name) / "fake.session"),
            patch("collectors.telegram_collector._is_authorized_telegram_session", return_value=True),
            patch("collectors.telegram_collector._telethon_pool_is_active", return_value=False),
            patch("collectors.telegram_collector._skip_legacy_runtime_failure", return_value={"ok": False, "fatal_errors": ["skipped"]}),
            patch("collectors.telegram_collector._record_telegram_runtime_failure", return_value={"ok": False, "fatal_errors": ["recorded"]}),
        ]

    def test_run_collect_skips_when_pyrogram_unavailable(self):
        from collectors.telegram_collector import run_collect
        import asyncio

        with patch("collectors.telegram_collector._load_pyrogram_client", return_value=None):
            result = asyncio.run(run_collect(self.settings))

        self.assertFalse(result.get("ok"))
        self.assertIn("pyrogram_not_available", result.get("fatal_errors", []))

    def test_run_collect_missing_credentials(self):
        from collectors.telegram_collector import run_collect
        import asyncio

        bad_settings = dict(self.settings)
        bad_settings["telegram_api_id"] = None
        bad_settings["telegram_api_hash"] = None

        with patch("collectors.telegram_collector._load_pyrogram_client", return_value=FakePyrogramClient):
            result = asyncio.run(run_collect(bad_settings))

        self.assertFalse(result.get("ok"))
        self.assertIn("telegram_api_credentials_missing", result.get("fatal_errors", []))

    def test_run_collect_happy_path_no_channels(self):
        from collectors.telegram_collector import run_collect
        import asyncio

        empty_db_path = Path(self.tmp.name) / "empty.db"
        create_db(empty_db_path)
        settings = dict(self.settings)
        settings["db_path"] = str(empty_db_path)

        def _fake_get_db(_s=None):
            conn = sqlite3.connect(str(empty_db_path))
            conn.row_factory = sqlite3.Row
            return conn
        patches = [
            patch("collectors.telegram_collector.get_db", _fake_get_db),
            patch("collectors.telegram_collector._existing_telegram_session_file", return_value=Path(self.tmp.name) / "fake.session"),
            patch("collectors.telegram_collector._is_authorized_telegram_session", return_value=True),
        ]
        stack = [cm.__enter__() for cm in patches]
        try:
            with patch("collectors.telegram_collector._load_pyrogram_client", return_value=FakePyrogramClient):
                result = asyncio.run(run_collect(settings))
        finally:
            for cm in reversed(patches):
                cm.__exit__(None, None, None)

        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("channels"), 0)

    def test_run_collect_with_channels_returns_ok(self):
        from collectors.telegram_collector import run_collect
        import asyncio

        patches = self._patch_session_and_db()
        # Also mock the actual channel collection so we don't need real MTProto
        patches.append(patch("collectors.telegram_collector.collect_channel", return_value=3))
        patches.append(patch("collectors.telegram_collector.download_media_batch", return_value={"downloaded": 0, "failed": 0}))
        stack = [cm.__enter__() for cm in patches]
        try:
            with patch("collectors.telegram_collector._load_pyrogram_client", return_value=FakePyrogramClient):
                result = asyncio.run(run_collect(self.settings))
        finally:
            for cm in reversed(patches):
                cm.__exit__(None, None, None)

        self.assertTrue(result.get("ok"))
        self.assertEqual(result.get("channels"), 1)
        self.assertEqual(result.get("items_new"), 3)

    def test_run_collect_handles_unauthorized_session(self):
        from collectors.telegram_collector import run_collect
        import asyncio

        def _fake_get_db(_s=None):
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            return conn
        patches = [
            patch("collectors.telegram_collector.get_db", _fake_get_db),
            patch("collectors.telegram_collector._existing_telegram_session_file", return_value=Path(self.tmp.name) / "fake.session"),
            patch("collectors.telegram_collector._is_authorized_telegram_session", return_value=False),
            patch("collectors.telegram_collector._telethon_pool_is_active", return_value=False),
        ]
        stack = [cm.__enter__() for cm in patches]
        try:
            with patch("collectors.telegram_collector._load_pyrogram_client", return_value=FakePyrogramClient):
                result = asyncio.run(run_collect(self.settings))
        finally:
            for cm in reversed(patches):
                cm.__exit__(None, None, None)

        self.assertFalse(result.get("ok"))
        self.assertIn("telegram_session_unauthorized", result.get("fatal_errors", []))

    def test_run_collect_eof_error_during_context(self):
        from collectors.telegram_collector import run_collect
        import asyncio

        def _fake_get_db(_s=None):
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            return conn
        patches = [
            patch("collectors.telegram_collector.get_db", _fake_get_db),
            patch("collectors.telegram_collector._existing_telegram_session_file", return_value=Path(self.tmp.name) / "fake.session"),
            patch("collectors.telegram_collector._is_authorized_telegram_session", return_value=True),
        ]
        stack = [cm.__enter__() for cm in patches]
        try:
            with patch("collectors.telegram_collector._load_pyrogram_client", return_value=FakeUnauthorizedClient):
                result = asyncio.run(run_collect(self.settings))
        finally:
            for cm in reversed(patches):
                cm.__exit__(None, None, None)

        self.assertFalse(result.get("ok"))
        self.assertIn("telegram_session_missing", result.get("fatal_errors", []))


if __name__ == "__main__":
    unittest.main()
