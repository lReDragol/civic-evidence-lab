from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from collectors.evidence_archive import EvidenceArchive, ArchiveError, ArchiveQuotaError


class DownloadClient:
    def __init__(self, data=b"original bytes", *, error=None, result=True):
        self.data = data
        self.error = error
        self.result = result
        self.calls = []

    async def download_media(self, message, *, file):
        self.calls.append(message.id)
        file.write(self.data[:3])
        if self.error:
            raise self.error
        file.write(self.data[3:])
        return file if self.result else None


def message(message_id=1, text="Original post", data=b"original bytes", *, media=True):
    document = SimpleNamespace(id=42) if media else None
    return SimpleNamespace(
        id=message_id, message=text, date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        edit_date=None, views=1, forwards=0, media=document, document=document,
        photo=None, file=SimpleNamespace(size=len(data), mime_type="application/octet-stream", name="original.bin"),
    )


class ArchiveTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "archive"
        self.settings = {
            "evidence_archive_root": str(self.root),
            "evidence_archive_max_bytes": 10000,
            "evidence_archive_reserve_bytes": 0,
        }
        self.archive = EvidenceArchive(self.settings)

    def assert_no_partials(self):
        self.assertEqual(list(self.root.glob(".tmp/telegram-*.part")), [])

    async def test_downloads_original_bytes_and_deduplicates(self):
        client = DownloadClient()
        first = await self.archive.download(client, message())
        second = await self.archive.download(client, message(2))
        self.assertEqual(client.calls, [1, 2])
        self.assertEqual(first, second)
        self.assertEqual(first.sha256, hashlib.sha256(client.data).hexdigest())
        self.assertEqual(first.path.name, first.sha256)
        self.assertEqual(first.path.read_bytes(), client.data)
        self.assertEqual(first.size, len(client.data))
        self.assertEqual(len(list(self.root.glob("sha256/*/*/*"))), 1)
        self.assert_no_partials()

    async def test_real_telethon_download_media_with_mocked_transport(self):
        try:
            from telethon import TelegramClient, types
            from telethon.sessions import StringSession
        except ImportError:
            self.skipTest("Telethon optional dependency not installed")
        data = b"actual telethon document download"
        doc = types.Document(
            id=123, access_hash=0, file_reference=b"", date=datetime.now(timezone.utc),
            mime_type="application/pdf", size=len(data), dc_id=2, attributes=[],
        )
        msg = types.Message(id=3, peer_id=types.PeerChannel(1), media=types.MessageMediaDocument(document=doc))
        client = TelegramClient(StringSession(), 12345, "0" * 32)

        async def chunks(*args, **kwargs):
            yield data[:8]
            yield data[8:]

        with patch.object(client, "_iter_download", side_effect=chunks) as transport:
            saved = await self.archive.download(client, msg)
        transport.assert_called_once()
        self.assertEqual(saved.path.read_bytes(), data)

    async def test_capacity_preflight_does_not_download(self):
        self.archive.max_bytes = 2
        client = DownloadClient()
        with self.assertRaises(ArchiveQuotaError):
            await self.archive.download(client, message())
        self.assertEqual(client.calls, [])
        self.assert_no_partials()

    async def test_reserve_preflight_does_not_download(self):
        self.archive.reserve_bytes = 100
        client = DownloadClient()
        with patch("collectors.evidence_archive.shutil.disk_usage", return_value=SimpleNamespace(free=105)):
            with self.assertRaisesRegex(ArchiveQuotaError, "disk_reserve"):
                await self.archive.download(client, message())
        self.assertEqual(client.calls, [])

    async def test_unknown_size_and_lying_size_cannot_overrun_capacity(self):
        for size in (0, 1):
            with self.subTest(size=size):
                self.archive.max_bytes = 5
                msg = message()
                msg.file.size = size
                with self.assertRaises(ArchiveQuotaError):
                    await self.archive.download(DownloadClient(), msg)
                self.assert_no_partials()

    async def test_disk_reserve_checked_before_each_write(self):
        msg = message()
        msg.file.size = 0
        self.archive.reserve_bytes = 50
        usage = [SimpleNamespace(free=n) for n in (100, 100, 51)]
        with patch("collectors.evidence_archive.shutil.disk_usage", side_effect=usage):
            with self.assertRaisesRegex(ArchiveQuotaError, "disk_reserve"):
                await self.archive.download(DownloadClient(), msg)
        self.assert_no_partials()

    async def test_partial_failure_preserves_unowned_files_and_evidence(self):
        original = self.archive.store_bytes(b"permanent evidence")
        foreign = self.root / ".tmp" / "other-writer.part"
        foreign.write_bytes(b"not ours")
        with self.assertRaises(OSError):
            await self.archive.download(DownloadClient(error=OSError("failure")), message())
        self.assertEqual(original.path.read_bytes(), b"permanent evidence")
        self.assertEqual(foreign.read_bytes(), b"not ours")
        self.assert_no_partials()

    async def test_cancellation_cleans_only_own_partial(self):
        with self.assertRaises(asyncio.CancelledError):
            await self.archive.download(DownloadClient(error=asyncio.CancelledError()), message())
        self.assert_no_partials()

    async def test_timeout_cleans_partial(self):
        class SlowClient:
            async def download_media(self, msg, *, file):
                file.write(b"part")
                await asyncio.sleep(60)
        self.archive.timeout = 0.01
        with self.assertRaises(TimeoutError):
            await self.archive.download(SlowClient(), message())
        self.assert_no_partials()

    async def test_path_only_none_and_truncated_downloads_are_not_evidence(self):
        class PathOnly:
            async def download_media(self, msg, *, file):
                return "C:/proposed/path.bin"
        for client in (PathOnly(), DownloadClient(result=False), DownloadClient(data=b"tiny")):
            with self.subTest(client=type(client).__name__):
                with self.assertRaises(ArchiveError):
                    await self.archive.download(client, message())
                self.assert_no_partials()
                self.assertEqual(list(self.root.glob("sha256/*/*/*")), [])

    async def test_corrupt_digest_path_is_never_overwritten(self):
        original = self.archive.store_bytes(b"original bytes")
        original.path.write_bytes(b"corrupted!")
        with self.assertRaisesRegex(ArchiveError, "corruption"):
            await self.archive.download(DownloadClient(), message())
        self.assertEqual(original.path.read_bytes(), b"corrupted!")
        self.assert_no_partials()

    async def test_busy_archive_does_not_download(self):
        client = DownloadClient()
        with self.archive._locked():
            with self.assertRaisesRegex(ArchiveError, "archive_busy"):
                await self.archive.download(client, message())
        self.assertEqual(client.calls, [])

    async def test_abandoned_partials_count_against_cap_without_deletion(self):
        self.root.mkdir()
        foreign = self.root / "old.part"
        foreign.write_bytes(b"x" * 9999)
        with self.assertRaises(ArchiveQuotaError):
            await self.archive.download(DownloadClient(), message())
        self.assertEqual(foreign.stat().st_size, 9999)

    async def test_publish_failure_removes_only_own_partial(self):
        permanent = self.archive.store_bytes(b"permanent")
        with patch("collectors.evidence_archive.os.link", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                await self.archive.download(DownloadClient(), message())
        self.assert_no_partials()
        self.assertEqual(permanent.path.read_bytes(), b"permanent")

    def test_default_capacity_and_reserve(self):
        archive = EvidenceArchive({"evidence_archive_root": str(self.root)})
        self.assertEqual(archive.max_bytes, 200 * 1000**3)
        self.assertEqual(archive.reserve_bytes, 50 * 1000**3)
        self.assertFalse(self.root.exists())

    def test_invalid_limits_fail_closed(self):
        for setting, value in (("evidence_archive_max_bytes", 0), ("evidence_archive_reserve_bytes", -1),
                               ("telegram_media_timeout_seconds", 0), ("evidence_archive_root", "relative")):
            with self.subTest(setting=setting), self.assertRaises(ValueError):
                EvidenceArchive(dict(self.settings, **{setting: value}))


if __name__ == "__main__":
    unittest.main()
