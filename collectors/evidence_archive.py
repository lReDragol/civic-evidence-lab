"""Append-only SHA-256 originals, independent of database and session settings.

Settings: evidence_archive_root (default E:\\CivicEvidence),
evidence_archive_max_bytes (200 GB), evidence_archive_reserve_bytes (50 GB),
and telegram_media_timeout_seconds (300). No eviction or stale-file cleanup.
All cooperating writers share a nonblocking OS lock; a busy archive is retryable.
"""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import math
import os
from pathlib import Path
import shutil
import tempfile


class ArchiveError(RuntimeError):
    """Safe machine-readable failure; never includes downloader/session details."""


class ArchiveQuotaError(ArchiveError):
    pass


@dataclass(frozen=True)
class ArchivedFile:
    path: Path
    sha256: str
    size: int


def _hash_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


class _QuotaWriter:
    def __init__(self, stream, archive, used):
        self.stream = stream
        self.archive = archive
        self.used = used
        self.size = 0

    def write(self, data):
        self.archive._check(self.used + self.size, len(data))
        written = self.stream.write(data)
        if written != len(data):
            raise ArchiveError("short_write")
        self.size += written
        return written

    def tell(self):
        return self.size

    def flush(self):
        self.stream.flush()


class EvidenceArchive:
    def __init__(self, settings=None):
        settings = settings or {}
        self.root = Path(settings.get("evidence_archive_root", r"E:\CivicEvidence"))
        if not self.root.is_absolute():
            raise ValueError("evidence_archive_root_must_be_absolute")
        self.max_bytes = int(settings.get("evidence_archive_max_bytes", 200 * 1000**3))
        self.reserve_bytes = int(settings.get("evidence_archive_reserve_bytes", 50 * 1000**3))
        self.timeout = float(settings.get("telegram_media_timeout_seconds", 300))
        if self.max_bytes <= 0 or self.reserve_bytes < 0 or not math.isfinite(self.timeout) or self.timeout <= 0:
            raise ValueError("invalid_evidence_archive_limits")

    @contextmanager
    def _locked(self):
        lock_path = self.root / ".archive.lock"
        if self.root.is_symlink() or self.root.is_junction() or lock_path.is_symlink():
            raise ArchiveError("archive_link_not_allowed")
        self.root.mkdir(parents=True, exist_ok=True)
        # The lock file is permanent. Unlinking it would let another process lock
        # a different inode while this writer still owns the original lock.
        with lock_path.open("a+b") as lock:
            locked = False
            try:
                if os.name == "nt":
                    import msvcrt
                    lock.seek(0)
                    msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
            except OSError:
                raise ArchiveError("archive_busy") from None
            try:
                yield
            finally:
                if locked:
                    if os.name == "nt":
                        lock.seek(0)
                        msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _used_bytes(self):
        # Include abandoned partials and unrelated files, but never remove them.
        total = 0

        def fail_scan(error):
            raise ArchiveError("archive_usage_scan_failed") from None

        for parent, dirs, files in os.walk(self.root, followlinks=False, onerror=fail_scan):
            for name in dirs + files:
                path = Path(parent) / name
                if path.is_symlink() or path.is_junction():
                    raise ArchiveError("archive_link_not_allowed")
            for name in files:
                if Path(parent) == self.root and name == ".archive.lock":
                    continue
                total += (Path(parent) / name).stat().st_size
        return total

    def _check(self, used, additional):
        if used + additional > self.max_bytes:
            raise ArchiveQuotaError("archive_capacity_exceeded")
        if shutil.disk_usage(self.root).free - additional < self.reserve_bytes:
            raise ArchiveQuotaError("archive_disk_reserve")

    @contextmanager
    def _temporary(self, expected_size=0):
        with self._locked():
            used = self._used_bytes()
            self._check(used, expected_size)
            temp_dir = self.root / ".tmp"
            temp_dir.mkdir(exist_ok=True)
            fd, name = tempfile.mkstemp(prefix="telegram-", suffix=".part", dir=temp_dir)
            path = Path(name)
            try:
                with os.fdopen(fd, "wb", buffering=0) as stream:
                    yield path, _QuotaWriter(stream, self, used)
            finally:
                # This exact mkstemp path is the only file we may delete.
                path.unlink(missing_ok=True)

    def _publish(self, path, writer):
        writer.flush()
        os.fsync(writer.stream.fileno())
        size = path.stat().st_size
        if not size or size != writer.size:
            raise ArchiveError("empty_or_incomplete_download")
        digest = _hash_file(path)
        target = self.root / "sha256" / digest[:2] / digest[2:4] / digest
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            # Atomic no-replace publication, including on Windows/NTFS.
            os.link(path, target)
        except FileExistsError:
            if not target.is_file() or target.stat().st_size != size or _hash_file(target) != digest:
                raise ArchiveError("archive_hash_collision_or_corruption") from None
        return ArchivedFile(target, digest, size)

    async def download(self, client, message):
        file = getattr(message, "file", None)
        expected = int(getattr(file, "size", 0) or 0)
        with self._temporary(max(0, expected)) as (path, writer):
            # Give Telethon a bounded binary sink, never a proposed evidence path.
            result = await asyncio.wait_for(
                client.download_media(message, file=writer), timeout=self.timeout,
            )
            if result is None or (expected > 0 and writer.size != expected):
                raise ArchiveError("empty_or_incomplete_download")
            return self._publish(path, writer)

    def store_bytes(self, data: bytes):
        with self._temporary(len(data)) as (path, writer):
            writer.write(data)
            return self._publish(path, writer)
