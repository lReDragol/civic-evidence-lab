from __future__ import annotations

import hashlib
import sqlite3
import time
from pathlib import Path
from typing import Any


MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


def _ensure_ledger(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version       TEXT PRIMARY KEY,
            name          TEXT NOT NULL,
            checksum      TEXT NOT NULL,
            applied_at    TEXT NOT NULL DEFAULT (datetime('now')),
            duration_ms   INTEGER NOT NULL DEFAULT 0,
            metadata_json TEXT
        )
        """
    )
    conn.commit()


def _migration_files(migrations_dir: Path) -> list[Path]:
    if not migrations_dir.exists():
        return []
    return sorted(path for path in migrations_dir.glob("*.sql") if path.is_file())


def _version_and_name(path: Path) -> tuple[str, str]:
    stem = path.stem
    version, separator, name = stem.partition("_")
    if not separator or not version.isdigit():
        raise ValueError(f"Invalid migration filename: {path.name}")
    return version, name


def _statements(sql: str):
    # complete_statement understands trigger bodies and quoted semicolons.
    statement = ""
    for char in sql:
        statement += char
        if char == ";" and sqlite3.complete_statement(statement):
            yield statement
            statement = ""
    if statement.strip():
        yield statement


def apply_pending_migrations(
    conn: sqlite3.Connection,
    *,
    migrations_dir: Path | None = None,
) -> dict[str, Any]:
    """Apply immutable SQL migrations and verify already-applied checksums."""

    if conn.in_transaction:
        raise RuntimeError("Migrations require a connection without pending writes")
    target_dir = migrations_dir or MIGRATIONS_DIR
    _ensure_ledger(conn)
    applied: list[dict[str, Any]] = []
    skipped: list[str] = []

    for path in _migration_files(target_dir):
        version, name = _version_and_name(path)
        sql = path.read_text(encoding="utf-8")
        checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        started = time.perf_counter()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT checksum FROM schema_migrations WHERE version=?", (version,)).fetchone()
            if row is not None:
                if str(row[0]) != checksum:
                    raise RuntimeError(f"Migration checksum mismatch for {path.name}")
                conn.commit()
                skipped.append(version)
                continue
            conn.execute("PRAGMA defer_foreign_keys=ON")
            # Do not use executescript: it implicitly commits existing transactions.
            def guard(action, arg1, arg2, database, trigger):
                if action in (sqlite3.SQLITE_TRANSACTION, sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH):
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK
            conn.set_authorizer(guard)
            try:
                for statement in _statements(sql):
                    conn.execute(statement)
            finally:
                conn.set_authorizer(None)
            duration_ms = int((time.perf_counter() - started) * 1000)
            conn.execute(
                """
                INSERT INTO schema_migrations(version, name, checksum, duration_ms)
                VALUES(?,?,?,?)
                """,
                (version, name, checksum, duration_ms),
            )
            conn.commit()
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        applied.append(
            {
                "version": version,
                "name": name,
                "checksum": checksum,
                "duration_ms": duration_ms,
            }
        )

    return {
        "ok": True,
        "migrations_dir": str(target_dir),
        "applied": applied,
        "skipped": skipped,
    }
