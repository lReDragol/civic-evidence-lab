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


def apply_pending_migrations(
    conn: sqlite3.Connection,
    *,
    migrations_dir: Path | None = None,
) -> dict[str, Any]:
    """Apply immutable SQL migrations and verify already-applied checksums."""

    target_dir = migrations_dir or MIGRATIONS_DIR
    _ensure_ledger(conn)
    applied: list[dict[str, Any]] = []
    skipped: list[str] = []

    for path in _migration_files(target_dir):
        version, name = _version_and_name(path)
        sql = path.read_text(encoding="utf-8")
        checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        row = conn.execute(
            "SELECT checksum FROM schema_migrations WHERE version=?",
            (version,),
        ).fetchone()
        if row is not None:
            existing_checksum = str(row[0])
            if existing_checksum != checksum:
                raise RuntimeError(
                    f"Migration checksum mismatch for {path.name}: "
                    f"database={existing_checksum} file={checksum}"
                )
            skipped.append(version)
            continue

        started = time.perf_counter()
        script = (
            "PRAGMA defer_foreign_keys=ON;\n"
            "BEGIN IMMEDIATE;\n"
            f"{sql}\n"
            "COMMIT;\n"
        )
        try:
            conn.executescript(script)
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
