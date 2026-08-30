"""Shared pytest/unittest fixtures and utilities for Civic Evidence Lab tests.

This module is imported automatically by pytest. For unittest-based
files (the majority of this codebase), import helpers directly:

    from tests.conftest import create_db, PROJECT_ROOT, SCHEMA_PATH
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_db(db_path: Path) -> None:
    """Create a fresh SQLite database with the full project schema."""
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Schema not found: {SCHEMA_PATH}")
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


def get_test_settings(tmp_dir: Path) -> dict:
    """Return minimal settings dict pointing at a temp DB."""
    db_path = tmp_dir / "test.db"
    return {
        "db_path": str(db_path),
        "analysis_db_path": str(tmp_dir / "analysis_test.db"),
        "log_level": "WARNING",
        "ensure_schema_on_connect": True,
    }
