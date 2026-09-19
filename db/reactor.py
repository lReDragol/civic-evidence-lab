from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from db.migration_runner import apply_pending_migrations


PROJECT_ROOT = Path(__file__).resolve().parent.parent
REACTOR_MIGRATIONS = Path(__file__).resolve().parent / "reactor_migrations"


@dataclass(frozen=True)
class ReactorPaths:
    knowledge: Path
    ops: Path
    search: Path


def reactor_paths(settings: dict[str, Any] | None = None) -> ReactorPaths:
    settings = settings or {}
    root = Path(settings.get("reactor_db_dir") or PROJECT_ROOT / "db")
    paths = ReactorPaths(
        knowledge=Path(settings.get("reactor_knowledge_db") or root / "reactor_v2.db"),
        ops=Path(settings.get("reactor_ops_db") or root / "reactor_ops.db"),
        search=Path(settings.get("reactor_search_db") or root / "reactor_search.db"),
    )
    resolved = [path.resolve() for path in paths.__dict__.values()]
    forbidden = {(PROJECT_ROOT / "db/news_unified.db").resolve(), (PROJECT_ROOT / "news_unified.db").resolve()}
    if settings.get("db_path"):
        forbidden.add(Path(settings["db_path"]).resolve())
    if len(set(resolved)) != 3 or set(resolved) & forbidden:
        raise ValueError("Reactor knowledge/ops/search paths must be distinct and cannot target the legacy database")
    return paths


def _connect(path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        conn = sqlite3.connect(path.resolve().as_uri()+"?mode=ro", uri=True, timeout=30)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    if not readonly:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def open_reactor_db(
    kind: Literal["knowledge", "ops", "search"],
    *,
    settings: dict[str, Any] | None = None,
    readonly: bool = False,
) -> sqlite3.Connection:
    paths = reactor_paths(settings)
    return _connect(getattr(paths, kind), readonly=readonly)


def bootstrap_reactor_databases(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    paths = reactor_paths(settings)
    results: dict[str, Any] = {"ok": True, "paths": {}}
    for kind, path in (
        ("knowledge", paths.knowledge),
        ("ops", paths.ops),
        ("search", paths.search),
    ):
        conn = _connect(path)
        try:
            migration = apply_pending_migrations(
                conn,
                migrations_dir=REACTOR_MIGRATIONS / kind,
            )
            integrity = str(conn.execute("PRAGMA quick_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"{kind} reactor database failed quick_check: {integrity}")
            results["paths"][kind] = {
                "path": str(path),
                "migration": migration,
                "quick_check": integrity,
            }
        finally:
            conn.close()
    return results


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Bootstrap Reactor v2 SQLite databases")
    parser.add_argument("--check", action="store_true", help="Open existing databases read-only")
    args = parser.parse_args()
    if args.check:
        payload: dict[str, Any] = {"ok": True, "databases": {}}
        for kind, path in reactor_paths().__dict__.items():
            conn = _connect(path, readonly=True)
            try:
                payload["databases"][kind] = {
                    "path": str(path),
                    "quick_check": conn.execute("PRAGMA quick_check").fetchone()[0],
                    "migrations": conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0],
                }
            finally:
                conn.close()
    else:
        payload = bootstrap_reactor_databases()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
