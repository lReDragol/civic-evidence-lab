from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.db_utils import PROJECT_ROOT


def _now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def _json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


def _as_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (PROJECT_ROOT / path).resolve()


def _session_key(path: Path, client_type: str) -> str:
    stem = path.name
    if stem.endswith(".session"):
        stem = stem[: -len(".session")]
    if client_type == "telethon" and stem.endswith("_telethon"):
        stem = stem[: -len("_telethon")]
    return stem or path.stem


def telethon_session_has_auth_key(path: str | Path) -> bool:
    target = _as_path(path)
    if not target.exists() or target.stat().st_size <= 0:
        return False
    try:
        conn = sqlite3.connect(str(target))
        try:
            row = conn.execute("SELECT auth_key FROM sessions WHERE auth_key IS NOT NULL LIMIT 1").fetchone()
        finally:
            conn.close()
    except Exception:
        return False
    return bool(row and row[0])


def pyrogram_session_is_authorized(path: str | Path) -> bool:
    target = _as_path(path)
    if not target.exists() or target.stat().st_size <= 0:
        return False
    try:
        conn = sqlite3.connect(str(target))
        try:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(sessions)").fetchall()}
            if {"user_id", "is_bot"}.intersection(columns):
                row = conn.execute(
                    "SELECT user_id, is_bot FROM sessions WHERE auth_key IS NOT NULL LIMIT 1"
                ).fetchone()
                return bool(row and (row[0] is not None or row[1] is not None))
            row = conn.execute("SELECT auth_key FROM sessions WHERE auth_key IS NOT NULL LIMIT 1").fetchone()
            return bool(row and row[0])
        finally:
            conn.close()
    except Exception:
        return False


def discover_session_specs(settings: dict[str, Any]) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    explicit_telethon = settings.get("telegram_telethon_session_paths") or []
    if isinstance(explicit_telethon, str):
        explicit_telethon = [explicit_telethon]
    for raw_path in explicit_telethon:
        path = _as_path(str(raw_path))
        specs.append(
            {
                "session_key": _session_key(path, "telethon"),
                "client_type": "telethon",
                "session_path": str(path),
            }
        )

    if not explicit_telethon:
        session_dir = _as_path(settings.get("telegram_session_dir", str(PROJECT_ROOT / "config")))
        test_session_dir = session_dir / "telegram_test_sessions"
        for path in sorted(test_session_dir.glob("*_telethon.session")):
            specs.append(
                {
                    "session_key": _session_key(path, "telethon"),
                    "client_type": "telethon",
                    "session_path": str(path.resolve()),
                }
            )

        pyrogram_path = session_dir / "news_collector.session"
        if pyrogram_path.exists():
            specs.append(
                {
                    "session_key": "news_collector",
                    "client_type": "pyrogram",
                    "session_path": str(pyrogram_path.resolve()),
                }
            )

    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for spec in specs:
        deduped[(spec["client_type"], spec["session_key"])] = spec
    return list(deduped.values())


def _session_status(spec: dict[str, Any]) -> tuple[str, str | None]:
    path = Path(spec["session_path"])
    if not path.exists():
        return "failed", "missing_session_file"
    if spec["client_type"] == "telethon":
        return ("active", None) if telethon_session_has_auth_key(path) else ("failed", "unauthorized_session")
    if spec["client_type"] == "pyrogram":
        return ("active", None) if pyrogram_session_is_authorized(path) else ("failed", "unauthorized_session")
    return "failed", "unsupported_client_type"


def import_telegram_sessions(conn: sqlite3.Connection, settings: dict[str, Any]) -> dict[str, Any]:
    specs = discover_session_specs(settings)
    imported: list[dict[str, Any]] = []
    now = _now()
    for spec in specs:
        status, failure_class = _session_status(spec)
        metadata = {
            "exists": Path(spec["session_path"]).exists(),
            "imported_by": "telegram_session_pool",
        }
        conn.execute(
            """
            INSERT INTO telegram_sessions(
                session_key, client_type, session_path, status, last_attempt_at,
                failure_class, metadata_json, updated_at
            ) VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(session_key) DO UPDATE SET
                client_type=excluded.client_type,
                session_path=excluded.session_path,
                status=excluded.status,
                last_attempt_at=excluded.last_attempt_at,
                failure_class=excluded.failure_class,
                metadata_json=excluded.metadata_json,
                updated_at=excluded.updated_at
            """,
            (
                spec["session_key"],
                spec["client_type"],
                spec["session_path"],
                status,
                now,
                failure_class,
                _json(metadata),
                now,
            ),
        )
        imported.append({**spec, "status": status, "failure_class": failure_class})
    conn.commit()
    return {
        "ok": True,
        "sessions": imported,
        "active_count": sum(1 for item in imported if item["status"] == "active"),
        "failed_count": sum(1 for item in imported if item["status"] == "failed"),
    }


def active_telegram_sessions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    now = _now()
    rows = conn.execute(
        """
        SELECT session_key, client_type, session_path, status, cooldown_until, failure_class
        FROM telegram_sessions
        WHERE status='active'
          AND (cooldown_until IS NULL OR cooldown_until <= ?)
        ORDER BY session_key
        """,
        (now,),
    ).fetchall()
    return [dict(row) for row in rows]


def _telegram_sources(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, name, url
        FROM sources
        WHERE category='telegram'
          AND is_active=1
          AND COALESCE(access_method, '') IN ('telegram_tdlib', 'pyrogram', 'telegram', 'telegram_public', '')
        ORDER BY id
        """
    ).fetchall()
    return [dict(row) for row in rows]


def _choose_session(source: dict[str, Any], sessions: list[dict[str, Any]], assignment_version: str) -> str:
    key = f"{assignment_version}:{source.get('id')}:{source.get('url') or ''}"
    index = int(hashlib.sha256(key.encode("utf-8")).hexdigest(), 16) % len(sessions)
    return str(sessions[index]["session_key"])


def assign_telegram_sources(
    conn: sqlite3.Connection,
    *,
    assignment_version: str | None = None,
) -> dict[str, Any]:
    assignment_version = assignment_version or datetime.now().strftime("%Y%m%d")
    sessions = active_telegram_sessions(conn)
    sources = _telegram_sources(conn)
    assignments: dict[int, str] = {}
    now = _now()

    conn.execute(
        "UPDATE telegram_source_assignments SET is_active=0, updated_at=? WHERE assignment_version=?",
        (now, assignment_version),
    )
    conn.execute("UPDATE telegram_sessions SET assigned_count=0, updated_at=?", (now,))

    if sessions:
        for source in sources:
            session_key = _choose_session(source, sessions, assignment_version)
            assignments[int(source["id"])] = session_key
            conn.execute(
                """
                INSERT INTO telegram_source_assignments(
                    source_id, session_key, assignment_version, is_active, updated_at
                ) VALUES(?,?,?,?,?)
                ON CONFLICT(source_id, assignment_version) DO UPDATE SET
                    session_key=excluded.session_key,
                    is_active=excluded.is_active,
                    updated_at=excluded.updated_at
                """,
                (int(source["id"]), session_key, assignment_version, 1, now),
            )
        for session in sessions:
            assigned_count = sum(1 for value in assignments.values() if value == session["session_key"])
            conn.execute(
                "UPDATE telegram_sessions SET assigned_count=?, updated_at=? WHERE session_key=?",
                (assigned_count, now, session["session_key"]),
            )

    conn.commit()
    return {
        "ok": bool(sessions) or not sources,
        "assignment_version": assignment_version,
        "active_sessions": len(sessions),
        "source_count": len(sources),
        "assigned_sources": len(assignments),
        "assignments": assignments,
    }


def mark_session_result(
    conn: sqlite3.Connection,
    session_key: str,
    *,
    success: bool,
    failure_class: str | None = None,
    cooldown_until: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    now = _now()
    if success:
        conn.execute(
            """
            UPDATE telegram_sessions
            SET status='active', last_success_at=?, last_attempt_at=?,
                failure_class=NULL, cooldown_until=NULL, metadata_json=?, updated_at=?
            WHERE session_key=?
            """,
            (now, now, _json(metadata), now, session_key),
        )
    else:
        status = "cooldown" if cooldown_until else "failed"
        conn.execute(
            """
            UPDATE telegram_sessions
            SET status=?, last_attempt_at=?, failure_class=?,
                cooldown_until=?, metadata_json=?, updated_at=?
            WHERE session_key=?
            """,
            (status, now, failure_class or "runtime_error", cooldown_until, _json(metadata), now, session_key),
        )
    conn.commit()

