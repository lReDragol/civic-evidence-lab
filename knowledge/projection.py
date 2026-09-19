from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def start_generation(
    conn: sqlite3.Connection,
    *,
    projection_type: str,
    generation_key: str,
    source_watermark: str | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO projection_generations(
            projection_type, generation_key, status, source_watermark, started_at
        ) VALUES(?,?,'building',?,?)
        """,
        (projection_type, generation_key, source_watermark, _now_iso()),
    )
    conn.commit()
    return int(cur.lastrowid)


def mark_generation_validated(
    conn: sqlite3.Connection,
    generation_id: int,
    *,
    metrics: dict[str, Any] | None = None,
) -> None:
    changed = conn.execute(
        """
        UPDATE projection_generations
        SET status='validated', validated_at=?, metrics_json=?
        WHERE id=? AND status='building'
        """,
        (_now_iso(), json.dumps(metrics or {}, ensure_ascii=False, sort_keys=True), int(generation_id)),
    ).rowcount
    if changed != 1:
        raise RuntimeError(f"Generation {generation_id} is not building")
    conn.commit()


def activate_generation(conn: sqlite3.Connection, generation_id: int) -> None:
    row = conn.execute(
        "SELECT projection_type, status FROM projection_generations WHERE id=?",
        (int(generation_id),),
    ).fetchone()
    if row is None:
        raise KeyError(f"Unknown projection generation {generation_id}")
    if str(row[1]) != "validated":
        raise RuntimeError(f"Generation {generation_id} must be validated before activation")
    projection_type = str(row[0])
    now = _now_iso()
    conn.execute("BEGIN IMMEDIATE")
    try:
        active = conn.execute("SELECT id,source_watermark FROM projection_generations WHERE projection_type=? AND status='active'", (projection_type,)).fetchone()
        candidate = conn.execute("SELECT source_watermark FROM projection_generations WHERE id=?", (generation_id,)).fetchone()
        if active and active[0] > generation_id:
            raise RuntimeError("Stale generation cannot replace a newer active generation")
        if active and active[1] and not candidate[0]:
            raise RuntimeError("A versioned projection cannot be replaced by an unversioned one")
        if active and str(active[1] or "").isdigit() and str(candidate[0] or "").isdigit() and int(candidate[0]) < int(active[1]):
            raise RuntimeError("Source watermark regression")
        conn.execute(
            """
            UPDATE projection_generations
            SET status='superseded', finished_at=COALESCE(finished_at, ?)
            WHERE projection_type=? AND status='active' AND id<>?
            """,
            (now, projection_type, int(generation_id)),
        )
        changed = conn.execute(
            """
            UPDATE projection_generations
            SET status='active', activated_at=?, finished_at=?
            WHERE id=? AND status='validated'
            """,
            (now, now, int(generation_id)),
        ).rowcount
        if changed != 1:
            raise RuntimeError(f"Generation {generation_id} changed during activation")
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def fail_generation(conn: sqlite3.Connection, generation_id: int, error_text: str) -> None:
    conn.execute(
        """
        UPDATE projection_generations
        SET status='failed', error_text=?, finished_at=?
        WHERE id=? AND status IN ('building','validated')
        """,
        (str(error_text), _now_iso(), int(generation_id)),
    )
    conn.commit()


def active_generation_id(conn: sqlite3.Connection, projection_type: str) -> int | None:
    row = conn.execute(
        """
        SELECT id FROM projection_generations
        WHERE projection_type=? AND status='active'
        """,
        (projection_type,),
    ).fetchone()
    return int(row[0]) if row is not None else None
