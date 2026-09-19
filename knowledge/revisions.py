from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def _canonical_payload(raw_payload: Any) -> str:
    if isinstance(raw_payload, str):
        stripped = raw_payload.strip()
        if not stripped:
            return ""
        try:
            value = json.loads(stripped)
        except (json.JSONDecodeError, TypeError):
            return stripped
    else:
        value = raw_payload
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def payload_hash(raw_payload: Any) -> str:
    return hashlib.sha256(_canonical_payload(raw_payload).encode("utf-8")).hexdigest()


def text_hash(text: Any) -> str | None:
    normalized = " ".join(str(text or "").split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest() if normalized else None


def record_source_revision(conn: sqlite3.Connection, **fields) -> dict[str, Any]:
    """Atomically record content and its observation, including A -> B -> A."""
    conn.execute("SAVEPOINT source_revision_write")
    try:
        result = _record_source_revision(conn, **fields)
        previous = conn.execute("SELECT id,source_revision_id,sequence_no FROM source_observations WHERE source_object_id=? ORDER BY sequence_no DESC LIMIT 1", (result["source_object_id"],)).fetchone()
        if previous is None or previous[1] != result["revision_id"]:
            cur = conn.execute("""INSERT INTO source_observations(source_object_id,source_revision_id,sequence_no,
                observed_at,fetched_at,previous_observation_id) VALUES(?,?,?,?,?,?)""",
                (result["source_object_id"],result["revision_id"],previous[2]+1 if previous else 1,
                 fields.get("observed_at"),_now_iso(),previous[0] if previous else None))
            result["observation_id"] = cur.lastrowid
            result["observation_created"] = True
        else:
            result["observation_id"] = previous[0]
            result["observation_created"] = False
        conn.execute("RELEASE source_revision_write")
        return result
    except Exception:
        conn.execute("ROLLBACK TO source_revision_write")
        conn.execute("RELEASE source_revision_write")
        raise


def _record_source_revision(
    conn: sqlite3.Connection,
    *,
    source_id: int,
    external_id: str,
    raw_payload: Any,
    raw_item_id: int | None = None,
    content_item_id: int | None = None,
    content_text: str | None = None,
    observed_at: str | None = None,
    canonical_url: str | None = None,
    object_kind: str = "content",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Append an immutable source revision, or touch the identical current one."""

    external_id = str(external_id or "").strip()
    if not external_id:
        raise ValueError("external_id is required for source revision identity")
    now = _now_iso()
    canonical_payload = _canonical_payload(raw_payload)
    digest = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
    source_column = "source_system_id" if "source_system_id" in {r[1] for r in conn.execute("PRAGMA table_info(source_objects)")} else "source_id"

    conn.execute(
        f"""
        INSERT INTO source_objects(
            {source_column}, external_id, object_kind, canonical_url,
            first_seen_at, last_seen_at, metadata_json
        ) VALUES(?,?,?,?,?,?,?)
        ON CONFLICT({source_column}, external_id) DO UPDATE SET
            last_seen_at=excluded.last_seen_at,
            canonical_url=COALESCE(excluded.canonical_url, source_objects.canonical_url),
            metadata_json=COALESCE(excluded.metadata_json, source_objects.metadata_json)
        """,
        (
            int(source_id),
            external_id,
            object_kind,
            canonical_url,
            now,
            now,
            json.dumps(metadata, ensure_ascii=False, sort_keys=True) if metadata else None,
        ),
    )
    object_row = conn.execute(
        f"SELECT id FROM source_objects WHERE {source_column}=? AND external_id=?",
        (int(source_id), external_id),
    ).fetchone()
    source_object_id = int(object_row[0])
    current = conn.execute(
        """
        SELECT id, revision_no, payload_hash
        FROM source_revisions
        WHERE source_object_id=? AND is_current=1
        """,
        (source_object_id,),
    ).fetchone()
    if current is not None and str(current[2]) == digest:
        return {
            "source_object_id": source_object_id,
            "revision_id": int(current[0]),
            "revision_no": int(current[1]),
            "created": False,
            "payload_hash": digest,
        }

    previous_id = int(current[0]) if current is not None else None
    revision_no = conn.execute("SELECT COALESCE(MAX(revision_no),0)+1 FROM source_revisions WHERE source_object_id=?", (source_object_id,)).fetchone()[0]
    if previous_id is not None:
        conn.execute(
            "UPDATE source_revisions SET is_current=0 WHERE id=?",
            (previous_id,),
        )
    historical = conn.execute("SELECT id,revision_no FROM source_revisions WHERE source_object_id=? AND payload_hash=?", (source_object_id,digest)).fetchone()
    if historical:
        conn.execute("UPDATE source_revisions SET is_current=1 WHERE id=?", (historical[0],))
        return {"source_object_id": source_object_id, "revision_id": historical[0],
                "revision_no": historical[1], "created": False, "payload_hash": digest}
    legacy_columns = "raw_item_id, content_item_id," if source_column == "source_id" else ""
    legacy_values = (raw_item_id, content_item_id) if legacy_columns else ()
    placeholders = "?, ?," if legacy_columns else ""
    cur = conn.execute(
        f"""
        INSERT INTO source_revisions(
            source_object_id, {legacy_columns} revision_no,
            payload_hash, text_hash, payload_json, observed_at, fetched_at,
            supersedes_revision_id, is_current, metadata_json
        ) VALUES(?,{placeholders}?,?,?,?,?,?,?,1,?)
        """,
        (
            source_object_id,
            *legacy_values,
            revision_no,
            digest,
            text_hash(content_text),
            canonical_payload,
            observed_at,
            now,
            previous_id,
            json.dumps(metadata, ensure_ascii=False, sort_keys=True) if metadata else None,
        ),
    )
    return {
        "source_object_id": source_object_id,
        "revision_id": int(cur.lastrowid),
        "revision_no": revision_no,
        "created": True,
        "payload_hash": digest,
    }


def store_raw_item_with_revision(
    conn: sqlite3.Connection,
    *,
    source_id: int,
    external_id: str,
    raw_payload: Any,
    observed_at: str | None = None,
    content_item_id: int | None = None,
    content_text: str | None = None,
    canonical_url: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Upsert collector state while preserving every changed payload as a revision."""

    canonical_payload = _canonical_payload(raw_payload)
    digest = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
    existing = conn.execute(
        "SELECT id, COALESCE(hash_sha256, '') FROM raw_source_items WHERE source_id=? AND external_id=?",
        (int(source_id), str(external_id)),
    ).fetchone()
    created = existing is None
    updated = False
    if existing is None:
        cur = conn.execute(
            """
            INSERT INTO raw_source_items(
                source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed
            ) VALUES(?,?,?,?,?,0)
            """,
            (int(source_id), str(external_id), canonical_payload, _now_iso(), digest),
        )
        raw_item_id = int(cur.lastrowid)
    else:
        raw_item_id = int(existing[0])
        if str(existing[1]) != digest:
            conn.execute(
                """
                UPDATE raw_source_items
                SET raw_payload=?, hash_sha256=?, collected_at=?, is_processed=0
                WHERE id=?
                """,
                (canonical_payload, digest, _now_iso(), raw_item_id),
            )
            updated = True
    revision = record_source_revision(
        conn,
        source_id=source_id,
        external_id=external_id,
        raw_payload=canonical_payload,
        raw_item_id=raw_item_id,
        content_item_id=content_item_id,
        content_text=content_text,
        observed_at=observed_at,
        canonical_url=canonical_url,
        metadata=metadata,
    )
    return {
        "raw_item_id": raw_item_id,
        "created": created,
        "updated": updated,
        "revision": revision,
    }


def sync_source_revisions(conn: sqlite3.Connection, *, limit: int | None = None) -> dict[str, int]:
    params: list[Any] = []
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT ?"
        params.append(max(1, int(limit)))
    rows = conn.execute(
        f"""
        SELECT
            r.id AS raw_item_id,
            r.source_id,
            r.external_id,
            r.raw_payload,
            r.collected_at,
            c.id AS content_item_id,
            c.body_text,
            c.published_at,
            c.url
        FROM raw_source_items r
        LEFT JOIN content_items c ON c.raw_item_id=r.id
        WHERE r.external_id IS NOT NULL AND TRIM(r.external_id) != ''
        ORDER BY r.id
        {limit_sql}
        """,
        tuple(params),
    ).fetchall()
    created = 0
    reused = 0
    for row in rows:
        result = record_source_revision(
            conn,
            source_id=int(row[1]),
            external_id=str(row[2]),
            raw_payload=row[3] or "",
            raw_item_id=int(row[0]),
            content_item_id=int(row[5]) if row[5] is not None else None,
            content_text=row[6],
            observed_at=row[7] or row[4],
            canonical_url=row[8],
        )
        if result["created"]:
            created += 1
        else:
            reused += 1
    conn.commit()
    return {"items_seen": len(rows), "revisions_created": created, "revisions_reused": reused}
