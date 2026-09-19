from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from db.reactor import bootstrap_reactor_databases, open_reactor_db, reactor_paths


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def _canonical_json(value: Any) -> str:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                value = json.loads(stripped)
            except (TypeError, json.JSONDecodeError):
                value = {"raw_text": stripped}
        else:
            value = {}
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _legacy_connection(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _register_sources(legacy: sqlite3.Connection, target: sqlite3.Connection) -> dict[int, int]:
    mapping: dict[int, int] = {}
    rows = legacy.execute(
        """
        SELECT id, name, category, url, is_official, credibility_tier, owner, is_active
        FROM sources
        ORDER BY id
        """
    ).fetchall()
    for row in rows:
        source_key = f"legacy_source:{int(row['id'])}"
        target.execute(
            """
            INSERT INTO source_systems(source_key,source_type,title,canonical_url,policy_json)
            VALUES(?,?,?,?,?)
            ON CONFLICT(source_key) DO UPDATE SET
                source_type=excluded.source_type,
                title=excluded.title,
                canonical_url=COALESCE(excluded.canonical_url, source_systems.canonical_url),
                policy_json=excluded.policy_json
            """,
            (
                source_key,
                str(row["category"] or "other"),
                str(row["name"] or source_key),
                row["url"],
                json.dumps(
                    {
                        "legacy_source_id": int(row["id"]),
                        "official": bool(row["is_official"]),
                        "credibility_tier": row["credibility_tier"],
                        "owner": row["owner"],
                        "active": bool(row["is_active"]),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            ),
        )
        mapping[int(row["id"])] = int(
            target.execute("SELECT id FROM source_systems WHERE source_key=?", (source_key,)).fetchone()[0]
        )
    return mapping


def replay_legacy_revisions(
    legacy_db: str | Path,
    *,
    settings: dict[str, Any] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Replay immutable source revisions without importing legacy derived truth."""

    legacy_path = Path(legacy_db).resolve()
    if not legacy_path.exists():
        raise FileNotFoundError(legacy_path)
    if legacy_path in {p.resolve() for p in reactor_paths(settings).__dict__.values()}:
        raise ValueError("Legacy source and Reactor destinations must be different files")
    bootstrap_reactor_databases(settings)
    legacy = _legacy_connection(legacy_path)
    target = open_reactor_db("knowledge", settings=settings)
    stats = {
        "sources": 0,
        "objects_seen": 0,
        "objects_created": 0,
        "revisions_created": 0,
        "revisions_reused": 0,
        "blobs_linked": 0,
        "missing_external_id": 0,
    }
    try:
        objects_before = target.execute("SELECT COUNT(*) FROM source_objects").fetchone()[0]
        source_map = _register_sources(legacy, target)
        stats["sources"] = len(source_map)
        params: list[Any] = []
        limit_sql = ""
        if limit is not None:
            limit_sql = "LIMIT ?"
            params.append(max(1, int(limit)))
        rows = legacy.execute(
            f"""
            SELECT
                r.id AS raw_item_id,
                r.source_id,
                r.external_id,
                r.raw_payload,
                r.collected_at,
                c.id AS content_item_id,
                c.content_type,
                c.title,
                c.body_text,
                c.published_at,
                c.url,
                c.status
            FROM raw_source_items r
            LEFT JOIN content_items c ON c.raw_item_id=r.id
            ORDER BY r.id, c.id
            {limit_sql}
            """,
            tuple(params),
        ).fetchall()
        now = _now_iso()
        for row in rows:
            stats["objects_seen"] += 1
            external_id = str(row["external_id"] or "").strip()
            if not external_id:
                external_id = f"raw:{int(row['raw_item_id'])}"
                stats["missing_external_id"] += 1
            source_system_id = source_map[int(row["source_id"])]
            observed_at = row["published_at"] or row["collected_at"] or now
            payload = _canonical_json(
                row["raw_payload"]
                or {
                    "legacy_content_id": row["content_item_id"],
                    "content_type": row["content_type"],
                    "title": row["title"],
                    "body_text": row["body_text"],
                    "published_at": row["published_at"],
                    "url": row["url"],
                }
            )
            payload_digest = _hash(payload)
            target.execute(
                """
                INSERT INTO source_objects(
                    source_system_id,external_id,object_kind,canonical_url,
                    first_seen_at,last_seen_at,metadata_json
                ) VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(source_system_id,external_id) DO UPDATE SET
                    last_seen_at=excluded.last_seen_at,
                    canonical_url=COALESCE(excluded.canonical_url, source_objects.canonical_url)
                """,
                (
                    source_system_id,
                    external_id,
                    str(row["content_type"] or "content"),
                    row["url"],
                    row["collected_at"] or now,
                    now,
                    json.dumps(
                        {
                            "legacy_raw_item_id": int(row["raw_item_id"]),
                            "legacy_content_item_id": row["content_item_id"],
                            "legacy_status": row["status"],
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                ),
            )
            object_id = int(
                target.execute(
                    "SELECT id FROM source_objects WHERE source_system_id=? AND external_id=?",
                    (source_system_id, external_id),
                ).fetchone()[0]
            )
            revision = target.execute(
                "SELECT id FROM source_revisions WHERE source_object_id=? AND payload_hash=?",
                (object_id, payload_digest),
            ).fetchone()
            if revision is not None:
                revision_id = int(revision[0])
                target.execute("UPDATE source_revisions SET is_current=0 WHERE source_object_id=? AND id<>?", (object_id,revision_id))
                target.execute("UPDATE source_revisions SET is_current=1 WHERE id=?", (revision_id,))
                stats["revisions_reused"] += 1
            else:
                previous = target.execute(
                    """
                    SELECT id,revision_no FROM source_revisions
                    WHERE source_object_id=? AND is_current=1
                    """,
                    (object_id,),
                ).fetchone()
                if previous is not None:
                    target.execute("UPDATE source_revisions SET is_current=0 WHERE id=?", (int(previous[0]),))
                revision_no = target.execute("SELECT COALESCE(MAX(revision_no),0)+1 FROM source_revisions WHERE source_object_id=?", (object_id,)).fetchone()[0]
                revision_id = int(
                    target.execute(
                        """
                        INSERT INTO source_revisions(
                            source_object_id,revision_no,payload_hash,text_hash,payload_json,
                            observed_at,fetched_at,supersedes_revision_id,is_current,metadata_json
                        ) VALUES(?,?,?,?,?,?,?,?,1,?)
                        """,
                        (
                            object_id,
                            revision_no,
                            payload_digest,
                            _hash(" ".join(str(row["body_text"] or "").split())) if row["body_text"] else None,
                            payload,
                            observed_at,
                            row["collected_at"] or now,
                            int(previous[0]) if previous is not None else None,
                            json.dumps({"imported_from": str(legacy_path)}, ensure_ascii=False),
                        ),
                    ).lastrowid
                )
                stats["revisions_created"] += 1

            observation = target.execute("SELECT id,source_revision_id,sequence_no FROM source_observations WHERE source_object_id=? ORDER BY sequence_no DESC LIMIT 1", (object_id,)).fetchone()
            if observation is None or observation[1] != revision_id:
                target.execute("""INSERT INTO source_observations(source_object_id,source_revision_id,sequence_no,observed_at,fetched_at,previous_observation_id)
                    VALUES(?,?,?,?,?,?)""", (object_id,revision_id,observation[2]+1 if observation else 1,observed_at,now,observation[0] if observation else None))

            blobs = legacy.execute(
                """
                SELECT hash_sha256,mime_type,file_size,
                       COALESCE(storage_rel_path,file_path) AS storage_path,metadata_json
                FROM raw_blobs WHERE raw_item_id=? AND hash_sha256 IS NOT NULL
                """,
                (int(row["raw_item_id"]),),
            ).fetchall()
            for blob in blobs:
                target.execute(
                    """
                    INSERT INTO blobs(sha256,media_type,byte_size,storage_path,metadata_json)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(sha256) DO UPDATE SET
                        storage_path=COALESCE(blobs.storage_path, excluded.storage_path)
                    """,
                    (
                        blob["hash_sha256"],
                        blob["mime_type"],
                        int(blob["file_size"] or 0),
                        str(blob["storage_path"] or ""),
                        blob["metadata_json"],
                    ),
                )
                blob_id = int(target.execute("SELECT id FROM blobs WHERE sha256=?", (blob["hash_sha256"],)).fetchone()[0])
                target.execute(
                    "INSERT OR IGNORE INTO revision_blobs(revision_id,blob_id,role) VALUES(?,?,'attachment')",
                    (revision_id, blob_id),
                )
                stats["blobs_linked"] += 1
        target.commit()
        stats["objects_created"] = int(target.execute("SELECT COUNT(*) FROM source_objects").fetchone()[0]) - objects_before
        return {"ok": True, "legacy_db": str(legacy_path), **stats}
    except Exception:
        target.rollback()
        raise
    finally:
        legacy.close()
        target.close()
