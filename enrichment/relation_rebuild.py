from __future__ import annotations

import sqlite3
from typing import Any

from enrichment.common import open_db
from graph.relation_candidates import rebuild_and_promote_relation_candidates


def _relation_kind_for_affiliation(role_type: str) -> str:
    value = (role_type or "").lower()
    if any(
        marker in value
        for marker in (
            "director",
            "директор",
            "руковод",
            "president",
            "президент",
            "председател",
            "vice_president",
            "board_chair",
            "general_director",
        )
    ):
        return "head_of"
    return "member_of"


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _insert_entity_relation(
    conn: sqlite3.Connection,
    *,
    from_entity_id: int,
    to_entity_id: int,
    relation_type: str,
    evidence_item_id: int | None,
    strength: str,
    detected_by: str,
):
    columns = _table_columns(conn, "entity_relations")
    payload: dict[str, Any] = {
        "from_entity_id": from_entity_id,
        "to_entity_id": to_entity_id,
        "relation_type": relation_type,
        "evidence_item_id": evidence_item_id,
        "strength": strength,
        "detected_by": detected_by,
    }
    if "support_count" in columns:
        payload["support_count"] = 1 if evidence_item_id else 0
    if "score" in columns:
        payload["score"] = 0.94 if detected_by == "company_affiliations" else 0.91
    if "bidirectional" in columns:
        payload["bidirectional"] = 0
    column_sql = ", ".join(payload.keys())
    placeholder_sql = ", ".join("?" for _ in payload)
    conn.execute(
        f"INSERT OR IGNORE INTO entity_relations({column_sql}) VALUES({placeholder_sql})",
        tuple(payload.values()),
    )


def run_relation_rebuild_enriched(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = settings or {}
    conn = open_db(settings)
    inserted = 0
    try:
        conn.execute(
            """
            DELETE FROM entity_relations
            WHERE detected_by IN ('company_affiliations', 'restriction_events')
            """
        )
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='company_affiliations'").fetchone():
            for row in conn.execute(
                """
                SELECT entity_id, company_entity_id, role_type, source_content_id
                FROM company_affiliations
                WHERE company_entity_id IS NOT NULL
                """
            ).fetchall():
                relation_type = _relation_kind_for_affiliation(row["role_type"])
                _insert_entity_relation(
                    conn,
                    from_entity_id=int(row["entity_id"]),
                    to_entity_id=int(row["company_entity_id"]),
                    relation_type=relation_type,
                    evidence_item_id=row["source_content_id"],
                    strength="strong",
                    detected_by="company_affiliations",
                )
                inserted += int(conn.execute("SELECT changes()").fetchone()[0])
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='restriction_events'").fetchone():
            for row in conn.execute(
                """
                SELECT issuer_entity_id, target_entity_id, source_content_id
                FROM restriction_events
                WHERE issuer_entity_id IS NOT NULL AND target_entity_id IS NOT NULL
                """
            ).fetchall():
                _insert_entity_relation(
                    conn,
                    from_entity_id=int(row["issuer_entity_id"]),
                    to_entity_id=int(row["target_entity_id"]),
                    relation_type="restricted",
                    evidence_item_id=row["source_content_id"],
                    strength="strong",
                    detected_by="restriction_events",
                )
                inserted += int(conn.execute("SELECT changes()").fetchone()[0])
        conn.commit()
    finally:
        conn.close()
    candidate_stats = rebuild_and_promote_relation_candidates(settings)
    return {
        "ok": True,
        "items_new": inserted,
        "artifacts": {"entity_relations_inserted": inserted, "candidate_relations": candidate_stats},
    }
