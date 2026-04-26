from __future__ import annotations

from difflib import SequenceMatcher
from typing import Any

from enrichment.common import (
    body_signature,
    clean_text,
    ensure_review_task,
    json_dumps,
    open_db,
    stable_hash,
    title_signature,
)


def _candidate_rows(conn):
    return conn.execute(
        """
        SELECT id, source_id, external_id, content_type, title, body_text, url, published_at
        FROM content_items
        WHERE COALESCE(title, '') <> '' OR COALESCE(body_text, '') <> ''
        ORDER BY id
        """
    ).fetchall()


def _similarity(title_a: str, title_b: str, body_a: str, body_b: str) -> float:
    title_score = SequenceMatcher(None, title_signature(title_a), title_signature(title_b)).ratio()
    body_score = SequenceMatcher(None, body_signature(body_a), body_signature(body_b)).ratio()
    return round((title_score * 0.7) + (body_score * 0.3), 4)


def run_content_dedupe(settings: dict[str, Any] | None = None, *, min_cluster_size: int = 2) -> dict[str, Any]:
    settings = settings or {}
    conn = open_db(settings)
    try:
        rows = _candidate_rows(conn)
        grouped: dict[str, list] = {}
        for row in rows:
            signature = title_signature(row["title"] or "", row["body_text"] or "")
            if not signature:
                continue
            grouped.setdefault(signature, []).append(row)

        clusters_created = 0
        cluster_item_rows = 0
        review_tasks = 0
        for signature, items in grouped.items():
            if len(items) < min_cluster_size:
                continue
            canonical = min(items, key=lambda item: item["id"])
            similarity = 0.0
            if len(items) >= 2:
                similarity = _similarity(
                    canonical["title"] or "",
                    items[1]["title"] or "",
                    canonical["body_text"] or "",
                    items[1]["body_text"] or "",
                )
            cluster_key = stable_hash(signature, prefix="content:")
            payload = {
                "cluster_key": cluster_key,
                "signature": signature,
                "items": [int(item["id"]) for item in items],
                "canonical_content_id": int(canonical["id"]),
            }
            existing = conn.execute(
                "SELECT id FROM content_clusters WHERE cluster_key=? LIMIT 1",
                (cluster_key,),
            ).fetchone()
            if existing:
                cluster_id = int(existing[0])
                conn.execute(
                    """
                    UPDATE content_clusters
                    SET canonical_content_id=?, canonical_title=?, similarity_score=?, item_count=?,
                        first_seen_at=COALESCE(first_seen_at, ?), last_seen_at=?, metadata_json=?, updated_at=?
                    WHERE id=?
                    """,
                    (
                        int(canonical["id"]),
                        canonical["title"] or canonical["body_text"][:140],
                        similarity,
                        len(items),
                        canonical["published_at"] or None,
                        items[-1]["published_at"] or canonical["published_at"] or None,
                        json_dumps(payload),
                        clean_text(items[-1]["published_at"]) or None,
                        cluster_id,
                    ),
                )
            else:
                cur = conn.execute(
                    """
                    INSERT INTO content_clusters(
                        cluster_key, cluster_type, canonical_content_id, canonical_title,
                        method, similarity_score, item_count, first_seen_at, last_seen_at, metadata_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        cluster_key,
                        "document_dedupe",
                        int(canonical["id"]),
                        canonical["title"] or canonical["body_text"][:140],
                        "title_signature",
                        similarity,
                        len(items),
                        canonical["published_at"] or None,
                        items[-1]["published_at"] or canonical["published_at"] or None,
                        json_dumps(payload),
                    ),
                )
                cluster_id = int(cur.lastrowid)
                clusters_created += 1

            for item in items:
                cur = conn.execute(
                    """
                    INSERT OR REPLACE INTO content_cluster_items(
                        id, cluster_id, content_item_id, similarity_score, reason, is_canonical, metadata_json
                    )
                    VALUES(
                        COALESCE((SELECT id FROM content_cluster_items WHERE cluster_id=? AND content_item_id=?), NULL),
                        ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (
                        cluster_id,
                        int(item["id"]),
                        cluster_id,
                        int(item["id"]),
                        _similarity(canonical["title"] or "", item["title"] or "", canonical["body_text"] or "", item["body_text"] or ""),
                        "normalized_title_duplicate",
                        1 if int(item["id"]) == int(canonical["id"]) else 0,
                        json_dumps({"source_id": item["source_id"], "url": item["url"]}),
                    ),
                )
                if cur.rowcount:
                    cluster_item_rows += 1

            task_id = ensure_review_task(
                conn,
                task_key=f"content-cluster-{cluster_key}",
                queue_key="content_duplicates",
                subject_type="content_cluster",
                subject_id=cluster_id,
                candidate_payload=payload,
                suggested_action="merge",
                confidence=max(similarity, 0.86),
                machine_reason="Normalized title/body duplicate cluster",
                source_links=[item["url"] for item in items if clean_text(item["url"])],
                status="open",
            )
            if task_id:
                review_tasks += 1

        conn.commit()
        return {
            "ok": True,
            "items_seen": len(rows),
            "items_new": cluster_item_rows,
            "clusters_created": clusters_created,
            "review_tasks_created": review_tasks,
        }
    finally:
        conn.close()

