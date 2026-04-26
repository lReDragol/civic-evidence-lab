from __future__ import annotations

import json
import sqlite3
from typing import Any

from config.db_utils import get_db, load_settings

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
except Exception:  # pragma: no cover
    TfidfVectorizer = None
    cosine_similarity = None


def _row_dicts(rows) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def _normalized_text(text: str) -> str:
    import re

    return re.sub(r"\s+", " ", (text or "").replace("ё", "е").lower()).strip()


def _build_neighbors(
    items: list[dict[str, Any]],
    *,
    source_kind: str,
    neighbor_kind: str,
    top_k: int,
    threshold: float,
) -> list[tuple[str, int, str, int, float, str, str]]:
    if not items or not TfidfVectorizer or not cosine_similarity:
        return []

    texts = [_normalized_text(item["text"]) for item in items]
    vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=1)
    matrix = vectorizer.fit_transform(texts)
    sims = cosine_similarity(matrix)
    rows: list[tuple[str, int, str, int, float, str, str]] = []
    for idx, item in enumerate(items):
        scored = []
        for jdx, other in enumerate(items):
            if idx == jdx:
                continue
            score = float(sims[idx, jdx])
            if score < threshold:
                continue
            scored.append((score, other["id"]))
        scored.sort(reverse=True)
        for score, neighbor_id in scored[:top_k]:
            rows.append(
                (
                    source_kind,
                    int(item["id"]),
                    neighbor_kind,
                    int(neighbor_id),
                    round(score, 4),
                    "tfidf",
                    json.dumps({"threshold": threshold}, ensure_ascii=False),
                )
            )
    return rows


def build_semantic_index(settings: dict[str, Any] | None = None, top_k: int = 5, limit_per_kind: int = 1500) -> dict[str, Any]:
    settings = settings or load_settings()
    conn = get_db(settings)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("DELETE FROM semantic_neighbors WHERE method='tfidf'")

        content_rows = _row_dicts(
            conn.execute(
                """
                SELECT id, COALESCE(title, '') || ' ' || COALESCE(body_text, '') AS text
                FROM content_items
                WHERE length(COALESCE(title, '') || COALESCE(body_text, '')) > 20
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit_per_kind,),
            ).fetchall()
        )
        claim_rows = _row_dicts(
            conn.execute(
                """
                SELECT id, COALESCE(canonical_text, claim_text, '') AS text
                FROM claims
                WHERE status != 'archived_low_signal'
                  AND length(COALESCE(canonical_text, claim_text, '')) > 10
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit_per_kind,),
            ).fetchall()
        )
        entity_rows = _row_dicts(
            conn.execute(
                """
                SELECT id, COALESCE(canonical_name, '') || ' ' || COALESCE(description, '') AS text
                FROM entities
                WHERE length(COALESCE(canonical_name, '') || COALESCE(description, '')) > 5
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit_per_kind,),
            ).fetchall()
        )

        insert_rows = []
        insert_rows.extend(_build_neighbors(content_rows, source_kind="content", neighbor_kind="content", top_k=top_k, threshold=0.22))
        insert_rows.extend(_build_neighbors(claim_rows, source_kind="claim", neighbor_kind="claim", top_k=top_k, threshold=0.35))
        insert_rows.extend(_build_neighbors(entity_rows, source_kind="entity", neighbor_kind="entity", top_k=top_k, threshold=0.40))

        conn.executemany(
            """
            INSERT OR REPLACE INTO semantic_neighbors(
                source_kind, source_id, neighbor_kind, neighbor_id, score, method, metadata_json
            ) VALUES(?,?,?,?,?,?,?)
            """,
            insert_rows,
        )
        conn.commit()
        return {
            "ok": True,
            "neighbors_indexed": len(insert_rows),
            "content_items": len(content_rows),
            "claims": len(claim_rows),
            "entities": len(entity_rows),
            "method": "tfidf",
        }
    finally:
        conn.close()
