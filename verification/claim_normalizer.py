from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import defaultdict
from datetime import datetime
from typing import Any

from config.db_utils import get_db, load_settings

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
except Exception:  # pragma: no cover - optional import fallback
    TfidfVectorizer = None
    cosine_similarity = None


LOW_SIGNAL_EXACT = {
    "сказал",
    "заявил",
    "заявила",
    "допрос",
    "задержан",
    "мобилизован",
    "арестован",
    "пригрозил",
}

LOW_SIGNAL_RE = re.compile(
    r"^(?:сказал|заявил|заявила|задержан|мобилизован|арестован|допрос|пригрозил|обещал)$",
    re.I,
)


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("ё", "е").lower()).strip()


def canonicalize_claim_text(claim_text: str, claim_type: str | None = None) -> str | None:
    normalized = _normalize_text(claim_text)
    if not normalized:
        return None
    normalized = re.sub(r"[«»\"“”„]", "", normalized)
    normalized = re.sub(r"[.,;:!?…]+$", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    words = re.findall(r"\b[\w-]+\b", normalized, flags=re.UNICODE)
    if len(words) < 3:
        return None
    if LOW_SIGNAL_RE.match(normalized):
        return None
    if normalized in LOW_SIGNAL_EXACT:
        return None
    if claim_type == "public_statement" and len(words) < 4:
        return None
    return normalized


def canonical_hash(canonical_text: str) -> str:
    return hashlib.sha256(canonical_text.encode("utf-8", errors="ignore")).hexdigest()


def _semantic_groups(items: list[dict[str, Any]], threshold: float = 0.82) -> list[list[int]]:
    if not items:
        return []
    if not TfidfVectorizer or not cosine_similarity:
        return [[item["claim_id"]] for item in items]

    texts = [item["canonical_text"] for item in items]
    matrix = TfidfVectorizer(ngram_range=(1, 2), min_df=1).fit_transform(texts)
    sims = cosine_similarity(matrix)
    parent = list(range(len(items)))

    def find(idx: int) -> int:
        while parent[idx] != idx:
            parent[idx] = parent[parent[idx]]
            idx = parent[idx]
        return idx

    def union(a: int, b: int):
        root_a = find(a)
        root_b = find(b)
        if root_a != root_b:
            parent[root_b] = root_a

    for idx in range(len(items)):
        for jdx in range(idx + 1, len(items)):
            if items[idx]["claim_type"] != items[jdx]["claim_type"]:
                continue
            if sims[idx, jdx] >= threshold:
                union(idx, jdx)

    grouped: dict[int, list[int]] = defaultdict(list)
    for idx, item in enumerate(items):
        grouped[find(idx)].append(int(item["claim_id"]))
    return list(grouped.values())


def _claim_rows(conn: sqlite3.Connection, limit: int | None = None) -> list[sqlite3.Row]:
    sql = """
        SELECT id, content_item_id, claim_text, claim_type, status
        FROM claims
        ORDER BY id
    """
    params: list[Any] = []
    if limit:
        sql += " LIMIT ?"
        params.append(int(limit))
    return conn.execute(sql, params).fetchall()


def sync_claim_clusters(settings: dict[str, Any] | None = None, limit: int | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    conn = get_db(settings)
    conn.row_factory = sqlite3.Row
    try:
        rows = _claim_rows(conn, limit=limit)
        conn.execute("DELETE FROM claim_occurrences")
        conn.execute("DELETE FROM claim_clusters")

        valid_items: list[dict[str, Any]] = []
        archived = 0
        for row in rows:
            canonical = canonicalize_claim_text(row["claim_text"], row["claim_type"])
            if canonical is None:
                conn.execute(
                    """
                    UPDATE claims
                    SET canonical_text=NULL,
                        canonical_hash=NULL,
                        claim_cluster_id=NULL,
                        status='archived_low_signal',
                        needs_review=0
                    WHERE id=?
                    """,
                    (row["id"],),
                )
                archived += 1
                continue
            valid_items.append(
                {
                    "claim_id": int(row["id"]),
                    "content_item_id": int(row["content_item_id"]),
                    "claim_type": row["claim_type"] or "",
                    "canonical_text": canonical,
                    "canonical_hash": canonical_hash(canonical),
                    "claim_text": row["claim_text"],
                }
            )

        groups = _semantic_groups(valid_items)
        by_claim_id = {item["claim_id"]: item for item in valid_items}
        cluster_count = 0
        occurrence_count = 0
        for claim_ids in groups:
            members = [by_claim_id[claim_id] for claim_id in claim_ids]
            canonical_text = sorted((item["canonical_text"] for item in members), key=len)[0]
            cluster_key = f"{members[0]['claim_type']}:{canonical_hash(canonical_text)}"
            cur = conn.execute(
                """
                INSERT INTO claim_clusters(cluster_key, canonical_text, claim_type, method, support_count, metadata_json, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    cluster_key,
                    canonical_text,
                    members[0]["claim_type"],
                    "semantic_tfidf" if len(members) > 1 and TfidfVectorizer else "canonical",
                    len(members),
                    json.dumps({"claim_ids": claim_ids}, ensure_ascii=False),
                    datetime.now().isoformat(),
                    datetime.now().isoformat(),
                ),
            )
            cluster_id = int(cur.lastrowid)
            cluster_count += 1
            for item in members:
                conn.execute(
                    """
                    UPDATE claims
                    SET canonical_text=?, canonical_hash=?, claim_cluster_id=?, status=CASE WHEN status='archived_low_signal' THEN 'unverified' ELSE status END
                    WHERE id=?
                    """,
                    (canonical_text, item["canonical_hash"], cluster_id, item["claim_id"]),
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO claim_occurrences(
                        claim_cluster_id, claim_id, content_item_id, occurrence_text, occurrence_hash, source_kind, metadata_json
                    ) VALUES(?,?,?,?,?,?,?)
                    """,
                    (
                        cluster_id,
                        item["claim_id"],
                        item["content_item_id"],
                        item["claim_text"],
                        hashlib.sha256(item["claim_text"].encode("utf-8", errors="ignore")).hexdigest(),
                        "claim",
                        json.dumps({"canonical_hash": item["canonical_hash"]}, ensure_ascii=False),
                    ),
                )
                occurrence_count += 1

        conn.commit()
        return {
            "ok": True,
            "claims_seen": len(rows),
            "archived_low_signal": archived,
            "clusters_created": cluster_count,
            "occurrences_created": occurrence_count,
        }
    finally:
        conn.close()
