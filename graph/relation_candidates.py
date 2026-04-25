from __future__ import annotations

import json
import logging
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from config.db_utils import get_db, load_settings


log = logging.getLogger(__name__)

MIN_SUPPORT_ITEMS = 3
MIN_SUPPORT_SOURCES = 2
MIN_SUPPORT_DOMAINS = 2
PROMOTION_SCORE_THRESHOLD = 0.68
PROMOTED_RELATION_TYPES = (
    "likely_association",
    "same_vote_pattern",
    "same_bill_cluster",
    "same_contract_cluster",
    "same_case_cluster",
)
GENERIC_TAGS = {
    "regional",
    "international",
    "technology",
    "технологии",
    "искусственный интеллект",
}
ROLE_COMPATIBILITY = {
    ("person", "organization"): 1.0,
    ("organization", "person"): 1.0,
    ("person", "person"): 0.85,
    ("organization", "organization"): 0.8,
    ("person", "location"): 0.55,
    ("location", "person"): 0.55,
    ("organization", "location"): 0.6,
    ("location", "organization"): 0.6,
    ("location", "location"): 0.45,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def _parse_json(raw_text: str | None, default: Any):
    if not raw_text:
        return default
    try:
        return json.loads(raw_text)
    except (TypeError, json.JSONDecodeError):
        return default


def _normalize_domain(url: str | None, source_id: int | None) -> str:
    if url:
        parsed = urlparse(url)
        host = parsed.netloc.lower().strip()
        if host.startswith("www."):
            host = host[4:]
        if host:
            return host
    return f"source:{source_id or 'unknown'}"


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip().rstrip("Z")
    if not raw:
        return None
    if len(raw) == 10:
        raw = f"{raw}T00:00:00"
    try:
        return datetime.fromisoformat(raw[:19])
    except ValueError:
        return None


def _temporal_score(dates: list[datetime]) -> float:
    if not dates:
        return 0.4
    if len(dates) == 1:
        return 0.7
    ordered = sorted(dates)
    diff_days = max(0, (ordered[-1] - ordered[0]).days)
    if diff_days <= 30:
        return 1.0
    if diff_days <= 90:
        return 0.85
    if diff_days <= 180:
        return 0.7
    if diff_days <= 365:
        return 0.5
    return 0.25


def _role_score(entity_a_type: str, entity_b_type: str) -> float:
    return ROLE_COMPATIBILITY.get((entity_a_type, entity_b_type), 0.5)


def _specific_tags(tags: set[str]) -> list[str]:
    result = []
    for tag in sorted(tags):
        lowered = (tag or "").strip().lower()
        if not lowered:
            continue
        if lowered in GENERIC_TAGS:
            continue
        result.append(tag)
    return result


def _load_set_map(conn, sql: str) -> dict[int, set[int]]:
    data: dict[int, set[int]] = defaultdict(set)
    for entity_id, value in conn.execute(sql).fetchall():
        if entity_id is None or value is None:
            continue
        data[int(entity_id)].add(int(value))
    return data


def _load_vote_map(conn) -> dict[int, dict[int, str]]:
    data: dict[int, dict[int, str]] = defaultdict(dict)
    if not _table_exists(conn, "bill_votes"):
        return data
    for entity_id, vote_session_id, vote_result in conn.execute(
        """
        SELECT entity_id, vote_session_id, vote_result
        FROM bill_votes
        WHERE entity_id IS NOT NULL
        """
    ).fetchall():
        if entity_id is None or vote_session_id is None:
            continue
        data[int(entity_id)][int(vote_session_id)] = str(vote_result or "")
    return data


def _load_risk_map(conn) -> dict[int, set[int]]:
    if not _table_exists(conn, "risk_patterns"):
        return defaultdict(set)
    data: dict[int, set[int]] = defaultdict(set)
    for risk_id, entity_ids in conn.execute(
        "SELECT id, entity_ids FROM risk_patterns"
    ).fetchall():
        for entity_id in _parse_json(entity_ids, []):
            if isinstance(entity_id, int):
                data[entity_id].add(int(risk_id))
            elif isinstance(entity_id, str) and entity_id.isdigit():
                data[int(entity_id)].add(int(risk_id))
    return data


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _shared_content_rows(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT DISTINCT
            CASE WHEN em1.entity_id < em2.entity_id THEN em1.entity_id ELSE em2.entity_id END AS entity_a,
            CASE WHEN em1.entity_id < em2.entity_id THEN em2.entity_id ELSE em1.entity_id END AS entity_b,
            em1.content_item_id,
            ci.source_id,
            COALESCE(s.url, '') AS source_url,
            COALESCE(s.category, '') AS source_category,
            COALESCE(ci.published_at, ci.collected_at, '') AS seen_at,
            COALESCE(ci.title, '') AS title,
            COALESCE(ci.body_text, '') AS body_text
        FROM entity_mentions em1
        JOIN entity_mentions em2
          ON em1.content_item_id = em2.content_item_id
         AND em1.entity_id < em2.entity_id
        JOIN content_items ci ON ci.id = em1.content_item_id
        LEFT JOIN sources s ON s.id = ci.source_id
        """
    ).fetchall()
    result = []
    for row in rows:
        result.append(
            {
                "entity_a": int(row[0]),
                "entity_b": int(row[1]),
                "content_item_id": int(row[2]),
                "source_id": int(row[3]) if row[3] is not None else None,
                "source_url": row[4],
                "source_category": row[5],
                "seen_at": row[6],
                "title": row[7],
                "body_text": row[8],
            }
        )
    return result


def _load_tag_map(conn) -> dict[int, set[str]]:
    if not _table_exists(conn, "content_tags"):
        return defaultdict(set)
    data: dict[int, set[str]] = defaultdict(set)
    for content_item_id, tag_name in conn.execute(
        "SELECT content_item_id, tag_name FROM content_tags"
    ).fetchall():
        data[int(content_item_id)].add(str(tag_name or ""))
    return data


def _score_candidate(
    entity_a_type: str,
    entity_b_type: str,
    shared_rows: list[dict[str, Any]],
    shared_tags: set[str],
    *,
    case_overlap: int,
    bill_overlap: int,
    contract_overlap: int,
    risk_overlap: int,
    shared_claims: int,
    vote_overlap_count: int,
    vote_overlap_ratio: float,
) -> dict[str, Any]:
    support_items = len({row["content_item_id"] for row in shared_rows})
    source_ids = {row["source_id"] for row in shared_rows if row["source_id"] is not None}
    support_sources = len(source_ids)
    domains = {_normalize_domain(row["source_url"], row["source_id"]) for row in shared_rows}
    support_domains = len(domains)
    categories = {row["source_category"] for row in shared_rows if row["source_category"]}
    support_categories = len(categories)
    dates = [value for value in (_parse_date(row["seen_at"]) for row in shared_rows) if value is not None]
    specific_tags = _specific_tags(shared_tags)
    avg_len = statistics.mean(
        max(0, len((row.get("title") or "").strip()) + len((row.get("body_text") or "").strip()))
        for row in shared_rows
    ) if shared_rows else 0.0

    source_independence = min(1.0, 0.6 * min(1.0, support_sources / 4.0) + 0.4 * min(1.0, support_domains / 3.0))
    evidence_overlap = min(
        1.0,
        min(0.15, shared_claims * 0.05)
        + (0.25 if case_overlap else 0.0)
        + min(0.20, bill_overlap * 0.07)
        + (0.25 if contract_overlap else 0.0)
        + (0.20 if risk_overlap else 0.0)
        + (0.15 if vote_overlap_count >= 5 and vote_overlap_ratio >= 0.6 else 0.0),
    )
    temporal_proximity = _temporal_score(dates)
    role_compatibility = _role_score(entity_a_type, entity_b_type)
    tag_overlap = min(1.0, len(specific_tags) / 5.0)
    text_specificity = min(1.0, 0.5 * min(1.0, avg_len / 600.0) + 0.5 * min(1.0, len(specific_tags) / 6.0))

    score = (
        0.30 * source_independence
        + 0.25 * evidence_overlap
        + 0.15 * temporal_proximity
        + 0.15 * role_compatibility
        + 0.10 * tag_overlap
        + 0.05 * text_specificity
    )

    return {
        "score": round(score, 4),
        "source_independence": round(source_independence, 4),
        "evidence_overlap": round(evidence_overlap, 4),
        "temporal_proximity": round(temporal_proximity, 4),
        "role_compatibility": round(role_compatibility, 4),
        "tag_overlap": round(tag_overlap, 4),
        "text_specificity": round(text_specificity, 4),
        "support_items": support_items,
        "support_sources": support_sources,
        "support_domains": support_domains,
        "support_categories": support_categories,
        "specific_tags": specific_tags[:12],
    }


def _score_value(
    source_independence: float,
    evidence_overlap: float,
    temporal_proximity: float,
    role_compatibility: float,
    tag_overlap: float,
    text_specificity: float,
) -> float:
    return (
        0.30 * source_independence
        + 0.25 * evidence_overlap
        + 0.15 * temporal_proximity
        + 0.15 * role_compatibility
        + 0.10 * tag_overlap
        + 0.05 * text_specificity
    )


def _apply_structural_seed_floor(
    metrics: dict[str, Any],
    *,
    structural_seed_kind: str | None,
    candidate_type: str,
) -> dict[str, Any]:
    if structural_seed_kind == "vote" and candidate_type == "same_vote_pattern":
        metrics["source_independence"] = round(max(float(metrics["source_independence"]), 0.85), 4)
        metrics["evidence_overlap"] = round(max(float(metrics["evidence_overlap"]), 0.85), 4)
        metrics["temporal_proximity"] = round(max(float(metrics["temporal_proximity"]), 0.75), 4)
    elif structural_seed_kind == "bill" and candidate_type == "same_bill_cluster":
        metrics["source_independence"] = round(max(float(metrics["source_independence"]), 0.50), 4)
        metrics["evidence_overlap"] = round(max(float(metrics["evidence_overlap"]), 0.55), 4)
        metrics["temporal_proximity"] = round(max(float(metrics["temporal_proximity"]), 0.55), 4)
    elif structural_seed_kind == "contract" and candidate_type == "same_contract_cluster":
        metrics["source_independence"] = round(max(float(metrics["source_independence"]), 0.45), 4)
        metrics["evidence_overlap"] = round(max(float(metrics["evidence_overlap"]), 0.45), 4)
        metrics["temporal_proximity"] = round(max(float(metrics["temporal_proximity"]), 0.50), 4)

    metrics["score"] = round(
        _score_value(
            float(metrics["source_independence"]),
            float(metrics["evidence_overlap"]),
            float(metrics["temporal_proximity"]),
            float(metrics["role_compatibility"]),
            float(metrics["tag_overlap"]),
            float(metrics["text_specificity"]),
        ),
        4,
    )
    return metrics


def _candidate_type(
    *,
    bill_overlap: int,
    contract_overlap: int,
    case_overlap: int,
    risk_overlap: int,
    vote_overlap_count: int,
    vote_overlap_ratio: float,
) -> str:
    if vote_overlap_count >= 20 and vote_overlap_ratio >= 0.75:
        return "same_vote_pattern"
    if contract_overlap >= 1:
        return "same_contract_cluster"
    if case_overlap >= 1 or risk_overlap >= 1:
        return "same_case_cluster"
    if bill_overlap >= 3:
        return "same_bill_cluster"
    return "likely_association"


def _pairs_from_membership_map(
    membership_map: dict[int, set[int]],
    *,
    max_group_size: int = 12,
    min_shared: int = 1,
) -> set[tuple[int, int]]:
    reverse_map: dict[int, set[int]] = defaultdict(set)
    for entity_id, values in membership_map.items():
        for value in values:
            reverse_map[int(value)].add(int(entity_id))

    pair_values: dict[tuple[int, int], set[int]] = defaultdict(set)
    for value_id, entity_ids in reverse_map.items():
        unique_ids = sorted(entity_ids)
        if len(unique_ids) < 2 or len(unique_ids) > max_group_size:
            continue
        for entity_a, entity_b in combinations(unique_ids, 2):
            pair_values[(entity_a, entity_b)].add(int(value_id))
    return {
        pair
        for pair, shared_values in pair_values.items()
        if len(shared_values) >= max(1, int(min_shared))
    }


def _vote_seed_pairs(
    vote_map: dict[int, dict[int, str]],
    *,
    eligible_entities: set[int],
    min_same_votes: int = 20,
    min_ratio: float = 0.75,
) -> dict[tuple[int, int], dict[str, Any]]:
    entity_ids = sorted(
        entity_id
        for entity_id, votes in vote_map.items()
        if entity_id in eligible_entities and len(votes) >= min_same_votes
    )
    vote_sets = {entity_id: set(vote_map[entity_id]) for entity_id in entity_ids}
    seeds: dict[tuple[int, int], dict[str, Any]] = {}
    for index, entity_a in enumerate(entity_ids):
        votes_a = vote_map[entity_a]
        vote_ids_a = vote_sets[entity_a]
        for entity_b in entity_ids[index + 1:]:
            shared_vote_ids = vote_ids_a & vote_sets[entity_b]
            if len(shared_vote_ids) < min_same_votes:
                continue
            same_votes = sum(
                1 for vote_id in shared_vote_ids if votes_a.get(vote_id) == vote_map[entity_b].get(vote_id)
            )
            vote_ratio = same_votes / len(shared_vote_ids) if shared_vote_ids else 0.0
            if same_votes >= min_same_votes and vote_ratio >= min_ratio:
                seeds[(entity_a, entity_b)] = {
                    "same_vote_count": same_votes,
                    "shared_vote_count": len(shared_vote_ids),
                    "same_vote_ratio": round(vote_ratio, 4),
                }
    return seeds


def _delete_previous_candidate_state(conn):
    conn.execute("DELETE FROM relation_support")
    conn.execute("DELETE FROM relation_candidates WHERE origin LIKE 'candidate_builder:%'")
    conn.execute(
        """
        DELETE FROM entity_relations
        WHERE relation_type='mentioned_together'
           OR COALESCE(detected_by, '') LIKE 'co_occurrence:%'
           OR COALESCE(detected_by, '') LIKE 'relation_candidate:%'
           OR relation_type IN ({placeholders})
        """.format(placeholders=",".join("?" * len(PROMOTED_RELATION_TYPES))),
        PROMOTED_RELATION_TYPES,
    )
    conn.commit()


def rebuild_relation_candidates(settings: dict | None = None) -> dict[str, Any]:
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    try:
        _delete_previous_candidate_state(conn)
        if not _table_exists(conn, "entity_mentions") or not _table_exists(conn, "relation_candidates"):
            return {
                "ok": False,
                "fatal_errors": ["relation_candidate_schema_missing"],
            }

        entity_types = {
            int(row[0]): str(row[1] or "")
            for row in conn.execute("SELECT id, entity_type FROM entities").fetchall()
        }
        tag_map = _load_tag_map(conn)
        case_map = _load_set_map(
            conn,
            """
            SELECT em.entity_id, cc.case_id
            FROM case_claims cc
            JOIN claims cl ON cl.id = cc.claim_id
            JOIN entity_mentions em ON em.content_item_id = cl.content_item_id
            """,
        ) if _table_exists(conn, "case_claims") and _table_exists(conn, "claims") else defaultdict(set)
        bill_map = _load_set_map(conn, "SELECT entity_id, bill_id FROM bill_sponsors WHERE entity_id IS NOT NULL") if _table_exists(conn, "bill_sponsors") else defaultdict(set)
        contract_map = _load_set_map(conn, "SELECT entity_id, contract_id FROM contract_parties WHERE entity_id IS NOT NULL") if _table_exists(conn, "contract_parties") else defaultdict(set)
        risk_map = _load_risk_map(conn)
        vote_map = _load_vote_map(conn)

        pair_rows = defaultdict(list)
        for record in _shared_content_rows(conn):
            pair_rows[(record["entity_a"], record["entity_b"])].append(record)
        structural_seed_pairs = _pairs_from_membership_map(contract_map, max_group_size=6)
        bill_seed_pairs = _pairs_from_membership_map(bill_map, max_group_size=24, min_shared=3)
        vote_seed_pairs = _vote_seed_pairs(
            vote_map,
            eligible_entities={entity_id for entity_id, entity_type in entity_types.items() if entity_type == "person"},
        )
        pair_keys = set(pair_rows)
        pair_keys.update(structural_seed_pairs)
        pair_keys.update(bill_seed_pairs)
        pair_keys.update(vote_seed_pairs)

        created = 0
        support_rows_created = 0
        skipped_low_support = 0
        promoted_candidates = 0

        for entity_a, entity_b in sorted(pair_keys):
            shared_rows = pair_rows.get((entity_a, entity_b), [])
            support_items = len({row["content_item_id"] for row in shared_rows})
            support_sources = len({row["source_id"] for row in shared_rows if row["source_id"] is not None})
            support_domains = len({_normalize_domain(row["source_url"], row["source_id"]) for row in shared_rows})
            structural_seed_kind = None
            if (entity_a, entity_b) in vote_seed_pairs:
                structural_seed_kind = "vote"
            elif (entity_a, entity_b) in bill_seed_pairs:
                structural_seed_kind = "bill"
            elif (entity_a, entity_b) in structural_seed_pairs:
                structural_seed_kind = "contract"

            entity_a_type = entity_types.get(entity_a, "entity")
            entity_b_type = entity_types.get(entity_b, "entity")
            shared_content_ids = sorted({row["content_item_id"] for row in shared_rows})
            shared_tags = set()
            for content_id in shared_content_ids:
                shared_tags.update(tag_map.get(content_id, set()))

            case_overlap = len(case_map.get(entity_a, set()) & case_map.get(entity_b, set()))
            bill_overlap = len(bill_map.get(entity_a, set()) & bill_map.get(entity_b, set()))
            contract_overlap = len(contract_map.get(entity_a, set()) & contract_map.get(entity_b, set()))
            risk_overlap = len(risk_map.get(entity_a, set()) & risk_map.get(entity_b, set()))
            has_structural_signal = structural_seed_kind is not None
            if support_items < MIN_SUPPORT_ITEMS or support_sources < MIN_SUPPORT_SOURCES or support_domains < MIN_SUPPORT_DOMAINS:
                if not has_structural_signal:
                    skipped_low_support += 1
                    continue
            shared_claims = 0
            if _table_exists(conn, "claims"):
                if shared_content_ids:
                    row = conn.execute(
                        "SELECT COUNT(*) FROM claims WHERE content_item_id IN ({})".format(
                            ",".join("?" * len(shared_content_ids))
                        ),
                        shared_content_ids,
                    ).fetchone()
                    shared_claims = int(row[0]) if row else 0

            vote_seed = vote_seed_pairs.get((entity_a, entity_b), {})
            if vote_seed:
                same_votes = int(vote_seed.get("same_vote_count") or 0)
                shared_vote_count = int(vote_seed.get("shared_vote_count") or 0)
                vote_overlap_ratio = float(vote_seed.get("same_vote_ratio") or 0.0)
            else:
                shared_vote_ids = set(vote_map.get(entity_a, {}).keys()) & set(vote_map.get(entity_b, {}).keys())
                same_votes = sum(
                    1
                    for vote_id in shared_vote_ids
                    if vote_map.get(entity_a, {}).get(vote_id) == vote_map.get(entity_b, {}).get(vote_id)
                )
                shared_vote_count = len(shared_vote_ids)
                vote_overlap_ratio = (same_votes / shared_vote_count) if shared_vote_count else 0.0

            metrics = _score_candidate(
                entity_a_type,
                entity_b_type,
                shared_rows,
                shared_tags,
                case_overlap=case_overlap,
                bill_overlap=bill_overlap,
                contract_overlap=contract_overlap,
                risk_overlap=risk_overlap,
                shared_claims=shared_claims,
                vote_overlap_count=same_votes,
                vote_overlap_ratio=vote_overlap_ratio,
            )
            candidate_type = _candidate_type(
                bill_overlap=bill_overlap,
                contract_overlap=contract_overlap,
                case_overlap=case_overlap,
                risk_overlap=risk_overlap,
                vote_overlap_count=same_votes,
                vote_overlap_ratio=vote_overlap_ratio,
            )
            metrics = _apply_structural_seed_floor(
                metrics,
                structural_seed_kind=structural_seed_kind,
                candidate_type=candidate_type,
            )
            if metrics["score"] >= PROMOTION_SCORE_THRESHOLD:
                promotion_state = "promoted"
            elif structural_seed_kind and not shared_rows:
                promotion_state = "review"
            else:
                promotion_state = "pending"
            if promotion_state == "promoted":
                promoted_candidates += 1

            metadata = {
                "shared_claims": shared_claims,
                "case_overlap": case_overlap,
                "bill_overlap": bill_overlap,
                "contract_overlap": contract_overlap,
                "risk_overlap": risk_overlap,
                "same_vote_count": same_votes,
                "shared_vote_count": shared_vote_count,
                "same_vote_ratio": round(vote_overlap_ratio, 4),
                "support_domains_list": sorted({_normalize_domain(row["source_url"], row["source_id"]) for row in shared_rows}),
                "support_categories_list": sorted({row["source_category"] for row in shared_rows if row["source_category"]}),
                "structural_seed": bool(structural_seed_kind and not shared_rows),
                "structural_seed_kind": structural_seed_kind,
            }

            cur = conn.execute(
                """
                INSERT INTO relation_candidates(
                    entity_a_id, entity_b_id, candidate_type, origin, score,
                    source_independence, evidence_overlap, temporal_proximity,
                    role_compatibility, tag_overlap, text_specificity,
                    support_items, support_sources, support_domains, support_categories,
                    first_seen_at, last_seen_at, sample_content_ids, promotion_state,
                    promoted_relation_type, metadata_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    entity_a,
                    entity_b,
                    candidate_type,
                    "candidate_builder:hybrid" if structural_seed_kind and not shared_rows else "candidate_builder:co_occurrence",
                    metrics["score"],
                    metrics["source_independence"],
                    metrics["evidence_overlap"],
                    metrics["temporal_proximity"],
                    metrics["role_compatibility"],
                    metrics["tag_overlap"],
                    metrics["text_specificity"],
                    metrics["support_items"],
                    metrics["support_sources"],
                    metrics["support_domains"],
                    metrics["support_categories"],
                    min((row["seen_at"] for row in shared_rows if row["seen_at"]), default=None),
                    max((row["seen_at"] for row in shared_rows if row["seen_at"]), default=None),
                    json.dumps(shared_content_ids[:20], ensure_ascii=False),
                    promotion_state,
                    candidate_type if promotion_state == "promoted" else None,
                    json.dumps(metadata, ensure_ascii=False),
                ),
            )
            candidate_id = int(cur.lastrowid)
            created += 1

            for row in shared_rows:
                conn.execute(
                    """
                    INSERT INTO relation_support(
                        candidate_id, support_kind, content_item_id, source_id, domain, category, metadata_json
                    ) VALUES(?,?,?,?,?,?,?)
                    """,
                    (
                        candidate_id,
                        "content",
                        row["content_item_id"],
                        row["source_id"],
                        _normalize_domain(row["source_url"], row["source_id"]),
                        row["source_category"],
                        json.dumps({"seen_at": row["seen_at"]}, ensure_ascii=False),
                    ),
                )
                support_rows_created += 1

            if contract_overlap:
                for contract_id in sorted(contract_map.get(entity_a, set()) & contract_map.get(entity_b, set()))[:10]:
                    conn.execute(
                        """
                        INSERT INTO relation_support(candidate_id, support_kind, metadata_json)
                        VALUES(?,?,?)
                        """,
                        (
                            candidate_id,
                            "contract",
                            json.dumps({"contract_id": contract_id}, ensure_ascii=False),
                        ),
                    )
                    support_rows_created += 1

            if bill_overlap:
                for bill_id in sorted(bill_map.get(entity_a, set()) & bill_map.get(entity_b, set()))[:10]:
                    conn.execute(
                        """
                        INSERT INTO relation_support(candidate_id, support_kind, metadata_json)
                        VALUES(?,?,?)
                        """,
                        (
                            candidate_id,
                            "bill",
                            json.dumps({"bill_id": bill_id}, ensure_ascii=False),
                        ),
                    )
                    support_rows_created += 1

            for tag_name in metrics["specific_tags"]:
                conn.execute(
                    """
                    INSERT INTO relation_support(candidate_id, support_kind, tag_name)
                    VALUES(?,?,?)
                    """,
                    (candidate_id, "tag", tag_name),
                )
                support_rows_created += 1

            for metric_key, metric_value in (
                ("shared_claims", shared_claims),
                ("case_overlap", case_overlap),
                ("bill_overlap", bill_overlap),
                ("contract_overlap", contract_overlap),
                ("risk_overlap", risk_overlap),
                ("same_vote_count", same_votes),
                ("shared_vote_count", shared_vote_count),
                ("same_vote_ratio", round(vote_overlap_ratio, 4)),
            ):
                conn.execute(
                    """
                    INSERT INTO relation_support(candidate_id, support_kind, metric_value, metadata_json)
                    VALUES(?,?,?,?)
                    """,
                    (
                        candidate_id,
                        "metric",
                        float(metric_value),
                        json.dumps({"metric": metric_key}, ensure_ascii=False),
                    ),
                )
                support_rows_created += 1

        conn.commit()
        promoted = promote_relation_candidates(conn)
        return {
            "ok": True,
            "candidate_pairs": created,
            "relation_candidates_created": created,
            "support_rows_created": support_rows_created,
            "skipped_low_support": skipped_low_support,
            "promoted_candidates": promoted["promoted_candidates"],
            "promoted_relations": promoted["promoted_relations"],
        }
    finally:
        conn.close()


def promote_relation_candidates(conn_or_settings: Any = None, score_threshold: float = PROMOTION_SCORE_THRESHOLD) -> dict[str, int]:
    close_conn = False
    if isinstance(conn_or_settings, dict) or conn_or_settings is None:
        conn = get_db(conn_or_settings or load_settings())
        close_conn = True
    else:
        conn = conn_or_settings

    try:
        conn.execute(
            "DELETE FROM entity_relations WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'"
        )
        promoted_candidates = 0
        promoted_relations = 0
        for row in conn.execute(
            """
            SELECT id, entity_a_id, entity_b_id, candidate_type, score, sample_content_ids, metadata_json, promotion_state
            FROM relation_candidates
            ORDER BY score DESC, id
            """
        ).fetchall():
            candidate_id, entity_a, entity_b, candidate_type, score, sample_content_ids, metadata_json, promotion_state = row
            if float(score or 0.0) < score_threshold:
                conn.execute(
                    """
                    UPDATE relation_candidates
                    SET promotion_state=?, promoted_relation_type=NULL, promoted_at=NULL
                    WHERE id=?
                    """,
                    ("review" if promotion_state == "review" else "pending", candidate_id),
                )
                continue

            sample_ids = _parse_json(sample_content_ids, [])
            evidence_item_id = sample_ids[0] if sample_ids else None
            strength = "strong" if float(score) >= 0.82 else "moderate" if float(score) >= 0.74 else "weak"
            detected_by = f"relation_candidate:{candidate_id}:score={float(score):.3f}"
            conn.execute(
                """
                INSERT INTO entity_relations(
                    from_entity_id, to_entity_id, relation_type, evidence_item_id, strength, detected_by
                ) VALUES(?,?,?,?,?,?)
                """,
                (
                    entity_a,
                    entity_b,
                    candidate_type,
                    evidence_item_id,
                    strength,
                    detected_by,
                ),
            )
            promoted_candidates += 1
            promoted_relations += 1
            conn.execute(
                """
                UPDATE relation_candidates
                SET promotion_state='promoted', promoted_relation_type=?, promoted_at=?, metadata_json=?
                WHERE id=?
                """,
                (
                    candidate_type,
                    _now_iso(),
                    metadata_json,
                    candidate_id,
                ),
            )
        conn.commit()
        return {
            "promoted_candidates": promoted_candidates,
            "promoted_relations": promoted_relations,
        }
    finally:
        if close_conn:
            conn.close()


def rebuild_and_promote_relation_candidates(settings: dict | None = None) -> dict[str, Any]:
    return rebuild_relation_candidates(settings)
