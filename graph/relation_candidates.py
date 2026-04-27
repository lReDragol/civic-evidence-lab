from __future__ import annotations

import json
import ipaddress
import logging
import re
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import networkx as nx

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
OFFICIAL_SOURCE_CATEGORIES = {"official_registry", "official_site"}
HARD_CONTENT_TYPES = {
    "registry_record",
    "court_record",
    "enforcement",
    "procurement",
    "bill",
    "transcript",
    "restriction_record",
    "declaration",
    "official_document",
}
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
GENERIC_LOCATION_TOKENS = {
    "россии",
    "россия",
    "рф",
    "москва",
    "москвы",
    "страна",
    "страны",
    "регион",
    "региона",
    "город",
    "область",
}
AMBIGUOUS_PERSON_TOKENS = {
    "кирилл",
    "светлана",
    "сергей",
    "олег",
    "андрей",
    "иван",
    "мария",
}
PROMOTION_BRIDGE_TYPES = {
    "Bill",
    "Contract",
    "VotePattern",
    "RestrictionEvent",
    "Affiliation",
    "Disclosure",
    "Asset",
}
OFFICIAL_PROMOTION_BRIDGE_TYPES = {
    "RestrictionEvent",
    "Affiliation",
    "Disclosure",
    "Asset",
}
OFFICIAL_SEED_KINDS = {"restriction", "affiliation", "disclosure"}
OFFICIAL_CANDIDATE_CONTENT_TYPES = {"restriction_record", "declaration", "official_document"}
NON_CONTENT_PATH_TYPES = PROMOTION_BRIDGE_TYPES | {"OfficialDocument"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def _parse_json(raw_text: str | None, default: Any):
    if not raw_text:
        return default
    try:
        return json.loads(raw_text)
    except (TypeError, json.JSONDecodeError):
        return default


def _normalize_domain(url: str | None, source_id: int | None = None) -> str | None:
    if not url:
        return None
    raw = str(url).strip()
    if not raw:
        return None
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = (parsed.netloc or parsed.path.split("/", 1)[0]).lower().strip()
    if host.startswith("www."):
        host = host[4:]
    if not host or host.startswith("source:"):
        return None
    if host in {"telegram-export", "unknown"}:
        return None
    if "." not in host:
        try:
            ipaddress.ip_address(host)
        except ValueError:
            return None
    return host


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


def _tokenize_name(value: str | None) -> list[str]:
    return re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", (value or "").lower())


def _normalized_name_key(value: str | None) -> str:
    return " ".join(_tokenize_name(value))


def _name_key_variants(value: str | None) -> set[str]:
    base = _normalized_name_key(value)
    if not base:
        return set()
    variants = {base}
    if " рф" in base:
        variants.add(base.replace(" рф", " российской федерации"))
    if " российской федерации" in base:
        variants.add(base.replace(" российской федерации", " рф"))
    return {variant.strip() for variant in variants if variant.strip()}


def _entity_specificity(entity_type: str, canonical_name: str) -> tuple[float, str | None]:
    tokens = _tokenize_name(canonical_name)
    if not tokens:
        return 0.15, "low_entity_specificity"
    if entity_type == "location":
        normalized = " ".join(tokens)
        if normalized in GENERIC_LOCATION_TOKENS or any(token in GENERIC_LOCATION_TOKENS for token in tokens):
            return 0.1, "low_entity_specificity"
        if len(tokens) == 1 and len(tokens[0]) <= 12:
            return 0.25, "low_entity_specificity"
        return 0.45, None
    if entity_type == "person":
        if len(tokens) >= 2:
            return 1.0, None
        token = tokens[0]
        if token in AMBIGUOUS_PERSON_TOKENS or len(token) <= 6:
            return 0.35, "low_entity_specificity"
        return 0.65, None
    if entity_type == "organization":
        if len("".join(tokens)) <= 4:
            return 0.45, "low_entity_specificity"
        return 0.9, None
    return 0.7, None


def _pair_entity_quality(
    entity_a_type: str,
    entity_a_name: str,
    entity_b_type: str,
    entity_b_name: str,
    *,
    bridge_types: set[str],
) -> tuple[float, str | None]:
    quality_a, reason_a = _entity_specificity(entity_a_type, entity_a_name)
    quality_b, reason_b = _entity_specificity(entity_b_type, entity_b_name)
    quality = round(min(quality_a, quality_b), 4)
    if quality >= 0.45:
        return quality, None
    if bridge_types & PROMOTION_BRIDGE_TYPES:
        return quality, None
    return quality, reason_a or reason_b or "low_entity_specificity"


def _load_set_map(conn, sql: str) -> dict[int, set[int]]:
    data: dict[int, set[int]] = defaultdict(set)
    for entity_id, value in conn.execute(sql).fetchall():
        if entity_id is None or value is None:
            continue
        data[int(entity_id)].add(int(value))
    return data


def _organization_entity_lookup(conn) -> dict[str, set[int]]:
    lookup: dict[str, set[int]] = defaultdict(set)
    if not _table_exists(conn, "entities"):
        return lookup
    for entity_id, entity_type, canonical_name in conn.execute(
        """
        SELECT id, entity_type, canonical_name
        FROM entities
        WHERE entity_type='organization' AND canonical_name IS NOT NULL AND TRIM(canonical_name) <> ''
        """
    ).fetchall():
        if entity_id is None:
            continue
        for key in _name_key_variants(canonical_name):
            lookup[key].add(int(entity_id))
    return lookup


def _resolve_organization_entity_id(organization_name: str | None, lookup: dict[str, set[int]]) -> int | None:
    candidates: set[int] = set()
    for key in _name_key_variants(organization_name):
        candidates.update(lookup.get(key, set()))
    if len(candidates) != 1:
        return None
    return next(iter(candidates))


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


def _load_direct_pair_map(conn, sql: str) -> dict[tuple[int, int], set[int]]:
    data: dict[tuple[int, int], set[int]] = defaultdict(set)
    for left_id, right_id, value in conn.execute(sql).fetchall():
        if left_id is None or right_id is None or value is None:
            continue
        entity_a = min(int(left_id), int(right_id))
        entity_b = max(int(left_id), int(right_id))
        if entity_a == entity_b:
            continue
        data[(entity_a, entity_b)].add(int(value))
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
        SELECT
            CASE WHEN em1.entity_id < em2.entity_id THEN em1.entity_id ELSE em2.entity_id END AS entity_a,
            CASE WHEN em1.entity_id < em2.entity_id THEN em2.entity_id ELSE em1.entity_id END AS entity_b,
            ci.id AS content_item_id,
            ci.source_id,
            COALESCE(NULLIF(ci.url, ''), NULLIF(s.url, ''), '') AS source_url,
            COALESCE(s.category, '') AS source_category,
            COALESCE(ci.content_type, '') AS content_type,
            COALESCE(ci.published_at, ci.collected_at, '') AS seen_at,
            COALESCE(ci.title, '') AS title,
            COALESCE(ci.body_text, '') AS body_text,
            cci.cluster_id,
            cc.canonical_content_id,
            COALESCE(cc.cluster_type, '') AS cluster_type,
            COALESCE(cci.is_canonical, 0) AS is_canonical,
            COALESCE(cc.item_count, 1) AS cluster_item_count
        FROM entity_mentions em1
        JOIN entity_mentions em2
          ON em1.content_item_id = em2.content_item_id
         AND em1.entity_id < em2.entity_id
        JOIN content_items ci ON ci.id = em1.content_item_id
        LEFT JOIN sources s ON s.id = ci.source_id
        LEFT JOIN content_cluster_items cci ON cci.content_item_id = ci.id
        LEFT JOIN content_clusters cc ON cc.id = cci.cluster_id
        WHERE COALESCE(ci.status, '') <> 'suppressed_template'
        ORDER BY
            entity_a,
            entity_b,
            COALESCE(cci.cluster_id, -ci.id),
            COALESCE(ci.source_id, -1),
            COALESCE(cci.is_canonical, 0) DESC,
            ci.id ASC
        """
    ).fetchall()
    grouped: dict[tuple[int, int, str, int | None], dict[str, Any]] = {}
    for row in rows:
        entity_a = int(row[0])
        entity_b = int(row[1])
        content_item_id = int(row[2])
        source_id = int(row[3]) if row[3] is not None else None
        cluster_id = int(row[10]) if row[10] is not None else None
        canonical_content_id = int(row[11]) if row[11] is not None else None
        support_unit = f"cluster:{cluster_id}" if cluster_id is not None else f"content:{content_item_id}"
        group_key = (entity_a, entity_b, support_unit, source_id)
        record = grouped.get(group_key)
        if record is None:
            grouped[group_key] = {
                "entity_a": entity_a,
                "entity_b": entity_b,
                "content_item_id": content_item_id,
                "source_id": source_id,
                "source_url": row[4],
                "source_category": row[5],
                "content_type": row[6],
                "seen_at": row[7],
                "title": row[8],
                "body_text": row[9],
                "cluster_id": cluster_id,
                "canonical_content_id": canonical_content_id,
                "cluster_type": row[12] or None,
                "cluster_item_count": int(row[14] or 1),
                "support_unit": support_unit,
                "duplicate_count": 1,
                "domain": _normalize_domain(row[4], source_id),
            }
        else:
            record["duplicate_count"] += 1
    return list(grouped.values())


def _load_content_context_map(conn) -> dict[int, dict[str, Any]]:
    if not _table_exists(conn, "content_items"):
        return {}
    rows = conn.execute(
        """
        SELECT
            ci.id AS content_item_id,
            ci.source_id,
            COALESCE(NULLIF(ci.url, ''), NULLIF(s.url, ''), '') AS source_url,
            COALESCE(s.category, '') AS source_category,
            COALESCE(ci.content_type, '') AS content_type,
            COALESCE(ci.published_at, ci.collected_at, '') AS seen_at,
            COALESCE(ci.title, '') AS title,
            COALESCE(ci.body_text, '') AS body_text,
            cci.cluster_id,
            cc.canonical_content_id,
            COALESCE(cc.cluster_type, '') AS cluster_type,
            COALESCE(cci.is_canonical, 0) AS is_canonical,
            COALESCE(cc.item_count, 1) AS cluster_item_count
        FROM content_items ci
        LEFT JOIN sources s ON s.id = ci.source_id
        LEFT JOIN content_cluster_items cci ON cci.content_item_id = ci.id
        LEFT JOIN content_clusters cc ON cc.id = cci.cluster_id
        """
    ).fetchall()
    data: dict[int, dict[str, Any]] = {}
    for row in rows:
        content_item_id = int(row[0])
        source_id = int(row[1]) if row[1] is not None else None
        cluster_id = int(row[8]) if row[8] is not None else None
        canonical_content_id = int(row[9]) if row[9] is not None else None
        data[content_item_id] = {
            "content_item_id": content_item_id,
            "source_id": source_id,
            "source_url": row[2],
            "source_category": row[3],
            "content_type": row[4],
            "seen_at": row[5],
            "title": row[6],
            "body_text": row[7],
            "cluster_id": cluster_id,
            "canonical_content_id": canonical_content_id,
            "cluster_type": row[10] or None,
            "is_canonical": bool(row[11]),
            "cluster_item_count": int(row[12] or 1),
        }
    return data


def _official_support_unit(bridge_kind: str, bridge_id: int, content_context: dict[str, Any] | None) -> str:
    if content_context:
        cluster_id = content_context.get("cluster_id")
        if cluster_id is not None:
            return f"cluster:{int(cluster_id)}"
        content_item_id = content_context.get("content_item_id")
        if content_item_id is not None:
            return f"content:{int(content_item_id)}"
    return f"{bridge_kind}:{int(bridge_id)}"


def _official_bridge_row(
    *,
    bridge_kind: str,
    bridge_id: int,
    entity_a: int,
    entity_b: int,
    default_content_type: str,
    source_content_id: int | None,
    source_url: str | None,
    source_category: str | None,
    seen_at: str | None,
    title: str | None,
    body_text: str | None,
    evidence_class: str | None,
    content_context: dict[str, Any] | None,
) -> dict[str, Any]:
    context = content_context or {}
    resolved_source_id = context.get("source_id")
    resolved_source_url = str(context.get("source_url") or source_url or "").strip()
    resolved_source_category = str(context.get("source_category") or source_category or "").strip()
    resolved_content_type = str(context.get("content_type") or default_content_type or "").strip()
    resolved_seen_at = str(context.get("seen_at") or seen_at or "").strip()
    resolved_title = str(context.get("title") or title or "").strip()
    resolved_body_text = str(context.get("body_text") or body_text or "").strip()
    cluster_id = context.get("cluster_id")
    canonical_content_id = context.get("canonical_content_id")
    duplicate_count = int(context.get("cluster_item_count") or 1)
    support_unit = _official_support_unit(bridge_kind, bridge_id, context if context else None)
    lowered_evidence_class = str(evidence_class or "").strip().lower()
    is_hard = bool(
        lowered_evidence_class == "hard"
        or resolved_source_category in OFFICIAL_SOURCE_CATEGORIES
        or resolved_content_type in HARD_CONTENT_TYPES
        or bridge_kind in {"restriction_event", "affiliation", "disclosure", "asset"}
    )
    return {
        "entity_a": int(entity_a),
        "entity_b": int(entity_b),
        "content_item_id": int(source_content_id) if source_content_id is not None else None,
        "source_id": int(resolved_source_id) if resolved_source_id is not None else None,
        "source_url": resolved_source_url,
        "source_category": resolved_source_category or "official_site",
        "content_type": resolved_content_type or default_content_type,
        "seen_at": resolved_seen_at,
        "title": resolved_title,
        "body_text": resolved_body_text,
        "cluster_id": int(cluster_id) if cluster_id is not None else None,
        "canonical_content_id": int(canonical_content_id) if canonical_content_id is not None else None,
        "cluster_type": context.get("cluster_type"),
        "cluster_item_count": duplicate_count,
        "support_unit": support_unit,
        "duplicate_count": max(1, duplicate_count),
        "domain": _normalize_domain(resolved_source_url, resolved_source_id),
        "bridge_kind": bridge_kind,
        "bridge_id": int(bridge_id),
        "bridge_support_unit": f"{bridge_kind}:{int(bridge_id)}",
        "is_official_bridge": True,
        "is_hard_evidence": is_hard,
        "evidence_class": lowered_evidence_class or None,
    }


def _append_bridge_support_row(
    target_map: dict[tuple[int, int], list[dict[str, Any]]],
    *,
    bridge_kind: str,
    bridge_id: int,
    entity_left: int | None,
    entity_right: int | None,
    default_content_type: str,
    source_content_id: int | None,
    source_url: str | None,
    source_category: str | None,
    seen_at: str | None,
    title: str | None,
    body_text: str | None,
    evidence_class: str | None,
    content_context_map: dict[int, dict[str, Any]],
) -> None:
    if entity_left is None or entity_right is None:
        return
    entity_a = min(int(entity_left), int(entity_right))
    entity_b = max(int(entity_left), int(entity_right))
    if entity_a == entity_b:
        return
    content_context = content_context_map.get(int(source_content_id)) if source_content_id is not None else None
    target_map[(entity_a, entity_b)].append(
        _official_bridge_row(
            bridge_kind=bridge_kind,
            bridge_id=int(bridge_id),
            entity_a=entity_a,
            entity_b=entity_b,
            default_content_type=default_content_type,
            source_content_id=int(source_content_id) if source_content_id is not None else None,
            source_url=source_url,
            source_category=source_category,
            seen_at=seen_at,
            title=title,
            body_text=body_text,
            evidence_class=evidence_class,
            content_context=content_context,
        )
    )


def _load_official_bridge_support_rows(
    conn,
    *,
    content_context_map: dict[int, dict[str, Any]],
    organization_lookup: dict[str, set[int]] | None = None,
) -> dict[tuple[int, int], list[dict[str, Any]]]:
    support_rows: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    if _table_exists(conn, "restriction_events"):
        for row in conn.execute(
            """
            SELECT
                id,
                issuer_entity_id,
                target_entity_id,
                source_content_id,
                COALESCE(source_url, '') AS source_url,
                COALESCE(event_date, '') AS seen_at,
                COALESCE(restriction_type, '') AS title,
                COALESCE(stated_justification, '') AS body_text,
                COALESCE(evidence_class, 'support') AS evidence_class
            FROM restriction_events
            WHERE issuer_entity_id IS NOT NULL AND target_entity_id IS NOT NULL
            """
        ).fetchall():
            _append_bridge_support_row(
                support_rows,
                bridge_kind="restriction_event",
                bridge_id=int(row[0]),
                entity_left=row[1],
                entity_right=row[2],
                default_content_type="restriction_record",
                source_content_id=int(row[3]) if row[3] is not None else None,
                source_url=row[4],
                source_category="official_site",
                seen_at=row[5],
                title=row[6],
                body_text=row[7],
                evidence_class=row[8],
                content_context_map=content_context_map,
            )
    if _table_exists(conn, "company_affiliations"):
        for row in conn.execute(
            """
            SELECT
                id,
                entity_id,
                company_entity_id,
                source_content_id,
                COALESCE(source_url, '') AS source_url,
                COALESCE(period_end, period_start, '') AS seen_at,
                COALESCE(role_title, role_type, company_name, '') AS title,
                COALESCE(metadata_json, '') AS body_text,
                COALESCE(evidence_class, 'support') AS evidence_class
            FROM company_affiliations
            WHERE entity_id IS NOT NULL AND company_entity_id IS NOT NULL
            """
        ).fetchall():
            _append_bridge_support_row(
                support_rows,
                bridge_kind="affiliation",
                bridge_id=int(row[0]),
                entity_left=row[1],
                entity_right=row[2],
                default_content_type="official_document",
                source_content_id=int(row[3]) if row[3] is not None else None,
                source_url=row[4],
                source_category="official_registry",
                seen_at=row[5],
                title=row[6],
                body_text=row[7],
                evidence_class=row[8],
                content_context_map=content_context_map,
            )
    if _table_exists(conn, "compensation_facts"):
        for row in conn.execute(
            """
            SELECT
                id,
                entity_id,
                employer_entity_id,
                source_content_id,
                COALESCE(source_url, '') AS source_url,
                COALESCE(compensation_year, '') AS seen_at,
                COALESCE(role_title, amount_text, '') AS title,
                COALESCE(metadata_json, '') AS body_text,
                COALESCE(evidence_class, 'support') AS evidence_class
            FROM compensation_facts
            WHERE entity_id IS NOT NULL AND employer_entity_id IS NOT NULL
            """
        ).fetchall():
            _append_bridge_support_row(
                support_rows,
                bridge_kind="disclosure",
                bridge_id=int(row[0]),
                entity_left=row[1],
                entity_right=row[2],
                default_content_type="declaration",
                source_content_id=int(row[3]) if row[3] is not None else None,
                source_url=row[4],
                source_category="official_site",
                seen_at=str(row[5]) if row[5] is not None else "",
                title=row[6],
                body_text=row[7],
                evidence_class=row[8],
                content_context_map=content_context_map,
            )
    org_lookup = organization_lookup or {}
    if org_lookup and _table_exists(conn, "person_disclosures") and _table_exists(conn, "official_positions"):
        seen_pairs: set[tuple[int, int]] = set()
        for row in conn.execute(
            """
            SELECT
                pd.id,
                pd.entity_id,
                pd.source_content_id,
                COALESCE(pd.source_url, '') AS source_url,
                COALESCE(pd.disclosure_year, '') AS seen_at,
                COALESCE(pd.raw_income_text, '') AS title,
                COALESCE(pd.metadata_json, '') AS body_text,
                COALESCE(pd.evidence_class, 'support') AS evidence_class,
                COALESCE(pd.source_type, '') AS source_type,
                COALESCE(op.organization, '') AS organization_name
            FROM person_disclosures pd
            JOIN official_positions op ON op.entity_id = pd.entity_id
            WHERE pd.entity_id IS NOT NULL
              AND op.organization IS NOT NULL
              AND TRIM(op.organization) <> ''
            ORDER BY pd.id, CASE COALESCE(op.is_active, 0) WHEN 1 THEN 0 ELSE 1 END, op.id DESC
            """
        ).fetchall():
            disclosure_id = int(row[0])
            organization_entity_id = _resolve_organization_entity_id(row[9], org_lookup)
            if organization_entity_id is None:
                continue
            dedupe_key = (disclosure_id, int(organization_entity_id))
            if dedupe_key in seen_pairs:
                continue
            seen_pairs.add(dedupe_key)
            source_type = str(row[8] or "").strip().lower()
            source_category = "official_site" if source_type.startswith("official") else "official_site"
            _append_bridge_support_row(
                support_rows,
                bridge_kind="disclosure",
                bridge_id=disclosure_id,
                entity_left=row[1],
                entity_right=organization_entity_id,
                default_content_type="declaration",
                source_content_id=int(row[2]) if row[2] is not None else None,
                source_url=row[3],
                source_category=source_category,
                seen_at=str(row[4]) if row[4] is not None else "",
                title=row[5] or f"Disclosure #{disclosure_id}",
                body_text=row[6],
                evidence_class=row[7],
                content_context_map=content_context_map,
            )
    return support_rows


def _merge_membership_map(target: dict[int, set[int]], source: dict[int, set[int]]) -> dict[int, set[int]]:
    for entity_id, values in source.items():
        target[int(entity_id)].update(int(value) for value in values)
    return target


def _merge_pair_map(target: dict[tuple[int, int], set[int]], source: dict[tuple[int, int], set[int]]) -> dict[tuple[int, int], set[int]]:
    for pair, values in source.items():
        entity_a, entity_b = pair
        target[(int(entity_a), int(entity_b))].update(int(value) for value in values)
    return target


def _bridge_rows_to_maps(
    rows_map: dict[tuple[int, int], list[dict[str, Any]]],
    *,
    bridge_kind: str,
) -> tuple[dict[int, set[int]], dict[tuple[int, int], set[int]]]:
    membership_map: dict[int, set[int]] = defaultdict(set)
    pair_map: dict[tuple[int, int], set[int]] = defaultdict(set)
    for pair, rows in rows_map.items():
        entity_a, entity_b = pair
        for row in rows:
            if row.get("bridge_kind") != bridge_kind:
                continue
            bridge_id = row.get("bridge_id")
            if bridge_id is None:
                continue
            bridge_id_int = int(bridge_id)
            membership_map[int(entity_a)].add(bridge_id_int)
            membership_map[int(entity_b)].add(bridge_id_int)
            pair_map[(int(entity_a), int(entity_b))].add(bridge_id_int)
    return membership_map, pair_map


def _has_existing_non_candidate_relation(conn, entity_a: int, entity_b: int) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM entity_relations
        WHERE COALESCE(detected_by, '') NOT LIKE 'relation_candidate:%'
          AND (
            (from_entity_id=? AND to_entity_id=?)
            OR
            (from_entity_id=? AND to_entity_id=?)
          )
        LIMIT 1
        """,
        (entity_a, entity_b, entity_b, entity_a),
    ).fetchone()
    return bool(row)


def _merge_support_rows(
    shared_rows: list[dict[str, Any]],
    official_bridge_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, int | None, str | None], dict[str, Any]] = {}

    def upsert(row: dict[str, Any]) -> None:
        key = (
            str(row.get("support_unit") or f"content:{row.get('content_item_id') or 0}"),
            int(row["source_id"]) if row.get("source_id") is not None else None,
            str(row.get("domain") or "") or None,
        )
        existing = merged.get(key)
        if existing is None:
            cloned = dict(row)
            cloned["_bridge_types"] = set()
            cloned["_bridge_units"] = set()
            if row.get("bridge_kind"):
                cloned["_bridge_types"].add(str(row["bridge_kind"]))
                cloned["_bridge_units"].add(str(row.get("bridge_support_unit") or f"{row['bridge_kind']}:{row.get('bridge_id')}"))
            merged[key] = cloned
            return
        existing["duplicate_count"] = max(
            int(existing.get("duplicate_count") or 1),
            int(row.get("duplicate_count") or 1),
        )
        if row.get("is_official_bridge"):
            existing["is_official_bridge"] = True
        if row.get("is_hard_evidence"):
            existing["is_hard_evidence"] = True
        if row.get("bridge_kind"):
            existing["_bridge_types"].add(str(row["bridge_kind"]))
            existing["_bridge_units"].add(str(row.get("bridge_support_unit") or f"{row['bridge_kind']}:{row.get('bridge_id')}"))
        for field in ("content_type", "source_category", "source_url", "seen_at", "title", "body_text"):
            if not existing.get(field) and row.get(field):
                existing[field] = row[field]
        if existing.get("content_item_id") is None and row.get("content_item_id") is not None:
            existing["content_item_id"] = row["content_item_id"]
        if existing.get("canonical_content_id") is None and row.get("canonical_content_id") is not None:
            existing["canonical_content_id"] = row["canonical_content_id"]
        if existing.get("cluster_id") is None and row.get("cluster_id") is not None:
            existing["cluster_id"] = row["cluster_id"]

    for row in shared_rows:
        upsert(row)
    for row in official_bridge_rows:
        upsert(row)

    result: list[dict[str, Any]] = []
    for row in merged.values():
        row["bridge_types"] = sorted(row.pop("_bridge_types", set()))
        row["bridge_units"] = sorted(row.pop("_bridge_units", set()))
        result.append(row)
    return result


def _load_tag_map(conn) -> dict[int, set[str]]:
    if not _table_exists(conn, "content_tags"):
        return defaultdict(set)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(content_tags)").fetchall()}
    params: list[Any] = []
    predicate = ""
    if "decision_source" in columns:
        has_v3 = conn.execute(
            "SELECT 1 FROM content_tags WHERE COALESCE(decision_source, '')='classifier_v3' LIMIT 1"
        ).fetchone()
        if has_v3:
            predicate = "WHERE COALESCE(decision_source, '')='classifier_v3'"
    data: dict[int, set[str]] = defaultdict(set)
    for content_item_id, tag_name in conn.execute(
        f"SELECT content_item_id, tag_name FROM content_tags {predicate}",
        params,
    ).fetchall():
        data[int(content_item_id)].add(str(tag_name or ""))
    return data


def _load_claim_cluster_map(conn) -> dict[int, set[int]]:
    if not _table_exists(conn, "claims"):
        return defaultdict(set)
    columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(claims)").fetchall()
    }
    if "claim_cluster_id" not in columns:
        return defaultdict(set)
    data: dict[int, set[int]] = defaultdict(set)
    for entity_id, cluster_id in conn.execute(
        """
        SELECT DISTINCT em.entity_id, cl.claim_cluster_id
        FROM claims cl
        JOIN entity_mentions em ON em.content_item_id = cl.content_item_id
        WHERE cl.claim_cluster_id IS NOT NULL
        """
    ).fetchall():
        if entity_id is None or cluster_id is None:
            continue
        data[int(entity_id)].add(int(cluster_id))
    return data


def _load_entity_semantic_scores(conn) -> dict[tuple[int, int], float]:
    if not _table_exists(conn, "semantic_neighbors"):
        return {}
    scores: dict[tuple[int, int], float] = {}
    for source_id, neighbor_id, score in conn.execute(
        """
        SELECT source_id, neighbor_id, score
        FROM semantic_neighbors
        WHERE source_kind='entity'
          AND neighbor_kind='entity'
        """
    ).fetchall():
        if source_id is None or neighbor_id is None:
            continue
        entity_a = min(int(source_id), int(neighbor_id))
        entity_b = max(int(source_id), int(neighbor_id))
        pair = (entity_a, entity_b)
        scores[pair] = max(float(score or 0.0), scores.get(pair, 0.0))
    return scores


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
    support_items = len(shared_rows)
    source_ids = {row["source_id"] for row in shared_rows if row["source_id"] is not None}
    support_sources = len(source_ids)
    domains = {row["domain"] for row in shared_rows if row.get("domain")}
    support_domains = len(domains)
    categories = {row["source_category"] for row in shared_rows if row["source_category"]}
    support_categories = len(categories)
    dates = [value for value in (_parse_date(row["seen_at"]) for row in shared_rows) if value is not None]
    specific_tags = _specific_tags(shared_tags)
    duplicate_count_total = sum(max(1, int(row.get("duplicate_count") or 1)) for row in shared_rows)
    cluster_ids = {int(row["cluster_id"]) for row in shared_rows if row.get("cluster_id") is not None}
    content_types = {str(row.get("content_type") or "") for row in shared_rows if row.get("content_type")}
    avg_len = statistics.mean(
        max(0, len((row.get("title") or "").strip()) + len((row.get("body_text") or "").strip()))
        for row in shared_rows
    ) if shared_rows else 0.0

    source_independence = min(1.0, 0.45 * min(1.0, support_sources / 4.0) + 0.55 * min(1.0, support_domains / 3.0))
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
        "duplicate_count_total": duplicate_count_total,
        "cluster_ids": sorted(cluster_ids),
        "content_types": sorted(content_types),
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


def _structural_score(
    *,
    structural_seed_kind: str | None,
    candidate_type: str,
    bill_overlap: int,
    contract_overlap: int,
    case_overlap: int,
    risk_overlap: int,
    vote_overlap_count: int,
    vote_overlap_ratio: float,
) -> float:
    if structural_seed_kind == "vote" and candidate_type == "same_vote_pattern":
        return round(min(1.0, 0.65 + min(0.2, vote_overlap_count / 100.0) + min(0.15, vote_overlap_ratio * 0.15)), 4)
    if structural_seed_kind == "bill" and candidate_type == "same_bill_cluster":
        return round(min(1.0, 0.35 + min(0.3, bill_overlap * 0.1)), 4)
    if structural_seed_kind == "contract" and candidate_type == "same_contract_cluster":
        return round(min(1.0, 0.40 + min(0.25, contract_overlap * 0.15)), 4)
    if structural_seed_kind == "case" and candidate_type == "same_case_cluster":
        return round(min(1.0, 0.35 + min(0.2, case_overlap * 0.08) + min(0.2, risk_overlap * 0.08)), 4)
    if structural_seed_kind == "restriction":
        return 0.55
    if structural_seed_kind == "affiliation":
        return 0.5
    if structural_seed_kind == "disclosure":
        return 0.45
    return 0.0


def _candidate_state(
    *,
    candidate_type: str,
    structural_seed_kind: str | None,
    support_items: int,
    support_sources: int,
    support_domains: int,
    support_claim_cluster_count: int,
    support_hard_evidence_count: int,
    semantic_support_score: float,
    calibrated_score: float,
    explain_path: list[dict[str, Any]],
    shortest_bridge_path: list[dict[str, Any]],
    promotion_block_reason: str | None,
) -> str:
    has_evidence_support = any(
        (
            support_items > 0,
            support_sources > 0,
            support_domains > 0,
            support_hard_evidence_count > 0,
        )
    )
    has_support_layer = any(
        (
            has_evidence_support,
            support_claim_cluster_count > 0,
            semantic_support_score >= 0.35,
        )
    )
    bridge_types = {
        str(node.get("node_type") or "")
        for node in (shortest_bridge_path or [])
    } | {
        str(node.get("node_type") or "")
        for node in (explain_path or [])
    }
    has_nonseed_bridge = bool(bridge_types & PROMOTION_BRIDGE_TYPES)
    has_official_bridge = bool(bridge_types & OFFICIAL_PROMOTION_BRIDGE_TYPES)
    if structural_seed_kind and not has_evidence_support:
        return "seed_only"
    if promotion_block_reason:
        if promotion_block_reason == "low_entity_specificity":
            return "seed_only" if structural_seed_kind else "pending"
        if promotion_block_reason in {
            "same_case_requires_nonseed_bridge",
            "same_case_requires_evidence_bridge",
            "official_bridge_missing",
        }:
            return "seed_only" if structural_seed_kind else "pending"
        if has_evidence_support or (not structural_seed_kind and has_support_layer):
            return "review"
        if structural_seed_kind:
            return "seed_only"
        return "pending"
    if candidate_type == "same_case_cluster" and not has_nonseed_bridge:
        return "seed_only" if structural_seed_kind else "pending"
    if (
        explain_path
        and shortest_bridge_path
        and calibrated_score >= PROMOTION_SCORE_THRESHOLD
        and has_nonseed_bridge
        and (
            (support_items >= 1 and support_sources >= 2 and support_domains >= 2)
            or (
                candidate_type != "same_case_cluster"
                and support_hard_evidence_count >= 1
                and support_items >= 1
                and support_domains >= 1
                and has_official_bridge
            )
        )
    ):
        return "promoted"
    if candidate_type == "same_case_cluster" and has_evidence_support:
        return "review"
    if has_evidence_support or (not structural_seed_kind and has_support_layer):
        return "review"
    if structural_seed_kind:
        return "seed_only"
    return "pending"


def _build_explain_path(
    *,
    shared_content_ids: list[int],
    shared_claim_cluster_ids: list[int],
    shared_case_ids: list[int],
    shared_bill_ids: list[int],
    shared_contract_ids: list[int],
    shared_risk_ids: list[int],
    shared_restriction_ids: list[int],
    shared_affiliation_ids: list[int],
    shared_disclosure_ids: list[int],
    shared_asset_ids: list[int],
    shared_official_document_ids: list[int],
    shared_vote_count: int,
    entity_semantic_score: float,
) -> list[dict[str, Any]]:
    path: list[dict[str, Any]] = []
    if shared_claim_cluster_ids:
        path.append({"node_type": "ClaimCluster", "ids": shared_claim_cluster_ids[:5]})
    if shared_content_ids:
        path.append({"node_type": "Content", "ids": shared_content_ids[:5]})
    if shared_case_ids:
        path.append({"node_type": "Case", "ids": shared_case_ids[:5]})
    if shared_bill_ids:
        path.append({"node_type": "Bill", "ids": shared_bill_ids[:5]})
    if shared_contract_ids:
        path.append({"node_type": "Contract", "ids": shared_contract_ids[:5]})
    if shared_risk_ids:
        path.append({"node_type": "Risk", "ids": shared_risk_ids[:5]})
    if shared_restriction_ids:
        path.append({"node_type": "RestrictionEvent", "ids": shared_restriction_ids[:5]})
    if shared_affiliation_ids:
        path.append({"node_type": "Affiliation", "ids": shared_affiliation_ids[:5]})
    if shared_disclosure_ids:
        path.append({"node_type": "Disclosure", "ids": shared_disclosure_ids[:5]})
    if shared_asset_ids:
        path.append({"node_type": "Asset", "ids": shared_asset_ids[:5]})
    if shared_official_document_ids:
        path.append({"node_type": "OfficialDocument", "ids": shared_official_document_ids[:5]})
    if shared_vote_count:
        path.append({"node_type": "VotePattern", "same_vote_count": int(shared_vote_count)})
    if entity_semantic_score >= 0.35:
        path.append({"node_type": "SemanticNeighbor", "score": round(entity_semantic_score, 4)})
    return path


def _build_shortest_bridge_path(
    *,
    entity_a: int,
    entity_b: int,
    shared_content_rows: list[dict[str, Any]],
    shared_claim_cluster_ids: list[int],
    shared_case_ids: list[int],
    shared_bill_ids: list[int],
    shared_contract_ids: list[int],
    shared_risk_ids: list[int],
    shared_restriction_ids: list[int],
    shared_affiliation_ids: list[int],
    shared_disclosure_ids: list[int],
    shared_asset_ids: list[int],
    shared_vote_count: int,
    entity_semantic_score: float,
) -> list[dict[str, Any]]:
    graph = nx.Graph()
    node_a = ("Entity", int(entity_a))
    node_b = ("Entity", int(entity_b))
    graph.add_nodes_from((node_a, node_b))

    def add_bridge(node_type: str, node_id: int | str, weight: float) -> None:
        bridge = (node_type, node_id)
        graph.add_edge(node_a, bridge, weight=weight)
        graph.add_edge(bridge, node_b, weight=weight)

    for claim_cluster_id in shared_claim_cluster_ids[:6]:
        add_bridge("ClaimCluster", int(claim_cluster_id), 1.35)
    for row in shared_content_rows[:12]:
        if row.get("content_item_id") is None:
            continue
        content_item_id = int(row["content_item_id"])
        is_hard = (
            (row.get("source_category") in OFFICIAL_SOURCE_CATEGORIES)
            or (str(row.get("content_type") or "") in HARD_CONTENT_TYPES)
        )
        add_bridge("Content", content_item_id, 1.75 if not is_hard else 1.45)
        if is_hard:
            add_bridge("OfficialDocument", content_item_id, 1.0)
    for case_id in shared_case_ids[:6]:
        add_bridge("Case", int(case_id), 1.8)
    for bill_id in shared_bill_ids[:6]:
        add_bridge("Bill", int(bill_id), 0.95)
    for contract_id in shared_contract_ids[:6]:
        add_bridge("Contract", int(contract_id), 0.9)
    for risk_id in shared_risk_ids[:6]:
        add_bridge("Risk", int(risk_id), 1.6)
    for restriction_id in shared_restriction_ids[:6]:
        add_bridge("RestrictionEvent", int(restriction_id), 0.85)
    for affiliation_id in shared_affiliation_ids[:6]:
        add_bridge("Affiliation", int(affiliation_id), 0.95)
    for disclosure_id in shared_disclosure_ids[:6]:
        add_bridge("Disclosure", int(disclosure_id), 1.0)
    for asset_id in shared_asset_ids[:6]:
        add_bridge("Asset", int(asset_id), 1.05)
    if shared_vote_count:
        add_bridge("VotePattern", int(shared_vote_count), 0.95)
    if entity_semantic_score >= 0.35:
        add_bridge("SemanticNeighbor", "entity", 1.7)

    try:
        path_nodes = nx.shortest_path(graph, node_a, node_b, weight="weight")
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        return []
    result: list[dict[str, Any]] = []
    for node_type, node_id in path_nodes[1:-1]:
        payload: dict[str, Any] = {"node_type": node_type}
        if isinstance(node_id, int):
            payload["ids"] = [node_id]
        else:
            payload["value"] = node_id
        result.append(payload)
    return result


def _node_similarity_score(bridge_membership_a: set[tuple[str, int]], bridge_membership_b: set[tuple[str, int]]) -> float:
    if not bridge_membership_a and not bridge_membership_b:
        return 0.0
    union = bridge_membership_a | bridge_membership_b
    if not union:
        return 0.0
    return round(len(bridge_membership_a & bridge_membership_b) / len(union), 4)


def _community_membership(
    bridge_memberships: dict[str, dict[int, set[int]]],
    candidate_entities: set[int],
) -> dict[int, int]:
    graph = nx.Graph()
    for bridge_type, membership_map in bridge_memberships.items():
        for entity_id, bridge_ids in membership_map.items():
            if entity_id not in candidate_entities:
                continue
            entity_node = ("Entity", int(entity_id))
            graph.add_node(entity_node)
            for bridge_id in bridge_ids:
                bridge_node = (bridge_type, int(bridge_id))
                graph.add_edge(entity_node, bridge_node)
    if graph.number_of_nodes() == 0:
        return {}
    if graph.number_of_nodes() > 15000:
        return {}
    communities = list(nx.algorithms.community.greedy_modularity_communities(graph))
    community_map: dict[int, int] = {}
    for index, community in enumerate(communities):
        for node_type, node_id in community:
            if node_type == "Entity":
                community_map[int(node_id)] = index
    return community_map


def _candidate_type(
    *,
    bill_overlap: int,
    contract_overlap: int,
    case_overlap: int,
    risk_overlap: int,
    vote_overlap_count: int,
    vote_overlap_ratio: float,
    structural_seed_kind: str | None,
    shared_claims: int,
) -> str:
    if vote_overlap_count >= 20 and vote_overlap_ratio >= 0.75:
        return "same_vote_pattern"
    if contract_overlap >= 1:
        return "same_contract_cluster"
    if structural_seed_kind == "case" and (case_overlap >= 1 or risk_overlap >= 1):
        return "same_case_cluster"
    if case_overlap >= 2 or risk_overlap >= 2 or (case_overlap >= 1 and shared_claims >= 3):
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
    if _table_exists(conn, "relation_features"):
        conn.execute("DELETE FROM relation_features")
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

        entity_records = {
            int(row[0]): {
                "type": str(row[1] or ""),
                "name": str(row[2] or ""),
            }
            for row in conn.execute("SELECT id, entity_type, canonical_name FROM entities").fetchall()
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
        bill_map = _load_set_map(
            conn,
            "SELECT entity_id, bill_id FROM bill_sponsors WHERE entity_id IS NOT NULL",
        ) if _table_exists(conn, "bill_sponsors") else defaultdict(set)
        contract_map = _load_set_map(
            conn,
            "SELECT entity_id, contract_id FROM contract_parties WHERE entity_id IS NOT NULL",
        ) if _table_exists(conn, "contract_parties") else defaultdict(set)
        risk_map = _load_risk_map(conn)
        claim_cluster_map = _load_claim_cluster_map(conn)
        entity_semantic_scores = _load_entity_semantic_scores(conn)
        vote_map = _load_vote_map(conn)
        restriction_map = _load_set_map(
            conn,
            """
            SELECT issuer_entity_id AS entity_id, id FROM restriction_events WHERE issuer_entity_id IS NOT NULL
            UNION ALL
            SELECT target_entity_id AS entity_id, id FROM restriction_events WHERE target_entity_id IS NOT NULL
            """,
        ) if _table_exists(conn, "restriction_events") else defaultdict(set)
        restriction_pair_map = _load_direct_pair_map(
            conn,
            """
            SELECT issuer_entity_id, target_entity_id, id
            FROM restriction_events
            WHERE issuer_entity_id IS NOT NULL AND target_entity_id IS NOT NULL
            """,
        ) if _table_exists(conn, "restriction_events") else defaultdict(set)
        affiliation_map = _load_set_map(
            conn,
            """
            SELECT entity_id, id FROM company_affiliations WHERE entity_id IS NOT NULL
            UNION ALL
            SELECT company_entity_id, id FROM company_affiliations WHERE company_entity_id IS NOT NULL
            """,
        ) if _table_exists(conn, "company_affiliations") else defaultdict(set)
        affiliation_pair_map = _load_direct_pair_map(
            conn,
            """
            SELECT entity_id, company_entity_id, id
            FROM company_affiliations
            WHERE entity_id IS NOT NULL AND company_entity_id IS NOT NULL
            """,
        ) if _table_exists(conn, "company_affiliations") else defaultdict(set)
        disclosure_map = _load_set_map(
            conn,
            """
            SELECT entity_id, id FROM compensation_facts WHERE entity_id IS NOT NULL
            UNION ALL
            SELECT employer_entity_id, id FROM compensation_facts WHERE employer_entity_id IS NOT NULL
            """,
        ) if _table_exists(conn, "compensation_facts") else defaultdict(set)
        disclosure_pair_map = _load_direct_pair_map(
            conn,
            """
            SELECT entity_id, employer_entity_id, id
            FROM compensation_facts
            WHERE entity_id IS NOT NULL AND employer_entity_id IS NOT NULL
            """,
        ) if _table_exists(conn, "compensation_facts") else defaultdict(set)
        asset_map = _load_set_map(
            conn,
            "SELECT entity_id, id FROM declared_assets WHERE entity_id IS NOT NULL",
        ) if _table_exists(conn, "declared_assets") else defaultdict(set)

        content_context_map = _load_content_context_map(conn)
        organization_lookup = _organization_entity_lookup(conn)
        pair_rows = defaultdict(list)
        for record in _shared_content_rows(conn):
            pair_rows[(record["entity_a"], record["entity_b"])].append(record)
        official_bridge_support_map = _load_official_bridge_support_rows(
            conn,
            content_context_map=content_context_map,
            organization_lookup=organization_lookup,
        )
        disclosure_bridge_membership_map, disclosure_bridge_pair_map = _bridge_rows_to_maps(
            official_bridge_support_map,
            bridge_kind="disclosure",
        )
        disclosure_map = _merge_membership_map(disclosure_map, disclosure_bridge_membership_map)
        disclosure_pair_map = _merge_pair_map(disclosure_pair_map, disclosure_bridge_pair_map)
        structural_seed_pairs = _pairs_from_membership_map(contract_map, max_group_size=6)
        bill_seed_pairs = _pairs_from_membership_map(bill_map, max_group_size=24, min_shared=3)
        case_seed_pairs = _pairs_from_membership_map(case_map, max_group_size=16, min_shared=1)
        risk_seed_pairs = _pairs_from_membership_map(risk_map, max_group_size=12, min_shared=1)
        vote_seed_pairs = _vote_seed_pairs(
            vote_map,
            eligible_entities={entity_id for entity_id, entity in entity_records.items() if entity["type"] == "person"},
        )
        pair_keys = set(pair_rows)
        pair_keys.update(structural_seed_pairs)
        pair_keys.update(bill_seed_pairs)
        pair_keys.update(case_seed_pairs)
        pair_keys.update(risk_seed_pairs)
        pair_keys.update(vote_seed_pairs)
        pair_keys.update(restriction_pair_map)
        pair_keys.update(affiliation_pair_map)
        pair_keys.update(disclosure_pair_map)

        bridge_memberships = {
            "Case": case_map,
            "Bill": bill_map,
            "Contract": contract_map,
            "Risk": risk_map,
            "RestrictionEvent": restriction_map,
            "Affiliation": affiliation_map,
            "Disclosure": disclosure_map,
            "Asset": asset_map,
        }
        community_map = _community_membership(
            bridge_memberships,
            {entity_id for pair in pair_keys for entity_id in pair},
        )

        created = 0
        support_rows_created = 0
        skipped_low_support = 0
        promoted_candidates = 0

        for entity_a, entity_b in sorted(pair_keys):
            shared_rows = pair_rows.get((entity_a, entity_b), [])
            official_bridge_rows = official_bridge_support_map.get((entity_a, entity_b), [])
            support_rows = _merge_support_rows(shared_rows, official_bridge_rows)
            structural_seed_kind = None
            if (entity_a, entity_b) in vote_seed_pairs:
                structural_seed_kind = "vote"
            elif (entity_a, entity_b) in restriction_pair_map:
                structural_seed_kind = "restriction"
            elif (entity_a, entity_b) in affiliation_pair_map:
                structural_seed_kind = "affiliation"
            elif (entity_a, entity_b) in disclosure_pair_map:
                structural_seed_kind = "disclosure"
            elif (entity_a, entity_b) in bill_seed_pairs:
                structural_seed_kind = "bill"
            elif (entity_a, entity_b) in case_seed_pairs or (entity_a, entity_b) in risk_seed_pairs:
                structural_seed_kind = "case"
            elif (entity_a, entity_b) in structural_seed_pairs:
                structural_seed_kind = "contract"

            entity_a_type = entity_records.get(entity_a, {}).get("type", "entity")
            entity_b_type = entity_records.get(entity_b, {}).get("type", "entity")
            entity_a_name = entity_records.get(entity_a, {}).get("name", "")
            entity_b_name = entity_records.get(entity_b, {}).get("name", "")
            shared_content_ids = sorted(
                {
                    int(row["content_item_id"])
                    for row in support_rows
                    if row.get("content_item_id") is not None
                }
            )
            shared_tags = set()
            for content_id in shared_content_ids:
                shared_tags.update(tag_map.get(content_id, set()))

            shared_case_ids = sorted(case_map.get(entity_a, set()) & case_map.get(entity_b, set()))
            shared_bill_ids = sorted(bill_map.get(entity_a, set()) & bill_map.get(entity_b, set()))
            shared_contract_ids = sorted(contract_map.get(entity_a, set()) & contract_map.get(entity_b, set()))
            shared_risk_ids = sorted(risk_map.get(entity_a, set()) & risk_map.get(entity_b, set()))
            shared_restriction_ids = sorted(restriction_pair_map.get((entity_a, entity_b), set()))
            shared_affiliation_ids = sorted(affiliation_pair_map.get((entity_a, entity_b), set()))
            shared_disclosure_ids = sorted(disclosure_pair_map.get((entity_a, entity_b), set()))
            shared_asset_ids = sorted(asset_map.get(entity_a, set()) & asset_map.get(entity_b, set()))

            case_overlap = len(shared_case_ids)
            bill_overlap = len(shared_bill_ids)
            contract_overlap = len(shared_contract_ids)
            risk_overlap = len(shared_risk_ids)

            support_items = len(support_rows)
            support_sources = len({row["source_id"] for row in support_rows if row["source_id"] is not None})
            support_domains = len({row["domain"] for row in support_rows if row.get("domain")})
            has_structural_signal = bool(structural_seed_kind)
            if (
                support_items < MIN_SUPPORT_ITEMS
                or support_sources < MIN_SUPPORT_SOURCES
                or support_domains < MIN_SUPPORT_DOMAINS
            ):
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
            shared_claim_cluster_ids = sorted(claim_cluster_map.get(entity_a, set()) & claim_cluster_map.get(entity_b, set()))
            support_claim_cluster_count = len(shared_claim_cluster_ids)
            entity_semantic_score = float(entity_semantic_scores.get((entity_a, entity_b), 0.0))
            shared_official_document_ids = sorted(
                {
                    int(row["content_item_id"])
                    for row in support_rows
                    if row.get("content_item_id") is not None
                    and (
                        row.get("source_category") in OFFICIAL_SOURCE_CATEGORIES
                        or str(row.get("content_type") or "") in HARD_CONTENT_TYPES
                        or bool(row.get("is_official_bridge"))
                    )
                }
            )

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
                support_rows,
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
                structural_seed_kind=structural_seed_kind,
                shared_claims=shared_claims,
            )
            bridge_membership_a = {
                (bridge_type, int(value))
                for bridge_type, membership_map in bridge_memberships.items()
                for value in membership_map.get(entity_a, set())
            }
            bridge_membership_b = {
                (bridge_type, int(value))
                for bridge_type, membership_map in bridge_memberships.items()
                for value in membership_map.get(entity_b, set())
            }
            node_similarity_score = _node_similarity_score(bridge_membership_a, bridge_membership_b)
            same_community_score = 1.0 if community_map.get(entity_a) is not None and community_map.get(entity_a) == community_map.get(entity_b) else 0.0
            structural_score = _structural_score(
                structural_seed_kind=structural_seed_kind,
                candidate_type=candidate_type,
                bill_overlap=bill_overlap,
                contract_overlap=contract_overlap,
                case_overlap=case_overlap,
                risk_overlap=risk_overlap,
                vote_overlap_count=same_votes,
                vote_overlap_ratio=vote_overlap_ratio,
            )
            content_support_score = round(
                min(
                    1.0,
                    0.4 * min(1.0, metrics["support_items"] / 3.0)
                    + 0.35 * min(1.0, metrics["support_sources"] / 2.0)
                    + 0.25 * min(1.0, metrics["support_domains"] / 2.0),
                ),
                4,
            )
            real_host_diversity_score = round(min(1.0, metrics["support_domains"] / 2.0), 4)
            source_diversity_score = round(
                min(1.0, 0.35 * min(1.0, metrics["support_sources"] / 2.0) + 0.65 * real_host_diversity_score),
                4,
            )
            semantic_support_score = round(
                max(
                    min(1.0, support_claim_cluster_count / 3.0),
                    entity_semantic_score,
                    min(1.0, 0.65 * node_similarity_score + 0.35 * same_community_score),
                ),
                4,
            )
            evidence_quality_score = round(
                min(
                    1.0,
                    (
                        0.65
                        * min(
                            1.0,
                            len(shared_official_document_ids) / max(1, metrics["support_items"]),
                        )
                    )
                    + (
                        0.35
                        * min(
                            1.0,
                            len({row["source_category"] for row in support_rows if row.get("source_category") in OFFICIAL_SOURCE_CATEGORIES}) / 2.0,
                        )
                    ),
                ),
                4,
            )
            support_hard_evidence_count = len(
                {
                    row["support_unit"]
                    for row in support_rows
                    if (
                        row.get("is_hard_evidence")
                        or row.get("source_category") in OFFICIAL_SOURCE_CATEGORIES
                        or str(row.get("content_type") or "") in HARD_CONTENT_TYPES
                    )
                }
            )
            dedupe_support_score = round(
                min(
                    1.0,
                    metrics["support_items"] / max(1, metrics["duplicate_count_total"]),
                ),
                4,
            )
            explain_path = _build_explain_path(
                shared_content_ids=shared_content_ids,
                shared_claim_cluster_ids=shared_claim_cluster_ids,
                shared_case_ids=shared_case_ids,
                shared_bill_ids=shared_bill_ids,
                shared_contract_ids=shared_contract_ids,
                shared_risk_ids=shared_risk_ids,
                shared_restriction_ids=shared_restriction_ids,
                shared_affiliation_ids=shared_affiliation_ids,
                shared_disclosure_ids=shared_disclosure_ids,
                shared_asset_ids=shared_asset_ids,
                shared_official_document_ids=shared_official_document_ids,
                shared_vote_count=same_votes,
                entity_semantic_score=entity_semantic_score,
            )
            shortest_bridge_path = _build_shortest_bridge_path(
                entity_a=entity_a,
                entity_b=entity_b,
                shared_content_rows=support_rows,
                shared_claim_cluster_ids=shared_claim_cluster_ids,
                shared_case_ids=shared_case_ids,
                shared_bill_ids=shared_bill_ids,
                shared_contract_ids=shared_contract_ids,
                shared_risk_ids=shared_risk_ids,
                shared_restriction_ids=shared_restriction_ids,
                shared_affiliation_ids=shared_affiliation_ids,
                shared_disclosure_ids=shared_disclosure_ids,
                shared_asset_ids=shared_asset_ids,
                shared_vote_count=same_votes,
                entity_semantic_score=entity_semantic_score,
            )
            bridge_types = {
                str(node.get("node_type") or "")
                for node in shortest_bridge_path
            } | {
                str(node.get("node_type") or "")
                for node in explain_path
            }
            official_bridge_types = bridge_types & OFFICIAL_PROMOTION_BRIDGE_TYPES
            entity_quality_score, entity_block_reason = _pair_entity_quality(
                entity_a_type,
                entity_a_name,
                entity_b_type,
                entity_b_name,
                bridge_types=bridge_types,
            )
            bridge_diversity_score = round(
                min(
                    1.0,
                    0.45 * min(1.0, len({node.get("node_type") for node in explain_path if node.get("node_type") in NON_CONTENT_PATH_TYPES}) / 3.0)
                    + 0.35 * node_similarity_score
                    + 0.20 * same_community_score,
                ),
                4,
            )
            promotion_block_reason = entity_block_reason
            if candidate_type == "same_case_cluster":
                has_nonseed_bridge = bool(bridge_types & PROMOTION_BRIDGE_TYPES)
                has_evidence_bridge = bool(
                    bridge_types
                    & {
                        "RestrictionEvent",
                        "Affiliation",
                        "Disclosure",
                        "Asset",
                        "OfficialDocument",
                        "Bill",
                        "Contract",
                    }
                )
                if not has_nonseed_bridge:
                    promotion_block_reason = promotion_block_reason or "same_case_requires_nonseed_bridge"
                elif not (
                    metrics["support_domains"] > 0
                    or support_hard_evidence_count > 0
                    or has_evidence_bridge
                ):
                    promotion_block_reason = promotion_block_reason or "same_case_requires_evidence_bridge"
            elif (
                support_hard_evidence_count > 0
                and (
                    structural_seed_kind in OFFICIAL_SEED_KINDS
                    or any(str(content_type or "") in OFFICIAL_CANDIDATE_CONTENT_TYPES for content_type in metrics["content_types"])
                )
                and not official_bridge_types
            ):
                promotion_block_reason = promotion_block_reason or "official_bridge_missing"
            elif metrics["support_items"] > 0 and metrics["support_sources"] >= 2 and metrics["support_domains"] == 0:
                promotion_block_reason = promotion_block_reason or "fake_domain_diversity"
            elif metrics["support_items"] > 0 and dedupe_support_score < 0.45 and metrics["support_sources"] < 2:
                promotion_block_reason = promotion_block_reason or "duplicate_amplified_support"
            official_bridge_bonus = 0.03 * min(1.0, len(official_bridge_types))
            calibrated_score = round(
                min(
                    1.0,
                    0.15 * structural_score
                    + 0.15 * float(metrics["score"])
                    + 0.14 * content_support_score
                    + 0.11 * source_diversity_score
                    + 0.10 * semantic_support_score
                    + 0.10 * evidence_quality_score
                    + 0.07 * float(metrics["temporal_proximity"])
                    + 0.08 * entity_quality_score
                    + 0.05 * dedupe_support_score
                    + 0.05 * real_host_diversity_score
                    + 0.05 * bridge_diversity_score
                    + official_bridge_bonus
                ),
                4,
            )
            candidate_state = _candidate_state(
                candidate_type=candidate_type,
                structural_seed_kind=structural_seed_kind,
                support_items=metrics["support_items"],
                support_sources=metrics["support_sources"],
                support_domains=metrics["support_domains"],
                support_claim_cluster_count=support_claim_cluster_count,
                support_hard_evidence_count=support_hard_evidence_count,
                semantic_support_score=semantic_support_score,
                calibrated_score=calibrated_score,
                explain_path=explain_path,
                shortest_bridge_path=shortest_bridge_path,
                promotion_block_reason=promotion_block_reason,
            )
            promotion_state = candidate_state
            if candidate_state == "promoted":
                promoted_candidates += 1

            evidence_mix = {
                "content_types": metrics["content_types"],
                "official_content_types": sorted(
                    {
                        str(row.get("content_type") or "")
                        for row in support_rows
                        if row.get("is_official_bridge")
                        or row.get("source_category") in OFFICIAL_SOURCE_CATEGORIES
                        or str(row.get("content_type") or "") in HARD_CONTENT_TYPES
                    }
                ),
                "source_categories": sorted({row["source_category"] for row in support_rows if row.get("source_category")}),
                "domains": sorted({row["domain"] for row in support_rows if row.get("domain")}),
                "telegram_only": bool(support_rows) and all((row.get("source_category") or "") == "telegram" for row in support_rows),
                "content_clusters": {
                    "cluster_ids": metrics["cluster_ids"],
                    "cluster_count": len(metrics["cluster_ids"]),
                },
                "official_bridge_count": len(
                    {
                        bridge_unit
                        for row in support_rows
                        for bridge_unit in row.get("bridge_units", [])
                    }
                ),
                "duplicate_count_total": metrics["duplicate_count_total"],
                "bridge_types": [str(node.get("node_type") or "") for node in explain_path],
            }
            metadata = {
                "shared_claims": shared_claims,
                "shared_claim_cluster_count": support_claim_cluster_count,
                "entity_semantic_score": entity_semantic_score,
                "node_similarity_score": node_similarity_score,
                "same_community_score": same_community_score,
                "case_overlap": case_overlap,
                "bill_overlap": bill_overlap,
                "contract_overlap": contract_overlap,
                "risk_overlap": risk_overlap,
                "restriction_overlap": len(shared_restriction_ids),
                "affiliation_overlap": len(shared_affiliation_ids),
                "disclosure_overlap": len(shared_disclosure_ids),
                "asset_overlap": len(shared_asset_ids),
                "same_vote_count": same_votes,
                "shared_vote_count": shared_vote_count,
                "same_vote_ratio": round(vote_overlap_ratio, 4),
                "support_domains_list": evidence_mix["domains"],
                "support_categories_list": sorted({row["source_category"] for row in support_rows if row["source_category"]}),
                "structural_seed": bool(structural_seed_kind and not shared_rows),
                "structural_seed_kind": structural_seed_kind,
                "explain_path": explain_path,
                "shortest_bridge_path": shortest_bridge_path,
                "promotion_block_reason": promotion_block_reason,
            }

            cur = conn.execute(
                """
                INSERT INTO relation_candidates(
                    entity_a_id, entity_b_id, candidate_type, seed_kind, origin, score,
                    structural_score, semantic_score, support_score, calibrated_score,
                    source_independence, evidence_overlap, temporal_proximity,
                    role_compatibility, tag_overlap, text_specificity,
                    support_items, support_sources, support_domains, support_categories,
                    support_claim_cluster_count, support_hard_evidence_count,
                    first_seen_at, last_seen_at, sample_content_ids, candidate_state, promotion_state,
                    promoted_relation_type, promotion_block_reason, evidence_mix_json, explain_path_json, metadata_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    entity_a,
                    entity_b,
                    candidate_type,
                    structural_seed_kind,
                    "candidate_builder:hybrid" if structural_seed_kind and not shared_rows else "candidate_builder:co_occurrence",
                    metrics["score"],
                    structural_score,
                    semantic_support_score,
                    content_support_score,
                    calibrated_score,
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
                    support_claim_cluster_count,
                    support_hard_evidence_count,
                    min((row["seen_at"] for row in shared_rows if row["seen_at"]), default=None),
                    max((row["seen_at"] for row in shared_rows if row["seen_at"]), default=None),
                    json.dumps(shared_content_ids[:20], ensure_ascii=False),
                    candidate_state,
                    promotion_state,
                    candidate_type if candidate_state == "promoted" else None,
                    promotion_block_reason,
                    json.dumps(evidence_mix, ensure_ascii=False),
                    json.dumps(explain_path, ensure_ascii=False),
                    json.dumps(metadata, ensure_ascii=False),
                ),
            )
            candidate_id = int(cur.lastrowid)
            created += 1

            conn.execute(
                """
                INSERT OR REPLACE INTO relation_features(
                    candidate_id, structural_score, content_support_score, source_diversity_score,
                    semantic_support_score, shared_claim_cluster_score, evidence_quality_score,
                    entity_quality_score, dedupe_support_score, real_host_diversity_score, bridge_diversity_score,
                    temporal_score, role_compatibility_score, calibrated_score, explain_path_json, metadata_json, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    candidate_id,
                    structural_score,
                    content_support_score,
                    source_diversity_score,
                    semantic_support_score,
                    round(min(1.0, support_claim_cluster_count / 3.0), 4),
                    evidence_quality_score,
                    entity_quality_score,
                    dedupe_support_score,
                    real_host_diversity_score,
                    bridge_diversity_score,
                    metrics["temporal_proximity"],
                    metrics["role_compatibility"],
                    calibrated_score,
                    json.dumps(explain_path, ensure_ascii=False),
                    json.dumps(metadata, ensure_ascii=False),
                    _now_iso(),
                ),
            )

            for row in support_rows:
                if not (
                    row.get("content_item_id") is not None
                    or row.get("source_id") is not None
                    or row.get("domain")
                    or row.get("source_category")
                ):
                    continue
                conn.execute(
                    """
                    INSERT INTO relation_support(
                        candidate_id, support_kind, support_class, content_item_id, source_id, domain, category, metadata_json
                    ) VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (
                        candidate_id,
                        row.get("bridge_kind") or "content",
                        "evidence",
                        row.get("content_item_id"),
                        row["source_id"],
                        row.get("domain"),
                        row["source_category"],
                        json.dumps(
                            {
                                "seen_at": row["seen_at"],
                                "cluster_id": row.get("cluster_id"),
                                "canonical_content_id": row.get("canonical_content_id"),
                                "duplicate_count": row.get("duplicate_count"),
                                "content_type": row.get("content_type"),
                                "bridge_kind": row.get("bridge_kind"),
                                "bridge_id": row.get("bridge_id"),
                                "is_official_bridge": bool(row.get("is_official_bridge")),
                            },
                            ensure_ascii=False,
                        ),
                    ),
                )
                support_rows_created += 1

            for content_item_id in shared_official_document_ids[:10]:
                conn.execute(
                    """
                    INSERT INTO relation_support(candidate_id, support_kind, support_class, evidence_item_id, metadata_json)
                    VALUES(?,?,?,?,?)
                    """,
                    (
                        candidate_id,
                        "official_document",
                        "support",
                        content_item_id,
                        json.dumps({"content_item_id": content_item_id}, ensure_ascii=False),
                    ),
                )
                support_rows_created += 1

            for claim_cluster_id in shared_claim_cluster_ids[:10]:
                conn.execute(
                    """
                    INSERT INTO relation_support(candidate_id, support_kind, support_class, metadata_json)
                    VALUES(?,?,?,?)
                    """,
                    (
                        candidate_id,
                        "claim_cluster",
                        "support",
                        json.dumps({"claim_cluster_id": claim_cluster_id}, ensure_ascii=False),
                    ),
                )
                support_rows_created += 1

            if entity_semantic_score >= 0.35:
                conn.execute(
                    """
                    INSERT INTO relation_support(candidate_id, support_kind, support_class, metric_value, metadata_json)
                    VALUES(?,?,?,?,?)
                    """,
                    (
                        candidate_id,
                        "semantic_neighbor",
                        "support",
                        entity_semantic_score,
                        json.dumps({"pair": [entity_a, entity_b]}, ensure_ascii=False),
                    ),
                )
                support_rows_created += 1

            if contract_overlap:
                for contract_id in sorted(contract_map.get(entity_a, set()) & contract_map.get(entity_b, set()))[:10]:
                    conn.execute(
                        """
                        INSERT INTO relation_support(candidate_id, support_kind, support_class, metadata_json)
                        VALUES(?,?,?,?)
                        """,
                        (
                        candidate_id,
                        "contract",
                        "seed",
                        json.dumps({"contract_id": contract_id}, ensure_ascii=False),
                    ),
                    )
                    support_rows_created += 1

            if bill_overlap:
                for bill_id in sorted(bill_map.get(entity_a, set()) & bill_map.get(entity_b, set()))[:10]:
                    conn.execute(
                        """
                        INSERT INTO relation_support(candidate_id, support_kind, support_class, metadata_json)
                        VALUES(?,?,?,?)
                        """,
                        (
                            candidate_id,
                            "bill",
                            "seed",
                            json.dumps({"bill_id": bill_id}, ensure_ascii=False),
                        ),
                    )
                    support_rows_created += 1

            if case_overlap:
                for case_id in sorted(case_map.get(entity_a, set()) & case_map.get(entity_b, set()))[:10]:
                    conn.execute(
                        """
                        INSERT INTO relation_support(candidate_id, support_kind, support_class, metadata_json)
                        VALUES(?,?,?,?)
                        """,
                        (
                            candidate_id,
                            "case",
                            "seed",
                            json.dumps({"case_id": case_id}, ensure_ascii=False),
                        ),
                    )
                    support_rows_created += 1

            if risk_overlap:
                for risk_id in sorted(risk_map.get(entity_a, set()) & risk_map.get(entity_b, set()))[:10]:
                    conn.execute(
                        """
                        INSERT INTO relation_support(candidate_id, support_kind, support_class, metadata_json)
                        VALUES(?,?,?,?)
                        """,
                        (
                            candidate_id,
                            "risk",
                            "seed",
                            json.dumps({"risk_id": risk_id}, ensure_ascii=False),
                        ),
                    )
                    support_rows_created += 1

            for restriction_id in shared_restriction_ids[:10]:
                conn.execute(
                    """
                    INSERT INTO relation_support(candidate_id, support_kind, support_class, metadata_json)
                    VALUES(?,?,?,?)
                    """,
                    (
                        candidate_id,
                        "restriction_event",
                        "support",
                        json.dumps({"restriction_event_id": restriction_id}, ensure_ascii=False),
                    ),
                )
                support_rows_created += 1

            for affiliation_id in shared_affiliation_ids[:10]:
                conn.execute(
                    """
                    INSERT INTO relation_support(candidate_id, support_kind, support_class, metadata_json)
                    VALUES(?,?,?,?)
                    """,
                    (
                        candidate_id,
                        "affiliation",
                        "support",
                        json.dumps({"affiliation_id": affiliation_id}, ensure_ascii=False),
                    ),
                )
                support_rows_created += 1

            for disclosure_id in shared_disclosure_ids[:10]:
                conn.execute(
                    """
                    INSERT INTO relation_support(candidate_id, support_kind, support_class, metadata_json)
                    VALUES(?,?,?,?)
                    """,
                    (
                        candidate_id,
                        "disclosure",
                        "support",
                        json.dumps({"disclosure_id": disclosure_id}, ensure_ascii=False),
                    ),
                )
                support_rows_created += 1

            for asset_id in shared_asset_ids[:10]:
                conn.execute(
                    """
                    INSERT INTO relation_support(candidate_id, support_kind, support_class, metadata_json)
                    VALUES(?,?,?,?)
                    """,
                    (
                        candidate_id,
                        "asset",
                        "support",
                        json.dumps({"asset_id": asset_id}, ensure_ascii=False),
                    ),
                )
                support_rows_created += 1

            for tag_name in metrics["specific_tags"]:
                conn.execute(
                    """
                    INSERT INTO relation_support(candidate_id, support_kind, support_class, tag_name)
                    VALUES(?,?,?,?)
                    """,
                    (candidate_id, "tag", "seed", tag_name),
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
                    INSERT INTO relation_support(candidate_id, support_kind, support_class, metric_value, metadata_json)
                    VALUES(?,?,?,?,?)
                    """,
                    (
                        candidate_id,
                        "metric",
                        "seed",
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
            SELECT id, entity_a_id, entity_b_id, candidate_type, score, calibrated_score, sample_content_ids, metadata_json, candidate_state, promotion_state
            FROM relation_candidates
            ORDER BY score DESC, id
            """
        ).fetchall():
            (
                candidate_id,
                entity_a,
                entity_b,
                candidate_type,
                score,
                calibrated_score,
                sample_content_ids,
                metadata_json,
                candidate_state,
                promotion_state,
            ) = row
            if candidate_state != "promoted" or float(calibrated_score or 0.0) < score_threshold:
                conn.execute(
                    """
                    UPDATE relation_candidates
                    SET promotion_state=?, candidate_state=?, promoted_relation_type=NULL, promoted_at=NULL
                    WHERE id=?
                    """,
                    (candidate_state or promotion_state or "pending", candidate_state or promotion_state or "pending", candidate_id),
                )
                continue

            sample_ids = _parse_json(sample_content_ids, [])
            evidence_item_id = sample_ids[0] if sample_ids else None
            strength = "strong" if float(calibrated_score) >= 0.82 else "moderate" if float(calibrated_score) >= 0.74 else "weak"
            detected_by = f"relation_candidate:{candidate_id}:score={float(calibrated_score):.3f}"
            promoted_candidates += 1
            if not _has_existing_non_candidate_relation(conn, int(entity_a), int(entity_b)):
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
                promoted_relations += 1
            conn.execute(
                """
                UPDATE relation_candidates
                SET promotion_state='promoted', candidate_state='promoted', promoted_relation_type=?, promoted_at=?, metadata_json=?
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
