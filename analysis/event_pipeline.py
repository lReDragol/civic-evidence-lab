from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent

from config.db_utils import get_db


DERIVATION_TYPES = (
    "clean_factual_text",
    "structured_extract",
    "event_summary_fragment",
)
OFFICIAL_CONTENT_TYPES = {
    "restriction_record",
    "official_document",
    "registry_record",
    "court_record",
    "procurement",
    "bill",
    "transcript",
    "declaration",
    "disclosure",
}
EVENT_ROLE_TYPES = {
    "issuer",
    "target",
    "affected",
    "commentator",
    "executor",
    "supporter",
    "opponent",
    "court",
    "regulator",
}
CLAIM_TO_FACT_TYPE = {
    "restriction": "restriction",
    "reaction": "reaction",
    "statement": "statement",
    "decision": "decision",
    "court_step": "court_step",
    "ownership": "ownership",
    "income": "income",
    "appointment": "appointment",
    "protest": "protest",
    "appeal": "appeal",
}


def table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _normalize_space(text: Any) -> str:
    value = str(text or "")
    value = re.sub(r"\s+", " ", value, flags=re.UNICODE).strip()
    return value


def _normalize_claim_text(text: Any) -> str:
    value = _normalize_space(text).strip(" \t\r\n.,;:!?-–—\"'«»()[]")
    return value.casefold()


def _clean_factual_text(title: Any, body: Any) -> str:
    text = _normalize_space(body)
    title_text = _normalize_space(title)
    for pattern in (
        r"(?:подписывайтесь|подпишитесь).*$",
        r"(?:подробнее|источник|ссылка).*$",
        r"(?:читайте также|см\. также).*$",
    ):
        text = re.sub(pattern, "", text, flags=re.IGNORECASE | re.UNICODE)
    text = _normalize_space(text)
    if not text:
        return title_text
    if title_text and title_text.casefold() not in text.casefold() and len(text) < 220:
        text = f"{title_text}. {text}"
    return text


def _hash_input(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _parse_json(raw_value: Any, default: Any):
    if not raw_value:
        return default
    try:
        return json.loads(raw_value)
    except (json.JSONDecodeError, TypeError):
        return default


def _coalesce_datetime(item: dict[str, Any]) -> str:
    return str(item.get("published_at") or item.get("collected_at") or item.get("event_date") or "")


def _first_sentence(text: str, max_len: int = 220) -> str:
    clean = _normalize_space(text)
    if not clean:
        return ""
    match = re.split(r"(?<=[.!?])\s+", clean, maxsplit=1)
    sentence = match[0] if match else clean
    if len(sentence) > max_len:
        return sentence[: max_len - 1].rstrip() + "…"
    return sentence


def _pick_event_type(restrictions: list[dict[str, Any]], claims: list[dict[str, Any]], items: list[dict[str, Any]]) -> str:
    for row in restrictions:
        if row.get("restriction_type"):
            return str(row["restriction_type"])
    for row in claims:
        claim_type = str(row.get("claim_type") or "").strip()
        if claim_type:
            return claim_type
    for item in items:
        content_type = str(item.get("content_type") or "").strip()
        if content_type in OFFICIAL_CONTENT_TYPES:
            return content_type
    return "event"


def _pick_event_title(cluster: dict[str, Any], restrictions: list[dict[str, Any]], items: list[dict[str, Any]]) -> str:
    canonical_title = _normalize_space(cluster.get("canonical_title"))
    if canonical_title:
        return canonical_title
    if restrictions:
        restriction = restrictions[0]
        target = _normalize_space(restriction.get("target_name")) or _normalize_space(restriction.get("target_entity_name"))
        if target and restriction.get("restriction_type") == "internet_block":
            return f"Блокировка {target}"
    for item in items:
        title = _normalize_space(item.get("title"))
        if title:
            return title
    return f"Событие {cluster.get('cluster_id') or 'без названия'}"


def _build_structured_extract(
    item: dict[str, Any],
    mentions: list[dict[str, Any]],
    claims: list[dict[str, Any]],
    restrictions: list[dict[str, Any]],
) -> dict[str, Any]:
    actors: list[dict[str, Any]] = []
    organizations: list[dict[str, Any]] = []
    locations: list[str] = []
    for mention in mentions:
        record = {
            "entity_id": mention.get("entity_id"),
            "name": mention.get("canonical_name"),
            "entity_type": mention.get("entity_type"),
            "role": mention.get("mention_type"),
            "confidence": mention.get("confidence"),
        }
        if mention.get("entity_type") == "organization":
            organizations.append(record)
        elif mention.get("entity_type") == "location":
            if mention.get("canonical_name"):
                locations.append(str(mention["canonical_name"]))
        else:
            actors.append(record)
    legal_basis = [row.get("legal_basis") for row in restrictions if row.get("legal_basis")]
    justification = [row.get("stated_justification") for row in restrictions if row.get("stated_justification")]
    return {
        "actors": actors,
        "organizations": organizations,
        "dates": [value for value in (_coalesce_datetime(item), item.get("published_at")) if value],
        "locations": locations,
        "actions": [row.get("claim_type") or "statement" for row in claims if row.get("claim_type")],
        "legal_basis": legal_basis,
        "affected_groups": [row.get("target_name") for row in restrictions if row.get("target_name")],
        "explicit_claims": [row.get("canonical_text") or row.get("claim_text") for row in claims if row.get("claim_text")],
        "uncertainty_markers": [],
        "stated_justification": justification,
        "content_type": item.get("content_type"),
        "source_id": item.get("source_id"),
    }


def _material_role(item: dict[str, Any], official_ids: set[int], first_non_official_id: int | None) -> str:
    content_id = int(item["id"])
    if content_id in official_ids:
        return "official_doc"
    if first_non_official_id == content_id:
        return "origin"
    if str(item.get("content_type") or "").strip().lower() in {"video", "photo", "image"}:
        return "media"
    return "update"


def _source_strength(item: dict[str, Any], role: str) -> str:
    if role == "official_doc":
        return "hard"
    if str(item.get("source_category") or "").startswith("official"):
        return "support"
    return "signal"


def _canonical_content_groups(conn, limit: int | None = None) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    if table_exists(conn, "content_clusters") and table_exists(conn, "content_cluster_items"):
        cluster_rows = conn.execute(
            """
            SELECT cc.id AS cluster_id, cc.cluster_key, cc.cluster_type, cc.canonical_content_id, cc.canonical_title,
                   cc.first_seen_at, cc.last_seen_at, cc.status,
                   cci.content_item_id, cci.is_canonical, cci.similarity_score,
                   ci.id AS id,
                   ci.source_id, ci.external_id, ci.content_type, ci.title, ci.body_text,
                   ci.published_at, ci.collected_at, ci.url, ci.status AS content_status
            FROM content_clusters cc
            JOIN content_cluster_items cci ON cci.cluster_id = cc.id
            JOIN content_items ci ON ci.id = cci.content_item_id
            WHERE cc.status='active'
            ORDER BY cc.id, COALESCE(ci.published_at, ci.collected_at, '') ASC, ci.id ASC
            """
        ).fetchall()
        grouped: dict[int, dict[str, Any]] = {}
        for row in cluster_rows:
            cluster_id = int(row["cluster_id"])
            group = grouped.setdefault(
                cluster_id,
                {
                    "cluster_id": cluster_id,
                    "cluster_key": row["cluster_key"],
                    "cluster_type": row["cluster_type"],
                    "canonical_content_id": row["canonical_content_id"],
                    "canonical_title": row["canonical_title"],
                    "first_seen_at": row["first_seen_at"],
                    "last_seen_at": row["last_seen_at"],
                    "items": [],
                },
            )
            group["items"].append(dict(row))
        groups = list(grouped.values())
    if not groups:
        for row in conn.execute(
            """
            SELECT ci.id AS content_item_id, ci.id AS canonical_content_id, NULL AS cluster_id,
                   NULL AS cluster_key, NULL AS cluster_type, ci.title AS canonical_title,
                   ci.published_at AS first_seen_at, ci.published_at AS last_seen_at,
                   ci.id, ci.source_id, ci.external_id, ci.content_type, ci.title, ci.body_text,
                   ci.published_at, ci.collected_at, ci.url, ci.status AS content_status
            FROM content_items ci
            ORDER BY COALESCE(ci.published_at, ci.collected_at, '') ASC, ci.id ASC
            """
        ).fetchall():
            groups.append(
                {
                    "cluster_id": None,
                    "cluster_key": f"singleton:{row['content_item_id']}",
                    "cluster_type": "singleton",
                    "canonical_content_id": row["canonical_content_id"],
                    "canonical_title": row["canonical_title"],
                    "first_seen_at": row["first_seen_at"],
                    "last_seen_at": row["last_seen_at"],
                    "items": [dict(row)],
                }
            )
    if limit:
        return groups[:limit]
    return groups


def build_event_pipeline(settings: dict[str, Any] | None = None, limit: int | None = None) -> dict[str, Any]:
    conn = get_db(settings or {})
    try:
        conn.execute("DELETE FROM fact_evidence")
        conn.execute("DELETE FROM event_facts")
        conn.execute("DELETE FROM event_timeline")
        conn.execute("DELETE FROM event_entities")
        conn.execute("DELETE FROM event_items")
        conn.execute("DELETE FROM events")
        conn.execute(
            """
            DELETE FROM content_derivations
            WHERE model_provider='deterministic'
              AND model_name='event-pipeline-v1'
              AND prompt_version='event-pipeline-v1'
              AND derivation_type IN ('clean_factual_text', 'structured_extract', 'event_summary_fragment')
            """
        )

        mention_rows = conn.execute(
            """
            SELECT em.content_item_id, em.entity_id, em.mention_type, em.confidence,
                   e.canonical_name, e.entity_type
            FROM entity_mentions em
            JOIN entities e ON e.id = em.entity_id
            ORDER BY em.content_item_id, em.confidence DESC, em.id ASC
            """
        ).fetchall() if table_exists(conn, "entity_mentions") and table_exists(conn, "entities") else []
        mentions_by_content: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in mention_rows:
            mentions_by_content[int(row["content_item_id"])].append(dict(row))

        claim_rows = conn.execute(
            """
            SELECT id, content_item_id, claim_text, canonical_text, claim_type, confidence_final, status
            FROM claims
            ORDER BY id
            """
        ).fetchall() if table_exists(conn, "claims") else []
        claims_by_content: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in claim_rows:
            claims_by_content[int(row["content_item_id"])].append(dict(row))

        evidence_rows = conn.execute(
            """
            SELECT id, claim_id, evidence_item_id, evidence_type, evidence_class, strength, notes
            FROM evidence_links
            ORDER BY id
            """
        ).fetchall() if table_exists(conn, "evidence_links") else []
        evidence_by_claim: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in evidence_rows:
            evidence_by_claim[int(row["claim_id"])].append(dict(row))

        restriction_rows = conn.execute(
            """
            SELECT re.*, ei.canonical_name AS issuer_name, et.canonical_name AS target_entity_name
            FROM restriction_events re
            LEFT JOIN entities ei ON ei.id = re.issuer_entity_id
            LEFT JOIN entities et ON et.id = re.target_entity_id
            ORDER BY re.id
            """
        ).fetchall() if table_exists(conn, "restriction_events") else []
        restrictions_by_content: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in restriction_rows:
            if row["source_content_id"] is not None:
                restrictions_by_content[int(row["source_content_id"])].append(dict(row))

        groups = _canonical_content_groups(conn, limit=limit)
        derivations_written = 0
        events_created = 0
        facts_written = 0
        timeline_written = 0

        for group in groups:
            items = list(group["items"])
            if not items:
                continue

            item_ids = [int(item["id"]) for item in items if item.get("id") is not None]
            group_claims: list[dict[str, Any]] = []
            group_restrictions: list[dict[str, Any]] = []
            for content_id in item_ids:
                group_claims.extend(claims_by_content.get(content_id, []))
                group_restrictions.extend(restrictions_by_content.get(content_id, []))

            cleaned_by_item: dict[int, str] = {}
            for item in items:
                content_id = int(item["id"])
                item_mentions = mentions_by_content.get(content_id, [])
                item_claims = claims_by_content.get(content_id, [])
                item_restrictions = restrictions_by_content.get(content_id, [])
                cleaned_text = _clean_factual_text(item.get("title"), item.get("body_text"))
                cleaned_by_item[content_id] = cleaned_text
                derivations = {
                    "clean_factual_text": {
                        "output_text": cleaned_text,
                        "output_json": None,
                        "confidence": 0.86,
                    },
                    "structured_extract": {
                        "output_text": None,
                        "output_json": _build_structured_extract(item, item_mentions, item_claims, item_restrictions),
                        "confidence": 0.82,
                    },
                    "event_summary_fragment": {
                        "output_text": _first_sentence(cleaned_text, max_len=260),
                        "output_json": {
                            "proposed_role": "official_doc"
                            if str(item.get("content_type") or "").strip().lower() in OFFICIAL_CONTENT_TYPES
                            else "update",
                            "published_at": _coalesce_datetime(item),
                            "content_type": item.get("content_type"),
                        },
                        "confidence": 0.8,
                    },
                }
                for derivation_type, payload in derivations.items():
                    input_hash = _hash_input(
                        {
                            "content_item_id": content_id,
                            "derivation_type": derivation_type,
                            "title": item.get("title"),
                            "body_text": item.get("body_text"),
                            "published_at": item.get("published_at"),
                            "url": item.get("url"),
                        }
                    )
                    conn.execute(
                        """
                        INSERT INTO content_derivations(
                            content_item_id, derivation_type, model_provider, model_name, prompt_version,
                            input_hash, output_text, output_json, confidence, status
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            content_id,
                            derivation_type,
                            "deterministic",
                            "event-pipeline-v1",
                            "event-pipeline-v1",
                            input_hash,
                            payload.get("output_text"),
                            _json(payload.get("output_json")) if payload.get("output_json") is not None else None,
                            payload.get("confidence") or 0,
                            "ready",
                        ),
                    )
                    derivations_written += 1

            sorted_items = sorted(items, key=lambda item: (_coalesce_datetime(item), int(item["id"])))
            event_date_start = min((_coalesce_datetime(item) for item in sorted_items if _coalesce_datetime(item)), default="")
            event_date_end = max((_coalesce_datetime(item) for item in sorted_items if _coalesce_datetime(item)), default="")
            observed_at_by_content = {
                int(item["id"]): (_coalesce_datetime(item) or event_date_start or None)
                for item in sorted_items
            }
            official_ids = {
                int(item["id"])
                for item in sorted_items
                if str(item.get("content_type") or "").strip().lower() in OFFICIAL_CONTENT_TYPES
                or str(item.get("content_status") or "").strip().lower().startswith("official")
            }
            non_official_items = [item for item in sorted_items if int(item["id"]) not in official_ids]
            first_non_official_id = int(non_official_items[0]["id"]) if non_official_items else None

            event_type = _pick_event_type(group_restrictions, group_claims, sorted_items)
            event_title = _pick_event_title(group, group_restrictions, sorted_items)
            summary_short = _first_sentence(
                cleaned_by_item.get(int(sorted_items[0]["id"])) or group.get("canonical_title") or event_title
            )
            narrative_parts = []
            for item in sorted_items:
                content_id = int(item["id"])
                role = _material_role(item, official_ids, first_non_official_id)
                fragment = cleaned_by_item.get(content_id) or _normalize_space(item.get("title"))
                if fragment:
                    narrative_parts.append(f"{role}: {fragment}")
            summary_long = "\n".join(narrative_parts)
            event_metadata = {
                "cluster_id": group.get("cluster_id"),
                "cluster_key": group.get("cluster_key"),
                "canonical_content_id": group.get("canonical_content_id"),
                "item_ids": item_ids,
            }
            cursor = conn.execute(
                """
                INSERT INTO events(
                    canonical_title, event_type, summary_short, summary_long, status,
                    event_date_start, event_date_end, first_observed_at, last_observed_at,
                    importance_score, confidence, metadata_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    event_title,
                    event_type,
                    summary_short,
                    summary_long,
                    "active",
                    event_date_start or None,
                    event_date_end or None,
                    event_date_start or None,
                    event_date_end or None,
                    0.8 if group_restrictions else 0.6,
                    0.88 if group_restrictions or group_claims else 0.5,
                    _json(event_metadata),
                ),
            )
            event_id = int(cursor.lastrowid)
            events_created += 1

            for sort_order, item in enumerate(sorted_items):
                content_id = int(item["id"])
                item_role = _material_role(item, official_ids, first_non_official_id)
                strength = _source_strength(item, item_role)
                conn.execute(
                    """
                    INSERT INTO event_items(event_id, content_item_id, content_cluster_id, item_role, source_strength, metadata_json)
                    VALUES(?,?,?,?,?,?)
                    """,
                    (
                        event_id,
                        content_id,
                        group.get("cluster_id"),
                        item_role,
                        strength,
                        _json(
                            {
                                "cluster_key": group.get("cluster_key"),
                                "content_type": item.get("content_type"),
                                "source_id": item.get("source_id"),
                            }
                        ),
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO event_timeline(
                        event_id, timeline_date, title, description, content_item_id, document_content_id, sort_order, metadata_json
                    ) VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (
                        event_id,
                        _coalesce_datetime(item) or None,
                        _normalize_space(item.get("title")) or event_title,
                        cleaned_by_item.get(content_id) or _normalize_space(item.get("body_text")),
                        content_id,
                        content_id if content_id in official_ids else None,
                        sort_order,
                        _json({"item_role": item_role}),
                    ),
                )
                timeline_written += 1

            event_entity_map: dict[tuple[int, str], dict[str, Any]] = {}
            for restriction in group_restrictions:
                if restriction.get("issuer_entity_id"):
                    key = (int(restriction["issuer_entity_id"]), "issuer")
                    event_entity_map[key] = {
                        "entity_id": int(restriction["issuer_entity_id"]),
                        "role": "issuer",
                        "confidence": 0.99,
                        "observed_at": restriction.get("event_date") or event_date_start,
                    }
                if restriction.get("target_entity_id"):
                    key = (int(restriction["target_entity_id"]), "target")
                    event_entity_map[key] = {
                        "entity_id": int(restriction["target_entity_id"]),
                        "role": "target",
                        "confidence": 0.99,
                        "observed_at": restriction.get("event_date") or event_date_start,
                    }

            for content_id in item_ids:
                for mention in mentions_by_content.get(content_id, []):
                    role = str(mention.get("mention_type") or "").strip().lower()
                    if role not in EVENT_ROLE_TYPES:
                        role = "affected" if mention.get("entity_type") == "person" else "target"
                    key = (int(mention["entity_id"]), role)
                    existing = event_entity_map.get(key)
                    confidence = float(mention.get("confidence") or 0)
                    if existing is None or confidence > float(existing.get("confidence") or 0):
                        event_entity_map[key] = {
                            "entity_id": int(mention["entity_id"]),
                            "role": role,
                            "confidence": confidence,
                            "observed_at": observed_at_by_content.get(content_id) or event_date_start or None,
                        }
            for entity in sorted(event_entity_map.values(), key=lambda item: (item["role"], item["entity_id"])):
                conn.execute(
                    """
                    INSERT INTO event_entities(event_id, entity_id, role, confidence, valid_from, valid_to, observed_at, metadata_json)
                    VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (
                        event_id,
                        entity["entity_id"],
                        entity["role"],
                        entity["confidence"],
                        event_date_start or None,
                        event_date_end or None,
                        entity.get("observed_at") or event_date_start or None,
                        _json({"event_type": event_type}),
                    ),
                )

            for claim in group_claims:
                canonical_text = _normalize_claim_text(claim.get("canonical_text") or claim.get("claim_text"))
                if not canonical_text:
                    continue
                fact_type = CLAIM_TO_FACT_TYPE.get(str(claim.get("claim_type") or "").strip().lower()) or str(
                    claim.get("claim_type") or "statement"
                )
                fact_cursor = conn.execute(
                    """
                    INSERT INTO event_facts(
                        event_id, claim_id, fact_type, canonical_text, polarity, valid_from, valid_to,
                        observed_at, confidence, metadata_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        event_id,
                        claim["id"],
                        fact_type,
                        canonical_text,
                        "neutral",
                        event_date_start or None,
                        event_date_end or None,
                        event_date_start or None,
                        float(claim.get("confidence_final") or 0.75),
                        _json({"content_item_id": claim.get("content_item_id"), "status": claim.get("status")}),
                    ),
                )
                fact_id = int(fact_cursor.lastrowid)
                facts_written += 1
                linked_rows = evidence_by_claim.get(int(claim["id"]), [])
                if linked_rows:
                    for evidence in linked_rows:
                        conn.execute(
                            """
                            INSERT INTO fact_evidence(
                                fact_id, content_item_id, document_content_id, evidence_type,
                                evidence_class, source_strength, metadata_json
                            ) VALUES(?,?,?,?,?,?,?)
                            """,
                            (
                                fact_id,
                                claim.get("content_item_id"),
                                evidence.get("evidence_item_id"),
                                evidence.get("evidence_type"),
                                evidence.get("evidence_class") or "support",
                                evidence.get("strength") or "support",
                                _json({"evidence_link_id": evidence.get("id"), "notes": evidence.get("notes")}),
                            ),
                        )
                else:
                    conn.execute(
                        """
                        INSERT INTO fact_evidence(
                            fact_id, content_item_id, document_content_id, evidence_type,
                            evidence_class, source_strength, metadata_json
                        ) VALUES(?,?,?,?,?,?,?)
                        """,
                        (
                            fact_id,
                            claim.get("content_item_id"),
                            None,
                            "content_item",
                            "support",
                            "support",
                            _json({"derived_from_claim": claim.get("id")}),
                        ),
                    )

        conn.commit()
        return {
            "ok": True,
            "events_created": events_created,
            "derivations_written": derivations_written,
            "facts_written": facts_written,
            "timeline_written": timeline_written,
            "items_seen": sum(len(group.get("items", [])) for group in groups),
            "artifacts": {
                "groups_processed": len(groups),
                "derivation_types": list(DERIVATION_TYPES),
            },
        }
    finally:
        conn.close()
