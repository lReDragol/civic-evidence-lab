from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import urlparse

from knowledge.projection import (
    activate_generation,
    fail_generation,
    mark_generation_validated,
    start_generation,
)


SOCIAL_SOURCE_CATEGORIES = {"social", "social_media", "telegram", "vk", "youtube"}
HARD_CONTENT_TYPES = {
    "bill",
    "court_record",
    "declaration",
    "official_document",
    "procurement",
    "registry_record",
    "restriction_record",
    "transcript",
}
SIGNAL_ONLY_TYPES = {
    "co_mention",
    "community",
    "same_case",
    "same_case_cluster",
    "semantic_neighbor",
    "similarity",
}


@dataclass(frozen=True)
class PredicateSpec:
    predicate: str
    subject_roles: frozenset[str]
    object_roles: frozenset[str]


PREDICATE_REGISTRY: dict[str, PredicateSpec] = {
    "restricts": PredicateSpec(
        "restricts",
        frozenset({"actor", "executor", "issuer", "regulator"}),
        frozenset({"affected", "object", "target"}),
    ),
    "sanctions": PredicateSpec(
        "sanctions",
        frozenset({"actor", "court", "executor", "issuer", "regulator"}),
        frozenset({"affected", "object", "target"}),
    ),
    "appoints": PredicateSpec(
        "appoints",
        frozenset({"actor", "appointing_authority", "issuer"}),
        frozenset({"appointee", "object", "target"}),
    ),
    "owns": PredicateSpec(
        "owns",
        frozenset({"actor", "owner", "subject"}),
        frozenset({"asset", "company", "object", "target"}),
    ),
    "affiliated_with": PredicateSpec(
        "affiliated_with",
        frozenset({"actor", "member", "person", "subject"}),
        frozenset({"company", "object", "organization", "target"}),
    ),
    "contracts_with": PredicateSpec(
        "contracts_with",
        frozenset({"actor", "buyer", "customer", "issuer"}),
        frozenset({"contractor", "object", "supplier", "target"}),
    ),
    "votes_on": PredicateSpec(
        "votes_on",
        frozenset({"actor", "deputy", "voter"}),
        frozenset({"bill", "object", "proposal", "target"}),
    ),
    "states_about": PredicateSpec(
        "states_about",
        frozenset({"actor", "commentator", "issuer", "speaker"}),
        frozenset({"object", "subject_matter", "target"}),
    ),
}


FACT_TYPE_PREDICATES: dict[str, str] = {
    "affiliation": "affiliated_with",
    "appointment": "appoints",
    "block_start": "restricts",
    "censorship": "restricts",
    "contract": "contracts_with",
    "fine": "sanctions",
    "ownership": "owns",
    "penalty": "sanctions",
    "procurement": "contracts_with",
    "public_statement": "states_about",
    "restriction": "restricts",
    "sanction": "sanctions",
    "statement": "states_about",
    "vote": "votes_on",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _normal(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _polarity(value: Any) -> str:
    normalized = _normal(value)
    aliases = {
        "affirmed": "positive",
        "negated": "negative",
        "unknown": "uncertain",
    }
    normalized = aliases.get(normalized, normalized)
    return normalized if normalized in {"positive", "negative", "neutral", "uncertain"} else "uncertain"


def _predicate_spec(fact_type: Any, metadata: dict[str, Any]) -> PredicateSpec | None:
    requested = _normal(metadata.get("predicate"))
    if requested in PREDICATE_REGISTRY:
        return PREDICATE_REGISTRY[requested]
    predicate = FACT_TYPE_PREDICATES.get(_normal(fact_type))
    return PREDICATE_REGISTRY.get(predicate or "")


def _has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=?",
        (table_name,),
    ).fetchone() is not None


def _reactor_schema(conn: sqlite3.Connection) -> bool:
    return _has_table(conn, "facts") and _has_table(conn, "evidence_items")


def _argument_revision(row: sqlite3.Row) -> int | None:
    revision = (row["source_revision_id"] if "source_revision_id" in row.keys()
                else _json_object(row["metadata_json"]).get("source_revision_id"))
    return revision if type(revision) is int and revision > 0 else None


def _argument_group(row: sqlite3.Row) -> str:
    metadata = _json_object(row["metadata_json"])
    revision = _argument_revision(row)
    content = row["content_item_id"]
    scope = (f"revision:{revision}" if revision is not None else f"content:{content}")
    scope = f"fact:{row['fact_id']}:{scope}"
    if revision is None and content is None:
        return f"{scope}:argument:{row['id']}"
    explicit = metadata.get("argument_group") or metadata.get("relation_key")
    if explicit:
        return f"{scope}:explicit:{explicit}"
    locator = str(row["evidence_locator"] or "").strip()
    content_id = row["content_item_id"]
    if locator or content_id is not None:
        return f"{scope}:evidence:{locator}"
    return f"{scope}:argument:{row['id']}"


def _hostname(*urls: Any) -> str | None:
    for value in urls:
        if not value:
            continue
        try:
            host = (urlparse(str(value)).hostname or "").lower().strip(".")
        except ValueError:
            host = ""
        if host:
            return host[4:] if host.startswith("www.") else host
    return None


def _owner(row: sqlite3.Row, metadata: dict[str, Any]) -> str | None:
    value = metadata.get("source_owner") or row["source_owner"]
    if value and str(value).strip():
        return str(value).strip().casefold()
    return None


def _is_social(row: sqlite3.Row, metadata: dict[str, Any]) -> bool:
    category = _normal(metadata.get("source_category") or row["source_category"])
    host = _hostname(row["content_url"], row["source_url"])
    return category in SOCIAL_SOURCE_CATEGORIES or host in {"t.me", "telegram.me", "vk.com"}


def _is_primary(row: sqlite3.Row, metadata: dict[str, Any]) -> bool:
    if bool(metadata.get("is_primary")) or bool(row["is_official"]):
        return True
    category = _normal(row["source_category"])
    return category.startswith("official") or category in {"court", "government", "registry"}


def _tier_rank(tier: str) -> int:
    return {"E0": 0, "E1": 1, "E2": 2, "E3": 3}.get(tier, 0)


def _evidence_tier(row: sqlite3.Row, metadata: dict[str, Any]) -> str:
    if row["source_revision_id"] is None:
        return "E0"
    verdict = _normal(metadata.get("authenticity_verdict"))
    if verdict in {"disputed", "fake", "rejected"}:
        return "E0"

    social = _is_social(row, metadata)
    primary = _is_primary(row, metadata)
    hard = _normal(row["evidence_class"]) in {"hard", "primary", "official"}
    hard = hard or _normal(row["source_strength"]) in {"hard", "primary", "strong"}
    hard = hard or _normal(row["content_type"]) in HARD_CONTENT_TYPES

    requested = str(metadata.get("evidence_tier") or "").upper()
    if requested == "E3" and primary and hard and not social:
        return "E3"
    if requested in {"E2", "E3"} and not social:
        return "E2"
    if requested == "E1":
        return "E1"
    if primary and hard and not social:
        return "E3"
    if not social and (hard or _normal(row["credibility_tier"]) in {"a", "b"}):
        return "E2"
    return "E1"


def _float(metadata: dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        value = float(metadata.get(key, default))
        return max(0.0, min(1.0, value)) if math.isfinite(value) else 0.0
    except (TypeError, ValueError):
        return default


def _natural_key(
    fact_id: int,
    subject_id: int,
    predicate: str,
    object_id: int,
    polarity: str,
    valid_from: Any,
    valid_to: Any,
) -> str:
    payload = "|".join(
        str(value or "")
        for value in (fact_id, subject_id, predicate, object_id, polarity, valid_from, valid_to)
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _fact_arguments(conn: sqlite3.Connection, fact_id: int) -> list[sqlite3.Row]:
    if _reactor_schema(conn):
        return conn.execute(
            """
            SELECT fa.id, fa.fact_id, fa.entity_id, fa.argument_role,
                   NULL AS content_item_id, fa.source_revision_id,
                   fa.evidence_locator, fa.span_start, fa.span_end, fa.text_span,
                   fa.confidence, fa.inference_method, fa.metadata_json,
                   e.entity_type
            FROM fact_arguments fa
            JOIN entities e ON e.id=fa.entity_id
            WHERE fa.fact_id=?
            ORDER BY fa.id
            """,
            (int(fact_id),),
        ).fetchall()
    return conn.execute(
        """
        SELECT fa.*, e.entity_type
        FROM fact_arguments fa
        JOIN entities e ON e.id=fa.entity_id
        LEFT JOIN projection_generations pg ON pg.id=fa.generation_id
        WHERE fa.fact_id=?
          AND (fa.generation_id IS NULL OR pg.status='active')
        ORDER BY fa.id
        """,
        (int(fact_id),),
    ).fetchall()


def _explicit_pairs(
    arguments: Iterable[sqlite3.Row], spec: PredicateSpec
) -> list[tuple[sqlite3.Row, sqlite3.Row]]:
    groups: dict[str, list[sqlite3.Row]] = {}
    for row in arguments:
        if _normal(row["entity_type"]) == "location":
            continue
        groups.setdefault(_argument_group(row), []).append(row)

    pairs: list[tuple[sqlite3.Row, sqlite3.Row]] = []
    for rows in groups.values():
        subjects = [row for row in rows if _normal(row["argument_role"]) in spec.subject_roles]
        objects = [row for row in rows if _normal(row["argument_role"]) in spec.object_roles]
        # Ambiguous groups must be resolved upstream; expanding them would manufacture relations.
        if len(subjects) != 1 or len(objects) != 1:
            continue
        if int(subjects[0]["entity_id"]) == int(objects[0]["entity_id"]):
            continue
        pairs.append((subjects[0], objects[0]))
    return pairs


def _fact_evidence(conn: sqlite3.Connection, fact_id: int) -> list[dict[str, Any]]:
    if _reactor_schema(conn):
        rows = conn.execute(
            """
            SELECT ei.id AS evidence_item_id, ei.source_revision_id,
                   ei.evidence_tier, ei.source_owner, ei.locator,
                   ei.text_span, ei.entailment_score, ei.authenticity_score,
                   ei.verification_state, ei.metadata_json,
                   fe.evidence_class, sr.source_object_id, sr.payload_hash,
                   ss.source_type, ss.policy_json
            FROM fact_evidence fe
            JOIN evidence_items ei ON ei.id=fe.evidence_item_id
            JOIN source_revisions sr ON sr.id=ei.source_revision_id
            JOIN source_objects so ON so.id=sr.source_object_id
            JOIN source_systems ss ON ss.id=so.source_system_id
            WHERE fe.fact_id=?
            ORDER BY ei.id
            """,
            (int(fact_id),),
        ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            metadata = _json_object(row["metadata_json"])
            source_policy = _json_object(row["policy_json"])
            tier = str(row["evidence_tier"] or "E0").upper()
            social = _normal(row["source_type"]) in SOCIAL_SOURCE_CATEGORIES
            primary = bool(source_policy.get("official")) or tier == "E3"
            result.append(
                {
                    "evidence_item_id": int(row["evidence_item_id"]),
                    "fact_evidence_id": None,
                    "content_item_id": None,
                    "source_revision_id": int(row["source_revision_id"]),
                    "source_object_id": row["source_object_id"],
                    "payload_hash": row["payload_hash"],
                    "evidence_class": row["evidence_class"],
                    "verification_state": row["verification_state"],
                    "evidence_tier": tier,
                    "entailment_score": _float(dict(row), "entailment_score"),
                    "authenticity_score": _float(dict(row), "authenticity_score"),
                    "source_reliability": _float(metadata, "source_reliability", 1.0 if tier == "E3" else 0.75),
                    "source_owner": str(row["source_owner"] or "").casefold() or None,
                    "evidence_locator": row["locator"],
                    "text_span": row["text_span"],
                    "primary": primary,
                    "social": social,
                    "metadata": {
                        **metadata,
                        "primary": primary,
                        "social": social,
                        "verification_state": metadata.get("verification_state") or row["verification_state"],
                    },
                }
            )
        return result
    rows = conn.execute(
        """
        SELECT fe.id AS fact_evidence_id,
               fe.content_item_id,
               fe.document_content_id,
               fe.evidence_type,
               fe.evidence_class,
               fe.source_strength,
               fe.metadata_json,
               ci.id AS evidence_content_id,
               ci.content_type,
               ci.url AS content_url,
               s.id AS source_id,
               s.category AS source_category,
               s.is_official,
               s.credibility_tier,
               s.owner AS source_owner,
               s.url AS source_url,
               sr.id AS source_revision_id,
               sr.source_object_id, sr.payload_hash,
               sr.observed_at AS revision_observed_at
        FROM fact_evidence fe
        LEFT JOIN content_items ci
          ON ci.id=COALESCE(fe.document_content_id, fe.content_item_id)
        LEFT JOIN sources s ON s.id=ci.source_id
        LEFT JOIN source_revisions sr
          ON sr.content_item_id=ci.id AND sr.is_current=1
        WHERE fe.fact_id=? AND fe.superseded_at IS NULL
        ORDER BY fe.id, sr.id DESC
        """,
        (int(fact_id),),
    ).fetchall()

    result: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for row in rows:
        metadata = _json_object(row["metadata_json"])
        tier = _evidence_tier(row, metadata)
        owner = _owner(row, metadata)
        locator = str(
            metadata.get("evidence_locator")
            or metadata.get("locator")
            or (f"revision:{row['source_revision_id']}" if row["source_revision_id"] else "")
        )
        key = (
            row["fact_evidence_id"],
            row["source_revision_id"],
            row["evidence_content_id"],
            locator,
        )
        if key in seen:
            continue
        seen.add(key)
        primary = _is_primary(row, metadata)
        social = _is_social(row, metadata)
        result.append(
            {
                "fact_evidence_id": row["fact_evidence_id"],
                "content_item_id": row["evidence_content_id"],
                "source_revision_id": row["source_revision_id"],
                "source_object_id": row["source_object_id"],
                "payload_hash": row["payload_hash"],
                "evidence_class": row["evidence_class"],
                "evidence_type": row["evidence_type"],
                "verification_state": metadata.get("verification_state") or metadata.get("authenticity_verdict"),
                "evidence_tier": tier,
                "entailment_score": _float(metadata, "entailment_score"),
                "authenticity_score": _float(
                    metadata, "authenticity_score",
                    1.0 if _normal(metadata.get("authenticity_verdict")) == "confirmed" else 0.0
                ),
                "source_reliability": _float(
                    metadata,
                    "source_reliability",
                    1.0 if tier == "E3" else (0.75 if tier == "E2" else 0.35),
                ),
                "source_owner": owner,
                "evidence_locator": locator or None,
                "text_span": metadata.get("evidence_quote") or metadata.get("text_span"),
                "primary": primary,
                "social": social,
                "metadata": {
                    **metadata,
                    "content_type": row["content_type"],
                    "evidence_class": metadata.get("evidence_class") or row["evidence_class"],
                    "source_category": row["source_category"],
                    "source_id": row["source_id"],
                    "source_strength": row["source_strength"],
                    "primary": primary,
                    "social": social,
                },
            }
        )
    return result


def _supports_assertion(item: dict[str, Any]) -> bool:
    metadata = item.get("metadata", {})
    # Verification and stance are independent of a source's prestige or evidence tier.
    if _normal(item.get("verification_state")) not in {"verified", "confirmed", "accepted", "authentic"}:
        return False
    for key in ("verification_state", "authenticity_verdict"):
        if metadata.get(key) and _normal(metadata[key]) not in {"verified", "confirmed", "accepted", "authentic"}:
            return False
    allowed_stances = {"", "support", "supports", "supporting", "document", "hard", "primary", "official", "evidence"}
    if any(_normal(value) not in allowed_stances for value in (
        item.get("evidence_class"), item.get("evidence_type"),
        metadata.get("evidence_class"), metadata.get("stance"),
    )):
        return False
    return item.get("source_revision_id") is not None and _float(item, "authenticity_score") >= 0.80


def _independent(left: dict[str, Any], right: dict[str, Any]) -> bool:
    if not left.get("source_owner") or not right.get("source_owner"):
        return False
    if _normal(left["source_owner"]) == _normal(right["source_owner"]):
        return False
    left_meta, right_meta = left.get("metadata", {}), right.get("metadata", {})
    if any(meta.get("independence_verified") is not True for meta in (left_meta, right_meta)):
        return False
    reviewed_origins = False
    for key in ("origin_key", "independence_group"):
        a, b = left_meta.get(key), right_meta.get(key)
        unknown = {"", "unknown", "unverified", "none", "null", "n/a"}
        if (isinstance(a, str) and isinstance(b, str)
                and _normal(a) not in unknown and _normal(b) not in unknown):
            if _normal(a) == _normal(b):
                return False
            reviewed_origins = True
    if not reviewed_origins:
        return False
    # Explicit provenance can disprove independence even across different owners.
    for key in ("source_revision_id", "source_object_id", "payload_hash"):
        if left.get(key) is not None and left.get(key) == right.get(key):
            return False
    for key in ("origin_id", "origin_key", "syndication_id", "independence_group", "original_source", "upstream_source"):
        a, b = left.get("metadata", {}).get(key), right.get("metadata", {}).get(key)
        if a and b and _normal(a) == _normal(b):
            return False
    return True


def _pair_evidence(
    evidence: list[dict[str, Any]],
    pairs: list[tuple[sqlite3.Row, sqlite3.Row]],
    predicate: str,
    polarity: str,
) -> list[dict[str, Any]]:
    def matches(item: dict[str, Any], subject: sqlite3.Row, obj: sqlite3.Row) -> bool:
        revision = _argument_revision(subject)
        if revision is None or revision != _argument_revision(obj):
            return False
        if item["source_revision_id"] == revision:
            return True
        # Cross-revision entailment must be reviewed for this exact directed pair and locator.
        expected = {
            "fact_id": subject["fact_id"],
            "subject_entity_id": subject["entity_id"],
            "object_entity_id": obj["entity_id"],
            "predicate": predicate,
            "polarity": polarity,
            "argument_revision_id": revision,
            "subject_locator": subject["evidence_locator"],
            "object_locator": obj["evidence_locator"],
        }
        if not expected["subject_locator"] or not expected["object_locator"]:
            return False
        reviews = item.get("metadata", {}).get("pair_entailments")
        if not isinstance(reviews, list):
            return False
        return any(
            isinstance(review, dict) and review.get("verified") is True
            and all(type(review.get(key)) is type(value) and review[key] == value
                    for key, value in expected.items())
            for review in reviews
        )

    return [item for item in evidence if any(matches(item, subject, obj) for subject, obj in pairs)]


def _promotion(evidence: list[dict[str, Any]], polarity: str) -> tuple[str, str | None]:
    if polarity not in {"positive", "negative"}:
        return ("review" if evidence else "candidate"), "non_assertive_polarity"
    supporting = [item for item in evidence if _supports_assertion(item)]
    if any(
        item["evidence_tier"] == "E3" and item["entailment_score"] >= 0.92
        and item["primary"] and not item["social"]
        for item in supporting
    ):
        return "promoted", "single_primary_E3"

    eligible = [
        item
        for item in supporting
        if _tier_rank(item["evidence_tier"]) >= 2
        and item["entailment_score"] >= 0.80
        and item["source_owner"]
    ]
    if any(
        _independent(left, right)
        and any(not item["social"] or item["primary"] for item in (left, right))
        for index, left in enumerate(eligible) for right in eligible[index + 1:]
    ):
        return "promoted", "two_independent_E2_plus"
    return ("review" if evidence else "candidate"), "insufficient_independent_evidence"


def record_relation_signal(
    conn: sqlite3.Connection,
    *,
    generation_id: int,
    entity_a_id: int,
    entity_b_id: int,
    signal_type: str,
    source_unit_key: str,
    signal_score: float = 0.0,
    observed_at: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> int:
    normalized_type = _normal(signal_type)
    if normalized_type not in SIGNAL_ONLY_TYPES:
        raise ValueError(f"Unsupported relation signal type: {signal_type}")
    entity_a_id, entity_b_id = sorted((int(entity_a_id), int(entity_b_id)))
    if entity_a_id == entity_b_id:
        raise ValueError("A relation signal requires two distinct entities")
    conn.execute(
        """
        INSERT OR IGNORE INTO relation_signals(
            generation_id, entity_a_id, entity_b_id, signal_type, signal_score,
            source_unit_key, observed_at, metadata_json
        ) VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            int(generation_id),
            entity_a_id,
            entity_b_id,
            normalized_type,
            max(0.0, min(1.0, float(signal_score))),
            str(source_unit_key),
            observed_at,
            json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
        ),
    )
    row = conn.execute(
        """
        SELECT id FROM relation_signals
        WHERE generation_id=? AND entity_a_id=? AND entity_b_id=?
          AND signal_type=? AND source_unit_key=?
        """,
        (generation_id, entity_a_id, entity_b_id, normalized_type, str(source_unit_key)),
    ).fetchone()
    conn.commit()
    return int(row[0])


def _insert_assertion(
    conn: sqlite3.Connection,
    *,
    generation_id: int,
    fact: sqlite3.Row,
    spec: PredicateSpec,
    subject: sqlite3.Row,
    obj: sqlite3.Row,
    evidence: list[dict[str, Any]],
) -> int:
    polarity = _polarity(fact["polarity"])
    state, reason = _promotion(evidence, polarity)
    entailment = max((item["entailment_score"] for item in evidence if _supports_assertion(item)), default=0.0)
    confidence = min(
        max(0.0, float(fact["confidence"] or 0.0)),
        max(0.0, float(subject["confidence"] or 0.0)),
        max(0.0, float(obj["confidence"] or 0.0)),
        entailment,
    )
    natural_key = _natural_key(
        int(fact["id"]),
        int(subject["entity_id"]),
        spec.predicate,
        int(obj["entity_id"]),
        polarity,
        fact["valid_from"],
        fact["valid_to"],
    )
    cursor = conn.execute(
        """
        INSERT INTO relation_assertions(
            generation_id, natural_key, subject_entity_id, predicate, object_entity_id,
            fact_id, event_id, polarity, confidence, state, valid_from, valid_to,
            observed_at, promotion_reason, metadata_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            generation_id,
            natural_key,
            int(subject["entity_id"]),
            spec.predicate,
            int(obj["entity_id"]),
            int(fact["id"]),
            int(fact["event_id"]),
            polarity,
            confidence,
            state,
            fact["valid_from"],
            fact["valid_to"],
            fact["observed_at"],
            reason,
            json.dumps(
                {
                    "argument_group": _argument_group(subject),
                    "fact_type": fact["fact_type"],
                    "subject_argument_id": subject["id"],
                    "object_argument_id": obj["id"],
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
        ),
    )
    assertion_id = int(cursor.lastrowid)
    for item in evidence:
        if not _supports_assertion(item):
            continue
        if _reactor_schema(conn):
            conn.execute(
                "INSERT OR IGNORE INTO relation_assertion_evidence(assertion_id,evidence_item_id) VALUES(?,?)",
                (assertion_id, int(item["evidence_item_id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO relation_assertion_evidence(
                    assertion_id, fact_evidence_id, content_item_id, source_revision_id,
                    evidence_tier, entailment_score, authenticity_score, source_reliability,
                    source_owner, evidence_locator, text_span, metadata_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    assertion_id,
                    item["fact_evidence_id"],
                    item["content_item_id"],
                    item["source_revision_id"],
                    item["evidence_tier"],
                    item["entailment_score"],
                    item["authenticity_score"],
                    item["source_reliability"],
                    item["source_owner"],
                    item["evidence_locator"],
                    item["text_span"],
                    json.dumps(item["metadata"], ensure_ascii=False, sort_keys=True),
                ),
            )
    conn.execute(
        """
        INSERT INTO projection_generation_members(
            generation_id, object_type, object_id, natural_key, payload_hash
        ) VALUES(?,?,?,?,?)
        """,
        (generation_id, "relation_assertion", assertion_id, natural_key, natural_key),
    )
    return assertion_id


def _validate_generation(conn: sqlite3.Connection, generation_id: int) -> dict[str, int]:
    assertions = int(
        conn.execute(
            "SELECT COUNT(*) FROM relation_assertions WHERE generation_id=?", (generation_id,)
        ).fetchone()[0]
    )
    locations = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM relation_assertions ra
            JOIN entities s ON s.id=ra.subject_entity_id
            JOIN entities o ON o.id=ra.object_entity_id
            WHERE ra.generation_id=?
              AND (lower(s.entity_type)='location' OR lower(o.entity_type)='location')
            """,
            (generation_id,),
        ).fetchone()[0]
    )
    missing_arguments = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM relation_assertions ra
            WHERE ra.generation_id=? AND (
                NOT EXISTS(
                    SELECT 1 FROM fact_arguments fa
                    WHERE fa.fact_id=ra.fact_id AND fa.entity_id=ra.subject_entity_id
                ) OR NOT EXISTS(
                    SELECT 1 FROM fact_arguments fa
                    WHERE fa.fact_id=ra.fact_id AND fa.entity_id=ra.object_entity_id
                )
            )
            """,
            (generation_id,),
        ).fetchone()[0]
    )
    invalid_promotions = 0
    promoted_ids = conn.execute(
        "SELECT id, polarity, fact_id, predicate, subject_entity_id, object_entity_id "
        "FROM relation_assertions WHERE generation_id=? AND state='promoted'",
        (generation_id,),
    ).fetchall()
    for assertion_id, polarity, fact_id, predicate, subject_id, object_id in promoted_ids:
        if _reactor_schema(conn):
            evidence_key = "evidence_item_id"
        else:
            evidence_key = "fact_evidence_id"
        linked = {row[0] for row in conn.execute(
            f"SELECT {evidence_key} FROM relation_assertion_evidence WHERE assertion_id=?",
            (assertion_id,),
        )}
        evidence = [item for item in _fact_evidence(conn, fact_id) if item[evidence_key] in linked]
        spec = PREDICATE_REGISTRY.get(predicate)
        pairs = ([] if spec is None else [
            (subject, obj) for subject, obj in _explicit_pairs(_fact_arguments(conn, fact_id), spec)
            if subject["entity_id"] == subject_id and obj["entity_id"] == object_id
        ])
        scoped = _pair_evidence(evidence, pairs, predicate, polarity)
        if len(scoped) != len(evidence) or _promotion(scoped, str(polarity))[0] != "promoted":
            invalid_promotions += 1

    metrics = {
        "assertions": assertions,
        "invalid_promotions": invalid_promotions,
        "location_assertions": locations,
        "missing_fact_arguments": missing_arguments,
        "promoted": len(promoted_ids),
    }
    if locations or missing_arguments or invalid_promotions:
        raise RuntimeError(f"Relation generation failed validation: {metrics}")
    return metrics


def rebuild_relation_assertions(
    conn: sqlite3.Connection,
    *,
    generation_key: str | None = None,
    source_watermark: str | None = None,
) -> dict[str, Any]:
    """Build and atomically activate a precision-first relation projection."""
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    generation_id: int | None = None
    try:
        generation_key = generation_key or f"relations:{_now_iso()}"
        generation_id = start_generation(
            conn,
            projection_type="relations",
            generation_key=generation_key,
            source_watermark=source_watermark,
        )
        conn.execute("BEGIN IMMEDIATE")
        if _reactor_schema(conn):
            facts = conn.execute(
                """
                SELECT id,generation_id,event_id,fact_type,predicate,canonical_text,
                       polarity,modality,confidence,valid_from,valid_to,observed_at,
                       metadata_json
                FROM current_facts_v
                ORDER BY id
                """
            ).fetchall()
        else:
            facts = conn.execute(
                """
                SELECT * FROM event_facts
                WHERE superseded_at IS NULL
                ORDER BY id
                """
            ).fetchall()
        try:
            for fact in facts:
                metadata = _json_object(fact["metadata_json"])
                if _reactor_schema(conn):
                    # Reactor stores the canonical predicate in a column, not metadata.
                    predicate = _normal(fact["predicate"])
                    if predicate not in PREDICATE_REGISTRY:
                        continue
                    metadata["predicate"] = predicate
                    if _normal(fact["modality"]) != "asserted":
                        continue
                spec = _predicate_spec(fact["fact_type"], metadata)
                if spec is None:
                    continue
                arguments = _fact_arguments(conn, int(fact["id"]))
                pairs = _explicit_pairs(arguments, spec)
                if not pairs:
                    continue
                evidence = _fact_evidence(conn, int(fact["id"]))
                grouped_pairs: dict[tuple[int, int], list[tuple[sqlite3.Row, sqlite3.Row]]] = {}
                for subject, obj in pairs:
                    pair = (int(subject["entity_id"]), int(obj["entity_id"]))
                    grouped_pairs.setdefault(pair, []).append((subject, obj))
                for occurrences in grouped_pairs.values():
                    subject, obj = occurrences[0]
                    _insert_assertion(
                        conn,
                        generation_id=generation_id,
                        fact=fact,
                        spec=spec,
                        subject=subject,
                        obj=obj,
                        evidence=_pair_evidence(evidence, occurrences, spec.predicate, _polarity(fact["polarity"])),
                    )
            metrics = _validate_generation(conn, generation_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        mark_generation_validated(conn, generation_id, metrics=metrics)
        activate_generation(conn, generation_id)
        return {
            "generation_id": generation_id,
            "generation_key": generation_key,
            **metrics,
        }
    except Exception as exc:
        conn.rollback()
        if generation_id is not None:
            fail_generation(conn, generation_id, str(exc))
        raise
    finally:
        conn.row_factory = previous_row_factory


__all__ = [
    "FACT_TYPE_PREDICATES",
    "PREDICATE_REGISTRY",
    "PredicateSpec",
    "rebuild_relation_assertions",
    "record_relation_signal",
]
