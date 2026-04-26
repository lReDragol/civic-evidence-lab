from __future__ import annotations

import json
import logging
import math
import sqlite3
from typing import Any

from classifier.tagger_granular import infer_granular_tags
from classifier.tagger_v2 import infer_tags_v2_with_explanations
from config.db_utils import get_db, load_settings


log = logging.getLogger(__name__)

GENERIC_TAGS = {"technology", "international", "regional", "технологии", "искусственный интеллект", "ес"}
STRICT_CONTENT_PRIORS = {
    "official_profile",
    "deputy_profile",
    "executive_profile",
    "restriction_record",
    "company_report",
    "declaration",
    "anticorruption_declaration",
}
PARLIAMENT_CONTEXT = (
    "депутат",
    "госдум",
    "фракц",
    "комитет",
    "законопроект",
    "совет федерации",
    "сенатор",
    "голосован",
    "дума",
)

GENERIC_CONTEXT_RULES = {
    "искусственный интеллект": ("искусственн интеллект", "нейросет", "chatgpt", "машинн обуч", "глубок обуч"),
    "технологии": ("технолог", "цифров", "кибератак", "кибер", "vpn", "взлом", "интернет"),
    "technology": ("технолог", "цифров", "кибератак", "кибер", "vpn", "взлом", "интернет"),
    "international": ("евросоюз", "брюссель", "нато", "оон", "международ", "санкц", "китай", "сша"),
    "ес": ("евросоюз", "брюссель", "санкц", "европейск"),
    "regional": ("регион", "губернатор", "мэр", "област", "край", "республик"),
}

RISK_TAGS = {
    "possible_corruption",
    "possible_disinformation",
    "needs_verification",
    "official_confirmation",
    "surveillance_risk",
    "flagged_rhetoric",
    "high_risk",
    "manipulation",
    "contradiction",
    "unverified_claim",
    "threat",
    "false_promise",
    "conflict_of_interest",
}

EVENT_TAGS = {
    "арест",
    "задержание",
    "обыск",
    "допрос",
    "уголовное дело",
    "закон",
    "законопроект",
    "голосование",
    "блокировка",
    "иноагент",
    "митинг",
    "забастовка",
    "война",
    "спецоперация",
}


def _normalize(text: str) -> str:
    import re

    return re.sub(r"\s+", " ", (text or "").replace("ё", "е").lower()).strip()


def _namespace_for(tag_name: str, tag_level: int) -> str:
    lowered = _normalize(tag_name)
    if lowered.startswith("депутат:") or lowered.startswith("чиновник:") or lowered.startswith("орган:"):
        return "actor"
    if lowered.startswith("регион:"):
        return "region"
    if lowered.startswith("manip:") or lowered in {"document_attached", "official_confirmation"}:
        return "evidence"
    if lowered in RISK_TAGS or lowered.startswith("risk/"):
        return "risk"
    if lowered in EVENT_TAGS or tag_level == 1:
        return "event"
    if lowered in {"needs_review", "negated_claim"}:
        return "process"
    return "topic"


def _normalized_tag(tag_name: str) -> str:
    lowered = _normalize(tag_name)
    if "/" in lowered:
        return lowered.split("/", 1)[1]
    if ":" in lowered:
        return lowered.split(":", 1)[1]
    return lowered


def _calibrate_confidence(raw_score: float, support_votes: int = 1) -> float:
    centered = max(0.0, raw_score) - 0.55
    base = 1.0 / (1.0 + math.exp(-6.0 * centered))
    vote_bonus = min(0.15, max(0, support_votes - 1) * 0.05)
    return round(min(0.99, max(0.05, base + vote_bonus)), 4)


def _record_vote(
    conn: sqlite3.Connection,
    *,
    content_item_id: int,
    voter_name: str,
    tag_name: str,
    namespace: str,
    normalized_tag: str,
    vote_value: str,
    confidence_raw: float,
    evidence_text: str = "",
    metadata: dict[str, Any] | None = None,
):
    conn.execute(
        """
        INSERT INTO content_tag_votes(
            content_item_id, voter_name, tag_name, namespace, normalized_tag, vote_value,
            confidence_raw, evidence_text, metadata_json
        ) VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (
            content_item_id,
            voter_name,
            tag_name,
            namespace,
            normalized_tag,
            vote_value,
            float(confidence_raw or 0.0),
            evidence_text[:300],
            json.dumps(metadata or {}, ensure_ascii=False),
        ),
    )


def _strong_context_for_generic(normalized_text: str, normalized_tag: str) -> bool:
    patterns = GENERIC_CONTEXT_RULES.get(normalized_tag, ())
    return any(pattern in normalized_text for pattern in patterns)


def _allow_generic_tag_for_content_type(content_type: str, normalized_text: str, normalized_tag: str, support_votes: int) -> bool:
    lowered_type = _normalize(content_type)
    if lowered_type in STRICT_CONTENT_PRIORS:
        return False
    if support_votes >= 2 and _strong_context_for_generic(normalized_text, normalized_tag):
        return True
    return _strong_context_for_generic(normalized_text, normalized_tag)


def _upsert_final_tag(
    conn: sqlite3.Connection,
    *,
    content_item_id: int,
    tag_level: int,
    tag_name: str,
    namespace: str,
    normalized_tag: str,
    confidence_raw: float,
    confidence_calibrated: float,
):
    existing = conn.execute(
        """
        SELECT id FROM content_tags
        WHERE content_item_id=? AND tag_level=? AND tag_name=?
        """,
        (content_item_id, tag_level, tag_name),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE content_tags
            SET namespace=?, normalized_tag=?, confidence=?, confidence_calibrated=?, tag_source='classifier_v3', decision_source='classifier_v3'
            WHERE id=?
            """,
            (namespace, normalized_tag, confidence_raw, confidence_calibrated, existing[0]),
        )
    else:
        conn.execute(
            """
            INSERT INTO content_tags(
                content_item_id, tag_level, tag_name, namespace, normalized_tag,
                confidence, confidence_calibrated, tag_source, decision_source
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                content_item_id,
                tag_level,
                tag_name,
                namespace,
                normalized_tag,
                confidence_raw,
                confidence_calibrated,
                "classifier_v3",
                "classifier_v3",
            ),
        )


def _cleanup_strict_generic_tags(conn: sqlite3.Connection) -> int:
    content_types = ",".join("?" * len(STRICT_CONTENT_PRIORS))
    generic_tags = ",".join("?" * len(GENERIC_TAGS))
    params = tuple(sorted(STRICT_CONTENT_PRIORS)) + tuple(sorted(GENERIC_TAGS))
    existing = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM content_tags ct
        JOIN content_items ci ON ci.id = ct.content_item_id
        WHERE COALESCE(ci.content_type, '') IN ({content_types})
          AND lower(COALESCE(ct.normalized_tag, ct.tag_name, '')) IN ({generic_tags})
          AND COALESCE(ct.decision_source, '') <> 'manual_review'
        """,
        params,
    ).fetchone()
    deleted = int(existing[0] or 0) if existing else 0
    if deleted <= 0:
        return 0
    conn.execute(
        f"""
        DELETE FROM content_tags
        WHERE id IN (
            SELECT ct.id
            FROM content_tags ct
            JOIN content_items ci ON ci.id = ct.content_item_id
            WHERE COALESCE(ci.content_type, '') IN ({content_types})
              AND lower(COALESCE(ct.normalized_tag, ct.tag_name, '')) IN ({generic_tags})
              AND COALESCE(ct.decision_source, '') <> 'manual_review'
        )
        """,
        params,
    )
    return deleted


def classify_content_items(settings: dict[str, Any] | None = None, batch_size: int = 1000) -> dict[str, Any]:
    settings = settings or load_settings()
    conn = get_db(settings)
    conn.row_factory = sqlite3.Row
    try:
        cleaned_generic_tags = _cleanup_strict_generic_tags(conn)
        content_columns = {row[1] for row in conn.execute("PRAGMA table_info(content_items)").fetchall()}
        if "classification_v3_processed" in content_columns:
            conn.execute(
                """
                UPDATE content_items
                SET classification_v3_processed = 1
                WHERE COALESCE(classification_v3_processed, 0) = 0
                  AND (
                        id IN (SELECT DISTINCT content_item_id FROM content_tag_votes)
                        OR id IN (
                            SELECT DISTINCT content_item_id
                            FROM content_tags
                            WHERE COALESCE(decision_source, '')='classifier_v3'
                        )
                  )
                """
            )
        rows = conn.execute(
            """
            SELECT id, content_type, title, body_text, status
            FROM content_items
            WHERE (length(body_text) > 5 OR length(title) > 3)
              AND COALESCE(classification_v3_processed, 0) = 0
              AND COALESCE(status, '') != 'suppressed_template'
            ORDER BY id
            LIMIT ?
            """,
            (batch_size,),
        ).fetchall()
        if not rows:
            conn.commit()
            return {
                "ok": True,
                "processed": 0,
                "tags_written": 0,
                "votes_written": 0,
                "cleanup_deleted": cleaned_generic_tags,
            }

        processed = 0
        tags_written = 0
        votes_written = 0
        for row in rows:
            content_id = int(row["id"])
            content_type = str(row["content_type"] or "")
            text = f"{row['title'] or ''}\n{row['body_text'] or ''}".strip()
            norm_text = _normalize(text)
            has_parliament_context = any(ctx in norm_text for ctx in PARLIAMENT_CONTEXT)

            conn.execute("DELETE FROM content_tag_votes WHERE content_item_id=?", (content_id,))
            conn.execute(
                "DELETE FROM content_tags WHERE content_item_id=? AND COALESCE(decision_source, '')='classifier_v3'",
                (content_id,),
            )

            supported: dict[tuple[int, str], dict[str, Any]] = {}

            tags_v2, explanations = infer_tags_v2_with_explanations(text)
            for level, tag_list in tags_v2.items():
                for tag_name, score in tag_list:
                    namespace = _namespace_for(tag_name, level)
                    normalized_tag = _normalized_tag(tag_name)
                    generic = normalized_tag in GENERIC_TAGS
                    strong_context = _strong_context_for_generic(norm_text, normalized_tag)
                    vote_value = "support"
                    if generic and not _allow_generic_tag_for_content_type(content_type, norm_text, normalized_tag, 1):
                        vote_value = "abstain"
                    evidence_text = ""
                    level_explanations = explanations.get(level, [])
                    for item in level_explanations:
                        if item.get("tag_name") == tag_name:
                            evidence_text = item.get("trigger") or ""
                            break
                    _record_vote(
                        conn,
                        content_item_id=content_id,
                        voter_name=f"rule_l{level}",
                        tag_name=tag_name,
                        namespace=namespace,
                        normalized_tag=normalized_tag,
                        vote_value=vote_value,
                        confidence_raw=min(float(score or 0.0) / 20.0, 1.0),
                        evidence_text=evidence_text,
                        metadata={"score": score, "generic": generic},
                    )
                    votes_written += 1
                    if vote_value != "support":
                        continue
                    key = (level, tag_name)
                    supported.setdefault(
                        key,
                        {
                            "level": level,
                            "tag_name": tag_name,
                            "namespace": namespace,
                            "normalized_tag": normalized_tag,
                            "score_sum": 0.0,
                            "votes": 0,
                        },
                    )
                    supported[key]["score_sum"] += min(float(score or 0.0) / 20.0, 1.0)
                    supported[key]["votes"] += 1

            granular = infer_granular_tags(text)
            for tag_name in granular["keyword"]:
                namespace = _namespace_for(tag_name, 0)
                normalized_tag = _normalized_tag(tag_name)
                generic = normalized_tag in GENERIC_TAGS
                vote_value = "support" if (not generic or _allow_generic_tag_for_content_type(content_type, norm_text, normalized_tag, 1)) else "abstain"
                _record_vote(
                    conn,
                    content_item_id=content_id,
                    voter_name="granular_keyword",
                    tag_name=tag_name,
                    namespace=namespace,
                    normalized_tag=normalized_tag,
                    vote_value=vote_value,
                    confidence_raw=0.78,
                    evidence_text=tag_name,
                    metadata={"generic": generic},
                )
                votes_written += 1
                if vote_value == "support":
                    key = (0, tag_name)
                    supported.setdefault(
                        key,
                        {
                            "level": 0,
                            "tag_name": tag_name,
                            "namespace": namespace,
                            "normalized_tag": normalized_tag,
                            "score_sum": 0.0,
                            "votes": 0,
                        },
                    )
                    supported[key]["score_sum"] += 0.78
                    supported[key]["votes"] += 1

            for tag_name in granular["region"]:
                namespace = _namespace_for(tag_name, 0)
                normalized_tag = _normalized_tag(tag_name)
                _record_vote(
                    conn,
                    content_item_id=content_id,
                    voter_name="granular_region",
                    tag_name=tag_name,
                    namespace=namespace,
                    normalized_tag=normalized_tag,
                    vote_value="support",
                    confidence_raw=0.84,
                    evidence_text=tag_name,
                )
                votes_written += 1
                key = (0, tag_name)
                supported.setdefault(
                    key,
                    {
                        "level": 0,
                        "tag_name": tag_name,
                        "namespace": namespace,
                        "normalized_tag": normalized_tag,
                        "score_sum": 0.0,
                        "votes": 0,
                    },
                )
                supported[key]["score_sum"] += 0.84
                supported[key]["votes"] += 1

            for tag_name in granular["deputy"]:
                namespace = _namespace_for(tag_name, 0)
                normalized_tag = _normalized_tag(tag_name)
                vote_value = "support" if has_parliament_context else "abstain"
                _record_vote(
                    conn,
                    content_item_id=content_id,
                    voter_name="granular_actor",
                    tag_name=tag_name,
                    namespace=namespace,
                    normalized_tag=normalized_tag,
                    vote_value=vote_value,
                    confidence_raw=0.88 if has_parliament_context else 0.2,
                    evidence_text=tag_name,
                    metadata={"parliament_context": has_parliament_context},
                )
                votes_written += 1
                if vote_value == "support":
                    key = (0, tag_name)
                    supported.setdefault(
                        key,
                        {
                            "level": 0,
                            "tag_name": tag_name,
                            "namespace": namespace,
                            "normalized_tag": normalized_tag,
                            "score_sum": 0.0,
                            "votes": 0,
                        },
                    )
                    supported[key]["score_sum"] += 0.88
                    supported[key]["votes"] += 1

            for payload in supported.values():
                generic = payload["normalized_tag"] in GENERIC_TAGS
                if generic and not _allow_generic_tag_for_content_type(
                    content_type,
                    norm_text,
                    payload["normalized_tag"],
                    int(payload["votes"]),
                ):
                    continue
                raw_conf = round(payload["score_sum"] / max(1, payload["votes"]), 4)
                calibrated = _calibrate_confidence(raw_conf, payload["votes"])
                _upsert_final_tag(
                    conn,
                    content_item_id=content_id,
                    tag_level=int(payload["level"]),
                    tag_name=payload["tag_name"],
                    namespace=payload["namespace"],
                    normalized_tag=payload["normalized_tag"],
                    confidence_raw=raw_conf,
                    confidence_calibrated=calibrated,
                )
                tags_written += 1

            conn.execute(
                "UPDATE content_items SET granular_processed=1, classification_v3_processed=1 WHERE id=?",
                (content_id,),
            )
            processed += 1

        conn.commit()
        return {
            "ok": True,
            "processed": processed,
            "tags_written": tags_written,
            "votes_written": votes_written,
            "cleanup_deleted": cleaned_generic_tags,
        }
    finally:
        conn.close()
