from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from classifier.tagger_v3 import GENERIC_TAGS, STRICT_CONTENT_PRIORS
from config.db_utils import PROJECT_ROOT, get_db, load_settings
from config.source_health import (
    HEALTHY_STATES,
    acceptance_mode,
    effective_source_state,
    fallback_urls,
    load_source_health_manifest,
    manifest_entry,
    match_warning_source,
    primary_urls,
    required_for_gate,
    smoke_fixture,
)
from runtime.state import _classify_failure_class
from runtime.state import get_runtime_metadata, now_iso


DEFAULT_CONFIG = {
    "strict_gate": True,
    "max_degraded_sources": 0,
    "max_critical_warning_jobs": 0,
    "max_zero_support_review_candidates": 0,
    "max_generic_promoted_candidates": 0,
    "max_fake_domain_diversity_candidates": 0,
    "max_promoted_without_nonseed_bridge": 0,
    "max_promoted_same_case_cluster": 0,
    "max_promoted_location_candidates": 0,
    "max_promoted_without_event_fact_or_official_bridge": 0,
    "max_duplicate_amplified_promotions": 0,
    "max_duplicate_leakage": 0,
    "report_path": str(PROJECT_ROOT / "reports" / "qa_quality_latest.json"),
}
GENERIC_RELATION_LOCATIONS = {"россии", "россия", "москва", "москвы", "рф"}
PROMOTION_BRIDGE_TYPES = {
    "Event",
    "Fact",
    "Bill",
    "Contract",
    "VotePattern",
    "RestrictionEvent",
    "Affiliation",
    "Disclosure",
    "Asset",
}
EVENT_FACT_OR_OFFICIAL_BRIDGE_TYPES = {
    "Event",
    "Fact",
    "RestrictionEvent",
    "Affiliation",
    "Disclosure",
    "Asset",
    "OfficialDocument",
}


def _config(settings: dict[str, Any]) -> dict[str, Any]:
    cfg = dict(DEFAULT_CONFIG)
    cfg.update(settings.get("quality_gate", {}) or {})
    return cfg


def _report_path(settings: dict[str, Any]) -> Path:
    configured = _config(settings).get("report_path") or DEFAULT_CONFIG["report_path"]
    path = Path(configured)
    return path if path.is_absolute() else (PROJECT_ROOT / path)


def _critical_warning_rows(rows: list[tuple[str, str, str]]) -> list[dict[str, str]]:
    critical_rows: list[dict[str, str]] = []
    for job_id, started_at, warnings_json in rows:
        try:
            warnings = json.loads(warnings_json or "[]")
        except json.JSONDecodeError:
            warnings = [warnings_json] if warnings_json else []
        if not isinstance(warnings, list):
            warnings = [str(warnings)]
        for warning in warnings:
            warning_text = str(warning or "").strip()
            if not warning_text:
                continue
            failure_class = _classify_failure_class(error_text=warning_text)
            lowered = warning_text.lower()
            if failure_class in {"timeout", "tls", "not_found", "bad_asset"} or "snapshot_not_found" in lowered:
                critical_rows.append(
                    {
                        "job_id": job_id,
                        "started_at": started_at,
                        "warning": warning_text,
                        "failure_class": failure_class or "quality_warning",
                    }
                )
    return critical_rows


def _append_once(items: list[str], value: str) -> None:
    if value not in items:
        items.append(value)


def _parse_json(raw_text: str | None, default: Any) -> Any:
    if not raw_text:
        return default
    try:
        return json.loads(raw_text)
    except (TypeError, json.JSONDecodeError):
        return default


def _is_generic_relation_entity(entity_type: str, canonical_name: str) -> bool:
    lowered = str(canonical_name or "").strip().lower()
    if entity_type == "location" and lowered in GENERIC_RELATION_LOCATIONS:
        return True
    return False


def _review_task_source_links(row: dict[str, Any]) -> list[str]:
    links: list[str] = []
    for value in row.get("primary_urls") or []:
        if value and value not in links:
            links.append(str(value))
    for value in row.get("fallback_urls") or []:
        if value and value not in links:
            links.append(str(value))
    if row.get("fixture_path"):
        links.append(str(row["fixture_path"]))
    return links


def _sync_source_review_tasks(conn, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    try:
        from enrichment.common import ensure_review_task
    except Exception:
        return
    for row in rows:
        ensure_review_task(
            conn,
            task_key=f"source:{row['source_key']}",
            queue_key="sources",
            subject_type="source_health",
            candidate_payload=row,
            suggested_action="needs_more_docs" if row["effective_state"] == "blocked" else "promote",
            confidence=0.99 if row["effective_state"] == "blocked" else 0.91,
            machine_reason=row.get("failure_class") or row.get("quality_issue") or row.get("effective_state"),
            source_links=_review_task_source_links(row),
        )
    conn.commit()


def _sync_relation_review_tasks(conn, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    try:
        from enrichment.common import ensure_review_task
    except Exception:
        return
    for row in rows:
        suggested_action = "reject"
        if row["issue"] in {"promoted_without_nonseed_bridge", "promoted_without_event_fact_or_official_bridge", "blocked_official_bridge"}:
            suggested_action = "needs_more_docs"
        ensure_review_task(
            conn,
            task_key=f"relation:{row['candidate_id']}:{row['issue']}",
            queue_key="relations",
            subject_type="relation_candidate",
            subject_id=int(row["candidate_id"]),
            candidate_payload=row,
            suggested_action=suggested_action,
            confidence=0.97,
            machine_reason=row["issue"],
            source_links=row.get("source_links", []),
        )
    conn.commit()


def _relation_source_links(conn, candidate_id: int) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT COALESCE(NULLIF(ci.url, ''), NULLIF(s.url, ''), '')
        FROM relation_support rs
        LEFT JOIN content_items ci ON ci.id = COALESCE(rs.content_item_id, rs.evidence_item_id)
        LEFT JOIN sources s ON s.id = COALESCE(rs.source_id, ci.source_id)
        WHERE rs.candidate_id = ?
        """
        ,
        (int(candidate_id),),
    ).fetchall()
    return [str(row[0]) for row in rows if row and row[0]]


def build_quality_gate(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    cfg = _config(settings)
    report_path = _report_path(settings)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = load_source_health_manifest(settings)

    conn = get_db(settings)
    conn.row_factory = None
    try:
        classifier_status = get_runtime_metadata(conn, "classifier_audit_last_status", "unknown")
        classifier_report = get_runtime_metadata(conn, "classifier_audit_last_report", {}) or {}
        reviewed_baseline_ready = bool((classifier_report or {}).get("reviewed_baseline_ready"))

        generic_false_positive_rows = conn.execute(
            """
            SELECT
                ci.content_type,
                ct.tag_name,
                COUNT(*) AS total
            FROM content_tags ct
            JOIN content_items ci ON ci.id = ct.content_item_id
            WHERE COALESCE(ct.decision_source, '')='classifier_v3'
              AND COALESCE(ci.content_type, '') IN ({content_types})
              AND lower(COALESCE(ct.normalized_tag, ct.tag_name, '')) IN ({generic_tags})
            GROUP BY ci.content_type, ct.tag_name
            ORDER BY total DESC, ci.content_type, ct.tag_name
            LIMIT 30
            """.format(
                content_types=",".join("?" * len(STRICT_CONTENT_PRIORS)),
                generic_tags=",".join("?" * len(GENERIC_TAGS)),
            ),
            tuple(sorted(STRICT_CONTENT_PRIORS)) + tuple(sorted(GENERIC_TAGS)),
        ).fetchall()

        zero_support_rows = conn.execute(
            """
            SELECT id, entity_a_id, entity_b_id, candidate_type, candidate_state, support_items, support_sources, support_domains
            FROM relation_candidates
            WHERE candidate_state IN ('review', 'promoted')
              AND COALESCE(support_items, 0) = 0
            ORDER BY id
            LIMIT 100
            """
        ).fetchall()
        promoted_rows = conn.execute(
            """
            SELECT
                rc.id,
                rc.entity_a_id,
                rc.entity_b_id,
                rc.candidate_type,
                rc.support_items,
                rc.support_sources,
                rc.support_domains,
                COALESCE(rc.explain_path_json, '[]') AS explain_path_json,
                COALESCE(rc.evidence_mix_json, '{}') AS evidence_mix_json,
                COALESCE(ea.entity_type, '') AS entity_a_type,
                COALESCE(ea.canonical_name, '') AS entity_a_name,
                COALESCE(eb.entity_type, '') AS entity_b_type,
                COALESCE(eb.canonical_name, '') AS entity_b_name,
                COALESCE(rf.dedupe_support_score, 0) AS dedupe_support_score
            FROM relation_candidates rc
            LEFT JOIN entities ea ON ea.id = rc.entity_a_id
            LEFT JOIN entities eb ON eb.id = rc.entity_b_id
            LEFT JOIN relation_features rf ON rf.candidate_id = rc.id
            WHERE rc.candidate_state='promoted'
            ORDER BY rc.id
            """
        ).fetchall()
        fake_domain_rows = conn.execute(
            """
            SELECT
                rc.id,
                rc.entity_a_id,
                rc.entity_b_id,
                rc.candidate_type,
                SUM(
                    CASE
                        WHEN rs.support_class='evidence'
                         AND COALESCE(rs.domain, '') <> ''
                         AND (rs.domain LIKE 'source:%' OR instr(rs.domain, '.') = 0)
                        THEN 1 ELSE 0
                    END
                ) AS fake_domain_rows,
                COUNT(
                    DISTINCT CASE
                        WHEN rs.support_class='evidence'
                         AND COALESCE(rs.domain, '') <> ''
                         AND rs.domain NOT LIKE 'source:%'
                         AND instr(rs.domain, '.') > 0
                        THEN rs.domain
                    END
                ) AS real_domain_count
            FROM relation_candidates rc
            JOIN relation_support rs ON rs.candidate_id = rc.id
            WHERE rc.candidate_state IN ('review', 'promoted')
            GROUP BY rc.id, rc.entity_a_id, rc.entity_b_id, rc.candidate_type
            HAVING fake_domain_rows > 0 OR (MAX(COALESCE(rc.support_domains, 0)) > 0 AND real_domain_count = 0)
            ORDER BY rc.id
            """
        ).fetchall()

        generic_promoted_rows: list[dict[str, Any]] = []
        promoted_same_case_rows: list[dict[str, Any]] = []
        promoted_location_rows: list[dict[str, Any]] = []
        promoted_without_nonseed_bridge: list[dict[str, Any]] = []
        promoted_without_event_fact_or_official_bridge: list[dict[str, Any]] = []
        duplicate_amplified_promotions: list[dict[str, Any]] = []
        relation_review_rows: list[dict[str, Any]] = []
        for row in promoted_rows:
            (
                candidate_id,
                entity_a_id,
                entity_b_id,
                candidate_type,
                support_items,
                support_sources,
                support_domains,
                explain_path_json,
                evidence_mix_json,
                entity_a_type,
                entity_a_name,
                entity_b_type,
                entity_b_name,
                dedupe_support_score,
            ) = row
            explain_path = _parse_json(explain_path_json, [])
            evidence_mix = _parse_json(evidence_mix_json, {})
            bridge_types = {str(node.get("node_type") or "") for node in explain_path if isinstance(node, dict)}
            if candidate_type == "same_case_cluster":
                payload = {
                    "candidate_id": int(candidate_id),
                    "issue": "demoted_same_case",
                    "candidate_type": candidate_type,
                    "entity_a_id": int(entity_a_id),
                    "entity_b_id": int(entity_b_id),
                }
                promoted_same_case_rows.append(payload)
                relation_review_rows.append(payload)
            if entity_a_type == "location" or entity_b_type == "location":
                payload = {
                    "candidate_id": int(candidate_id),
                    "issue": "location_role_only",
                    "candidate_type": candidate_type,
                    "entity_a_id": int(entity_a_id),
                    "entity_b_id": int(entity_b_id),
                    "entity_a_type": entity_a_type,
                    "entity_a_name": entity_a_name,
                    "entity_b_type": entity_b_type,
                    "entity_b_name": entity_b_name,
                }
                promoted_location_rows.append(payload)
                relation_review_rows.append(payload)
            if _is_generic_relation_entity(entity_a_type, entity_a_name) or _is_generic_relation_entity(entity_b_type, entity_b_name):
                payload = {
                    "candidate_id": int(candidate_id),
                    "issue": "generic_entity_promotion",
                    "candidate_type": candidate_type,
                    "entity_a_id": int(entity_a_id),
                    "entity_b_id": int(entity_b_id),
                    "entity_a_name": entity_a_name,
                    "entity_b_name": entity_b_name,
                }
                generic_promoted_rows.append(payload)
                relation_review_rows.append(payload)
            if not (bridge_types & PROMOTION_BRIDGE_TYPES):
                payload = {
                    "candidate_id": int(candidate_id),
                    "issue": "promoted_without_nonseed_bridge",
                    "candidate_type": candidate_type,
                    "entity_a_id": int(entity_a_id),
                    "entity_b_id": int(entity_b_id),
                    "bridge_types": sorted(bridge_types),
                }
                promoted_without_nonseed_bridge.append(payload)
                relation_review_rows.append(payload)
            if not (bridge_types & EVENT_FACT_OR_OFFICIAL_BRIDGE_TYPES):
                payload = {
                    "candidate_id": int(candidate_id),
                    "issue": "promoted_without_event_fact_or_official_bridge",
                    "candidate_type": candidate_type,
                    "entity_a_id": int(entity_a_id),
                    "entity_b_id": int(entity_b_id),
                    "bridge_types": sorted(bridge_types),
                }
                promoted_without_event_fact_or_official_bridge.append(payload)
                relation_review_rows.append(payload)
            if float(dedupe_support_score or 0.0) < 0.5:
                payload = {
                    "candidate_id": int(candidate_id),
                    "issue": "duplicate_amplified_promotion",
                    "candidate_type": candidate_type,
                    "entity_a_id": int(entity_a_id),
                    "entity_b_id": int(entity_b_id),
                    "dedupe_support_score": float(dedupe_support_score or 0.0),
                    "evidence_mix": evidence_mix,
                }
                duplicate_amplified_promotions.append(payload)
                relation_review_rows.append(payload)

        blocked_official_rows = conn.execute(
            """
            SELECT
                rc.id,
                rc.entity_a_id,
                rc.entity_b_id,
                rc.candidate_type,
                rc.candidate_state,
                COALESCE(rc.promotion_block_reason, '') AS promotion_block_reason,
                COALESCE(rc.evidence_mix_json, '{}') AS evidence_mix_json,
                COALESCE(rc.explain_path_json, '[]') AS explain_path_json,
                COALESCE(ea.entity_type, '') AS entity_a_type,
                COALESCE(ea.canonical_name, '') AS entity_a_name,
                COALESCE(eb.entity_type, '') AS entity_b_type,
                COALESCE(eb.canonical_name, '') AS entity_b_name,
                COALESCE(rc.support_items, 0),
                COALESCE(rc.support_sources, 0),
                COALESCE(rc.support_domains, 0),
                COALESCE(rc.support_hard_evidence_count, 0)
            FROM relation_candidates rc
            LEFT JOIN entities ea ON ea.id = rc.entity_a_id
            LEFT JOIN entities eb ON eb.id = rc.entity_b_id
            WHERE rc.candidate_state IN ('seed_only', 'review', 'pending')
              AND COALESCE(rc.support_hard_evidence_count, 0) >= 1
              AND COALESCE(rc.promotion_block_reason, '') <> ''
            ORDER BY rc.id
            """
        ).fetchall()
        blocked_official_candidates: list[dict[str, Any]] = []
        for row in blocked_official_rows:
            (
                candidate_id,
                entity_a_id,
                entity_b_id,
                candidate_type,
                candidate_state,
                promotion_block_reason,
                evidence_mix_json,
                explain_path_json,
                entity_a_type,
                entity_a_name,
                entity_b_type,
                entity_b_name,
                support_items,
                support_sources,
                support_domains,
                support_hard_evidence_count,
            ) = row
            evidence_mix = _parse_json(evidence_mix_json, {})
            explain_path = _parse_json(explain_path_json, [])
            issue = ""
            if promotion_block_reason == "official_bridge_missing":
                issue = "blocked_official_bridge"
            elif promotion_block_reason == "location_role_only":
                issue = "location_role_only"
            elif promotion_block_reason == "low_entity_specificity":
                issue = "low_specificity_entity"
            elif promotion_block_reason == "duplicate_amplified_support":
                issue = "duplicate_amplified_support"
            elif promotion_block_reason in {"same_case_requires_nonseed_bridge", "same_case_requires_evidence_bridge", "same_case_not_promotable"}:
                issue = "seed_flood_same_case"
            if not issue:
                continue
            payload = {
                "candidate_id": int(candidate_id),
                "issue": issue,
                "candidate_type": candidate_type,
                "candidate_state": candidate_state,
                "promotion_block_reason": promotion_block_reason,
                "entity_a_id": int(entity_a_id),
                "entity_b_id": int(entity_b_id),
                "entity_a_type": entity_a_type,
                "entity_a_name": entity_a_name,
                "entity_b_type": entity_b_type,
                "entity_b_name": entity_b_name,
                "support_items": int(support_items or 0),
                "support_sources": int(support_sources or 0),
                "support_domains": int(support_domains or 0),
                "support_hard_evidence_count": int(support_hard_evidence_count or 0),
                "bridge_types": [str(node.get("node_type") or "") for node in explain_path if isinstance(node, dict)],
                "evidence_mix": evidence_mix,
                "source_links": _relation_source_links(conn, int(candidate_id)),
            }
            blocked_official_candidates.append(payload)
            relation_review_rows.append(payload)

        blocked_seed_rows = conn.execute(
            """
            SELECT
                rc.id,
                rc.entity_a_id,
                rc.entity_b_id,
                rc.candidate_type,
                rc.candidate_state,
                COALESCE(rc.promotion_block_reason, '') AS promotion_block_reason,
                COALESCE(rc.evidence_mix_json, '{}') AS evidence_mix_json,
                COALESCE(rc.explain_path_json, '[]') AS explain_path_json,
                COALESCE(ea.entity_type, '') AS entity_a_type,
                COALESCE(ea.canonical_name, '') AS entity_a_name,
                COALESCE(eb.entity_type, '') AS entity_b_type,
                COALESCE(eb.canonical_name, '') AS entity_b_name,
                COALESCE(rc.support_items, 0),
                COALESCE(rc.support_sources, 0),
                COALESCE(rc.support_domains, 0),
                COALESCE(rc.support_hard_evidence_count, 0),
                COALESCE(rf.calibrated_score, rc.calibrated_score, 0)
            FROM relation_candidates rc
            LEFT JOIN entities ea ON ea.id = rc.entity_a_id
            LEFT JOIN entities eb ON eb.id = rc.entity_b_id
            LEFT JOIN relation_features rf ON rf.candidate_id = rc.id
            WHERE rc.candidate_state IN ('seed_only', 'review', 'pending')
              AND COALESCE(rc.promotion_block_reason, '') IN (
                  'same_case_requires_nonseed_bridge',
                  'same_case_requires_evidence_bridge',
                  'same_case_not_promotable',
                  'location_role_only',
                  'low_entity_specificity',
                  'duplicate_amplified_support'
              )
            ORDER BY COALESCE(rf.calibrated_score, rc.calibrated_score, 0) DESC, rc.id
            LIMIT 200
            """
        ).fetchall()
        blocked_seed_candidates: list[dict[str, Any]] = []
        for row in blocked_seed_rows:
            (
                candidate_id,
                entity_a_id,
                entity_b_id,
                candidate_type,
                candidate_state,
                promotion_block_reason,
                evidence_mix_json,
                explain_path_json,
                entity_a_type,
                entity_a_name,
                entity_b_type,
                entity_b_name,
                support_items,
                support_sources,
                support_domains,
                support_hard_evidence_count,
                calibrated_score,
            ) = row
            evidence_mix = _parse_json(evidence_mix_json, {})
            explain_path = _parse_json(explain_path_json, [])
            issue = ""
            if promotion_block_reason in {"same_case_requires_nonseed_bridge", "same_case_requires_evidence_bridge", "same_case_not_promotable"}:
                issue = "seed_flood_same_case"
            elif promotion_block_reason == "location_role_only":
                issue = "location_role_only"
            elif promotion_block_reason == "low_entity_specificity":
                issue = "low_specificity_entity"
            elif promotion_block_reason == "duplicate_amplified_support":
                issue = "duplicate_amplified_support"
            if not issue:
                continue
            payload = {
                "candidate_id": int(candidate_id),
                "issue": issue,
                "candidate_type": candidate_type,
                "candidate_state": candidate_state,
                "promotion_block_reason": promotion_block_reason,
                "entity_a_id": int(entity_a_id),
                "entity_b_id": int(entity_b_id),
                "entity_a_type": entity_a_type,
                "entity_a_name": entity_a_name,
                "entity_b_type": entity_b_type,
                "entity_b_name": entity_b_name,
                "support_items": int(support_items or 0),
                "support_sources": int(support_sources or 0),
                "support_domains": int(support_domains or 0),
                "support_hard_evidence_count": int(support_hard_evidence_count or 0),
                "calibrated_score": float(calibrated_score or 0.0),
                "bridge_types": [str(node.get("node_type") or "") for node in explain_path if isinstance(node, dict)],
                "evidence_mix": evidence_mix,
                "source_links": _relation_source_links(conn, int(candidate_id)),
            }
            blocked_seed_candidates.append(payload)
            relation_review_rows.append(payload)

        suppressed_claims = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM claims cl
                JOIN content_items ci ON ci.id = cl.content_item_id
                WHERE COALESCE(ci.status, '')='suppressed_template'
                """
            ).fetchone()[0]
        )
        suppressed_case_claims = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM case_claims cc
                JOIN claims cl ON cl.id = cc.claim_id
                JOIN content_items ci ON ci.id = cl.content_item_id
                WHERE COALESCE(ci.status, '')='suppressed_template'
                """
            ).fetchone()[0]
        ) if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='case_claims'").fetchone() else 0
        duplicate_leakage = suppressed_claims + suppressed_case_claims

        degraded_rows = conn.execute(
            """
            SELECT source_key, state, COALESCE(quality_state, 'unknown') AS quality_state, COALESCE(failure_class, '') AS failure_class, COALESCE(last_error, '') AS last_error
            FROM source_sync_state
            WHERE COALESCE(quality_state, state)='degraded'
            ORDER BY source_key
            """
        ).fetchall()
        source_state_rows = conn.execute(
            """
            SELECT source_key, state, COALESCE(quality_state, 'unknown') AS quality_state,
                   COALESCE(failure_class, '') AS failure_class, COALESCE(last_error, '') AS last_error,
                   COALESCE(metadata_json, '{}') AS metadata_json
            FROM source_sync_state
            ORDER BY source_key
            """
        ).fetchall()
        source_state_map: dict[str, tuple[Any, ...]] = {str(row[0]): row for row in source_state_rows}
        tracked_source_keys = set(source_state_map)

        source_acceptance_rows: list[dict[str, Any]] = []
        fixture_backed_sources: list[str] = []
        archive_backed_sources: list[str] = []
        unresolved_blockers: list[dict[str, Any]] = []
        source_review_rows: list[dict[str, Any]] = []
        acceptance_by_source: dict[str, dict[str, Any]] = {}

        def resolve_source_acceptance(source_key: str) -> dict[str, Any]:
            existing = acceptance_by_source.get(source_key)
            if existing:
                return existing
            sync_row = source_state_map.get(source_key)
            entry = manifest_entry(source_key, settings=settings, manifest=manifest)
            entry_mode = acceptance_mode(entry)
            try:
                metadata = json.loads(sync_row[5]) if sync_row and sync_row[5] else {}
            except json.JSONDecodeError:
                metadata = {}
            fixture_smoke = smoke_fixture(source_key, settings=settings, manifest=manifest)
            failure_class = ""
            if sync_row:
                failure_class = (sync_row[3] or "").strip()
                if not failure_class and (sync_row[4] or "").strip():
                    failure_class = _classify_failure_class(error_text=sync_row[4]) or ""
            if not failure_class and entry_mode in {"archive_ok", "fixture_ok"}:
                failure_class = str((fixture_smoke or {}).get("failure_class") or "")
            row = {
                "source_key": source_key,
                "required_for_gate": bool(required_for_gate(entry)),
                "acceptance_mode": entry_mode,
                "state": sync_row[1] if sync_row else "unknown",
                "quality_state": sync_row[2] if sync_row else "unknown",
                "failure_class": failure_class,
                "last_error": sync_row[4] if sync_row else "",
                "quality_issue": str(metadata.get("quality_issue") or ""),
                "primary_urls": primary_urls(entry),
                "fallback_urls": fallback_urls(entry),
                "fixture_ok": bool(fixture_smoke.get("ok")),
                "fixture_path": fixture_smoke.get("fixture_path") if fixture_smoke else None,
                "archive_derived": bool(metadata.get("archive_derived") or (fixture_smoke or {}).get("archive_derived")),
                "fallback_used": metadata.get("fallback_used"),
            }
            row["effective_state"] = effective_source_state(
                state=row["state"],
                quality_state=row["quality_state"],
                failure_class=row["failure_class"],
                metadata=metadata,
                manifest_entry_value=entry,
                fixture_smoke=fixture_smoke,
            )
            source_acceptance_rows.append(row)
            acceptance_by_source[source_key] = row
            if row["effective_state"] == "healthy_fixture":
                fixture_backed_sources.append(source_key)
            if row["effective_state"] == "healthy_archive":
                archive_backed_sources.append(source_key)
            if row["required_for_gate"]:
                if row["effective_state"] not in HEALTHY_STATES:
                    unresolved_blockers.append(row)
                    source_review_rows.append(row)
            elif row["effective_state"] in {"degraded_live", "degraded_parser", "blocked"}:
                source_review_rows.append(row)
            return row

        for source_key in sorted(tracked_source_keys):
            resolve_source_acceptance(source_key)

        ok_with_warning_rows = conn.execute(
            """
            SELECT jr.job_id, jr.started_at, jr.warnings_json
            FROM job_runs jr
            JOIN (
                SELECT job_id, MAX(id) AS max_id
                FROM job_runs
                GROUP BY job_id
            ) latest ON latest.max_id = jr.id
            WHERE status='ok'
              AND warnings_json IS NOT NULL
              AND warnings_json NOT IN ('', '[]', 'null')
            ORDER BY jr.id DESC
            LIMIT 100
            """
        ).fetchall()
        critical_warning_jobs = _critical_warning_rows(ok_with_warning_rows)
        unresolved_warning_jobs: list[dict[str, Any]] = []
        for row in critical_warning_jobs:
            source_key = match_warning_source(row["warning"], settings=settings, manifest=manifest)
            row["source_key"] = source_key
            source_required = False
            if source_key:
                acceptance_row = resolve_source_acceptance(source_key)
                source_required = bool(acceptance_row.get("required_for_gate"))
            row["resolved_by_source_policy"] = bool(
                source_key
                and (
                    acceptance_by_source.get(source_key, {}).get("effective_state") in HEALTHY_STATES
                    or not source_required
                )
            )
            if not row["resolved_by_source_policy"]:
                unresolved_warning_jobs.append(row)

        degrade_reasons: list[str] = []
        warnings: list[str] = []
        if classifier_status == "degraded":
            _append_once(degrade_reasons, "classifier_precision_gate_failed")
        if not reviewed_baseline_ready:
            warnings.append("reviewed_baseline_pending")
        if len(zero_support_rows) > int(cfg["max_zero_support_review_candidates"]):
            _append_once(degrade_reasons, "relation_quality_gate_failed")
        if len(generic_promoted_rows) > int(cfg["max_generic_promoted_candidates"]):
            _append_once(degrade_reasons, "relation_quality_gate_failed")
        if len(promoted_same_case_rows) > int(cfg["max_promoted_same_case_cluster"]):
            _append_once(degrade_reasons, "relation_quality_gate_failed")
        if len(promoted_location_rows) > int(cfg["max_promoted_location_candidates"]):
            _append_once(degrade_reasons, "relation_quality_gate_failed")
        if len(fake_domain_rows) > int(cfg["max_fake_domain_diversity_candidates"]):
            _append_once(degrade_reasons, "relation_quality_gate_failed")
        if len(promoted_without_nonseed_bridge) > int(cfg["max_promoted_without_nonseed_bridge"]):
            _append_once(degrade_reasons, "relation_quality_gate_failed")
        if len(promoted_without_event_fact_or_official_bridge) > int(cfg["max_promoted_without_event_fact_or_official_bridge"]):
            _append_once(degrade_reasons, "relation_quality_gate_failed")
        if len(duplicate_amplified_promotions) > int(cfg["max_duplicate_amplified_promotions"]):
            _append_once(degrade_reasons, "relation_quality_gate_failed")
        if duplicate_leakage > int(cfg["max_duplicate_leakage"]):
            _append_once(degrade_reasons, "dedupe_leak_gate_failed")
        if len(unresolved_blockers) > int(cfg["max_degraded_sources"]):
            _append_once(degrade_reasons, "source_health_gate_failed")
        if len(unresolved_warning_jobs) > int(cfg["max_critical_warning_jobs"]):
            _append_once(degrade_reasons, "source_health_gate_failed")

        if source_review_rows:
            _sync_source_review_tasks(conn, source_review_rows)
        if relation_review_rows:
            _sync_relation_review_tasks(conn, relation_review_rows)

        ai_failure_rows = conn.execute(
            """
            SELECT COALESCE(NULLIF(TRIM(failure_kind), ''), 'unknown') AS failure_kind, COUNT(*) AS total
            FROM ai_task_attempts
            WHERE status <> 'ok'
            GROUP BY COALESCE(NULLIF(TRIM(failure_kind), ''), 'unknown')
            ORDER BY total DESC, failure_kind
            """
        ).fetchall() if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='ai_task_attempts'").fetchone() else []
        event_link_rows = conn.execute(
            """
            SELECT candidate_state, COUNT(*) AS total
            FROM event_candidates
            GROUP BY candidate_state
            """
        ).fetchall() if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_candidates'").fetchone() else []
        event_link_counts = {str(row[0] or "unknown"): int(row[1] or 0) for row in event_link_rows}

        report = {
            "ok": not degrade_reasons,
            "generated_at": now_iso(),
            "classifier_status": classifier_status,
            "reviewed_baseline_ready": reviewed_baseline_ready,
            "top_false_positive_tags": [
                {"content_type": row[0], "tag_name": row[1], "count": int(row[2] or 0)}
                for row in generic_false_positive_rows
            ],
            "relation_quality": {
                "zero_support_review_candidates": len(zero_support_rows),
                "promoted_with_generic_entity": len(generic_promoted_rows),
                "promoted_same_case_cluster": len(promoted_same_case_rows),
                "promoted_with_location_entity": len(promoted_location_rows),
                "promoted_with_fake_domain_diversity": len(fake_domain_rows),
                "promoted_without_nonseed_bridge": len(promoted_without_nonseed_bridge),
                "promoted_without_event_fact_or_official_bridge": len(promoted_without_event_fact_or_official_bridge),
                "duplicate_amplified_promotions": len(duplicate_amplified_promotions),
                "rows": [
                    {
                        "candidate_id": int(row[0]),
                        "entity_a_id": int(row[1]),
                        "entity_b_id": int(row[2]),
                        "candidate_type": row[3],
                        "candidate_state": row[4],
                        "support_items": int(row[5] or 0),
                        "support_sources": int(row[6] or 0),
                        "support_domains": int(row[7] or 0),
                    }
                    for row in zero_support_rows
                ],
                "generic_promotions": generic_promoted_rows,
                "same_case_promotions": promoted_same_case_rows,
                "location_promotions": promoted_location_rows,
                "fake_domain_diversity_rows": [
                    {
                        "candidate_id": int(row[0]),
                        "entity_a_id": int(row[1]),
                        "entity_b_id": int(row[2]),
                        "candidate_type": row[3],
                        "fake_domain_rows": int(row[4] or 0),
                        "real_domain_count": int(row[5] or 0),
                    }
                    for row in fake_domain_rows
                ],
                "promoted_without_nonseed_bridge_rows": promoted_without_nonseed_bridge,
                "promoted_without_event_fact_or_official_bridge_rows": promoted_without_event_fact_or_official_bridge,
                "duplicate_amplified_promotion_rows": duplicate_amplified_promotions,
                "blocked_official_candidates": blocked_official_candidates,
                "blocked_seed_candidates": blocked_seed_candidates,
            },
            "ai_sweep": {
                "failure_kind_breakdown": {str(row[0]): int(row[1] or 0) for row in ai_failure_rows},
            },
            "event_linking": {
                "link_existing_count": event_link_counts.get("link_existing", 0),
                "merge_review_count": event_link_counts.get("merge_review", 0),
                "standalone_count": event_link_counts.get("standalone", 0),
                "create_candidate_count": event_link_counts.get("create_candidate", 0) + event_link_counts.get("create_event_candidate", 0),
                "rejected_count": event_link_counts.get("rejected", 0),
            },
            "dedupe_leakage": {
                "suppressed_claims": suppressed_claims,
                "suppressed_case_claims": suppressed_case_claims,
                "total": duplicate_leakage,
            },
            "source_health": {
                "degraded_count": len(degraded_rows),
                "rows": [
                    {
                        "source_key": row[0],
                        "state": row[1],
                        "quality_state": row[2],
                        "failure_class": row[3],
                        "last_error": row[4],
                    }
                    for row in degraded_rows
                ],
            },
            "source_acceptance": {
                "rows": source_acceptance_rows,
            },
            "fixture_backed_sources": fixture_backed_sources,
            "archive_backed_sources": archive_backed_sources,
            "unresolved_blockers": unresolved_blockers,
            "ok_with_warning_jobs": [
                {
                    "job_id": row[0],
                    "started_at": row[1],
                    "warnings_json": row[2],
                }
                for row in ok_with_warning_rows
            ],
            "critical_warning_jobs": critical_warning_jobs,
            "unresolved_warning_jobs": unresolved_warning_jobs,
            "degrade_reasons": degrade_reasons,
            "report_path": str(report_path),
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        fatal_errors = degrade_reasons[:] if degrade_reasons and bool(cfg.get("strict_gate", True)) else []
        return {
            "ok": not fatal_errors,
            "items_seen": (
                len(generic_false_positive_rows)
                + len(zero_support_rows)
                + len(generic_promoted_rows)
                + len(fake_domain_rows)
                + len(promoted_without_nonseed_bridge)
                + len(duplicate_amplified_promotions)
                + len(degraded_rows)
                + len(ok_with_warning_rows)
            ),
            "items_new": 0,
            "items_updated": 0,
            "warnings": warnings + degrade_reasons,
            "fatal_errors": fatal_errors,
            "artifacts": report,
        }
    finally:
        conn.close()


def main() -> None:
    print(json.dumps(build_quality_gate(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
