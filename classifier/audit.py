from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Iterable

from config.db_utils import PROJECT_ROOT, get_db, load_settings
from runtime.state import get_runtime_metadata, now_iso, set_runtime_metadata


DEFAULT_CONFIG = {
    "gold_claims_target": 300,
    "gold_relations_target": 300,
    "gold_tags_target": 300,
    "gold_duplicates_target": 300,
    "min_reviewed_per_kind": 30,
    "claims_precision_threshold": 0.85,
    "relations_precision_threshold": 0.80,
    "tags_precision_threshold": 0.85,
    "duplicates_precision_threshold": 0.85,
    "claim_status_drift_threshold": 0.20,
    "relation_drift_threshold": 0.30,
    "tag_drift_threshold": 0.35,
    "duplicate_drift_threshold": 0.30,
    "strict_gate": True,
    "report_path": str(PROJECT_ROOT / "reports" / "classifier_audit_latest.json"),
}
AUDIT_BASELINE_VERSION = "2026-04-26-classifier-v3-reviewed-baseline"


def _audit_config(settings: dict[str, Any]) -> dict[str, Any]:
    cfg = dict(DEFAULT_CONFIG)
    cfg.update(settings.get("classifier_audit", {}) or {})
    return cfg


def _report_path(settings: dict[str, Any]) -> Path:
    configured = _audit_config(settings).get("report_path") or DEFAULT_CONFIG["report_path"]
    path = Path(configured)
    return path if path.is_absolute() else (PROJECT_ROOT / path)


def _row_dicts(rows: Iterable[Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def _round_robin_sample(
    rows: list[dict[str, Any]],
    *,
    limit: int,
    group_key: Callable[[dict[str, Any]], str],
) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = str(group_key(row) or "unknown")
        buckets[key].append(row)

    sample: list[dict[str, Any]] = []
    ordered_keys = sorted(buckets)
    while buckets and len(sample) < limit:
        next_keys: list[str] = []
        for key in ordered_keys:
            bucket = buckets.get(key) or []
            if not bucket:
                continue
            sample.append(bucket.pop(0))
            if bucket:
                next_keys.append(key)
            else:
                buckets.pop(key, None)
            if len(sample) >= limit:
                break
        ordered_keys = next_keys
    return sample


def _distribution(rows: Iterable[tuple[Any, int]]) -> dict[str, int]:
    result: dict[str, int] = {}
    for key, count in rows:
        result[str(key or "unknown")] = int(count or 0)
    return result


def _normalize_distribution(data: dict[str, int]) -> dict[str, float]:
    total = float(sum(max(0, int(value)) for value in data.values()))
    if total <= 0:
        return {}
    return {key: max(0, int(value)) / total for key, value in data.items()}


def _total_variation_distance(current: dict[str, int], baseline: dict[str, int]) -> float:
    current_norm = _normalize_distribution(current)
    baseline_norm = _normalize_distribution(baseline)
    keys = set(current_norm) | set(baseline_norm)
    if not keys:
        return 0.0
    return round(
        0.5 * sum(abs(current_norm.get(key, 0.0) - baseline_norm.get(key, 0.0)) for key in keys),
        4,
    )


def _review_precision(rows: Iterable[Any]) -> dict[str, dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"reviewed": 0, "score_sum": 0.0, "by_status": defaultdict(int)})
    for row in rows:
        sample_kind = str(row["sample_kind"] or "unknown")
        review_status = str(row["review_status"] or "pending")
        if review_status not in {"correct", "partially", "wrong"}:
            continue
        stats[sample_kind]["reviewed"] += 1
        stats[sample_kind]["by_status"][review_status] += 1
        if review_status == "correct":
            stats[sample_kind]["score_sum"] += 1.0
        elif review_status == "partially":
            stats[sample_kind]["score_sum"] += 0.5

    result: dict[str, dict[str, Any]] = {}
    for sample_kind, payload in stats.items():
        reviewed = int(payload["reviewed"])
        precision = round(payload["score_sum"] / reviewed, 4) if reviewed else None
        result[sample_kind] = {
            "reviewed": reviewed,
            "precision": precision,
            "by_status": dict(payload["by_status"]),
        }
    return result


def _classifier_v3_tag_predicate(conn) -> tuple[str, tuple[Any, ...]]:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(content_tags)").fetchall()}
    if "decision_source" not in columns:
        return "", ()
    has_v3 = conn.execute(
        "SELECT 1 FROM content_tags WHERE COALESCE(decision_source, '')='classifier_v3' LIMIT 1"
    ).fetchone()
    if not has_v3:
        return "", ()
    return "WHERE COALESCE(ct.decision_source, '')='classifier_v3'", ()


def _sample_claim_rows(conn, limit: int) -> list[dict[str, Any]]:
    rows = _row_dicts(
        conn.execute(
            """
            SELECT
                c.id,
                c.content_item_id,
                COALESCE(c.status, 'unverified') AS status,
                COALESCE(c.claim_type, '') AS claim_type,
                COALESCE(c.claim_text, '') AS claim_text,
                COALESCE(c.confidence_final, c.confidence_auto, 0) AS confidence,
                COALESCE(ci.title, '') AS title,
                COALESCE(ci.published_at, ci.collected_at, c.created_at, '') AS seen_at
            FROM claims c
            JOIN content_items ci ON ci.id = c.content_item_id
            ORDER BY COALESCE(ci.published_at, ci.collected_at, c.created_at, '') DESC, c.id DESC
            LIMIT ?
            """,
            (max(limit * 4, limit),),
        ).fetchall()
    )
    return _round_robin_sample(rows, limit=limit, group_key=lambda row: row["status"])


def _sample_relation_rows(conn, limit: int) -> list[dict[str, Any]]:
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='relation_candidates'").fetchone() is None:
        return []
    rows = _row_dicts(
        conn.execute(
            """
            SELECT
                id,
                entity_a_id,
                entity_b_id,
                candidate_type,
                score,
                promotion_state,
                support_items,
                support_sources,
                support_domains,
                COALESCE(last_seen_at, first_seen_at, '') AS seen_at,
                COALESCE(metadata_json, '{}') AS metadata_json
            FROM relation_candidates
            ORDER BY score DESC, COALESCE(last_seen_at, first_seen_at, '') DESC, id DESC
            LIMIT ?
            """,
            (max(limit * 4, limit),),
        ).fetchall()
    )
    return _round_robin_sample(rows, limit=limit, group_key=lambda row: row["candidate_type"])


def _sample_tag_rows(conn, limit: int) -> list[dict[str, Any]]:
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='content_tags'").fetchone() is None:
        return []
    predicate, params = _classifier_v3_tag_predicate(conn)
    rows = _row_dicts(
        conn.execute(
            f"""
            SELECT
                ct.id,
                ct.content_item_id,
                COALESCE(ct.tag_name, '') AS tag_name,
                ct.tag_level,
                COALESCE(ct.confidence, 0) AS confidence,
                COALESCE(ct.tag_source, '') AS tag_source,
                COALESCE(ci.title, '') AS title,
                COALESCE(ci.published_at, ci.collected_at, '') AS seen_at
            FROM content_tags ct
            JOIN content_items ci ON ci.id = ct.content_item_id
            {predicate}
            ORDER BY COALESCE(ci.published_at, ci.collected_at, '') DESC, ct.confidence DESC, ct.id DESC
            LIMIT ?
            """,
            (*params, max(limit * 4, limit)),
        ).fetchall()
    )
    return _round_robin_sample(rows, limit=limit, group_key=lambda row: row["tag_name"])


def _sample_duplicate_rows(conn, limit: int) -> list[dict[str, Any]]:
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='review_tasks'").fetchone() is None:
        return []
    rows = _row_dicts(
        conn.execute(
            """
            SELECT
                rt.id,
                rt.subject_id,
                COALESCE(rt.suggested_action, '') AS suggested_action,
                COALESCE(rt.confidence, 0) AS confidence,
                COALESCE(rt.machine_reason, '') AS machine_reason,
                COALESCE(cc.cluster_type, 'document_dedupe') AS cluster_type,
                COALESCE(cc.status, 'active') AS cluster_status,
                COALESCE(cc.item_count, 0) AS item_count,
                COALESCE(cc.canonical_title, '') AS canonical_title,
                COALESCE(cc.updated_at, cc.created_at, '') AS seen_at
            FROM review_tasks rt
            LEFT JOIN content_clusters cc
              ON cc.id = rt.subject_id
            WHERE rt.queue_key='content_duplicates'
            ORDER BY COALESCE(rt.confidence, 0) DESC, rt.id DESC
            LIMIT ?
            """,
            (max(limit * 4, limit),),
        ).fetchall()
    )
    return _round_robin_sample(rows, limit=limit, group_key=lambda row: row["suggested_action"] or row["cluster_type"])


def _insert_samples(conn, *, batch_name: str, sample_kind: str, rows: list[dict[str, Any]]) -> int:
    inserted = 0
    for row in rows:
        target_id = int(row["id"])
        payload = dict(row)
        actual_label = None
        target_ref = None
        if sample_kind == "claim":
            actual_label = str(row["status"])
            target_ref = f"claim:{target_id}"
        elif sample_kind == "relation_candidate":
            actual_label = str(row["candidate_type"])
            target_ref = f"relation_candidate:{target_id}"
        elif sample_kind == "tag_assignment":
            actual_label = str(row["tag_name"])
            target_ref = f"tag_assignment:{target_id}"
        elif sample_kind == "content_duplicate":
            actual_label = str(row["suggested_action"] or row["cluster_type"] or "content_duplicate")
            target_ref = f"content_duplicate:{target_id}"

        conn.execute(
            """
            INSERT INTO classifier_audit_samples(
                sample_kind, target_id, target_ref, actual_label, batch_name, payload_json
            ) VALUES(?,?,?,?,?,?)
            """,
            (
                sample_kind,
                target_id,
                target_ref,
                actual_label,
                batch_name,
                json.dumps(payload, ensure_ascii=False),
            ),
        )
        inserted += 1
    return inserted


def _current_distributions(conn) -> dict[str, dict[str, int]]:
    tag_predicate, tag_params = _classifier_v3_tag_predicate(conn)
    return {
        "claim_status": _distribution(
            conn.execute(
                "SELECT COALESCE(status, 'unverified') AS status, COUNT(*) FROM claims GROUP BY COALESCE(status, 'unverified')"
            ).fetchall()
        ),
        "relation_types": _distribution(
            conn.execute(
                """
                SELECT COALESCE(candidate_type, 'unknown') AS candidate_type, COUNT(*)
                FROM relation_candidates
                GROUP BY COALESCE(candidate_type, 'unknown')
                """
            ).fetchall()
        )
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='relation_candidates'").fetchone()
        else {},
        "top_tags": _distribution(
            conn.execute(
                f"""
                SELECT COALESCE(tag_name, 'unknown') AS tag_name, COUNT(*) AS total
                FROM content_tags
                {tag_predicate.replace('ct.', '')}
                GROUP BY COALESCE(tag_name, 'unknown')
                ORDER BY total DESC, tag_name
                LIMIT 50
                """,
                tag_params,
            ).fetchall()
        )
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='content_tags'").fetchone()
        else {},
        "duplicate_actions": _distribution(
            conn.execute(
                """
                SELECT COALESCE(suggested_action, 'unknown') AS suggested_action, COUNT(*)
                FROM review_tasks
                WHERE queue_key='content_duplicates'
                GROUP BY COALESCE(suggested_action, 'unknown')
                """
            ).fetchall()
        )
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='review_tasks'").fetchone()
        else {},
    }


def _reviewed_distributions(conn) -> dict[str, dict[str, int]]:
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='classifier_audit_samples'").fetchone() is None:
        return {
            "claim_status": {},
            "relation_types": {},
            "top_tags": {},
            "duplicate_actions": {},
        }

    def grouped(sample_kind: str) -> dict[str, int]:
        return _distribution(
            conn.execute(
                """
                SELECT COALESCE(actual_label, 'unknown') AS actual_label, COUNT(*)
                FROM classifier_audit_samples
                WHERE sample_kind=?
                  AND review_status IN ('correct', 'partially', 'wrong')
                GROUP BY COALESCE(actual_label, 'unknown')
                """,
                (sample_kind,),
            ).fetchall()
        )

    return {
        "claim_status": grouped("claim"),
        "relation_types": grouped("relation_candidate"),
        "top_tags": grouped("tag_assignment"),
        "duplicate_actions": grouped("content_duplicate"),
    }


def build_classifier_audit(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    cfg = _audit_config(settings)
    report_path = _report_path(settings)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    conn = get_db(settings)
    try:
        pipeline_version = get_runtime_metadata(conn, "current_pipeline_version") or f"manual-{now_iso()}"
        batch_name = f"audit:{pipeline_version}"

        conn.execute("DELETE FROM classifier_audit_samples WHERE batch_name=?", (batch_name,))

        claims = _sample_claim_rows(conn, int(cfg["gold_claims_target"]))
        relations = _sample_relation_rows(conn, int(cfg["gold_relations_target"]))
        tags = _sample_tag_rows(conn, int(cfg["gold_tags_target"]))
        duplicates = _sample_duplicate_rows(conn, int(cfg["gold_duplicates_target"]))

        inserted = 0
        inserted += _insert_samples(conn, batch_name=batch_name, sample_kind="claim", rows=claims)
        inserted += _insert_samples(conn, batch_name=batch_name, sample_kind="relation_candidate", rows=relations)
        inserted += _insert_samples(conn, batch_name=batch_name, sample_kind="tag_assignment", rows=tags)
        inserted += _insert_samples(conn, batch_name=batch_name, sample_kind="content_duplicate", rows=duplicates)

        live_distributions = _current_distributions(conn)
        reviewed_distributions = _reviewed_distributions(conn)
        reviewed_ready = any(
            reviewed_distributions.get(key) for key in ("claim_status", "relation_types", "top_tags", "duplicate_actions")
        )
        preferred_kind = "reviewed" if reviewed_ready else "reviewed_pending"
        current_distributions = live_distributions
        reviewed_metrics = _review_precision(
            conn.execute(
                """
                SELECT sample_kind, review_status
                FROM classifier_audit_samples
                WHERE review_status IN ('correct', 'partially', 'wrong')
                """
            ).fetchall()
        )

        baseline_version = get_runtime_metadata(conn, "classifier_audit_baseline_version", None)
        baseline_record = get_runtime_metadata(conn, "classifier_audit_baseline", None) or {}
        baseline_reset = bool(baseline_record) and baseline_version != AUDIT_BASELINE_VERSION
        if baseline_reset:
            baseline_record = {}

        if isinstance(baseline_record, dict) and "distributions" in baseline_record:
            baseline_kind = str(baseline_record.get("kind") or "reviewed_pending")
            baseline = baseline_record.get("distributions") or {}
        else:
            baseline_kind = "reviewed_pending"
            baseline = baseline_record if isinstance(baseline_record, dict) else {}

        if baseline and baseline_kind != "reviewed":
            baseline = {}
            baseline_kind = preferred_kind
            baseline_reset = True

        if not reviewed_ready:
            baseline = {}
            baseline_kind = "reviewed_pending"

        if baseline:
            drift = {
                "claim_status": _total_variation_distance(
                    current_distributions["claim_status"],
                    (baseline.get("claim_status") or {}),
                ),
                "relation_types": _total_variation_distance(
                    current_distributions["relation_types"],
                    (baseline.get("relation_types") or {}),
                ),
                "top_tags": _total_variation_distance(
                    current_distributions["top_tags"],
                    (baseline.get("top_tags") or {}),
                ),
                "duplicate_actions": _total_variation_distance(
                    current_distributions["duplicate_actions"],
                    (baseline.get("duplicate_actions") or {}),
                ),
            }
        else:
            drift = {
                "claim_status": 0.0,
                "relation_types": 0.0,
                "top_tags": 0.0,
                "duplicate_actions": 0.0,
            }

        precision_thresholds = {
            "claim": float(cfg["claims_precision_threshold"]),
            "relation_candidate": float(cfg["relations_precision_threshold"]),
            "tag_assignment": float(cfg["tags_precision_threshold"]),
            "content_duplicate": float(cfg["duplicates_precision_threshold"]),
        }
        min_reviewed = int(cfg["min_reviewed_per_kind"])
        degrade_reasons: list[str] = []
        if baseline:
            if drift["claim_status"] > float(cfg["claim_status_drift_threshold"]):
                degrade_reasons.append(f"claim_status_drift>{cfg['claim_status_drift_threshold']}")
            if drift["relation_types"] > float(cfg["relation_drift_threshold"]):
                degrade_reasons.append(f"relation_types_drift>{cfg['relation_drift_threshold']}")
            if drift["top_tags"] > float(cfg["tag_drift_threshold"]):
                degrade_reasons.append(f"top_tags_drift>{cfg['tag_drift_threshold']}")
            if drift["duplicate_actions"] > float(cfg["duplicate_drift_threshold"]):
                degrade_reasons.append(f"duplicate_actions_drift>{cfg['duplicate_drift_threshold']}")

        for sample_kind, threshold in precision_thresholds.items():
            metrics = reviewed_metrics.get(sample_kind)
            if not metrics:
                continue
            if int(metrics["reviewed"]) < min_reviewed or metrics["precision"] is None:
                continue
            if float(metrics["precision"]) < threshold:
                degrade_reasons.append(f"{sample_kind}_precision<{threshold}")

        degraded = bool(degrade_reasons)
        if reviewed_ready and (not baseline or not degraded):
            set_runtime_metadata(
                conn,
                "classifier_audit_baseline",
                {
                "kind": preferred_kind,
                    "distributions": reviewed_distributions,
                },
            )
            set_runtime_metadata(conn, "classifier_audit_baseline_version", AUDIT_BASELINE_VERSION)
        elif not reviewed_ready:
            set_runtime_metadata(conn, "classifier_audit_baseline", {})
            set_runtime_metadata(conn, "classifier_audit_baseline_version", AUDIT_BASELINE_VERSION)

        report = {
            "ok": not degraded,
            "pipeline_version": pipeline_version,
            "batch_name": batch_name,
            "generated_at": now_iso(),
            "sample_targets": {
                "claim": int(cfg["gold_claims_target"]),
                "relation_candidate": int(cfg["gold_relations_target"]),
                "tag_assignment": int(cfg["gold_tags_target"]),
                "content_duplicate": int(cfg["gold_duplicates_target"]),
            },
            "reviewed_baseline_ready": reviewed_ready,
            "samples_created": {
                "claim": len(claims),
                "relation_candidate": len(relations),
                "tag_assignment": len(tags),
                "content_duplicate": len(duplicates),
            },
            "reviewed_metrics": reviewed_metrics,
            "drift": drift,
            "baseline_exists": bool(baseline),
            "baseline_kind": baseline_kind if baseline else preferred_kind,
            "drift_source": "live_vs_reviewed" if baseline else preferred_kind,
            "degraded": degraded,
            "degrade_reasons": degrade_reasons,
            "baseline_version": AUDIT_BASELINE_VERSION,
            "baseline_reset": baseline_reset,
            "distributions": current_distributions,
            "live_distributions": live_distributions,
            "reviewed_distributions": reviewed_distributions,
            "report_path": str(report_path),
        }

        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        set_runtime_metadata(conn, "classifier_audit_last_batch", batch_name)
        set_runtime_metadata(conn, "classifier_audit_last_status", "degraded" if degraded else "ok")
        set_runtime_metadata(conn, "classifier_audit_last_report", report)
        conn.commit()

        warnings = list(degrade_reasons)
        if not reviewed_ready:
            warnings.append("reviewed_baseline_pending")
        fatal_errors = ["classifier_drift_gate_failed"] if degraded and bool(cfg.get("strict_gate", True)) else []
        return {
            "ok": not fatal_errors,
            "items_seen": len(claims) + len(relations) + len(tags) + len(duplicates),
            "items_new": inserted,
            "items_updated": sum(int(metrics["reviewed"]) for metrics in reviewed_metrics.values()),
            "warnings": warnings,
            "fatal_errors": fatal_errors,
            "artifacts": report,
        }
    finally:
        conn.close()


def main() -> None:
    print(json.dumps(build_classifier_audit(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
