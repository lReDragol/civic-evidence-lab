from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from classifier.tagger_v3 import GENERIC_TAGS, STRICT_CONTENT_PRIORS
from config.db_utils import PROJECT_ROOT, get_db, load_settings
from runtime.state import _classify_failure_class
from runtime.state import get_runtime_metadata, now_iso


DEFAULT_CONFIG = {
    "strict_gate": True,
    "max_degraded_sources": 0,
    "max_critical_warning_jobs": 0,
    "max_zero_support_review_candidates": 0,
    "max_duplicate_leakage": 0,
    "report_path": str(PROJECT_ROOT / "reports" / "qa_quality_latest.json"),
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


def build_quality_gate(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    cfg = _config(settings)
    report_path = _report_path(settings)
    report_path.parent.mkdir(parents=True, exist_ok=True)

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

        degrade_reasons: list[str] = []
        warnings: list[str] = []
        if classifier_status == "degraded":
            _append_once(degrade_reasons, "classifier_precision_gate_failed")
        if not reviewed_baseline_ready:
            warnings.append("reviewed_baseline_pending")
        if len(zero_support_rows) > int(cfg["max_zero_support_review_candidates"]):
            _append_once(degrade_reasons, "relation_quality_gate_failed")
        if duplicate_leakage > int(cfg["max_duplicate_leakage"]):
            _append_once(degrade_reasons, "dedupe_leak_gate_failed")
        if len(degraded_rows) > int(cfg["max_degraded_sources"]):
            _append_once(degrade_reasons, "source_health_gate_failed")
        if len(critical_warning_jobs) > int(cfg["max_critical_warning_jobs"]):
            _append_once(degrade_reasons, "source_health_gate_failed")

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
            "ok_with_warning_jobs": [
                {
                    "job_id": row[0],
                    "started_at": row[1],
                    "warnings_json": row[2],
                }
                for row in ok_with_warning_rows
            ],
            "critical_warning_jobs": critical_warning_jobs,
            "degrade_reasons": degrade_reasons,
            "report_path": str(report_path),
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        fatal_errors = degrade_reasons[:] if degrade_reasons and bool(cfg.get("strict_gate", True)) else []
        return {
            "ok": not fatal_errors,
            "items_seen": len(generic_false_positive_rows) + len(zero_support_rows) + len(degraded_rows) + len(ok_with_warning_rows),
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
