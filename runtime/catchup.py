from __future__ import annotations

import uuid
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from config.db_utils import get_db, load_settings
from runtime.contracts import now_iso
from runtime.runner import run_job_once
from runtime.state import set_runtime_metadata


DEFAULT_CATCHUP_JOB_IDS = [
    "watch_folder",
    "telegram",
    "telegram_telethon_pool",
    "telegram_public_fallback",
    "rss",
    "official",
    "playwright_official",
    "gov",
    "minjust",
    "duma_bills",
    "duma_votes_2y",
    "deputies",
    "ocr",
    "content_dedupe",
    "tagger",
    "ner",
    "entity_resolve",
    "event_pipeline",
    "evidence_link",
    "relations",
    "relation_rebuild_enriched",
    "quality_gate",
]


DEFAULT_REQUIRED_JOB_IDS = {
    "telegram_public_fallback",
    "content_dedupe",
    "event_pipeline",
    "evidence_link",
    "relations",
    "relation_rebuild_enriched",
    "quality_gate",
}


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt).date()
        except ValueError:
            continue
    return None


def _catchup_window(settings: dict[str, Any]) -> tuple[str, str]:
    today = date.today()
    default_days = int(settings.get("catchup_default_days", 14) or 14)
    db = get_db(settings)
    try:
        row = db.execute(
            "SELECT last_success_at FROM source_sync_state WHERE source_key='collect_catchup' LIMIT 1"
        ).fetchone()
    finally:
        db.close()
    last_success = _parse_date(row[0]) if row and row[0] else None
    start = last_success or (today - timedelta(days=max(1, default_days)))
    if start > today:
        start = today
    return start.isoformat(), today.isoformat()


def _configured_jobs(settings: dict[str, Any]) -> list[str]:
    raw = settings.get("catchup_job_ids")
    if isinstance(raw, list) and raw:
        return [str(item).strip() for item in raw if str(item).strip() and str(item).strip() != "collect_catchup"]
    return list(DEFAULT_CATCHUP_JOB_IDS)


def _required_jobs(settings: dict[str, Any], job_ids: list[str]) -> set[str]:
    raw = settings.get("catchup_required_job_ids")
    if isinstance(raw, list):
        return {str(item).strip() for item in raw if str(item).strip()}
    return DEFAULT_REQUIRED_JOB_IDS.intersection(job_ids)


def _report_path(settings: dict[str, Any]) -> Path:
    reports_dir = Path(settings.get("reports_dir", "reports"))
    return reports_dir / "collect_catchup_latest.json"


def _extract_quality_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    for result in reversed(results):
        if result.get("job_id") != "quality_gate":
            continue
        artifacts = result.get("artifacts") or {}
        report = _unwrap_quality_report(artifacts)
        if isinstance(report, dict):
            return {
                "ok": bool(result.get("ok")),
                "relation_quality": _compact_relation_quality(report.get("relation_quality") or {}),
                "source_health": report.get("source_health") or report.get("source_acceptance") or {},
                "warnings": result.get("warnings") or [],
            }
    return {}


def _unwrap_quality_report(artifacts: Any) -> dict[str, Any] | None:
    if not isinstance(artifacts, dict):
        return None
    report = artifacts.get("report")
    if isinstance(report, dict):
        return report
    if "relation_quality" in artifacts or "source_health" in artifacts or "source_acceptance" in artifacts:
        return artifacts
    nested = artifacts.get("artifacts")
    if isinstance(nested, dict) and (
        "relation_quality" in nested or "source_health" in nested or "source_acceptance" in nested
    ):
        return nested
    return None


def _compact_relation_quality(relation_quality: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "zero_support_review_candidates",
        "promoted_with_generic_entity",
        "promoted_same_case_cluster",
        "promoted_with_location_entity",
        "promoted_with_fake_domain_diversity",
        "promoted_without_nonseed_bridge",
        "promoted_without_event_fact_or_official_bridge",
        "duplicate_amplified_promotions",
        "relations_open_review_tasks",
        "total_relation_review_candidates",
        "same_case_review_candidates",
        "same_case_review_ratio",
        "location_role_only_review_candidates",
        "location_role_only_review_ratio",
    ]
    return {key: relation_quality.get(key) for key in keys if key in relation_quality}


def _compact_artifacts(job_id: str, artifacts: Any) -> dict[str, Any]:
    if not isinstance(artifacts, dict):
        return {}
    if job_id == "quality_gate":
        report = _unwrap_quality_report(artifacts) or {}
        return {
            "ok": report.get("ok", artifacts.get("ok")),
            "generated_at": report.get("generated_at"),
            "reviewed_baseline_ready": report.get("reviewed_baseline_ready"),
            "relation_quality": _compact_relation_quality(report.get("relation_quality") or {}),
            "source_acceptance": report.get("source_acceptance") or {},
            "document_verdict_counts": report.get("document_verdict_counts") or {},
            "event_linking": report.get("event_linking") or {},
            "ai_sweep": report.get("ai_sweep") or {},
        }
    allowed = {}
    for key in ("window_start", "window_end", "fallback", "fallback_used", "transport", "urls"):
        if key in artifacts:
            allowed[key] = artifacts[key]
    nested = artifacts.get("artifacts")
    if isinstance(nested, dict):
        for key in ("window_start", "window_end", "fallback", "fallback_used", "transport", "urls"):
            if key in nested:
                allowed[key] = nested[key]
    return allowed


def _write_report(settings: dict[str, Any], report: dict[str, Any]) -> None:
    path = _report_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def run_collect_catchup(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run a bounded catch-up collection pass from last success through today."""
    settings = settings or load_settings()
    window_start, window_end = _catchup_window(settings)
    owner_prefix = f"catchup:{uuid.uuid4().hex[:10]}"
    started_at = now_iso()
    results: list[dict[str, Any]] = []
    warnings: list[str] = []
    fatal_errors: list[str] = []
    critical_failures: list[dict[str, Any]] = []
    items_seen = 0
    items_new = 0
    items_updated = 0
    job_ids = _configured_jobs(settings)
    required_job_ids = _required_jobs(settings, job_ids)

    for job_id in job_ids:
        result = run_job_once(
            job_id,
            settings=settings,
            trigger_mode="catchup",
            requested_by="ui",
            owner=f"{owner_prefix}:{job_id}",
        )
        compact = {
            "job_id": job_id,
            "ok": bool(result.get("ok")),
            "items_seen": int(result.get("items_seen") or 0),
            "items_new": int(result.get("items_new") or 0),
            "items_updated": int(result.get("items_updated") or 0),
            "warnings": list(result.get("warnings") or [])[:10],
            "fatal_errors": list(result.get("fatal_errors") or [])[:10],
            "retriable_errors": list(result.get("retriable_errors") or [])[:10],
            "artifacts": _compact_artifacts(job_id, result.get("artifacts") or {}),
        }
        results.append(compact)
        items_seen += compact["items_seen"]
        items_new += compact["items_new"]
        items_updated += compact["items_updated"]
        if not compact["ok"]:
            reason = compact["fatal_errors"] or compact["retriable_errors"] or compact["warnings"] or ["job_failed"]
            warnings.append(f"{job_id}:{'; '.join(str(item) for item in reason[:2])}")
            if job_id in required_job_ids:
                failure = {
                    "job_id": job_id,
                    "reason": [str(item) for item in reason[:3]],
                }
                critical_failures.append(failure)
                fatal_errors.append(f"{job_id}:{'; '.join(failure['reason'])}")

    db = get_db(settings)
    try:
        set_runtime_metadata(db, "last_collect_catchup_started_at", started_at)
        set_runtime_metadata(db, "last_collect_catchup_finished_at", now_iso())
        set_runtime_metadata(db, "last_collect_catchup_window", f"{window_start}:{window_end}")
    finally:
        db.close()

    finished_at = now_iso()
    ok = not critical_failures
    quality_summary = _extract_quality_summary(results)
    source_quality_state = "degraded" if critical_failures else "warning" if warnings else "ok"
    source_quality_issue = "; ".join(fatal_errors[:5] if critical_failures else warnings[:5]) or None
    report = {
        "ok": ok,
        "started_at": started_at,
        "finished_at": finished_at,
        "window_start": window_start,
        "window_end": window_end,
        "items_seen": items_seen,
        "items_new": items_new,
        "items_updated": items_updated,
        "warnings": warnings[:50],
        "fatal_errors": fatal_errors[:50],
        "critical_failures": critical_failures,
        "quality_summary": quality_summary,
        "jobs": results,
    }
    _write_report(settings, report)

    return {
        "ok": ok,
        "items_seen": items_seen,
        "items_new": items_new,
        "items_updated": items_updated,
        "warnings": warnings[:50],
        "fatal_errors": fatal_errors[:50],
        "artifacts": {
            "window_start": window_start,
            "window_end": window_end,
            "jobs": results,
            "report_path": str(_report_path(settings)),
            "quality_summary": quality_summary,
            "source_quality_state": source_quality_state,
            "source_quality_issue": source_quality_issue,
        },
    }
