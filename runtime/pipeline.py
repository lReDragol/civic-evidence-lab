from __future__ import annotations

import logging
from typing import Any

from config.db_utils import get_db, load_settings
from runtime.contracts import JobResult, now_iso
from runtime.registry import PIPELINE_JOB_IDS, get_job_spec
from runtime.runner import run_job_once
from runtime.state import (
    finish_pipeline_run,
    set_runtime_metadata,
    start_pipeline_run,
)


log = logging.getLogger(__name__)


def generate_pipeline_version(mode: str) -> str:
    stamp = now_iso().replace("-", "").replace(":", "").replace("T", "")
    return f"{mode}-{stamp}"


def run_pipeline(
    mode: str,
    *,
    settings: dict[str, Any] | None = None,
    requested_by: str = "cli",
    owner: str | None = None,
) -> dict[str, Any]:
    settings = settings or load_settings()
    if mode not in PIPELINE_JOB_IDS:
        return JobResult.failure(
            job_id=f"pipeline:{mode}",
            started_at=now_iso(),
            fatal_errors=[f"unknown_pipeline_mode:{mode}"],
        ).to_dict()

    conn = get_db(settings)
    started_at = now_iso()
    pipeline_version = generate_pipeline_version(mode)
    stages = list(PIPELINE_JOB_IDS[mode])
    pipeline_run_id = start_pipeline_run(
        conn,
        pipeline_version=pipeline_version,
        mode=mode,
        requested_by=requested_by,
        stages=stages,
    )
    set_runtime_metadata(conn, "current_pipeline_version", pipeline_version)
    set_runtime_metadata(conn, "current_pipeline_mode", mode)
    set_runtime_metadata(conn, "current_pipeline_status", "running")

    items_seen = 0
    items_new = 0
    items_updated = 0
    warnings: list[str] = []
    retriable_errors: list[str] = []
    fatal_errors: list[str] = []
    stage_results: list[dict[str, Any]] = []
    succeeded_ids: set[str] = set()
    skipped_ids: set[str] = set()
    failed_ids: set[str] = set()

    try:
        for job_id in stages:
            spec = get_job_spec(job_id)
            if spec and spec.depends_on:
                unmet = [d for d in spec.depends_on if d not in succeeded_ids]
                if unmet:
                    skipped_ids.add(job_id)
                    skip_msg = f"skipped:dependency_not_met:{','.join(unmet)}"
                    warnings.append(f"{job_id}:{skip_msg}")
                    stage_results.append({"job_id": job_id, "result": {"ok": False, "skipped": True, "warnings": [skip_msg]}})
                    log.info("Pipeline job %s skipped: deps not met (%s)", job_id, ", ".join(unmet))
                    continue

            result = run_job_once(
                job_id,
                settings=settings,
                trigger_mode="pipeline",
                requested_by=requested_by,
                pipeline_version=pipeline_version,
                pipeline_run_id=pipeline_run_id,
                owner=owner,
            )
            stage_results.append({"job_id": job_id, "result": result})
            items_seen += int(result.get("items_seen") or 0)
            items_new += int(result.get("items_new") or 0)
            items_updated += int(result.get("items_updated") or 0)
            warnings.extend(str(item) for item in (result.get("warnings") or []))
            retriable_errors.extend(str(item) for item in (result.get("retriable_errors") or []))
            fatal_errors.extend(str(item) for item in (result.get("fatal_errors") or []))

            if result.get("ok"):
                succeeded_ids.add(job_id)
            elif result.get("skipped"):
                skipped_ids.add(job_id)
            else:
                failed_ids.add(job_id)

        ok = not fatal_errors and not retriable_errors and not failed_ids
        skipped_count = len(skipped_ids)
        if skipped_count:
            warnings.insert(0, f"pipeline_skipped_{skipped_count}_jobs")
        result = JobResult(
            ok=ok,
            job_id=f"pipeline:{mode}",
            started_at=started_at,
            finished_at=now_iso(),
            items_seen=items_seen,
            items_new=items_new,
            items_updated=items_updated,
            warnings=warnings,
            retriable_errors=retriable_errors,
            fatal_errors=fatal_errors,
            artifacts={
                "pipeline_version": pipeline_version,
                "pipeline_run_id": pipeline_run_id,
                "stages": stage_results,
                "succeeded": sorted(succeeded_ids),
                "skipped": sorted(skipped_ids),
                "failed": sorted(failed_ids),
            },
        ).to_dict()

        finish_pipeline_run(conn, pipeline_run_id, ok=ok, result=result)
        set_runtime_metadata(conn, "current_pipeline_status", "ok" if ok else "failed")
        if ok:
            set_runtime_metadata(conn, "last_successful_pipeline_version", pipeline_version)
            set_runtime_metadata(conn, f"last_successful_pipeline_version:{mode}", pipeline_version)
        else:
            set_runtime_metadata(conn, "last_failed_pipeline_version", pipeline_version)
        return result
    finally:
        conn.close()
