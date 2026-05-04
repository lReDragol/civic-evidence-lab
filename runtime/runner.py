from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
import traceback
from typing import Any

from config.db_utils import get_db, load_settings
from runtime.contracts import JobResult, normalize_job_output, now_iso
from runtime.registry import get_job_spec, run_job_callable
from runtime.state import (
    acquire_job_lease,
    finish_job_run,
    heartbeat_job_lease,
    record_source_health_report,
    release_job_lease,
    set_runtime_metadata,
    start_job_run,
    update_source_sync_state,
)


log = logging.getLogger(__name__)

RETRIABLE_TOKENS = (
    "timeout",
    "temporar",
    "tls",
    "ssl",
    "429",
    "403",
    "connection",
    "reset by peer",
    "locked",
    "busy",
    "try again",
)

CRITICAL_WARNING_TOKENS = (
    "snapshot",
    "404",
    "tls",
    "ssl",
    "cert",
    "timeout",
    "map.svg",
    "placeholder",
    "missing",
)
FINALIZATION_RETRIES = 6
FINALIZATION_RETRY_DELAY_SEC = 0.6


def _owner_name(prefix: str) -> str:
    return f"{prefix}:{os.getpid()}:{threading.get_ident()}"


def _is_retriable_exception(exc: Exception) -> bool:
    text = f"{type(exc).__name__}: {exc}".lower()
    return any(token in text for token in RETRIABLE_TOKENS)


def _heartbeat_loop(stop_event: threading.Event, settings: dict[str, Any], job_id: str, owner: str, ttl_seconds: int):
    interval = max(10, min(15, ttl_seconds // 3))
    while not stop_event.wait(interval):
        conn = get_db(settings)
        try:
            try:
                heartbeat_job_lease(conn, job_id, owner, ttl_seconds=ttl_seconds)
            except sqlite3.OperationalError as error:
                if _is_retriable_exception(error):
                    log.warning("Heartbeat retry skipped for %s: %s", job_id, error)
                    continue
                raise
            except Exception as error:
                if _is_retriable_exception(error):
                    log.warning("Heartbeat transient error for %s: %s", job_id, error)
                    continue
                raise
        finally:
            conn.close()


def _quality_for_job_result(result: dict[str, Any]) -> tuple[str, str | None, str | None]:
    artifacts = result.get("artifacts") or {}
    source_quality_state = str(artifacts.get("source_quality_state") or "").strip().lower()
    if source_quality_state in {"ok", "warning", "degraded"} and (result.get("ok") or source_quality_state == "degraded"):
        source_quality_issue = str(artifacts.get("source_quality_issue") or "").strip() or None
        explicit_failure_class = str(artifacts.get("source_failure_class") or "").strip() or None
        if source_quality_state == "degraded":
            return "degraded", source_quality_issue, explicit_failure_class
        return source_quality_state, source_quality_issue, None

    warnings = [str(item) for item in (result.get("warnings") or []) if item]
    warning_text = "; ".join(warnings) if warnings else None
    if not result.get("ok"):
        error_text = "; ".join(
            str(item)
            for item in ((result.get("fatal_errors") or []) + (result.get("retriable_errors") or []))[:3]
        ) or "job_failed"
        lowered = error_text.lower()
        if "timeout" in lowered:
            failure_class = "timeout"
        elif any(token in lowered for token in ("tls", "ssl", "cert")):
            failure_class = "tls"
        elif "404" in lowered:
            failure_class = "not_found"
        else:
            failure_class = "runtime_error"
        return "degraded", error_text, failure_class
    if warning_text:
        lowered = warning_text.lower()
        if any(token in lowered for token in CRITICAL_WARNING_TOKENS):
            if "timeout" in lowered:
                failure_class = "timeout"
            elif any(token in lowered for token in ("tls", "ssl", "cert")):
                failure_class = "tls"
            elif "404" in lowered:
                failure_class = "not_found"
            elif "map.svg" in lowered or "placeholder" in lowered:
                failure_class = "bad_asset"
            else:
                failure_class = "quality_warning"
            return "degraded", warning_text, failure_class
        return "warning", warning_text, None
    return "ok", None, None


def _mark_source_state(conn, spec, result: dict[str, Any]):
    success = bool(result.get("ok"))
    error_text = None
    if not success:
        errors = result.get("fatal_errors") or result.get("retriable_errors") or []
        error_text = "; ".join(str(item) for item in errors[:3]) if errors else "job_failed"
    quality_state, quality_issue, failure_class = _quality_for_job_result(result)
    for source_key in spec.source_keys:
        update_source_sync_state(
            conn,
            source_key=source_key,
            success=success,
            transport_mode=spec.id,
            last_error=error_text,
            quality_state=quality_state,
            quality_issue=quality_issue,
            failure_class=failure_class,
            metadata={
                "job_id": spec.id,
                "items_seen": result.get("items_seen", 0),
                "items_new": result.get("items_new", 0),
                "items_updated": result.get("items_updated", 0),
            },
        )


def _finalize_job_state(
    settings: dict[str, Any],
    *,
    job_id: str,
    owner: str,
    run_id: int | None,
    spec,
    result: dict[str, Any],
    pipeline_version: str | None,
):
    last_error: Exception | None = None
    for attempt in range(FINALIZATION_RETRIES):
        conn = get_db(settings)
        try:
            if job_id == "source_health" and result.get("ok"):
                record_source_health_report(conn, result.get("artifacts") or {}, settings=settings)
            if job_id == "analysis_snapshot" and result.get("ok"):
                version = pipeline_version or str(
                    (result.get("artifacts") or {}).get("pipeline_version") or ""
                ).strip() or None
                if version:
                    set_runtime_metadata(conn, "analysis_built_from_pipeline_version", version)
                set_runtime_metadata(conn, "analysis_generated_at", result.get("finished_at") or now_iso())
            if job_id == "obsidian_export" and result.get("ok"):
                version = (
                    pipeline_version
                    or str(
                        (result.get("artifacts") or {}).get("pipeline_version") or ""
                    ).strip()
                    or None
                )
                if version:
                    set_runtime_metadata(conn, "obsidian_built_from_pipeline_version", version)
                set_runtime_metadata(conn, "obsidian_export_generated_at", result.get("finished_at") or now_iso())
            _mark_source_state(conn, spec, result)
            if run_id is not None:
                finish_job_run(conn, run_id, result)
            set_runtime_metadata(conn, f"last_job_finished:{job_id}", result.get("finished_at") or now_iso())
            release_job_lease(conn, job_id, owner)
            return
        except sqlite3.OperationalError as error:
            last_error = error
            if not _is_retriable_exception(error) or attempt >= FINALIZATION_RETRIES - 1:
                raise
            log.warning("Finalization retry for %s skipped on locked DB (%d/%d): %s", job_id, attempt + 1, FINALIZATION_RETRIES, error)
            time.sleep(FINALIZATION_RETRY_DELAY_SEC)
        finally:
            conn.close()
    if last_error:
        raise last_error


def run_job_once(
    job_id: str,
    *,
    settings: dict[str, Any] | None = None,
    trigger_mode: str = "manual",
    requested_by: str = "cli",
    pipeline_version: str | None = None,
    pipeline_run_id: int | None = None,
    owner: str | None = None,
    respect_leases: bool = True,
) -> dict[str, Any]:
    settings = settings or load_settings()
    spec = get_job_spec(job_id)
    started_at = now_iso()
    owner = owner or _owner_name(job_id)

    if spec is None:
        return JobResult.failure(
            job_id=job_id,
            started_at=started_at,
            fatal_errors=[f"unknown_job:{job_id}"],
        ).to_dict()

    conn = get_db(settings)
    stop_event = threading.Event()
    heartbeat_thread = None
    run_id = None
    result: dict[str, Any] = JobResult.failure(
        job_id=job_id,
        started_at=started_at,
        fatal_errors=["job_runner_no_result"],
    ).to_dict()

    try:
        if respect_leases and not acquire_job_lease(
            conn,
            job_id,
            owner,
            ttl_seconds=spec.timeout_seconds,
            payload={"trigger_mode": trigger_mode, "requested_by": requested_by},
        ):
            result = JobResult.failure(
                job_id=job_id,
                started_at=started_at,
                retriable_errors=[f"active_lease:{job_id}"],
            ).to_dict()
            return result

        run_id = start_job_run(
            conn,
            job_id=job_id,
            trigger_mode=trigger_mode,
            requested_by=requested_by,
            owner=owner,
            pipeline_version=pipeline_version,
            pipeline_run_id=pipeline_run_id,
        )
        set_runtime_metadata(conn, f"last_job_started:{job_id}", started_at)

        heartbeat_thread = threading.Thread(
            target=_heartbeat_loop,
            args=(stop_event, settings, job_id, owner, spec.timeout_seconds),
            daemon=True,
        )
        heartbeat_thread.start()

        raw_result = run_job_callable(job_id, settings)
        result = normalize_job_output(job_id, started_at, raw_result).to_dict()
    except Exception as exc:
        formatted = traceback.format_exc(limit=12)
        if _is_retriable_exception(exc):
            result = JobResult.failure(
                job_id=job_id,
                started_at=started_at,
                retriable_errors=[f"{type(exc).__name__}: {exc}"],
                artifacts={"traceback": formatted},
            ).to_dict()
        else:
            result = JobResult.failure(
                job_id=job_id,
                started_at=started_at,
                fatal_errors=[f"{type(exc).__name__}: {exc}"],
                artifacts={"traceback": formatted},
            ).to_dict()
    finally:
        stop_event.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=2)

        try:
            conn.close()
            _finalize_job_state(
                settings,
                job_id=job_id,
                owner=owner,
                run_id=run_id,
                spec=spec,
                result=result,
                pipeline_version=pipeline_version,
            )
        except Exception as error:
            message = f"finalization_error:{type(error).__name__}: {error}"
            if _is_retriable_exception(error):
                result.setdefault("warnings", []).append(message)
            else:
                result.setdefault("fatal_errors", []).append(message)
                result["ok"] = False
        finally:
            conn.close()

    return result
