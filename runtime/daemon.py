from __future__ import annotations

import argparse
import json
import logging
import os
import threading
import time
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config.db_utils import ensure_dirs, get_db, load_settings, setup_logging
from agents.pipeline_runner import run_monitoring_cycle
from runtime.pipeline import run_pipeline
from runtime.registry import JOB_SPECS, get_job_spec, interval_for_job
from runtime.runner import run_job_once
from runtime.state import (
    DAEMON_JOB_ID,
    acquire_job_lease,
    active_job_lease,
    daemon_stop_requested,
    heartbeat_job_lease,
    has_recent_successful_run,
    record_runtime_event,
    recover_abandoned_runs,
    release_job_lease,
    request_daemon_stop,
    set_runtime_metadata,
    now_iso,
)


log = logging.getLogger(__name__)
DAEMON_LEASE_TTL_SECONDS = 180
DEPENDENCY_CHECK_WINDOW_HOURS = 48


def _check_deps_met(settings: dict[str, Any], job_id: str) -> tuple[bool, list[str]]:
    spec = get_job_spec(job_id)
    if not spec or not spec.depends_on:
        return True, []
    conn = get_db(settings)
    try:
        unmet = []
        for dep_id in spec.depends_on:
            if not has_recent_successful_run(conn, dep_id, within_hours=DEPENDENCY_CHECK_WINDOW_HOURS):
                unmet.append(dep_id)
        return len(unmet) == 0, unmet
    finally:
        conn.close()


def _run_job_with_deps(job_id: str, settings: dict[str, Any], trigger_mode: str, requested_by: str, owner: str):
    deps_met, unmet = _check_deps_met(settings, job_id)
    if not deps_met:
        log.warning("Job %s skipped: dependencies not met (%s)", job_id, ", ".join(unmet))
        return
    run_job_once(
        job_id=job_id,
        settings=settings,
        trigger_mode=trigger_mode,
        requested_by=requested_by,
        owner=owner,
    )


def _daemon_owner() -> str:
    return f"daemon:{os.getpid()}"


def _daemon_scheduler(settings: dict[str, Any], daemon_owner: str) -> BackgroundScheduler:
    scheduler_cfg = settings.get("scheduler", {}) if isinstance(settings.get("scheduler"), dict) else {}
    scheduler = BackgroundScheduler(
        job_defaults={
            "coalesce": bool(scheduler_cfg.get("coalesce", True)),
            "max_instances": int(scheduler_cfg.get("max_instances", 1)),
            "misfire_grace_time": int(scheduler_cfg.get("misfire_grace_time", 900)),
        }
    )

    for spec in JOB_SPECS:
        if not spec.scheduled:
            continue
        if spec.stage == "maintenance" and spec.id != "backup":
            continue
        interval_seconds = interval_for_job(settings, spec.id)
        scheduler.add_job(
            _run_job_with_deps,
            IntervalTrigger(seconds=interval_seconds),
            kwargs={
                "job_id": spec.id,
                "settings": settings,
                "trigger_mode": "scheduled",
                "requested_by": "daemon",
                "owner": daemon_owner,
            },
            id=f"job:{spec.id}",
            replace_existing=True,
            name=spec.name,
        )

    nightly_interval = int(scheduler_cfg.get("nightly_interval_seconds", 86400))
    weekly_interval = int(scheduler_cfg.get("weekly_interval_seconds", 604800))
    scheduler.add_job(
        run_pipeline,
        IntervalTrigger(seconds=nightly_interval),
        kwargs={"mode": "nightly", "settings": settings, "requested_by": "daemon"},
        id="pipeline:nightly",
        replace_existing=True,
        name="Nightly pipeline",
    )
    scheduler.add_job(
        run_pipeline,
        IntervalTrigger(seconds=weekly_interval),
        kwargs={"mode": "weekly_maintenance", "settings": settings, "requested_by": "daemon"},
        id="pipeline:weekly_maintenance",
        replace_existing=True,
        name="Weekly maintenance pipeline",
    )

    monitoring_interval = int(scheduler_cfg.get("monitoring_interval_seconds", 300))
    scheduler.add_job(
        run_monitoring_cycle,
        IntervalTrigger(seconds=monitoring_interval),
        kwargs={"settings": settings},
        id="pipeline:monitor",
        replace_existing=True,
        name="Pipeline monitoring cycle",
    )
    return scheduler


def _daemon_heartbeat(stop_event: threading.Event, settings: dict[str, Any], owner: str):
    check_counter = 0
    # Slow heartbeat to 60s to reduce SQLite write contention
    while not stop_event.wait(60):
        conn = None
        try:
            conn = get_db(settings)
            heartbeat_job_lease(conn, DAEMON_JOB_ID, owner, ttl_seconds=DAEMON_LEASE_TTL_SECONDS)
            set_runtime_metadata(conn, "daemon_owner", owner)
            set_runtime_metadata(conn, "daemon_last_seen_at", now_iso())
            record_runtime_event(
                conn,
                level="debug",
                event_type="daemon_heartbeat",
                stage="daemon",
                job_id=DAEMON_JOB_ID,
                message="daemon heartbeat",
                payload={"owner": owner},
            )

            check_counter += 1
            if check_counter % 6 == 0:
                from llm.key_monitor import check_and_alert
                check_and_alert(conn)

        except Exception as error:
            log.warning("Daemon heartbeat skipped: %s: %s", type(error).__name__, error)
        finally:
            if conn is not None:
                conn.close()


def run_daemon(settings: dict[str, Any] | None = None, *, no_preflight: bool = False) -> dict[str, Any]:
    settings = settings or load_settings()
    setup_logging(settings)
    ensure_dirs(settings)
    daemon_owner = _daemon_owner()

    from config.db_utils import exec_schema, SCHEMA_PATH
    from db.migrate_v3 import migrate
    conn = get_db(settings)
    try:
        exec_schema(conn, SCHEMA_PATH)
        migrate(conn)
        recovery = recover_abandoned_runs(conn)
        if not acquire_job_lease(
            conn,
            DAEMON_JOB_ID,
            daemon_owner,
            ttl_seconds=DAEMON_LEASE_TTL_SECONDS,
            payload={"pid": os.getpid()},
        ):
            lease = active_job_lease(conn, DAEMON_JOB_ID)
            return {
                "ok": False,
                "error": "daemon_already_running",
                "active_lease": lease,
                "recovery": recovery,
            }
        request_daemon_stop(conn, False)
    finally:
        conn.close()

    if not no_preflight:
        run_job_once("source_health", settings=settings, trigger_mode="preflight", requested_by="daemon", owner=daemon_owner)

    scheduler = _daemon_scheduler(settings, daemon_owner)
    stop_event = threading.Event()
    heartbeat_thread = threading.Thread(
        target=_daemon_heartbeat,
        args=(stop_event, settings, daemon_owner),
        daemon=True,
    )
    heartbeat_thread.start()
    scheduler.start()

    try:
        while True:
            conn = get_db(settings)
            try:
                if daemon_stop_requested(conn):
                    break
            finally:
                conn.close()
            time.sleep(1)
    finally:
        stop_event.set()
        heartbeat_thread.join(timeout=2)
        scheduler.shutdown(wait=False)
        conn = get_db(settings)
        try:
            release_job_lease(conn, DAEMON_JOB_ID, daemon_owner)
            request_daemon_stop(conn, False)
        finally:
            conn.close()

    return {"ok": True, "daemon_owner": daemon_owner}


def daemon_status(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    setup_logging(settings)
    conn = get_db(settings)
    try:
        lease = active_job_lease(conn, DAEMON_JOB_ID)
        return {"ok": True, "running": lease is not None, "active_lease": lease}
    finally:
        conn.close()


def stop_daemon(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    setup_logging(settings)
    conn = get_db(settings)
    try:
        lease = active_job_lease(conn, DAEMON_JOB_ID)
        request_daemon_stop(conn, True)
        record_runtime_event(
            conn,
            level="warning",
            event_type="daemon_stop_requested",
            stage="daemon",
            job_id=DAEMON_JOB_ID,
            message="daemon stop requested",
            payload={"active_lease": lease},
        )
        return {
            "ok": True,
            "stop_requested": True,
            "running": lease is not None,
            "active_lease": lease,
        }
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Run the Civic Evidence Lab background daemon")
    parser.add_argument("--no-preflight", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--stop", action="store_true")
    args = parser.parse_args()
    if args.status:
        result = daemon_status()
    elif args.stop:
        result = stop_daemon()
    else:
        result = run_daemon(no_preflight=args.no_preflight)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result.get("ok") else 1)


if __name__ == "__main__":
    main()
