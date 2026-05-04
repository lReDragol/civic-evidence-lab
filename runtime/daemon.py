from __future__ import annotations

import argparse
import json
import os
import threading
import time
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config.db_utils import ensure_dirs, get_db, load_settings, setup_logging
from runtime.pipeline import run_pipeline
from runtime.registry import JOB_SPECS, interval_for_job
from runtime.runner import run_job_once
from runtime.state import (
    DAEMON_JOB_ID,
    acquire_job_lease,
    active_job_lease,
    daemon_stop_requested,
    heartbeat_job_lease,
    recover_abandoned_runs,
    release_job_lease,
    request_daemon_stop,
    set_runtime_metadata,
    now_iso,
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
            run_job_once,
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
    return scheduler


def _daemon_heartbeat(stop_event: threading.Event, settings: dict[str, Any], owner: str):
    while not stop_event.wait(10):
        conn = get_db(settings)
        try:
            heartbeat_job_lease(conn, DAEMON_JOB_ID, owner, ttl_seconds=45)
            set_runtime_metadata(conn, "daemon_owner", owner)
            set_runtime_metadata(conn, "daemon_last_seen_at", now_iso())
        finally:
            conn.close()


def run_daemon(settings: dict[str, Any] | None = None, *, no_preflight: bool = False) -> dict[str, Any]:
    settings = settings or load_settings()
    setup_logging(settings)
    ensure_dirs(settings)
    daemon_owner = _daemon_owner()
    conn = get_db(settings)
    try:
        recovery = recover_abandoned_runs(conn)
        if not acquire_job_lease(
            conn,
            DAEMON_JOB_ID,
            daemon_owner,
            ttl_seconds=45,
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


def main():
    parser = argparse.ArgumentParser(description="Run the Civic Evidence Lab background daemon")
    parser.add_argument("--no-preflight", action="store_true")
    args = parser.parse_args()
    result = run_daemon(no_preflight=args.no_preflight)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result.get("ok") else 1)


if __name__ == "__main__":
    main()
