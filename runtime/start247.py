from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from collectors.telegram_session_pool import assign_telegram_sources, import_telegram_sessions
from config.db_utils import PROJECT_ROOT, ensure_dirs, get_db, load_settings, setup_logging
from runtime import task_scheduler
from runtime.state import (
    DAEMON_JOB_ID,
    active_job_lease,
    recover_abandoned_runs,
    set_runtime_metadata,
)


def _spawn_detached(command: list[str]) -> dict[str, Any]:
    flags = 0
    kwargs: dict[str, Any] = {
        "cwd": str(PROJECT_ROOT),
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
    }
    if sys.platform.startswith("win"):
        flags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        kwargs["creationflags"] = flags
    process = subprocess.Popen(command, **kwargs)
    return {"ok": True, "pid": process.pid, "creationflags": flags}


def _active_lease(conn, job_id: str) -> dict[str, Any] | None:
    try:
        return active_job_lease(conn, job_id)
    except Exception:
        return None


def ensure_247(
    settings: dict[str, Any] | None = None,
    *,
    install_autostart: bool = True,
    start_daemon: bool = True,
    start_catchup: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    settings = settings or load_settings()
    setup_logging(settings)
    ensure_dirs(settings)

    conn = get_db(settings)
    try:
        recovery = recover_abandoned_runs(conn)
        telegram_import = import_telegram_sessions(conn, settings)
        assignment = assign_telegram_sources(conn)

        autostart: dict[str, Any]
        if install_autostart:
            try:
                autostart = task_scheduler.install_task(
                    repo_root=PROJECT_ROOT,
                    python_exe=sys.executable,
                    no_preflight=True,
                    dry_run=dry_run,
                )
            except Exception as error:
                autostart = {
                    "ok": False,
                    "install_mode": "autostart_failed",
                    "error": f"{type(error).__name__}: {error}",
                }
        else:
            autostart = {"ok": True, "install_mode": "skipped"}

        autostart_mode = str(autostart.get("install_mode") or ("ok" if autostart.get("ok") else "autostart_failed"))
        set_runtime_metadata(conn, "mode_247_enabled", "True")
        set_runtime_metadata(conn, "mode_247_last_started_at", __import__("runtime.state", fromlist=["now_iso"]).now_iso())
        set_runtime_metadata(conn, "mode_247_autostart_status", autostart_mode)
        set_runtime_metadata(conn, "mode_247_last_result", {"autostart": autostart, "telegram": telegram_import})

        daemon_lease = _active_lease(conn, DAEMON_JOB_ID)
        daemon = {"started": False, "status": "already_running" if daemon_lease else "skipped", "active_lease": daemon_lease}
        catchup_lease = _active_lease(conn, "collect_catchup")
        catchup = {"started": False, "status": "already_running" if catchup_lease else "skipped", "active_lease": catchup_lease}
    finally:
        conn.close()

    if start_daemon and not daemon["active_lease"] and not dry_run:
        daemon_spawn = _spawn_detached([sys.executable, "-m", "runtime.daemon", "--no-preflight"])
        daemon = {"started": True, "status": "starting", **daemon_spawn}
    elif start_daemon and not daemon["active_lease"] and dry_run:
        daemon = {"started": False, "status": "dry_run"}

    if start_catchup and not catchup["active_lease"] and not dry_run:
        catchup_spawn = _spawn_detached(
            [
                sys.executable,
                "-m",
                "runtime.run_job",
                "--job",
                "collect_catchup",
                "--trigger-mode",
                "manual",
                "--requested-by",
                "247",
            ]
        )
        catchup = {"started": True, "status": "starting", **catchup_spawn}
    elif start_catchup and not catchup["active_lease"] and dry_run:
        catchup = {"started": False, "status": "dry_run"}

    return {
        "ok": True,
        "mode": "24/7",
        "recovery": recovery,
        "autostart": autostart,
        "telegram": {
            "import": telegram_import,
            "assignment": assignment,
        },
        "daemon": daemon,
        "catchup": catchup,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Enable Civic Evidence Lab 24/7 runtime.")
    parser.add_argument("--no-autostart", action="store_true")
    parser.add_argument("--no-daemon", action="store_true")
    parser.add_argument("--no-catchup", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    result = ensure_247(
        install_autostart=not args.no_autostart,
        start_daemon=not args.no_daemon,
        start_catchup=not args.no_catchup,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
