from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TASK_NAME = r"CivicEvidenceLab\RuntimeDaemon"


def _normalize_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (PROJECT_ROOT / path).resolve()


def daemon_wrapper_path(repo_root: str | Path = PROJECT_ROOT) -> Path:
    return _normalize_path(repo_root) / "runtime" / "generated" / "daemon_task_wrapper.cmd"


def daemon_log_path(repo_root: str | Path = PROJECT_ROOT) -> Path:
    return _normalize_path(repo_root) / "logs" / "runtime_daemon_task.log"


def startup_folder() -> Path:
    appdata = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    return appdata / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"


def startup_launcher_path(task_name: str = DEFAULT_TASK_NAME) -> Path:
    safe_name = task_name.replace("\\", "_").replace("/", "_").replace(":", "_")
    return startup_folder() / f"{safe_name}.cmd"


def build_wrapper_contents(
    *,
    repo_root: str | Path = PROJECT_ROOT,
    python_exe: str | Path = sys.executable,
    no_preflight: bool = True,
) -> str:
    repo_root_path = _normalize_path(repo_root)
    python_path = _normalize_path(python_exe)
    log_path = daemon_log_path(repo_root_path)
    daemon_args = "--no-preflight" if no_preflight else ""
    return (
        "@echo off\r\n"
        "setlocal\r\n"
        f'cd /d "{repo_root_path}"\r\n'
        f'if not exist "{log_path.parent}" mkdir "{log_path.parent}"\r\n'
        f'echo [%DATE% %TIME%] starting runtime.daemon>> "{log_path}"\r\n'
        f'"{python_path}" -m runtime.daemon {daemon_args} >> "{log_path}" 2>&1\r\n'
        "set EXITCODE=%ERRORLEVEL%\r\n"
        f'echo [%DATE% %TIME%] runtime.daemon exited with %EXITCODE%>> "{log_path}"\r\n'
        "exit /b %EXITCODE%\r\n"
    )


def write_wrapper(
    *,
    repo_root: str | Path = PROJECT_ROOT,
    python_exe: str | Path = sys.executable,
    no_preflight: bool = True,
) -> Path:
    wrapper_path = daemon_wrapper_path(repo_root)
    wrapper_path.parent.mkdir(parents=True, exist_ok=True)
    wrapper_path.write_text(
        build_wrapper_contents(repo_root=repo_root, python_exe=python_exe, no_preflight=no_preflight),
        encoding="utf-8",
    )
    return wrapper_path


def write_startup_launcher(*, task_name: str = DEFAULT_TASK_NAME, repo_root: str | Path = PROJECT_ROOT) -> Path:
    launcher_path = startup_launcher_path(task_name)
    wrapper_path = daemon_wrapper_path(repo_root)
    launcher_path.parent.mkdir(parents=True, exist_ok=True)
    launcher_path.write_text(
        "@echo off\r\n"
        "setlocal\r\n"
        f'call "{wrapper_path}"\r\n',
        encoding="utf-8",
    )
    return launcher_path


def build_schtasks_create_command(
    *,
    task_name: str = DEFAULT_TASK_NAME,
    wrapper_path: str | Path,
    schedule: str = "onlogon",
    user: str | None = None,
    force: bool = True,
) -> list[str]:
    schedule_name = schedule.strip().lower()
    if schedule_name not in {"onlogon", "onstart"}:
        raise ValueError(f"Unsupported schedule: {schedule}")
    wrapper = _normalize_path(wrapper_path)
    trigger = "ONLOGON" if schedule_name == "onlogon" else "ONSTART"
    command = [
        "schtasks",
        "/Create",
        "/TN",
        task_name,
        "/TR",
        f'cmd.exe /c ""{wrapper}""',
        "/SC",
        trigger,
    ]
    if user and schedule_name != "onstart":
        command.extend(["/RU", user])
    if force:
        command.append("/F")
    return command


def build_schtasks_query_command(task_name: str = DEFAULT_TASK_NAME) -> list[str]:
    return ["schtasks", "/Query", "/TN", task_name, "/V", "/FO", "LIST"]


def build_schtasks_remove_command(task_name: str = DEFAULT_TASK_NAME, *, force: bool = True) -> list[str]:
    command = ["schtasks", "/Delete", "/TN", task_name]
    if force:
        command.append("/F")
    return command


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def install_task(
    *,
    task_name: str = DEFAULT_TASK_NAME,
    repo_root: str | Path = PROJECT_ROOT,
    python_exe: str | Path = sys.executable,
    schedule: str = "onlogon",
    user: str | None = None,
    force: bool = True,
    no_preflight: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    wrapper_path = write_wrapper(repo_root=repo_root, python_exe=python_exe, no_preflight=no_preflight)
    startup_path = startup_launcher_path(task_name)
    command = build_schtasks_create_command(
        task_name=task_name,
        wrapper_path=wrapper_path,
        schedule=schedule,
        user=user,
        force=force,
    )
    if dry_run:
        return {
            "ok": True,
            "task_name": task_name,
            "schedule": schedule,
            "wrapper_path": str(wrapper_path),
            "startup_launcher": str(startup_path),
            "command": command,
            "dry_run": True,
        }

    completed = _run(command)
    fallback_reason = (completed.stderr or "").strip().lower()
    if completed.returncode != 0 and schedule == "onlogon" and "access is denied" in fallback_reason:
        launcher_path = write_startup_launcher(task_name=task_name, repo_root=repo_root)
        return {
            "ok": True,
            "task_name": task_name,
            "schedule": schedule,
            "wrapper_path": str(wrapper_path),
            "startup_launcher": str(launcher_path),
            "command": command,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
            "returncode": completed.returncode,
            "install_mode": "startup_folder",
            "warnings": ["schtasks_access_denied_fallback_startup_folder"],
        }

    return {
        "ok": completed.returncode == 0,
        "task_name": task_name,
        "schedule": schedule,
        "wrapper_path": str(wrapper_path),
        "startup_launcher": str(startup_path),
        "command": command,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "returncode": completed.returncode,
        "install_mode": "task_scheduler" if completed.returncode == 0 else "task_scheduler_failed",
    }


def remove_task(*, task_name: str = DEFAULT_TASK_NAME, force: bool = True, dry_run: bool = False) -> dict[str, Any]:
    command = build_schtasks_remove_command(task_name, force=force)
    launcher_path = startup_launcher_path(task_name)
    if dry_run:
        return {
            "ok": True,
            "task_name": task_name,
            "command": command,
            "startup_launcher": str(launcher_path),
            "dry_run": True,
        }
    removed_startup = False
    if launcher_path.exists():
        launcher_path.unlink()
        removed_startup = True
    completed = _run(command)
    return {
        "ok": completed.returncode == 0 or removed_startup,
        "task_name": task_name,
        "command": command,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "returncode": completed.returncode,
        "startup_launcher": str(launcher_path),
        "removed_startup_launcher": removed_startup,
    }


def query_task(*, task_name: str = DEFAULT_TASK_NAME, dry_run: bool = False) -> dict[str, Any]:
    command = build_schtasks_query_command(task_name)
    launcher_path = startup_launcher_path(task_name)
    if dry_run:
        return {
            "ok": True,
            "task_name": task_name,
            "command": command,
            "startup_launcher": str(launcher_path),
            "dry_run": True,
        }
    completed = _run(command)
    if completed.returncode != 0 and launcher_path.exists():
        return {
            "ok": True,
            "task_name": task_name,
            "command": command,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
            "returncode": completed.returncode,
            "install_mode": "startup_folder",
            "startup_launcher": str(launcher_path),
        }
    return {
        "ok": completed.returncode == 0,
        "task_name": task_name,
        "command": command,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "returncode": completed.returncode,
        "install_mode": "task_scheduler" if completed.returncode == 0 else "task_scheduler_failed",
        "startup_launcher": str(launcher_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Install/query/remove the Windows Task Scheduler entry for runtime.daemon")
    parser.add_argument("--task-name", default=DEFAULT_TASK_NAME)
    parser.add_argument("--repo-root", default=str(PROJECT_ROOT))
    parser.add_argument("--python", dest="python_exe", default=sys.executable)
    parser.add_argument("--schedule", choices=["onlogon", "onstart"], default="onlogon")
    parser.add_argument("--user", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--remove", action="store_true")
    parser.add_argument("--query", action="store_true")
    parser.add_argument("--with-preflight", action="store_true")
    parser.add_argument("--no-force", action="store_true")
    args = parser.parse_args()

    if args.remove and args.query:
        raise SystemExit("Use either --remove or --query, not both.")

    if args.remove:
        result = remove_task(task_name=args.task_name, force=not args.no_force, dry_run=args.dry_run)
    elif args.query:
        result = query_task(task_name=args.task_name, dry_run=args.dry_run)
    else:
        result = install_task(
            task_name=args.task_name,
            repo_root=args.repo_root,
            python_exe=args.python_exe,
            schedule=args.schedule,
            user=args.user,
            force=not args.no_force,
            no_preflight=not args.with_preflight,
            dry_run=args.dry_run,
        )

    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result.get("ok") else 1)


if __name__ == "__main__":
    main()
