from __future__ import annotations

import argparse
import json

from config.db_utils import get_db, load_settings
from runtime.state import force_recover_job, recover_abandoned_runs, request_daemon_stop


def main():
    parser = argparse.ArgumentParser(description="Recover abandoned runtime state or request daemon stop")
    parser.add_argument("--stale-seconds", type=int, default=1800)
    parser.add_argument("--request-daemon-stop", action="store_true")
    parser.add_argument("--clear-daemon-stop", action="store_true")
    parser.add_argument("--force-job", action="append", default=[], help="Force-release lease and abandon running job by id")
    args = parser.parse_args()

    settings = load_settings()
    conn = get_db(settings)
    try:
        recovery = recover_abandoned_runs(conn, stale_seconds=args.stale_seconds)
        forced = {job_id: force_recover_job(conn, job_id, reason="Force recovered via runtime.recover") for job_id in args.force_job}
        if args.request_daemon_stop:
            request_daemon_stop(conn, True)
        if args.clear_daemon_stop:
            request_daemon_stop(conn, False)
        result = {
            "recovery": recovery,
            "forced": forced,
            "daemon_stop_requested": bool(args.request_daemon_stop and not args.clear_daemon_stop),
        }
    finally:
        conn.close()

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
