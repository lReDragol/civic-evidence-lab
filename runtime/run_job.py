from __future__ import annotations

import argparse
import json

from runtime.runner import run_job_once


def main():
    parser = argparse.ArgumentParser(description="Run one registered job with runtime tracking")
    parser.add_argument("--job", required=True)
    parser.add_argument("--trigger-mode", default="manual")
    parser.add_argument("--requested-by", default="cli")
    args = parser.parse_args()
    result = run_job_once(
        args.job,
        trigger_mode=args.trigger_mode,
        requested_by=args.requested_by,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result.get("ok") else 1)


if __name__ == "__main__":
    main()
