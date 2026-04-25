from __future__ import annotations

import argparse
import json

from runtime.pipeline import run_pipeline


def main():
    parser = argparse.ArgumentParser(description="Run an orchestrated pipeline mode")
    parser.add_argument("--mode", choices=["incremental", "nightly", "weekly_maintenance"], required=True)
    parser.add_argument("--requested-by", default="cli")
    args = parser.parse_args()
    result = run_pipeline(args.mode, requested_by=args.requested_by)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result.get("ok") else 1)


if __name__ == "__main__":
    main()
