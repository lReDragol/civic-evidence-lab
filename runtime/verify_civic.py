"""Verify additive legacy migration on a new backup, never on the source DB."""
import argparse
import json
import sqlite3
from pathlib import Path
from config.db_utils import exec_schema
from db.migration_runner import apply_pending_migrations


def verify_copy(source, target):
    if target.exists() or target.resolve()==source.resolve():
        raise ValueError("Verification target must be a new distinct file")
    target.parent.mkdir(parents=True,exist_ok=True)
    with sqlite3.connect(source.resolve().as_uri()+"?mode=ro",uri=True) as original:
        with sqlite3.connect(target) as copy:
            original.backup(copy,pages=4096)
    conn=sqlite3.connect(target)
    try:
        tracked=("sources","raw_source_items","content_items","entity_relations","claims","agent_tasks")
        before={table:conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in tracked}
        foreign_before=len(conn.execute("PRAGMA foreign_key_check").fetchall())
        exec_schema(conn)
        applied=apply_pending_migrations(conn)
        repeated=apply_pending_migrations(conn)
        after={table:conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in tracked}
        check=conn.execute("PRAGMA quick_check").fetchone()[0]
        foreign_after=len(conn.execute("PRAGMA foreign_key_check").fetchall())
        return {"source_read_only":str(source),"target":str(target),"before":before,"after":after,
            "unchanged_counts":before==after,"quick_check":check,"foreign_keys_before":foreign_before,
            "foreign_keys_after":foreign_after,"migration":applied,"second_pass":repeated,
            "ok":before==after and check=="ok" and foreign_after<=foreign_before}
    finally:
        conn.close()


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source",type=Path,required=True)
    parser.add_argument("--target",type=Path,required=True)
    parser.add_argument("--report",type=Path,required=True)
    args=parser.parse_args()
    result=verify_copy(args.source,args.target)
    args.report.parent.mkdir(parents=True,exist_ok=True)
    args.report.write_text(json.dumps(result,indent=2),encoding="utf-8")
    print(json.dumps(result))
    raise SystemExit(0 if result["ok"] else 1)
