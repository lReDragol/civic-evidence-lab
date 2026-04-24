from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings
from investigation.engine import InvestigationEngine
from investigation.dossier import DossierGenerator
from investigation.models import Confidence


def main():
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Investigation Engine CLI")
    parser.add_argument("--entity", type=int, required=True, help="Seed entity ID")
    parser.add_argument("--hops", type=int, default=3, help="Max expansion hops")
    parser.add_argument("--min-confidence", type=str, default="likely",
                        choices=["confirmed", "likely", "unconfirmed", "disputed"],
                        help="Minimum confidence to expand")
    parser.add_argument("--db", type=str, default=None, help="Database path override")
    parser.add_argument("--save", action="store_true", help="Save result to DB")
    parser.add_argument("--json", type=str, default=None, help="Save result JSON to file")
    parser.add_argument("--gui", action="store_true", help="Launch DearPyGui viewer")
    parser.add_argument("--find-entity", type=str, default=None,
                        help="Search for entity by name and show IDs")
    args = parser.parse_args()

    settings = load_settings()
    db_path = args.db or settings.get("db_path", "db/news_unified.db")

    if args.find_entity:
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, canonical_name, entity_type, inn FROM entities "
            "WHERE canonical_name LIKE ? ORDER BY id LIMIT 20",
            (f"%{args.find_entity}%",),
        ).fetchall()
        for r in rows:
            inn_str = f" ИНН:{r['inn']}" if r["inn"] else ""
            print(f"  ID={r['id']} [{r['entity_type']}] {r['canonical_name']}{inn_str}")
        conn.close()
        return

    min_conf = Confidence(args.min_confidence)
    engine = InvestigationEngine(db_path)

    print(f"Starting investigation: entity={args.entity}, hops={args.hops}, min_confidence={min_conf.value}")
    result = engine.investigate(args.entity, max_hops=args.hops, min_confidence=min_conf)

    dossier = DossierGenerator(result).generate()
    print(dossier)

    if args.save:
        from db.migrate_v3 import migrate, save_investigation
        conn = get_db(settings)
        migrate(conn)
        inv_id = save_investigation(conn, args.entity, result, dossier,
                                     params={"hops": args.hops, "min_confidence": min_conf.value})
        conn.close()
        print(f"Saved investigation id={inv_id}")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            f.write(result.to_json())
        print(f"JSON saved to {args.json}")

    if args.gui:
        from investigation.node_viewer import launch_viewer
        p = launch_viewer(result, dossier)
        print(f"DearPyGui viewer launched (PID={p.pid})")
        p.join()

    engine.close()


if __name__ == "__main__":
    main()
