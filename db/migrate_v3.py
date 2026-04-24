import json
import logging
import sqlite3
import sys
from pathlib import Path

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import load_settings

log = logging.getLogger(__name__)


def migrate(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS investigation_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            seed_entity_id INTEGER NOT NULL,
            title TEXT,
            params_json TEXT,
            result_json TEXT,
            dossier_text TEXT,
            status TEXT DEFAULT 'active',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (seed_entity_id) REFERENCES entities(id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_inv_results_seed
        ON investigation_results(seed_entity_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_inv_results_status
        ON investigation_results(status)
    """)
    conn.commit()
    log.info("migrate_v3: investigation_results table created")


def save_investigation(conn, seed_entity_id, result, dossier_text, params=None):
    title = f"Расследование: {result.seed_name}"
    cur = conn.execute(
        "INSERT INTO investigation_results(seed_entity_id, title, params_json, result_json, dossier_text) "
        "VALUES(?,?,?,?,?)",
        (
            seed_entity_id,
            title,
            json.dumps(params or {}, ensure_ascii=False),
            result.to_json(),
            dossier_text,
        ),
    )
    conn.commit()
    return cur.lastrowid


def load_investigation(conn, investigation_id):
    row = conn.execute(
        "SELECT * FROM investigation_results WHERE id=?", (investigation_id,)
    ).fetchone()
    if not row:
        return None
    from investigation.models import InvestigationResult
    result = InvestigationResult.from_json(row["result_json"])
    return {
        "id": row["id"],
        "seed_entity_id": row["seed_entity_id"],
        "title": row["title"],
        "params": json.loads(row["params_json"]) if row["params_json"] else {},
        "result": result,
        "dossier_text": row["dossier_text"],
        "status": row["status"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def list_investigations(conn, status=None):
    query = "SELECT id, seed_entity_id, title, status, created_at FROM investigation_results"
    params = []
    if status:
        query += " WHERE status=?"
        params.append(status)
    query += " ORDER BY created_at DESC LIMIT 50"
    return [dict(r) for r in conn.execute(query, params).fetchall()]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    settings = load_settings()
    from config.db_utils import get_db
    conn = get_db(settings)
    migrate(conn)
    conn.close()
    print("Migration v3 complete")
