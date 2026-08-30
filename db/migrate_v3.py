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

    conn.execute("""
        CREATE TABLE IF NOT EXISTS daemon_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type TEXT NOT NULL,
            severity TEXT DEFAULT 'warning',
            message TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_daemon_alerts_type
        ON daemon_alerts(alert_type)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_daemon_alerts_severity
        ON daemon_alerts(severity)
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS ner_new_entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id INTEGER NOT NULL,
            entity_type TEXT,
            canonical_name TEXT,
            discovered_at TEXT DEFAULT (datetime('now')),
            source_content_id INTEGER,
            search_triggered INTEGER DEFAULT 0,
            FOREIGN KEY (entity_id) REFERENCES entities(id),
            FOREIGN KEY (source_content_id) REFERENCES content_items(id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ner_new_entities_search
        ON ner_new_entities(search_triggered)
    """)

    conn.commit()
    log.info("migrate_v3: daemon_alerts + ner_new_entities tables created")

    try:
        conn.execute("ALTER TABLE content_items ADD COLUMN garbage_checked INTEGER DEFAULT 0")
    except Exception:
        pass

    try:
        conn.execute("ALTER TABLE content_items ADD COLUMN claims_processed INTEGER DEFAULT 0")
    except Exception:
        pass

    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_content_items_llm ON content_items(llm_processed)",
        "CREATE INDEX IF NOT EXISTS idx_content_items_ner ON content_items(ner_processed)",
        "CREATE INDEX IF NOT EXISTS idx_content_items_classif ON content_items(classification_v3_processed)",
        "CREATE INDEX IF NOT EXISTS idx_content_items_garbage ON content_items(garbage_checked)",
        "CREATE INDEX IF NOT EXISTS idx_content_items_claims ON content_items(claims_processed)",
        "CREATE INDEX IF NOT EXISTS idx_content_items_status ON content_items(status)",
    ]:
        try:
            conn.execute(idx_sql)
        except Exception:
            pass

    conn.commit()
    log.info("migrate_v3: garbage_checked + claims_processed columns + indexes added")


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
