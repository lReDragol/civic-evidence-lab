import json
import logging
import sqlite3
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SETTINGS_PATH = PROJECT_ROOT / "config" / "settings.json"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _load_settings():
    if SETTINGS_PATH.exists():
        return json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    return {}


def _get_db(settings):
    p = Path(settings.get("db_path", str(PROJECT_ROOT / "db" / "news_unified.db")))
    conn = sqlite3.connect(str(p))
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


NEW_TABLES_SQL = """

CREATE TABLE IF NOT EXISTS bills (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    number          TEXT NOT NULL,
    title           TEXT NOT NULL,
    bill_type       TEXT,
    status          TEXT,
    registration_date TEXT,
    duma_url        TEXT,
    committee       TEXT,
    keywords        TEXT,
    annotation      TEXT,
    raw_data        TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(number)
);

CREATE INDEX IF NOT EXISTS idx_bills_number ON bills(number);
CREATE INDEX IF NOT EXISTS idx_bills_status ON bills(status);
CREATE INDEX IF NOT EXISTS idx_bills_type ON bills(bill_type);
CREATE INDEX IF NOT EXISTS idx_bills_date ON bills(registration_date);

CREATE TABLE IF NOT EXISTS bill_sponsors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bill_id         INTEGER NOT NULL,
    entity_id       INTEGER,
    sponsor_name    TEXT NOT NULL,
    sponsor_role    TEXT,
    faction         TEXT,
    is_collective   INTEGER DEFAULT 0,
    FOREIGN KEY (bill_id) REFERENCES bills(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_bill_sponsors_bill ON bill_sponsors(bill_id);
CREATE INDEX IF NOT EXISTS idx_bill_sponsors_entity ON bill_sponsors(entity_id);
CREATE INDEX IF NOT EXISTS idx_bill_sponsors_faction ON bill_sponsors(faction);

CREATE TABLE IF NOT EXISTS bill_votes_sessions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bill_id         INTEGER NOT NULL,
    vote_date       TEXT NOT NULL,
    vote_stage      TEXT,
    total_for       INTEGER DEFAULT 0,
    total_against   INTEGER DEFAULT 0,
    total_abstained INTEGER DEFAULT 0,
    total_absent    INTEGER DEFAULT 0,
    total_present   INTEGER DEFAULT 0,
    result          TEXT,
    duma_session    TEXT,
    raw_data        TEXT,
    FOREIGN KEY (bill_id) REFERENCES bills(id) ON DELETE CASCADE,
    UNIQUE(bill_id, vote_date, vote_stage)
);

CREATE INDEX IF NOT EXISTS idx_bill_vote_sessions_bill ON bill_vote_sessions(bill_id);
CREATE INDEX IF NOT EXISTS idx_bill_vote_sessions_date ON bill_vote_sessions(vote_date);

CREATE TABLE IF NOT EXISTS bill_votes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vote_session_id INTEGER NOT NULL,
    entity_id       INTEGER,
    deputy_name     TEXT NOT NULL,
    faction         TEXT,
    vote_result     TEXT NOT NULL,
    raw_data        TEXT,
    FOREIGN KEY (vote_session_id) REFERENCES bill_vote_sessions(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE SET NULL,
    UNIQUE(vote_session_id, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_bill_votes_session ON bill_votes(vote_session_id);
CREATE INDEX IF NOT EXISTS idx_bill_votes_entity ON bill_votes(entity_id);
CREATE INDEX IF NOT EXISTS idx_bill_votes_result ON bill_votes(vote_result);
CREATE INDEX IF NOT EXISTS idx_bill_votes_faction ON bill_votes(faction);

CREATE TABLE IF NOT EXISTS official_positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    position_title  TEXT NOT NULL,
    organization    TEXT NOT NULL,
    region          TEXT,
    faction         TEXT,
    started_at      TEXT,
    ended_at        TEXT,
    source_url      TEXT,
    source_type     TEXT,
    is_active       INTEGER DEFAULT 1,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_official_positions_entity ON official_positions(entity_id);
CREATE INDEX IF NOT EXISTS idx_official_positions_org ON official_positions(organization);
CREATE INDEX IF NOT EXISTS idx_official_positions_active ON official_positions(is_active);

CREATE TABLE IF NOT EXISTS party_memberships (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    party_name      TEXT NOT NULL,
    role            TEXT,
    started_at      TEXT,
    ended_at        TEXT,
    source_url      TEXT,
    is_current      INTEGER DEFAULT 1,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_party_memberships_entity ON party_memberships(entity_id);
CREATE INDEX IF NOT EXISTS idx_party_memberships_party ON party_memberships(party_name);
CREATE INDEX IF NOT EXISTS idx_party_memberships_current ON party_memberships(is_current);

CREATE TABLE IF NOT EXISTS investigative_materials (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER,
    material_type   TEXT NOT NULL,
    title           TEXT NOT NULL,
    summary         TEXT,
    involved_entities TEXT,
    referenced_laws TEXT,
    referenced_cases TEXT,
    publication_date TEXT,
    source_org      TEXT,
    source_credibility TEXT,
    verification_status TEXT DEFAULT 'unverified',
    url             TEXT,
    raw_data        TEXT,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_investigative_type ON investigative_materials(material_type);
CREATE INDEX IF NOT EXISTS idx_investigative_status ON investigative_materials(verification_status);
CREATE INDEX IF NOT EXISTS idx_investigative_org ON investigative_materials(source_org);

CREATE TABLE IF NOT EXISTS tag_explanations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_tag_id  INTEGER NOT NULL,
    trigger_text    TEXT NOT NULL,
    trigger_rule    TEXT NOT NULL,
    matched_pattern TEXT,
    confidence_raw  REAL,
    FOREIGN KEY (content_tag_id) REFERENCES content_tags(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tag_explanations_tag ON tag_explanations(content_tag_id);

CREATE TABLE IF NOT EXISTS law_references (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL,
    law_type        TEXT NOT NULL,
    law_number      TEXT,
    article         TEXT,
    context         TEXT,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_law_references_item ON law_references(content_item_id);
CREATE INDEX IF NOT EXISTS idx_law_references_type ON law_references(law_type);
CREATE INDEX IF NOT EXISTS idx_law_references_number ON law_references(law_number);

"""


def apply_v2_migrations(conn):
    existing_tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}

    new_tables = [
        "bills", "bill_sponsors", "bill_vote_sessions", "bill_votes",
        "official_positions", "party_memberships", "investigative_materials",
        "tag_explanations", "law_references",
    ]

    needs_creation = any(t not in existing_tables for t in new_tables)
    if needs_creation:
        statements = [s.strip() for s in NEW_TABLES_SQL.split(";") if s.strip() and not s.strip().startswith("--")]
        for stmt in statements:
            try:
                conn.execute(stmt)
            except Exception as e:
                log.warning("Migration statement failed: %s — %s", stmt[:80], e)
        conn.commit()
        log.info("V2 schema: created new tables")

    cols_content = {row[1] for row in conn.execute("PRAGMA table_info(content_items)")}
    if "authenticity_score" not in cols_content:
        conn.execute("ALTER TABLE content_items ADD COLUMN authenticity_score REAL DEFAULT NULL")
        conn.commit()
        log.info("Added authenticity_score to content_items")

    cols_claims = {row[1] for row in conn.execute("PRAGMA table_info(claims)")}
    new_claim_cols = {
        "temporal_consistency": "temporal_consistency REAL DEFAULT 0",
        "cross_source_score": "cross_source_score REAL DEFAULT 0",
        "entity_verification_score": "entity_verification_score REAL DEFAULT 0",
        "rhetoric_risk_score": "rhetoric_risk_score REAL DEFAULT 0",
        "contradiction_score": "contradiction_score REAL DEFAULT 0",
    }
    for col, ddl in new_claim_cols.items():
        if col not in cols_claims:
            conn.execute(f"ALTER TABLE claims ADD COLUMN {ddl}")
            conn.commit()
            log.info("Added %s to claims", col)

    for table in new_tables:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info("  %s: %d rows", table, cnt)


def main():
    settings = _load_settings()
    conn = _get_db(settings)
    apply_v2_migrations(conn)

    all_tables = [row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )]
    log.info("All tables (%d): %s", len(all_tables), ", ".join(all_tables))
    conn.close()
    log.info("V2 migration complete")


if __name__ == "__main__":
    main()
