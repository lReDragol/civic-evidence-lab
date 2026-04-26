import json
import logging
import os
import sqlite3
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
SETTINGS_PATH = CONFIG_DIR / "settings.json"
SECRETS_PATH = CONFIG_DIR / "secrets.json"
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"

ADDITIVE_COLUMNS = {
    "content_items": {
        "classification_v3_processed": "INTEGER DEFAULT 0",
    },
    "claims": {
        "canonical_text": "TEXT",
        "canonical_hash": "TEXT",
        "claim_cluster_id": "INTEGER",
    },
    "evidence_links": {
        "evidence_class": "TEXT DEFAULT 'support'",
    },
    "content_tags": {
        "namespace": "TEXT",
        "normalized_tag": "TEXT",
        "confidence_calibrated": "REAL",
        "decision_source": "TEXT",
    },
    "relation_candidates": {
        "seed_kind": "TEXT",
        "structural_score": "REAL DEFAULT 0",
        "semantic_score": "REAL DEFAULT 0",
        "support_score": "REAL DEFAULT 0",
        "calibrated_score": "REAL DEFAULT 0",
        "support_claim_cluster_count": "INTEGER DEFAULT 0",
        "support_hard_evidence_count": "INTEGER DEFAULT 0",
        "candidate_state": "TEXT DEFAULT 'pending'",
        "explain_path_json": "TEXT",
    },
}

ADDITIVE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS content_tag_votes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL,
    voter_name      TEXT NOT NULL,
    tag_name        TEXT NOT NULL,
    namespace       TEXT,
    normalized_tag  TEXT,
    vote_value      TEXT NOT NULL,
    confidence_raw  REAL DEFAULT 0,
    evidence_text   TEXT,
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_content_tag_votes_item ON content_tag_votes(content_item_id);
CREATE INDEX IF NOT EXISTS idx_content_tag_votes_tag ON content_tag_votes(normalized_tag);
CREATE INDEX IF NOT EXISTS idx_content_tag_votes_vote ON content_tag_votes(vote_value);

CREATE TABLE IF NOT EXISTS semantic_neighbors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_kind     TEXT NOT NULL,
    source_id       INTEGER NOT NULL,
    neighbor_kind   TEXT NOT NULL,
    neighbor_id     INTEGER NOT NULL,
    score           REAL DEFAULT 0,
    method          TEXT DEFAULT 'tfidf',
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(source_kind, source_id, neighbor_kind, neighbor_id, method)
);
CREATE INDEX IF NOT EXISTS idx_semantic_neighbors_source ON semantic_neighbors(source_kind, source_id);
CREATE INDEX IF NOT EXISTS idx_semantic_neighbors_neighbor ON semantic_neighbors(neighbor_kind, neighbor_id);

CREATE TABLE IF NOT EXISTS claim_clusters (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster_key     TEXT NOT NULL UNIQUE,
    canonical_text  TEXT NOT NULL,
    claim_type      TEXT,
    method          TEXT DEFAULT 'canonical',
    status          TEXT DEFAULT 'active',
    support_count   INTEGER DEFAULT 0,
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_claim_clusters_type ON claim_clusters(claim_type);
CREATE INDEX IF NOT EXISTS idx_claim_clusters_status ON claim_clusters(status);

CREATE TABLE IF NOT EXISTS claim_occurrences (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_cluster_id INTEGER NOT NULL,
    claim_id        INTEGER,
    content_item_id INTEGER,
    occurrence_text TEXT NOT NULL,
    occurrence_hash TEXT NOT NULL,
    source_kind     TEXT DEFAULT 'claim',
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (claim_cluster_id) REFERENCES claim_clusters(id) ON DELETE CASCADE,
    FOREIGN KEY (claim_id) REFERENCES claims(id) ON DELETE SET NULL,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL,
    UNIQUE(claim_cluster_id, content_item_id, occurrence_hash)
);
CREATE INDEX IF NOT EXISTS idx_claim_occurrences_cluster ON claim_occurrences(claim_cluster_id);
CREATE INDEX IF NOT EXISTS idx_claim_occurrences_claim ON claim_occurrences(claim_id);

CREATE TABLE IF NOT EXISTS relation_features (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id    INTEGER NOT NULL UNIQUE,
    structural_score REAL DEFAULT 0,
    content_support_score REAL DEFAULT 0,
    source_diversity_score REAL DEFAULT 0,
    semantic_support_score REAL DEFAULT 0,
    shared_claim_cluster_score REAL DEFAULT 0,
    evidence_quality_score REAL DEFAULT 0,
    temporal_score REAL DEFAULT 0,
    role_compatibility_score REAL DEFAULT 0,
    calibrated_score REAL DEFAULT 0,
    explain_path_json TEXT,
    metadata_json   TEXT,
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (candidate_id) REFERENCES relation_candidates(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_relation_features_candidate ON relation_features(candidate_id);
"""


def load_settings() -> dict:
    settings = {}
    if SETTINGS_PATH.exists():
        settings = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    if SECRETS_PATH.exists():
        secrets = json.loads(SECRETS_PATH.read_text(encoding="utf-8"))
        for k, v in secrets.items():
            if v is not None:
                settings[k] = v
    return settings


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    except sqlite3.DatabaseError:
        return set()


def _create_index_if_columns_exist(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    index_name: str,
    columns_sql: str,
    required_columns: tuple[str, ...],
):
    existing = _table_columns(conn, table_name)
    if not existing:
        return
    if any(column_name not in existing for column_name in required_columns):
        return
    conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({columns_sql})")


def ensure_additive_schema(conn: sqlite3.Connection):
    for table_name, columns in ADDITIVE_COLUMNS.items():
        existing = _table_columns(conn, table_name)
        if not existing:
            continue
        for column_name, column_sql in columns.items():
            if column_name in existing:
                continue
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")
    conn.executescript(ADDITIVE_SCHEMA_SQL)
    _create_index_if_columns_exist(
        conn,
        table_name="claims",
        index_name="idx_claims_canonical_hash",
        columns_sql="canonical_hash",
        required_columns=("canonical_hash",),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="claims",
        index_name="idx_claims_cluster",
        columns_sql="claim_cluster_id",
        required_columns=("claim_cluster_id",),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="evidence_links",
        index_name="idx_evidence_class",
        columns_sql="evidence_class",
        required_columns=("evidence_class",),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="content_tags",
        index_name="idx_content_tags_namespace",
        columns_sql="namespace",
        required_columns=("namespace",),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="content_tags",
        index_name="idx_content_tags_normalized",
        columns_sql="normalized_tag",
        required_columns=("normalized_tag",),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="relation_candidates",
        index_name="idx_relation_candidates_candidate_state",
        columns_sql="candidate_state",
        required_columns=("candidate_state",),
    )
    conn.commit()


def _execute_schema_sql(conn: sqlite3.Connection, sql: str):
    for raw_statement in sql.split(";"):
        statement = raw_statement.strip()
        if not statement:
            continue
        try:
            conn.execute(statement)
        except sqlite3.OperationalError as error:
            lowered = str(error).lower()
            normalized = statement.upper()
            if normalized.startswith("CREATE INDEX") and (
                "no such column" in lowered or "has no column named" in lowered
            ):
                log.warning("Skipping schema statement due to missing legacy column: %s", statement.splitlines()[0][:180])
                continue
            raise


def exec_schema(conn: sqlite3.Connection, schema_path: Path | None = None):
    target_schema = schema_path or SCHEMA_PATH
    sql = target_schema.read_text(encoding="utf-8")
    ensure_additive_schema(conn)
    _execute_schema_sql(conn, sql)
    ensure_additive_schema(conn)
    conn.commit()


def get_db(settings: dict = None) -> sqlite3.Connection:
    if settings is None:
        settings = load_settings()
    db_path = Path(settings.get("db_path", str(PROJECT_ROOT / "db" / "news_unified.db")))
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    if settings.get("ensure_schema_on_connect", True):
        exec_schema(conn, SCHEMA_PATH)
    return conn


def ensure_dirs(settings: dict = None):
    if settings is None:
        settings = load_settings()
    for key in [
        "inbox_tiktok", "inbox_documents", "inbox_youtube",
        "processed_tiktok", "processed_youtube", "processed_documents",
        "processed_telegram", "processed_keyframes",
    ]:
        p = Path(settings.get(key, str(PROJECT_ROOT / key.replace("_", "/", 1))))
        p.mkdir(parents=True, exist_ok=True)


def setup_logging(settings: dict = None):
    if settings is None:
        settings = load_settings()

    log_level = getattr(logging, settings.get("log_level", "INFO").upper(), logging.INFO)
    log_file = settings.get("log_file", str(PROJECT_ROOT / "app.log"))
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    managed_handlers = [
        h for h in root_logger.handlers
        if getattr(h, "_news_archive_handler", False)
    ]
    for handler in managed_handlers:
        root_logger.removeHandler(handler)
        handler.close()

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(log_level)
    ch.setFormatter(fmt)
    ch._news_archive_handler = True
    root_logger.addHandler(ch)

    fh = RotatingFileHandler(
        str(log_path), maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    fh.setLevel(log_level)
    fh.setFormatter(fmt)
    fh._news_archive_handler = True
    root_logger.addHandler(fh)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("pyrogram").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
