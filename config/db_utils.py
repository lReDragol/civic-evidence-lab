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


def exec_schema(conn: sqlite3.Connection, schema_path: Path | None = None):
    target_schema = schema_path or SCHEMA_PATH
    sql = target_schema.read_text(encoding="utf-8")
    conn.executescript(sql)
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
