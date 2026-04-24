import hashlib
import json
import logging
import os
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DB_DIR = PROJECT_ROOT / "db"
SCHEMA_PATH = DB_DIR / "schema.sql"
SEED_PATH = CONFIG_DIR / "sources_seed.json"
SETTINGS_PATH = CONFIG_DIR / "settings.json"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db.file_store import attach_file, materialize_attachment


def load_settings() -> dict:
    if SETTINGS_PATH.exists():
        return json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    return {}


def get_db_path(settings: dict) -> Path:
    p = settings.get("db_path", str(DB_DIR / "news_unified.db"))
    return Path(p)


def get_legacy_db_path(settings: dict) -> Path:
    p = settings.get("legacy_db_path", str(PROJECT_ROOT / "news_unified.db"))
    return Path(p)


def exec_schema(conn: sqlite3.Connection, schema_path: Path):
    sql = schema_path.read_text(encoding="utf-8")
    conn.executescript(sql)
    conn.commit()
    log.info("Schema executed from %s", schema_path)


def seed_sources(conn: sqlite3.Connection, seed_path: Path):
    if not seed_path.exists():
        log.warning("Seed file not found: %s", seed_path)
        return 0

    sources = json.loads(seed_path.read_text(encoding="utf-8"))
    inserted = 0
    for src in sources:
        try:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO sources(
                    name, category, subcategory, url, access_method,
                    is_official, credibility_tier, region, country,
                    owner, bias_notes, political_alignment, is_active,
                    update_frequency, notes
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    src.get("name", ""),
                    src.get("category", ""),
                    src.get("subcategory"),
                    src.get("url"),
                    src.get("access_method"),
                    int(src.get("is_official", 0)),
                    src.get("credibility_tier", "C"),
                    src.get("region"),
                    src.get("country", "RU"),
                    src.get("owner"),
                    src.get("bias_notes"),
                    src.get("political_alignment"),
                    int(src.get("is_active", 1)),
                    src.get("update_frequency"),
                    src.get("notes"),
                ),
            )
            inserted += cur.rowcount
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    log.info("Seeded %d sources from %s", inserted, seed_path)
    return inserted


def get_or_create_legacy_telegram_source(conn: sqlite3.Connection) -> int:
    url = "legacy://telegram-export"
    row = conn.execute(
        "SELECT id FROM sources WHERE category='telegram' AND url=?",
        (url,),
    ).fetchone()
    if row:
        conn.execute(
            """
            UPDATE sources
            SET name='Telegram legacy export',
                subcategory='legacy_export',
                access_method='legacy_sqlite_import',
                is_active=0,
                notes='Сообщения, импортированные из старой SQLite-БД exports/messages/photos'
            WHERE id=?
            """,
            (row[0],),
        )
        conn.commit()
        return row[0]

    cur = conn.execute(
        """
        INSERT INTO sources(
            name, category, subcategory, url, access_method,
            is_official, credibility_tier, is_active, update_frequency, notes
        ) VALUES(?,?,?,?,?,?,?,?,?,?)
        """,
        (
            "Telegram legacy export",
            "telegram",
            "legacy_export",
            url,
            "legacy_sqlite_import",
            0,
            "B",
            0,
            "manual",
            "Сообщения, импортированные из старой SQLite-БД exports/messages/photos",
        ),
    )
    conn.commit()
    return cur.lastrowid


def reassign_existing_legacy_items(conn: sqlite3.Connection, legacy_source_id: int) -> int:
    raw_ids = [
        row[0]
        for row in conn.execute(
            """
            SELECT id FROM raw_source_items
            WHERE external_id LIKE 'legacy:%' AND source_id != ?
            """,
            (legacy_source_id,),
        ).fetchall()
    ]
    if not raw_ids:
        return 0

    for start in range(0, len(raw_ids), 500):
        batch = raw_ids[start:start + 500]
        placeholders = ",".join("?" for _ in batch)
        conn.execute(
            f"UPDATE raw_source_items SET source_id=? WHERE id IN ({placeholders})",
            [legacy_source_id] + batch,
        )
        conn.execute(
            f"UPDATE content_items SET source_id=? WHERE raw_item_id IN ({placeholders})",
            [legacy_source_id] + batch,
        )
    conn.commit()
    log.info("Reassigned %d legacy raw items to dedicated Telegram legacy source", len(raw_ids))
    return len(raw_ids)


def _legacy_export_key(row: sqlite3.Row) -> str:
    base = f"{row['legacy_export_id']}|{row['export_dir'] or ''}|{row['export_name'] or ''}"
    return hashlib.sha256(base.encode("utf-8", errors="ignore")).hexdigest()[:12]


def _legacy_external_id(row: sqlite3.Row) -> str:
    msg_id = row["message_id"] or row["legacy_message_db_id"]
    return f"legacy:{_legacy_export_key(row)}:{msg_id}"


def _find_existing_legacy_item(
    conn: sqlite3.Connection,
    source_id: int,
    new_external_id: str,
    old_external_id: str,
    row: sqlite3.Row,
):
    existing = conn.execute(
        "SELECT id, external_id, raw_payload FROM raw_source_items WHERE source_id=? AND external_id=?",
        (source_id, new_external_id),
    ).fetchone()
    if existing:
        return existing[0]

    legacy_match = conn.execute(
        "SELECT id, external_id, raw_payload FROM raw_source_items WHERE source_id=? AND external_id=?",
        (source_id, old_external_id),
    ).fetchone()
    if not legacy_match:
        return None

    try:
        payload = json.loads(legacy_match[2] or "{}")
    except json.JSONDecodeError:
        payload = {}

    same_export = (
        (payload.get("export_dir") or "") == (row["export_dir"] or "")
        and (payload.get("source_file") or "") == (row["source_file"] or "")
    )
    if not same_export:
        return None

    conn.execute(
        "UPDATE raw_source_items SET external_id=? WHERE id=?",
        (new_external_id, legacy_match[0]),
    )
    conn.execute(
        "UPDATE content_items SET external_id=? WHERE raw_item_id=?",
        (new_external_id, legacy_match[0]),
    )
    return legacy_match[0]


def _content_id_for_raw(conn: sqlite3.Connection, raw_id: int):
    row = conn.execute("SELECT id FROM content_items WHERE raw_item_id=? LIMIT 1", (raw_id,)).fetchone()
    return row[0] if row else None


def _safe_filename(name: str) -> str:
    cleaned = "".join(ch if ch not in '<>:"/\\|?*' else "_" for ch in name)
    return cleaned.strip(" .") or "file"


def _legacy_photo_path(
    photo_row: sqlite3.Row,
    msg_row: sqlite3.Row,
    settings: dict,
):
    export_dir = Path(msg_row["export_dir"] or "")
    rel_path = photo_row["photo_rel_path"] or ""
    basename = photo_row["photo_basename"] or os.path.basename(rel_path) or "photo.jpg"
    candidates = [
        export_dir / rel_path,
        export_dir / "news_output" / "photos" / basename,
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate

    blob = photo_row["image_blob"]
    if blob is None:
        return None

    ext = (photo_row["image_ext"] or Path(basename).suffix.lstrip(".") or "jpg").lower()
    sha = photo_row["image_sha256"] or hashlib.sha256(blob).hexdigest()
    storage_root = Path(settings.get("processed_telegram", str(PROJECT_ROOT / "processed" / "telegram")))
    dest_dir = storage_root / "legacy_photos" / _legacy_export_key(msg_row)
    dest_dir.mkdir(parents=True, exist_ok=True)
    base_no_ext = Path(basename).stem or sha[:16]
    dest = dest_dir / _safe_filename(f"{sha[:16]}_{base_no_ext}.{ext}")
    if not dest.exists():
        dest.write_bytes(blob)
    return dest


def _import_legacy_photos(
    new_conn: sqlite3.Connection,
    old_conn: sqlite3.Connection,
    msg_row: sqlite3.Row,
    content_id: int,
    raw_id: int,
    settings: dict,
) -> int:
    imported = 0
    legacy_message_id = msg_row["legacy_message_db_id"]
    photo_rows = old_conn.execute(
        """
        SELECT id, photo_rel_path, photo_basename, exists_on_disk,
               image_blob, image_ext, image_size, image_sha256
        FROM photos
        WHERE message_db_id=?
        """,
        (legacy_message_id,),
    ).fetchall()

    handled_rel_paths = set()
    for photo in photo_rows:
        rel_path = (photo["photo_rel_path"] or "").strip()
        handled_rel_paths.add(rel_path)
        photo_path = _legacy_photo_path(photo, msg_row, settings)
        if not photo_path:
            continue

        ext = photo["image_ext"] or photo_path.suffix.lstrip(".") or "jpg"
        mime = "image/png" if ext.lower() == "png" else "image/jpeg"
        attach_file(
            new_conn,
            content_id,
            raw_id,
            photo_path,
            "photo",
            original_url=f"legacy-photo:{photo['id']}",
            mime_type=mime,
            hash_sha256=photo["image_sha256"] or None,
            file_size=photo["image_size"] or None,
            metadata={
                "legacy_photo_id": photo["id"],
                "legacy_rel_path": rel_path,
                "legacy_export_dir": msg_row["export_dir"],
            },
            legacy_paths=[
                rel_path,
                str(Path(msg_row["export_dir"] or "") / rel_path),
            ],
        )
        imported += 1

    for rel_path in (msg_row["kept_photos"] or "").split(";"):
        rel_path = rel_path.strip()
        if not rel_path or rel_path in handled_rel_paths:
            continue
        basename = os.path.basename(rel_path)
        export_dir = Path(msg_row["export_dir"] or "")
        for candidate in [
            export_dir / rel_path,
            export_dir / "news_output" / "photos" / basename,
        ]:
            if candidate.exists() and candidate.is_file():
                attach_file(
                    new_conn,
                    content_id,
                    raw_id,
                    candidate,
                    "photo",
                    original_url=f"legacy-photo-path:{rel_path}",
                    metadata={"legacy_rel_path": rel_path, "legacy_export_dir": msg_row["export_dir"]},
                    legacy_paths=[rel_path],
                )
                imported += 1
                break

    return imported


def import_legacy_data(new_conn: sqlite3.Connection, legacy_db_path: Path, settings: dict):
    if not legacy_db_path.exists():
        log.warning("Legacy DB not found: %s — skipping import", legacy_db_path)
        return 0

    old_conn = sqlite3.connect(str(legacy_db_path))
    old_conn.row_factory = sqlite3.Row

    existing_tables = {row[0] for row in old_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}

    if "messages" not in existing_tables:
        log.info("No messages table in legacy DB — nothing to import")
        old_conn.close()
        return 0

    telegram_src_id = get_or_create_legacy_telegram_source(new_conn)
    reassign_existing_legacy_items(new_conn, telegram_src_id)

    count = 0
    photo_count = 0
    rows = old_conn.execute(
        """
        SELECT m.id AS legacy_message_db_id, m.source_file, m.message_id, m.date, m.time,
               m.datetime_raw, m.headline, m.text, m.links, m.tags,
               m.decision, m.kept_photos,
               e.id AS legacy_export_id, e.export_dir, e.export_name
        FROM messages m
        JOIN exports e ON e.id = m.export_id
        WHERE COALESCE(m.decision, 'keep') = 'keep'
        ORDER BY m.date, m.time, m.id
        """
    )

    for row in rows:
        old_external_id = str(row["message_id"] or row["legacy_message_db_id"] or "")
        external_id = _legacy_external_id(row)
        date_str = row["date"] or ""
        time_str = row["time"] or ""
        dt_raw = row["datetime_raw"] or ""

        published_at = ""
        if date_str and time_str:
            published_at = f"{date_str}T{time_str}"
        elif date_str:
            published_at = date_str

        headline = (row["headline"] or "").strip()
        text = (row["text"] or "").strip()
        title = headline or text[:200]

        raw_payload = json.dumps(
            {
                "source_file": row["source_file"],
                "export_dir": row["export_dir"],
                "export_name": row["export_name"],
                "headline": headline,
                "text": text,
                "links": row["links"],
                "tags_legacy": row["tags"],
                "kept_photos": row["kept_photos"],
                "legacy_message_db_id": row["legacy_message_db_id"],
                "legacy_export_id": row["legacy_export_id"],
            },
            ensure_ascii=False,
        )

        hash_sha256 = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()

        raw_id = _find_existing_legacy_item(new_conn, telegram_src_id, external_id, old_external_id, row)
        if raw_id is None:
            cur = new_conn.execute(
                """
                INSERT INTO raw_source_items(
                    source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed
                ) VALUES(?,?,?,?,?,1)
                """,
                (telegram_src_id, external_id, raw_payload, published_at or datetime.now().isoformat(), hash_sha256),
            )
            raw_id = cur.lastrowid

        body = "\n".join(part for part in [headline, text] if part)

        content_id = _content_id_for_raw(new_conn, raw_id)
        if content_id is None:
            new_conn.execute(
                """
                INSERT INTO content_items(
                    source_id, raw_item_id, external_id, content_type,
                    title, body_text, published_at, collected_at, url, status
                ) VALUES(?,?,?,?,?,?,?,?,?,'unverified')
                """,
                (
                    telegram_src_id, raw_id, external_id, "post",
                    title, body, published_at, datetime.now().isoformat(),
                    f"legacy://{row['export_dir']}#{external_id}",
                ),
            )
            content_id = new_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        legacy_tags = (row["tags"] or "").split(",")
        for tag_str in legacy_tags:
            tag_name = tag_str.strip()
            if not tag_name:
                continue
            try:
                new_conn.execute(
                    "INSERT OR IGNORE INTO content_tags(content_item_id, tag_level, tag_name, tag_source) VALUES(?,0,?,'legacy')",
                    (content_id, tag_name),
                )
            except Exception:
                pass

        photo_count += _import_legacy_photos(new_conn, old_conn, row, content_id, raw_id, settings)

        count += 1

    new_conn.commit()
    old_conn.close()
    log.info("Imported/updated %d legacy messages and %d photos", count, photo_count)
    return count


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, ddl: str):
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
        conn.commit()
        log.info("Added %s.%s", table, column)


def backfill_attachment_blobs(conn: sqlite3.Connection):
    rows = conn.execute(
        """
        SELECT a.id
        FROM attachments a
        JOIN content_items c ON c.id = a.content_item_id
        WHERE a.blob_id IS NULL
          AND COALESCE(a.file_path, '') != ''
        """
    ).fetchall()

    updated = 0
    for row in rows:
        if materialize_attachment(conn, row[0]):
            updated += 1
    conn.commit()
    log.info("Backfilled %d attachment blob links", updated)
    return updated


def apply_migrations(conn: sqlite3.Connection):
    _add_column_if_missing(conn, "raw_blobs", "original_filename", "original_filename TEXT")
    _add_column_if_missing(conn, "raw_blobs", "storage_rel_path", "storage_rel_path TEXT")
    _add_column_if_missing(conn, "raw_blobs", "metadata_json", "metadata_json TEXT")
    _add_column_if_missing(conn, "raw_blobs", "missing_on_disk", "missing_on_disk INTEGER DEFAULT 0")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_blobs_raw_item ON raw_blobs(raw_item_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_blobs_hash ON raw_blobs(hash_sha256)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_blobs_type ON raw_blobs(blob_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_attachments_content ON attachments(content_item_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_attachments_blob ON attachments(blob_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_attachments_hash ON attachments(hash_sha256)")
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_attachments_content_blob_unique
        ON attachments(content_item_id, blob_id)
        WHERE blob_id IS NOT NULL
    """)
    conn.commit()

    legacy_source_id = get_or_create_legacy_telegram_source(conn)
    reassign_existing_legacy_items(conn, legacy_source_id)

    cur = conn.execute("PRAGMA table_info(evidence_links)")
    cols = {row[1]: row[2] for row in cur.fetchall()}
    if "evidence_item_id" in cols and cols["evidence_item_id"] == "INTEGER NOT NULL":
        count = conn.execute("SELECT COUNT(*) FROM evidence_links").fetchone()[0]
        if count == 0:
            conn.execute("DROP TABLE IF EXISTS evidence_links")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS evidence_links (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    claim_id        INTEGER NOT NULL,
                    evidence_item_id INTEGER,
                    evidence_type   TEXT NOT NULL,
                    strength        TEXT DEFAULT 'moderate',
                    notes           TEXT,
                    linked_by       TEXT,
                    linked_at       TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY (claim_id) REFERENCES claims(id) ON DELETE CASCADE,
                    FOREIGN KEY (evidence_item_id) REFERENCES content_items(id) ON DELETE CASCADE
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_evidence_claim ON evidence_links(claim_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_evidence_item ON evidence_links(evidence_item_id)")
            conn.commit()
            log.info("Migrated evidence_links: evidence_item_id now nullable")
        else:
            log.warning("evidence_links has %d rows — manual migration needed for NOT NULL -> nullable", count)

    content_cols = {row[1] for row in conn.execute("PRAGMA table_info(content_items)")}
    if "ner_processed" not in content_cols:
        conn.execute("ALTER TABLE content_items ADD COLUMN ner_processed INTEGER DEFAULT 0")
        conn.commit()
        log.info("Added ner_processed column to content_items")

    if "llm_processed" not in content_cols:
        conn.execute("ALTER TABLE content_items ADD COLUMN llm_processed INTEGER DEFAULT 0")
        conn.commit()
        log.info("Added llm_processed column to content_items")

    if "quotes_processed" not in content_cols:
        conn.execute("ALTER TABLE content_items ADD COLUMN quotes_processed INTEGER DEFAULT 0")
        conn.commit()
        log.info("Added quotes_processed column to content_items")

    if "granular_processed" not in content_cols:
        conn.execute("ALTER TABLE content_items ADD COLUMN granular_processed INTEGER DEFAULT 0")
        conn.commit()
        log.info("Added granular_processed column to content_items")


def migrate(legacy_import: bool = True):
    settings = load_settings()
    db_path = get_db_path(settings)
    legacy_path = get_legacy_db_path(settings)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    log.info("DB: %s", db_path)

    exec_schema(conn, SCHEMA_PATH)
    apply_migrations(conn)
    seed_sources(conn, SEED_PATH)

    if legacy_import:
        import_legacy_data(conn, legacy_path, settings)

    backfill_attachment_blobs(conn)

    tables = [row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )]
    log.info("Tables: %s", tables)

    for table in ["sources", "content_items", "entities", "claims", "cases", "quotes"]:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info("  %s: %d rows", table, cnt)

    conn.close()
    log.info("Migration complete")


if __name__ == "__main__":
    skip_legacy = "--no-legacy" in sys.argv
    migrate(legacy_import=not skip_legacy)
