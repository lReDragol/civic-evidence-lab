import hashlib
import json
import mimetypes
import sqlite3
from pathlib import Path
from typing import Iterable, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def guess_mime(path: Path, fallback: str = "application/octet-stream") -> str:
    mime, _ = mimetypes.guess_type(str(path))
    return mime or fallback


def normalize_path(path: Path | str) -> str:
    return str(Path(path).resolve(strict=False))


def storage_rel_path(path: Path | str) -> str:
    resolved = Path(path).resolve(strict=False)
    try:
        return str(resolved.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(resolved)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _row_value(row, key: str, default=None):
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default


def ensure_raw_blob(
    conn: sqlite3.Connection,
    raw_item_id: int,
    file_path: Path | str,
    blob_type: str,
    *,
    original_url: Optional[str] = None,
    mime_type: Optional[str] = None,
    hash_sha256: Optional[str] = None,
    file_size: Optional[int] = None,
    metadata: Optional[dict] = None,
) -> Optional[int]:
    if not raw_item_id:
        return None

    path = Path(file_path)
    path_str = normalize_path(path)
    exists = path.exists() and path.is_file()

    if exists:
        hash_sha256 = hash_sha256 or file_hash(path)
        file_size = file_size if file_size is not None else path.stat().st_size
        mime_type = mime_type or guess_mime(path)

    if not hash_sha256:
        return None

    original_ref = original_url or f"file://{path_str}"
    existing = conn.execute(
        """
        SELECT id FROM raw_blobs
        WHERE raw_item_id = ?
          AND (hash_sha256 = ? OR original_url = ? OR file_path = ?)
        LIMIT 1
        """,
        (raw_item_id, hash_sha256, original_ref, path_str),
    ).fetchone()

    cols = _table_columns(conn, "raw_blobs")
    values = {
        "raw_item_id": raw_item_id,
        "blob_type": blob_type,
        "file_path": path_str,
        "original_filename": path.name,
        "storage_rel_path": storage_rel_path(path),
        "original_url": original_ref,
        "mime_type": mime_type or guess_mime(path),
        "file_size": file_size or 0,
        "hash_sha256": hash_sha256,
        "metadata_json": json.dumps(metadata or {}, ensure_ascii=False) if metadata else None,
        "missing_on_disk": 0 if exists else 1,
    }
    selected = [name for name in values if name in cols]

    if existing:
        blob_id = existing[0]
        update_cols = [name for name in selected if name not in {"raw_item_id", "hash_sha256"}]
        if update_cols:
            conn.execute(
                f"UPDATE raw_blobs SET {', '.join(f'{name}=?' for name in update_cols)} WHERE id=?",
                [values[name] for name in update_cols] + [blob_id],
            )
        return blob_id

    placeholders = ",".join("?" for _ in selected)
    cur = conn.execute(
        f"INSERT INTO raw_blobs({', '.join(selected)}) VALUES({placeholders})",
        [values[name] for name in selected],
    )
    return cur.lastrowid


def attach_file(
    conn: sqlite3.Connection,
    content_item_id: int,
    raw_item_id: int,
    file_path: Path | str,
    attachment_type: str,
    *,
    original_url: Optional[str] = None,
    mime_type: Optional[str] = None,
    hash_sha256: Optional[str] = None,
    file_size: Optional[int] = None,
    ocr_text: Optional[str] = None,
    is_original: int = 1,
    metadata: Optional[dict] = None,
    legacy_paths: Optional[Iterable[str]] = None,
) -> int:
    path = Path(file_path)
    path_str = normalize_path(path)

    if path.exists() and path.is_file():
        hash_sha256 = hash_sha256 or file_hash(path)
        file_size = file_size if file_size is not None else path.stat().st_size
        mime_type = mime_type or guess_mime(path)

    blob_id = ensure_raw_blob(
        conn,
        raw_item_id,
        path,
        attachment_type,
        original_url=original_url,
        mime_type=mime_type,
        hash_sha256=hash_sha256,
        file_size=file_size,
        metadata=metadata,
    )

    existing = None
    if blob_id is not None:
        existing = conn.execute(
            "SELECT id FROM attachments WHERE content_item_id=? AND blob_id=? LIMIT 1",
            (content_item_id, blob_id),
        ).fetchone()
    if existing is None and hash_sha256:
        existing = conn.execute(
            "SELECT id FROM attachments WHERE content_item_id=? AND hash_sha256=? LIMIT 1",
            (content_item_id, hash_sha256),
        ).fetchone()
    if existing is None:
        paths = [path_str]
        paths.extend(str(p) for p in (legacy_paths or []) if p)
        placeholders = ",".join("?" for _ in paths)
        existing = conn.execute(
            f"SELECT id FROM attachments WHERE content_item_id=? AND file_path IN ({placeholders}) LIMIT 1",
            [content_item_id] + paths,
        ).fetchone()
    if existing is None and path.name:
        existing = conn.execute(
            """
            SELECT id FROM attachments
            WHERE content_item_id=?
              AND (hash_sha256='' OR hash_sha256 IS NULL)
              AND lower(file_path) LIKE ?
            LIMIT 1
            """,
            (content_item_id, f"%{path.name.lower()}"),
        ).fetchone()

    values = (
        blob_id,
        path_str,
        attachment_type,
        hash_sha256 or "",
        file_size or 0,
        mime_type or guess_mime(path),
        ocr_text,
        int(is_original),
    )

    if existing:
        att_id = existing[0]
        conn.execute(
            """
            UPDATE attachments
            SET blob_id=?, file_path=?, attachment_type=?, hash_sha256=?,
                file_size=?, mime_type=?, ocr_text=COALESCE(?, ocr_text),
                is_original=?
            WHERE id=?
            """,
            values + (att_id,),
        )
        return att_id

    cur = conn.execute(
        """
        INSERT INTO attachments(
            content_item_id, blob_id, file_path, attachment_type,
            hash_sha256, file_size, mime_type, ocr_text, is_original
        ) VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (content_item_id,) + values,
    )
    return cur.lastrowid


def materialize_attachment(conn: sqlite3.Connection, attachment_id: int) -> bool:
    row = conn.execute(
        """
        SELECT a.*, c.raw_item_id
        FROM attachments a
        JOIN content_items c ON c.id = a.content_item_id
        WHERE a.id=?
        """,
        (attachment_id,),
    ).fetchone()
    if not row:
        return False

    file_path = Path(_row_value(row, "file_path", ""))
    if not file_path.exists() or not file_path.is_file():
        return False

    attach_file(
        conn,
        _row_value(row, "content_item_id"),
        _row_value(row, "raw_item_id"),
        file_path,
        _row_value(row, "attachment_type") or "file",
        mime_type=_row_value(row, "mime_type"),
        ocr_text=_row_value(row, "ocr_text"),
        is_original=_row_value(row, "is_original", 1),
        legacy_paths=[_row_value(row, "file_path", "")],
    )
    return True
