import hashlib
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in os.sys.path:
    os.sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings, ensure_dirs
from db.file_store import attach_file

log = logging.getLogger(__name__)

VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".flv", ".wmv", ".ts"}
DOC_EXTENSIONS = {".pdf", ".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".gif", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".txt", ".rtf", ".odt", ".ods", ".zip", ".rar", ".7z"}


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def probe_video(path: Path) -> dict:
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", str(path)],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
    except Exception as e:
        log.warning("ffprobe failed for %s: %s", path, e)
    return {}


def extract_keyframes(video_path: Path, output_dir: Path, fps: float = 0.033) -> List[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    pattern = str(output_dir / f"{video_path.stem}_%04d.jpg")
    try:
        subprocess.run(
            ["ffmpeg", "-i", str(video_path), "-vf", f"fps={fps}", "-q:v", "2", pattern],
            capture_output=True, timeout=120,
        )
        return sorted(output_dir.glob(f"{video_path.stem}_*.jpg"))
    except Exception as e:
        log.warning("keyframe extraction failed for %s: %s", video_path, e)
        return []


def find_source_for_file(conn: sqlite3.Connection, filename: str, inbox_type: str) -> Optional[int]:
    name_lower = filename.lower()
    rows = conn.execute(
        "SELECT id, name, url, subcategory FROM sources WHERE is_active=1 AND category IN ('tiktok','youtube','user_upload') ORDER BY id"
    ).fetchall()

    for row in rows:
        src_name = (row["name"] or "").lower()
        src_url = (row["url"] or "").lower()
        if src_name in name_lower or any(part in name_lower for part in src_name.split()):
            return row["id"]

    fallback_cat = {"tiktok": "tiktok", "youtube": "youtube", "documents": "user_upload"}.get(inbox_type, "user_upload")
    row = conn.execute(
        "SELECT id FROM sources WHERE category=? AND is_active=1 LIMIT 1",
        (fallback_cat,),
    ).fetchone()
    return row[0] if row else None


def process_video_file(filepath: Path, inbox_type: str, conn: sqlite3.Connection, settings: dict) -> bool:
    source_id = find_source_for_file(conn, filepath.name, inbox_type)
    if not source_id:
        source_id = 1

    sha = file_hash(filepath)
    probe = probe_video(filepath)

    duration = 0
    format_info = probe.get("format", {})
    if format_info.get("duration"):
        try:
            duration = float(format_info["duration"])
        except (ValueError, TypeError):
            pass

    payload = {
        "filename": filepath.name,
        "file_size": filepath.stat().st_size,
        "hash_sha256": sha,
        "duration_seconds": duration,
        "probe": probe,
        "inbox_type": inbox_type,
    }
    raw_json = json.dumps(payload, ensure_ascii=False, default=str)

    existing = conn.execute(
        "SELECT id FROM raw_source_items WHERE hash_sha256=?", (sha,)
    ).fetchone()
    if existing:
        log.info("Already processed: %s (hash=%s)", filepath.name, sha[:12])
        return True

    cur = conn.execute(
        """INSERT INTO raw_source_items(source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed)
           VALUES(?,?,?,?,?,0)""",
        (source_id, sha[:32], raw_json, datetime.now().isoformat(), sha),
    )
    raw_id = cur.lastrowid

    content_type = "video"
    title = filepath.stem.replace("_", " ").replace("-", " ")
    if len(title) > 200:
        title = title[:200]

    cur2 = conn.execute(
        """INSERT INTO content_items(source_id, raw_item_id, external_id, content_type, title, body_text, published_at, collected_at, url, status)
           VALUES(?,?,?,?,?,?,?,?,?,'raw_signal')""",
        (source_id, raw_id, sha[:32], content_type, title, "", datetime.now().isoformat(), datetime.now().isoformat(), str(filepath)),
    )
    content_id = cur2.lastrowid

    processed_base = {
        "tiktok": settings.get("processed_tiktok", ""),
        "youtube": settings.get("processed_youtube", ""),
    }.get(inbox_type, settings.get("processed_documents", ""))

    ts = datetime.now().strftime("%Y-%m")
    dest_dir = Path(processed_base) / ts
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filepath.name

    try:
        shutil.copy2(str(filepath), str(dest_path))
    except Exception as e:
        log.error("Copy failed %s: %s", filepath, e)
        conn.rollback()
        return False
    conn.execute("UPDATE content_items SET url=? WHERE id=?", (str(dest_path), content_id))

    attach_file(
        conn,
        content_id,
        raw_id,
        dest_path,
        "video",
        mime_type=f"video/{filepath.suffix.lstrip('.')}",
        hash_sha256=sha,
        file_size=dest_path.stat().st_size,
        metadata={"source_inbox": inbox_type, "original_path": str(filepath)},
    )

    keyframe_dir = Path(settings.get("processed_keyframes", "")) / ts / filepath.stem
    keyframes = extract_keyframes(dest_path, keyframe_dir)
    for kf in keyframes:
        kf_sha = file_hash(kf)
        attach_file(
            conn,
            content_id,
            raw_id,
            kf,
            "keyframe",
            mime_type="image/jpeg",
            hash_sha256=kf_sha,
            file_size=kf.stat().st_size,
            is_original=0,
            metadata={"derived_from": str(dest_path)},
        )

    conn.execute("UPDATE raw_source_items SET is_processed=1 WHERE id=?", (raw_id,))
    conn.commit()

    log.info("Processed video: %s -> %s (%d keyframes)", filepath.name, dest_path, len(keyframes))
    return True


def process_document_file(filepath: Path, conn: sqlite3.Connection, settings: dict) -> bool:
    source_id = find_source_for_file(conn, filepath.name, "documents")
    if not source_id:
        source_id = 1

    sha = file_hash(filepath)

    existing = conn.execute(
        "SELECT id FROM raw_source_items WHERE hash_sha256=?", (sha,)
    ).fetchone()
    if existing:
        log.info("Already processed: %s", filepath.name)
        return True

    ext = filepath.suffix.lower()
    doc_type_map = {
        ".pdf": "document", ".jpg": "document", ".jpeg": "document",
        ".png": "document", ".bmp": "document", ".tiff": "document",
        ".doc": "document", ".docx": "document", ".xls": "document",
        ".xlsx": "document", ".csv": "document", ".txt": "document",
    }
    content_type = doc_type_map.get(ext, "document")
    attachment_type = "scan" if ext in {".jpg", ".jpeg", ".png", ".bmp", ".tiff"} else "pdf" if ext == ".pdf" else "document"

    payload = {
        "filename": filepath.name,
        "file_size": filepath.stat().st_size,
        "hash_sha256": sha,
        "extension": ext,
    }
    raw_json = json.dumps(payload, ensure_ascii=False)

    cur = conn.execute(
        """INSERT INTO raw_source_items(source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed)
           VALUES(?,?,?,?,?,0)""",
        (source_id, sha[:32], raw_json, datetime.now().isoformat(), sha),
    )
    raw_id = cur.lastrowid

    title = filepath.stem.replace("_", " ").replace("-", " ")
    cur2 = conn.execute(
        """INSERT INTO content_items(source_id, raw_item_id, external_id, content_type, title, body_text, published_at, collected_at, url, status)
           VALUES(?,?,?,?,?,?,?,?,?,'raw_signal')""",
        (source_id, raw_id, sha[:32], content_type, title, "", datetime.now().isoformat(), datetime.now().isoformat(), str(filepath)),
    )
    content_id = cur2.lastrowid

    processed_base = Path(settings.get("processed_documents", ""))
    ts = datetime.now().strftime("%Y-%m")
    dest_dir = processed_base / ts
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / filepath.name

    try:
        shutil.copy2(str(filepath), str(dest_path))
    except Exception as e:
        log.error("Copy failed %s: %s", filepath, e)
        conn.rollback()
        return False
    conn.execute("UPDATE content_items SET url=? WHERE id=?", (str(dest_path), content_id))

    mime_map = {
        ".pdf": "application/pdf", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }

    attach_file(
        conn,
        content_id,
        raw_id,
        dest_path,
        attachment_type,
        mime_type=mime_map.get(ext, "application/octet-stream"),
        hash_sha256=sha,
        file_size=dest_path.stat().st_size,
        metadata={"source_inbox": "documents", "original_path": str(filepath)},
    )

    conn.execute("UPDATE raw_source_items SET is_processed=1 WHERE id=?", (raw_id,))
    conn.commit()

    log.info("Processed document: %s -> %s", filepath.name, dest_path)
    return True


def scan_inbox(inbox_dir: Path, inbox_type: str, conn: sqlite3.Connection, settings: dict):
    if not inbox_dir.exists():
        return

    extensions = VIDEO_EXTENSIONS if inbox_type in ("tiktok", "youtube") else DOC_EXTENSIONS

    for item in sorted(inbox_dir.iterdir()):
        if item.is_dir():
            continue
        if item.suffix.lower() not in extensions:
            if item.suffix.lower() not in VIDEO_EXTENSIONS and item.suffix.lower() not in DOC_EXTENSIONS:
                continue

        log.info("Found: %s (%s)", item.name, item.suffix)

        if inbox_type in ("tiktok", "youtube") or item.suffix.lower() in VIDEO_EXTENSIONS:
            success = process_video_file(item, inbox_type, conn, settings)
        else:
            success = process_document_file(item, conn, settings)

        if success:
            try:
                item.unlink()
                log.info("Removed from inbox: %s", item.name)
            except Exception as e:
                log.warning("Cannot remove %s: %s", item, e)


def scan_all_inboxes(settings: dict = None):
    if settings is None:
        settings = load_settings()

    ensure_dirs(settings)
    conn = get_db(settings)

    try:
        scan_inbox(Path(settings["inbox_tiktok"]), "tiktok", conn, settings)
        scan_inbox(Path(settings["inbox_youtube"]), "youtube", conn, settings)
        scan_inbox(Path(settings["inbox_documents"]), "documents", conn, settings)
    finally:
        conn.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scan_all_inboxes()


if __name__ == "__main__":
    main()
