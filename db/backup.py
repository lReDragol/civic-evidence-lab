import logging
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def backup_database(
    db_path: str = None,
    backup_dir: str = None,
    max_backups: int = 7,
    compress: bool = True,
) -> str:
    if db_path is None:
        from config.db_utils import load_settings
        settings = load_settings()
        db_path = settings.get("db_path", "db/news_unified.db")

    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    if backup_dir is None:
        backup_dir = db_path.parent / "backups"
    backup_dir = Path(backup_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"news_unified_{timestamp}.db"
    backup_path = backup_dir / backup_name

    source_conn = sqlite3.connect(str(db_path))
    source_conn.execute("PRAGMA wal_checkpoint(FULL)")

    backup_conn = sqlite3.connect(str(backup_path))
    source_conn.backup(backup_conn)
    backup_conn.close()
    source_conn.close()

    log.info("Backup created: %s", backup_path)

    if compress:
        import gzip
        gz_path = Path(str(backup_path) + ".gz")
        with open(backup_path, "rb") as f_in:
            with gzip.open(gz_path, "wb", compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out)
        backup_path.unlink()
        log.info("Compressed: %s (%.1f MB)", gz_path, gz_path.stat().st_size / 1e6)
        backup_path = gz_path

    _rotate_backups(backup_dir, max_backups)

    return str(backup_path)


def _rotate_backups(backup_dir: Path, max_backups: int):
    backups = sorted(backup_dir.glob("news_unified_*.db.gz"), reverse=True)
    if len(backups) <= max_backups:
        return
    for old in backups[max_backups:]:
        old.unlink()
        log.info("Removed old backup: %s", old)


def restore_database(
    backup_path: str,
    db_path: str = None,
) -> str:
    backup_path = Path(backup_path)
    if not backup_path.exists():
        raise FileNotFoundError(f"Backup not found: {backup_path}")

    if db_path is None:
        from config.db_utils import load_settings
        settings = load_settings()
        db_path = settings.get("db_path", "db/news_unified.db")
    db_path = Path(db_path)

    if backup_path.suffix == ".gz":
        import gzip
        temp_db = backup_path.with_suffix("")
        with gzip.open(backup_path, "rb") as f_in:
            with open(temp_db, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        shutil.copy2(temp_db, db_path)
        temp_db.unlink()
    else:
        shutil.copy2(backup_path, db_path)

    log.info("Restored database from %s to %s", backup_path, db_path)
    return str(db_path)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    path = backup_database()
    print(f"Backup: {path}")


if __name__ == "__main__":
    main()
