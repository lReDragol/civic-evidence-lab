import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Sequence

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from collectors.official_scraper import (
    duma_bills_collect,
    gis_gkh_collect,
    government_collect,
    kremlin_transcripts_collect,
    minjust_inoagents_collect,
    pravo_collect,
    rosreestr_collect,
    zakupki_collect,
)
from tools.check_official_sources import check_sources

SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"
DEFAULT_SOURCE_DB = PROJECT_ROOT / "db" / "news_unified.db"
DEFAULT_TARGET_DB = PROJECT_ROOT / "db" / "news_evidence.db"
DEFAULT_REPORT = PROJECT_ROOT / "reports" / "evidence_backfill_latest.json"

EVIDENCE_SOURCE_CATEGORIES = ("official_registry", "official_site", "media")
EVIDENCE_CONTENT_TYPES = (
    "registry_record",
    "court_record",
    "enforcement",
    "procurement",
    "bill",
    "transcript",
    "article",
    "official_page",
    "deputy_profile",
)
COPY_TABLES = (
    "sources",
    "raw_source_items",
    "raw_blobs",
    "content_items",
    "attachments",
    "content_tags",
)


def open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def remove_db(path: Path) -> None:
    for candidate in [path, path.with_suffix(path.suffix + "-wal"), path.with_suffix(path.suffix + "-shm")]:
        if candidate.exists():
            candidate.unlink()


def exec_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.commit()


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    return [row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def batched(values: Sequence[int], size: int = 500) -> Iterable[Sequence[int]]:
    for idx in range(0, len(values), size):
        yield values[idx:idx + size]


def copy_rows_by_ids(
    src_conn: sqlite3.Connection,
    dst_conn: sqlite3.Connection,
    table: str,
    ids: Sequence[int],
) -> int:
    if not ids:
        return 0
    columns = table_columns(src_conn, table)
    quoted_cols = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    copied = 0
    for batch in batched(list(ids)):
        batch_placeholders = ", ".join("?" for _ in batch)
        rows = src_conn.execute(
            f"SELECT {quoted_cols} FROM {table} WHERE id IN ({batch_placeholders}) ORDER BY id",
            list(batch),
        ).fetchall()
        if not rows:
            continue
        dst_conn.executemany(
            f"INSERT OR IGNORE INTO {table}({quoted_cols}) VALUES({placeholders})",
            [tuple(row[col] for col in columns) for row in rows],
        )
        copied += len(rows)
    dst_conn.commit()
    return copied


def rebuild_fts(conn: sqlite3.Connection) -> None:
    conn.execute("INSERT INTO content_search(content_search) VALUES('rebuild')")
    conn.commit()


def normalize_sources(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE sources
        SET url='minjust.gov.ru/ru/pages/reestr-inostryannykh-agentov/',
            access_method='json_api'
        WHERE name='Реестр иноагентов'
        """
    )
    conn.commit()


def build_evidence_db(source_db: Path, target_db: Path, reset: bool) -> Dict[str, object]:
    if not source_db.exists():
        raise FileNotFoundError(f"Source DB not found: {source_db}")

    target_db.parent.mkdir(parents=True, exist_ok=True)
    if reset:
        remove_db(target_db)

    src_conn = open_db(source_db)
    dst_conn = open_db(target_db)
    try:
        exec_schema(dst_conn)

        source_ids = [
            row["id"]
            for row in src_conn.execute(
                """
                SELECT id
                FROM sources
                WHERE category IN ({})
                ORDER BY id
                """.format(",".join("?" for _ in EVIDENCE_SOURCE_CATEGORIES)),
                EVIDENCE_SOURCE_CATEGORIES,
            ).fetchall()
        ]

        content_ids = [
            row["id"]
            for row in src_conn.execute(
                """
                SELECT c.id
                FROM content_items c
                JOIN sources s ON s.id = c.source_id
                WHERE s.category IN ({})
                   OR c.content_type IN ({})
                ORDER BY c.id
                """.format(
                    ",".join("?" for _ in EVIDENCE_SOURCE_CATEGORIES),
                    ",".join("?" for _ in EVIDENCE_CONTENT_TYPES),
                ),
                list(EVIDENCE_SOURCE_CATEGORIES) + list(EVIDENCE_CONTENT_TYPES),
            ).fetchall()
        ]

        raw_ids = [
            row["id"]
            for row in src_conn.execute(
                """
                SELECT DISTINCT r.id
                FROM raw_source_items r
                JOIN content_items c ON c.raw_item_id = r.id
                WHERE c.id IN ({})
                ORDER BY r.id
                """.format(",".join("?" for _ in content_ids)) if content_ids else "SELECT id FROM raw_source_items WHERE 0",
                content_ids,
            ).fetchall()
        ] if content_ids else []

        blob_ids = set()
        if raw_ids:
            for row in src_conn.execute(
                """
                SELECT id
                FROM raw_blobs
                WHERE raw_item_id IN ({})
                """.format(",".join("?" for _ in raw_ids)),
                raw_ids,
            ).fetchall():
                blob_ids.add(row["id"])
        if content_ids:
            for row in src_conn.execute(
                """
                SELECT DISTINCT blob_id
                FROM attachments
                WHERE content_item_id IN ({})
                  AND blob_id IS NOT NULL
                """.format(",".join("?" for _ in content_ids)),
                content_ids,
            ).fetchall():
                blob_ids.add(row["blob_id"])

        attachment_ids = [
            row["id"]
            for row in src_conn.execute(
                """
                SELECT id
                FROM attachments
                WHERE content_item_id IN ({})
                ORDER BY id
                """.format(",".join("?" for _ in content_ids)) if content_ids else "SELECT id FROM attachments WHERE 0",
                content_ids,
            ).fetchall()
        ] if content_ids else []

        tag_ids = [
            row["id"]
            for row in src_conn.execute(
                """
                SELECT id
                FROM content_tags
                WHERE content_item_id IN ({})
                ORDER BY id
                """.format(",".join("?" for _ in content_ids)) if content_ids else "SELECT id FROM content_tags WHERE 0",
                content_ids,
            ).fetchall()
        ] if content_ids else []

        copied = {
            "sources": copy_rows_by_ids(src_conn, dst_conn, "sources", source_ids),
            "raw_source_items": copy_rows_by_ids(src_conn, dst_conn, "raw_source_items", raw_ids),
            "raw_blobs": copy_rows_by_ids(src_conn, dst_conn, "raw_blobs", sorted(blob_ids)),
            "content_items": copy_rows_by_ids(src_conn, dst_conn, "content_items", content_ids),
            "attachments": copy_rows_by_ids(src_conn, dst_conn, "attachments", attachment_ids),
            "content_tags": copy_rows_by_ids(src_conn, dst_conn, "content_tags", tag_ids),
        }
        normalize_sources(dst_conn)
        rebuild_fts(dst_conn)

        return {
            "source_db": str(source_db),
            "target_db": str(target_db),
            "copied": copied,
        }
    finally:
        dst_conn.close()
        src_conn.close()


def _run_collector(name: str, fn: Callable[[], object], *, zero_is_error: bool = False) -> Dict[str, object]:
    started = time.perf_counter()
    try:
        collected = fn()
        collected_int = int(collected or 0)
        ok = not (zero_is_error and collected_int == 0)
        return {
            "ok": ok,
            "collected": collected_int,
            "duration_sec": round(time.perf_counter() - started, 3),
            "error": "" if ok else "collector returned 0 records; check source_health/network diagnostics",
        }
    except Exception as exc:
        return {
            "ok": False,
            "collected": 0,
            "duration_sec": round(time.perf_counter() - started, 3),
            "error": f"{type(exc).__name__}: {exc}",
        }


def run_backfill(target_db: Path, pages: int) -> Dict[str, object]:
    settings = {"db_path": str(target_db)}
    return {
        "minjust": _run_collector("minjust", lambda: minjust_inoagents_collect(settings)),
        "zakupki": _run_collector("zakupki", lambda: zakupki_collect(settings, queries=[None], pages=pages)),
        "duma": _run_collector(
            "duma",
            lambda: duma_bills_collect(
                settings,
                pages=pages,
                queries=[
                    "жкх",
                    "собствен",
                    "жиль",
                    "реестр",
                    "ндс",
                    "налог",
                    "бюджет",
                    "суд",
                    "иностран",
                    "штраф",
                    "закуп",
                    "росреестр",
                    "гис жкх",
                ],
            ),
        ),
        "gis_gkh": _run_collector("gis_gkh", lambda: gis_gkh_collect(settings)),
        "government": _run_collector("government", lambda: government_collect(settings, pages=pages), zero_is_error=True),
        "pravo": _run_collector("pravo", lambda: pravo_collect(settings, pages=pages)),
        "rosreestr": _run_collector("rosreestr", lambda: rosreestr_collect(settings, pages=pages), zero_is_error=True),
        "kremlin": _run_collector(
            "kremlin",
            lambda: kremlin_transcripts_collect(settings, pages=pages),
            zero_is_error=True,
        ),
    }


def collect_stats(db_path: Path) -> Dict[str, object]:
    conn = open_db(db_path)
    try:
        return {
            "sources_by_category": [
                dict(row)
                for row in conn.execute(
                    "SELECT category, COUNT(*) AS n FROM sources GROUP BY category ORDER BY n DESC"
                ).fetchall()
            ],
            "content_by_category": [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT s.category, COUNT(*) AS n
                    FROM content_items c
                    JOIN sources s ON s.id = c.source_id
                    GROUP BY s.category
                    ORDER BY n DESC
                    """
                ).fetchall()
            ],
            "content_by_type": [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT content_type, COUNT(*) AS n
                    FROM content_items
                    GROUP BY content_type
                    ORDER BY n DESC
                    """
                ).fetchall()
            ],
            "integrity_check": conn.execute("PRAGMA integrity_check").fetchone()[0],
            "foreign_key_issues": [tuple(row) for row in conn.execute("PRAGMA foreign_key_check").fetchall()],
        }
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build and backfill a standalone official/media evidence DB")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB))
    parser.add_argument("--target-db", default=str(DEFAULT_TARGET_DB))
    parser.add_argument("--pages", type=int, default=2, help="Page count for HTML-based collectors")
    parser.add_argument("--no-reset", action="store_true", help="Keep existing target DB and append into it")
    parser.add_argument("--skip-backfill", action="store_true", help="Only copy current official/media corpus from source DB")
    parser.add_argument("--skip-health", action="store_true", help="Do not probe official source availability")
    parser.add_argument("--health-timeout", type=int, default=8)
    parser.add_argument("--report", default=str(DEFAULT_REPORT))
    args = parser.parse_args()

    source_db = Path(args.source_db)
    target_db = Path(args.target_db)

    result = {
        "build": build_evidence_db(source_db, target_db, reset=not args.no_reset),
        "backfill": None,
    }
    if not args.skip_backfill:
        result["backfill"] = run_backfill(target_db, pages=args.pages)

    if not args.skip_health:
        result["source_health"] = check_sources(timeout=args.health_timeout)

    result["stats"] = collect_stats(target_db)

    output = json.dumps(result, ensure_ascii=False, indent=2)
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
