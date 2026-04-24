import logging
import re
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)


def ensure_fts_triggers(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TRIGGER IF NOT EXISTS content_items_ai AFTER INSERT ON content_items BEGIN
            INSERT INTO content_search(rowid, title, body_text) VALUES (new.id, new.title, new.body_text);
        END;

        CREATE TRIGGER IF NOT EXISTS content_items_ad AFTER DELETE ON content_items BEGIN
            INSERT INTO content_search(content_search, rowid, title, body_text) VALUES ('delete', old.id, old.title, old.body_text);
        END;

        CREATE TRIGGER IF NOT EXISTS content_items_au AFTER UPDATE ON content_items BEGIN
            INSERT INTO content_search(content_search, rowid, title, body_text) VALUES ('delete', old.id, old.title, old.body_text);
            INSERT INTO content_search(rowid, title, body_text) VALUES (new.id, new.title, new.body_text);
        END;
    """)
    conn.commit()


def rebuild_fts(conn: sqlite3.Connection):
    conn.execute("INSERT INTO content_search(content_search) VALUES('rebuild')")
    conn.commit()
    log.info("FTS5 index rebuilt")


def search(
    query: str,
    conn: sqlite3.Connection = None,
    settings: dict = None,
    content_type: str = "",
    status: str = "",
    tag: str = "",
    source_id: int = 0,
    date_from: str = "",
    date_to: str = "",
    limit: int = 100,
    offset: int = 0,
) -> Dict:
    if conn is None:
        if settings is None:
            settings = load_settings()
        conn = get_db(settings)

    fts_query = _prepare_fts_query(query)
    if not fts_query:
        return {"results": [], "total": 0, "query": query}

    where_parts = []
    params = []

    if content_type:
        where_parts.append("c.content_type = ?")
        params.append(content_type)
    if status:
        where_parts.append("c.status = ?")
        params.append(status)
    if source_id:
        where_parts.append("c.source_id = ?")
        params.append(source_id)
    if date_from:
        where_parts.append("c.published_at >= ?")
        params.append(date_from)
    if date_to:
        where_parts.append("c.published_at <= ?")
        params.append(date_to)
    if tag:
        where_parts.append("""EXISTS (SELECT 1 FROM content_tags ct WHERE ct.content_item_id = c.id AND ct.tag_name = ?)""")
        params.append(tag)

    where_clause = ""
    if where_parts:
        where_clause = " AND " + " AND ".join(where_parts)

    total = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM content_search cs
        JOIN content_items c ON c.id = cs.rowid
        WHERE cs.content_search MATCH ?{where_clause}
        """,
        [fts_query] + params,
    ).fetchone()[0]

    rows = conn.execute(
        f"""
        SELECT
            c.id, c.title, c.body_text, c.published_at, c.content_type,
            c.status, c.url, s.name as source_name,
            snippet(content_search, 0, '>>', '<<', '...', 40) as title_snippet,
            snippet(content_search, 1, '>>', '<<', '...', 80) as body_snippet,
            rank
        FROM content_search cs
        JOIN content_items c ON c.id = cs.rowid
        LEFT JOIN sources s ON s.id = c.source_id
        WHERE cs.content_search MATCH ?{where_clause}
        ORDER BY rank
        LIMIT ? OFFSET ?
        """,
        [fts_query] + params + [limit, offset],
    ).fetchall()

    results = []
    for r in rows:
        results.append({
            "id": r[0],
            "title": r[1] or "",
            "body_text": r[2] or "",
            "published_at": r[3] or "",
            "content_type": r[4],
            "status": r[5],
            "url": r[6] or "",
            "source_name": r[7] or "",
            "title_snippet": r[8] or r[1] or "",
            "body_snippet": r[9] or (r[2][:200] if r[2] else ""),
            "rank": r[10],
        })

    return {
        "results": results,
        "total": total,
        "query": query,
        "fts_query": fts_query,
    }


def search_entities(
    name: str,
    entity_type: str = "",
    conn: sqlite3.Connection = None,
    settings: dict = None,
    limit: int = 50,
) -> List[Dict]:
    if conn is None:
        if settings is None:
            settings = load_settings()
        conn = get_db(settings)

    where_parts = ["(e.canonical_name LIKE ? OR EXISTS (SELECT 1 FROM entity_aliases ea WHERE ea.entity_id = e.id AND ea.alias LIKE ?))"]
    params = [f"%{name}%", f"%{name}%"]

    if entity_type:
        where_parts.append("e.entity_type = ?")
        params.append(entity_type)

    where = " AND ".join(where_parts)

    rows = conn.execute(
        f"""
        SELECT e.id, e.entity_type, e.canonical_name,
               (SELECT COUNT(*) FROM entity_mentions WHERE entity_id = e.id) as mention_count,
               GROUP_CONCAT(ea.alias, ', ')
        FROM entities e
        LEFT JOIN entity_aliases ea ON ea.entity_id = e.id
        WHERE {where}
        GROUP BY e.id
        ORDER BY mention_count DESC
        LIMIT ?
        """,
        params + [limit],
    ).fetchall()

    return [
        {
            "id": r[0],
            "entity_type": r[1],
            "canonical_name": r[2],
            "mention_count": r[3],
            "aliases": r[4] or "",
        }
        for r in rows
    ]


def search_quotes(
    text: str,
    entity_id: int = 0,
    rhetoric_class: str = "",
    flagged_only: bool = False,
    conn: sqlite3.Connection = None,
    settings: dict = None,
    limit: int = 50,
) -> List[Dict]:
    if conn is None:
        if settings is None:
            settings = load_settings()
        conn = get_db(settings)

    where_parts = ["q.quote_text LIKE ?"]
    params = [f"%{text}%"]

    if entity_id:
        where_parts.append("q.entity_id = ?")
        params.append(entity_id)
    if rhetoric_class:
        where_parts.append("q.rhetoric_class = ?")
        params.append(rhetoric_class)
    if flagged_only:
        where_parts.append("q.is_flagged = 1")

    where = " AND ".join(where_parts)

    rows = conn.execute(
        f"""
        SELECT q.id, q.quote_text, q.rhetoric_class, q.is_flagged,
               q.entity_id, e.canonical_name, q.content_item_id,
               q.timecode_start, q.timecode_end
        FROM quotes q
        LEFT JOIN entities e ON e.id = q.entity_id
        WHERE {where}
        ORDER BY q.is_flagged DESC, q.id DESC
        LIMIT ?
        """,
        params + [limit],
    ).fetchall()

    return [
        {
            "id": r[0],
            "quote_text": r[1],
            "rhetoric_class": r[2],
            "is_flagged": r[3],
            "entity_id": r[4],
            "entity_name": r[5] or "",
            "content_item_id": r[6],
            "timecode": f"{r[7]}-{r[8]}" if r[7] else "",
        }
        for r in rows
    ]


def _prepare_fts_query(query: str) -> str:
    if not query or not query.strip():
        return ""

    tokens = re.split(r'\s+', query.strip())
    fts_tokens = []
    for t in tokens:
        if not t:
            continue
        t = t.strip('"*')
        if not t:
            continue
        if len(t) >= 2:
            fts_tokens.append(f'"{t}"')
        else:
            fts_tokens.append(t)

    if not fts_tokens:
        return ""

    return " AND ".join(fts_tokens)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("query", help="Search query")
    parser.add_argument("--type", default="", help="Content type filter")
    parser.add_argument("--status", default="", help="Status filter")
    parser.add_argument("--tag", default="", help="Tag filter")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--rebuild", action="store_true", help="Rebuild FTS5 index")
    args = parser.parse_args()

    settings = load_settings()
    conn = get_db(settings)

    if args.rebuild:
        ensure_fts_triggers(conn)
        rebuild_fts(conn)

    result = search(args.query, conn=conn, content_type=args.type,
                    status=args.status, tag=args.tag, limit=args.limit)

    print(f"Found {result['total']} results for '{result['fts_query']}':\n")
    for r in result["results"]:
        print(f"  [{r['id']}] {r['title_snippet']}")
        print(f"      {r['body_snippet'][:120]}")
        print(f"      type={r['content_type']} status={r['status']} source={r['source_name']}")
        print()

    conn.close()


if __name__ == "__main__":
    main()
