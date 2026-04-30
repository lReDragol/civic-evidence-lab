import argparse
import json
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.db_utils import load_settings
from runtime.state import get_runtime_metadata, set_runtime_metadata


BAD_FILENAME_CHARS = '<>:"/\\|?*\n\r\t'
EXPORTED_VAULT_DIRS = {
    "Affiliations",
    "Assets",
    "Attachments",
    "Bills",
    "Cases",
    "Claims",
    "Content",
    "Contracts",
    "Disclosures",
    "Entities",
    "Events",
    "Facts",
    "Files",
    "Profiles",
    "Restrictions",
    "ReviewPacks",
    "Risks",
    "Sources",
    "Tags",
    "VoteSessions",
    "WeakLinks",
}
EXPORTED_VAULT_FILES = {"index.md"}


def slugify(value: str, fallback: str = "note", max_len: int = 90) -> str:
    value = (value or "").strip()
    cleaned = []
    last_dash = False
    for ch in value:
        if ch in BAD_FILENAME_CHARS or ch.isspace():
            if not last_dash:
                cleaned.append("-")
                last_dash = True
            continue
        cleaned.append(ch)
        last_dash = False
    slug = "".join(cleaned).strip(" .-")
    if not slug:
        slug = fallback
    return slug[:max_len].strip(" .-") or fallback


def md_escape(value) -> str:
    return str(value or "").replace("|", "\\|").replace("\n", " ").strip()


def yaml_scalar(value) -> str:
    return json.dumps(value if value is not None else "", ensure_ascii=False)


def note_link(rel_path: str, title: Optional[str] = None) -> str:
    target = rel_path.replace("\\", "/").removesuffix(".md")
    if title:
        return f"[[{target}|{title}]]"
    return f"[[{target}]]"


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def write_note(vault: Path, rel_path: str, body: str) -> str:
    out_path = vault / rel_path
    ensure_dir(out_path.parent)
    out_path.write_text(body.rstrip() + "\n", encoding="utf-8")
    return rel_path.replace("\\", "/")


def clean_exported_vault(vault: Path):
    """Remove generated export content without touching Obsidian/user settings."""
    ensure_dir(vault)
    for dirname in EXPORTED_VAULT_DIRS:
        path = vault / dirname
        if path.exists():
            shutil.rmtree(path)
    for filename in EXPORTED_VAULT_FILES:
        path = vault / filename
        if path.exists() and path.is_file():
            path.unlink()


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def rows(conn: sqlite3.Connection, sql: str, params: Iterable = ()):
    return conn.execute(sql, tuple(params)).fetchall()


def one(conn: sqlite3.Connection, sql: str, params: Iterable = ()):
    return conn.execute(sql, tuple(params)).fetchone()


def copy_attachment(vault: Path, row: sqlite3.Row, copy_media: bool = True) -> tuple[Optional[str], str]:
    file_path = Path(row["file_path"] or "")
    if not file_path.exists() or not file_path.is_file():
        return None, f"missing: {row['file_path'] or ''}"

    ext = file_path.suffix or ".bin"
    digest = row["hash_sha256"] or f"attachment-{row['id']}"
    safe_name = slugify(file_path.stem, fallback=f"attachment-{row['id']}", max_len=55)
    out_name = f"{digest[:16]}-{safe_name}{ext}"
    dest = vault / "Attachments" / out_name
    if copy_media:
        ensure_dir(dest.parent)
        if not dest.exists() or dest.stat().st_size != file_path.stat().st_size:
            shutil.copy2(file_path, dest)

    wikilink = f"![[Attachments/{out_name}]]" if (row["mime_type"] or "").startswith("image/") else f"[[Attachments/{out_name}]]"
    return wikilink, out_name


def frontmatter(data: dict) -> str:
    lines = ["---"]
    for key, value in data.items():
        if isinstance(value, list):
            lines.append(f"{key}:")
            for item in value:
                lines.append(f"  - {yaml_scalar(item)}")
        else:
            lines.append(f"{key}: {yaml_scalar(value)}")
    lines.append("---")
    return "\n".join(lines)


def export_index(vault: Path, conn: sqlite3.Connection, generated_at: str):
    pipeline_version = get_runtime_metadata(
        conn,
        "analysis_built_from_pipeline_version",
        get_runtime_metadata(conn, "current_pipeline_version"),
    )
    table_names = [
        "sources", "raw_source_items", "raw_blobs", "content_items",
        "attachments", "entities", "claims", "cases", "quotes",
        "entity_relations", "risk_patterns",
    ]
    lines = [
        frontmatter(
            {
                "type": "database_index",
                "generated_at": generated_at,
                "built_from_pipeline_version": pipeline_version,
            }
        ),
        "# Архив новостей",
        "",
        f"Экспорт создан: `{generated_at}`",
        f"Pipeline version: `{pipeline_version or ''}`",
        "",
        "## Счётчики",
        "",
        "| Таблица | Записей |",
        "|---|---:|",
    ]
    for table in table_names:
        try:
            count = one(conn, f"SELECT COUNT(*) FROM {table}")[0]
        except sqlite3.Error:
            count = 0
        lines.append(f"| `{table}` | {count} |")
    lines.extend([
        "",
        "## Разделы",
        "",
        "- [[Sources/index|Источники]]",
        "- [[Content/index|Контент]]",
        "- [[Claims/index|Заявления]]",
        "- [[Cases/index|Кейсы]]",
        "- [[Entities/index|Сущности]]",
        "- [[Files/index|Файлы]]",
    ])
    write_note(vault, "index.md", "\n".join(lines))


def export_sources(vault: Path, conn: sqlite3.Connection) -> dict[int, str]:
    source_map: dict[int, str] = {}
    source_rows = rows(conn, "SELECT * FROM sources ORDER BY category, name")
    index_lines = ["# Источники", "", "| ID | Категория | Название | Tier |", "|---:|---|---|---|"]
    for src in source_rows:
        rel = f"Sources/{src['category'] or 'source'}/{src['id']}-{slugify(src['name'], 'source')}.md"
        source_map[src["id"]] = rel
        index_lines.append(f"| {src['id']} | {md_escape(src['category'])} | {note_link(rel, md_escape(src['name']))} | {md_escape(src['credibility_tier'])} |")
        body = [
            frontmatter({
                "type": "source",
                "source_id": src["id"],
                "category": src["category"],
                "credibility_tier": src["credibility_tier"],
                "is_official": bool(src["is_official"]),
            }),
            f"# {src['name'] or 'Источник'}",
            "",
            f"- ID: `{src['id']}`",
            f"- Категория: `{src['category'] or ''}`",
            f"- URL: {src['url'] or ''}",
            f"- Метод доступа: `{src['access_method'] or ''}`",
            f"- Официальный: `{src['is_official']}`",
            f"- Tier: `{src['credibility_tier'] or ''}`",
            f"- Активен: `{src['is_active']}`",
            "",
            "## Заметки",
            "",
            src["notes"] or "",
        ]
        write_note(vault, rel, "\n".join(body))
    write_note(vault, "Sources/index.md", "\n".join(index_lines))
    return source_map


def content_note_path(row: sqlite3.Row) -> str:
    published = row["published_at"] or row["collected_at"] or ""
    month = published[:7] if len(published) >= 7 else "unknown"
    title = row["title"] or row["body_text"] or f"content-{row['id']}"
    return f"Content/{month}/{row['id']}-{slugify(title, f'content-{row['id']}')}.md"


def export_content(vault: Path, conn: sqlite3.Connection, source_map: dict[int, str], limit: Optional[int], copy_media: bool) -> dict[int, str]:
    sql = """
        SELECT c.*, s.name AS source_name, s.category AS source_category
        FROM content_items c
        LEFT JOIN sources s ON s.id = c.source_id
        ORDER BY COALESCE(c.published_at, c.collected_at, c.id)
    """
    params = []
    if limit:
        sql += " LIMIT ?"
        params.append(limit)

    content_rows = rows(conn, sql, params)
    content_map: dict[int, str] = {}
    index_lines = ["# Контент", "", "| ID | Дата | Тип | Статус | Заголовок |", "|---:|---|---|---|---|"]

    for item in content_rows:
        rel = content_note_path(item)
        content_map[item["id"]] = rel
        index_lines.append(
            f"| {item['id']} | {md_escape((item['published_at'] or '')[:10])} | {md_escape(item['content_type'])} | {md_escape(item['status'])} | {note_link(rel, md_escape(item['title'] or 'без заголовка'))} |"
        )

        tags = rows(conn, "SELECT tag_level, tag_name, confidence, tag_source FROM content_tags WHERE content_item_id=? ORDER BY tag_level, tag_name", (item["id"],))
        claims = rows(conn, "SELECT id, claim_text, status FROM claims WHERE content_item_id=? ORDER BY id", (item["id"],))
        entities = rows(conn, """
            SELECT e.id, e.entity_type, e.canonical_name, em.mention_type, em.confidence
            FROM entity_mentions em
            JOIN entities e ON e.id = em.entity_id
            WHERE em.content_item_id=?
            ORDER BY e.entity_type, e.canonical_name
        """, (item["id"],))
        attachments = rows(conn, "SELECT * FROM attachments WHERE content_item_id=? ORDER BY id", (item["id"],))

        body = [
            frontmatter({
                "type": "content_item",
                "content_id": item["id"],
                "source_id": item["source_id"],
                "content_type": item["content_type"],
                "status": item["status"],
                "published_at": item["published_at"],
                "url": item["url"],
                "tags": [t["tag_name"] for t in tags],
            }),
            f"# {item['title'] or 'Без заголовка'}",
            "",
            f"- ID: `{item['id']}`",
            f"- Источник: {note_link(source_map[item['source_id']], item['source_name']) if item['source_id'] in source_map else (item['source_name'] or '')}",
            f"- Тип: `{item['content_type'] or ''}`",
            f"- Статус: `{item['status'] or ''}`",
            f"- Опубликовано: `{item['published_at'] or ''}`",
            f"- URL: {item['url'] or ''}",
            "",
            "## Текст",
            "",
            item["body_text"] or "",
        ]

        if attachments:
            body.extend(["", "## Вложения", ""])
            for att in attachments:
                link, status = copy_attachment(vault, att, copy_media=copy_media)
                label = link or f"`{status}`"
                body.append(f"- {label} type=`{att['attachment_type']}` size=`{att['file_size'] or 0}` hash=`{att['hash_sha256'] or ''}`")
                if att["ocr_text"]:
                    body.extend(["", f"### OCR attachment {att['id']}", "", att["ocr_text"]])

        if tags:
            body.extend(["", "## Теги", "", "| Уровень | Тег | Уверенность | Источник |", "|---:|---|---:|---|"])
            for tag in tags:
                body.append(f"| {tag['tag_level']} | {md_escape(tag['tag_name'])} | {tag['confidence'] or 0:.2f} | {md_escape(tag['tag_source'])} |")

        if claims:
            body.extend(["", "## Заявления", ""])
            for claim in claims:
                body.append(f"- `#{claim['id']}` `{claim['status']}` {claim['claim_text']}")

        if entities:
            body.extend(["", "## Сущности", ""])
            for ent in entities:
                body.append(f"- `{ent['entity_type']}` {ent['canonical_name']} ({ent['mention_type']}, {ent['confidence'] or 0:.2f})")

        write_note(vault, rel, "\n".join(body))

    write_note(vault, "Content/index.md", "\n".join(index_lines))
    return content_map


def export_claims(vault: Path, conn: sqlite3.Connection, content_map: dict[int, str], limit: Optional[int]):
    sql = "SELECT * FROM claims ORDER BY id"
    params = []
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    claim_rows = rows(conn, sql, params)
    index_lines = ["# Заявления", "", "| ID | Статус | Тип | Текст |", "|---:|---|---|---|"]
    for claim in claim_rows:
        rel = f"Claims/{claim['id']}-{slugify(claim['claim_text'], f'claim-{claim['id']}')}.md"
        index_lines.append(f"| {claim['id']} | {md_escape(claim['status'])} | {md_escape(claim['claim_type'])} | {note_link(rel, md_escape(claim['claim_text'][:80]))} |")
        evidence = rows(conn, "SELECT * FROM evidence_links WHERE claim_id=? ORDER BY id", (claim["id"],))
        body = [
            frontmatter({"type": "claim", "claim_id": claim["id"], "status": claim["status"]}),
            f"# Заявление {claim['id']}",
            "",
            claim["claim_text"] or "",
            "",
            f"- Контент: {note_link(content_map[claim['content_item_id']]) if claim['content_item_id'] in content_map else claim['content_item_id']}",
            f"- Тип: `{claim['claim_type'] or ''}`",
            f"- Статус: `{claim['status'] or ''}`",
            f"- Auto confidence: `{claim['confidence_auto'] or ''}`",
            f"- Final confidence: `{claim['confidence_final'] or ''}`",
        ]
        if evidence:
            body.extend(["", "## Evidence", ""])
            for ev in evidence:
                ev_target = note_link(content_map[ev["evidence_item_id"]]) if ev["evidence_item_id"] in content_map else ev["evidence_item_id"]
                body.append(f"- `{ev['evidence_type']}` `{ev['strength']}` -> {ev_target} {ev['notes'] or ''}")
        write_note(vault, rel, "\n".join(body))
    write_note(vault, "Claims/index.md", "\n".join(index_lines))


def export_cases(vault: Path, conn: sqlite3.Connection, limit: Optional[int]):
    sql = "SELECT * FROM cases ORDER BY id"
    params = []
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    case_rows = rows(conn, sql, params)
    index_lines = ["# Кейсы", "", "| ID | Статус | Тип | Название |", "|---:|---|---|---|"]
    for case in case_rows:
        rel = f"Cases/{case['id']}-{slugify(case['title'], f'case-{case['id']}')}.md"
        index_lines.append(f"| {case['id']} | {md_escape(case['status'])} | {md_escape(case['case_type'])} | {note_link(rel, md_escape(case['title']))} |")
        linked_claims = rows(conn, """
            SELECT cc.role, cl.id, cl.claim_text, cl.status
            FROM case_claims cc
            JOIN claims cl ON cl.id = cc.claim_id
            WHERE cc.case_id=?
            ORDER BY cl.id
        """, (case["id"],))
        events = rows(conn, "SELECT * FROM case_events WHERE case_id=? ORDER BY event_date, event_order, id", (case["id"],))
        body = [
            frontmatter({"type": "case", "case_id": case["id"], "status": case["status"]}),
            f"# {case['title']}",
            "",
            case["description"] or "",
            "",
            f"- ID: `{case['id']}`",
            f"- Тип: `{case['case_type'] or ''}`",
            f"- Статус: `{case['status'] or ''}`",
            f"- Регион: `{case['region'] or ''}`",
        ]
        if linked_claims:
            body.extend(["", "## Заявления", ""])
            for claim in linked_claims:
                body.append(f"- `{claim['role']}` `#{claim['id']}` `{claim['status']}` {claim['claim_text']}")
        if events:
            body.extend(["", "## Таймлайн", ""])
            for event in events:
                body.append(f"- `{event['event_date']}` {event['event_title']} {event['event_description'] or ''}")
        write_note(vault, rel, "\n".join(body))
    write_note(vault, "Cases/index.md", "\n".join(index_lines))


def export_entities(vault: Path, conn: sqlite3.Connection, limit: Optional[int]):
    sql = "SELECT * FROM entities ORDER BY entity_type, canonical_name"
    params = []
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    entity_rows = rows(conn, sql, params)
    index_lines = ["# Сущности", "", "| ID | Тип | Имя |", "|---:|---|---|"]
    for ent in entity_rows:
        rel = f"Entities/{ent['entity_type'] or 'entity'}/{ent['id']}-{slugify(ent['canonical_name'], f'entity-{ent['id']}')}.md"
        index_lines.append(f"| {ent['id']} | {md_escape(ent['entity_type'])} | {note_link(rel, md_escape(ent['canonical_name']))} |")
        mentions_count = one(conn, "SELECT COUNT(*) FROM entity_mentions WHERE entity_id=?", (ent["id"],))[0]
        body = [
            frontmatter({"type": "entity", "entity_id": ent["id"], "entity_type": ent["entity_type"]}),
            f"# {ent['canonical_name']}",
            "",
            f"- ID: `{ent['id']}`",
            f"- Тип: `{ent['entity_type'] or ''}`",
            f"- ИНН: `{ent['inn'] or ''}`",
            f"- ОГРН: `{ent['ogrn'] or ''}`",
            f"- Упоминаний: `{mentions_count}`",
            "",
            ent["description"] or "",
        ]
        write_note(vault, rel, "\n".join(body))
    write_note(vault, "Entities/index.md", "\n".join(index_lines))


def export_files_index(vault: Path, conn: sqlite3.Connection, copy_media: bool, limit: Optional[int]):
    sql = "SELECT * FROM raw_blobs ORDER BY id"
    params = []
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    blob_rows = rows(conn, sql, params)
    lines = ["# Файлы", "", "| ID | Тип | Файл | Размер | SHA-256 |", "|---:|---|---|---:|---|"]
    for blob in blob_rows:
        fake_att = {
            "id": blob["id"],
            "file_path": blob["file_path"],
            "hash_sha256": blob["hash_sha256"],
            "mime_type": blob["mime_type"],
        }
        link, status = copy_attachment(vault, fake_att, copy_media=copy_media)
        lines.append(f"| {blob['id']} | {md_escape(blob['blob_type'])} | {link or md_escape(status)} | {blob['file_size'] or 0} | `{blob['hash_sha256'] or ''}` |")
    write_note(vault, "Files/index.md", "\n".join(lines))


def export_obsidian(
    db_path: Path,
    vault: Path,
    limit: Optional[int] = None,
    copy_media: bool = True,
    mode: str = "graph",
    clean: bool = False,
):
    mode = (mode or "graph").strip().lower()
    if clean:
        clean_exported_vault(vault)
    if mode == "graph":
        from tools.export_obsidian_graph import export_graph_obsidian

        export_graph_obsidian(
            db_path=db_path,
            vault=vault,
            limit=limit,
            copy_media=copy_media,
        )
        return
    if mode != "archive":
        raise ValueError(f"Unsupported export mode: {mode}")

    ensure_dir(vault)
    conn = connect(db_path)
    generated_at = datetime.now().isoformat(timespec="seconds")
    try:
        export_index(vault, conn, generated_at)
        source_map = export_sources(vault, conn)
        content_map = export_content(vault, conn, source_map, limit, copy_media)
        export_claims(vault, conn, content_map, limit)
        export_cases(vault, conn, limit)
        export_entities(vault, conn, limit)
        export_files_index(vault, conn, copy_media, limit)
        set_runtime_metadata(
            conn,
            "obsidian_built_from_pipeline_version",
            get_runtime_metadata(
                conn,
                "analysis_built_from_pipeline_version",
                get_runtime_metadata(conn, "current_pipeline_version"),
            ),
        )
        set_runtime_metadata(conn, "obsidian_export_generated_at", generated_at)
    finally:
        conn.close()


def main():
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Export news database to an Obsidian vault")
    parser.add_argument("--db", default=settings.get("db_path", str(PROJECT_ROOT / "db" / "news_unified.db")))
    parser.add_argument("--vault", default=settings.get("obsidian_export_dir", str(PROJECT_ROOT / "obsidian_export")))
    parser.add_argument("--limit", type=int, default=0, help="Limit exported content/claims/cases/entities for smoke tests")
    parser.add_argument("--mode", choices=("graph", "archive"), default="graph", help="Obsidian export mode")
    parser.add_argument("--no-media", action="store_true", help="Write notes without copying media files")
    parser.add_argument("--clean", action="store_true", help="Remove generated notes/media before exporting; preserves .obsidian")
    args = parser.parse_args()

    db_path = Path(args.db)
    vault = Path(args.vault)
    export_obsidian(
        db_path=db_path,
        vault=vault,
        limit=args.limit or None,
        copy_media=not args.no_media,
        mode=args.mode,
        clean=args.clean,
    )
    print(f"Obsidian export complete ({args.mode}): {vault}")


if __name__ == "__main__":
    main()
