import logging
import re
import sqlite3
import sys
import json
from pathlib import Path
from typing import Dict, List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

LAW_PATTERNS = [
    (re.compile(
        r'(?:Федеральн[аы][яй]\s+закон|ФЗ|федеральн[аы][яй]\s+закон)\s*[—\-–]?\s*№?\s*(\d+(?:[-–]\d+)*)\s*[—\-–]?\s*(?:ФЗ)?',
        re.I
    ), "ФЗ"),

    (re.compile(
        r'(\d+(?:[-–]\d+)*)\s*[—\-–]?\s*ФЗ\b',
        re.I
    ), "ФЗ"),

    (re.compile(
        r'(?:ст\.?\s*|стать[еяьи]\s+)(\d+(?:\.\d+)*)\s*(?:(?:ч\.?\s*|част[ьяьи]\s*)(\d+)\s*)?'
        r'(?:УК\s*РФ|Уголовн[аы][яй]\s+кодекс|уголовн[аы][яй]\s+кодекс)',
        re.I
    ), "УК РФ"),

    (re.compile(
        r'(?:ст\.?\s*|стать[еяьи]\s+)(\d+(?:\.\d+)*)\s*(?:(?:ч\.?\s*|част[ьяьи]\s*)(\d+)\s*)?'
        r'(?:КоАП|Кодекс\s+об\s+административ|кодекс\s+об\s+административ)',
        re.I
    ), "КоАП"),

    (re.compile(
        r'(?:ст\.?\s*|стать[еяьи]\s+)(\d+(?:\.\d+)*)\s*(?:(?:ч\.?\s*|част[ьяьи]\s*)(\d+)\s*)?'
        r'(?:ГК\s*РФ|Гражданск[аы][яй]\s+кодекс|гражданск[аы][яй]\s+кодекс)',
        re.I
    ), "ГК РФ"),

    (re.compile(
        r'(?:ст\.?\s*|стать[еяьи]\s+)(\d+(?:\.\d+)*)\s*(?:(?:ч\.?\s*|част[ьяьи]\s*)(\d+)\s*)?'
        r'(?:НК\s*РФ|Налогов[аы][яй]\s+кодекс|налогов[аы][яй]\s+кодекс)',
        re.I
    ), "НК РФ"),

    (re.compile(
        r'(?:ст\.?\s*|стать[еяьи]\s+)(\d+(?:\.\d+)*)\s*(?:(?:ч\.?\s*|част[ьяьи]\s*)(\d+)\s*)?'
        r'(?:ТК\s*РФ|Трудов[аы][яй]\s+кодекс|трудов[аы][яй]\s+кодекс)',
        re.I
    ), "ТК РФ"),

    (re.compile(
        r'(?:ЖК\s*РФ|Жилищн[аы][яй]\s+кодекс|жилищн[аы][яй]\s+кодекс)\s*(?:ст\.?\s*|стать[еяьи]\s+)?(\d+(?:\.\d+)*)?',
        re.I
    ), "ЖК РФ"),

    (re.compile(
        r'(?:ст\.?\s*|стать[еяьи]\s+)(\d+(?:\.\d+)*)\s*(?:(?:ч\.?\s*|част[ьяьи]\s*)(\d+)\s*)?'
        r'(?:СК\s*РФ|Семейн[аы][яй]\s+кодекс|семейн[аы][яй]\s+кодекс)',
        re.I
    ), "СК РФ"),

    (re.compile(
        r'(?:постановлен|Постановлен)[а-яА-ЯёЁ]*\s+(?:Правительства\s+РФ|правительства\s+РФ|Правительства\s+Российской\s+Федерации)?\s*№?\s*(\S+)',
        re.I
    ), "постановление"),

    (re.compile(
        r'(?:Указ|указ)\s+(?:Президента\s+РФ|президента\s+РФ|Президента\s+Российской\s+Федерации)?\s*№?\s*(\S+)',
        re.I
    ), "указ"),

    (re.compile(
        r'(?:Приказ|приказ)\s+(?:Мин(?:истерства)?\s*\w+|ФНС|Росреестр|РКН|ФАС|ФССП|МВД|ФСБ|СК|Прокуратур)?\s*№?\s*(\S+)',
        re.I
    ), "приказ"),

    (re.compile(
        r'(?:Решение|решение)\s+(?:Суда|суда|КС\s*РФ|Конституционн|Верховн)?\s*№?\s*(\S+)',
        re.I
    ), "решение"),

    (re.compile(
        r'(?:ст\.?\s*|стать[еяьи]\s+)(\d+(?:\.\d+)*)\s*(?:(?:ч\.?\s*|част[ьяьи]\s*)(\d+)\s*)?'
        r'(?:(?:УК|КоАП|ГК|НК|ТК|ЖК|СК|АПК|БК|ВК|ЗК|ГрК|ВоК|УПК|ГПК|КАС)\s*РФ)',
        re.I
    ), "кодекс"),
]


def extract_law_references(text: str) -> List[Dict]:
    if not text:
        return []

    results = []
    seen = set()

    for pattern, law_type in LAW_PATTERNS:
        for match in pattern.finditer(text):
            groups = match.groups()
            number = groups[0] if len(groups) >= 1 and groups[0] else ""
            article = ""
            part = groups[1] if len(groups) >= 2 and groups[1] else ""

            if law_type in ("ФЗ",):
                article = number
                number_clean = number
            elif law_type in ("УК РФ", "КоАП", "ГК РФ", "НК РФ", "ТК РФ", "ЖК РФ",
                              "СК РФ", "кодекс"):
                article = f"ч.{part} ст.{number}" if part else f"ст.{number}"
                number_clean = ""
            else:
                number_clean = number.rstrip(".,;:) ")

            start = max(0, match.start() - 60)
            end = min(len(text), match.end() + 40)
            context = text[start:end].strip()
            context = re.sub(r"\s+", " ", context)

            key = (law_type, number_clean, article)
            if key in seen:
                continue
            seen.add(key)

            results.append({
                "law_type": law_type,
                "law_number": number_clean,
                "article": article,
                "context": context[:300],
            })

    return results


def store_law_references(conn, content_item_id: int, refs: List[Dict]):
    if not refs:
        return 0
    stored = 0
    for ref in refs:
        try:
            conn.execute(
                "INSERT INTO law_references(content_item_id, law_type, law_number, article, context) VALUES(?,?,?,?,?)",
                (content_item_id, ref["law_type"], ref.get("law_number", ""),
                 ref.get("article", ""), ref.get("context", "")),
            )
            stored += 1
        except Exception:
            pass
    return stored


def extract_and_store_for_item(conn, content_item_id: int, text: str) -> int:
    refs = extract_law_references(text)
    if refs:
        return store_law_references(conn, content_item_id, refs)
    return 0


def process_all_content(settings=None, limit: int = 5000, reprocess: bool = False):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)

    query = """
        SELECT c.id, c.title, c.body_text
        FROM content_items c
        WHERE length(c.body_text) > 50
    """
    if not reprocess:
        query += " AND c.id NOT IN (SELECT DISTINCT content_item_id FROM law_references)"
    query += " LIMIT ?"

    rows = conn.execute(query, (limit,)).fetchall()
    log.info("Processing %d content items for law references", len(rows))

    total_refs = 0
    items_with_refs = 0

    for content_id, title, body in rows:
        text = f"{title or ''}\n{body or ''}"
        refs = extract_law_references(text)
        if refs:
            stored = store_law_references(conn, content_id, refs)
            total_refs += stored
            items_with_refs += 1

        if total_refs % 100 == 0 and total_refs > 0:
            conn.commit()

    conn.commit()

    total_in_db = conn.execute("SELECT COUNT(*) FROM law_references").fetchone()[0]
    log.info("Law references: %d extracted, %d items with refs, %d total in DB",
             total_refs, items_with_refs, total_in_db)
    conn.close()
    return {"refs_extracted": total_refs, "items_with_refs": items_with_refs, "total_in_db": total_in_db}


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Extract law references from content")
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--reprocess", action="store_true", help="Reprocess already processed items")
    parser.add_argument("--test", type=str, help="Test extraction on given text")
    args = parser.parse_args()

    if args.test:
        refs = extract_law_references(args.test)
        for r in refs:
            print(f"  {r['law_type']} {r.get('law_number', '')} {r.get('article', '')}")
            print(f"    context: {r['context'][:100]}")
        return

    result = process_all_content(limit=args.limit, reprocess=args.reprocess)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
