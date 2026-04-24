import json
import logging
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

QUOTE_PATTERNS = [
    (re.compile(r'«([^»]{10,500})»', re.DOTALL), "direct"),
    (re.compile(r'"([^"]{10,500})"', re.DOTALL), "direct"),
    (re.compile(r'(?:заявил|сказал|выступил|написал|подчеркнул|отметил|добавил|уточнил|сообщил|обратился)[^.:]{0,40}[:\-]\s*([А-ЯЁ][^.!?]+[.!?])', re.I), "indirect"),
    (re.compile(r'(?:по\s+словам|по\s+мнению|как\s+заявил|как\s+отметил|как\s+сказал)[^.:]{0,40}[:\-]?\s*([А-ЯЁ][^.!?]+[.!?])', re.I), "reported"),
]

RHETORIC_RULES = [
    (re.compile(r'(враг|предатель|измена|вражеск|пятая\s+колон|национал-предат|иностранный\s+агент)', re.I), "hostile_labeling"),
    (re.compile(r'(мы\s+должны|необходимо|обязаны|придётся|вынуждены|нет\s+другого\s+выхода|без\s+альтернатив)', re.I), "pressure"),
    (re.compile(r'(всё\s+под\s+контролем|ситуация\s+стабильн|нет\s+причин\s+для\s+беспокойств|паниковать\s+не\s+стоит)', re.I), "minimization"),
    (re.compile(r'(западн|коллективный\s+запад|англосакс|нATO|НАТО|вашингтон|брюссель|лондон)[^\s]*(?:винов|угрож|атак|вмеш)', re.I), "external_blame"),
    (re.compile(r'(это\s+(?:и\s+есть|и\s+будет|и\s+останется)|так\s+было\s+всегда|так\s+устроен|историческ)', re.I), "naturalization"),
    (re.compile(r'(кремль|президент|глав[аы]|руководств|власть)\s+(?:принял|реш[ии]л|утвердил|подписал|дал\s+указание)', re.I), "authority_appeal"),
    (re.compile(r'(фейк|дезинформ|ложн|манипуляц|фейковы|ненаучн|лженаук)', re.I), "dismissal"),
    (re.compile(r'(оскорб|унизитель|грязн|подон|мраз|твар|скот|дебил|идиот|дурак|придур)', re.I), "insult"),
    (re.compile(r'(угрож|пригроз|накажем|ответ|последств|жёстк|непримирим)', re.I), "threat"),
    (re.compile(r'(обещал|гарантир|будет\s+лучше|подним|увелич|улучш|снизим|решим\s+проблему)', re.I), "promise"),
]

DEPUTY_SURNAMES = [
    "володин", "зюганов", "миронов", "слуцкий", "жириновский", "невзоров",
    "мизулина", "яровая", "пушкина", "хинштейн", "красненков", "бутрина",
    "матвеев", "парфёнов", "гаврилов", "турчак", "саблин", "пискарёв",
    "марданшин", "луговой", "тимофеева", "лантратова", "фаррахов",
    "крашенинников", "васильев", "терентьев", "дегтярёв", "сечин",
    "преженталь", "резник", "голованов", "иванов", "кравцов",
]


def _find_deputy_entity(conn: sqlite3.Connection, text: str) -> Optional[int]:
    text_lower = text.lower()
    for surname in DEPUTY_SURNAMES:
        if surname in text_lower:
            row = conn.execute(
                "SELECT entity_id FROM deputy_profiles dp JOIN entities e ON e.id = dp.entity_id WHERE e.canonical_name LIKE ?",
                (f"%{surname.capitalize()}%",),
            ).fetchone()
            if row:
                return row[0]
            row2 = conn.execute(
                "SELECT id FROM entities WHERE entity_type='person' AND canonical_name LIKE ? LIMIT 1",
                (f"%{surname.capitalize()}%",),
            ).fetchone()
            if row2:
                return row2[0]
    return None


def _detect_rhetoric(text: str) -> List[str]:
    classes = []
    for pattern, rhetoric_class in RHETORIC_RULES:
        if pattern.search(text):
            classes.append(rhetoric_class)
    return classes


def extract_quotes(text: str) -> List[Dict]:
    quotes = []
    for pattern, qtype in QUOTE_PATTERNS:
        for match in pattern.finditer(text):
            quote_text = match.group(1).strip()
            if len(quote_text) > 10:
                quotes.append({
                    "text": quote_text,
                    "type": qtype,
                    "start": match.start(),
                    "end": match.end(),
                })
    return quotes


def process_content_quotes(settings: dict = None, batch_size: int = 500):
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    rows = conn.execute(
        """
        SELECT c.id, c.body_text, c.title
        FROM content_items c
        WHERE (length(c.body_text) > 50 OR length(c.title) > 20)
          AND c.quotes_processed = 0
        ORDER BY c.id
        LIMIT ?
        """,
        (batch_size,),
    ).fetchall()

    if not rows:
        log.info("No new content items for quote extraction")
        conn.close()
        return

    log.info("Extracting quotes from %d items", len(rows))

    total_quotes = 0
    flagged_quotes = 0

    for row in rows:
        content_id = row["id"]
        text = f"{row['title'] or ''}\n{row['body_text'] or ''}"
        if len(text) < 50:
            conn.execute("UPDATE content_items SET quotes_processed=1 WHERE id=?", (content_id,))
            continue

        quotes = extract_quotes(text)
        if not quotes:
            conn.execute("UPDATE content_items SET quotes_processed=1 WHERE id=?", (content_id,))
            continue

        entity_id = _find_deputy_entity(conn, text)

        for q in quotes:
            rhetoric_classes = _detect_rhetoric(q["text"])
            rhetoric_str = ",".join(rhetoric_classes) if rhetoric_classes else None
            is_flagged = 1 if rhetoric_classes else 0

            try:
                conn.execute(
                    """INSERT INTO quotes(content_item_id, entity_id, quote_text, rhetoric_class, is_flagged)
                       VALUES(?,?,?,?,?)""",
                    (content_id, entity_id, q["text"][:2000], rhetoric_str, is_flagged),
                )
                total_quotes += 1
                if is_flagged:
                    flagged_quotes += 1
            except Exception as e:
                log.warning("Failed to insert quote for item %d: %s", content_id, e)

        conn.execute("UPDATE content_items SET quotes_processed=1 WHERE id=?", (content_id,))

    conn.commit()

    quote_count = conn.execute("SELECT COUNT(*) FROM quotes").fetchone()[0]
    flagged = conn.execute("SELECT COUNT(*) FROM quotes WHERE is_flagged=1").fetchone()[0]
    log.info("Quotes: %d total (%d flagged), %d new this batch", quote_count, flagged, total_quotes)

    conn.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    process_content_quotes()


if __name__ == "__main__":
    main()
