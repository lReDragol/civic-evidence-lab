import json
import logging
import os
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

CLASSIFY_PROMPT = """Ты — аналитик российских новостей. Классифицируй следующий текст по трём уровням:

L1 (тип события): одно из: detention, court_decision, public_statement, censorship_action, vote_record, procurement_claim, ownership_claim, corruption_claim, mobilization_claim, abuse_claim, protest, legislation, policy_change, election, sanction, military, terrorism, fraud, repression, surveillance, other

L2 (тема): одно из: politics, law, economy, corruption, security, media, human_rights, social, duma, government, intelligence, propaganda, courts, military, elections, healthcare, education, environment, international

L3 (оценка риска): одно или несколько из: high_risk, manipulation, contradiction, unverified_claim, hate_speech, threat, false_promise, conflict_of_interest

Также оцени:
- manipulation_risk: число от 0 до 1 (вероятность манипуляции/дезинформации)
- key_claim: главная утверждаемая фраза (если есть)
- sentiment: positive/negative/neutral

Ответь ТОЛЬКО в формате JSON, без пояснений:
{
  "l1": "...",
  "l2": "...",
  "l3": ["..."],
  "manipulation_risk": 0.0,
  "key_claim": "...",
  "sentiment": "..."
}"""


def _call_ollama(text: str, model: str = "qwen2.5:14b", host: str = "http://localhost:11434") -> Optional[Dict]:
    try:
        from ollama import Client
        client = Client(host=host)
        prompt = f"{CLASSIFY_PROMPT}\n\nТекст:\n{text[:3000]}"
        response = client.chat(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0.1, "num_predict": 512},
        )
        content = response["message"]["content"].strip()
        json_match = re.search(r'\{[^{}]+\}', content, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
        log.warning("No JSON found in LLM response: %s", content[:100])
    except Exception as e:
        log.warning("Ollama call failed: %s", e)
    return None


def _store_llm_tags(conn: sqlite3.Connection, content_id: int, result: Dict):
    if result.get("l1"):
        conn.execute(
            "INSERT OR IGNORE INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source) VALUES(?,1,?,?,'llm')",
            (content_id, result["l1"], 0.8),
        )
    if result.get("l2"):
        conn.execute(
            "INSERT OR IGNORE INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source) VALUES(?,2,?,?,'llm')",
            (content_id, result["l2"], 0.8),
        )
    for tag in result.get("l3", []):
        if tag:
            conn.execute(
                "INSERT OR IGNORE INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source) VALUES(?,3,?,?,'llm')",
                (content_id, tag, 0.7),
            )

    manip_risk = result.get("manipulation_risk", 0)
    if isinstance(manip_risk, (int, float)):
        conn.execute(
            "UPDATE content_items SET status=CASE WHEN ? > 0.5 THEN 'flagged' ELSE status END WHERE id=?",
            (manip_risk, content_id),
        )

    key_claim = result.get("key_claim", "")
    if key_claim and len(key_claim) > 10:
        try:
            conn.execute(
                """INSERT INTO claims(content_item_id, claim_text, claim_type, status, source_score, needs_review, manipulation_risk)
                   VALUES(?,?,?,'unverified',0.5,1,?)""",
                (content_id, key_claim[:500], result.get("l1", "unclassified"), manip_risk),
            )
        except Exception:
            pass


def classify_content(settings: dict = None, batch_size: int = 100):
    if settings is None:
        settings = load_settings()

    model = settings.get("ollama_model", "qwen2.5:14b")
    host = settings.get("ollama_host", "http://localhost:11434")

    conn = get_db(settings)

    rows = conn.execute(
        """
        SELECT c.id, c.body_text, c.title
        FROM content_items c
        WHERE (length(c.body_text) > 30 OR length(c.title) > 10)
          AND c.llm_processed = 0
        ORDER BY c.id
        LIMIT ?
        """,
        (batch_size,),
    ).fetchall()

    if not rows:
        log.info("No content items to classify via LLM")
        conn.close()
        return

    log.info("LLM classifying %d items (model=%s)", len(rows), model)

    classified = 0
    failed = 0
    for row in rows:
        content_id = row["id"]
        text = f"{row['title'] or ''}\n{row['body_text'] or ''}"
        if len(text.strip()) < 30:
            conn.execute("UPDATE content_items SET llm_processed=1 WHERE id=?", (content_id,))
            continue

        result = _call_ollama(text, model=model, host=host)
        if result:
            _store_llm_tags(conn, content_id, result)
            classified += 1
        else:
            failed += 1

        conn.execute("UPDATE content_items SET llm_processed=1 WHERE id=?", (content_id,))

        if (classified + failed) % 10 == 0:
            conn.commit()

    conn.commit()

    log.info("LLM classification done: %d classified, %d failed", classified, failed)
    conn.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    classify_content()


if __name__ == "__main__":
    main()
