import json
import logging
import os
import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings
from verification.claim_normalizer import canonical_hash, canonicalize_claim_text

log = logging.getLogger(__name__)

VALID_L1 = {
    "court", "detention", "public_statement", "censorship_action", "vote_record",
    "procurement_claim", "ownership_claim", "corruption_claim", "mobilization_claim",
    "abuse_claim", "protest", "legislation", "policy_change", "election", "sanction",
    "military", "terrorism", "fraud", "repression", "surveillance", "economic",
    "accident", "conflict", "bankruptcy",
}

VALID_L2 = {
    "politics", "law", "economy", "corruption", "security", "media", "human_rights",
    "social", "duma", "government", "intelligence", "propaganda", "courts", "military",
    "elections", "healthcare", "education", "environment", "international", "housing",
    "technology", "finance", "regional",
}

VALID_L3 = {
    "high_risk", "manipulation", "contradiction", "unverified_claim", "hate_speech",
    "threat", "false_promise", "conflict_of_interest", "needs_verification",
    "possible_corruption", "possible_disinformation", "document_attached",
    "official_confirmation", "surveillance_risk",
}

MANIPULATION_TECHNIQUES = {
    "whataboutism", "appeal_to_fear", "false_dichotomy", "straw_man",
    "ad_hominem", "bandwagon", "cherry_picking", "red_herring",
    "loaded_language", "gaslighting", "appeal_to_authority",
    "false_equivalence", "deflection", "victim_blaming",
}

CLASSIFY_PROMPT_V2 = """Ты — аналитик российских новостей и публичных заявлений. Проанализируй текст и дай структурированную оценку.

ДОПУСТИМЫЕ ЗНАЧЕНИЯ ТЕГОВ (используй ТОЛЬКО эти):

L1 (тип события): {l1_options}
L2 (тема): {l2_options}
L3 (оценка риска, можно несколько): {l3_options}

Также извлеки:
- law_references: список упомянутых законов/статей (формат: {{"type": "ФЗ/УК/КоАП/постановление/указ", "number": "номер", "article": "статья"}})
- named_entities: список упомянутых ФИО и организаций
- manipulation_techniques: список техник манипуляции из: {manip_options}
- reasoning: краткое объяснение почему выбраны эти теги (1-2 предложения)

Оцени:
- manipulation_risk: 0.0-1.0
- key_claim: главная утверждаемая фраза
- sentiment: positive/negative/neutral
- is_negated: true если утверждение отрицается (НЕ арестован, НЕ происходило и т.п.)

Ответь ТОЛЬКО JSON:
{{
  "l1": "...",
  "l2": "...",
  "l3": ["..."],
  "manipulation_risk": 0.0,
  "manipulation_techniques": [],
  "key_claim": "...",
  "sentiment": "...",
  "is_negated": false,
  "law_references": [],
  "named_entities": [],
  "reasoning": "..."
}}"""


def _build_prompt() -> str:
    return CLASSIFY_PROMPT_V2.format(
        l1_options=", ".join(sorted(VALID_L1)),
        l2_options=", ".join(sorted(VALID_L2)),
        l3_options=", ".join(sorted(VALID_L3)),
        manip_options=", ".join(sorted(MANIPULATION_TECHNIQUES)),
    )


def _validate_and_normalize(result: Dict) -> Dict:
    normalized = {}

    l1 = result.get("l1", "")
    if l1 in VALID_L1:
        normalized["l1"] = l1
    elif l1:
        l1_lower = l1.lower().replace(" ", "_")
        if l1_lower in VALID_L1:
            normalized["l1"] = l1_lower
        else:
            normalized["l1"] = None
    else:
        normalized["l1"] = None

    l2 = result.get("l2", "")
    if l2 in VALID_L2:
        normalized["l2"] = l2
    elif l2:
        l2_lower = l2.lower().replace(" ", "_")
        if l2_lower in VALID_L2:
            normalized["l2"] = l2_lower
        else:
            normalized["l2"] = None
    else:
        normalized["l2"] = None

    l3_raw = result.get("l3", [])
    if isinstance(l3_raw, str):
        l3_raw = [l3_raw]
    normalized["l3"] = [t for t in l3_raw if t in VALID_L3]

    manip_risk = result.get("manipulation_risk", 0)
    try:
        manip_risk = float(manip_risk)
        manip_risk = max(0.0, min(1.0, manip_risk))
    except (TypeError, ValueError):
        manip_risk = 0.0
    normalized["manipulation_risk"] = manip_risk

    manip_techs = result.get("manipulation_techniques", [])
    if isinstance(manip_techs, str):
        manip_techs = [manip_techs]
    normalized["manipulation_techniques"] = [t for t in manip_techs if t in MANIPULATION_TECHNIQUES]

    normalized["key_claim"] = str(result.get("key_claim", ""))[:500]
    normalized["sentiment"] = result.get("sentiment", "neutral")
    if normalized["sentiment"] not in ("positive", "negative", "neutral"):
        normalized["sentiment"] = "neutral"

    normalized["is_negated"] = bool(result.get("is_negated", False))

    law_refs = result.get("law_references", [])
    if isinstance(law_refs, list):
        normalized["law_references"] = [
            r for r in law_refs
            if isinstance(r, dict) and r.get("type") and (r.get("number") or r.get("article"))
        ]
    else:
        normalized["law_references"] = []

    named_ents = result.get("named_entities", [])
    if isinstance(named_ents, list):
        normalized["named_entities"] = [e for e in named_ents if isinstance(e, (str, dict))][:10]
    else:
        normalized["named_entities"] = []

    normalized["reasoning"] = str(result.get("reasoning", ""))[:500]

    return normalized


def _extract_json_robust(text: str) -> Optional[Dict]:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    m = re.search(r'\{[^{}]*\}', text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass

    m = re.search(r'\{.*\}', text, re.DOTALL)
    if m:
        candidate = m.group()
        depth = 0
        for i, ch in enumerate(candidate):
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(candidate[:i+1])
                    except json.JSONDecodeError:
                        break

    fields = {}
    for field in ["l1", "l2", "key_claim", "sentiment", "reasoning"]:
        m = re.search(rf'"{field}"\s*:\s*"([^"]*)"', text)
        if m:
            fields[field] = m.group(1)
    for field in ["manipulation_risk"]:
        m = re.search(rf'"{field}"\s*:\s*([0-9.]+)', text)
        if m:
            fields[field] = float(m.group(1))
    for field in ["is_negated"]:
        m = re.search(rf'"{field}"\s*:\s*(true|false)', text, re.I)
        if m:
            fields[field] = m.group(1).lower() == "true"
    m = re.search(r'"l3"\s*:\s*\[([^\]]*)\]', text)
    if m:
        items = re.findall(r'"([^"]*)"', m.group(1))
        fields["l3"] = items
    m = re.search(r'"manipulation_techniques"\s*:\s*\[([^\]]*)\]', text)
    if m:
        items = re.findall(r'"([^"]*)"', m.group(1))
        fields["manipulation_techniques"] = items

    return fields if fields else None


def _call_ollama(text: str, model: str = "qwen2.5:14b",
                 host: str = "http://localhost:11434") -> Optional[Dict]:
    try:
        from ollama import Client
        client = Client(host=host)
        prompt = f"{_build_prompt()}\n\nТекст:\n{text[:3000]}"
        response = client.chat(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0.1, "num_predict": 800},
        )
        content = response["message"]["content"].strip()
        raw = _extract_json_robust(content)
        if raw:
            return _validate_and_normalize(raw)
        log.warning("No valid JSON from LLM: %s", content[:100])
    except Exception as e:
        log.warning("Ollama call failed: %s", e)
    return None


def _store_llm_results(conn: sqlite3.Connection, content_id: int, result: Dict):
    if result.get("l1"):
        existing = conn.execute(
            "SELECT id, confidence FROM content_tags WHERE content_item_id=? AND tag_level=1 AND tag_name=? AND tag_source='rule'",
            (content_id, result["l1"]),
        ).fetchone()
        if existing:
            rule_conf = existing[1] or 0.5
            llm_conf = 0.8
            conn.execute(
                "UPDATE content_tags SET confidence=? WHERE id=?",
                (max(rule_conf, llm_conf), existing[0]),
            )
        else:
            conn.execute(
                "INSERT OR IGNORE INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source) VALUES(?,1,?,0.8,'llm')",
                (content_id, result["l1"]),
            )
        tag_row = conn.execute(
            "SELECT id FROM content_tags WHERE content_item_id=? AND tag_level=1 AND tag_name=?",
            (content_id, result["l1"]),
        ).fetchone()
        if tag_row and result.get("reasoning"):
            try:
                conn.execute(
                    "INSERT INTO tag_explanations(content_tag_id, trigger_text, trigger_rule, matched_pattern, confidence_raw) VALUES(?,?,?,?,?)",
                    (tag_row[0], result["reasoning"][:300], result["l1"], "llm_v2", 0.8),
                )
            except Exception:
                pass

    if result.get("l2"):
        existing = conn.execute(
            "SELECT id, confidence FROM content_tags WHERE content_item_id=? AND tag_level=2 AND tag_name=? AND tag_source='rule'",
            (content_id, result["l2"]),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE content_tags SET confidence=max(confidence,0.8) WHERE id=?",
                (existing[0],),
            )
        else:
            conn.execute(
                "INSERT OR IGNORE INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source) VALUES(?,2,?,0.8,'llm')",
                (content_id, result["l2"]),
            )

    for tag in result.get("l3", []):
        conn.execute(
            "INSERT OR IGNORE INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source) VALUES(?,3,?,0.7,'llm')",
            (content_id, tag),
        )

    manip_risk = result.get("manipulation_risk", 0)
    if manip_risk > 0.5:
        conn.execute(
            "UPDATE content_items SET status='flagged' WHERE id=? AND status NOT IN ('evidence','confirmed')",
            (content_id,),
        )

    key_claim = result.get("key_claim", "")
    canonical_claim = canonicalize_claim_text(key_claim, result.get("l1"))
    if canonical_claim and len(canonical_claim) > 10:
        try:
            conn.execute(
                """
                INSERT INTO claims(
                    content_item_id, claim_text, canonical_text, canonical_hash,
                    claim_type, status, source_score, needs_review, manipulation_risk
                ) VALUES(?,?,?,?,?,'unverified',0.5,1,?)
                """,
                (
                    content_id,
                    key_claim[:500],
                    canonical_claim[:500],
                    canonical_hash(canonical_claim),
                    result.get("l1") or "unclassified",
                    manip_risk,
                ),
            )
        except Exception:
            pass

    if result.get("is_negated"):
        try:
            conn.execute(
                "INSERT OR IGNORE INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source) VALUES(?,3,'negated_claim',0.9,'llm')",
                (content_id,),
            )
        except Exception:
            pass

    for tech in result.get("manipulation_techniques", []):
        try:
            conn.execute(
                "INSERT OR IGNORE INTO content_tags(content_item_id, tag_level, tag_name, confidence, tag_source) VALUES(?,3,?,0.7,'llm')",
                (content_id, f"manip:{tech}"),
            )
        except Exception:
            pass

    if result.get("law_references"):
        try:
            from classifier.law_reference_extractor import store_law_references
            refs = [{"law_type": r.get("type", ""), "law_number": r.get("number", ""),
                     "article": r.get("article", ""), "context": ""} for r in result["law_references"]]
            store_law_references(conn, content_id, refs)
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

    log.info("LLM v2 classifying %d items (model=%s)", len(rows), model)

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
            _store_llm_results(conn, content_id, result)
            conn.execute("UPDATE content_items SET llm_processed=1 WHERE id=?", (content_id,))
            classified += 1
        else:
            failed += 1

        if (classified + failed) % 10 == 0:
            conn.commit()

    conn.commit()
    log.info("LLM v2 classification done: %d classified, %d failed", classified, failed)
    conn.close()
    return {"classified": classified, "failed": failed}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=100)
    args = parser.parse_args()

    result = classify_content(batch_size=args.batch)
    if result:
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
