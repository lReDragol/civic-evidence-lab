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


_OPENAI_COMPAT = {
    "deepseek": "https://api.deepseek.com/v1/chat/completions",
    "fireworks": "https://api.fireworks.ai/inference/v1/chat/completions",
    "together": "https://api.together.xyz/v1/chat/completions",
    "huggingface": "https://router.huggingface.co/v1/chat/completions",
    "groq": "https://api.groq.com/openai/v1/chat/completions",
    "mistral": "https://api.mistral.ai/v1/chat/completions",
    "openrouter": "https://openrouter.ai/api/v1/chat/completions",
    "perplexity": "https://api.perplexity.ai/chat/completions",
    "openai": "https://api.openai.com/v1/chat/completions",
}


def _direct_llm_call(provider: str, model: str, api_key: str, system_msg: str, user_msg: str) -> Optional[str]:
    import requests
    endpoint = _OPENAI_COMPAT.get(provider.lower())
    if not endpoint:
        log.warning("No endpoint for provider %s", provider)
        return None
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg},
        ],
        "temperature": 0.15,
        "max_tokens": 1024,
    }
    try:
        resp = requests.post(
            endpoint,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=90,
        )
        if resp.status_code >= 400:
            log.warning("LLM API %s returned %d: %s", provider, resp.status_code, resp.text[:200])
            return None
        data = resp.json()
        choices = data.get("choices") or []
        if choices:
            content = (choices[0].get("message") or {}).get("content", "")
            if content:
                return content.strip()
        return None
    except Exception as e:
        log.warning("LLM API call failed (%s): %s", provider, e)
        return None


_SYSTEM_MSG = (
    "Ты — аналитик российских новостей. Ты получаешь текст новости и должна вернуть ТОЛЬКО JSON-объект. "
    "Никакого дополнительного текста, пояснений или markdown-обёрток. Только чистый JSON."
)


def _call_cloud_llm(text: str, settings: dict = None, conn=None) -> Optional[Dict]:
    from llm.key_pool import choose_key_for_stage, bootstrap_provider_catalog, import_keys_from_file, record_key_failure, record_key_success
    from config.db_utils import get_db, load_settings
    from pathlib import Path

    if settings is None:
        settings = load_settings()

    own_conn = conn is None
    if own_conn:
        conn = get_db(settings)
    try:
        key_file = str(Path(settings.get("project_root", ".")) / "key.json")
        try:
            import_keys_from_file(conn, key_file)
        except Exception as exc:
            log.warning("import_keys_from_file failed: %s", exc)
        bootstrap_provider_catalog(conn)

        prompt = f"{_build_prompt()}\n\nТекст:\n{text[:3000]}"
        user_msg = prompt

        exclude_keys: set = set()
        max_attempts = 3

        for attempt in range(max_attempts):
            key_info = choose_key_for_stage(
                conn, stage="tag_reasoning",
                provider_priority=["deepseek", "mistral", "groq", "fireworks", "together", "huggingface", "openrouter", "perplexity", "openai"],
                exclude_key_ids=exclude_keys,
            )
            if not key_info:
                break

            model_name = key_info.get("model_name") or key_info.get("model") or ""
            if not model_name:
                exclude_keys.add(key_info["key_id"])
                continue

            output = _direct_llm_call(
                provider=key_info["provider"],
                model=model_name,
                api_key=key_info["api_key"],
                system_msg=_SYSTEM_MSG,
                user_msg=user_msg,
            )

            key_id = key_info.get("key_id")
            if output:
                raw = _extract_json_robust(output)
                if raw:
                    if key_id:
                        record_key_success(conn, key_id)
                    return _validate_and_normalize(raw)
                log.warning("No valid JSON from %s/%s (attempt %d): %s", key_info["provider"], model_name, attempt+1, output[:100])
                if key_id:
                    record_key_failure(conn, key_id, failure_kind="bad_output", error_text="non_json_response")
                    exclude_keys.add(key_id)
            else:
                if key_id:
                    record_key_failure(conn, key_id, failure_kind="empty_response", error_text="empty")
                    exclude_keys.add(key_id)

    except Exception as e:
        log.warning("Cloud LLM call failed: %s", e)
    finally:
        if own_conn:
            try:
                conn.close()
            except Exception as exc:
                log.warning("conn.close failed: %s", exc)
    return None


def _call_ollama(text: str, settings: dict = None, conn=None, **_kwargs) -> Optional[Dict]:
    """Compatibility shim for older tests/callers; LLM v2 now routes via the key pool."""
    return _call_cloud_llm(text, settings=settings, conn=conn)


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
            except Exception as exc:
                log.warning("tag_explanation insert failed: %s", exc)

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


_IMAGE_PATTERN = re.compile(r'^(фото в телеграмме\s*\(photo_\d+.*\)|\s*)$', re.IGNORECASE)


def _is_image_only(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return True
    if len(stripped) < 15 and _IMAGE_PATTERN.match(stripped):
        return True
    if re.match(r'^\s*фото в телеграмме\s*\(photo_\d+.*?\)\s*$', stripped, re.IGNORECASE):
        return True
    return False


def classify_content(settings: dict = None, batch_size: int = 100):
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    skipped_images = conn.execute(
        "UPDATE content_items SET llm_processed=1 WHERE llm_processed=0 AND length(body_text) <= 15"
    ).rowcount
    if skipped_images:
        conn.commit()
        log.info("Pre-marked %d image-only/empty items as llm_processed", skipped_images)

    rows = conn.execute(
        """
        SELECT c.id, c.body_text, c.title
        FROM content_items c
        WHERE (length(c.body_text) > 15 OR length(c.title) > 10)
          AND c.llm_processed = 0
          AND COALESCE(c.status, '') NOT IN ('suppressed_garbage', 'suppressed_promo', 'suppressed_template')
        ORDER BY c.id
        LIMIT ?
        """,
        (batch_size,),
    ).fetchall()

    if not rows:
        log.info("No content items to classify via LLM")
        conn.close()
        return

    log.info("LLM v2 classifying %d items (cloud API)", len(rows))

    classified = 0
    failed = 0
    for row in rows:
        content_id = row["id"]
        text = f"{row['title'] or ''}\n{row['body_text'] or ''}"
        if _is_image_only(text) or len(text.strip()) < 30:
            conn.execute("UPDATE content_items SET llm_processed=1 WHERE id=?", (content_id,))
            continue

        result = _call_cloud_llm(text, settings=settings, conn=conn)
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


_PROVIDER_MODEL_OVERRIDE = {
    "mistral": "mistral-small-latest",
    "groq": "qwen/qwen3-32b",
    "fireworks": "fireworks/gpt-oss-120b",
    "deepseek": "deepseek-v4-flash",
    "together": "openai/gpt-oss-120b",
    "huggingface": "Qwen/Qwen3.5-397B-A17B",
    "openrouter": "openrouter/auto",
    "perplexity": "sonar",
    "openai": "gpt-5",
}


def classify_content_parallel(settings: dict = None, batch_size: int = 500, workers: int = 10):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from llm.key_pool import list_active_keys, bootstrap_provider_catalog, import_keys_from_file, record_key_failure, record_key_success
    from pathlib import Path
    import threading

    if settings is None:
        settings = load_settings()

    conn_main = get_db(settings)

    skipped_images = conn_main.execute(
        "UPDATE content_items SET llm_processed=1 WHERE llm_processed=0 AND length(body_text) <= 15"
    ).rowcount
    if skipped_images:
        conn_main.commit()
        log.info("Pre-marked %d image-only/empty items as llm_processed", skipped_images)

    rows = conn_main.execute(
        """
        SELECT c.id, c.body_text, c.title
        FROM content_items c
        WHERE (length(c.body_text) > 15 OR length(c.title) > 10)
          AND c.llm_processed = 0
          AND COALESCE(c.status, '') NOT IN ('suppressed_garbage', 'suppressed_promo', 'suppressed_template')
        ORDER BY c.id
        LIMIT ?
        """,
        (batch_size,),
    ).fetchall()
    conn_main.close()

    if not rows:
        log.info("No content items to classify via LLM")
        return

    prompt_template = _build_prompt()
    log.info("LLM v2 parallel classifying %d items with %d workers", len(rows), workers)

    key_file = str(Path(settings.get("project_root", ".")) / "key.json")

    _key_lock = threading.Lock()
    _key_index = [0]
    _all_keys = []

    def _load_keys():
        c = get_db(settings)
        try:
            import_keys_from_file(c, key_file)
            bootstrap_provider_catalog(c)
            ks = list_active_keys(c)
            ks = [k for k in ks if k.get("model_name") and k.get("api_key")]
            return ks
        finally:
            c.close()

    _all_keys = _load_keys()
    _all_keys.sort(key=lambda k: (k.get("failure_count", 0), str(k.get("last_used_at") or "")))
    _all_keys = [k for k in _all_keys if k.get("failure_count", 0) == 0]
    _dead_providers = {"together"}
    _all_keys = [k for k in _all_keys if k["provider"] not in _dead_providers]
    log.info("Loaded %d healthy keys for parallel classification", len(_all_keys))

    classified = 0
    failed = 0
    _count_lock = threading.Lock()
    _db_lock = threading.Lock()

    def _process_item(row_tuple):
        nonlocal classified, failed
        content_id = row_tuple[0]
        text = (row_tuple[2] or '') + '\n' + (row_tuple[1] or '')
        if _is_image_only(text) or len(text.strip()) < 30:
            c = get_db(settings)
            try:
                c.execute("UPDATE content_items SET llm_processed=1 WHERE id=?", (content_id,))
                c.commit()
            finally:
                c.close()
            return True

        user_msg = prompt_template + '\n\nТекст:\n' + text[:3000]

        with _key_lock:
            if not _all_keys:
                return False
            idx = _key_index[0] % len(_all_keys)
            _key_index[0] += 1
            key_info = _all_keys[idx]

        provider = key_info["provider"]
        model_name = _PROVIDER_MODEL_OVERRIDE.get(provider, key_info.get("model_name", ""))
        api_key = key_info["api_key"]
        key_id = key_info.get("key_id")

        output = _direct_llm_call(
            provider=provider,
            model=model_name,
            api_key=api_key,
            system_msg=_SYSTEM_MSG,
            user_msg=user_msg,
        )

        if output:
            raw = _extract_json_robust(output)
            if raw:
                result = _validate_and_normalize(raw)
                with _db_lock:
                    c = get_db(settings)
                    try:
                        _store_llm_results(c, content_id, result)
                        c.execute("UPDATE content_items SET llm_processed=1 WHERE id=?", (content_id,))
                        c.commit()
                    finally:
                        c.close()
                if key_id:
                    c2 = get_db(settings)
                    try:
                        record_key_success(c2, key_id)
                    except Exception:
                        pass
                    finally:
                        c2.close()
                with _count_lock:
                    classified += 1
                return True

        if key_id:
            c2 = get_db(settings)
            try:
                record_key_failure(c2, key_id, failure_kind="bad_output", error_text="non_json_parallel")
            except Exception:
                pass
            finally:
                c2.close()

        with _count_lock:
            failed += 1
        return False

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_process_item, row): row for row in rows}
        done_count = 0
        for future in as_completed(futures):
            done_count += 1
            if done_count % 50 == 0:
                log.info("Progress: %d/%d (classified=%d, failed=%d)", done_count, len(rows), classified, failed)

    log.info("LLM v2 parallel classification done: %d classified, %d failed", classified, failed)
    return {"classified": classified, "failed": failed}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--parallel", action="store_true", default=True)
    parser.add_argument("--sequential", action="store_true", default=False)
    args = parser.parse_args()

    if args.sequential:
        result = classify_content(batch_size=args.batch)
    else:
        result = classify_content_parallel(batch_size=args.batch, workers=args.workers)
    if result:
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
