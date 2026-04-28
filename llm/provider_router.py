from __future__ import annotations

import json
import re
from typing import Any

import requests


class ProviderTaskError(RuntimeError):
    pass


JSON_BLOCK_RE = re.compile(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", re.IGNORECASE)

STAGE_SPECS: dict[str, dict[str, Any]] = {
    "clean_factual_text": {
        "goal": (
            "Convert noisy source text into one concise factual paragraph. Remove CTA, slogans, repost markers, "
            "channel tails, repeated signatures and emotional filler. Do not add new facts."
        ),
        "web_policy": (
            "Use ONLY the provided unit payload. Do NOT use external web search, outside knowledge, or facts not "
            "explicitly present in the material."
        ),
        "output_contract": (
            'Return JSON with keys: "output_text" (clean factual paragraph), '
            '"output_json" ({"cleaned_text": "...", "removed_noise": [...]}), '
            '"confidence" (0..1).'
        ),
    },
    "structured_extract": {
        "goal": (
            "Extract structured facts from the material. Keep uncertainty explicit. Do not infer unsupported facts."
        ),
        "web_policy": (
            "Extract ONLY from the provided material and explicit document anchors in the payload. Do NOT add "
            "external actors, biographies, or context that is not grounded in the unit."
        ),
        "output_contract": (
            'Return JSON with keys: "output_text", "output_json" and "confidence". '
            '"output_json" must contain arrays/objects for actors, organizations, dates, locations, actions, '
            'legal_basis, affected_groups, explicit_claims, uncertainty_markers, document_anchors.'
        ),
    },
    "event_link_hint": {
        "goal": (
            "Decide whether this unit should link to an existing event, create a new event candidate, or remain standalone."
        ),
        "web_policy": (
            "Use ONLY the provided unit packet and event candidate context. Do NOT fetch or invent outside context, "
            "historical background, or famous-event knowledge. If the packet is insufficient, prefer standalone."
        ),
        "output_contract": (
            'Return JSON with keys: "output_text", "output_json" and "confidence". '
            '"output_json" must contain action (link_existing_event|create_event_candidate|standalone), '
            'event_id if known, reason, matched_signals, and abstain_reason if linking is too weak.'
        ),
    },
    "tag_reasoning": {
        "goal": (
            "Propose conservative tags using only supported context. Generic tags require strong justification, and "
            "for official_profile/declaration/restriction_record content you should prefer abstain over inferred background."
        ),
        "web_policy": (
            "Use ONLY the provided raw/derived/event context in the payload. Do NOT add tags from outside knowledge, "
            "biographical background, prior convictions, affiliations, or political context unless they are explicit in the packet."
        ),
        "output_contract": (
            'Return JSON with keys: "output_text", "output_json" and "confidence". '
            '"output_json" should contain tags, abstain_tags, rationale, signal_layers, and abstain_reason. '
            'Only emit a tag when at least one explicit supported signal is present; generic tags require two independent signals.'
        ),
    },
    "relation_reasoning": {
        "goal": (
            "Propose relation-support hints and bridge types without asserting unsupported relations as facts."
        ),
        "web_policy": (
            "You MAY use web-grounded official or documentary context to validate bridge types, but do NOT introduce "
            "unrelated actors or unsupported claims."
        ),
        "output_contract": (
            'Return JSON with keys: "output_text", "output_json" and "confidence". '
            '"output_json" should contain bridge_types, support_hints, official_bridges, and blocker_hints.'
        ),
    },
    "event_synthesis": {
        "goal": (
            "Synthesize a canonical event summary, ordered timeline, and participant roles from the provided packet."
        ),
        "web_policy": (
            "Synthesize ONLY from the provided canonical event packet. Do NOT add outside facts."
        ),
        "output_contract": (
            'Return JSON with keys: "output_text", "output_json" and "confidence". '
            '"output_json" must contain summary_short, summary_long, timeline, participants, open_questions.'
        ),
    },
}


def _post_json(url: str, *, headers: dict[str, str], payload: dict[str, Any], timeout: int = 120) -> dict[str, Any]:
    response = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if response.status_code >= 400:
        raise ProviderTaskError(f"{response.status_code} {response.text[:500]}")
    try:
        return response.json()
    except json.JSONDecodeError as error:  # pragma: no cover - defensive
        raise ProviderTaskError(f"invalid_json_response:{error}") from error


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def _task_stage(task: dict[str, Any]) -> str:
    return str(task.get("stage") or "").strip() or "unknown"


def _stage_prompt(task: dict[str, Any]) -> tuple[str, str]:
    stage = _task_stage(task)
    spec = STAGE_SPECS.get(
        stage,
        {
            "goal": "Process the task conservatively and do not invent facts.",
            "web_policy": "Use only the provided payload unless the stage explicitly permits web-grounded support.",
            "output_contract": 'Return JSON with keys: "output_text", "output_json", "confidence".',
        },
    )
    system_prompt = (
        "You are an evidence-platform worker.\n"
        "Rules:\n"
        "- Do not invent facts.\n"
        "- Preserve uncertainty.\n"
        f"- {spec['web_policy']}\n"
        "- Return ONLY valid JSON, no prose outside JSON.\n\n"
        f"Stage: {stage}\n"
        f"Goal: {spec['goal']}\n"
        f"Output contract: {spec['output_contract']}"
    )
    user_prompt = _json_dumps(task)
    return system_prompt, user_prompt


def _stage_allows_web(stage: str) -> bool:
    return stage in {"relation_reasoning"}


def _extract_chat_text(data: dict[str, Any]) -> str:
    if isinstance(data.get("output_text"), str) and data.get("output_text"):
        return str(data["output_text"])
    choices = data.get("choices") or []
    if choices:
        message = (choices[0] or {}).get("message") or {}
        content = message.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for chunk in content:
                if isinstance(chunk, dict):
                    text = chunk.get("text") or chunk.get("content")
                    if text:
                        parts.append(str(text))
                elif isinstance(chunk, str):
                    parts.append(chunk)
            return "\n".join(part for part in parts if part).strip()
    output = data.get("output") or []
    if isinstance(output, list):
        parts: list[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            for content in item.get("content") or []:
                if isinstance(content, dict):
                    text = content.get("text") or content.get("output_text")
                    if text:
                        parts.append(str(text))
        return "\n".join(part for part in parts if part).strip()
    return ""


def _extract_json_object(text: str) -> dict[str, Any] | None:
    candidate = str(text or "").strip()
    if not candidate:
        return None
    try:
        parsed = json.loads(candidate)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass
    fenced = JSON_BLOCK_RE.search(candidate)
    if fenced:
        try:
            parsed = json.loads(fenced.group(1))
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    start = candidate.find("{")
    end = candidate.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(candidate[start : end + 1])
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def _stage_fallback_output(stage: str, text: str) -> dict[str, Any]:
    text = str(text or "").strip()
    if stage == "clean_factual_text":
        return {"cleaned_text": text, "removed_noise": []}
    if stage == "structured_extract":
        return {
            "actors": [],
            "organizations": [],
            "dates": [],
            "locations": [],
            "actions": [],
            "legal_basis": [],
            "affected_groups": [],
            "explicit_claims": [],
            "uncertainty_markers": [],
            "document_anchors": [],
            "raw_text": text,
        }
    if stage == "event_link_hint":
        return {"action": "standalone", "reason": text}
    if stage == "tag_reasoning":
        return {"tags": [], "abstain_tags": [], "rationale": text, "signal_layers": []}
    if stage == "relation_reasoning":
        return {"bridge_types": [], "support_hints": [], "official_bridges": [], "blocker_hints": [], "raw_text": text}
    if stage == "event_synthesis":
        return {
            "summary_short": text[:280],
            "summary_long": text,
            "timeline": [],
            "participants": [],
            "open_questions": [],
        }
    return {"response": text}


def _coerce_stage_result(stage: str, raw_text: str, parsed_json: dict[str, Any] | None) -> tuple[str, dict[str, Any], float]:
    raw_text = str(raw_text or "").strip()
    envelope = parsed_json or {}
    output_text = envelope.get("output_text")
    output_json = envelope.get("output_json")
    confidence = envelope.get("confidence")

    if not isinstance(output_text, str) or not output_text.strip():
        if stage == "event_synthesis" and isinstance(output_json, dict):
            output_text = str(output_json.get("summary_short") or output_json.get("summary_long") or raw_text)
        else:
            output_text = raw_text

    if not isinstance(output_json, dict):
        output_json = _stage_fallback_output(stage, raw_text)
    if confidence is None:
        confidence = 0.55 if parsed_json else 0.45
    try:
        confidence_value = max(0.0, min(1.0, float(confidence)))
    except (TypeError, ValueError):
        confidence_value = 0.45
    return output_text.strip(), output_json, confidence_value


def _normalize_result(provider: str, model: str, stage: str, data: dict[str, Any]) -> dict[str, Any]:
    raw_text = _extract_chat_text(data)
    parsed = _extract_json_object(raw_text)
    output_text, output_json, confidence = _coerce_stage_result(stage, raw_text, parsed)
    return {
        "provider": provider,
        "model": model,
        "output_text": output_text,
        "output_json": output_json,
        "confidence": confidence,
        "raw_response": data,
    }


def _openai_run(model: str, api_key: str, task: dict[str, Any]) -> dict[str, Any]:
    system_prompt, user_prompt = _stage_prompt(task)
    stage = _task_stage(task)
    payload = {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [{"type": "input_text", "text": system_prompt}],
            },
            {
                "role": "user",
                "content": [{"type": "input_text", "text": user_prompt}],
            },
        ],
    }
    if _stage_allows_web(stage):
        payload["tools"] = [{"type": "web_search"}]
    data = _post_json(
        "https://api.openai.com/v1/responses",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        payload=payload,
    )
    return _normalize_result("openai", model, stage, data)


def _perplexity_run(model: str, api_key: str, task: dict[str, Any]) -> dict[str, Any]:
    system_prompt, user_prompt = _stage_prompt(task)
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    data = _post_json(
        "https://api.perplexity.ai/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        payload=payload,
    )
    return _normalize_result("perplexity", model, _task_stage(task), data)


def _groq_run(model: str, api_key: str, task: dict[str, Any]) -> dict[str, Any]:
    system_prompt, user_prompt = _stage_prompt(task)
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    data = _post_json(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        payload=payload,
    )
    return _normalize_result("groq", model, _task_stage(task), data)


def _mistral_run(model: str, api_key: str, task: dict[str, Any]) -> dict[str, Any]:
    system_prompt, user_prompt = _stage_prompt(task)
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    data = _post_json(
        "https://api.mistral.ai/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        payload=payload,
    )
    return _normalize_result("mistral", model, _task_stage(task), data)


def _openrouter_run(model: str, api_key: str, task: dict[str, Any]) -> dict[str, Any]:
    system_prompt, user_prompt = _stage_prompt(task)
    stage = _task_stage(task)
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    if _stage_allows_web(stage):
        payload["plugins"] = [{"id": "web"}]
    data = _post_json(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        payload=payload,
    )
    return _normalize_result("openrouter", model, stage, data)


def run_ai_task(*, conn: Any = None, provider: str, model: str, api_key: str, task: dict[str, Any]) -> dict[str, Any]:
    provider_name = str(provider or "").strip().lower()
    if provider_name == "openai":
        return _openai_run(model, api_key, task)
    if provider_name == "perplexity":
        return _perplexity_run(model, api_key, task)
    if provider_name == "groq":
        return _groq_run(model, api_key, task)
    if provider_name == "mistral":
        return _mistral_run(model, api_key, task)
    if provider_name == "openrouter":
        return _openrouter_run(model, api_key, task)
    raise ProviderTaskError(f"unsupported_provider:{provider_name}")
