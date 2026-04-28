from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from config.db_utils import PROJECT_ROOT, get_db
from llm.key_pool import (
    bootstrap_provider_catalog,
    choose_key_for_stage,
    import_keys_from_file,
    record_key_failure,
    record_key_success,
    reactivate_recoverable_keys,
)
from llm.provider_router import run_ai_task
from enrichment.common import ensure_review_task
from runtime.contracts import now_iso
from runtime.state import finish_pipeline_run, set_runtime_metadata, start_pipeline_run

PROMPT_VERSIONS: dict[str, str] = {
    "clean_factual_text": "ai-sweep-v2-cleaner",
    "structured_extract": "ai-sweep-v2-extract",
    "event_link_hint": "ai-sweep-v2-event-link",
    "tag_reasoning": "ai-sweep-v2-tags",
    "relation_reasoning": "ai-sweep-v1-relations",
    "event_synthesis": "ai-sweep-v1-synthesis",
}
DERIVATION_STAGES = {
    "clean_factual_text",
    "structured_extract",
    "event_summary_fragment",
    "tag_reasoning",
    "relation_reasoning",
}
STAGES_BY_KIND: dict[str, tuple[str, ...]] = {
    "content_cluster": (
        "clean_factual_text",
        "structured_extract",
        "event_link_hint",
        "tag_reasoning",
        "relation_reasoning",
    ),
    "content_item": (
        "clean_factual_text",
        "structured_extract",
        "event_link_hint",
        "tag_reasoning",
        "relation_reasoning",
    ),
    "event": ("event_synthesis",),
    "review_task": ("relation_reasoning",),
}
STAGE_ORDER = {stage: index for index, stage in enumerate(
    (
        "clean_factual_text",
        "structured_extract",
        "event_link_hint",
        "tag_reasoning",
        "relation_reasoning",
        "event_synthesis",
    )
)}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _json_loads(value: Any, default: Any = None) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _normalize_space(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _hash_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def _ai_settings(settings: dict[str, Any]) -> dict[str, Any]:
    cfg = dict(settings.get("ai_sweep") or {})
    cfg.setdefault("key_file", str(Path(settings.get("project_root") or PROJECT_ROOT) / "key.json"))
    cfg.setdefault("default_worker_count", 12)
    cfg.setdefault("min_parallel_workers", 10)
    cfg.setdefault("max_parallel_workers", 24)
    cfg.setdefault("dead_key_threshold", 3)
    cfg.setdefault("max_units_per_run", 0)
    cfg.setdefault("provider_priority", ["mistral", "perplexity", "groq", "openrouter", "openai"])
    cfg.setdefault("mode", "pilot")
    cfg.setdefault("campaign_seed", "ai-pilot-2026-04-27")
    cfg.setdefault("campaign_key", f"pilot:{cfg['campaign_seed']}")
    cfg.setdefault("provider_mode", "conservative")
    cfg.setdefault("max_attempts_per_work_item", 6)
    cfg.setdefault("max_failures_per_provider_per_item", 2)
    cfg.setdefault("pilot_target_units", 232)
    cfg.setdefault(
        "pilot_distribution",
        {
            "content_item": 120,
            "content_cluster": 40,
            "event": 40,
            "review_task": 32,
        },
    )
    cfg.setdefault("prompt_versions", {})
    return cfg


def _provider_priority(settings: dict[str, Any]) -> list[str]:
    return list(_ai_settings(settings).get("provider_priority") or ["mistral", "perplexity", "groq", "openrouter", "openai"])


def _prompt_version_for_stage(stage: str, settings: dict[str, Any]) -> str:
    overrides = dict(_ai_settings(settings).get("prompt_versions") or {})
    return str(overrides.get(stage) or PROMPT_VERSIONS.get(stage) or "ai-sweep-v1")


def _stage_provider_priority(stage: str, settings: dict[str, Any]) -> list[str]:
    stage_specific = {
        "clean_factual_text": ["mistral", "openrouter", "groq", "perplexity", "openai"],
        "structured_extract": ["mistral", "openrouter", "groq", "perplexity", "openai"],
        "event_link_hint": ["mistral", "groq", "openrouter", "openai", "perplexity"],
        "tag_reasoning": ["mistral", "groq", "openrouter", "openai", "perplexity"],
        "relation_reasoning": ["perplexity", "openai", "openrouter", "groq", "mistral"],
        "event_synthesis": ["openai", "perplexity", "openrouter", "mistral", "groq"],
    }
    return list(stage_specific.get(stage) or _provider_priority(settings))


def _bootstrap_key_pool(conn: sqlite3.Connection, settings: dict[str, Any]) -> dict[str, Any]:
    catalog = bootstrap_provider_catalog(conn)
    key_file = Path(_ai_settings(settings).get("key_file") or "")
    imported = {"inserted": 0, "updated": 0, "skipped": 0}
    if key_file.exists():
        imported = import_keys_from_file(conn, key_file)
    recovered = reactivate_recoverable_keys(conn)
    return {"catalog": catalog, "keys": imported, "recovered": recovered, "key_file": str(key_file)}


def _cluster_units(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not (_table_exists(conn, "content_clusters") and _table_exists(conn, "content_cluster_items")):
        return []
    rows = conn.execute(
        """
        SELECT
            cc.id,
            cc.cluster_key,
            cc.cluster_type,
            cc.canonical_content_id,
            cc.canonical_title,
            ci.content_type,
            s.category,
            cc.first_seen_at,
            cc.last_seen_at,
            cc.item_count
        FROM content_clusters cc
        LEFT JOIN content_items ci ON ci.id = cc.canonical_content_id
        LEFT JOIN sources s ON s.id = ci.source_id
        WHERE COALESCE(cc.status, 'active')='active'
        ORDER BY COALESCE(cc.first_seen_at, cc.last_seen_at, ''), cc.id
        """
    ).fetchall()
    units: list[dict[str, Any]] = []
    for row in rows:
        cluster_key = row[1] or f"cluster:{row[0]}"
        units.append(
            {
                "unit_kind": "content_cluster",
                "unit_key": str(cluster_key),
                "cluster_id": int(row[0]),
                "cluster_key": str(cluster_key),
                "cluster_type": row[2],
                "canonical_content_id": row[3],
                "canonical_title": row[4],
                "content_type": row[5],
                "source_category": row[6],
                "first_seen_at": row[7],
                "last_seen_at": row[8],
                "item_count": int(row[9] or 0),
            }
        )
    return units


def _singleton_content_units(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    join = ""
    where = ""
    if _table_exists(conn, "content_cluster_items"):
        join = "LEFT JOIN content_cluster_items cci ON cci.content_item_id = ci.id"
        where = "AND cci.content_item_id IS NULL"
    rows = conn.execute(
        f"""
        SELECT
            ci.id,
            ci.source_id,
            ci.content_type,
            ci.title,
            ci.published_at,
            ci.status
        FROM content_items ci
        {join}
        WHERE 1=1
        {where}
        ORDER BY COALESCE(ci.published_at, ci.collected_at, ''), ci.id
        """
    ).fetchall()
    return [
        {
            "unit_kind": "content_item",
            "unit_key": f"content:{int(row[0])}",
            "content_item_id": int(row[0]),
            "canonical_content_id": int(row[0]),
            "source_id": row[1],
            "content_type": row[2],
            "title": row[3],
            "published_at": row[4],
            "status": row[5],
        }
        for row in rows
    ]


def _event_units(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "events"):
        return []
    rows = conn.execute(
        """
        SELECT id, canonical_title, event_type, status, event_date_start, event_date_end
        FROM events
        WHERE COALESCE(status, 'active') <> 'archived'
        ORDER BY COALESCE(event_date_start, first_observed_at, created_at, ''), id
        """
    ).fetchall()
    return [
        {
            "unit_kind": "event",
            "unit_key": f"event:{int(row[0])}",
            "event_id": int(row[0]),
            "canonical_title": row[1],
            "event_type": row[2],
            "status": row[3],
            "event_date_start": row[4],
            "event_date_end": row[5],
        }
        for row in rows
    ]


def _review_units(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "review_tasks"):
        return []
    rows = conn.execute(
        """
        SELECT id, queue_key, subject_type, subject_id, candidate_payload, suggested_action, confidence, machine_reason
        FROM review_tasks
        WHERE COALESCE(status, 'open')='open'
        ORDER BY created_at, id
        """
    ).fetchall()
    return [
        {
            "unit_kind": "review_task",
            "unit_key": f"review:{int(row[0])}",
            "review_task_id": int(row[0]),
            "queue_key": row[1],
            "subject_type": row[2],
            "subject_id": row[3],
            "candidate_payload": row[4],
            "suggested_action": row[5],
            "confidence": row[6],
            "machine_reason": row[7],
        }
        for row in rows
    ]


def canonicalize_units(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    units = []
    units.extend(_cluster_units(conn))
    units.extend(_singleton_content_units(conn))
    units.extend(_event_units(conn))
    units.extend(_review_units(conn))
    return units


def _unit_sample_bucket(unit: dict[str, Any]) -> str:
    kind = str(unit.get("unit_kind") or "")
    return kind if kind in {"content_item", "content_cluster", "event", "review_task"} else "other"


def _unit_priority(unit: dict[str, Any]) -> tuple[int, int]:
    bucket = _unit_sample_bucket(unit)
    content_type = str(unit.get("content_type") or "").strip().lower()
    source_category = str(unit.get("source_category") or unit.get("queue_key") or "").strip().lower()
    if bucket == "review_task":
        queue_order = {"relations": 0, "content_duplicates": 1, "sources": 2}
        return (queue_order.get(source_category, 3), 0)
    if content_type in {"official_profile", "declaration", "restriction_record"}:
        return (0, 0)
    if source_category in {"telegram", "rss", "media", "news"}:
        return (1, 0)
    return (2, 0)


def _stable_rank(seed: str, unit_key: str) -> str:
    return hashlib.sha256(f"{seed}:{unit_key}".encode("utf-8")).hexdigest()


def _build_campaign_selection(units: list[dict[str, Any]], settings: dict[str, Any]) -> list[dict[str, Any]]:
    cfg = _ai_settings(settings)
    seed = str(cfg.get("campaign_seed") or "ai-pilot")
    target = int(cfg.get("pilot_target_units") or 232)
    quotas = {str(key): int(value or 0) for key, value in dict(cfg.get("pilot_distribution") or {}).items()}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for unit in units:
        bucket = _unit_sample_bucket(unit)
        grouped.setdefault(bucket, []).append(unit)
    selected: list[dict[str, Any]] = []
    used: set[tuple[str, str]] = set()

    def _ordered(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(items, key=lambda unit: (_unit_priority(unit), _stable_rank(seed, str(unit["unit_key"])), str(unit["unit_key"])))

    for bucket, quota in quotas.items():
        for unit in _ordered(grouped.get(bucket, []))[: max(0, quota)]:
            key = (str(unit["unit_kind"]), str(unit["unit_key"]))
            if key in used:
                continue
            used.add(key)
            selected.append({"unit_kind": key[0], "unit_key": key[1], "sample_bucket": bucket})

    if len(selected) < target:
        remaining: list[dict[str, Any]] = []
        for bucket, bucket_units in grouped.items():
            for unit in bucket_units:
                key = (str(unit["unit_kind"]), str(unit["unit_key"]))
                if key in used:
                    continue
                remaining.append(unit)
        for unit in _ordered(remaining)[: max(0, target - len(selected))]:
            key = (str(unit["unit_kind"]), str(unit["unit_key"]))
            if key in used:
                continue
            used.add(key)
            selected.append({"unit_kind": key[0], "unit_key": key[1], "sample_bucket": _unit_sample_bucket(unit)})

    return selected[:target]


def ensure_ai_sweep_campaign(conn: sqlite3.Connection, settings: dict[str, Any], units: list[dict[str, Any]]) -> dict[str, Any]:
    cfg = _ai_settings(settings)
    campaign_key = str(cfg.get("campaign_key") or f"pilot:{cfg.get('campaign_seed')}")
    seed = str(cfg.get("campaign_seed") or campaign_key)
    mode = str(cfg.get("mode") or "pilot")
    provider_mode = str(cfg.get("provider_mode") or "conservative")
    row = conn.execute(
        """
        SELECT id, selection_json, prompt_versions_json, sample_size, status
        FROM ai_sweep_campaigns
        WHERE campaign_key=?
        LIMIT 1
        """,
        (campaign_key,),
    ).fetchone()
    selection: list[dict[str, Any]]
    if row and row[1]:
        selection = json.loads(row[1] or "[]")
        conn.execute(
            """
            UPDATE ai_sweep_campaigns
            SET prompt_versions_json=?, updated_at=?
            WHERE id=?
            """,
            (_json_dumps({stage: _prompt_version_for_stage(stage, settings) for stage in STAGE_ORDER}), now_iso(), int(row[0])),
        )
        conn.commit()
        return {
            "campaign_id": int(row[0]),
            "campaign_key": campaign_key,
            "campaign_seed": seed,
            "selection": selection,
            "provider_mode": provider_mode,
            "sample_size": int(row[3] or len(selection)),
        }

    selection = _build_campaign_selection(units, settings)
    prompt_versions = {stage: _prompt_version_for_stage(stage, settings) for stage in STAGE_ORDER}
    if row:
        conn.execute(
            """
            UPDATE ai_sweep_campaigns
            SET campaign_seed=?, mode=?, provider_mode=?, sample_size=?, selection_json=?, prompt_versions_json=?, updated_at=?
            WHERE id=?
            """,
            (seed, mode, provider_mode, len(selection), _json_dumps(selection), _json_dumps(prompt_versions), now_iso(), int(row[0])),
        )
        campaign_id = int(row[0])
    else:
        cursor = conn.execute(
            """
            INSERT INTO ai_sweep_campaigns(
                campaign_key, campaign_seed, mode, provider_mode, sample_size, selection_json, prompt_versions_json,
                status, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                campaign_key,
                seed,
                mode,
                provider_mode,
                len(selection),
                _json_dumps(selection),
                _json_dumps(prompt_versions),
                "planned",
                now_iso(),
                now_iso(),
            ),
        )
        campaign_id = int(cursor.lastrowid)
    conn.commit()
    return {
        "campaign_id": campaign_id,
        "campaign_key": campaign_key,
        "campaign_seed": seed,
        "selection": selection,
        "provider_mode": provider_mode,
        "sample_size": len(selection),
    }


def _build_unit_context(conn: sqlite3.Connection, unit: dict[str, Any]) -> dict[str, Any]:
    unit_kind = unit["unit_kind"]
    if unit_kind == "content_cluster":
        content_id = int(unit["canonical_content_id"])
        content_row = conn.execute(
            """
            SELECT ci.id, ci.source_id, ci.external_id, ci.content_type, ci.title, ci.body_text,
                   ci.published_at, ci.collected_at, ci.url, ci.status, s.name, s.category
            FROM content_items ci
            JOIN sources s ON s.id = ci.source_id
            WHERE ci.id=?
            """,
            (content_id,),
        ).fetchone()
        items = conn.execute(
            """
            SELECT ci.id, ci.title, ci.content_type, ci.published_at, ci.url, s.category AS source_category
            FROM content_cluster_items cci
            JOIN content_items ci ON ci.id = cci.content_item_id
            JOIN sources s ON s.id = ci.source_id
            WHERE cci.cluster_id=?
            ORDER BY COALESCE(ci.published_at, ci.collected_at, ''), ci.id
            """,
            (int(unit["cluster_id"]),),
        ).fetchall()
        return {
            "content_item_id": content_id,
            "content_row": dict(content_row) if content_row else {},
            "cluster_items": [dict(item) for item in items],
        }
    if unit_kind == "content_item":
        content_id = int(unit["content_item_id"])
        content_row = conn.execute(
            """
            SELECT ci.id, ci.source_id, ci.external_id, ci.content_type, ci.title, ci.body_text,
                   ci.published_at, ci.collected_at, ci.url, ci.status, s.name, s.category
            FROM content_items ci
            JOIN sources s ON s.id = ci.source_id
            WHERE ci.id=?
            """,
            (content_id,),
        ).fetchone()
        return {"content_item_id": content_id, "content_row": dict(content_row) if content_row else {}}
    if unit_kind == "event":
        event_id = int(unit["event_id"])
        event_row = conn.execute(
            """
            SELECT id, canonical_title, event_type, summary_short, summary_long, status,
                   event_date_start, event_date_end, first_observed_at, last_observed_at
            FROM events
            WHERE id=?
            """,
            (event_id,),
        ).fetchone()
        item_rows = []
        if _table_exists(conn, "event_items"):
            item_rows = conn.execute(
                """
                SELECT ei.content_item_id, ei.content_cluster_id, ei.item_role, ei.source_strength
                FROM event_items ei
                WHERE ei.event_id=?
                ORDER BY ei.id
                """,
                (event_id,),
            ).fetchall()
        return {"event_row": dict(event_row) if event_row else {}, "event_items": [dict(row) for row in item_rows]}
    return {}


GENERIC_TAG_NAMES = {
    "technology",
    "international",
    "regional",
    "ес",
    "ии",
    "искусственный интеллект",
    "технологии",
}
STRICT_CONTENT_TYPES = {"official_profile", "declaration", "restriction_record"}


def _canonical_unit_counts(conn: sqlite3.Connection) -> dict[str, int]:
    units = canonicalize_units(conn)
    counts = {
        "content_clusters": sum(1 for unit in units if unit["unit_kind"] == "content_cluster"),
        "singleton_content": sum(1 for unit in units if unit["unit_kind"] == "content_item"),
        "events": sum(1 for unit in units if unit["unit_kind"] == "event"),
        "open_review_tasks": sum(1 for unit in units if unit["unit_kind"] == "review_task"),
    }
    counts["canonical_units_total"] = sum(counts.values())
    return counts


def _selected_content_ids(unit_index: dict[tuple[str, str], dict[str, Any]], selection: list[dict[str, Any]]) -> set[int]:
    ids: set[int] = set()
    for entry in selection:
        unit = unit_index.get((str(entry["unit_kind"]), str(entry["unit_key"])))
        if not unit:
            continue
        content_id = unit.get("canonical_content_id") or unit.get("content_item_id")
        if content_id:
            ids.add(int(content_id))
    return ids


def _sample_unit_snapshot(
    conn: sqlite3.Connection,
    unit: dict[str, Any],
    *,
    sample_bucket: str,
) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "unit_kind": unit["unit_kind"],
        "unit_key": unit["unit_key"],
        "sample_bucket": sample_bucket,
    }
    if unit["unit_kind"] in {"content_item", "content_cluster"}:
        content_id = int(unit.get("canonical_content_id") or unit.get("content_item_id") or 0)
        row = conn.execute(
            """
            SELECT ci.id, ci.content_type, ci.title, ci.body_text, ci.published_at, ci.url, s.category AS source_category
            FROM content_items ci
            JOIN sources s ON s.id = ci.source_id
            WHERE ci.id=?
            """,
            (content_id,),
        ).fetchone()
        tags = conn.execute(
            """
            SELECT tag_name, confidence
            FROM content_tags
            WHERE content_item_id=?
            ORDER BY confidence DESC, id ASC
            LIMIT 12
            """,
            (content_id,),
        ).fetchall()
        derivations = conn.execute(
            """
            SELECT derivation_type, output_text, output_json, confidence, model_provider, model_name, prompt_version
            FROM content_derivations
            WHERE content_item_id=?
            ORDER BY updated_at DESC, id DESC
            """,
            (content_id,),
        ).fetchall()
        event_hints = conn.execute(
            """
            SELECT suggested_event_id, candidate_state, confidence
            FROM event_candidates
            WHERE unit_kind=? AND unit_key=?
            ORDER BY updated_at DESC, id DESC
            LIMIT 5
            """,
            (unit["unit_kind"], unit["unit_key"]),
        ).fetchall()
        snapshot.update(
            {
                "content_id": content_id,
                "content_type": row["content_type"] if row else None,
                "source_category": row["source_category"] if row else None,
                "title": row["title"] if row else None,
                "raw_excerpt": (row["body_text"] or "")[:500] if row else "",
                "tags": [dict(tag) for tag in tags],
                "derivations": [
                    {
                        "derivation_type": drv["derivation_type"],
                        "confidence": float(drv["confidence"] or 0),
                        "provider": drv["model_provider"],
                        "model": drv["model_name"],
                        "prompt_version": drv["prompt_version"],
                        "output_text": (drv["output_text"] or "")[:500],
                        "output_json": json.loads(drv["output_json"] or "{}") if drv["output_json"] else {},
                    }
                    for drv in derivations
                ],
                "current_derivations": current_derivations_for_content(conn, int(content_id)),
                "event_hints": [dict(hint) for hint in event_hints],
            }
        )
        return snapshot
    if unit["unit_kind"] == "event":
        event_id = int(unit["event_id"])
        event_row = conn.execute(
            """
            SELECT canonical_title, event_type, summary_short, summary_long, event_date_start, event_date_end
            FROM events WHERE id=?
            """,
            (event_id,),
        ).fetchone()
        snapshot.update(
            {
                "event_id": event_id,
                "canonical_title": event_row["canonical_title"] if event_row else None,
                "event_type": event_row["event_type"] if event_row else None,
                "summary_short": event_row["summary_short"] if event_row else None,
                "summary_long": (event_row["summary_long"] or "")[:800] if event_row else None,
                "timeline_count": conn.execute("SELECT COUNT(*) FROM event_timeline WHERE event_id=?", (event_id,)).fetchone()[0],
                "entity_count": conn.execute("SELECT COUNT(*) FROM event_entities WHERE event_id=?", (event_id,)).fetchone()[0],
                "fact_count": conn.execute("SELECT COUNT(*) FROM event_facts WHERE event_id=?", (event_id,)).fetchone()[0],
            }
        )
        return snapshot
    if unit["unit_kind"] == "review_task":
        row = conn.execute(
            """
            SELECT queue_key, subject_type, subject_id, suggested_action, confidence, machine_reason, candidate_payload
            FROM review_tasks
            WHERE id=?
            """,
            (int(unit["review_task_id"]),),
        ).fetchone()
        snapshot.update(dict(row) if row else {})
    return snapshot


def build_ai_sweep_pilot_report(
    settings: dict[str, Any],
    *,
    report_path: str | Path | None = None,
    sample_limit: int = 30,
) -> dict[str, Any]:
    conn = get_db(settings)
    conn.row_factory = sqlite3.Row
    try:
        units = canonicalize_units(conn)
        unit_index = {(unit["unit_kind"], unit["unit_key"]): unit for unit in units}
        campaign = ensure_ai_sweep_campaign(conn, settings, units)
        selection = list(campaign["selection"])
        selected_content_ids = _selected_content_ids(unit_index, selection)
        derivation_counts = dict(
            conn.execute(
                """
                SELECT derivation_type, COUNT(*)
                FROM content_derivations
                WHERE content_item_id IN ({})
                GROUP BY derivation_type
                """.format(",".join("?" for _ in selected_content_ids) or "NULL"),
                tuple(selected_content_ids),
            ).fetchall()
        ) if selected_content_ids else {}
        strict_generic_count = 0
        if selected_content_ids:
            strict_generic_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM content_tags ct
                JOIN content_items ci ON ci.id = ct.content_item_id
                WHERE ct.content_item_id IN ({})
                  AND LOWER(COALESCE(ci.content_type, '')) IN ('official_profile', 'declaration', 'restriction_record')
                  AND LOWER(COALESCE(ct.tag_name, '')) IN ({})
                """.format(",".join("?" for _ in selected_content_ids), ",".join("?" for _ in GENERIC_TAG_NAMES)),
                tuple(selected_content_ids) + tuple(sorted(GENERIC_TAG_NAMES)),
            ).fetchone()[0]
        sample_snapshot = []
        for entry in selection[: max(1, int(sample_limit))]:
            unit = unit_index.get((str(entry["unit_kind"]), str(entry["unit_key"])))
            if not unit:
                continue
            sample_snapshot.append(
                _sample_unit_snapshot(conn, unit, sample_bucket=str(entry.get("sample_bucket") or entry["unit_kind"]))
            )
        report = {
            "generated_at": now_iso(),
            "campaign_id": int(campaign["campaign_id"]),
            "campaign_key": campaign["campaign_key"],
            "campaign_seed": campaign["campaign_seed"],
            "provider_mode": campaign["provider_mode"],
            "canonical_unit_counts": _canonical_unit_counts(conn),
            "selected_counts": {
                bucket: sum(1 for entry in selection if str(entry.get("sample_bucket")) == bucket)
                for bucket in ("content_item", "content_cluster", "event", "review_task")
            },
            "selected_content_ids": len(selected_content_ids),
            "derivations_by_type": derivation_counts,
            "event_candidates_selected": conn.execute(
                """
                SELECT COUNT(*)
                FROM event_candidates
                WHERE (unit_kind, unit_key) IN ({})
                """.format(",".join(["(?, ?)"] * len(selection)) or "(NULL, NULL)"),
                tuple(v for entry in selection for v in (entry["unit_kind"], entry["unit_key"])) if selection else (),
            ).fetchone()[0] if selection else 0,
            "strict_generic_tag_count": int(strict_generic_count or 0),
            "sample_snapshot": sample_snapshot,
        }
        if report_path:
            target = Path(report_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(_json_dumps(report), encoding="utf-8")
        return report
    finally:
        conn.close()


def build_ai_sweep_pilot_diff(
    before: dict[str, Any],
    after: dict[str, Any],
    *,
    report_path: str | Path | None = None,
) -> dict[str, Any]:
    diff = {
        "generated_at": now_iso(),
        "campaign_key": after.get("campaign_key") or before.get("campaign_key"),
        "selected_counts": after.get("selected_counts", {}),
        "strict_generic_tag_count_before": int(before.get("strict_generic_tag_count") or 0),
        "strict_generic_tag_count_after": int(after.get("strict_generic_tag_count") or 0),
        "strict_generic_tag_delta": int(after.get("strict_generic_tag_count") or 0) - int(before.get("strict_generic_tag_count") or 0),
        "derivations_by_type_before": before.get("derivations_by_type", {}),
        "derivations_by_type_after": after.get("derivations_by_type", {}),
        "event_candidates_selected_before": int(before.get("event_candidates_selected") or 0),
        "event_candidates_selected_after": int(after.get("event_candidates_selected") or 0),
    }
    if report_path:
        target = Path(report_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(_json_dumps(diff), encoding="utf-8")
    return diff


def _sample_derivation_index(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for row in snapshot.get("sample_snapshot", []) or []:
        if isinstance(row, dict) and row.get("unit_key"):
            index[str(row["unit_key"])] = row
    return index


def current_derivations_for_content(conn: sqlite3.Connection, content_item_id: int) -> dict[str, dict[str, Any]]:
    """Return the active derived output per derivation type without deleting history."""
    rows = conn.execute(
        """
        SELECT
            id, derivation_type, output_text, output_json, confidence, model_provider, model_name,
            prompt_version, input_hash, campaign_id, work_item_id, is_current, updated_at
        FROM content_derivations
        WHERE content_item_id=? AND COALESCE(status, 'ready')='ready'
        ORDER BY
            COALESCE(is_current, 0) DESC,
            updated_at DESC,
            prompt_version DESC,
            id DESC
        """,
        (int(content_item_id),),
    ).fetchall()
    current: dict[str, dict[str, Any]] = {}
    for row in rows:
        derivation_type = str(row["derivation_type"] if isinstance(row, sqlite3.Row) else row[1])
        if derivation_type in current:
            continue
        output_json_raw = row["output_json"] if isinstance(row, sqlite3.Row) else row[3]
        current[derivation_type] = {
            "id": int(row["id"] if isinstance(row, sqlite3.Row) else row[0]),
            "derivation_type": derivation_type,
            "output_text": row["output_text"] if isinstance(row, sqlite3.Row) else row[2],
            "output_json": _json_loads(output_json_raw, {}),
            "confidence": float((row["confidence"] if isinstance(row, sqlite3.Row) else row[4]) or 0),
            "provider": row["model_provider"] if isinstance(row, sqlite3.Row) else row[5],
            "model": row["model_name"] if isinstance(row, sqlite3.Row) else row[6],
            "prompt_version": row["prompt_version"] if isinstance(row, sqlite3.Row) else row[7],
            "input_hash": row["input_hash"] if isinstance(row, sqlite3.Row) else row[8],
            "campaign_id": row["campaign_id"] if isinstance(row, sqlite3.Row) else row[9],
            "work_item_id": row["work_item_id"] if isinstance(row, sqlite3.Row) else row[10],
            "is_current": bool(row["is_current"] if isinstance(row, sqlite3.Row) else row[11]),
        }
    return current


def _derivation_brief(row: dict[str, Any], derivation_type: str) -> dict[str, Any] | None:
    current = row.get("current_derivations") or {}
    if isinstance(current, dict) and isinstance(current.get(derivation_type), dict):
        latest = current[derivation_type]
        return {
            "provider": latest.get("provider"),
            "prompt_version": latest.get("prompt_version"),
            "output_text": str(latest.get("output_text") or ""),
            "output_json": latest.get("output_json") or {},
        }
    derivations = row.get("derivations") or []
    candidates = [item for item in derivations if isinstance(item, dict) and str(item.get("derivation_type")) == derivation_type]
    if not candidates:
        return None
    latest = sorted(
        candidates,
        key=lambda item: (
            str(item.get("prompt_version") or ""),
            str(item.get("provider") or ""),
            str(item.get("model") or ""),
        ),
    )[-1]
    return {
        "provider": latest.get("provider"),
        "prompt_version": latest.get("prompt_version"),
        "output_text": str(latest.get("output_text") or ""),
        "output_json": latest.get("output_json") or {},
    }


def build_ai_sweep_prompt_review(
    before: dict[str, Any],
    after: dict[str, Any],
    diff: dict[str, Any],
    *,
    report_path: str | Path | None = None,
) -> str:
    before_index = _sample_derivation_index(before)
    after_index = _sample_derivation_index(after)
    changed_units: list[dict[str, Any]] = []
    for unit_key, after_row in after_index.items():
        before_row = before_index.get(unit_key)
        if not before_row:
            continue
        before_clean = _derivation_brief(before_row, "clean_factual_text")
        after_clean = _derivation_brief(after_row, "clean_factual_text")
        before_extract = _derivation_brief(before_row, "structured_extract")
        after_extract = _derivation_brief(after_row, "structured_extract")
        before_tags = _derivation_brief(before_row, "tag_reasoning")
        after_tags = _derivation_brief(after_row, "tag_reasoning")
        if before_clean != after_clean or before_extract != after_extract or before_tags != after_tags:
            changed_units.append(
                {
                    "unit_key": unit_key,
                    "sample_bucket": after_row.get("sample_bucket"),
                    "content_type": after_row.get("content_type"),
                    "title": after_row.get("title") or after_row.get("canonical_title"),
                    "before_clean": before_clean,
                    "after_clean": after_clean,
                    "before_extract": before_extract,
                    "after_extract": after_extract,
                    "before_tags": before_tags,
                    "after_tags": after_tags,
                }
            )

    lines: list[str] = []
    lines.append("# AI Sweep Prompt Review")
    lines.append("")
    lines.append(f"- Generated at: `{now_iso()}`")
    lines.append(f"- Campaign: `{after.get('campaign_key')}`")
    lines.append(f"- Selected units: `{sum((after.get('selected_counts') or {}).values())}`")
    lines.append(f"- Changed sampled units (clean/extract/tags): `{len(changed_units)}`")
    lines.append(f"- Strict generic tags before: `{diff.get('strict_generic_tag_count_before', 0)}`")
    lines.append(f"- Strict generic tags after: `{diff.get('strict_generic_tag_count_after', 0)}`")
    lines.append("")
    lines.append("## Root Cause")
    lines.append("")
    lines.append(
        "- `clean_factual_text` and `structured_extract` previously allowed providers to over-enrich outputs with outside context. "
        "This was most visible on `official_profile` and `restriction_record` units, where biography, prior convictions, or "
        "media background leaked into derived text."
    )
    lines.append(
        "- `tag_reasoning` and `event_link_hint` were still on v1 prompt semantics. They did not always fail, but on strict content "
        "they could still pull in wider narrative context instead of staying inside the packet."
    )
    lines.append("")
    lines.append("## Fix Applied")
    lines.append("")
    lines.append("- `clean_factual_text` rerouted to `ai-sweep-v2-cleaner` and `structured_extract` to `ai-sweep-v2-extract`.")
    lines.append("- `tag_reasoning` bumped to `ai-sweep-v2-tags` with stricter abstain-first policy for `official_profile`, `declaration`, and `restriction_record`.")
    lines.append("- `event_link_hint` bumped to `ai-sweep-v2-event-link` with explicit standalone preference when the packet lacks enough merge evidence.")
    lines.append("- Provider routing now prefers `mistral`/`groq` for non-web stages and keeps `perplexity` as a late fallback for these stricter prompts.")
    lines.append("")
    lines.append("## Sample Before/After")
    lines.append("")
    if not changed_units:
        lines.append("- No sampled units changed in cleaner/extract/tag stages.")
    for unit in changed_units[:6]:
        lines.append(f"### {unit['unit_key']}")
        if unit.get("content_type"):
            lines.append(f"- Content type: `{unit['content_type']}`")
        if unit.get("title"):
            lines.append(f"- Title: `{_normalize_space(unit['title'])[:160]}`")
        before_clean = unit.get("before_clean") or {}
        after_clean = unit.get("after_clean") or {}
        before_extract = unit.get("before_extract") or {}
        after_extract = unit.get("after_extract") or {}
        before_tags = unit.get("before_tags") or {}
        after_tags = unit.get("after_tags") or {}
        lines.append(
            f"- Cleaner: `{before_clean.get('provider')}/{before_clean.get('prompt_version')}` → "
            f"`{after_clean.get('provider')}/{after_clean.get('prompt_version')}`"
        )
        lines.append(
            f"  - Before: `{_normalize_space(before_clean.get('output_text') or '')[:220]}`"
        )
        lines.append(
            f"  - After: `{_normalize_space(after_clean.get('output_text') or '')[:220]}`"
        )
        lines.append(
            f"- Extractor: `{before_extract.get('provider')}/{before_extract.get('prompt_version')}` → "
            f"`{after_extract.get('provider')}/{after_extract.get('prompt_version')}`"
        )
        lines.append(
            f"  - Before: `{_normalize_space(before_extract.get('output_text') or '')[:220]}`"
        )
        lines.append(
            f"  - After: `{_normalize_space(after_extract.get('output_text') or '')[:220]}`"
        )
        if before_tags or after_tags:
            lines.append(
                f"- Tag reasoning: `{before_tags.get('provider')}/{before_tags.get('prompt_version')}` → "
                f"`{after_tags.get('provider')}/{after_tags.get('prompt_version')}`"
            )
            lines.append(
                f"  - Before: `{_normalize_space(before_tags.get('output_text') or '')[:220]}`"
            )
            lines.append(
                f"  - After: `{_normalize_space(after_tags.get('output_text') or '')[:220]}`"
            )
        lines.append("")
    lines.append("## Next Checks")
    lines.append("")
    lines.append("- Re-run only `tag_reasoning` and `event_link_hint` for the same campaign when prompt versions change.")
    lines.append("- Keep idempotency guard: same campaign + same stage + same prompt_version + same input_hash must be skipped.")
    lines.append("- Review at least 30 sampled units after the v2 tags/event-link rerun and classify each as `better`, `same`, or `worse`.")
    lines.append("")
    text = "\n".join(lines).strip() + "\n"
    if report_path:
        target = Path(report_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    return text


def _stage_payload(unit: dict[str, Any], context: dict[str, Any], stage: str) -> dict[str, Any]:
    content_row = context.get("content_row") or {}
    event_row = context.get("event_row") or {}
    cluster_items = context.get("cluster_items") or []
    if stage == "event_synthesis" and isinstance(event_row, dict):
        event_row = {
            key: value
            for key, value in event_row.items()
            if key not in {"summary_short", "summary_long"}
        }
    return {
        "unit_kind": unit["unit_kind"],
        "unit_key": unit["unit_key"],
        "stage": stage,
        "content_id": context.get("content_item_id"),
        "event_id": unit.get("event_id"),
        "title": content_row.get("title") or event_row.get("canonical_title") or unit.get("canonical_title"),
        "body_text": content_row.get("body_text"),
        "content_type": content_row.get("content_type"),
        "published_at": content_row.get("published_at") or unit.get("published_at"),
        "url": content_row.get("url"),
        "source_category": content_row.get("category"),
        "cluster_key": unit.get("cluster_key"),
        "cluster_size": len(cluster_items),
        "event_context": event_row,
        "cluster_items": cluster_items,
        "review_payload": unit.get("candidate_payload"),
        "machine_reason": unit.get("machine_reason"),
    }


def _upsert_work_item(
    conn: sqlite3.Connection,
    *,
    campaign_id: int,
    unit: dict[str, Any],
    stage: str,
    prompt_version: str,
    input_hash: str,
    sample_bucket: str,
    payload: dict[str, Any],
) -> dict[str, int]:
    existing = conn.execute(
        """
        SELECT id, campaign_id, prompt_version, input_hash, status
        FROM ai_work_items
        WHERE unit_kind=? AND unit_key=? AND stage=?
        LIMIT 1
        """,
        (unit["unit_kind"], unit["unit_key"], stage),
    ).fetchone()
    if existing:
        existing_campaign_id = int(existing[1] or 0)
        existing_prompt = str(existing[2] or "")
        existing_input_hash = str(existing[3] or "")
        existing_status = str(existing[4] or "pending")
        same_identity = (
            existing_campaign_id == int(campaign_id)
            and existing_prompt == prompt_version
            and existing_input_hash == input_hash
        )
        if same_identity and existing_status == "completed":
            conn.execute(
                """
                UPDATE ai_work_items
                SET payload_json=?, updated_at=?
                WHERE id=?
                """,
                (_json_dumps(payload), now_iso(), int(existing[0])),
            )
            return {"inserted": 0, "reset": 0, "skipped": 1}
        reset = (not same_identity) or existing_status in {"failed", "stale", "needs_retry", "low_confidence"}
        conn.execute(
            """
            UPDATE ai_work_items
            SET campaign_id=?,
                prompt_version=?,
                input_hash=?,
                sample_bucket=?,
                payload_json=?,
                canonical_content_id=COALESCE(?, canonical_content_id),
                event_id=COALESCE(?, event_id),
                review_task_id=COALESCE(?, review_task_id),
                status=?,
                lease_owner=NULL,
                lease_expires_at=NULL,
                provider=CASE WHEN ? THEN NULL ELSE provider END,
                model_name=CASE WHEN ? THEN NULL ELSE model_name END,
                result_json=CASE WHEN ? THEN NULL ELSE result_json END,
                error_text=CASE WHEN ? THEN NULL ELSE error_text END,
                completed_at=CASE WHEN ? THEN NULL ELSE completed_at END,
                updated_at=?
            WHERE id=?
            """,
            (
                campaign_id,
                prompt_version,
                input_hash,
                sample_bucket,
                _json_dumps(payload),
                unit.get("canonical_content_id"),
                unit.get("event_id"),
                unit.get("review_task_id"),
                "pending" if reset else ("running" if existing_status == "running" else existing_status),
                int(reset),
                int(reset),
                int(reset),
                int(reset),
                int(reset),
                now_iso(),
                int(existing[0]),
            ),
        )
        return {"inserted": 0, "reset": 1 if reset else 0, "skipped": 0}
    conn.execute(
        """
        INSERT INTO ai_work_items(
            campaign_id, unit_kind, unit_key, stage, unit_ref_id, canonical_content_id, event_id, review_task_id,
            prompt_version, input_hash, sample_bucket, priority, status, payload_json, created_at, updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            campaign_id,
            unit["unit_kind"],
            unit["unit_key"],
            stage,
            unit.get("content_item_id") or unit.get("cluster_id") or unit.get("event_id") or unit.get("review_task_id"),
            unit.get("canonical_content_id"),
            unit.get("event_id"),
            unit.get("review_task_id"),
            prompt_version,
            input_hash,
            sample_bucket,
            50,
            "pending",
            _json_dumps(payload),
            now_iso(),
            now_iso(),
        ),
    )
    return {"inserted": 1, "reset": 0, "skipped": 0}


def enqueue_ai_work_items(settings: dict[str, Any]) -> dict[str, Any]:
    conn = get_db(settings)
    conn.row_factory = sqlite3.Row
    try:
        _bootstrap_key_pool(conn, settings)
        units = canonicalize_units(conn)
        campaign = ensure_ai_sweep_campaign(conn, settings, units)
        selection = {
            (str(entry["unit_kind"]), str(entry["unit_key"])): str(entry.get("sample_bucket") or str(entry["unit_kind"]))
            for entry in campaign["selection"]
        }
        inserted = 0
        reset = 0
        skipped = 0
        for unit in units:
            sample_bucket = selection.get((str(unit["unit_kind"]), str(unit["unit_key"])))
            if not sample_bucket:
                continue
            context = _build_unit_context(conn, unit)
            for stage in STAGES_BY_KIND.get(unit["unit_kind"], ()):
                prompt_version = _prompt_version_for_stage(stage, settings)
                payload = _stage_payload(unit, context, stage)
                input_hash = _hash_payload(payload)
                outcome = _upsert_work_item(
                    conn,
                    campaign_id=int(campaign["campaign_id"]),
                    unit=unit,
                    stage=stage,
                    prompt_version=prompt_version,
                    input_hash=input_hash,
                    sample_bucket=sample_bucket,
                    payload=payload,
                )
                inserted += int(outcome["inserted"])
                reset += int(outcome["reset"])
                skipped += int(outcome["skipped"])
        conn.commit()
        return {
            "ok": True,
            "campaign_id": int(campaign["campaign_id"]),
            "campaign_key": campaign["campaign_key"],
            "units_selected": len(selection),
            "items_new": int(inserted),
            "items_reset": int(reset),
            "items_skipped": int(skipped),
            "items_seen": len(selection),
        }
    finally:
        conn.close()


def _pending_units(conn: sqlite3.Connection, campaign_id: int, limit: int) -> list[tuple[str, str]]:
    params: list[Any] = []
    sql = """
        SELECT unit_kind, unit_key, MIN(id) AS first_id
        FROM ai_work_items
        WHERE campaign_id=?
          AND status <> 'completed'
        GROUP BY unit_kind, unit_key
        ORDER BY first_id
    """
    params.append(int(campaign_id))
    if limit > 0:
        sql += " LIMIT ?"
        params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [(str(row[0]), str(row[1])) for row in rows]


def _work_rows_for_units(conn: sqlite3.Connection, unit_keys: list[tuple[str, str]], campaign_id: int) -> list[sqlite3.Row]:
    if not unit_keys:
        return []
    placeholders = ",".join(["(?, ?)"] * len(unit_keys))
    params: list[Any] = [int(campaign_id)]
    for kind, key in unit_keys:
        params.extend([kind, key])
    rows = conn.execute(
        f"""
        SELECT id, unit_kind, unit_key, stage, canonical_content_id, event_id, review_task_id, payload_json, prompt_version, input_hash, sample_bucket
        FROM ai_work_items
        WHERE status <> 'completed'
          AND campaign_id=?
          AND (unit_kind, unit_key) IN ({placeholders})
        ORDER BY unit_kind, unit_key, id
        """,
        params,
    ).fetchall()
    return sorted(rows, key=lambda row: (row["unit_kind"], row["unit_key"], STAGE_ORDER.get(row["stage"], 999), row["id"]))


def _mark_work_item_running(conn: sqlite3.Connection, work_item_id: int, lease_owner: str) -> None:
    conn.execute(
        """
        UPDATE ai_work_items
        SET status='running', lease_owner=?, lease_expires_at=?, attempt_count=attempt_count + 1, updated_at=?
        WHERE id=?
        """,
        (lease_owner, now_iso(), now_iso(), int(work_item_id)),
    )
    conn.commit()


def _reset_running_work_items(conn: sqlite3.Connection, campaign_id: int) -> int:
    before = conn.total_changes
    conn.execute(
        """
        UPDATE ai_work_items
        SET status='pending',
            lease_owner=NULL,
            lease_expires_at=NULL,
            updated_at=?
        WHERE campaign_id=? AND status='running'
        """,
        (now_iso(), int(campaign_id)),
    )
    conn.commit()
    return conn.total_changes - before


def _mark_work_item_done(conn: sqlite3.Connection, work_item_id: int, *, status: str, provider: str | None, model_name: str | None, result_json: dict[str, Any] | None = None, error_text: str | None = None) -> None:
    conn.execute(
        """
        UPDATE ai_work_items
        SET status=?, provider=?, model_name=?, result_json=?, error_text=?, completed_at=?, updated_at=?, lease_owner=NULL, lease_expires_at=NULL
        WHERE id=?
        """,
        (
            status,
            provider,
            model_name,
            _json_dumps(result_json) if result_json is not None else None,
            error_text,
            now_iso() if status == "completed" else None,
            now_iso(),
            int(work_item_id),
        ),
    )
    conn.commit()


GENERIC_AUTO_TAGS = {"technology", "international", "regional", "технологии", "искусственный интеллект", "ес", "ии"}


def _tag_name_from_vote(item: Any) -> str:
    if isinstance(item, str):
        return _normalize_space(item)
    if isinstance(item, dict):
        return _normalize_space(item.get("tag") or item.get("tag_name") or item.get("normalized_tag"))
    return ""


def _tag_signal_layers(item: Any, output_json: dict[str, Any]) -> list[str]:
    raw_layers: Any = None
    if isinstance(item, dict):
        raw_layers = item.get("signal_layers") or item.get("supported_signals") or item.get("signals")
    if raw_layers is None:
        raw_layers = output_json.get("signal_layers")
    if isinstance(raw_layers, dict):
        raw_layers = list(raw_layers.keys())
    if not isinstance(raw_layers, list):
        return ["cleaned"]
    layers: list[str] = []
    for layer in raw_layers:
        if isinstance(layer, dict):
            value = layer.get("layer") or layer.get("signal_layer") or layer.get("source")
        else:
            value = layer
        normalized = _normalize_space(value).lower()
        if normalized:
            layers.append(normalized)
    return layers or ["cleaned"]


def _tag_namespace(tag_name: str) -> tuple[str | None, str]:
    tag = _normalize_space(tag_name).lower().replace("#", "")
    if "/" in tag:
        return tag.split("/", 1)[0], tag
    if ":" in tag:
        return tag.split(":", 1)[0], tag
    return None, tag


def _persist_tag_votes_from_derivation(
    conn: sqlite3.Connection,
    *,
    content_item_id: int,
    result: dict[str, Any],
    prompt_version: str,
    output_json: dict[str, Any],
) -> int:
    provider = str(result.get("provider") or "deterministic")
    model = str(result.get("model") or "deterministic")
    voter_name = f"ai_sweep:{provider}:{model}:{prompt_version}"
    conn.execute(
        "DELETE FROM content_tag_votes WHERE content_item_id=? AND voter_name=?",
        (int(content_item_id), voter_name),
    )
    inserted = 0

    def insert_vote(item: Any, vote_value: str, default_reason: str = "") -> None:
        nonlocal inserted
        tag_name = _tag_name_from_vote(item)
        if not tag_name:
            return
        namespace, normalized_tag = _tag_namespace(tag_name)
        layers = _tag_signal_layers(item, output_json)
        unique_layers = list(dict.fromkeys(layer for layer in layers if layer))
        reason = default_reason
        final_vote = vote_value
        if normalized_tag in GENERIC_AUTO_TAGS and len(unique_layers) < 2 and vote_value == "supported":
            final_vote = "needs_review"
            reason = "generic_tag_requires_two_independent_signals"
        confidence = result.get("confidence")
        if isinstance(item, dict) and item.get("confidence") is not None:
            confidence = item.get("confidence")
        try:
            confidence_value = max(0.0, min(1.0, float(confidence or 0)))
        except (TypeError, ValueError):
            confidence_value = 0.0
        for layer in unique_layers or ["cleaned"]:
            conn.execute(
                """
                INSERT INTO content_tag_votes(
                    content_item_id, voter_name, tag_name, namespace, normalized_tag,
                    vote_value, signal_layer, abstain_reason, confidence_raw, evidence_text, metadata_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(content_item_id),
                    voter_name,
                    tag_name,
                    namespace,
                    normalized_tag,
                    final_vote,
                    layer,
                    reason or None,
                    confidence_value,
                    result.get("output_text"),
                    _json_dumps(
                        {
                            "prompt_version": prompt_version,
                            "provider": provider,
                            "model": model,
                            "raw_vote": item,
                        }
                    ),
                ),
            )
            inserted += 1

    tags = output_json.get("tags") or []
    if isinstance(tags, list):
        for tag_item in tags:
            insert_vote(tag_item, "supported")
    abstain_tags = output_json.get("abstain_tags") or output_json.get("abstained_tags") or []
    if isinstance(abstain_tags, list):
        abstain_reason = _normalize_space(output_json.get("abstain_reason")) or "ai_abstain"
        for tag_item in abstain_tags:
            insert_vote(tag_item, "abstained", abstain_reason)
    return inserted


def _persist_content_derivation(
    conn: sqlite3.Connection,
    unit: dict[str, Any],
    stage: str,
    prompt_version: str,
    input_hash: str,
    result: dict[str, Any],
    payload: dict[str, Any],
    work_item_id: int | None = None,
) -> int:
    content_item_id = int(unit.get("canonical_content_id") or unit.get("content_item_id") or payload.get("content_id") or 0)
    if content_item_id <= 0:
        return 0
    output_json = result.get("output_json")
    if not isinstance(output_json, dict):
        output_json = {"value": output_json} if output_json is not None else {}
    event_context = output_json.get("event_context")
    fact_context = output_json.get("fact_context")
    temporal_window = output_json.get("temporal_window")
    campaign_id = None
    if work_item_id is not None:
        row = conn.execute("SELECT campaign_id FROM ai_work_items WHERE id=?", (int(work_item_id),)).fetchone()
        if row and row[0] is not None:
            campaign_id = int(row[0])
    before = conn.total_changes
    conn.execute(
        """
        UPDATE content_derivations
        SET is_current=0
        WHERE content_item_id=? AND derivation_type=?
        """,
        (content_item_id, stage),
    )
    conn.execute(
        """
        INSERT INTO content_derivations(
            content_item_id, campaign_id, work_item_id, derivation_type, model_provider, model_name, prompt_version, input_hash,
            output_text, output_json, event_context_json, fact_context_json, temporal_window_json,
            confidence, status, is_current, created_at, updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(content_item_id, derivation_type, model_provider, model_name, prompt_version, input_hash) DO UPDATE SET
            campaign_id=excluded.campaign_id,
            work_item_id=excluded.work_item_id,
            output_text=excluded.output_text,
            output_json=excluded.output_json,
            event_context_json=excluded.event_context_json,
            fact_context_json=excluded.fact_context_json,
            temporal_window_json=excluded.temporal_window_json,
            confidence=excluded.confidence,
            status=excluded.status,
            is_current=excluded.is_current,
            updated_at=excluded.updated_at
        """,
        (
            content_item_id,
            campaign_id,
            int(work_item_id) if work_item_id is not None else None,
            stage,
            result.get("provider") or "deterministic",
            result.get("model") or "deterministic",
            prompt_version,
            input_hash,
            result.get("output_text"),
            _json_dumps(output_json),
            _json_dumps(event_context) if event_context is not None else None,
            _json_dumps(fact_context) if fact_context is not None else None,
            _json_dumps(temporal_window) if temporal_window is not None else None,
            float(result.get("confidence") or 0),
            "ready",
            1,
            now_iso(),
            now_iso(),
        ),
    )
    if stage == "tag_reasoning":
        _persist_tag_votes_from_derivation(
            conn,
            content_item_id=content_item_id,
            result=result,
            prompt_version=prompt_version,
            output_json=output_json,
        )
    return conn.total_changes - before


def _persist_event_candidate(conn: sqlite3.Connection, unit: dict[str, Any], prompt_version: str, result: dict[str, Any]) -> int:
    output_json = result.get("output_json")
    if not isinstance(output_json, dict):
        return 0
    output_json = dict(output_json)
    raw_action = str(output_json.get("action") or "").strip().lower()
    suggested_event_id = output_json.get("event_id")
    try:
        suggested_event_id = int(suggested_event_id) if suggested_event_id is not None else None
    except (TypeError, ValueError):
        output_json["invalid_event_id"] = suggested_event_id
        suggested_event_id = None

    def normalize_candidate_state(action: str, has_event: bool) -> str:
        if action in {"link_existing_event", "link_existing", "suggested"}:
            return "link_existing" if has_event else "standalone"
        if action in {"create_event_candidate", "create_candidate"}:
            return "create_candidate"
        if action in {"merge_review", "merge_candidate", "possible_merge"}:
            return "merge_review"
        if action in {"rejected", "reject"}:
            return "rejected"
        if action == "standalone":
            return "standalone"
        return "standalone"

    candidate_state = normalize_candidate_state(raw_action, suggested_event_id is not None)
    if suggested_event_id is not None:
        event_exists = conn.execute("SELECT 1 FROM events WHERE id=? LIMIT 1", (suggested_event_id,)).fetchone()
        if not event_exists:
            output_json["invalid_event_id"] = suggested_event_id
            suggested_event_id = None
            candidate_state = normalize_candidate_state(raw_action, False)
    row = conn.execute(
        """
        SELECT id
        FROM event_candidates
        WHERE unit_kind=? AND unit_key=? AND COALESCE(suggested_event_id, 0)=COALESCE(?, 0)
        ORDER BY id DESC
        LIMIT 1
        """,
        (unit["unit_kind"], unit["unit_key"], suggested_event_id),
    ).fetchone()
    if row:
        candidate_id = int(row[0])
        conn.execute(
            """
            UPDATE event_candidates
            SET content_item_id=?, content_cluster_id=?, suggested_event_id=?, candidate_state=?, confidence=?, suggestion_json=?,
                model_provider=?, model_name=?, prompt_version=?, status='open', updated_at=?
            WHERE id=?
            """,
            (
                unit.get("content_item_id") or unit.get("canonical_content_id"),
                unit.get("cluster_id"),
                suggested_event_id,
                candidate_state,
                float(result.get("confidence") or 0),
                _json_dumps(output_json),
                result.get("provider"),
                result.get("model"),
                prompt_version,
                now_iso(),
                candidate_id,
            ),
        )
    else:
        cur = conn.execute(
        """
        INSERT INTO event_candidates(
            unit_kind, unit_key, content_item_id, content_cluster_id, suggested_event_id, candidate_state,
            confidence, suggestion_json, model_provider, model_name, prompt_version, status, created_at, updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            unit["unit_kind"],
            unit["unit_key"],
            unit.get("content_item_id") or unit.get("canonical_content_id"),
            unit.get("cluster_id"),
            suggested_event_id,
            candidate_state,
            float(result.get("confidence") or 0),
            _json_dumps(output_json),
            result.get("provider"),
            result.get("model"),
            prompt_version,
            "open",
            now_iso(),
            now_iso(),
        ),
    )
        candidate_id = int(cur.lastrowid)
    if candidate_state == "merge_review":
        reason = _normalize_space(output_json.get("reason")) or "event_merge_review"
        ensure_review_task(
            conn,
            task_key=f"event-candidate:{candidate_id}:merge_review",
            queue_key="events",
            subject_type="event_candidate",
            subject_id=candidate_id,
            related_id=suggested_event_id,
            candidate_payload={
                "unit": {
                    "unit_kind": unit.get("unit_kind"),
                    "unit_key": unit.get("unit_key"),
                    "content_item_id": unit.get("content_item_id") or unit.get("canonical_content_id"),
                    "content_cluster_id": unit.get("cluster_id"),
                },
                "suggestion": output_json,
                "provider": result.get("provider"),
                "model": result.get("model"),
                "prompt_version": prompt_version,
            },
            suggested_action="needs_review",
            confidence=float(result.get("confidence") or 0),
            machine_reason=reason,
            source_links=result.get("citations") if isinstance(result.get("citations"), list) else None,
        )
    return 1


def _persist_event_synthesis(conn: sqlite3.Connection, unit: dict[str, Any], result: dict[str, Any]) -> int:
    event_id = int(unit.get("event_id") or 0)
    if event_id <= 0:
        return 0
    output_json = result.get("output_json")
    if not isinstance(output_json, dict):
        output_json = {}
    conn.execute(
        """
        UPDATE events
        SET summary_short=?, summary_long=?, updated_at=?
        WHERE id=?
        """,
        (
            output_json.get("summary_short") or result.get("output_text"),
            output_json.get("summary_long") or result.get("output_text"),
            now_iso(),
            event_id,
        ),
    )
    timeline = output_json.get("timeline") or []
    if isinstance(timeline, list):
        for index, item in enumerate(timeline):
            if not isinstance(item, dict):
                continue
            title = _normalize_space(item.get("title"))
            if not title:
                continue
            exists = conn.execute(
                """
                SELECT id FROM event_timeline
                WHERE event_id=? AND COALESCE(timeline_date, '')=COALESCE(?, '') AND title=?
                LIMIT 1
                """,
                (event_id, item.get("date"), title),
            ).fetchone()
            if exists:
                continue
            conn.execute(
                """
                INSERT INTO event_timeline(event_id, timeline_date, title, description, sort_order, metadata_json)
                VALUES(?,?,?,?,?,?)
                """,
                (
                    event_id,
                    item.get("date"),
                    title,
                    item.get("description"),
                    index,
                    _json_dumps({"generated_by": "ai_sweep", "provider": result.get("provider"), "model": result.get("model")}),
                ),
            )
    participants = output_json.get("participants") or []
    if isinstance(participants, list) and _table_exists(conn, "entities"):
        for item in participants:
            if not isinstance(item, dict):
                continue
            name = _normalize_space(item.get("name"))
            role = _normalize_space(item.get("role")) or "commentator"
            if not name:
                continue
            entity_row = conn.execute(
                "SELECT id FROM entities WHERE canonical_name=? LIMIT 1",
                (name,),
            ).fetchone()
            if not entity_row:
                continue
            conn.execute(
                """
                INSERT INTO event_entities(event_id, entity_id, role, confidence, observed_at, metadata_json)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(event_id, entity_id, role) DO UPDATE SET
                    confidence=MAX(event_entities.confidence, excluded.confidence),
                    observed_at=COALESCE(event_entities.observed_at, excluded.observed_at),
                    metadata_json=excluded.metadata_json
                """,
                (
                    event_id,
                    int(entity_row[0]),
                    role,
                    float(result.get("confidence") or 0),
                    now_iso(),
                    _json_dumps({"generated_by": "ai_sweep"}),
                ),
            )
    return 1


def _persist_result(
    conn: sqlite3.Connection,
    unit: dict[str, Any],
    stage: str,
    prompt_version: str,
    input_hash: str,
    result: dict[str, Any],
    payload: dict[str, Any],
    work_item_id: int | None = None,
) -> int:
    if stage in DERIVATION_STAGES:
        return _persist_content_derivation(conn, unit, stage, prompt_version, input_hash, result, payload, work_item_id)
    if stage == "event_link_hint":
        return _persist_event_candidate(conn, unit, prompt_version, result)
    if stage == "event_synthesis":
        return _persist_event_synthesis(conn, unit, result)
    return 0


def _detect_failure_kind(error_text: str) -> str:
    lowered = (error_text or "").lower()
    if (
        "invalid_tools" in lowered
        or "connector is not supported" in lowered
        or "not a valid model id" in lowered
        or "invalid model" in lowered
        or "unsupported_provider" in lowered
    ):
        return "provider"
    if (
        "429" in lowered
        or "rate limit" in lowered
        or "rate_limit" in lowered
        or "too many requests" in lowered
    ):
        return "rate"
    if (
        "401" in lowered
        or "invalid api key" in lowered
        or "unauthorized" in lowered
        or "forbidden" in lowered
        or "billing" in lowered
        or "insufficient_quota" in lowered
        or "quota_exhausted" in lowered
        or ("quota" in lowered and "rate" not in lowered)
    ):
        return "auth"
    if "timeout" in lowered or "timed out" in lowered:
        return "timeout"
    return "provider"


def _worker_run(
    settings: dict[str, Any],
    unit: dict[str, Any],
    stage: str,
    prompt_version: str,
    input_hash: str,
    payload: dict[str, Any],
    work_item_id: int,
) -> dict[str, Any]:
    worker_settings = dict(settings)
    worker_settings["ensure_schema_on_connect"] = False
    conn = get_db(worker_settings)
    conn.row_factory = sqlite3.Row
    failures: list[dict[str, Any]] = []
    try:
        priority = _stage_provider_priority(stage, settings)
        cfg = _ai_settings(settings)
        max_attempts = max(1, int(cfg.get("max_attempts_per_work_item") or 6))
        max_provider_failures = max(1, int(cfg.get("max_failures_per_provider_per_item") or 2))
        exclude: set[int] = set()
        provider_failures: dict[str, int] = {}
        while True:
            if len(failures) >= max_attempts:
                return {
                    "ok": False,
                    "work_item_id": work_item_id,
                    "unit": unit,
                    "stage": stage,
                    "prompt_version": prompt_version,
                    "input_hash": input_hash,
                    "payload": payload,
                    "attempts": failures,
                    "error": "ai_task_retry_budget_exhausted",
                }
            chosen = choose_key_for_stage(conn, stage=stage, provider_priority=priority, exclude_key_ids=exclude)
            if not chosen:
                return {
                    "ok": False,
                    "work_item_id": work_item_id,
                    "unit": unit,
                    "stage": stage,
                    "prompt_version": prompt_version,
                    "input_hash": input_hash,
                    "payload": payload,
                    "attempts": failures,
                    "error": "no_active_keys_for_stage",
                }
            key_id = int(chosen["key_id"])
            provider = str(chosen["provider"])
            model_name = str(chosen["model_name"])
            try:
                response = run_ai_task(
                    conn=None,
                    provider=provider,
                    model=model_name,
                    api_key=str(chosen["api_key"]),
                    task={"stage": stage, "prompt_version": prompt_version, "unit": unit, "payload": payload},
                )
                record_key_success(conn, key_id)
                return {
                    "ok": True,
                    "work_item_id": work_item_id,
                    "unit": unit,
                    "stage": stage,
                    "prompt_version": prompt_version,
                    "input_hash": input_hash,
                    "payload": payload,
                    "provider": provider,
                    "model": model_name,
                    "key_id": key_id,
                    "result": dict(response or {}),
                    "attempts": failures + [{"provider": provider, "model": model_name, "key_id": key_id, "status": "ok"}],
                }
            except Exception as error:  # pragma: no cover - exercised via higher-level retry behavior
                failure_text = str(error)
                failure_kind = _detect_failure_kind(failure_text)
                record = record_key_failure(
                    conn,
                    key_id,
                    failure_kind=failure_kind,
                    error_text=failure_text,
                    remove_threshold=(
                        1
                        if failure_kind == "auth"
                        else (10**9 if failure_kind in {"rate", "timeout"} else int(_ai_settings(settings).get("dead_key_threshold") or 3))
                    ),
                )
                failures.append(
                    {
                        "provider": provider,
                        "model": model_name,
                        "key_id": key_id,
                        "status": "failed",
                        "error_text": failure_text,
                        "failure_kind": failure_kind,
                        "removed": bool(record.get("removed")),
                    }
                )
                exclude.add(key_id)
                provider_failures[provider] = provider_failures.get(provider, 0) + 1
                if provider_failures[provider] >= max_provider_failures:
                    provider_key_rows = conn.execute(
                        "SELECT id FROM llm_keys WHERE provider=? AND status='active'",
                        (provider,),
                    ).fetchall()
                    for provider_key_row in provider_key_rows:
                        exclude.add(int(provider_key_row[0]))
    finally:
        conn.close()


def _effective_worker_count(conn: sqlite3.Connection, settings: dict[str, Any], *, pending_items: int = 0) -> int:
    cfg = _ai_settings(settings)
    active_keys = conn.execute("SELECT COUNT(*) FROM llm_keys WHERE status='active'").fetchone()[0]
    minimum = int(cfg.get("min_parallel_workers") or 10)
    default = int(cfg.get("default_worker_count") or 12)
    maximum = int(cfg.get("max_parallel_workers") or 24)
    desired = max(minimum, default)
    if pending_items > 0:
        backlog_scaled = max(default, min(maximum, (int(pending_items) + 3) // 4))
        desired = max(desired, backlog_scaled)
    if int(active_keys or 0) <= 0:
        return 1
    return max(1, min(maximum, int(active_keys), desired))


def _record_attempts(conn: sqlite3.Connection, work_item_id: int, attempts: list[dict[str, Any]], final_result: dict[str, Any] | None = None) -> int:
    count = 0
    for attempt in attempts:
        key_id = attempt.get("key_id")
        row = conn.execute(
            """
            SELECT id FROM llm_keys
            WHERE provider=? AND id=?
            """,
            (attempt.get("provider"), key_id),
        ).fetchone() if key_id else None
        conn.execute(
            """
            INSERT INTO ai_task_attempts(
                work_item_id, provider, model_name, llm_key_id, status, failure_kind,
                error_text, output_json, started_at, finished_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                work_item_id,
                attempt.get("provider"),
                attempt.get("model"),
                int(row[0]) if row else None,
                attempt.get("status") or "unknown",
                attempt.get("failure_kind"),
                attempt.get("error_text"),
                _json_dumps(final_result) if final_result and attempt.get("status") == "ok" else None,
                now_iso(),
                now_iso(),
            ),
        )
        count += 1
    return count


def run_ai_full_sweep(settings: dict[str, Any]) -> dict[str, Any]:
    conn = get_db(settings)
    conn.row_factory = sqlite3.Row
    cfg = _ai_settings(settings)
    default_reports_dir = Path(settings.get("project_root") or PROJECT_ROOT) / "reports"
    reports_dir = Path(settings.get("reports_dir") or default_reports_dir)
    before_path = Path(cfg.get("before_report_path") or (reports_dir / "ai_sweep_pilot_before.json"))
    after_path = Path(cfg.get("after_report_path") or (reports_dir / "ai_sweep_pilot_after.json"))
    diff_path = Path(cfg.get("diff_report_path") or (reports_dir / "ai_sweep_pilot_diff.json"))
    prompt_review_path = Path(cfg.get("prompt_review_path") or (reports_dir / "ai_sweep_prompt_review.md"))
    pipeline_version = f"ai-sweep-{now_iso().replace('-', '').replace(':', '').replace('T', '')}"
    pipeline_run_id = start_pipeline_run(
        conn,
        pipeline_version=pipeline_version,
        mode="ai_sweep",
        requested_by="ai_full_sweep",
        stages=["canonicalize_units", "clean_factual_text", "structured_extract", "event_link_hint", "tag_reasoning", "relation_reasoning", "event_synthesis"],
    )
    bootstrap = _bootstrap_key_pool(conn, settings)
    before_report = build_ai_sweep_pilot_report(settings, report_path=before_path)
    enqueue_stats = enqueue_ai_work_items(settings)
    all_units = canonicalize_units(conn)
    unit_index = {(unit["unit_kind"], unit["unit_key"]): unit for unit in all_units}
    campaign = ensure_ai_sweep_campaign(conn, settings, all_units)
    max_units = int(_ai_settings(settings).get("max_units_per_run") or 0)
    selected_pairs = _pending_units(conn, int(campaign["campaign_id"]), max_units)
    selected_units = [unit_index[pair] for pair in selected_pairs if pair in unit_index]
    work_rows = _work_rows_for_units(conn, selected_pairs, int(campaign["campaign_id"]))
    if not work_rows:
        report = {
            "ok": True,
            "pipeline_version": pipeline_version,
            "pipeline_run_id": pipeline_run_id,
            "bootstrap": bootstrap,
            "enqueue": enqueue_stats,
            "campaign_id": int(campaign["campaign_id"]),
            "campaign_key": campaign["campaign_key"],
            "units_total": len(all_units),
            "units_selected": len(campaign["selection"]),
            "items_seen": 0,
            "items_new": 0,
            "items_updated": 0,
            "attempts": 0,
            "worker_count": 0,
            "warnings": ["ai_sweep_no_pending_units"],
            "before_report_path": str(before_path),
        }
        after_report = build_ai_sweep_pilot_report(settings, report_path=after_path)
        diff_report = build_ai_sweep_pilot_diff(before_report, after_report, report_path=diff_path)
        build_ai_sweep_prompt_review(before_report, after_report, diff_report, report_path=prompt_review_path)
        report["after_report_path"] = str(after_path)
        report["diff_report_path"] = str(diff_path)
        report["prompt_review_path"] = str(prompt_review_path)
        report["pilot_diff_summary"] = diff_report
        set_runtime_metadata(conn, "ai_sweep_latest_report", report)
        finish_pipeline_run(conn, pipeline_run_id, ok=True, result=report)
        conn.close()
        return report

    reclaimed_running = _reset_running_work_items(conn, int(campaign["campaign_id"]))
    if reclaimed_running:
        work_rows = _work_rows_for_units(conn, selected_pairs, int(campaign["campaign_id"]))

    worker_count = _effective_worker_count(conn, settings, pending_items=len(work_rows))
    owner = f"ai-sweep:{os.getpid()}"
    conn.execute(
        "UPDATE ai_sweep_campaigns SET status='running', last_run_at=?, updated_at=? WHERE id=?",
        (now_iso(), now_iso(), int(campaign["campaign_id"])),
    )
    conn.commit()
    for row in work_rows:
        _mark_work_item_running(conn, int(row["id"]), owner)

    futures = {}
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="ai-sweep") as executor:
        for row in work_rows:
            unit = unit_index[(row["unit_kind"], row["unit_key"])]
            payload = json.loads(row["payload_json"] or "{}")
            future = executor.submit(
                _worker_run,
                settings,
                unit,
                str(row["stage"]),
                str(row["prompt_version"] or _prompt_version_for_stage(str(row["stage"]), settings)),
                str(row["input_hash"] or _hash_payload(payload)),
                payload,
                int(row["id"]),
            )
            futures[future] = int(row["id"])

        items_seen = 0
        items_new = 0
        items_updated = 0
        attempts = 0
        warnings: list[str] = []
        retriable_errors: list[str] = []
        fatal_errors: list[str] = []
        completed_units: set[tuple[str, str]] = set()

        for future in as_completed(futures):
            payload = future.result()
            work_item_id = int(payload["work_item_id"])
            attempts += _record_attempts(conn, work_item_id, payload.get("attempts") or [], payload.get("result"))
            items_seen += 1
            if payload.get("ok"):
                result = dict(payload.get("result") or {})
                try:
                    updated = _persist_result(
                        conn,
                        payload["unit"],
                        payload["stage"],
                        str(payload.get("prompt_version") or _prompt_version_for_stage(str(payload["stage"]), settings)),
                        str(payload.get("input_hash") or _hash_payload(payload["payload"])),
                        result,
                        payload["payload"],
                        work_item_id,
                    )
                except Exception as error:  # pragma: no cover - exercised via live provider/output variability
                    retriable_errors.append(f"persist:{payload['stage']}:{error}")
                    _mark_work_item_done(
                        conn,
                        work_item_id,
                        status="failed",
                        provider=payload.get("provider"),
                        model_name=payload.get("model"),
                        result_json=result,
                        error_text=str(error),
                    )
                    continue
                _mark_work_item_done(
                    conn,
                    work_item_id,
                    status="completed",
                    provider=payload.get("provider"),
                    model_name=payload.get("model"),
                    result_json=result,
                )
                items_new += int(updated or 0)
                if updated:
                    items_updated += 1
                completed_units.add((payload["unit"]["unit_kind"], payload["unit"]["unit_key"]))
            else:
                error_text = str(payload.get("error") or "ai_task_failed")
                retriable_errors.append(error_text)
                _mark_work_item_done(
                    conn,
                    work_item_id,
                    status="failed",
                    provider=None,
                    model_name=None,
                    result_json=None,
                    error_text=error_text,
                )

    report = {
        "ok": not fatal_errors,
        "pipeline_version": pipeline_version,
        "pipeline_run_id": pipeline_run_id,
        "bootstrap": bootstrap,
        "enqueue": enqueue_stats,
        "campaign_id": int(campaign["campaign_id"]),
        "campaign_key": campaign["campaign_key"],
        "units_total": len(all_units),
        "units_selected": len(campaign["selection"]),
        "completed_units": len(completed_units),
        "items_seen": items_seen,
        "items_new": items_new,
        "items_updated": items_updated,
        "attempts": attempts,
        "worker_count": worker_count,
        "warnings": warnings,
        "retriable_errors": retriable_errors,
        "fatal_errors": fatal_errors,
        "before_report_path": str(before_path),
        "reclaimed_running": int(reclaimed_running),
    }
    conn.execute(
        "UPDATE ai_sweep_campaigns SET status=?, updated_at=?, completed_at=? WHERE id=?",
        ("completed" if report["ok"] else "failed", now_iso(), now_iso() if report["ok"] else None, int(campaign["campaign_id"])),
    )
    conn.commit()
    after_report = build_ai_sweep_pilot_report(settings, report_path=after_path)
    diff_report = build_ai_sweep_pilot_diff(before_report, after_report, report_path=diff_path)
    build_ai_sweep_prompt_review(before_report, after_report, diff_report, report_path=prompt_review_path)
    report["after_report_path"] = str(after_path)
    report["diff_report_path"] = str(diff_path)
    report["prompt_review_path"] = str(prompt_review_path)
    report["pilot_diff_summary"] = diff_report
    set_runtime_metadata(conn, "ai_sweep_latest_report", report)
    finish_pipeline_run(conn, pipeline_run_id, ok=bool(report["ok"]), result=report)
    conn.close()
    return report
