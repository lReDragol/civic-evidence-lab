from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

AGENT_GROUPS = {
    "classification": ["tag_reasoning", "structured_extract", "triage"],
    "verification": ["relation_reasoning", "event_synthesis", "arbiter"],
    "enrichment": ["clean_factual_text", "event_link_hint"],
    "research": ["evidence_research", "agent_search"],
}

TOTAL_AGENTS = sum(len(v) for v in AGENT_GROUPS.values())

KEYS_PER_AGENT = 2

BUFFER_FACTOR = 1.5

MIN_KEYS = int(TOTAL_AGENTS * KEYS_PER_AGENT * BUFFER_FACTOR)

CRITICAL_THRESHOLD = 0.20

WARNING_THRESHOLD = 0.40


def compute_min_keys() -> int:
    return MIN_KEYS


def key_health(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT provider, COUNT(*) as total, "
        "SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) as active, "
        "SUM(CASE WHEN status='removed' THEN 1 ELSE 0 END) as removed, "
        "SUM(CASE WHEN status='rate_limited' THEN 1 ELSE 0 END) as rate_limited "
        "FROM llm_keys GROUP BY provider"
    ).fetchall()

    total_active = 0
    by_provider = {}
    for row in rows:
        provider = row[0]
        active = row[2] or 0
        total_active += active
        by_provider[provider] = {
            "total": row[1],
            "active": active,
            "removed": row[3] or 0,
            "rate_limited": row[4] or 0,
        }

    min_keys = compute_min_keys()
    ratio = total_active / min_keys if min_keys > 0 else 0

    if ratio <= CRITICAL_THRESHOLD:
        severity = "critical"
    elif ratio <= WARNING_THRESHOLD:
        severity = "warning"
    else:
        severity = "ok"

    return {
        "total_active": total_active,
        "min_keys": min_keys,
        "ratio": round(ratio, 3),
        "severity": severity,
        "by_provider": by_provider,
        "agents": TOTAL_AGENTS,
        "groups": {k: len(v) for k, v in AGENT_GROUPS.items()},
    }


def check_and_alert(conn: sqlite3.Connection) -> dict[str, Any] | None:
    health = key_health(conn)
    severity = health["severity"]

    if severity == "ok":
        return None

    pct = int(health["ratio"] * 100)
    active = health["total_active"]
    minimum = health["min_keys"]

    if severity == "critical":
        msg = f"КЛЮЧЕЙ КРИТИЧЕСКИ МАЛО: {active}/{minimum} ({pct}%). Пополните key.json!"
    else:
        msg = f"Ключей мало: {active}/{minimum} ({pct}%). Рекомендуется пополнить key.json."

    conn.execute(
        "INSERT INTO daemon_alerts(alert_type, severity, message) VALUES(?,?,?)",
        ("key_low", severity, msg),
    )
    conn.commit()

    log.log(logging.CRITICAL if severity == "critical" else logging.WARNING, msg)
    return {"severity": severity, "message": msg, "health": health}


def health_summary(conn: sqlite3.Connection) -> str:
    health = key_health(conn)
    active = health["total_active"]
    minimum = health["min_keys"]
    pct = int(health["ratio"] * 100)
    severity = health["severity"]
    prov_summary = ", ".join(f"{p}:{d['active']}" for p, d in sorted(health["by_provider"].items()) if d["active"] > 0)
    return f"Keys: {active}/{minimum} ({pct}%) [{severity}] | {prov_summary}"
