from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


@dataclass
class JobResult:
    ok: bool
    job_id: str
    started_at: str
    finished_at: str
    items_seen: int = 0
    items_new: int = 0
    items_updated: int = 0
    warnings: list[str] = field(default_factory=list)
    retriable_errors: list[str] = field(default_factory=list)
    fatal_errors: list[str] = field(default_factory=list)
    next_cursor: str | None = None
    health: dict[str, Any] = field(default_factory=dict)
    artifacts: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def success(
        cls,
        job_id: str,
        started_at: str,
        *,
        items_seen: int = 0,
        items_new: int = 0,
        items_updated: int = 0,
        warnings: list[str] | None = None,
        next_cursor: str | None = None,
        health: dict[str, Any] | None = None,
        artifacts: dict[str, Any] | None = None,
        finished_at: str | None = None,
    ) -> "JobResult":
        return cls(
            ok=True,
            job_id=job_id,
            started_at=started_at,
            finished_at=finished_at or now_iso(),
            items_seen=int(items_seen or 0),
            items_new=int(items_new or 0),
            items_updated=int(items_updated or 0),
            warnings=list(warnings or []),
            next_cursor=next_cursor,
            health=dict(health or {}),
            artifacts=dict(artifacts or {}),
        )

    @classmethod
    def failure(
        cls,
        job_id: str,
        started_at: str,
        *,
        retriable_errors: list[str] | None = None,
        fatal_errors: list[str] | None = None,
        warnings: list[str] | None = None,
        health: dict[str, Any] | None = None,
        artifacts: dict[str, Any] | None = None,
        finished_at: str | None = None,
    ) -> "JobResult":
        return cls(
            ok=False,
            job_id=job_id,
            started_at=started_at,
            finished_at=finished_at or now_iso(),
            warnings=list(warnings or []),
            retriable_errors=list(retriable_errors or []),
            fatal_errors=list(fatal_errors or []),
            health=dict(health or {}),
            artifacts=dict(artifacts or {}),
        )


def _pick_first_number(payload: dict[str, Any], keys: tuple[str, ...]) -> int:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str) and value.strip().isdigit():
            return int(value.strip())
    return 0


def normalize_job_output(job_id: str, started_at: str, raw_result: Any) -> JobResult:
    finished_at = now_iso()
    if isinstance(raw_result, JobResult):
        raw_result.finished_at = raw_result.finished_at or finished_at
        return raw_result

    artifacts = raw_result if isinstance(raw_result, dict) else {"result": raw_result} if raw_result is not None else {}
    payload = artifacts if isinstance(artifacts, dict) else {}

    warnings = payload.get("warnings") if isinstance(payload.get("warnings"), list) else []
    retriable_errors = payload.get("retriable_errors") if isinstance(payload.get("retriable_errors"), list) else []
    fatal_errors = payload.get("fatal_errors") if isinstance(payload.get("fatal_errors"), list) else []
    health = payload.get("health") if isinstance(payload.get("health"), dict) else {}
    next_cursor = payload.get("next_cursor")

    items_seen = _pick_first_number(
        payload,
        (
            "items_seen",
            "checked",
            "seen",
            "processed",
            "total",
            "claims_total",
            "co_occurrence_pairs",
        ),
    )
    items_new = _pick_first_number(
        payload,
        (
            "items_new",
            "new",
            "inserted",
            "created",
            "collected",
            "relations_inserted",
            "contracts",
            "people_stored",
            "upgraded",
            "claims_with_evidence",
            "ok",
        ),
    )
    items_updated = _pick_first_number(
        payload,
        (
            "items_updated",
            "updated",
            "promoted",
            "parties",
            "new_evidence",
            "strong_relations",
            "moderate_relations",
        ),
    )

    ok = not fatal_errors and bool(payload.get("ok", True))
    if retriable_errors and not fatal_errors:
        ok = False

    if ok:
        return JobResult.success(
            job_id=job_id,
            started_at=started_at,
            finished_at=finished_at,
            items_seen=items_seen,
            items_new=items_new,
            items_updated=items_updated,
            warnings=warnings,
            next_cursor=next_cursor if isinstance(next_cursor, str) else None,
            health=health,
            artifacts=payload if isinstance(payload, dict) else artifacts,
        )

    return JobResult.failure(
        job_id=job_id,
        started_at=started_at,
        finished_at=finished_at,
        warnings=warnings,
        retriable_errors=retriable_errors,
        fatal_errors=fatal_errors or [str(payload.get("error") or "job_failed")],
        health=health,
        artifacts=payload if isinstance(payload, dict) else artifacts,
    )
