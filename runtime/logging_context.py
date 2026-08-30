from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator


LOG_CONTEXT_FIELDS = (
    "job_run_id",
    "pipeline_run_id",
    "source_key",
    "source_type",
    "telegram_session",
    "channel",
    "provider",
    "model",
    "item_id",
    "raw_item_id",
    "content_item_id",
)

_LOG_CONTEXT: ContextVar[dict[str, Any]] = ContextVar("news_log_context", default={})


def current_log_context() -> dict[str, Any]:
    return dict(_LOG_CONTEXT.get() or {})


def bind_log_context(**kwargs: Any) -> None:
    current = current_log_context()
    for key, value in kwargs.items():
        if key not in LOG_CONTEXT_FIELDS:
            continue
        current[key] = "" if value is None else value
    _LOG_CONTEXT.set(current)


def clear_log_context() -> None:
    _LOG_CONTEXT.set({})


@contextmanager
def log_context(**kwargs: Any) -> Iterator[None]:
    token = _LOG_CONTEXT.set({**current_log_context(), **{k: v for k, v in kwargs.items() if k in LOG_CONTEXT_FIELDS}})
    try:
        yield
    finally:
        _LOG_CONTEXT.reset(token)

