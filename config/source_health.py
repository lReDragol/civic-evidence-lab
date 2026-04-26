from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from config.db_utils import PROJECT_ROOT, load_settings


DEFAULT_MANIFEST_PATH = PROJECT_ROOT / "config" / "source_health_manifest.json"
HEALTHY_STATES = {"healthy_live", "healthy_archive", "healthy_fixture"}
TRANSPORT_FAILURE_CLASSES = {
    "timeout",
    "tls",
    "connection",
    "http_forbidden",
    "rate_limited",
    "not_found",
}


def source_health_manifest_path(settings: dict[str, Any] | None = None) -> Path:
    settings = settings or {}
    configured = settings.get("source_health_manifest_path")
    path = Path(configured) if configured else DEFAULT_MANIFEST_PATH
    return path if path.is_absolute() else (PROJECT_ROOT / path)


def load_source_health_manifest(settings: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    settings = settings or load_settings()
    path = source_health_manifest_path(settings)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    if isinstance(payload.get("sources"), dict):
        payload = payload["sources"]
    result: dict[str, dict[str, Any]] = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            result[str(key)] = value
    return result


def manifest_entry(
    source_key: str,
    settings: dict[str, Any] | None = None,
    manifest: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    manifest = manifest or load_source_health_manifest(settings)
    entry = manifest.get(str(source_key)) or {}
    return entry if isinstance(entry, dict) else {}


def _entry_values(entry: dict[str, Any], key: str, variant: str | int | None = None) -> list[str]:
    if not entry:
        return []
    raw = entry.get(key)
    if isinstance(raw, dict):
        values: list[str] = []
        probes: list[Any] = []
        if variant is not None:
            probes.extend([str(variant), variant])
        probes.extend(["default", "*"])
        for probe in probes:
            if probe not in raw:
                continue
            nested = raw[probe]
            if isinstance(nested, list):
                values.extend(str(item) for item in nested if item)
            elif nested:
                values.append(str(nested))
        return values
    if isinstance(raw, list):
        return [str(item) for item in raw if item]
    if raw:
        return [str(raw)]
    return []


def primary_urls(entry: dict[str, Any], *, variant: str | int | None = None) -> list[str]:
    return _entry_values(entry, "primary_urls", variant)


def fallback_urls(entry: dict[str, Any], *, variant: str | int | None = None) -> list[str]:
    return _entry_values(entry, "fallback_urls", variant)


def fixture_paths(
    entry: dict[str, Any],
    *,
    variant: str | int | None = None,
    settings: dict[str, Any] | None = None,
) -> list[Path]:
    settings = settings or {}
    path = source_health_manifest_path(settings)
    base_dir = path.parent
    result: list[Path] = []
    for raw in _entry_values(entry, "fixture_paths", variant):
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = (base_dir / candidate).resolve()
        result.append(candidate)
    return result


def find_fixture_path(
    source_key: str,
    *,
    variant: str | int | None = None,
    settings: dict[str, Any] | None = None,
    manifest: dict[str, dict[str, Any]] | None = None,
) -> Path | None:
    entry = manifest_entry(source_key, settings=settings, manifest=manifest)
    for candidate in fixture_paths(entry, variant=variant, settings=settings):
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def fixture_checksum(path: str | Path) -> str:
    file_path = Path(path)
    hasher = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def expected_quality(entry: dict[str, Any]) -> str:
    return str(entry.get("quality_expectations") or "").strip()


def acceptance_mode(entry: dict[str, Any]) -> str:
    return str(entry.get("acceptance_mode") or "direct_only").strip() or "direct_only"


def required_for_gate(entry: dict[str, Any]) -> bool:
    return bool(entry.get("required_for_gate"))


def _smoke_ok(expectation: str, text: str) -> tuple[bool, str | None]:
    lowered = text.lower()
    has_link = "<a" in lowered or "href=" in lowered
    has_table = "<table" in lowered
    if expectation == "html_listing":
        return (has_link and ("новост" in lowered or "правитель" in lowered or "government" in lowered), "html_listing_shape")
    if expectation == "document_listing":
        return (has_link and ("док" in lowered or "document" in lowered or "правитель" in lowered), "document_listing_shape")
    if expectation in {"official_publication", "official_documents"}:
        return (has_link and ("право" in lowered or "официаль" in lowered or "publication" in lowered), "official_publication_shape")
    if expectation == "archive_listing":
        return (has_link and ("архив" in lowered or "archive" in lowered or "rosreestr" in lowered), "archive_listing_shape")
    if expectation == "news_listing":
        return (has_link and ("нов" in lowered or "news" in lowered or "rosreestr" in lowered), "news_listing_shape")
    if expectation == "declaration_table":
        return (has_table and ("доход" in lowered or "фио" in lowered), "declaration_table_shape")
    if expectation == "management_page":
        return (("руковод" in lowered or "management" in lowered or "директор" in lowered or "генераль" in lowered), "management_page_shape")
    if expectation == "directory_listing":
        return (has_link and ("сенатор" in lowered or "совет федерации" in lowered or "council" in lowered), "directory_listing_shape")
    return (bool(text.strip()), None)


def smoke_fixture(
    source_key: str,
    *,
    settings: dict[str, Any] | None = None,
    variant: str | int | None = None,
    manifest: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    settings = settings or {}
    manifest = manifest or load_source_health_manifest(settings)
    entry = manifest_entry(source_key, settings=settings, manifest=manifest)
    path = find_fixture_path(source_key, variant=variant, settings=settings, manifest=manifest)
    mode = acceptance_mode(entry)
    if path is None:
        return {
            "ok": False,
            "source_key": source_key,
            "effective_state": "blocked",
            "failure_class": "missing_fixture",
            "fixture_path": None,
            "acceptance_mode": mode,
            "archive_derived": mode == "archive_ok",
        }
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception as error:
        return {
            "ok": False,
            "source_key": source_key,
            "effective_state": "blocked",
            "failure_class": "fixture_read_error",
            "error": str(error),
            "fixture_path": str(path),
            "acceptance_mode": mode,
            "archive_derived": mode == "archive_ok",
        }
    ok, failure_class = _smoke_ok(expected_quality(entry), text)
    effective_state = "healthy_archive" if ok and mode == "archive_ok" else "healthy_fixture" if ok else "degraded_parser"
    return {
        "ok": ok,
        "source_key": source_key,
        "effective_state": effective_state,
        "failure_class": None if ok else failure_class or "fixture_shape_mismatch",
        "fixture_path": str(path),
        "acceptance_mode": mode,
        "archive_derived": mode == "archive_ok",
        "checksum": fixture_checksum(path),
    }


def match_warning_source(
    warning_text: str,
    *,
    settings: dict[str, Any] | None = None,
    manifest: dict[str, dict[str, Any]] | None = None,
) -> str | None:
    text = str(warning_text or "").lower()
    if not text:
        return None
    manifest = manifest or load_source_health_manifest(settings)
    for source_key, entry in manifest.items():
        for marker in entry.get("warning_match", []) or []:
            if str(marker).lower() in text:
                return source_key
        if source_key.lower() in text:
            return source_key
    if "snapshot_not_found" in text or "duma:" in text:
        return "duma_disclosures"
    return None


def effective_source_state(
    *,
    state: str | None,
    quality_state: str | None,
    failure_class: str | None,
    metadata: dict[str, Any] | None,
    manifest_entry_value: dict[str, Any] | None,
    fixture_smoke: dict[str, Any] | None = None,
) -> str:
    metadata = metadata or {}
    manifest_entry_value = manifest_entry_value or {}
    mode = acceptance_mode(manifest_entry_value)
    normalized_quality = str(quality_state or "").strip().lower()
    normalized_state = str(state or "").strip().lower()
    has_resolved_fallback = bool(
        metadata.get("archive_derived")
        or metadata.get("fallback_used") in {"archive", "fixture"}
        or metadata.get("fixture_id")
    )
    if normalized_state == "degraded" and not has_resolved_fallback:
        raw_state = "degraded"
    else:
        raw_state = normalized_quality if normalized_quality and normalized_quality != "unknown" else normalized_state or "unknown"
    if raw_state in {"ok", "warning"}:
        if metadata.get("archive_derived") or metadata.get("fallback_used") == "archive":
            return "healthy_archive"
        if metadata.get("fallback_used") == "fixture" or metadata.get("fixture_id"):
            return "healthy_archive" if mode == "archive_ok" else "healthy_fixture"
        if raw_state == "ok":
            return "healthy_live"
        if (failure_class or "").strip().lower() in {"runtime_error", "fixture_shape_mismatch"}:
            return "degraded_parser"
        return "degraded_live"
    if fixture_smoke and fixture_smoke.get("ok"):
        return str(fixture_smoke.get("effective_state") or ("healthy_archive" if mode == "archive_ok" else "healthy_fixture"))
    if (failure_class or "").strip().lower() in TRANSPORT_FAILURE_CLASSES:
        return "degraded_live"
    if (failure_class or "").strip().lower() in {"runtime_error", "fixture_shape_mismatch"}:
        return "degraded_parser"
    return "blocked"
