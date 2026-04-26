from __future__ import annotations

import json
from typing import Any

from enrichment.common import (
    clean_text,
    ensure_content_item,
    ensure_raw_item,
    maybe_parse_extra_photo,
    normalize_text,
    open_db,
    parse_json,
    resolve_source_for_url,
)


def _normalize_existing_profile_rows(conn) -> int:
    updated = 0
    rows = conn.execute(
        """
        SELECT c.id, c.url, s.url AS source_url, s.subcategory, rs.raw_payload
        FROM content_items c
        LEFT JOIN sources s ON s.id = c.source_id
        LEFT JOIN raw_source_items rs ON rs.id = c.raw_item_id
        WHERE c.content_type='profile'
        ORDER BY c.id
        """
    ).fetchall()
    for row in rows:
        payload = parse_json(row["raw_payload"], {})
        haystack = " ".join(
            clean_text(value)
            for value in (
                row["url"],
                row["source_url"],
                row["subcategory"],
                payload.get("source_key") if isinstance(payload, dict) else "",
                payload.get("organization") if isinstance(payload, dict) else "",
            )
            if clean_text(value)
        ).lower()
        target_type = "official_profile"
        if "duma" in haystack or "deput" in haystack or "парламент" in haystack:
            target_type = "deputy_profile"
        if row["url"] and "council.gov" in row["url"]:
            target_type = "deputy_profile"
        if target_type != "profile":
            conn.execute("UPDATE content_items SET content_type=? WHERE id=?", (target_type, row["id"]))
            updated += 1
    return updated


def _materialize_deputy_profiles(conn) -> tuple[int, int]:
    created = 0
    updated = 0
    rows = conn.execute(
        """
        SELECT
            dp.entity_id,
            dp.full_name,
            dp.position,
            dp.faction,
            dp.region,
            dp.committee,
            dp.biography_url,
            dp.photo_url
        FROM deputy_profiles dp
        WHERE dp.is_active=1
        ORDER BY dp.entity_id
        """
    ).fetchall()
    for row in rows:
        profile_url = clean_text(row["biography_url"])
        source_id = resolve_source_for_url(
            conn,
            url=profile_url or "https://duma.gov.ru/deputies/",
            fallback_name="Профили должностных лиц",
            fallback_category="official_site",
            fallback_subcategory="parliament",
            is_official=1,
        )
        external_id = f"dossier:deputy_profile:{row['entity_id']}"
        raw_item_id = ensure_raw_item(
            conn,
            source_id=source_id,
            external_id=external_id,
            raw_payload={
                "entity_id": int(row["entity_id"]),
                "full_name": row["full_name"],
                "position": row["position"],
                "faction": row["faction"],
                "region": row["region"],
                "committee": row["committee"],
                "profile_url": profile_url,
                "photo_url": clean_text(row["photo_url"]),
            },
        )
        existing = conn.execute(
            "SELECT id FROM content_items WHERE source_id=? AND external_id=? LIMIT 1",
            (source_id, external_id),
        ).fetchone()
        ensure_content_item(
            conn,
            source_id=source_id,
            raw_item_id=raw_item_id,
            external_id=external_id,
            content_type="deputy_profile",
            title=clean_text(row["full_name"]),
            body_text="\n".join(
                part
                for part in (
                    clean_text(row["position"]),
                    f"Фракция: {clean_text(row['faction'])}" if clean_text(row["faction"]) else "",
                    f"Регион: {clean_text(row['region'])}" if clean_text(row["region"]) else "",
                    f"Комитет: {clean_text(row['committee'])}" if clean_text(row["committee"]) else "",
                    f"Профиль: {profile_url}" if profile_url else "",
                    f"Фото: {clean_text(row['photo_url'])}" if clean_text(row["photo_url"]) else "",
                )
                if part
            ),
            published_at=None,
            url=profile_url or clean_text(row["photo_url"]),
        )
        if existing:
            updated += 1
        else:
            created += 1
    return created, updated


def _materialize_official_profiles(conn) -> tuple[int, int]:
    created = 0
    updated = 0
    rows = conn.execute(
        """
        SELECT
            e.id AS entity_id,
            e.canonical_name,
            e.description,
            e.extra_data,
            op.position_title,
            op.organization,
            op.region,
            op.source_url,
            op.source_type
        FROM official_positions op
        JOIN entities e ON e.id = op.entity_id
        WHERE op.is_active=1
        ORDER BY e.id, op.id DESC
        """
    ).fetchall()
    seen: set[int] = set()
    for row in rows:
        entity_id = int(row["entity_id"])
        if entity_id in seen:
            continue
        seen.add(entity_id)
        extra = parse_json(row["extra_data"], {})
        profile_url = clean_text((extra or {}).get("profile_url") if isinstance(extra, dict) else "") or clean_text(row["source_url"])
        photo_url = maybe_parse_extra_photo(row["extra_data"])
        source_id = resolve_source_for_url(
            conn,
            url=profile_url or "https://government.ru/",
            fallback_name="Профили руководителей",
            fallback_category="official_site",
            fallback_subcategory="executive_directory",
            is_official=1,
        )
        external_id = f"dossier:official_profile:{entity_id}"
        raw_item_id = ensure_raw_item(
            conn,
            source_id=source_id,
            external_id=external_id,
            raw_payload={
                "entity_id": entity_id,
                "full_name": row["canonical_name"],
                "position_title": row["position_title"],
                "organization": row["organization"],
                "region": row["region"],
                "profile_url": profile_url,
                "photo_url": photo_url,
                "source_type": row["source_type"],
            },
        )
        existing = conn.execute(
            "SELECT id FROM content_items WHERE source_id=? AND external_id=? LIMIT 1",
            (source_id, external_id),
        ).fetchone()
        ensure_content_item(
            conn,
            source_id=source_id,
            raw_item_id=raw_item_id,
            external_id=external_id,
            content_type="official_profile",
            title=clean_text(row["canonical_name"]),
            body_text="\n".join(
                part
                for part in (
                    clean_text(row["position_title"]) or clean_text(row["description"]),
                    f"Организация: {clean_text(row['organization'])}" if clean_text(row["organization"]) else "",
                    f"Регион: {clean_text(row['region'])}" if clean_text(row["region"]) else "",
                    f"Источник: {clean_text(row['source_type'])}" if clean_text(row["source_type"]) else "",
                    f"Профиль: {profile_url}" if profile_url else "",
                    f"Фото: {photo_url}" if photo_url else "",
                )
                if part
            ),
            published_at=None,
            url=profile_url,
        )
        if existing:
            updated += 1
        else:
            created += 1
    return created, updated


def run_profiles_enrichment(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    settings = dict(settings or {})
    warnings: list[str] = []
    collection_stats: dict[str, Any] = {}

    collect_live = bool(settings.get("profiles_enrichment_collect", True))
    if collect_live:
        collector_settings = dict(settings)
        collector_settings["deputies_fetch_details"] = True
        collector_settings["deputies_html_pages"] = int(settings.get("deputies_html_pages", 4) or 4)
        try:
            module = __import__("collectors.deputy_profiles_scraper", fromlist=["collect_deputies_html", "collect_deputies_api"])
            if settings.get("duma_api_token"):
                collection_stats["deputies_api"] = module.collect_deputies_api(collector_settings) or 0
            else:
                collection_stats["deputies_html"] = module.collect_deputies_html(
                    collector_settings,
                    fetch_details=True,
                    max_pages=int(collector_settings["deputies_html_pages"]),
                ) or 0
        except Exception as error:
            warnings.append(f"deputies:{error}")
        try:
            collection_stats["senators"] = __import__(
                "collectors.senators_scraper",
                fromlist=["collect_senators"],
            ).collect_senators(fetch_profiles=True) or 0
        except Exception as error:
            warnings.append(f"senators:{error}")
        try:
            collection_stats["executive_directory"] = __import__(
                "collectors.executive_directory_scraper",
                fromlist=["collect_executive_directories"],
            ).collect_executive_directories(collector_settings) or {}
        except Exception as error:
            warnings.append(f"executive_directory:{error}")

    conn = open_db(settings)
    try:
        normalized = _normalize_existing_profile_rows(conn)
        deputy_created, deputy_updated = _materialize_deputy_profiles(conn)
        official_created, official_updated = _materialize_official_profiles(conn)
        conn.commit()
        return {
            "ok": True,
            "items_seen": deputy_created + deputy_updated + official_created + official_updated,
            "items_new": deputy_created + official_created,
            "items_updated": deputy_updated + official_updated + normalized,
            "warnings": warnings,
            "artifacts": {
                "normalized_profile_rows": normalized,
                "deputy_profile_content": {"created": deputy_created, "updated": deputy_updated},
                "official_profile_content": {"created": official_created, "updated": official_updated},
                **collection_stats,
            },
        }
    finally:
        conn.close()

