from __future__ import annotations

import mimetypes
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from requests import exceptions as requests_exceptions
import urllib3

from db.file_store import attach_file
from enrichment.common import (
    clean_text,
    ensure_content_item,
    ensure_dir,
    ensure_entity_media,
    ensure_raw_item,
    maybe_parse_extra_photo,
    parse_json,
    open_db,
    resolve_source_for_url,
    slugify,
)


SKIP_PHOTO_PATTERNS = (
    "map.svg",
    "placeholder",
    "avatar-placeholder",
    "no-photo",
    "/icons/",
    "/icon/",
    "/banner",
    "/banners/",
)
ALLOW_INSECURE_PHOTO_HOSTS = {"roskazna.gov.ru"}


def _extension_for_response(url: str, mime_type: str) -> str:
    ext = mimetypes.guess_extension((mime_type or "").split(";")[0].strip()) or ""
    if ext:
        return ext
    path = urlparse(url).path
    if "." in path.rsplit("/", 1)[-1]:
        return f".{path.rsplit('.', 1)[-1]}"
    return ".bin"


def _is_bad_photo_candidate(photo_url: str) -> bool:
    lowered = clean_text(photo_url).lower()
    if not lowered:
        return True
    return any(token in lowered for token in SKIP_PHOTO_PATTERNS)


def _fetch_photo(session: requests.Session, photo_url: str) -> requests.Response:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = session.get(photo_url, timeout=20, headers=headers)
        response.raise_for_status()
        return response
    except requests_exceptions.SSLError:
        host = (urlparse(photo_url).hostname or "").lower()
        if host in ALLOW_INSECURE_PHOTO_HOSTS:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            response = session.get(photo_url, timeout=20, headers=headers, verify=False)
            response.raise_for_status()
            return response
        raise


def _collect_candidates(conn, limit: int):
    candidates: dict[int, dict[str, Any]] = {}
    rows = conn.execute(
        """
        SELECT
            dp.entity_id,
            dp.full_name,
            dp.position,
            dp.biography_url,
            dp.photo_url,
            e.extra_data
        FROM deputy_profiles dp
        JOIN entities e ON e.id = dp.entity_id
        WHERE (COALESCE(dp.photo_url, '') <> '' OR COALESCE(dp.biography_url, '') <> '')
          AND NOT EXISTS (
              SELECT 1 FROM entity_media em
              WHERE em.entity_id = dp.entity_id AND em.media_kind = 'photo'
          )
        ORDER BY dp.entity_id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    for row in rows:
        candidates[int(row["entity_id"])] = {
            "entity_id": int(row["entity_id"]),
            "full_name": clean_text(row["full_name"]),
            "position": clean_text(row["position"]),
            "profile_url": clean_text(row["biography_url"]),
            "photo_url": clean_text(row["photo_url"]),
            "profile_kind": "deputy_profile",
        }
    if len(candidates) >= limit:
        return list(candidates.values())[:limit]

    rows = conn.execute(
        """
        SELECT DISTINCT
            op.entity_id,
            e.canonical_name,
            COALESCE(op.position_title, e.description) AS position_title,
            op.source_url,
            e.extra_data
        FROM official_positions op
        JOIN entities e ON e.id = op.entity_id
        WHERE op.is_active=1
          AND COALESCE(op.source_url, '') <> ''
          AND NOT EXISTS (
              SELECT 1 FROM entity_media em
              WHERE em.entity_id = op.entity_id AND em.media_kind = 'photo'
          )
        ORDER BY op.entity_id
        LIMIT ?
        """,
        (max(limit * 4, 200),),
    ).fetchall()
    for row in rows:
        entity_id = int(row["entity_id"])
        if entity_id in candidates:
            continue
        candidates[entity_id] = {
            "entity_id": entity_id,
            "full_name": clean_text(row["canonical_name"]),
            "position": clean_text(row["position_title"]),
            "profile_url": clean_text(row["source_url"]),
            "photo_url": maybe_parse_extra_photo(row["extra_data"]),
            "profile_kind": "official_profile",
            "extra_data": row["extra_data"],
        }
        if len(candidates) >= limit:
            return list(candidates.values())[:limit]

    rows = conn.execute(
        """
        SELECT e.id AS entity_id, e.canonical_name, e.description, e.extra_data
        FROM entities e
        WHERE e.entity_type='person'
          AND COALESCE(e.extra_data, '') <> ''
          AND NOT EXISTS (
              SELECT 1 FROM entity_media em
              WHERE em.entity_id = e.id AND em.media_kind = 'photo'
          )
        ORDER BY e.id
        LIMIT ?
        """,
        (max(limit * 3, 100),),
    ).fetchall()
    for row in rows:
        entity_id = int(row["entity_id"])
        if entity_id in candidates:
            continue
        photo_url = maybe_parse_extra_photo(row["extra_data"])
        if not photo_url:
            continue
        candidates[entity_id] = {
            "entity_id": entity_id,
            "full_name": clean_text(row["canonical_name"]),
            "position": clean_text(row["description"]),
            "profile_url": "",
            "photo_url": photo_url,
            "profile_kind": "official_profile",
        }
        if len(candidates) >= limit:
            break
    return list(candidates.values())[:limit]


def run_photo_backfill(settings: dict[str, Any] | None = None, *, limit: int = 100) -> dict[str, Any]:
    settings = settings or {}
    conn = open_db(settings)
    effective_limit = int(settings.get("photo_backfill_limit", limit) or limit)
    processed_root = ensure_dir(Path(settings.get("processed_documents") or Path("processed") / "documents") / "entity_media")
    session = requests.Session()
    created = 0
    updated = 0
    skipped_bad_assets = 0
    warnings: list[str] = []
    try:
        candidates = _collect_candidates(conn, effective_limit)
        for candidate in candidates:
            photo_url = clean_text(candidate.get("photo_url"))
            profile_url = clean_text(candidate.get("profile_url"))
            resolved_profile_url = profile_url
            if not photo_url and profile_url:
                try:
                    if "duma.gov.ru" in profile_url:
                        detail = __import__(
                            "collectors.deputy_profiles_scraper",
                            fromlist=["scrape_deputy_detail"],
                        ).scrape_deputy_detail(session, profile_url) or {}
                        photo_url = clean_text(detail.get("photo_url"))
                        resolved_profile_url = clean_text(detail.get("biography_url")) or profile_url
                    elif "council.gov.ru" in profile_url:
                        detail = __import__(
                            "collectors.senators_scraper",
                            fromlist=["scrape_senator_profile"],
                        ).scrape_senator_profile(session, profile_url) or {}
                        photo_url = clean_text(detail.get("photo_url"))
                        resolved_profile_url = profile_url
                    else:
                        response = session.get(profile_url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
                        response.raise_for_status()
                        detail = __import__(
                            "collectors.executive_directory_scraper",
                            fromlist=["parse_profile_page"],
                        ).parse_profile_page(response.text, response.url)
                        photo_url = clean_text(detail.get("photo_url"))
                        resolved_profile_url = clean_text(detail.get("profile_url")) or response.url or profile_url
                except Exception as error:
                    warnings.append(f"{candidate['entity_id']}: resolve-photo {error}")
            if not photo_url:
                continue
            if _is_bad_photo_candidate(photo_url):
                skipped_bad_assets += 1
                continue
            try:
                response = _fetch_photo(session, photo_url)
            except Exception as error:
                warnings.append(f"{candidate['entity_id']}: {error}")
                continue

            mime_type = clean_text(response.headers.get("Content-Type")) or "application/octet-stream"
            if "svg" in mime_type.lower():
                skipped_bad_assets += 1
                continue
            ext = _extension_for_response(photo_url, mime_type)
            photo_dir = ensure_dir(processed_root / "photos")
            file_name = f"{candidate['entity_id']}-{slugify(candidate['full_name'], 'photo')}{ext}"
            file_path = photo_dir / file_name
            file_path.write_bytes(response.content)

            source_id = resolve_source_for_url(
                conn,
                url=resolved_profile_url or photo_url,
                fallback_name="Entity profile media",
                fallback_category="official_site",
                fallback_subcategory="profile_media",
                is_official=1,
            )
            external_id = f"profile:{candidate['profile_kind']}:{candidate['entity_id']}"
            raw_item_id = ensure_raw_item(
                conn,
                source_id=source_id,
                external_id=external_id,
                raw_payload={
                    "entity_id": candidate["entity_id"],
                    "full_name": candidate["full_name"],
                    "position": candidate["position"],
                    "profile_url": resolved_profile_url,
                    "photo_url": photo_url,
                    "profile_kind": candidate["profile_kind"],
                },
            )
            content_id = ensure_content_item(
                conn,
                source_id=source_id,
                raw_item_id=raw_item_id,
                external_id=external_id,
                content_type=candidate["profile_kind"],
                title=candidate["full_name"],
                body_text="\n".join(
                    part
                    for part in (
                        candidate["position"],
                        f"Профиль: {resolved_profile_url}" if resolved_profile_url else "",
                        f"Фото: {photo_url}",
                    )
                    if part
                ),
                published_at=None,
                url=resolved_profile_url or photo_url,
            )
            attachment_id = attach_file(
                conn,
                content_id,
                raw_item_id,
                file_path,
                "photo",
                original_url=photo_url,
                mime_type=mime_type,
                legacy_paths=[photo_url],
            )
            media_id = ensure_entity_media(
                conn,
                entity_id=candidate["entity_id"],
                attachment_id=attachment_id,
                media_kind="photo",
                source_url=photo_url,
                caption=candidate["full_name"],
                is_primary=1,
                metadata={"profile_kind": candidate["profile_kind"]},
            )
            if candidate["profile_kind"] == "deputy_profile":
                conn.execute(
                    "UPDATE deputy_profiles SET photo_url=COALESCE(NULLIF(photo_url, ''), ?), biography_url=COALESCE(NULLIF(biography_url, ''), ?) WHERE entity_id=?",
                    (photo_url, resolved_profile_url or None, candidate["entity_id"]),
                )
            else:
                row = conn.execute("SELECT extra_data FROM entities WHERE id=?", (candidate["entity_id"],)).fetchone()
                extra = parse_json(row[0] if row else None, default={})
                if not isinstance(extra, dict):
                    extra = {}
                if not clean_text(extra.get("photo_url")):
                    extra["photo_url"] = photo_url
                if resolved_profile_url and not clean_text(extra.get("profile_url")):
                    extra["profile_url"] = resolved_profile_url
                conn.execute(
                    "UPDATE entities SET extra_data=? WHERE id=?",
                    (json.dumps(extra, ensure_ascii=False), candidate["entity_id"]),
                )
            if media_id:
                created += 1
            else:
                updated += 1
        conn.commit()
        return {
            "ok": True,
            "items_seen": len(candidates),
            "items_new": created,
            "items_updated": updated,
            "warnings": warnings,
            "artifacts": {"skipped_bad_assets": skipped_bad_assets},
        }
    finally:
        conn.close()
