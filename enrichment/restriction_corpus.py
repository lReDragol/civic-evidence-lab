from __future__ import annotations

from typing import Any

from enrichment.common import (
    clean_text,
    ensure_content_item,
    ensure_raw_item,
    ensure_review_task,
    find_person_entity,
    json_dumps,
    open_db,
)


def _restriction_type(title: str, body: str, source_name: str) -> tuple[str, str]:
    haystack = " ".join(part for part in (title, body, source_name) if part).lower()
    if "иноагент" in haystack or "foreign agent" in haystack:
        return "foreign_agent_registry", "association"
    if "блок" in haystack or "запрещ" in haystack or "ркн" in haystack:
        return "internet_block", "internet"
    if "митинг" in haystack or "публичн" in haystack:
        return "assembly_restriction", "assembly"
    if "арест" in haystack or "задерж" in haystack or "страж" in haystack:
        return "detention", "speech"
    return "official_restriction", "information"


def _issuer_guess(conn, source_name: str, source_url: str) -> int | None:
    for candidate in (
        "Роскомнадзор" if "rkn" in (source_url or "").lower() or "роскомнадзор" in (source_name or "").lower() else "",
        "Министерство юстиции Российской Федерации" if "minjust" in (source_url or "").lower() or "минюст" in (source_name or "").lower() else "",
        "Генеральная прокуратура Российской Федерации" if "genproc" in (source_url or "").lower() else "",
    ):
        if not candidate:
            continue
        entity_id = find_person_entity(conn, candidate)
        if entity_id:
            return entity_id
        row = conn.execute(
            "SELECT id FROM entities WHERE entity_type='organization' AND canonical_name=? LIMIT 1",
            (candidate,),
        ).fetchone()
        if row:
            return int(row[0])
    return None


def _looks_like_person(name: str) -> bool:
    tokens = [part for part in clean_text(name).replace('"', " ").split() if part]
    if len(tokens) < 2 or len(tokens) > 4:
        return False
    blocked = {"проект", "фонд", "общество", "институт", "центр", "партия", "организация", "агентство", "редакция"}
    lowered = {token.casefold() for token in tokens}
    return not any(token in blocked for token in lowered)


def _resolve_target_entity(conn, target_name: str) -> int | None:
    cleaned = clean_text(target_name)
    if not cleaned:
        return None
    stripped = cleaned
    for prefix in ("Иноагент:", "Иноагент", "Реестр:", "Блокировка:", "Запрет:"):
        if stripped.startswith(prefix):
            stripped = clean_text(stripped[len(prefix):])
            break
    person_entity_id = find_person_entity(conn, stripped)
    if person_entity_id:
        return person_entity_id
    for entity_type in ("organization", "person"):
        row = conn.execute(
            "SELECT id FROM entities WHERE entity_type=? AND canonical_name=? LIMIT 1",
            (entity_type, stripped),
        ).fetchone()
        if row:
            return int(row[0])
    entity_type = "person" if _looks_like_person(stripped) else "organization"
    cur = conn.execute(
        "INSERT INTO entities(entity_type, canonical_name, description) VALUES(?,?,?)",
        (entity_type, stripped, "Restriction corpus target"),
    )
    return int(cur.lastrowid)


def build_restriction_corpus(settings: dict[str, Any] | None = None, *, limit: int = 500) -> dict[str, Any]:
    settings = settings or {}
    conn = open_db(settings)
    created = 0
    updated = 0
    try:
        rows = conn.execute(
            """
            SELECT c.id, c.source_id, c.external_id, c.content_type, c.title, c.body_text, c.published_at, c.url,
                   s.name AS source_name, s.category, s.subcategory, s.is_official, s.url AS source_url
            FROM content_items c
            JOIN sources s ON s.id = c.source_id
            WHERE s.subcategory='restrictions'
               OR lower(s.url) LIKE '%minjust%'
               OR lower(s.url) LIKE '%rkn%'
               OR lower(c.content_type) IN ('registry_record', 'court_case')
            ORDER BY COALESCE(c.published_at, c.collected_at, c.id) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        for row in rows:
            restriction_type, right_category = _restriction_type(row["title"] or "", row["body_text"] or "", row["source_name"] or "")
            evidence_class = "hard" if int(row["is_official"] or 0) else "support"
            normalized_type = "restriction_record" if restriction_type != "detention" else "court_case"
            raw_item_id = ensure_raw_item(
                conn,
                source_id=int(row["source_id"]),
                external_id=f"restriction:{row['id']}",
                raw_payload={
                    "source_content_id": int(row["id"]),
                    "restriction_type": restriction_type,
                    "right_category": right_category,
                },
            )
            if normalized_type != row["content_type"]:
                ensure_content_item(
                    conn,
                    source_id=int(row["source_id"]),
                    raw_item_id=raw_item_id,
                    external_id=row["external_id"] or f"restriction:{row['id']}",
                    content_type=normalized_type,
                    title=row["title"] or "",
                    body_text=row["body_text"] or "",
                    published_at=row["published_at"],
                    url=row["url"],
                    status="official_document" if evidence_class == "hard" else row["content_type"],
                )
                updated += 1
            issuer_entity_id = _issuer_guess(conn, clean_text(row["source_name"]), clean_text(row["source_url"]))
            target_name = clean_text(row["title"]) or clean_text(row["body_text"][:160])
            target_entity_id = _resolve_target_entity(conn, target_name)
            existing = conn.execute(
                """
                SELECT id
                FROM restriction_events
                WHERE source_content_id=? AND restriction_type=?
                LIMIT 1
                """,
                (int(row["id"]), restriction_type),
            ).fetchone()
            payload = (
                issuer_entity_id,
                target_entity_id,
                target_name,
                None,
                restriction_type,
                right_category,
                None,
                clean_text(row["body_text"][:280]) or None,
                clean_text(row["published_at"]) or None,
                int(row["id"]),
                clean_text(row["url"]) or None,
                evidence_class,
                "high" if evidence_class == "hard" else "moderate",
                "open",
                json_dumps({"source_name": row["source_name"], "source_subcategory": row["subcategory"]}),
            )
            if existing:
                event_id = int(existing[0])
                conn.execute(
                    """
                    UPDATE restriction_events
                    SET issuer_entity_id=?, target_entity_id=?, target_name=?, region=?, restriction_type=?,
                        right_category=?, legal_basis=?, stated_justification=?, event_date=?, source_content_id=?,
                        source_url=?, evidence_class=?, severity=?, status=?, metadata_json=?, updated_at=datetime('now')
                    WHERE id=?
                    """,
                    payload + (event_id,),
                )
            else:
                cur = conn.execute(
                    """
                    INSERT INTO restriction_events(
                        issuer_entity_id, target_entity_id, target_name, region, restriction_type, right_category,
                        legal_basis, stated_justification, event_date, source_content_id, source_url,
                        evidence_class, severity, status, metadata_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    payload,
                )
                event_id = int(cur.lastrowid)
                created += 1
            ensure_review_task(
                conn,
                task_key=f"restriction:{row['id']}",
                queue_key="restrictions_justifications",
                subject_type="restriction_event",
                subject_id=event_id,
                candidate_payload={
                    "restriction_event_id": event_id,
                    "source_content_id": int(row["id"]),
                    "target_entity_id": target_entity_id,
                    "restriction_type": restriction_type,
                    "right_category": right_category,
                    "source_name": row["source_name"],
                },
                suggested_action="promote" if evidence_class == "hard" else "needs_more_docs",
                confidence=0.9 if evidence_class == "hard" else 0.62,
                machine_reason="Official restriction corpus event",
                source_links=[row["url"]] if clean_text(row["url"]) else [],
            )
        conn.commit()
        return {
            "ok": True,
            "items_seen": len(rows),
            "items_new": created,
            "items_updated": updated,
        }
    finally:
        conn.close()
