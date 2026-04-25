from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

from PySide6.QtCore import QObject, Signal, Slot

from ui.job_registry import JOB_DEFS, get_job_def, interval_for_job, serialize_jobs


STRUCTURAL_RELATION_TYPES = {
    "works_at",
    "head_of",
    "party_member",
    "member_of",
    "member_of_committee",
    "represents_region",
    "sponsored_bill",
    "voted_for",
    "voted_against",
    "voted_abstained",
    "voted_absent",
}
WEAK_RELATION_TYPES = {"mentioned_together"}
STRENGTH_ORDER = {"strong": 0, "moderate": 1, "weak": 2}

NAVIGATION = [
    {
        "key": "monitoring",
        "label": "Мониторинг",
        "sections": [
            {"key": "overview", "label": "Обзор"},
            {"key": "content", "label": "Контент"},
            {"key": "search", "label": "Поиск"},
        ],
    },
    {
        "key": "verification",
        "label": "Проверка",
        "sections": [
            {"key": "claims", "label": "Заявления"},
            {"key": "cases", "label": "Дела"},
        ],
    },
    {
        "key": "analytics",
        "label": "Аналитика",
        "sections": [
            {"key": "entities", "label": "Сущности"},
            {"key": "relations", "label": "Связи"},
            {"key": "officials", "label": "Руководство"},
        ],
    },
    {
        "key": "system",
        "label": "Система",
        "sections": [
            {"key": "settings", "label": "Настройки"},
        ],
    },
]


class DashboardDataService:
    def __init__(self, db: sqlite3.Connection, settings: dict[str, Any] | None = None):
        self.db = db
        self.settings = settings or {}

    def bootstrap_payload(
        self,
        *,
        running_jobs: set[str] | None = None,
        scheduler_running: bool = False,
        logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return {
            "navigation": NAVIGATION,
            "summary": self.overview_payload(
                running_jobs=running_jobs or set(),
                scheduler_running=scheduler_running,
            ),
            "sources": self.sources_payload(),
            "jobs": self.jobs_payload(
                running_jobs=running_jobs or set(),
                scheduler_running=scheduler_running,
                logs=logs or [],
            ),
        }

    def overview_payload(
        self,
        *,
        running_jobs: set[str] | None = None,
        scheduler_running: bool = False,
    ) -> dict[str, Any]:
        running_jobs = running_jobs or set()
        counts = {
            "content": self._count("content_items"),
            "claims": self._count("claims"),
            "entities": self._count("entities"),
            "cases": self._count("cases"),
            "relations": self._count("entity_relations"),
            "officials": self._count_distinct("official_positions", "entity_id", "is_active=1"),
        }
        secondary_counts = {
            "persons": self._count_where("entities", "entity_type='person'"),
            "quotes": self._count("quotes"),
            "flagged_quotes": self._count_where("quotes", "is_flagged=1"),
            "tags": self._count_distinct("content_tags", "tag_name"),
            "sources": self._count_where("sources", "is_active=1"),
            "deputies": self._count_where("deputy_profiles", "is_active=1"),
            "evidence": self._count("evidence_links"),
            "attachments": self._count("attachments"),
            "bills": self._count("bills"),
            "votes": self._count("bill_vote_sessions"),
            "investigation_materials": self._count("investigative_materials"),
        }
        graph_health = {
            "evidence_backed_relations": self._count_where("entity_relations", "evidence_item_id IS NOT NULL"),
            "weak_relations": self._count_where("relation_candidates", "promotion_state IN ('pending', 'review')"),
            "promoted_candidates": self._count_where("relation_candidates", "promotion_state='promoted'"),
            "tagged_items": self._count_distinct("content_tags", "content_item_id"),
            "untagged_items": self._count_where(
                "content_items",
                "id NOT IN (SELECT DISTINCT content_item_id FROM content_tags)",
            ),
            "granular_pending": self._count_where("content_items", "COALESCE(granular_processed, 0)=0"),
            "dead_letters": self._count_where("dead_letter_items", "resolved_at IS NULL"),
            "degraded_sources": self._count_where("source_sync_state", "state='degraded'"),
            "pipeline_version": self._runtime_metadata("last_successful_pipeline_version"),
            "analysis_pipeline_version": self._runtime_metadata("analysis_built_from_pipeline_version"),
            "export_pipeline_version": self._runtime_metadata("obsidian_built_from_pipeline_version"),
            "analysis_generated_at": self._runtime_metadata("analysis_generated_at"),
            "export_generated_at": self._runtime_metadata("obsidian_export_generated_at"),
        }
        runtime_health = {
            "daemon_running": self._table_exists("job_leases")
            and self.db.execute(
                "SELECT COUNT(*) FROM job_leases WHERE job_id='__daemon__'"
            ).fetchone()[0]
            > 0,
            "running_jobs": self._count_where("job_leases", "job_id != '__daemon__'"),
            "failed_last_day": self._count_where(
                "job_runs",
                "status IN ('failed', 'abandoned') AND started_at >= datetime('now', '-1 day')",
            ),
            "pending_candidates": self._count_where("relation_candidates", "promotion_state IN ('pending', 'review')"),
            "degraded_sources": self._count_where("source_sync_state", "state='degraded'"),
            "dead_letters": self._count_where("dead_letter_items", "resolved_at IS NULL"),
        }
        low_accountability = []
        if self._table_exists("accountability_index") and self._table_exists("deputy_profiles"):
            low_accountability = [
                self._row_to_dict(row)
                for row in self.db.execute(
                    """
                    SELECT dp.full_name, dp.faction, ai.calculated_score,
                           ai.public_speeches_count, ai.flagged_statements_count, ai.linked_cases_count
                    FROM accountability_index ai
                    JOIN deputy_profiles dp ON dp.id = ai.deputy_id
                    WHERE ai.period = (SELECT MAX(period) FROM accountability_index)
                    ORDER BY ai.calculated_score ASC, dp.full_name
                    LIMIT 10
                    """
                ).fetchall()
            ]
        recent_content = [
            self._row_to_dict(row)
            for row in self.db.execute(
                """
                SELECT ci.id, ci.title, ci.content_type, ci.published_at, s.name AS source_name
                FROM content_items ci
                LEFT JOIN sources s ON s.id = ci.source_id
                ORDER BY COALESCE(ci.published_at, ci.collected_at, '') DESC, ci.id DESC
                LIMIT 8
                """
            ).fetchall()
        ]
        recent_cases = [
            self._row_to_dict(row)
            for row in self.db.execute(
                """
                SELECT c.id, c.title, c.status, c.case_type,
                       (SELECT COUNT(*) FROM case_claims cc WHERE cc.case_id = c.id) AS claims_count
                FROM cases c
                ORDER BY COALESCE(c.updated_at, c.created_at, '') DESC, c.id DESC
                LIMIT 6
                """
            ).fetchall()
        ]
        return {
            "counts": counts,
            "secondary_counts": secondary_counts,
            "graph_health": graph_health,
            "runtime_health": runtime_health,
            "low_accountability": low_accountability,
            "running_jobs": sorted(running_jobs),
            "scheduler_running": scheduler_running,
            "recent_content": recent_content,
            "recent_cases": recent_cases,
        }

    def sources_payload(self, search: str = "", category: str = "") -> dict[str, Any]:
        where = ["is_active = 1"]
        params: list[Any] = []
        raw_query = self._query(search)
        category = (category or "").strip()
        if category == "official":
            where.append("is_official = 1")
        elif category:
            where.append("category = ?")
            params.append(category)

        rows = self.db.execute(
            f"""
            SELECT id, name, category, url, is_official, credibility_tier
            FROM sources
            WHERE {' AND '.join(where)}
            ORDER BY is_official DESC, name
            LIMIT 500
            """,
            params,
        ).fetchall()

        pinned_ids = self._pinned_sources()
        groups = {
            "pinned": {"key": "pinned", "label": "Закреплённые", "items": []},
            "official": {"key": "official", "label": "Официальные", "items": []},
            "telegram": {"key": "telegram", "label": "Telegram", "items": []},
            "media": {"key": "media", "label": "СМИ", "items": []},
            "youtube": {"key": "youtube", "label": "YouTube", "items": []},
            "other": {"key": "other", "label": "Другое", "items": []},
        }
        for row in rows:
            item = self._row_to_dict(row)
            item["pinned"] = item["id"] in pinned_ids
            if raw_query and not self._contains_query(raw_query, item.get("name"), item.get("url")):
                continue
            if item["pinned"]:
                groups["pinned"]["items"].append(item)
            elif item["is_official"]:
                groups["official"]["items"].append(item)
            elif item["category"] in ("telegram", "media", "youtube"):
                groups[item["category"]]["items"].append(item)
            else:
                groups["other"]["items"].append(item)

        ordered_groups = [group for group in groups.values() if group["items"]]
        return {"groups": ordered_groups}

    def jobs_payload(
        self,
        *,
        running_jobs: set[str] | None = None,
        scheduler_running: bool = False,
        logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        items = serialize_jobs(self.settings, running_jobs or set())
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in items:
            grouped[item["group"]].append(item)
        return {
            "scheduler_running": scheduler_running,
            "items": items,
            "groups": [{"label": group, "items": grouped[group]} for group in grouped],
            "logs": logs or [],
        }

    def screen_payload(self, screen: str, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        filters = filters or {}
        screen = (screen or "overview").strip().lower()
        if screen == "overview":
            return self.overview_payload()
        if screen in {"content", "search"}:
            return self._content_screen(filters)
        if screen == "claims":
            return self._claims_screen(filters)
        if screen == "cases":
            return self._cases_screen(filters)
        if screen == "entities":
            return self._entities_screen(filters)
        if screen == "relations":
            return self._relations_screen(filters)
        if screen == "officials":
            return self._officials_screen(filters)
        if screen == "settings":
            return self._settings_screen()
        return {"items": [], "detail": None}

    def relation_layer(
        self,
        relation_type: str,
        detected_by: str | None,
        evidence_item_id: int | None = None,
    ) -> str:
        if evidence_item_id:
            return "evidence"
        if relation_type in WEAK_RELATION_TYPES or (detected_by or "").startswith("co_occurrence:"):
            return "weak_similarity"
        if relation_type in STRUCTURAL_RELATION_TYPES:
            return "structural"
        return "evidence"

    def relation_sort_key(self, item: dict[str, Any]) -> tuple:
        layer_priority = {
            "evidence": 0,
            "structural": 1,
            "weak_similarity": 2,
        }
        strength_priority = STRENGTH_ORDER.get(str(item.get("strength") or "").strip().lower(), 3)
        support_items, support_sources = self._co_occurrence_support(item.get("detected_by"))
        return (
            layer_priority.get(item.get("layer"), 3),
            strength_priority,
            -int(bool(item.get("evidence_item_id"))),
            -support_sources,
            -support_items,
            -int(item.get("id") or 0),
        )

    def entity_detail(self, entity_id: int | None) -> dict[str, Any] | None:
        if not entity_id:
            return None
        row = self.db.execute(
            "SELECT id, entity_type, canonical_name, description, inn, ogrn FROM entities WHERE id=?",
            (entity_id,),
        ).fetchone()
        if not row:
            return None

        positions = [
            self._row_to_dict(item)
            for item in self.db.execute(
                """
                SELECT id, position_title, organization, source_type, source_url, is_active
                FROM official_positions
                WHERE entity_id=?
                ORDER BY is_active DESC, id DESC
                """,
                (entity_id,),
            ).fetchall()
        ] if self._table_exists("official_positions") else []

        content = [
            self._row_to_dict(item)
            for item in self.db.execute(
                """
                SELECT DISTINCT ci.id, ci.title, ci.content_type, ci.published_at, ci.url
                FROM entity_mentions em
                JOIN content_items ci ON ci.id = em.content_item_id
                WHERE em.entity_id=?
                ORDER BY COALESCE(ci.published_at, ci.collected_at, '') DESC, ci.id DESC
                LIMIT 20
                """,
                (entity_id,),
            ).fetchall()
        ] if self._table_exists("entity_mentions") else []

        claims = [
            self._row_to_dict(item)
            for item in self.db.execute(
                """
                SELECT DISTINCT cl.id, cl.claim_text, cl.status, ci.id AS content_id, ci.title AS content_title
                FROM entity_mentions em
                JOIN claims cl ON cl.content_item_id = em.content_item_id
                JOIN content_items ci ON ci.id = cl.content_item_id
                WHERE em.entity_id=?
                ORDER BY cl.id DESC
                LIMIT 20
                """,
                (entity_id,),
            ).fetchall()
        ] if self._table_exists("claims") and self._table_exists("entity_mentions") else []

        cases = [
            self._row_to_dict(item)
            for item in self.db.execute(
                """
                SELECT DISTINCT c.id, c.title, c.status, c.case_type
                FROM entity_mentions em
                JOIN claims cl ON cl.content_item_id = em.content_item_id
                JOIN case_claims cc ON cc.claim_id = cl.id
                JOIN cases c ON c.id = cc.case_id
                WHERE em.entity_id=?
                ORDER BY c.id DESC
                LIMIT 20
                """,
                (entity_id,),
            ).fetchall()
        ] if self._table_exists("case_claims") and self._table_exists("cases") and self._table_exists("claims") and self._table_exists("entity_mentions") else []

        relations = []
        if self._table_exists("entity_relations"):
            relation_rows = self.db.execute(
                """
                SELECT er.id, er.from_entity_id, er.to_entity_id, er.relation_type, er.strength, er.detected_by, er.evidence_item_id,
                       ef.canonical_name AS from_name, et.canonical_name AS to_name
                FROM entity_relations er
                JOIN entities ef ON ef.id = er.from_entity_id
                JOIN entities et ON et.id = er.to_entity_id
                WHERE er.from_entity_id=? OR er.to_entity_id=?
                LIMIT 200
                """,
                (entity_id, entity_id),
            ).fetchall()
            for rel in relation_rows:
                item = self._row_to_dict(rel)
                item["layer"] = self.relation_layer(
                    item["relation_type"],
                    item.get("detected_by"),
                    item.get("evidence_item_id"),
                )
                relations.append(item)
            relations.sort(key=self.relation_sort_key)
            relations = relations[:30]

        detail = self._row_to_dict(row)
        detail["entity_id"] = detail["id"]
        detail.update(
            {
                "positions": positions,
                "content": content,
                "claims": claims,
                "cases": cases,
                "relations": relations,
            }
        )
        return detail

    def _content_screen(self, filters: dict[str, Any]) -> dict[str, Any]:
        where = ["1=1"]
        params: list[Any] = []
        source_id = filters.get("source_id")
        raw_query = self._query(filters.get("query") or "")
        if source_id:
            where.append("ci.source_id=?")
            params.append(int(source_id))
        rows = [
            self._row_to_dict(row)
            for row in self.db.execute(
                f"""
                SELECT ci.id, ci.title, ci.content_type, ci.status, ci.published_at, ci.url, s.name AS source_name
                FROM content_items ci
                LEFT JOIN sources s ON s.id = ci.source_id
                WHERE {' AND '.join(where)}
                ORDER BY COALESCE(ci.published_at, ci.collected_at, '') DESC, ci.id DESC
                LIMIT 120
                """,
                params,
            ).fetchall()
        ]
        if raw_query:
            rows = [
                row
                for row in rows
                if self._contains_query(raw_query, row.get("title"), row.get("source_name"), row.get("content_type"))
            ]
        selected_id = filters.get("selected_id") or (rows[0]["id"] if rows else None)
        detail = None
        if selected_id:
            row = self.db.execute(
                """
                SELECT ci.id, ci.title, ci.body_text, ci.content_type, ci.status, ci.published_at, ci.url, s.name AS source_name
                FROM content_items ci
                LEFT JOIN sources s ON s.id = ci.source_id
                WHERE ci.id=?
                """,
                (selected_id,),
            ).fetchone()
            if row:
                detail = self._row_to_dict(row)
                detail["entities"] = [
                    self._row_to_dict(item)
                    for item in self.db.execute(
                        """
                        SELECT e.id, e.canonical_name, e.entity_type, em.mention_type
                        FROM entity_mentions em
                        JOIN entities e ON e.id = em.entity_id
                        WHERE em.content_item_id=?
                        ORDER BY e.canonical_name
                        """,
                        (selected_id,),
                    ).fetchall()
                ] if self._table_exists("entity_mentions") else []
                detail["claims"] = [
                    self._row_to_dict(item)
                    for item in self.db.execute(
                        "SELECT id, claim_text, status FROM claims WHERE content_item_id=? ORDER BY id DESC",
                        (selected_id,),
                    ).fetchall()
                ] if self._table_exists("claims") else []
        return {"items": rows, "detail": detail}

    def _claims_screen(self, filters: dict[str, Any]) -> dict[str, Any]:
        where = ["1=1"]
        params: list[Any] = []
        status = (filters.get("status") or "").strip()
        raw_query = self._query(filters.get("query") or "")
        if status:
            where.append("cl.status=?")
            params.append(status)
        rows = [
            self._row_to_dict(row)
            for row in self.db.execute(
                f"""
                SELECT cl.id, cl.claim_text, cl.status, cl.needs_review,
                       ci.id AS content_id, ci.title AS content_title,
                       (SELECT c.id FROM case_claims cc JOIN cases c ON c.id = cc.case_id WHERE cc.claim_id = cl.id LIMIT 1) AS case_id
                FROM claims cl
                JOIN content_items ci ON ci.id = cl.content_item_id
                WHERE {' AND '.join(where)}
                ORDER BY cl.id DESC
                LIMIT 120
                """,
                params,
            ).fetchall()
        ] if self._table_exists("claims") else []
        if raw_query:
            rows = [
                row
                for row in rows
                if self._contains_query(raw_query, row.get("claim_text"), row.get("content_title"))
            ]
        selected_id = filters.get("selected_id") or (rows[0]["id"] if rows else None)
        detail = None
        if selected_id and self._table_exists("claims"):
            row = self.db.execute(
                """
                SELECT cl.id, cl.claim_text, cl.status, cl.claim_type, cl.confidence_final, cl.needs_review,
                       ci.id AS content_id, ci.title AS content_title, ci.url AS content_url
                FROM claims cl
                JOIN content_items ci ON ci.id = cl.content_item_id
                WHERE cl.id=?
                """,
                (selected_id,),
            ).fetchone()
            if row:
                detail = self._row_to_dict(row)
                detail["evidence"] = [
                    self._row_to_dict(item)
                    for item in self.db.execute(
                        """
                        SELECT el.id, el.evidence_type, el.strength, el.notes, ci.id AS evidence_item_id, ci.title AS evidence_title
                        FROM evidence_links el
                        LEFT JOIN content_items ci ON ci.id = el.evidence_item_id
                        WHERE el.claim_id=?
                        ORDER BY el.id DESC
                        """,
                        (selected_id,),
                    ).fetchall()
                ] if self._table_exists("evidence_links") else []
        return {"items": rows, "detail": detail}

    def _cases_screen(self, filters: dict[str, Any]) -> dict[str, Any]:
        raw_query = self._query(filters.get("query") or "")
        params: list[Any] = []
        where = ["1=1"]
        rows = [
            self._row_to_dict(row)
            for row in self.db.execute(
                f"""
                SELECT c.id, c.title, c.status, c.case_type, c.started_at,
                       (SELECT COUNT(*) FROM case_claims cc WHERE cc.case_id = c.id) AS claims_count
                FROM cases c
                WHERE {' AND '.join(where)}
                ORDER BY COALESCE(c.updated_at, c.created_at, '') DESC, c.id DESC
                LIMIT 120
                """,
                params,
            ).fetchall()
        ] if self._table_exists("cases") else []
        if raw_query:
            rows = [
                row
                for row in rows
                if self._contains_query(raw_query, row.get("title"), row.get("case_type"), row.get("status"))
            ]
        selected_id = filters.get("selected_id") or (rows[0]["id"] if rows else None)
        detail = None
        if selected_id and self._table_exists("cases"):
            row = self.db.execute(
                "SELECT id, title, description, status, case_type, started_at, closed_at FROM cases WHERE id=?",
                (selected_id,),
            ).fetchone()
            if row:
                detail = self._row_to_dict(row)
                detail["claims"] = [
                    self._row_to_dict(item)
                    for item in self.db.execute(
                        """
                        SELECT cl.id, cl.claim_text, cl.status
                        FROM case_claims cc
                        JOIN claims cl ON cl.id = cc.claim_id
                        WHERE cc.case_id=?
                        ORDER BY cl.id DESC
                        """,
                        (selected_id,),
                    ).fetchall()
                ] if self._table_exists("case_claims") else []
                detail["events"] = [
                    self._row_to_dict(item)
                    for item in self.db.execute(
                        """
                        SELECT id, event_date, event_title, event_description, content_item_id
                        FROM case_events
                        WHERE case_id=?
                        ORDER BY event_order, event_date
                        """,
                        (selected_id,),
                    ).fetchall()
                ] if self._table_exists("case_events") else []
        return {"items": rows, "detail": detail}

    def _entities_screen(self, filters: dict[str, Any]) -> dict[str, Any]:
        raw_query = self._query(filters.get("query") or "")
        entity_type = (filters.get("entity_type") or "").strip()
        where = ["1=1"]
        params: list[Any] = []
        if entity_type:
            where.append("e.entity_type=?")
            params.append(entity_type)
        rows = [
            self._row_to_dict(row)
            for row in self.db.execute(
                f"""
                SELECT e.id, e.canonical_name, e.entity_type,
                       (SELECT COUNT(*) FROM entity_mentions em WHERE em.entity_id = e.id) AS content_count,
                       (SELECT COUNT(*) FROM official_positions op WHERE op.entity_id = e.id AND op.is_active=1) AS positions_count
                FROM entities e
                WHERE {' AND '.join(where)}
                ORDER BY positions_count DESC, content_count DESC, e.canonical_name
                LIMIT 150
                """,
                params,
            ).fetchall()
        ]
        if raw_query:
            rows = [
                row
                for row in rows
                if self._contains_query(raw_query, row.get("canonical_name"), row.get("entity_type"))
            ]
        selected_id = filters.get("selected_id") or (rows[0]["id"] if rows else None)
        return {
            "items": rows,
            "detail": self.entity_detail(int(selected_id)) if selected_id else None,
        }

    def _relations_screen(self, filters: dict[str, Any]) -> dict[str, Any]:
        query = self._query(filters.get("query") or "")
        layer = (filters.get("layer") or "").strip().lower()
        all_rows = []
        if self._table_exists("entity_relations"):
            for row in self.db.execute(
                """
                SELECT er.id, er.from_entity_id, er.to_entity_id, er.relation_type, er.strength, er.detected_by, er.evidence_item_id,
                       ef.canonical_name AS from_name, et.canonical_name AS to_name
                FROM entity_relations er
                JOIN entities ef ON ef.id = er.from_entity_id
                JOIN entities et ON et.id = er.to_entity_id
                ORDER BY er.id DESC
                LIMIT 300
                """
            ).fetchall():
                item = self._row_to_dict(row)
                item["layer"] = self.relation_layer(
                    item["relation_type"],
                    item.get("detected_by"),
                    item.get("evidence_item_id"),
                )
                all_rows.append(item)

        if query:
            all_rows = [
                item
                for item in all_rows
                if query in (item["from_name"] + " " + item["to_name"] + " " + item["relation_type"]).casefold()
            ]
        if layer:
            all_rows = [item for item in all_rows if item["layer"] == layer]
        all_rows.sort(key=self.relation_sort_key)

        selected_id = filters.get("selected_id") or (all_rows[0]["id"] if all_rows else None)
        detail = next((item for item in all_rows if item["id"] == selected_id), None)
        return {"items": all_rows[:120], "detail": detail}

    def _officials_screen(self, filters: dict[str, Any]) -> dict[str, Any]:
        raw_query = self._query(filters.get("query") or "")
        active_only = filters.get("active_only", True)
        where = ["1=1"]
        params: list[Any] = []
        if active_only:
            where.append("op.is_active=1")
        rows = self.db.execute(
            f"""
            SELECT e.id AS entity_id, e.canonical_name AS full_name, e.entity_type,
                   op.position_title, op.organization, op.source_url, op.is_active
            FROM official_positions op
            JOIN entities e ON e.id = op.entity_id
            WHERE {' AND '.join(where)}
            ORDER BY op.is_active DESC, e.canonical_name, op.id DESC
            LIMIT 200
            """,
            params,
        ).fetchall() if self._table_exists("official_positions") else []

        deduped_items = []
        seen: set[int] = set()
        for row in rows:
            item = self._row_to_dict(row)
            if item["entity_id"] in seen:
                continue
            seen.add(item["entity_id"])
            content_count = self.db.execute(
                "SELECT COUNT(*) FROM entity_mentions WHERE entity_id=?",
                (item["entity_id"],),
            ).fetchone()[0] if self._table_exists("entity_mentions") else 0
            item["content_count"] = int(content_count)
            deduped_items.append(item)
        if raw_query:
            deduped_items = [
                item
                for item in deduped_items
                if self._contains_query(raw_query, item.get("full_name"), item.get("organization"), item.get("position_title"))
            ]

        selected_id = filters.get("selected_id") or (deduped_items[0]["entity_id"] if deduped_items else None)
        detail = self.entity_detail(int(selected_id)) if selected_id else None
        return {"items": deduped_items, "detail": detail}

    def _settings_screen(self) -> dict[str, Any]:
        keys = [
            "db_path",
            "obsidian_export_dir",
            "watch_folder_interval_seconds",
            "telegram_collect_interval_seconds",
            "executive_directory_interval_seconds",
        ]
        items = [{"key": key, "value": self.settings.get(key)} for key in keys if key in self.settings]
        return {"items": items, "detail": {"project_root": str(Path.cwd())}}

    def _count(self, table_name: str) -> int:
        if not self._table_exists(table_name):
            return 0
        return int(self.db.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])

    def _count_where(self, table_name: str, where: str) -> int:
        if not self._table_exists(table_name):
            return 0
        return int(self.db.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {where}").fetchone()[0])

    def _count_distinct(self, table_name: str, column_name: str, where: str = "1=1") -> int:
        if not self._table_exists(table_name):
            return 0
        return int(
            self.db.execute(
                f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name} WHERE {where}"
            ).fetchone()[0]
        )

    def _table_exists(self, table_name: str) -> bool:
        row = self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name=?",
            (table_name,),
        ).fetchone()
        return row is not None

    def _runtime_metadata(self, key: str) -> Any:
        if not self._table_exists("runtime_metadata"):
            return None
        row = self.db.execute(
            "SELECT value_text, value_json FROM runtime_metadata WHERE key=?",
            (key,),
        ).fetchone()
        if not row:
            return None
        value_text, value_json = row
        if value_json:
            try:
                return json.loads(value_json)
            except json.JSONDecodeError:
                return value_json
        return value_text

    def _pinned_sources(self) -> set[int]:
        result: set[int] = set()
        for value in self.settings.get("pinned_sources", []):
            try:
                result.add(int(value))
            except (TypeError, ValueError):
                continue
        return result

    @staticmethod
    def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any]:
        return dict(row) if row is not None else {}

    @staticmethod
    def _query(query: str) -> str:
        query = (query or "").strip().casefold()
        if not query:
            return ""
        return query

    @staticmethod
    def _contains_query(query: str, *values: Any) -> bool:
        if not query:
            return True
        haystack = " ".join(str(value or "") for value in values).casefold()
        return query in haystack

    @staticmethod
    def _co_occurrence_support(detected_by: Any) -> tuple[int, int]:
        text = str(detected_by or "")
        if not text.startswith("co_occurrence:"):
            return (0, 0)
        items = 0
        sources = 0
        for part in text.split(":")[1:]:
            if "=" in part:
                key, value = part.split("=", 1)
                try:
                    if key == "items":
                        items = int(value)
                    elif key == "sources":
                        sources = int(value)
                except ValueError:
                    continue
            else:
                try:
                    items = max(items, int(part))
                except ValueError:
                    continue
        return (items, sources)


class DashboardBridge(QObject):
    bootstrapChanged = Signal(str)
    toastRaised = Signal(str)

    def __init__(self, service: DashboardDataService, controller, parent=None):
        super().__init__(parent)
        self.service = service
        self.controller = controller

    def emit_bootstrap(self):
        self.bootstrapChanged.emit(self._json(self.service.bootstrap_payload(
            running_jobs=set(self.controller.running_jobs()),
            scheduler_running=self.controller.scheduler_running(),
            logs=self.controller.logs(),
        )))

    def emit_toast(self, message: str, level: str = "info"):
        self.toastRaised.emit(self._json({"message": message, "level": level}))

    @Slot(result=str)
    def getBootstrap(self) -> str:
        return self._json(
            self.service.bootstrap_payload(
                running_jobs=set(self.controller.running_jobs()),
                scheduler_running=self.controller.scheduler_running(),
                logs=self.controller.logs(),
            )
        )

    @Slot(str, result=str)
    def getScreenPayload(self, payload_json: str) -> str:
        payload = self._parse_json(payload_json)
        return self._json(
            self.service.screen_payload(
                payload.get("screen", "overview"),
                payload.get("filters") or {},
            )
        )

    @Slot(str, result=str)
    def getSources(self, payload_json: str) -> str:
        payload = self._parse_json(payload_json)
        return self._json(
            self.service.sources_payload(
                search=payload.get("search", ""),
                category=payload.get("category", ""),
            )
        )

    @Slot(result=str)
    def getJobs(self) -> str:
        return self._json(
            self.service.jobs_payload(
                running_jobs=set(self.controller.running_jobs()),
                scheduler_running=self.controller.scheduler_running(),
                logs=self.controller.logs(),
            )
        )

    @Slot(int, result=str)
    def togglePinSource(self, source_id: int) -> str:
        self.controller.toggle_pin_source(int(source_id))
        self.emit_bootstrap()
        return self._json({"ok": True})

    @Slot(str)
    def runJob(self, job_id: str):
        self.controller.run_job(job_id)

    @Slot(str)
    def stopJob(self, job_id: str):
        self.controller.stop_job(job_id)

    @Slot()
    def toggleScheduler(self):
        self.controller.toggle_scheduler()

    @Slot(str, int)
    def updateJobInterval(self, job_id: str, seconds: int):
        self.controller.update_job_interval(job_id, int(seconds))

    @Slot()
    def exportObsidian(self):
        self.controller.export_obsidian()

    @staticmethod
    def _json(payload: dict[str, Any]) -> str:
        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def _parse_json(payload_json: str) -> dict[str, Any]:
        if not payload_json:
            return {}
        try:
            data = json.loads(payload_json)
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}
