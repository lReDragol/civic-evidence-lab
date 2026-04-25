from __future__ import annotations

import json
import re
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
WEAK_RELATION_TYPES = {
    "mentioned_together",
    "same_contract_cluster",
    "same_bill_cluster",
    "same_case_cluster",
    "same_vote_pattern",
    "likely_association",
}
STRENGTH_ORDER = {"strong": 0, "moderate": 1, "weak": 2}
CLAIM_STATUS_PRIORITY = {
    "verified": 4,
    "confirmed": 4,
    "partially_confirmed": 3,
    "open": 2,
    "unverified": 1,
    "draft": 0,
}
LOW_SIGNAL_CLAIMS = {
    "заявил",
    "сказал",
    "сообщил",
    "пообещал",
    "обещал",
    "допрос",
    "допроса",
    "задержан",
    "задержали",
    "арестован",
    "арестовали",
    "владеет",
    "пригрозил",
}
LOW_SIGNAL_CLAIM_RE = re.compile(
    r"^(?:был\s+)?(?:заявил|сказал|сообщил|пообещал|обещал|допрос(?:а)?|задержан(?:а|ы|о)?|арестован(?:а|ы|о)?|владеет|пригрозил)$",
    re.IGNORECASE,
)
RELATION_TYPE_META = {
    "works_at": {
        "label": "Работает в",
        "summary": "{from_name} занимает должность в {to_name}.",
    },
    "head_of": {
        "label": "Возглавляет",
        "summary": "{from_name} возглавляет {to_name}.",
    },
    "party_member": {
        "label": "Состоит в партии",
        "summary": "{from_name} относится к партии или фракции {to_name}.",
    },
    "member_of": {
        "label": "Состоит в",
        "summary": "{from_name} состоит в {to_name}.",
    },
    "member_of_committee": {
        "label": "Член комитета",
        "summary": "{from_name} состоит в комитете {to_name}.",
    },
    "represents_region": {
        "label": "Представляет регион",
        "summary": "{from_name} представляет регион {to_name}.",
    },
    "sponsored_bill": {
        "label": "Соавтор законопроекта",
        "summary": "{from_name} указан автором или соавтором законопроекта {to_name}.",
    },
    "voted_for": {
        "label": "Голосовал за",
        "summary": "{from_name} голосовал за {to_name}.",
    },
    "voted_against": {
        "label": "Голосовал против",
        "summary": "{from_name} голосовал против {to_name}.",
    },
    "voted_abstained": {
        "label": "Воздержался",
        "summary": "{from_name} воздержался при голосовании по {to_name}.",
    },
    "voted_absent": {
        "label": "Не голосовал",
        "summary": "{from_name} отсутствовал при голосовании по {to_name}.",
    },
    "mentioned_together": {
        "label": "Упоминаются вместе",
        "summary": "{from_name} и {to_name} встречаются в одних и тех же материалах.",
    },
    "same_contract_cluster": {
        "label": "Связаны по контрактам",
        "summary": "{from_name} и {to_name} попали в один контрактный кластер.",
    },
    "same_bill_cluster": {
        "label": "Связаны по законопроектам",
        "summary": "{from_name} и {to_name} попали в один законопроектный кластер.",
    },
    "same_case_cluster": {
        "label": "Связаны по делам",
        "summary": "{from_name} и {to_name} попали в один case-кластер.",
    },
    "same_vote_pattern": {
        "label": "Похожий паттерн голосований",
        "summary": "{from_name} и {to_name} показывают похожий паттерн голосований.",
    },
}
DETECTED_BY_LABELS = {
    "official_positions": "официальные должности",
    "party_memberships": "партийные принадлежности",
    "bill_sponsors": "список авторов законопроекта",
    "bill_votes": "записи голосований Госдумы",
    "investigation_case": "материал расследования",
    "risk_patterns": "детектор риск-паттернов",
}
LAYER_LABELS = {
    "structural": "структурная",
    "evidence": "доказательная",
    "weak_similarity": "слабая similarity",
}

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

    @staticmethod
    def _normalize_claim_text(text: Any) -> str:
        value = str(text or "")
        value = re.sub(r"^[^\wА-Яа-яЁё]+", "", value.strip(), flags=re.UNICODE)
        value = re.sub(r"\s+", " ", value)
        return value.strip(" \t\r\n.,;:!?-–—\"'«»()[]")

    def _is_low_signal_claim(self, text: Any) -> bool:
        cleaned = self._normalize_claim_text(text)
        if not cleaned:
            return True
        normalized = cleaned.casefold()
        if normalized in LOW_SIGNAL_CLAIMS or LOW_SIGNAL_CLAIM_RE.fullmatch(normalized):
            return True
        words = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", cleaned)
        alpha_words = [word for word in words if re.search(r"[A-Za-zА-Яа-яЁё]", word)]
        if len(alpha_words) <= 1 and len(cleaned) <= 18:
            return True
        if len(alpha_words) <= 2 and len(cleaned) <= 22 and not any(ch.isdigit() for ch in cleaned):
            return True
        return False

    def _claim_priority(self, item: dict[str, Any]) -> tuple:
        return (
            int(item.get("evidence_count") or 0),
            CLAIM_STATUS_PRIORITY.get(str(item.get("status") or "").strip().lower(), 0),
            float(item.get("confidence_final") or 0.0),
            len(self._normalize_claim_text(item.get("claim_text"))),
            int(item.get("id") or 0),
        )

    def _deduplicate_claim_items(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        for item in items:
            cleaned = self._normalize_claim_text(item.get("claim_text"))
            if self._is_low_signal_claim(cleaned):
                continue
            if not cleaned:
                continue
            key = cleaned.casefold()
            candidate = dict(item)
            candidate["claim_text"] = cleaned
            bucket = grouped.get(key)
            if not bucket:
                candidate["support_count"] = 1
                candidate["duplicate_claim_ids"] = [candidate.get("id")]
                candidate["support_content_ids"] = {candidate.get("content_id")}
                grouped[key] = candidate
                continue
            bucket["support_count"] = int(bucket.get("support_count") or 1) + 1
            bucket.setdefault("duplicate_claim_ids", []).append(candidate.get("id"))
            bucket.setdefault("support_content_ids", set()).add(candidate.get("content_id"))
            bucket["evidence_count"] = max(int(bucket.get("evidence_count") or 0), int(candidate.get("evidence_count") or 0))
            if self._claim_priority(candidate) > self._claim_priority(bucket):
                preserved = {
                    "support_count": bucket["support_count"],
                    "duplicate_claim_ids": bucket.get("duplicate_claim_ids", []),
                    "support_content_ids": bucket.get("support_content_ids", set()),
                }
                grouped[key] = candidate
                grouped[key].update(preserved)

        result = list(grouped.values())
        for item in result:
            item["support_content_count"] = len({value for value in item.get("support_content_ids", set()) if value})
            item["support_content_ids"] = sorted(
                int(value) for value in item.get("support_content_ids", set()) if value is not None
            )
            item["duplicate_claim_ids"] = [int(value) for value in item.get("duplicate_claim_ids", []) if value is not None]
        result.sort(
            key=lambda item: (
                -int(item.get("support_count") or 1),
                -int(item.get("evidence_count") or 0),
                -len(item.get("claim_text") or ""),
                -int(item.get("id") or 0),
            )
        )
        return result

    def relation_label(self, relation_type: str) -> str:
        meta = RELATION_TYPE_META.get(relation_type or "", {})
        return str(meta.get("label") or (relation_type or "связь").replace("_", " "))

    def relation_summary(self, relation_type: str, from_name: str, to_name: str) -> str:
        meta = RELATION_TYPE_META.get(relation_type or "", {})
        template = str(meta.get("summary") or "{from_name} связан с {to_name}.")
        return template.format(from_name=from_name or "—", to_name=to_name or "—")

    def relation_detected_label(self, detected_by: str | None) -> str:
        text = str(detected_by or "").strip()
        if not text:
            return "источник не указан"
        if text.startswith("executive_directory:"):
            return "официальный каталог руководства"
        if text.startswith("co_occurrence:"):
            support_items, support_sources = self._co_occurrence_support(text)
            return f"совместные упоминания ({support_items} материалов, {support_sources} источников)"
        return DETECTED_BY_LABELS.get(text, text.replace("_", " "))

    def relation_layer_label(self, layer: str | None) -> str:
        return LAYER_LABELS.get(str(layer or ""), str(layer or "—"))

    def _bill_context(self, entity_name: str | None) -> dict[str, Any] | None:
        if not entity_name or not self._table_exists("bills"):
            return None
        row = self.db.execute(
            """
            SELECT id, number, title, status, registration_date, duma_url
            FROM bills
            WHERE number=?
            LIMIT 1
            """,
            (entity_name,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def _evidence_context(self, evidence_item_id: int | None) -> dict[str, Any] | None:
        if not evidence_item_id or not self._table_exists("content_items"):
            return None
        row = self.db.execute(
            "SELECT id, title, url, published_at, content_type FROM content_items WHERE id=?",
            (evidence_item_id,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    @staticmethod
    def _compact_text(value: Any, limit: int = 92) -> str:
        text = re.sub(r"\s+", " ", str(value or "").strip())
        if len(text) <= limit:
            return text
        return f"{text[: limit - 1].rstrip()}…"

    @staticmethod
    def _full_text(value: Any) -> str:
        return re.sub(r"\s+", " ", str(value or "").strip())

    @staticmethod
    def _json_object(raw_value: Any) -> dict[str, Any]:
        if not raw_value:
            return {}
        try:
            payload = json.loads(str(raw_value))
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def _graph_node(
        self,
        node_id: str,
        role: str,
        label: str,
        title: Any,
        meta: Any = "",
        *,
        description: Any = "",
        jump_screen: str | None = None,
        jump_id: int | None = None,
    ) -> dict[str, Any]:
        node = {
            "id": str(node_id),
            "role": role,
            "label": self._compact_text(label, 24),
            "title": self._compact_text(title, 112) or "—",
            "meta": self._compact_text(meta, 84),
            "description": self._full_text(description),
        }
        if jump_screen and jump_id:
            node["jump_screen"] = jump_screen
            node["jump_id"] = int(jump_id)
        return node

    def _content_entities(self, content_item_id: int | None, limit: int = 3) -> list[dict[str, Any]]:
        if not content_item_id or not self._table_exists("entity_mentions"):
            return []
        rows = self.db.execute(
            """
            SELECT e.id, e.canonical_name, e.entity_type, em.mention_type
            FROM entity_mentions em
            JOIN entities e ON e.id = em.entity_id
            WHERE em.content_item_id=?
            ORDER BY
                CASE em.mention_type
                    WHEN 'subject' THEN 0
                    WHEN 'organization' THEN 1
                    ELSE 2
                END,
                e.canonical_name
            LIMIT ?
            """,
            (content_item_id, int(limit)),
        ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def _claim_evidence_graph(self, detail: dict[str, Any] | None) -> dict[str, Any] | None:
        if not detail or not detail.get("id"):
            return None

        claim_id = int(detail["id"])
        claim_node_id = f"claim:{claim_id}"
        nodes = [
            self._graph_node(
                claim_node_id,
                "claim",
                "Claim",
                detail.get("claim_text") or f"Claim #{claim_id}",
                " · ".join(
                    value
                    for value in [
                        detail.get("status"),
                        f"conf {detail.get('confidence_final')}" if detail.get("confidence_final") not in (None, "") else "",
                    ]
                    if value
                ),
                description=detail.get("claim_text") or f"Claim #{claim_id}",
                jump_screen="claims",
                jump_id=claim_id,
            )
        ]
        edges: list[dict[str, Any]] = []

        content_id = detail.get("content_id")
        if content_id:
            content_node_id = f"content:{int(content_id)}"
            nodes.append(
                self._graph_node(
                    content_node_id,
                    "content_origin",
                    "Источник",
                    detail.get("content_title") or f"Content #{content_id}",
                    " · ".join(
                        value
                        for value in [
                            detail.get("source_name"),
                            detail.get("published_at"),
                            detail.get("content_type"),
                        ]
                        if value
                    ),
                    description=detail.get("body_text") or detail.get("content_title") or f"Content #{content_id}",
                    jump_screen="content",
                    jump_id=int(content_id),
                )
            )
            edges.append({"from": content_node_id, "to": claim_node_id, "label": "источник", "kind": "origin"})

            for entity in self._content_entities(int(content_id), limit=3):
                entity_id = entity.get("id")
                if not entity_id:
                    continue
                entity_node_id = f"entity:{int(entity_id)}"
                nodes.append(
                    self._graph_node(
                        entity_node_id,
                        "entity",
                        entity.get("entity_type") or "entity",
                        entity.get("canonical_name") or f"Entity #{entity_id}",
                        entity.get("mention_type") or "",
                        description=entity.get("canonical_name") or f"Entity #{entity_id}",
                        jump_screen="entities",
                        jump_id=int(entity_id),
                    )
                )
                edges.append(
                    {
                        "from": entity_node_id,
                        "to": claim_node_id,
                        "label": entity.get("mention_type") or "упоминание",
                        "kind": "entity",
                    }
                )

        case_id = detail.get("case_id")
        if case_id:
            case_node_id = f"case:{int(case_id)}"
            nodes.append(
                self._graph_node(
                    case_node_id,
                    "case",
                    "Дело",
                    detail.get("case_title") or f"Case #{case_id}",
                    detail.get("case_type") or detail.get("status") or "",
                    description=detail.get("case_title") or f"Case #{case_id}",
                    jump_screen="cases",
                    jump_id=int(case_id),
                )
            )
            edges.append({"from": case_node_id, "to": claim_node_id, "label": "в деле", "kind": "case"})

        for index, evidence in enumerate(detail.get("evidence") or []):
            evidence_key = evidence.get("id") or evidence.get("evidence_item_id") or f"{claim_id}:{index}"
            evidence_node_id = f"evidence:{evidence_key}"
            evidence_title = (
                evidence.get("evidence_title")
                or evidence.get("notes")
                or evidence.get("evidence_type")
                or f"Evidence #{index + 1}"
            )
            evidence_meta = " · ".join(
                value
                for value in [
                    evidence.get("evidence_type"),
                    evidence.get("evidence_source_name"),
                    evidence.get("evidence_published_at"),
                    evidence.get("strength"),
                ]
                if value
            )
            node_kwargs: dict[str, Any] = {}
            if evidence.get("evidence_item_id"):
                node_kwargs = {
                    "jump_screen": "content",
                    "jump_id": int(evidence["evidence_item_id"]),
                }
            nodes.append(
                self._graph_node(
                    evidence_node_id,
                    "evidence",
                    "Evidence",
                    evidence_title,
                    evidence_meta,
                    description=evidence.get("notes")
                    or evidence.get("evidence_title")
                    or evidence.get("evidence_type")
                    or f"Evidence #{index + 1}",
                    **node_kwargs,
                )
            )
            edges.append(
                {
                    "from": evidence_node_id,
                    "to": claim_node_id,
                    "label": evidence.get("evidence_type") or "подтверждает",
                    "kind": "evidence",
                }
            )

        if len(nodes) < 2:
            return None
        return {"kind": "claim", "nodes": nodes, "edges": edges}

    def _relation_evidence_graph(self, detail: dict[str, Any] | None) -> dict[str, Any] | None:
        if not detail or not detail.get("id"):
            return None

        relation_id = int(detail["id"])
        relation_node_id = f"relation:{relation_id}"
        nodes = [
            self._graph_node(
                relation_node_id,
                "relation",
                detail.get("layer_label") or detail.get("layer") or "Связь",
                detail.get("relation_label") or detail.get("relation_type") or f"Relation #{relation_id}",
                " · ".join(
                    value
                    for value in [
                        detail.get("strength"),
                        detail.get("detected_label") or detail.get("detected_by"),
                    ]
                    if value
                ),
                description=detail.get("summary")
                or detail.get("relation_label")
                or detail.get("relation_type")
                or f"Relation #{relation_id}",
            )
        ]
        edges: list[dict[str, Any]] = []

        from_entity_id = detail.get("from_entity_id")
        if from_entity_id:
            from_node_id = f"entity:{int(from_entity_id)}"
            nodes.append(
                self._graph_node(
                    from_node_id,
                    "entity_from",
                    detail.get("from_type") or "from",
                    detail.get("from_name") or f"Entity #{from_entity_id}",
                    detail.get("from_description") or "",
                    description=detail.get("from_description")
                    or detail.get("from_name")
                    or f"Entity #{from_entity_id}",
                    jump_screen="entities",
                    jump_id=int(from_entity_id),
                )
            )
            edges.append(
                {
                    "from": from_node_id,
                    "to": relation_node_id,
                    "label": "источник связи",
                    "kind": "entity",
                }
            )

        to_entity_id = detail.get("to_entity_id")
        if to_entity_id:
            to_node_id = f"entity:{int(to_entity_id)}"
            nodes.append(
                self._graph_node(
                    to_node_id,
                    "entity_to",
                    detail.get("to_type") or "to",
                    detail.get("to_name") or f"Entity #{to_entity_id}",
                    detail.get("to_description") or "",
                    description=detail.get("to_description")
                    or detail.get("to_name")
                    or f"Entity #{to_entity_id}",
                    jump_screen="entities",
                    jump_id=int(to_entity_id),
                )
            )
            edges.append(
                {
                    "from": relation_node_id,
                    "to": to_node_id,
                    "label": detail.get("relation_label") or detail.get("relation_type") or "связь",
                    "kind": "relation",
                }
            )

        if detail.get("context_title"):
            context_node_id = f"context:{relation_id}"
            nodes.append(
                self._graph_node(
                    context_node_id,
                    "context",
                    detail.get("context_subtitle") or "Контекст",
                    detail.get("context_title"),
                    detail.get("context_url") or "",
                    description=detail.get("context_title"),
                )
            )
            edges.append({"from": context_node_id, "to": relation_node_id, "label": "контекст", "kind": "context"})

        if detail.get("evidence_title"):
            evidence_node_id = f"evidence:{detail.get('evidence_content_id') or relation_id}"
            node_kwargs: dict[str, Any] = {}
            if detail.get("evidence_content_id"):
                node_kwargs = {
                    "jump_screen": "content",
                    "jump_id": int(detail["evidence_content_id"]),
                }
            nodes.append(
                self._graph_node(
                    evidence_node_id,
                    "evidence",
                    "Evidence",
                    detail.get("evidence_title"),
                    detail.get("evidence_url") or detail.get("detected_label") or "",
                    description=detail.get("evidence_title")
                    or detail.get("evidence_url")
                    or detail.get("summary")
                    or "Evidence",
                    **node_kwargs,
                )
            )
            edges.append({"from": evidence_node_id, "to": relation_node_id, "label": "доказательство", "kind": "evidence"})

        if len(nodes) < 2:
            return None
        return {"kind": "relation", "nodes": nodes, "edges": edges}

    def _relation_map_graph(self, relations: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not relations:
            return None

        node_map: dict[str, dict[str, Any]] = {}
        degree_map: defaultdict[str, int] = defaultdict(int)
        edges: list[dict[str, Any]] = []
        edge_keys: set[tuple[Any, ...]] = set()

        for item in relations[:480]:
            from_entity_id = item.get("from_entity_id")
            to_entity_id = item.get("to_entity_id")
            if not from_entity_id or not to_entity_id:
                continue

            from_node_id = f"entity:{int(from_entity_id)}"
            to_node_id = f"entity:{int(to_entity_id)}"

            degree_map[from_node_id] += 1
            degree_map[to_node_id] += 1

            if from_node_id not in node_map:
                node_map[from_node_id] = self._graph_node(
                    from_node_id,
                    "map_entity",
                    item.get("from_type") or "entity",
                    item.get("from_name") or f"Entity #{from_entity_id}",
                    "",
                    description=item.get("from_description")
                    or item.get("from_name")
                    or f"Entity #{from_entity_id}",
                    jump_screen="entities",
                    jump_id=int(from_entity_id),
                )
            if to_node_id not in node_map:
                node_map[to_node_id] = self._graph_node(
                    to_node_id,
                    "map_entity",
                    item.get("to_type") or "entity",
                    item.get("to_name") or f"Entity #{to_entity_id}",
                    "",
                    description=item.get("to_description")
                    or item.get("to_name")
                    or f"Entity #{to_entity_id}",
                    jump_screen="entities",
                    jump_id=int(to_entity_id),
                )

            edge_payload = {
                "from": from_node_id,
                "to": to_node_id,
                "label": item.get("relation_label") or item.get("relation_type") or "связь",
                "kind": item.get("layer") or "relation",
                "strength": item.get("strength") or "",
                "detected_label": item.get("detected_label") or item.get("detected_by") or "",
                "summary": item.get("summary") or "",
            }
            relation_id = item.get("id")
            if relation_id not in (None, ""):
                edge_payload["id"] = int(relation_id)
            edge_key = (
                edge_payload["from"],
                edge_payload["to"],
                edge_payload["label"],
                edge_payload["kind"],
                edge_payload.get("id"),
            )
            if edge_key not in edge_keys:
                edge_keys.add(edge_key)
                edges.append(edge_payload)

        if len(node_map) < 2 or not edges:
            return None

        sorted_entity_ids = [
            int(node_id.split(":", 1)[1])
            for node_id, _node in sorted(degree_map.items(), key=lambda item: (-item[1], item[0]))[:72]
            if node_id.startswith("entity:")
        ]
        scoped_entity_ids = {entity_id for entity_id in sorted_entity_ids if entity_id}
        if scoped_entity_ids:
            self._relation_map_append_claim_bridges(scoped_entity_ids, node_map, degree_map, edges, edge_keys)
            self._relation_map_append_bill_bridges(scoped_entity_ids, node_map, degree_map, edges, edge_keys)
            self._relation_map_append_contract_bridges(scoped_entity_ids, node_map, degree_map, edges, edge_keys)

        for node_id, node in node_map.items():
            degree = degree_map.get(node_id, 0)
            meta = [f"связей {degree}"]
            if node.get("meta"):
                meta.append(str(node["meta"]))
            node["meta"] = self._compact_text(" · ".join(value for value in meta if value), 84)

        return {
            "kind": "relation_map",
            "nodes": list(node_map.values()),
            "edges": edges,
            "stats": {
                "nodes": len(node_map),
                "edges": len(edges),
                "entity_nodes": sum(1 for node in node_map.values() if node.get("role") == "map_entity"),
                "bridge_nodes": sum(1 for node in node_map.values() if str(node.get("role", "")).startswith("bridge_")),
            },
        }

    def _relation_map_append_claim_bridges(
        self,
        entity_ids: set[int],
        node_map: dict[str, dict[str, Any]],
        degree_map: defaultdict[str, int],
        edges: list[dict[str, Any]],
        edge_keys: set[tuple[Any, ...]],
    ) -> None:
        if not entity_ids or not self._table_exists("claims") or not self._table_exists("entity_mentions"):
            return
        placeholders = ",".join("?" for _ in entity_ids)
        claim_rows = self.db.execute(
            f"""
            SELECT cl.id, cl.claim_text, cl.status, cl.content_item_id,
                   COUNT(DISTINCT em.entity_id) AS matched_entities
            FROM claims cl
            JOIN entity_mentions em ON em.content_item_id = cl.content_item_id
            WHERE em.entity_id IN ({placeholders})
            GROUP BY cl.id
            HAVING COUNT(DISTINCT em.entity_id) >= 2
            ORDER BY matched_entities DESC, cl.id DESC
            LIMIT 36
            """,
            tuple(entity_ids),
        ).fetchall()
        for row in claim_rows:
            claim = self._row_to_dict(row)
            claim_id = int(claim["id"])
            claim_node_id = f"claim:{claim_id}"
            self._map_ensure_node(
                node_map,
                claim_node_id,
                "bridge_claim",
                "Claim",
                claim.get("claim_text") or f"Claim #{claim_id}",
                claim.get("status") or "",
                description=claim.get("claim_text") or f"Claim #{claim_id}",
                jump_screen="claims",
                jump_id=claim_id,
            )

            mention_rows = self.db.execute(
                f"""
                SELECT DISTINCT e.id, e.canonical_name, e.entity_type, e.description, em.mention_type
                FROM entity_mentions em
                JOIN entities e ON e.id = em.entity_id
                WHERE em.content_item_id=? AND em.entity_id IN ({placeholders})
                ORDER BY
                    CASE em.mention_type
                        WHEN 'subject' THEN 0
                        WHEN 'organization' THEN 1
                        ELSE 2
                    END,
                    e.canonical_name
                LIMIT 5
                """,
                (claim.get("content_item_id"), *tuple(entity_ids)),
            ).fetchall()
            for mention_row in mention_rows:
                entity = self._row_to_dict(mention_row)
                entity_id = int(entity["id"])
                entity_node_id = f"entity:{entity_id}"
                self._map_ensure_node(
                    node_map,
                    entity_node_id,
                    "map_entity",
                    entity.get("entity_type") or "entity",
                    entity.get("canonical_name") or f"Entity #{entity_id}",
                    "",
                    description=entity.get("description") or entity.get("canonical_name") or f"Entity #{entity_id}",
                    jump_screen="entities",
                    jump_id=entity_id,
                )
                self._map_add_edge(
                    edges,
                    edge_keys,
                    degree_map,
                    entity_node_id,
                    claim_node_id,
                    entity.get("mention_type") or "упоминание",
                    "claim",
                    summary=f"{entity.get('canonical_name') or 'Сущность'} фигурирует в claim.",
                )

            if claim.get("content_item_id") and self._table_exists("content_items"):
                content_row = self.db.execute(
                    """
                    SELECT ci.id, ci.title, ci.body_text, ci.published_at, ci.content_type, s.name AS source_name
                    FROM content_items ci
                    LEFT JOIN sources s ON s.id = ci.source_id
                    WHERE ci.id=?
                    """,
                    (claim["content_item_id"],),
                ).fetchone()
                if content_row:
                    content = self._row_to_dict(content_row)
                    content_id = int(content["id"])
                    content_node_id = f"content:{content_id}"
                    self._map_ensure_node(
                        node_map,
                        content_node_id,
                        "bridge_content",
                        "Контент",
                        content.get("title") or f"Content #{content_id}",
                        " · ".join(
                            value for value in [content.get("source_name"), content.get("published_at"), content.get("content_type")] if value
                        ),
                        description=content.get("body_text") or content.get("title") or f"Content #{content_id}",
                        jump_screen="content",
                        jump_id=content_id,
                    )
                    self._map_add_edge(
                        edges,
                        edge_keys,
                        degree_map,
                        content_node_id,
                        claim_node_id,
                        "источник",
                        "origin",
                        summary="Контент является источником claim.",
                    )

            if self._table_exists("case_claims") and self._table_exists("cases"):
                for case_row in self.db.execute(
                    """
                    SELECT c.id, c.title, c.case_type, c.status
                    FROM case_claims cc
                    JOIN cases c ON c.id = cc.case_id
                    WHERE cc.claim_id=?
                    ORDER BY c.id DESC
                    LIMIT 3
                    """,
                    (claim_id,),
                ).fetchall():
                    case_item = self._row_to_dict(case_row)
                    case_id = int(case_item["id"])
                    case_node_id = f"case:{case_id}"
                    self._map_ensure_node(
                        node_map,
                        case_node_id,
                        "bridge_case",
                        "Дело",
                        case_item.get("title") or f"Case #{case_id}",
                        " · ".join(value for value in [case_item.get("case_type"), case_item.get("status")] if value),
                        description=case_item.get("title") or f"Case #{case_id}",
                        jump_screen="cases",
                        jump_id=case_id,
                    )
                    self._map_add_edge(
                        edges,
                        edge_keys,
                        degree_map,
                        case_node_id,
                        claim_node_id,
                        "в деле",
                        "case",
                        summary="Claim входит в состав дела.",
                    )

            if self._table_exists("evidence_links"):
                for evidence_row in self.db.execute(
                    """
                    SELECT el.id, el.evidence_type, el.strength, el.notes,
                           ci.id AS evidence_item_id, ci.title AS evidence_title,
                           ci.published_at AS evidence_published_at,
                           s.name AS evidence_source_name
                    FROM evidence_links el
                    LEFT JOIN content_items ci ON ci.id = el.evidence_item_id
                    LEFT JOIN sources s ON s.id = ci.source_id
                    WHERE el.claim_id=?
                    ORDER BY el.id DESC
                    LIMIT 3
                    """,
                    (claim_id,),
                ).fetchall():
                    evidence = self._row_to_dict(evidence_row)
                    evidence_key = evidence.get("evidence_item_id") or evidence.get("id")
                    if not evidence_key:
                        continue
                    evidence_node_id = f"evidence:{int(evidence_key)}"
                    evidence_title = (
                        evidence.get("evidence_title")
                        or evidence.get("notes")
                        or evidence.get("evidence_type")
                        or f"Evidence #{evidence.get('id')}"
                    )
                    self._map_ensure_node(
                        node_map,
                        evidence_node_id,
                        "bridge_evidence",
                        "Evidence",
                        evidence_title,
                        " · ".join(
                            value
                            for value in [
                                evidence.get("evidence_type"),
                                evidence.get("evidence_source_name"),
                                evidence.get("evidence_published_at"),
                                evidence.get("strength"),
                            ]
                            if value
                        ),
                        description=evidence.get("notes") or evidence_title,
                        jump_screen="content" if evidence.get("evidence_item_id") else None,
                        jump_id=int(evidence["evidence_item_id"]) if evidence.get("evidence_item_id") else None,
                    )
                    self._map_add_edge(
                        edges,
                        edge_keys,
                        degree_map,
                        evidence_node_id,
                        claim_node_id,
                        evidence.get("evidence_type") or "подтверждает",
                        "evidence",
                        summary="Evidence поддерживает claim.",
                    )

    def _relation_map_append_bill_bridges(
        self,
        entity_ids: set[int],
        node_map: dict[str, dict[str, Any]],
        degree_map: defaultdict[str, int],
        edges: list[dict[str, Any]],
        edge_keys: set[tuple[Any, ...]],
    ) -> None:
        if not entity_ids or not self._table_exists("bill_sponsors") or not self._table_exists("bills"):
            return
        placeholders = ",".join("?" for _ in entity_ids)
        rows = self.db.execute(
            f"""
            SELECT b.id, b.number, b.title, b.status, b.registration_date,
                   COUNT(DISTINCT bs.entity_id) AS matched_entities
            FROM bill_sponsors bs
            JOIN bills b ON b.id = bs.bill_id
            WHERE bs.entity_id IN ({placeholders})
            GROUP BY b.id
            HAVING COUNT(DISTINCT bs.entity_id) >= 2
            ORDER BY matched_entities DESC, b.id DESC
            LIMIT 24
            """,
            tuple(entity_ids),
        ).fetchall()
        for row in rows:
            bill = self._row_to_dict(row)
            bill_id = int(bill["id"])
            bill_node_id = f"bill:{bill_id}"
            bill_title = " · ".join(value for value in [bill.get("number"), bill.get("title")] if value) or f"Bill #{bill_id}"
            self._map_ensure_node(
                node_map,
                bill_node_id,
                "bridge_bill",
                "Законопроект",
                bill_title,
                " · ".join(value for value in [bill.get("status"), bill.get("registration_date")] if value),
                description=bill.get("title") or bill.get("number") or f"Bill #{bill_id}",
            )
            sponsor_rows = self.db.execute(
                f"""
                SELECT DISTINCT e.id, e.canonical_name, e.entity_type, e.description
                FROM bill_sponsors bs
                JOIN entities e ON e.id = bs.entity_id
                WHERE bs.bill_id=? AND bs.entity_id IN ({placeholders})
                ORDER BY e.canonical_name
                LIMIT 6
                """,
                (bill_id, *tuple(entity_ids)),
            ).fetchall()
            for sponsor_row in sponsor_rows:
                entity = self._row_to_dict(sponsor_row)
                entity_id = int(entity["id"])
                entity_node_id = f"entity:{entity_id}"
                self._map_ensure_node(
                    node_map,
                    entity_node_id,
                    "map_entity",
                    entity.get("entity_type") or "entity",
                    entity.get("canonical_name") or f"Entity #{entity_id}",
                    "",
                    description=entity.get("description") or entity.get("canonical_name") or f"Entity #{entity_id}",
                    jump_screen="entities",
                    jump_id=entity_id,
                )
                self._map_add_edge(
                    edges,
                    edge_keys,
                    degree_map,
                    entity_node_id,
                    bill_node_id,
                    "соавтор",
                    "bill",
                    summary=f"{entity.get('canonical_name') or 'Сущность'} указан(а) в списке авторов законопроекта.",
                )

    def _relation_map_append_contract_bridges(
        self,
        entity_ids: set[int],
        node_map: dict[str, dict[str, Any]],
        degree_map: defaultdict[str, int],
        edges: list[dict[str, Any]],
        edge_keys: set[tuple[Any, ...]],
    ) -> None:
        if not entity_ids or not self._table_exists("contract_parties") or not self._table_exists("contracts"):
            return
        placeholders = ",".join("?" for _ in entity_ids)
        rows = self.db.execute(
            f"""
            SELECT c.id, c.contract_number, c.title, c.publication_date,
                   COUNT(DISTINCT cp.entity_id) AS matched_entities
            FROM contract_parties cp
            JOIN contracts c ON c.id = cp.contract_id
            WHERE cp.entity_id IN ({placeholders})
            GROUP BY c.id
            HAVING COUNT(DISTINCT cp.entity_id) >= 2
            ORDER BY matched_entities DESC, c.id DESC
            LIMIT 24
            """,
            tuple(entity_ids),
        ).fetchall()
        for row in rows:
            contract = self._row_to_dict(row)
            contract_id = int(contract["id"])
            contract_node_id = f"contract:{contract_id}"
            contract_title = " · ".join(
                value for value in [contract.get("contract_number"), contract.get("title")] if value
            ) or f"Contract #{contract_id}"
            self._map_ensure_node(
                node_map,
                contract_node_id,
                "bridge_contract",
                "Контракт",
                contract_title,
                contract.get("publication_date") or "",
                description=contract.get("title") or contract.get("contract_number") or f"Contract #{contract_id}",
            )
            party_rows = self.db.execute(
                f"""
                SELECT DISTINCT e.id, e.canonical_name, e.entity_type, e.description, cp.party_role
                FROM contract_parties cp
                JOIN entities e ON e.id = cp.entity_id
                WHERE cp.contract_id=? AND cp.entity_id IN ({placeholders})
                ORDER BY cp.party_role, e.canonical_name
                LIMIT 6
                """,
                (contract_id, *tuple(entity_ids)),
            ).fetchall()
            for party_row in party_rows:
                entity = self._row_to_dict(party_row)
                entity_id = int(entity["id"])
                entity_node_id = f"entity:{entity_id}"
                self._map_ensure_node(
                    node_map,
                    entity_node_id,
                    "map_entity",
                    entity.get("entity_type") or "entity",
                    entity.get("canonical_name") or f"Entity #{entity_id}",
                    "",
                    description=entity.get("description") or entity.get("canonical_name") or f"Entity #{entity_id}",
                    jump_screen="entities",
                    jump_id=entity_id,
                )
                self._map_add_edge(
                    edges,
                    edge_keys,
                    degree_map,
                    entity_node_id,
                    contract_node_id,
                    entity.get("party_role") or "сторона",
                    "contract",
                    summary=f"{entity.get('canonical_name') or 'Сущность'} выступает стороной контракта.",
                )

    def _map_ensure_node(
        self,
        node_map: dict[str, dict[str, Any]],
        node_id: str,
        role: str,
        label: str,
        title: Any,
        meta: Any = "",
        *,
        description: Any = "",
        jump_screen: str | None = None,
        jump_id: int | None = None,
    ) -> dict[str, Any]:
        if node_id not in node_map:
            node_map[node_id] = self._graph_node(
                node_id,
                role,
                label,
                title,
                meta,
                description=description,
                jump_screen=jump_screen,
                jump_id=jump_id,
            )
        return node_map[node_id]

    def _map_add_edge(
        self,
        edges: list[dict[str, Any]],
        edge_keys: set[tuple[Any, ...]],
        degree_map: defaultdict[str, int],
        from_node_id: str,
        to_node_id: str,
        label: str,
        kind: str,
        *,
        summary: str = "",
        strength: str = "",
        detected_label: str = "",
        edge_id: Any = None,
    ) -> None:
        edge_key = (from_node_id, to_node_id, label, kind, edge_id)
        if edge_key in edge_keys:
            return
        edge_keys.add(edge_key)
        degree_map[from_node_id] += 1
        degree_map[to_node_id] += 1
        payload = {
            "from": from_node_id,
            "to": to_node_id,
            "label": label,
            "kind": kind,
            "summary": summary,
            "strength": strength,
            "detected_label": detected_label,
        }
        if edge_id not in (None, ""):
            payload["id"] = edge_id
        edges.append(payload)

    def _relation_map_paths(
        self,
        graph: dict[str, Any] | None,
        from_entity_id: int | None,
        to_entity_id: int | None,
        *,
        limit: int = 4,
        max_depth: int = 6,
    ) -> list[dict[str, Any]]:
        if not graph or not from_entity_id or not to_entity_id:
            return []
        node_lookup = {str(node.get("id")): node for node in graph.get("nodes") or []}
        start_id = f"entity:{int(from_entity_id)}"
        end_id = f"entity:{int(to_entity_id)}"
        if start_id not in node_lookup or end_id not in node_lookup:
            return []

        adjacency: defaultdict[str, list[str]] = defaultdict(list)
        direct_pair = frozenset({start_id, end_id})
        for edge in graph.get("edges") or []:
            from_id = str(edge.get("from") or "")
            to_id = str(edge.get("to") or "")
            if not from_id or not to_id:
                continue
            if frozenset({from_id, to_id}) == direct_pair and str(edge.get("kind") or "") in {
                "structural",
                "weak_similarity",
                "relation",
                "evidence",
            }:
                continue
            adjacency[from_id].append(to_id)
            adjacency[to_id].append(from_id)

        def node_priority(node_id: str) -> tuple[int, int, str]:
            node = node_lookup.get(node_id) or {}
            role = str(node.get("role") or "")
            return (0 if role.startswith("bridge_") else 1, -len(str(node.get("title") or "")), node_id)

        queue: list[list[str]] = [[start_id]]
        seen_paths: set[tuple[str, ...]] = set()
        found: list[list[str]] = []
        shortest: int | None = None
        while queue and len(found) < limit:
            path = queue.pop(0)
            current = path[-1]
            if shortest is not None and len(path) > shortest + 2:
                continue
            if len(path) - 1 > max_depth:
                continue
            neighbours = sorted(set(adjacency.get(current, [])), key=node_priority)
            for next_node in neighbours:
                if next_node in path:
                    continue
                candidate = path + [next_node]
                candidate_key = tuple(candidate)
                if candidate_key in seen_paths:
                    continue
                seen_paths.add(candidate_key)
                if next_node == end_id:
                    shortest = len(candidate) if shortest is None else min(shortest, len(candidate))
                    found.append(candidate)
                    if len(found) >= limit:
                        break
                else:
                    queue.append(candidate)

        scored_paths = []
        for path in found:
            bridge_count = sum(
                1 for node_id in path[1:-1] if str((node_lookup.get(node_id) or {}).get("role", "")).startswith("bridge_")
            )
            entity_mid_count = sum(
                1 for node_id in path[1:-1] if str((node_lookup.get(node_id) or {}).get("role", "")) == "map_entity"
            )
            scored_paths.append((path, bridge_count, entity_mid_count))

        bridge_first = [item for item in scored_paths if item[1] > 0]
        chosen_paths = bridge_first
        if not chosen_paths:
            return []
        chosen_paths.sort(key=lambda item: (-item[1], item[2], len(item[0]), tuple(item[0])))

        result: list[dict[str, Any]] = []
        for path, _bridge_count, _entity_mid_count in chosen_paths[:limit]:
            nodes = []
            for node_id in path:
                node = node_lookup.get(node_id) or {}
                role = str(node.get("role") or "")
                title = str(node.get("title") or node_id)
                if role == "bridge_case":
                    display = f"Дело: {title}"
                elif role == "bridge_claim":
                    display = f"Claim: {title}"
                elif role == "bridge_content":
                    display = f"Контент: {title}"
                elif role == "bridge_evidence":
                    display = f"Evidence: {title}"
                elif role == "bridge_bill":
                    display = f"Законопроект: {title}"
                elif role == "bridge_contract":
                    display = f"Контракт: {title}"
                else:
                    display = title
                nodes.append(
                    {
                        "id": node_id,
                        "role": role,
                        "title": title,
                        "label": display,
                        "jump_screen": node.get("jump_screen"),
                        "jump_id": node.get("jump_id"),
                    }
                )
            result.append(
                {
                    "hops": len(path) - 1,
                    "label": " → ".join(node["label"] for node in nodes),
                    "nodes": nodes,
                }
            )
        return result

    def _relation_candidate_map_items(self, query: str, layer: str) -> list[dict[str, Any]]:
        if layer and layer != "weak_similarity":
            return []
        if not self._table_exists("relation_candidates"):
            return []

        rows = self.db.execute(
            """
            SELECT rc.id, rc.entity_a_id AS from_entity_id, rc.entity_b_id AS to_entity_id,
                   rc.candidate_type AS relation_type, rc.origin, rc.score, rc.support_items,
                   rc.support_sources, rc.support_domains, rc.metadata_json,
                   ea.canonical_name AS from_name, ea.entity_type AS from_type, ea.description AS from_description,
                   eb.canonical_name AS to_name, eb.entity_type AS to_type, eb.description AS to_description
            FROM relation_candidates rc
            JOIN entities ea ON ea.id = rc.entity_a_id
            JOIN entities eb ON eb.id = rc.entity_b_id
            WHERE rc.promotion_state='review'
            ORDER BY rc.score DESC, rc.id DESC
            LIMIT 180
            """
        ).fetchall()

        items: list[dict[str, Any]] = []
        for row in rows:
            item = self._row_to_dict(row)
            if query and query not in (
                f"{item.get('from_name', '')} {item.get('to_name', '')} {item.get('relation_type', '')}"
            ).casefold():
                continue
            metadata = self._json_object(item.get("metadata_json"))
            hints = []
            if metadata.get("bill_overlap"):
                hints.append(f"общие законопроекты {metadata['bill_overlap']}")
            if metadata.get("case_overlap"):
                hints.append(f"общие дела {metadata['case_overlap']}")
            if metadata.get("contract_overlap"):
                hints.append(f"общие контракты {metadata['contract_overlap']}")
            if metadata.get("risk_overlap"):
                hints.append(f"общие risk-patterns {metadata['risk_overlap']}")
            if metadata.get("same_vote_ratio"):
                hints.append(f"совпадение голосований {metadata['same_vote_ratio']}")
            if item.get("support_items"):
                hints.append(f"материалов {item['support_items']}")
            if item.get("support_sources"):
                hints.append(f"источников {item['support_sources']}")
            summary = self.relation_summary(
                item.get("relation_type", ""),
                item.get("from_name", ""),
                item.get("to_name", ""),
            )
            if hints:
                summary = f"{summary} Основания: {', '.join(map(str, hints[:4]))}."
            items.append(
                {
                    "id": None,
                    "from_entity_id": item.get("from_entity_id"),
                    "to_entity_id": item.get("to_entity_id"),
                    "from_name": item.get("from_name"),
                    "to_name": item.get("to_name"),
                    "from_type": item.get("from_type"),
                    "to_type": item.get("to_type"),
                    "from_description": item.get("from_description"),
                    "to_description": item.get("to_description"),
                    "relation_type": item.get("relation_type"),
                    "relation_label": self.relation_label(item.get("relation_type", "")),
                    "layer": "weak_similarity",
                    "strength": f"review · score {item.get('score')}",
                    "detected_by": item.get("origin") or "relation_candidates",
                    "detected_label": "review-кандидат",
                    "summary": summary,
                }
            )
        return items

    def _enrich_relation_item(self, item: dict[str, Any]) -> dict[str, Any]:
        item = dict(item)
        item["relation_label"] = self.relation_label(item.get("relation_type", ""))
        item["layer_label"] = self.relation_layer_label(item.get("layer"))
        item["detected_label"] = self.relation_detected_label(item.get("detected_by"))
        item["summary"] = self.relation_summary(
            item.get("relation_type", ""),
            item.get("from_name", ""),
            item.get("to_name", ""),
        )
        bill = self._bill_context(item.get("to_name"))
        if bill:
            item["context_title"] = bill.get("title")
            item["context_subtitle"] = "законопроект"
            item["context_url"] = bill.get("duma_url")
        elif item.get("to_description"):
            item["context_title"] = item.get("to_description")
        evidence = self._evidence_context(item.get("evidence_item_id"))
        if evidence:
            item["evidence_title"] = evidence.get("title")
            item["evidence_url"] = evidence.get("url")
            item["evidence_content_id"] = evidence.get("id")
        return item

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

        raw_claims = [
            self._row_to_dict(item)
            for item in self.db.execute(
                """
                SELECT DISTINCT cl.id, cl.claim_text, cl.status, cl.confidence_final,
                       ci.id AS content_id, ci.title AS content_title,
                       (SELECT COUNT(*) FROM evidence_links el WHERE el.claim_id = cl.id) AS evidence_count
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
        claims = self._deduplicate_claim_items(raw_claims)[:20]

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
                       ef.canonical_name AS from_name, ef.entity_type AS from_type, ef.description AS from_description,
                       et.canonical_name AS to_name, et.entity_type AS to_type, et.description AS to_description
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
                relations.append(self._enrich_relation_item(item))
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
                       ci.id AS content_id, ci.title AS content_title, ci.url AS content_url,
                       ci.published_at, ci.content_type, s.name AS source_name,
                       (SELECT c.id FROM case_claims cc JOIN cases c ON c.id = cc.case_id WHERE cc.claim_id = cl.id LIMIT 1) AS case_id,
                       (SELECT c.title FROM case_claims cc JOIN cases c ON c.id = cc.case_id WHERE cc.claim_id = cl.id LIMIT 1) AS case_title,
                       (SELECT c.case_type FROM case_claims cc JOIN cases c ON c.id = cc.case_id WHERE cc.claim_id = cl.id LIMIT 1) AS case_type
                FROM claims cl
                JOIN content_items ci ON ci.id = cl.content_item_id
                LEFT JOIN sources s ON s.id = ci.source_id
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
                        SELECT el.id, el.evidence_type, el.strength, el.notes,
                               ci.id AS evidence_item_id, ci.title AS evidence_title,
                               ci.published_at AS evidence_published_at,
                               ci.content_type AS evidence_content_type,
                               s.name AS evidence_source_name
                        FROM evidence_links el
                        LEFT JOIN content_items ci ON ci.id = el.evidence_item_id
                        LEFT JOIN sources s ON s.id = ci.source_id
                        WHERE el.claim_id=?
                        ORDER BY el.id DESC
                        """,
                        (selected_id,),
                    ).fetchall()
                ] if self._table_exists("evidence_links") else []
                detail["evidence_graph"] = self._claim_evidence_graph(detail)
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
                raw_claims = [
                    self._row_to_dict(item)
                    for item in self.db.execute(
                        """
                        SELECT cl.id, cl.claim_text, cl.status, cl.confidence_final,
                               cl.content_item_id AS content_id, ci.title AS content_title,
                               (SELECT COUNT(*) FROM evidence_links el WHERE el.claim_id = cl.id) AS evidence_count
                        FROM case_claims cc
                        JOIN claims cl ON cl.id = cc.claim_id
                        LEFT JOIN content_items ci ON ci.id = cl.content_item_id
                        WHERE cc.case_id=?
                        ORDER BY cl.id DESC
                        """,
                        (selected_id,),
                    ).fetchall()
                ] if self._table_exists("case_claims") else []
                detail["claims_total"] = len(raw_claims)
                detail["claims"] = self._deduplicate_claim_items(raw_claims)
                detail["claims_hidden_count"] = max(0, detail["claims_total"] - len(detail["claims"]))
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
                       ef.canonical_name AS from_name, ef.entity_type AS from_type, ef.description AS from_description,
                       et.canonical_name AS to_name, et.entity_type AS to_type, et.description AS to_description
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
                all_rows.append(self._enrich_relation_item(item))

        if query:
            all_rows = [
                item
                for item in all_rows
                if query in (item["from_name"] + " " + item["to_name"] + " " + item["relation_type"]).casefold()
            ]
        if layer:
            all_rows = [item for item in all_rows if item["layer"] == layer]
        all_rows.sort(key=self.relation_sort_key)

        visible_rows = all_rows[:120]
        map_rows = list(all_rows)
        map_rows.extend(self._relation_candidate_map_items(query, layer))
        map_graph = self._relation_map_graph(map_rows)
        selected_id = filters.get("selected_id") or (all_rows[0]["id"] if all_rows else None)
        detail = next((item for item in all_rows if item["id"] == selected_id), None)
        if detail:
            detail = dict(detail)
            detail["evidence_graph"] = self._relation_evidence_graph(detail)
            detail["bridge_paths"] = self._relation_map_paths(
                map_graph,
                detail.get("from_entity_id"),
                detail.get("to_entity_id"),
            )
        return {
            "items": visible_rows,
            "detail": detail,
            "map_graph": map_graph,
        }

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
                   op.position_title, op.organization, op.source_url, op.is_active, op.source_type,
                   CASE
                       WHEN op.source_type LIKE 'executive_directory:%' THEN 0
                       WHEN op.organization LIKE '%Правительство%' THEN 1
                       WHEN op.organization LIKE '%Министерство%' THEN 1
                       WHEN op.organization LIKE '%служба%' THEN 1
                       WHEN op.organization LIKE '%казначейство%' THEN 1
                       WHEN op.organization LIKE '%антимонополь%' THEN 1
                       WHEN op.organization LIKE '%Совет Федерации%' THEN 2
                       WHEN op.organization LIKE '%Дума%' THEN 3
                       ELSE 4
                   END AS sort_priority
            FROM official_positions op
            JOIN entities e ON e.id = op.entity_id
            WHERE {' AND '.join(where)}
            ORDER BY sort_priority ASC, op.is_active DESC, op.organization, e.canonical_name, op.id DESC
            LIMIT 400
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
            "deputies_interval_seconds",
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
