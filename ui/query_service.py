from __future__ import annotations

import base64
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Iterable, Sequence
from typing import Any


DOCUMENT_CONTENT_TYPES = (
    "bill",
    "court_record",
    "declaration",
    "document",
    "official_doc",
    "official_document",
    "procurement",
    "registry_record",
    "restriction_record",
)


class CivicQueryService:
    """Isolated Reactor read side. Never opens legacy DBs or runs migrations.

    Fixed SQL identifiers, keyset pages, SQL-side text caps and a VM deadline
    bound work and transport size even when stored documents are very large.
    """

    MAX_LIMIT = 50
    TEXT_LIMIT = 512
    DETAIL_LIMIT = 4096
    # resource: (table/view, list fields, extra inspector fields, filter aliases)
    RESOURCES = {
        "monitoring": ("source_objects", "id external_id object_kind canonical_url source_system_id last_seen_at",
                       "first_seen_at", {"source_id": "source_system_id", "status": "object_kind", "date": "last_seen_at"}),
        "elections": ("election_campaigns", "id campaign_key title", "", {}),
        "protocols": ("election_protocol_versions", "id scope_id protocol_type version_no validation_state publication_status sealed recorded_at",
                      "source_revision_id provenance_group document_locator validation_json reported_at fetched_at timezone",
                      {"scope_id": "scope_id", "status": "validation_state", "publication_status": "publication_status", "date": "recorded_at"}),
        "incidents": ("election_incidents", "id scope_id kind description state recorded_at",
                      "candidate_key basis_json reviewer review_reason review_evidence_id",
                      {"scope_id": "scope_id", "status": "state", "date": "recorded_at"}),
        "election_claims": ("election_claims", "id scope_id attributed_to stance claim_text recorded_at",
                            "incident_id source_revision_id locator subject_entity_id",
                            {"scope_id": "scope_id", "status": "stance", "date": "recorded_at"}),
        "scopes": ("election_ballot_scopes", "id campaign_id ballot_id precinct_id", "", {"campaign_id": "campaign_id"}),
        "evidence": ("evidence_items", "id evidence_key evidence_type evidence_tier verification_state text_span source_revision_id recorded_at",
                     "locator source_owner entailment_score authenticity_score",
                     {"status": "verification_state", "revision_id": "source_revision_id", "date": "recorded_at"}),
        "claim_evidence": ("civic_evidence_links", "id claim_id source_revision_id stance verification_state authenticity_state origin_key",
                           "locator_json reviewed_by created_at",
                           {"claim_id": "claim_id", "revision_id": "source_revision_id", "status": "verification_state", "date": "created_at"}),
        "threads": ("investigation_threads", "id thread_key question workflow_state current_revision_id", "created_at",
                    {"status": "workflow_state", "date": "created_at"}),
        "claims": ("civic_claims", "id source_revision_id claim_text polarity modality attribution status", "claim_key locator_json created_at",
                   {"status": "status", "revision_id": "source_revision_id", "date": "created_at"}),
        "facts": ("current_facts_v", "id event_id fact_type predicate canonical_text polarity modality confidence",
                   "fact_key valid_from valid_to observed_at recorded_at generation_id",
                   {"status": "modality", "event_id": "event_id", "date": "recorded_at"}),
        "graph": ("current_relation_assertions_v", "id subject_entity_id predicate object_entity_id state confidence fact_id event_id",
                  "polarity valid_from valid_to observed_at promotion_reason generation_id",
                  {"status": "state", "event_id": "event_id", "date": "observed_at"}),
        "review": ("verification_reviews", "id subject_type subject_id decision reason reviewer created_at",
                   "decided_at", {"status": "decision", "date": "created_at"}),
    }

    def __init__(self, settings: dict[str, Any] | None = None):
        self.settings = settings or {}

    @property
    def enabled(self) -> bool:
        return self.settings.get("civic_workbench_enabled") is True

    def config(self) -> dict[str, Any]:
        return {"enabled": self.enabled, "readonly": True, "max_limit": self.MAX_LIMIT}

    @staticmethod
    def unavailable(resource: str, reason: str) -> dict[str, Any]:
        return {"resource": resource, "availability": "unavailable", "reason": reason,
                "items": [], "detail": None, "total": None, "next_cursor": None, "readonly": True}

    def request(self, payload: dict[str, Any], *, detail: bool = False) -> dict[str, Any]:
        result = self._request(payload, detail=detail)
        if self.enabled and payload.get("resource", "monitoring") == "monitoring" and not detail:
            # Separate short-lived read snapshots; neither uses the UI/legacy
            # connection. Summary cards are global, not filtered list totals.
            result["summary"] = {kind: self._monitoring_summary(kind) for kind in ("knowledge", "ops")}
        return result

    def _monitoring_summary(self, kind: str) -> dict[str, Any]:
        knowledge_tables = {"sources": "source_objects", "revisions": "source_revisions",
                            "claims": "civic_claims", "evidence": "evidence_items",
                            "evidence_links": "civic_evidence_links", "threads": "investigation_threads"}
        keys = tuple(knowledge_tables) if kind == "knowledge" else ("active_fenced", "pending", "failed", "last_heartbeat")
        result = {"availability": "unavailable", "values": dict.fromkeys(keys), "readonly": True,
                  "sampled_at": datetime.now(timezone.utc).isoformat(), "scope": "global"}
        root = Path(self.settings.get("reactor_db_dir") or Path(__file__).resolve().parents[1] / "db")
        filename = "reactor_v2.db" if kind == "knowledge" else "reactor_ops.db"
        path = Path(self.settings.get(f"reactor_{kind}_db") or root / filename).resolve()
        conn = None
        try:
            conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.25)
            conn.execute("PRAGMA query_only=ON")
            deadline = time.monotonic() + 1.5
            conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
            conn.execute("BEGIN")
            if kind == "knowledge":
                tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if "source_revisions" not in tables:
                    raise sqlite3.DatabaseError("not Reactor knowledge")
                for key, table in knowledge_tables.items():
                    if table in tables:
                        result["values"][key] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                result["availability"] = "available" if all(v is not None for v in result["values"].values()) else "partial"
                if result["availability"] == "partial":
                    result["reason"] = "Some knowledge tables are unavailable."
            else:
                required = {"status", "lease_owner", "lease_token", "lease_expires_at", "deadline_at", "heartbeat_at"}
                columns = {row[1] for row in conn.execute("PRAGMA table_info(agent_tasks)")}
                if not required <= columns:
                    raise sqlite3.DatabaseError("fenced agent_tasks schema unavailable")
                row = conn.execute("""
                    SELECT COUNT(CASE WHEN status='running'
                        AND length(trim(COALESCE(lease_owner,'')))>0
                        AND length(trim(COALESCE(lease_token,'')))>0
                        AND julianday(lease_expires_at)>julianday('now')
                        AND (deadline_at IS NULL OR julianday(deadline_at)>julianday('now')) THEN 1 END),
                        COUNT(CASE WHEN status IN ('pending','needs_retry') THEN 1 END),
                        COUNT(CASE WHEN status='failed' THEN 1 END),
                        strftime('%Y-%m-%dT%H:%M:%SZ', MAX(julianday(heartbeat_at)))
                    FROM agent_tasks
                """).fetchone()
                result["values"] = dict(zip(keys, row))
                result["availability"] = "available"
        except sqlite3.Error as exc:
            # Discard partial counts on query failure; unknown must not look like
            # an empty healthy database. Successful empty tables do return zero.
            result["values"] = dict.fromkeys(keys)
            result["availability"] = "unavailable"
            result["reason"] = "Summary query budget exceeded." if "interrupt" in str(exc) else f"Reactor {kind} database or required schema is unavailable."
        finally:
            if conn is not None:
                conn.close()
        return result

    def _request(self, payload: dict[str, Any], *, detail: bool = False) -> dict[str, Any]:
        resource = str(payload.get("resource", "monitoring"))[:40]
        if not self.enabled:
            return self.unavailable(resource, "Civic workbench is disabled in settings.")
        if resource not in self.RESOURCES:
            return self.unavailable(resource, "Unsupported Reactor resource.")
        # Resolve exactly the same settings as Reactor, without importing its
        # bootstrap/migration module or creating directories/files.
        root = Path(self.settings.get("reactor_db_dir") or Path(__file__).resolve().parents[1] / "db")
        path = Path(self.settings.get("reactor_knowledge_db") or root / "reactor_v2.db").resolve()
        conn = None
        try:
            conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.25)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
            deadline = time.monotonic() + 1.5
            conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
            conn.create_function("ui_casefold", 1, UIQueryService._casefold, deterministic=True)
            conn.execute("BEGIN")
            return self._read(conn, resource, payload, detail)
        except (KeyError, ValueError, TypeError, OverflowError):
            return self.unavailable(resource, "Invalid filters, cursor or record ID.")
        except sqlite3.Error as exc:
            reason = "Query budget exceeded; narrow the filters." if "interrupt" in str(exc) else "Reactor database or required schema is unavailable. No migration was attempted."
            return self.unavailable(resource, reason)
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def _select(fields: list[str], cap: int) -> str:
        return ",".join(f"CASE WHEN typeof({field}) IN ('text','blob') THEN substr(CAST({field} AS TEXT),1,{cap}) ELSE {field} END AS {field}" for field in fields)

    def _rows(self, conn, table, fields, where, params, *, limit=51, cap=512):
        sql = f"SELECT {self._select(fields, cap)} FROM {table} WHERE {where} ORDER BY {fields[0]} DESC LIMIT ?"
        return [dict(row) for row in conn.execute(sql, (*params, limit))]

    def _read(self, conn, resource, payload, detail):
        # This marker prevents accidental use of the similarly named legacy schema.
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='source_revisions' AND type='table'").fetchone():
            return self.unavailable(resource, "Not a Reactor knowledge database.")
        table, list_fields, extra_fields, aliases = self.RESOURCES[resource]
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        fields = list_fields.split()
        if not set(fields + extra_fields.split()) <= columns:
            return self.unavailable(resource, "Required Reactor schema is not installed for this screen.")
        result = {"resource": resource, "source": table, "readonly": True,
                  "text_limit": self.DETAIL_LIMIT if detail else self.TEXT_LIMIT,
                  "filters": [key for key in aliases if key != "date"] + (["date_from", "date_to"] if "date" in aliases else [])}
        if detail:
            object_id = int(payload["id"])
            rows = self._rows(conn, table, fields + extra_fields.split(), "id=?", [object_id], limit=1, cap=self.DETAIL_LIMIT)
            result.update(availability="available" if rows else "empty", detail=rows[0] if rows else None)
            if rows:
                result["related"] = self._related(conn, resource, rows[0])
                result["related_limit"] = 20
            return result
        filters = payload.get("filters") or {}
        if not isinstance(filters, dict):
            raise ValueError("filters must be an object")
        allowed = {"query", "limit", "cursor", *result["filters"]}
        if any(key not in allowed and value not in (None, "") for key, value in filters.items()):
            raise ValueError("unsupported filter")
        limit = min(self.MAX_LIMIT, max(1, int(filters.get("limit") or 25)))
        where, params = ["1=1"], []
        query = str(filters.get("query") or "")
        if len(query) > 256:
            raise ValueError("query too long")
        if query.strip():
            search_fields = fields[1:]
            where.append("(" + " OR ".join(f"ui_casefold({field}) LIKE ? ESCAPE '\\'" for field in search_fields) + ")")
            params.extend([UIQueryService._like_pattern(query)] * len(search_fields))
        for key, column in aliases.items():
            if key == "date":
                for bound, operator in (("date_from", ">="), ("date_to", "<=")):
                    if filters.get(bound):
                        where.append(f"substr({column},1,10) {operator} ?")
                        params.append(str(filters[bound])[:10])
            elif filters.get(key) not in (None, ""):
                where.append(f"{column}=?")
                params.append(str(filters[key])[:128])
        predicate = " AND ".join(where)
        total = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {predicate}", params).fetchone()[0]
        if filters.get("cursor"):
            cursor = str(filters["cursor"])
            if len(cursor) > 256:
                raise ValueError("cursor too long")
            where.append("id < ?")
            params.append(UIQueryService._decode_cursor(cursor, resource))
        rows = self._rows(conn, table, fields, " AND ".join(where), params, limit=limit + 1)
        more = len(rows) > limit
        rows = rows[:limit]
        result.update(availability="available" if rows else "empty", items=rows, total=total,
                      limit=limit, next_cursor=UIQueryService._encode_cursor(resource, rows[-1]["id"]) if more else None)
        if resource == "graph":
            ids = sorted({row[key] for row in rows for key in ("subject_entity_id", "object_entity_id")})
            nodes = self._rows(conn, "entities", ["id", "canonical_name", "entity_type"],
                               "id IN (" + ",".join("?" for _ in ids) + ")", ids, limit=100) if ids else []
            result["graph"] = {"nodes": [{"id": node["id"], "label": node["canonical_name"]} for node in nodes],
                               "edges": [{"source": row["subject_entity_id"], "target": row["object_entity_id"], "label": row["predicate"]} for row in rows]}
            names = {node["id"]: node["canonical_name"] for node in nodes}
            for row in rows:
                row["subject_name"] = names.get(row["subject_entity_id"])
                row["object_name"] = names.get(row["object_entity_id"])
        return result

    def _related(self, conn, resource, row):
        specs = []
        if resource == "monitoring":
            specs.append(("current_revision", "source_revisions", "id revision_no observed_at fetched_at payload_json", "source_object_id=? AND is_current=1", row["id"]))
        if resource in ("evidence", "protocols", "election_claims", "claims", "claim_evidence"):
            specs.append(("source_revision", "source_revisions", "id source_object_id revision_no payload_hash observed_at fetched_at payload_json", "id=?", row["source_revision_id"]))
        if resource == "claims":
            specs.append(("evidence_links", "civic_evidence_links", "source_revision_id stance locator_json origin_key verification_state authenticity_state", "claim_id=?", row["id"]))
        if resource == "facts":
            specs.append(("evidence", "evidence_items", "id source_revision_id evidence_tier verification_state locator text_span", "id IN (SELECT evidence_item_id FROM fact_evidence WHERE fact_id=?)", row["id"]))
        if resource == "graph":
            specs.append(("evidence", "evidence_items", "id source_revision_id evidence_tier verification_state locator text_span", "id IN (SELECT evidence_item_id FROM relation_assertion_evidence WHERE assertion_id=?)", row["id"]))
        if resource == "protocols":
            specs.append(("numbers", "election_protocol_numbers", "field_key value verification_state source_revision_id locator verified_by", "protocol_id=?", row["id"]))
            specs.append(("acceptance", "election_accepted_protocols", "scope_id protocol_type protocol_id reviewer reason accepted_at", "protocol_id=?", row["id"]))
        if resource == "scopes":
            specs.extend([
                ("ballot", "election_ballots", "id campaign_id ballot_key title", "id=?", row["ballot_id"]),
                ("precinct", "election_precincts", "id campaign_id jurisdiction official_id category", "id=?", row["precinct_id"]),
            ])
        related = {}
        if resource == "threads":
            specs.extend([
                ("current_revision", "thread_revisions", "id thread_id revision_no previous_revision_id operation reason actor membership_json created_at", "id=? AND thread_id=" + str(int(row["id"])), row["current_revision_id"]),
                ("revision_history", "thread_revisions", "id thread_id revision_no operation reason actor created_at", "thread_id=?", row["id"]),
            ])
        for name, table, fields, predicate, value in specs:
            rows = self._rows(conn, table, fields.split(), predicate, [value], limit=21, cap=1024)
            related[name] = {"items": rows[:20], "has_more": len(rows) > 20}
        return related


class UIQueryService:
    """Server-side queries for the web UI.

    Every list operation applies filters in SQL before pagination. Cursor pages
    use the immutable primary key as a deterministic tie-breaker, so inserting
    newer rows does not move records between already-issued cursor pages.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        default_limit: int = 50,
        max_limit: int = 200,
    ) -> None:
        self.conn = conn
        self.default_limit = max(1, int(default_limit))
        self.max_limit = max(self.default_limit, int(max_limit))
        self.conn.create_function("ui_casefold", 1, self._casefold, deterministic=True)

    @staticmethod
    def _casefold(value: Any) -> str:
        return "" if value is None else str(value).casefold()

    def query(self, resource: str, **filters: Any) -> dict[str, Any]:
        handlers = {
            "content": self.query_content,
            "events": self.query_events,
            "entities": self.query_entities,
            "documents": self.query_documents,
            "relations": self.query_relations,
            "review": self.query_review_queues,
            "review_queues": self.query_review_queues,
        }
        try:
            handler = handlers[resource]
        except KeyError as exc:
            raise ValueError(f"Unsupported UI resource: {resource}") from exc
        return handler(**filters)

    def detail(self, resource: str, object_id: int) -> dict[str, Any] | None:
        handlers = {
            "content": self.content_detail,
            "events": self.event_detail,
            "entities": self.entity_detail,
            "documents": self.document_detail,
            "relations": self.relation_detail,
            "review": self.review_detail,
            "review_queues": self.review_detail,
        }
        try:
            handler = handlers[resource]
        except KeyError as exc:
            raise ValueError(f"Unsupported UI resource: {resource}") from exc
        return handler(int(object_id))

    def query_content(
        self,
        *,
        query: str = "",
        source_id: int | None = None,
        content_type: str = "",
        status: str = "",
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if not self._object_exists("content_items"):
            return self._empty_page("content", limit, offset)
        where, params = self._content_filters(
            query=query,
            source_id=source_id,
            content_type=content_type,
            status=status,
            date_from=date_from,
            date_to=date_to,
            alias="ci",
        )
        return self._query_page(
            resource="content",
            select_sql="""
                SELECT ci.id, ci.source_id, ci.external_id, ci.content_type,
                       ci.title, ci.published_at, ci.collected_at, ci.url,
                       ci.language, ci.status, s.name AS source_name,
                       s.category AS source_category
                FROM content_items ci
                LEFT JOIN sources s ON s.id=ci.source_id
            """,
            count_sql="SELECT COUNT(*) FROM content_items ci",
            where=where,
            params=params,
            id_expr="ci.id",
            limit=limit,
            offset=offset,
            cursor=cursor,
            facets=self._content_facets(where, params),
        )

    def query_documents(
        self,
        *,
        query: str = "",
        source_id: int | None = None,
        content_type: str = "",
        status: str = "",
        limit: int | None = None,
        offset: int = 0,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if not self._object_exists("content_items"):
            return self._empty_page("documents", limit, offset)
        where, params = self._content_filters(
            query=query,
            source_id=source_id,
            content_type=content_type,
            status=status,
            alias="ci",
            include_ocr=True,
        )
        placeholders = ",".join("?" for _ in DOCUMENT_CONTENT_TYPES)
        document_clause = f"ci.content_type IN ({placeholders})"
        params.extend(DOCUMENT_CONTENT_TYPES)
        if self._object_exists("attachments"):
            document_clause = (
                f"({document_clause} OR EXISTS ("
                "SELECT 1 FROM attachments da WHERE da.content_item_id=ci.id "
                "AND (da.attachment_type IN ('document','pdf','image') "
                "OR COALESCE(da.ocr_text,'') <> '')))"
            )
        where.append(document_clause)
        page = self._query_page(
            resource="documents",
            select_sql="""
                SELECT ci.id, ci.source_id, ci.external_id, ci.content_type,
                       ci.title, ci.published_at, ci.collected_at, ci.url,
                       ci.status, s.name AS source_name,
                       (SELECT COUNT(*) FROM attachments ax WHERE ax.content_item_id=ci.id)
                           AS attachment_count
                FROM content_items ci
                LEFT JOIN sources s ON s.id=ci.source_id
            """ if self._object_exists("attachments") else """
                SELECT ci.id, ci.source_id, ci.external_id, ci.content_type,
                       ci.title, ci.published_at, ci.collected_at, ci.url,
                       ci.status, s.name AS source_name, 0 AS attachment_count
                FROM content_items ci
                LEFT JOIN sources s ON s.id=ci.source_id
            """,
            count_sql="SELECT COUNT(*) FROM content_items ci",
            where=where,
            params=params,
            id_expr="ci.id",
            limit=limit,
            offset=offset,
            cursor=cursor,
            facets=self._content_facets(where, params),
        )
        return page

    def query_events(
        self,
        *,
        query: str = "",
        event_type: str = "",
        status: str = "",
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        source = self._active_or_legacy("current_events_v", "events")
        if source is None:
            return self._empty_page("events", limit, offset)
        where: list[str] = []
        params: list[Any] = []
        self._add_search(
            where,
            params,
            query,
            ("e.canonical_title", "e.summary_short", "e.summary_long", "e.event_type"),
        )
        self._add_equal(where, params, "e.event_type", event_type)
        self._add_equal(where, params, "e.status", status)
        if date_from:
            where.append("COALESCE(e.event_date_end,e.event_date_start,'') >= ?")
            params.append(date_from)
        if date_to:
            where.append("COALESCE(e.event_date_start,e.event_date_end,'') <= ?")
            params.append(date_to)
        if source == "events" and "superseded_at" in self._columns(source):
            where.append("e.superseded_at IS NULL")
        return self._query_page(
            resource="events",
            select_sql=f"""
                SELECT e.id, e.canonical_title, e.event_type, e.summary_short,
                       e.status, e.event_date_start, e.event_date_end,
                       e.importance_score, e.confidence
                FROM {source} e
            """,
            count_sql=f"SELECT COUNT(*) FROM {source} e",
            where=where,
            params=params,
            id_expr="e.id",
            limit=limit,
            offset=offset,
            cursor=cursor,
            facets=self._facet_groups(
                f"FROM {source} e", where, params,
                (("event_type", "e.event_type"), ("status", "e.status")),
            ),
            source=source,
        )

    def query_entities(
        self,
        *,
        query: str = "",
        entity_type: str = "",
        limit: int | None = None,
        offset: int = 0,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if not self._object_exists("entities"):
            return self._empty_page("entities", limit, offset)
        where: list[str] = []
        params: list[Any] = []
        search_columns = ["e.canonical_name", "e.entity_type"]
        if "description" in self._columns("entities"):
            search_columns.append("e.description")
        if query:
            terms = [f"ui_casefold(COALESCE({column},'')) LIKE ? ESCAPE '\\'" for column in search_columns]
            pattern = self._like_pattern(query)
            params.extend(pattern for _ in terms)
            if self._object_exists("entity_aliases"):
                terms.append(
                    "EXISTS (SELECT 1 FROM entity_aliases ea WHERE ea.entity_id=e.id "
                    "AND ui_casefold(COALESCE(ea.alias,'')) LIKE ? ESCAPE '\\')"
                )
                params.append(pattern)
            where.append("(" + " OR ".join(terms) + ")")
        self._add_equal(where, params, "e.entity_type", entity_type)
        mention_expr = (
            "(SELECT COUNT(*) FROM entity_mentions em WHERE em.entity_id=e.id)"
            if self._object_exists("entity_mentions") else "0"
        )
        return self._query_page(
            resource="entities",
            select_sql=f"""
                SELECT e.id, e.canonical_name, e.entity_type, e.description,
                       e.inn, e.ogrn, {mention_expr} AS content_count
                FROM entities e
            """,
            count_sql="SELECT COUNT(*) FROM entities e",
            where=where,
            params=params,
            id_expr="e.id",
            limit=limit,
            offset=offset,
            cursor=cursor,
            facets=self._facet_groups(
                "FROM entities e", where, params, (("entity_type", "e.entity_type"),)
            ),
        )

    def query_relations(
        self,
        *,
        query: str = "",
        state: str = "",
        predicate: str = "",
        entity_id: int | None = None,
        limit: int | None = None,
        offset: int = 0,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        active = self._active_or_legacy("current_relation_assertions_v", "entity_relations")
        if active is None:
            return self._empty_page("relations", limit, offset)
        where: list[str] = []
        params: list[Any] = []
        if active == "current_relation_assertions_v":
            self._add_search(
                where, params, query,
                ("se.canonical_name", "oe.canonical_name", "r.predicate"),
            )
            self._add_equal(where, params, "r.state", state)
            self._add_equal(where, params, "r.predicate", predicate)
            if entity_id is not None:
                where.append("(r.subject_entity_id=? OR r.object_entity_id=?)")
                params.extend((int(entity_id), int(entity_id)))
            from_sql = """
                FROM current_relation_assertions_v r
                JOIN entities se ON se.id=r.subject_entity_id
                JOIN entities oe ON oe.id=r.object_entity_id
            """
            select_sql = """
                SELECT r.id, r.subject_entity_id AS from_entity_id,
                       r.object_entity_id AS to_entity_id, r.predicate AS relation_type,
                       r.confidence, r.state, r.polarity, r.event_id, r.fact_id,
                       r.valid_from, r.valid_to, r.observed_at,
                       se.canonical_name AS from_name, se.entity_type AS from_type,
                       oe.canonical_name AS to_name, oe.entity_type AS to_type
            """ + from_sql
            facet_specs = (("state", "r.state"), ("predicate", "r.predicate"))
        else:
            self._add_search(
                where, params, query,
                ("se.canonical_name", "oe.canonical_name", "r.relation_type"),
            )
            self._add_equal(where, params, "r.relation_type", predicate)
            if state:
                where.append("1=0")
            if entity_id is not None:
                where.append("(r.from_entity_id=? OR r.to_entity_id=?)")
                params.extend((int(entity_id), int(entity_id)))
            if "superseded_at" in self._columns("entity_relations"):
                where.append("r.superseded_at IS NULL")
            from_sql = """
                FROM entity_relations r
                JOIN entities se ON se.id=r.from_entity_id
                JOIN entities oe ON oe.id=r.to_entity_id
            """
            select_sql = """
                SELECT r.id, r.from_entity_id, r.to_entity_id,
                       r.relation_type, r.strength, r.detected_by,
                       r.evidence_item_id, r.valid_from, r.valid_to, r.observed_at,
                       se.canonical_name AS from_name, se.entity_type AS from_type,
                       oe.canonical_name AS to_name, oe.entity_type AS to_type
            """ + from_sql
            facet_specs = (("predicate", "r.relation_type"), ("strength", "r.strength"))
        return self._query_page(
            resource="relations",
            select_sql=select_sql,
            count_sql="SELECT COUNT(*) " + from_sql,
            where=where,
            params=params,
            id_expr="r.id",
            limit=limit,
            offset=offset,
            cursor=cursor,
            facets=self._facet_groups(from_sql, where, params, facet_specs),
            source=active,
        )

    def query_review_queues(
        self,
        *,
        query: str = "",
        queue: str = "",
        status: str = "",
        subject_type: str = "",
        action: str = "",
        limit: int | None = None,
        offset: int = 0,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if not self._object_exists("review_tasks"):
            return self._empty_page("review", limit, offset)
        where: list[str] = []
        params: list[Any] = []
        self._add_search(
            where,
            params,
            query,
            ("r.task_key", "r.queue_key", "r.subject_type", "r.machine_reason", "r.suggested_action"),
        )
        self._add_equal(where, params, "r.queue_key", queue)
        self._add_equal(where, params, "r.status", status)
        self._add_equal(where, params, "r.subject_type", subject_type)
        self._add_equal(where, params, "r.suggested_action", action)
        return self._query_page(
            resource="review",
            select_sql="""
                SELECT r.id, r.task_key, r.queue_key, r.subject_type,
                       r.subject_id, r.related_id, r.suggested_action,
                       r.confidence, r.machine_reason, r.status,
                       r.review_pack_id, r.created_at, r.updated_at
                FROM review_tasks r
            """,
            count_sql="SELECT COUNT(*) FROM review_tasks r",
            where=where,
            params=params,
            id_expr="r.id",
            limit=limit,
            offset=offset,
            cursor=cursor,
            facets=self._facet_groups(
                "FROM review_tasks r",
                where,
                params,
                (
                    ("queue", "r.queue_key"),
                    ("status", "r.status"),
                    ("subject_type", "r.subject_type"),
                    ("action", "r.suggested_action"),
                ),
            ),
        )

    def content_detail(self, content_id: int) -> dict[str, Any] | None:
        if not self._object_exists("content_items"):
            return None
        row = self._fetchone(
            """
            SELECT ci.*, s.name AS source_name, s.category AS source_category,
                   s.credibility_tier AS source_credibility_tier
            FROM content_items ci
            LEFT JOIN sources s ON s.id=ci.source_id
            WHERE ci.id=?
            """,
            (int(content_id),),
        )
        if row is None:
            return None
        row["attachments"] = self._related_rows(
            "attachments",
            "SELECT * FROM attachments WHERE content_item_id=? ORDER BY id",
            content_id,
        )
        row["entities"] = self._related_rows(
            "entity_mentions",
            """
            SELECT e.id, e.canonical_name, e.entity_type, em.mention_type, em.confidence
            FROM entity_mentions em JOIN entities e ON e.id=em.entity_id
            WHERE em.content_item_id=? ORDER BY em.confidence DESC, e.canonical_name
            """,
            content_id,
        )
        row["claims"] = self._related_rows(
            "claims",
            "SELECT * FROM claims WHERE content_item_id=? ORDER BY id",
            content_id,
        )
        return row

    def document_detail(self, content_id: int) -> dict[str, Any] | None:
        detail = self.content_detail(content_id)
        if detail is None:
            return None
        detail["document_reviews"] = self._related_rows(
            "review_tasks",
            """
            SELECT * FROM review_tasks
            WHERE subject_type='content_item' AND subject_id=? AND queue_key='documents'
            ORDER BY id DESC
            """,
            content_id,
        )
        return detail

    def event_detail(self, event_id: int) -> dict[str, Any] | None:
        source = self._active_or_legacy("current_events_v", "events")
        if source is None:
            return None
        detail = self._fetchone(f"SELECT * FROM {source} WHERE id=?", (int(event_id),))
        if detail is None:
            return None
        active_filter = " AND superseded_at IS NULL"
        detail["timeline"] = self._related_rows(
            "event_timeline",
            "SELECT * FROM event_timeline WHERE event_id=?"
            + (active_filter if "superseded_at" in self._columns("event_timeline") else "")
            + " ORDER BY COALESCE(timeline_date,''), sort_order, id",
            event_id,
        )
        detail["entities"] = self._related_rows(
            "event_entities",
            """
            SELECT ee.*, e.canonical_name, e.entity_type
            FROM event_entities ee JOIN entities e ON e.id=ee.entity_id
            WHERE ee.event_id=?
            """ + (" AND ee.superseded_at IS NULL" if "superseded_at" in self._columns("event_entities") else "")
            + " ORDER BY ee.role, ee.confidence DESC, e.canonical_name",
            event_id,
        )
        detail["facts"] = self._related_rows(
            "event_facts",
            "SELECT * FROM event_facts WHERE event_id=?"
            + (active_filter if "superseded_at" in self._columns("event_facts") else "")
            + " ORDER BY id",
            event_id,
        )
        detail["items"] = self._related_rows(
            "event_items",
            """
            SELECT ei.*, ci.title, ci.content_type, ci.published_at, ci.url
            FROM event_items ei LEFT JOIN content_items ci ON ci.id=ei.content_item_id
            WHERE ei.event_id=?
            """ + (" AND ei.superseded_at IS NULL" if "superseded_at" in self._columns("event_items") else "")
            + " ORDER BY ei.id",
            event_id,
        )
        return detail

    def entity_detail(self, entity_id: int) -> dict[str, Any] | None:
        if not self._object_exists("entities"):
            return None
        detail = self._fetchone("SELECT * FROM entities WHERE id=?", (int(entity_id),))
        if detail is None:
            return None
        detail["aliases"] = self._related_rows(
            "entity_aliases", "SELECT * FROM entity_aliases WHERE entity_id=? ORDER BY alias", entity_id
        )
        detail["content"] = self._related_rows(
            "entity_mentions",
            """
            SELECT DISTINCT ci.id, ci.title, ci.content_type, ci.published_at, ci.url
            FROM entity_mentions em JOIN content_items ci ON ci.id=em.content_item_id
            WHERE em.entity_id=? ORDER BY ci.id DESC LIMIT 50
            """,
            entity_id,
        )
        detail["events"] = self._related_rows(
            "event_entities",
            """
            SELECT ee.role, ee.confidence, ev.id, ev.canonical_title,
                   ev.event_type, ev.event_date_start
            FROM event_entities ee JOIN events ev ON ev.id=ee.event_id
            WHERE ee.entity_id=?
            """ + (" AND ee.superseded_at IS NULL" if "superseded_at" in self._columns("event_entities") else "")
            + " ORDER BY ev.id DESC LIMIT 50",
            entity_id,
        )
        return detail

    def relation_detail(self, relation_id: int) -> dict[str, Any] | None:
        source = self._active_or_legacy("current_relation_assertions_v", "entity_relations")
        if source is None:
            return None
        if source == "current_relation_assertions_v":
            detail = self._fetchone(
                """
                SELECT r.*, se.canonical_name AS from_name, se.entity_type AS from_type,
                       oe.canonical_name AS to_name, oe.entity_type AS to_type
                FROM current_relation_assertions_v r
                JOIN entities se ON se.id=r.subject_entity_id
                JOIN entities oe ON oe.id=r.object_entity_id
                WHERE r.id=?
                """,
                (int(relation_id),),
            )
            if detail is not None:
                detail["evidence"] = self._related_rows(
                    "relation_assertion_evidence",
                    "SELECT * FROM relation_assertion_evidence WHERE assertion_id=? ORDER BY id",
                    relation_id,
                )
            return detail
        return self._fetchone(
            """
            SELECT r.*, se.canonical_name AS from_name, se.entity_type AS from_type,
                   oe.canonical_name AS to_name, oe.entity_type AS to_type
            FROM entity_relations r
            JOIN entities se ON se.id=r.from_entity_id
            JOIN entities oe ON oe.id=r.to_entity_id
            WHERE r.id=?
            """,
            (int(relation_id),),
        )

    def review_detail(self, task_id: int) -> dict[str, Any] | None:
        if not self._object_exists("review_tasks"):
            return None
        detail = self._fetchone("SELECT * FROM review_tasks WHERE id=?", (int(task_id),))
        if detail is None:
            return None
        detail["candidate_payload_json"] = self._json_value(detail.get("candidate_payload"), {})
        detail["source_links"] = self._json_value(detail.get("source_links_json"), [])
        return detail

    def _content_filters(
        self,
        *,
        query: str,
        source_id: int | None,
        content_type: str,
        status: str,
        alias: str,
        date_from: str | None = None,
        date_to: str | None = None,
        include_ocr: bool = False,
    ) -> tuple[list[str], list[Any]]:
        where: list[str] = []
        params: list[Any] = []
        columns = [f"{alias}.title", f"{alias}.body_text", f"{alias}.external_id", f"{alias}.url"]
        if query and include_ocr and self._object_exists("attachments"):
            pattern = self._like_pattern(query)
            clauses = [f"ui_casefold(COALESCE({column},'')) LIKE ? ESCAPE '\\'" for column in columns]
            params.extend(pattern for _ in clauses)
            clauses.append(
                f"EXISTS (SELECT 1 FROM attachments qa WHERE qa.content_item_id={alias}.id "
                "AND ui_casefold(COALESCE(qa.ocr_text,'')) LIKE ? ESCAPE '\\')"
            )
            params.append(pattern)
            where.append("(" + " OR ".join(clauses) + ")")
        else:
            self._add_search(where, params, query, columns)
        if source_id is not None:
            where.append(f"{alias}.source_id=?")
            params.append(int(source_id))
        self._add_equal(where, params, f"{alias}.content_type", content_type)
        self._add_equal(where, params, f"{alias}.status", status)
        if date_from:
            where.append(f"COALESCE({alias}.published_at,{alias}.collected_at,'') >= ?")
            params.append(date_from)
        if date_to:
            where.append(f"COALESCE({alias}.published_at,{alias}.collected_at,'') <= ?")
            params.append(date_to)
        return where, params

    def _content_facets(self, where: list[str], params: list[Any]) -> dict[str, list[dict[str, Any]]]:
        return self._facet_groups(
            "FROM content_items ci",
            where,
            params,
            (("content_type", "ci.content_type"), ("status", "ci.status"), ("source_id", "ci.source_id")),
        )

    def _query_page(
        self,
        *,
        resource: str,
        select_sql: str,
        count_sql: str,
        where: Sequence[str],
        params: Sequence[Any],
        id_expr: str,
        limit: int | None,
        offset: int,
        cursor: str | None,
        facets: dict[str, list[dict[str, Any]]],
        source: str | None = None,
    ) -> dict[str, Any]:
        page_limit = self._limit(limit)
        page_offset = max(0, int(offset or 0))
        base_where = list(where)
        base_params = list(params)
        total = int(self.conn.execute(
            count_sql + self._where_sql(base_where), base_params
        ).fetchone()[0])
        page_where = list(base_where)
        page_params = list(base_params)
        if cursor:
            cursor_id = self._decode_cursor(cursor, resource)
            page_where.append(f"{id_expr} < ?")
            page_params.append(cursor_id)
            page_offset = 0
        sql = (
            select_sql
            + self._where_sql(page_where)
            + f" ORDER BY {id_expr} DESC LIMIT ? OFFSET ?"
        )
        page_params.extend((page_limit + 1, page_offset))
        rows = self._fetchall(sql, page_params)
        has_more = len(rows) > page_limit
        items = rows[:page_limit]
        next_cursor = self._encode_cursor(resource, int(items[-1]["id"])) if has_more and items else None
        return {
            "items": items,
            "total": total,
            "limit": page_limit,
            "offset": page_offset,
            "next_cursor": next_cursor,
            "facets": facets,
            "source": source,
        }

    def _facet_groups(
        self,
        from_sql: str,
        where: Sequence[str],
        params: Sequence[Any],
        specifications: Iterable[tuple[str, str]],
    ) -> dict[str, list[dict[str, Any]]]:
        facets: dict[str, list[dict[str, Any]]] = {}
        where_sql = self._where_sql(where)
        for name, expression in specifications:
            sql = (
                f"SELECT {expression} AS value, COUNT(*) AS count {from_sql} {where_sql} "
                f"GROUP BY {expression} ORDER BY count DESC, value LIMIT 100"
            )
            facets[name] = [row for row in self._fetchall(sql, params) if row["value"] is not None]
        return facets

    def _related_rows(self, table: str, sql: str, object_id: int) -> list[dict[str, Any]]:
        if not self._object_exists(table):
            return []
        return self._fetchall(sql, (int(object_id),))

    def _active_or_legacy(self, active_view: str, legacy_table: str) -> str | None:
        if self._object_exists(active_view):
            return active_view
        return legacy_table if self._object_exists(legacy_table) else None

    def _object_exists(self, name: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name=? AND type IN ('table','view')",
            (name,),
        ).fetchone()
        return row is not None

    def _columns(self, name: str) -> set[str]:
        if not self._object_exists(name):
            return set()
        # The identifier originates from sqlite_master, never from user input.
        return {str(row[1]) for row in self.conn.execute(f'PRAGMA table_info("{name}")').fetchall()}

    def _fetchone(self, sql: str, params: Sequence[Any] = ()) -> dict[str, Any] | None:
        cursor = self.conn.execute(sql, tuple(params))
        row = cursor.fetchone()
        if row is None:
            return None
        return self._row_dict(cursor, row)

    def _fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        cursor = self.conn.execute(sql, tuple(params))
        return [self._row_dict(cursor, row) for row in cursor.fetchall()]

    @staticmethod
    def _row_dict(cursor: sqlite3.Cursor, row: Sequence[Any]) -> dict[str, Any]:
        if isinstance(row, sqlite3.Row):
            return dict(row)
        return {description[0]: value for description, value in zip(cursor.description or (), row)}

    @staticmethod
    def _where_sql(where: Sequence[str]) -> str:
        return " WHERE " + " AND ".join(where) if where else ""

    def _add_search(
        self,
        where: list[str],
        params: list[Any],
        query: str,
        columns: Sequence[str],
    ) -> None:
        if not query or not query.strip():
            return
        pattern = self._like_pattern(query)
        clauses = [f"ui_casefold(COALESCE({column},'')) LIKE ? ESCAPE '\\'" for column in columns]
        where.append("(" + " OR ".join(clauses) + ")")
        params.extend(pattern for _ in clauses)

    @staticmethod
    def _add_equal(where: list[str], params: list[Any], column: str, value: Any) -> None:
        if value is None or value == "":
            return
        where.append(f"{column}=?")
        params.append(value)

    @staticmethod
    def _like_pattern(query: str) -> str:
        value = str(query).strip().casefold()
        value = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        return f"%{value}%"

    def _limit(self, value: int | None) -> int:
        return min(self.max_limit, max(1, int(value or self.default_limit)))

    @staticmethod
    def _encode_cursor(resource: str, row_id: int) -> str:
        raw = json.dumps({"resource": resource, "id": row_id}, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    @staticmethod
    def _decode_cursor(value: str, resource: str) -> int:
        try:
            padded = value + "=" * (-len(value) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
            if payload.get("resource") != resource:
                raise ValueError("cursor resource mismatch")
            row_id = int(payload["id"])
            if row_id < 1:
                raise ValueError("invalid cursor id")
            return row_id
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("Invalid pagination cursor") from exc

    def _empty_page(self, resource: str, limit: int | None, offset: int) -> dict[str, Any]:
        return {
            "items": [],
            "total": 0,
            "limit": self._limit(limit),
            "offset": max(0, int(offset or 0)),
            "next_cursor": None,
            "facets": {},
            "source": None,
            "resource": resource,
        }

    @staticmethod
    def _json_value(value: Any, default: Any) -> Any:
        if not value:
            return default
        try:
            return json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return default
