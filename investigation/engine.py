from __future__ import annotations

import json
import heapq
import logging
import sqlite3
from collections import defaultdict, deque
from itertools import count
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

from .models import (
    Confidence,
    EvidenceChain,
    EvidenceItem,
    InvestigationEdge,
    InvestigationNode,
    InvestigationResult,
    Lead,
    NodeType,
    RELATION_CONFIDENCE,
    RELATION_LABELS,
)

log = logging.getLogger(__name__)


CHAIN_RELATION_WEIGHTS = {
    "contradicts": 1.35,
    "has_risk": 1.25,
    "government_contract": 1.2,
    "about_bill": 1.2,
    "sponsored_bill": 1.15,
    "voted_for": 1.05,
    "voted_against": 1.05,
    "voted_abstained": 1.0,
    "voted_absent": 0.95,
    "same_inn": 1.1,
    "involved_in": 1.05,
    "investigated_by": 1.1,
    "works_at": 0.95,
    "head_of": 0.95,
    "party_member": 0.9,
    "member_of": 0.9,
    "member_of_committee": 0.95,
    "represents_region": 0.9,
    "foreign_agent": 1.2,
    "mentioned_together": 0.45,
    "co_voter": 0.55,
    "co_sponsor": 0.6,
}

CHAIN_SOURCE_QUALITY = {
    "bill_votes": 1.1,
    "bill_vote_sessions": 1.15,
    "bill_sponsors": 1.1,
    "contracts": 1.1,
    "official_positions": 1.05,
    "investigative_materials": 1.0,
    "risk_patterns": 1.0,
    "entity_relations": 1.0,
    "inn_match": 1.1,
    "co_mentions": 0.7,
    "bill_votes_co": 0.75,
    "bill_sponsors_co": 0.8,
}

CHAIN_NODE_BONUS = {
    NodeType.BILL: 0.45,
    NodeType.CONTRACT: 0.4,
    NodeType.VOTE_SESSION: 0.25,
    NodeType.RISK: 0.35,
    NodeType.CLAIM: 0.25,
    NodeType.INVESTIGATIVE: 0.3,
    NodeType.LAW: 0.2,
}

NON_EXPANDING_CHAIN_RELATIONS = {"mentioned_together", "co_voter", "co_sponsor"}


class InvestigationEngine:
    _VIRTUAL_NODE_OFFSETS = {
        "bill": 1_000_000_000,
        "vote_session": 2_000_000_000,
        "contract": 3_000_000_000,
    }

    def __init__(self, db_path: str, max_nodes: int = 500, max_edges: int = 2000):
        self.db_path = db_path
        self.max_nodes = max_nodes
        self.max_edges = max_edges
        self._conn: Optional[sqlite3.Connection] = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def _table_exists(self, table_name: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name=?",
            (table_name,),
        ).fetchone()
        return row is not None

    def _decode_virtual_node_id(self, node_id: int) -> Optional[Tuple[str, int]]:
        if node_id >= 0:
            return None
        raw = abs(node_id)
        for kind, offset in sorted(
            self._VIRTUAL_NODE_OFFSETS.items(),
            key=lambda item: item[1],
            reverse=True,
        ):
            if raw > offset:
                return kind, raw - offset
        return None

    def _is_expandable_node_id(self, node_id: int) -> bool:
        return node_id > 0 or self._decode_virtual_node_id(node_id) is not None

    def _is_bidirectional_relation(self, relation_type: str) -> bool:
        return relation_type in {
            "co_sponsor",
            "co_voter",
            "mentioned_together",
            "same_inn",
            "contradicts",
        }

    def _connection_context_key(self, conn: Dict) -> Tuple:
        meta = conn.get("metadata", {}) or {}
        parts = []
        for key in (
            "context_type",
            "context_id",
            "bill_id",
            "vote_session_id",
            "material_id",
            "risk_pattern_id",
            "content_item_id",
            "evidence_item_id",
            "inn",
        ):
            value = meta.get(key)
            if value not in (None, "", [], {}):
                parts.append((key, self._freeze_value(value)))
        return tuple(parts)

    def _freeze_value(self, value):
        if isinstance(value, dict):
            return tuple(sorted((k, self._freeze_value(v)) for k, v in value.items()))
        if isinstance(value, (list, tuple, set)):
            return tuple(self._freeze_value(v) for v in value)
        return value

    def _merge_connection(self, existing: Dict, incoming: Dict) -> Dict:
        for key in ("co_mention_count", "support_count"):
            existing[key] = max(existing.get(key, 0), incoming.get(key, 0))

        meta = existing.setdefault("metadata", {})
        other_meta = incoming.get("metadata", {}) or {}
        for key in ("same_vote_count", "distinct_content_count", "distinct_source_count"):
            meta[key] = max(meta.get(key, 0), other_meta.get(key, 0))

        for key in ("vote_session_ids", "sample_vote_session_ids", "evidence_item_ids"):
            values = []
            for source in (meta.get(key, []), other_meta.get(key, [])):
                if isinstance(source, list):
                    values.extend(source)
            if values:
                seen = []
                for value in values:
                    if value not in seen:
                        seen.append(value)
                meta[key] = seen[:20]

        if other_meta.get("evidence_item_id") and not meta.get("evidence_item_id"):
            meta["evidence_item_id"] = other_meta["evidence_item_id"]
        return existing

    def _parse_json(self, raw_text: Optional[str], default):
        if not raw_text:
            return default
        try:
            return json.loads(raw_text)
        except (json.JSONDecodeError, TypeError):
            return default

    def _coerce_int(self, value: Any) -> Optional[int]:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            value = value.strip()
            if value.isdigit():
                try:
                    return int(value)
                except ValueError:
                    return None
        return None

    def _normalize_inn(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def _entity_ids_from_json(self, raw_text: Optional[str]) -> List[int]:
        data = self._parse_json(raw_text, [])
        if not isinstance(data, list):
            return []
        entity_ids: List[int] = []
        for item in data:
            entity_id = self._coerce_int(item)
            if entity_id is not None and entity_id not in entity_ids:
                entity_ids.append(entity_id)
        return entity_ids

    def _extract_involved_entities(self, raw_text: Optional[str]) -> List[Dict[str, Any]]:
        inv_data = self._parse_json(raw_text, [])
        if not isinstance(inv_data, list):
            return []

        refs: List[Dict[str, Any]] = []
        for inv in inv_data:
            if isinstance(inv, dict):
                inv_id = inv.get("entity_id")
                inv_name = inv.get("name", "")
                inv_type = inv.get("type", "organization")
                inv_role = inv.get("role", "")
            elif isinstance(inv, int):
                inv_id = inv
                inv_name = ""
                inv_type = "organization"
                inv_role = ""
            else:
                continue
            inv_id = self._coerce_int(inv_id)
            if inv_id is not None:
                refs.append({
                    "entity_id": inv_id,
                    "name": inv_name,
                    "type": inv_type,
                    "role": inv_role,
                })
        return refs

    def _virtual_node_id(self, kind: str, identifier: Any) -> int:
        offset = self._VIRTUAL_NODE_OFFSETS.get(kind, 9_000_000_000)
        numeric_id = self._coerce_int(identifier)
        if numeric_id is None:
            numeric_id = abs(hash(f"{kind}:{identifier}")) % 900_000_000
        return -(offset + numeric_id)

    def _bill_label(self, bill_number: Optional[str], bill_title: Optional[str]) -> str:
        number = (bill_number or "").strip()
        title = (bill_title or "").strip()
        if number and title:
            return f"{number} - {title[:120]}"
        if number:
            return f"Законопроект {number}"
        if title:
            return title[:140]
        return "Законопроект"

    def _contract_label(self, contract_number: Optional[str], contract_title: Optional[str]) -> str:
        number = (contract_number or "").strip()
        title = (contract_title or "").strip()
        if number and title:
            return f"Контракт {number}: {title[:120]}"
        if number:
            return f"Контракт {number}"
        if title:
            return title[:140]
        return "Госконтракт"

    def _resolve_bill_node(
        self,
        bill_id: Any,
        bill_number: Optional[str],
        bill_title: Optional[str],
        bill_status: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Tuple[int, str, str, Dict[str, Any]]:
        bill_label = self._bill_label(bill_number, bill_title)
        bill_meta: Dict[str, Any] = {
            "bill_id": bill_id,
            "bill_number": bill_number,
            "bill_title": bill_title,
            "bill_status": bill_status,
        }
        if extra:
            bill_meta.update(extra)

        if self._table_exists("entities") and bill_number:
            bill_entity = self.conn.execute(
                "SELECT id, canonical_name, entity_type FROM entities WHERE entity_type='law' AND canonical_name LIKE ?",
                (f"%{bill_number}%",),
            ).fetchone()
        else:
            bill_entity = None

        if bill_entity:
            return (
                bill_entity["id"],
                bill_entity["canonical_name"],
                bill_entity["entity_type"],
                bill_meta,
            )

        bill_node_id = self._virtual_node_id("bill", bill_id or bill_number or bill_label)
        bill_meta.update(
            {
                "virtual_node_type": "bill",
                "virtual_node_label": bill_label,
                "virtual_node_extra": {
                    "bill_id": bill_id,
                    "bill_number": bill_number,
                    "bill_title": bill_title,
                    "bill_status": bill_status,
                },
            }
        )
        return bill_node_id, bill_label, "bill", bill_meta

    def _vote_session_label(self, row: sqlite3.Row) -> str:
        bill_part = self._bill_label(row["number"], row["title"])
        vote_date = (row["vote_date"] or "").strip()
        if vote_date:
            return f"Голосование {vote_date} - {bill_part}"
        return f"Голосование #{row['vote_session_id']} - {bill_part}"

    def _node_from_connection(self, other_id: int, conn: Dict, hop: int) -> Optional[InvestigationNode]:
        entity = self._load_entity(other_id)
        if entity:
            return InvestigationNode(
                entity_id=other_id,
                canonical_name=entity["canonical_name"],
                entity_type=entity["entity_type"],
                node_type=NodeType.from_entity_type(entity["entity_type"]),
                hop=hop + 1,
                extra=self._load_entity_extra(other_id, entity["entity_type"]),
            )

        meta = conn.get("metadata", {}) or {}
        virtual_type = meta.get("virtual_node_type")
        if not virtual_type:
            return None

        try:
            node_type = NodeType(virtual_type)
        except ValueError:
            node_type = NodeType.CONTENT

        extra = dict(meta.get("virtual_node_extra", {}) or {})
        extra.setdefault("source", conn.get("source", ""))
        extra.setdefault("synthetic", True)
        return InvestigationNode(
            entity_id=other_id,
            canonical_name=meta.get("virtual_node_label") or conn.get("other_name") or f"ID:{other_id}",
            entity_type=virtual_type,
            node_type=node_type,
            hop=hop + 1,
            extra=extra,
        )

    def investigate(
        self,
        seed_entity_id: int,
        max_hops: int = 3,
        min_confidence: Confidence = Confidence.LIKELY,
        relation_types: Optional[List[str]] = None,
    ) -> InvestigationResult:
        result = InvestigationResult(seed_entity_id=seed_entity_id)
        seed = self._load_entity(seed_entity_id)
        if not seed:
            log.warning("Entity %d not found", seed_entity_id)
            return result

        result.seed_name = seed["canonical_name"]
        result.seed_type = seed["entity_type"]
        result.nodes[seed_entity_id] = InvestigationNode(
            entity_id=seed_entity_id,
            canonical_name=seed["canonical_name"],
            entity_type=seed["entity_type"],
            node_type=NodeType.from_entity_type(seed["entity_type"]),
            hop=0,
            extra=self._load_entity_extra(seed_entity_id, seed["entity_type"]),
        )

        visited: Set[int] = {seed_entity_id}
        queue: Deque[Tuple[int, int]] = deque([(seed_entity_id, 0)])

        while queue:
            current_id, hop = queue.popleft()
            if hop >= max_hops:
                continue
            if len(result.nodes) >= self.max_nodes:
                break
            if len(result.edges) >= self.max_edges:
                break

            if current_id in result.nodes:
                result.nodes[current_id].is_expanded = True

            connections = self._find_all_connections(current_id, relation_types)
            deduped = self._deduplicate_connections(connections)

            for conn in deduped:
                other_id = conn["other_entity_id"]
                if other_id == current_id:
                    continue

                confidence = self._verify_connection(conn)
                conn["confidence"] = confidence

                if confidence < min_confidence:
                    continue

                edge = self._build_edge(current_id, conn, hop)
                if edge.key in result.edge_keys:
                    continue

                result.edges.append(edge)

                if other_id not in visited:
                    visited.add(other_id)
                    other_node = self._node_from_connection(other_id, conn, hop)
                    if other_node:
                        result.nodes[other_id] = other_node
                        if self._is_expandable_node_id(other_id):
                            queue.append((other_id, hop + 1))

            result.max_hop = max(result.max_hop, hop + 1)

        result.contradictions = self._find_contradictions(result)
        result.risk_patterns = self._find_risk_patterns(result)
        result.evidence_chains = self._build_evidence_chains(result)
        result.leads = self._detect_leads(result)
        result.stats = self._compute_stats(result)

        log.info(
            "Investigation seed=%d: %d nodes, %d edges, %d chains, %d leads",
            seed_entity_id, len(result.nodes), len(result.edges),
            len(result.evidence_chains), len(result.leads),
        )
        return result

    def expand(
        self,
        result: InvestigationResult,
        entity_id: int,
        hops: int = 1,
        min_confidence: Confidence = Confidence.LIKELY,
    ) -> InvestigationResult:
        if entity_id not in result.nodes:
            entity = self._load_entity(entity_id)
            if not entity:
                return result
            result.nodes[entity_id] = InvestigationNode(
                entity_id=entity_id,
                canonical_name=entity["canonical_name"],
                entity_type=entity["entity_type"],
                node_type=NodeType.from_entity_type(entity["entity_type"]),
                extra=self._load_entity_extra(entity_id, entity["entity_type"]),
            )

        current_hop = result.nodes[entity_id].hop
        visited = set(result.nodes.keys())
        queue: Deque[Tuple[int, int]] = deque([(entity_id, current_hop)])

        while queue:
            cur_id, hop = queue.popleft()
            if hop >= current_hop + hops:
                continue
            if len(result.nodes) >= self.max_nodes or len(result.edges) >= self.max_edges:
                break

            if cur_id in result.nodes:
                result.nodes[cur_id].is_expanded = True

            connections = self._find_all_connections(cur_id)
            deduped = self._deduplicate_connections(connections)

            for conn in deduped:
                other_id = conn["other_entity_id"]
                if other_id == cur_id:
                    continue

                confidence = self._verify_connection(conn)
                if confidence < min_confidence:
                    continue

                edge = self._build_edge(cur_id, conn, hop)
                if edge.key in result.edge_keys:
                    continue
                result.edges.append(edge)

                if other_id not in visited:
                    visited.add(other_id)
                    other_node = self._node_from_connection(other_id, conn, hop)
                    if other_node:
                        new_hop = hop + 1
                        result.nodes[other_id] = other_node
                        if self._is_expandable_node_id(other_id):
                            queue.append((other_id, new_hop))

            result.max_hop = max(result.max_hop, hop + 1)

        result.evidence_chains = self._build_evidence_chains(result)
        result.leads = self._detect_leads(result)
        result.stats = self._compute_stats(result)
        return result

    def _load_entity(self, entity_id: int) -> Optional[Dict]:
        row = self.conn.execute(
            "SELECT id, entity_type, canonical_name, inn, ogrn, description FROM entities WHERE id=?",
            (entity_id,),
        ).fetchone()
        if not row:
            return None
        return dict(row)

    def _load_entity_extra(self, entity_id: int, entity_type: str) -> Dict:
        extra: Dict[str, Any] = {}

        if entity_type == "person":
            if self._table_exists("deputy_profiles"):
                dp = self.conn.execute(
                    "SELECT id, full_name, position, faction, region, committee, duma_id, "
                    "date_elected, income_latest, is_active FROM deputy_profiles WHERE entity_id=?",
                    (entity_id,),
                ).fetchone()
            else:
                dp = None
            if dp:
                extra["deputy_profile"] = dict(dp)

            if self._table_exists("official_positions"):
                positions = self.conn.execute(
                    "SELECT position_title, organization, region, faction, started_at, ended_at, is_active "
                    "FROM official_positions WHERE entity_id=? ORDER BY is_active DESC, started_at DESC LIMIT 5",
                    (entity_id,),
                ).fetchall()
            else:
                positions = []
            if positions:
                extra["positions"] = [dict(p) for p in positions]

            if self._table_exists("party_memberships"):
                parties = self.conn.execute(
                    "SELECT party_name, role, started_at, ended_at, is_current "
                    "FROM party_memberships WHERE entity_id=? ORDER BY is_current DESC LIMIT 5",
                    (entity_id,),
                ).fetchall()
            else:
                parties = []
            if parties:
                extra["parties"] = [dict(p) for p in parties]

            if dp and self._table_exists("accountability_index"):
                acc = self.conn.execute(
                    "SELECT calculated_score, public_speeches_count, verifiable_claims_count, "
                    "confirmed_contradictions, flagged_statements_count, votes_tracked_count, linked_cases_count "
                    "FROM accountability_index WHERE deputy_id=? ORDER BY id DESC LIMIT 1",
                    (dp["id"],),
                ).fetchone()
                if acc:
                    extra["accountability"] = dict(acc)

        elif entity_type == "organization":
            row = self.conn.execute(
                "SELECT inn, ogrn, description FROM entities WHERE id=?", (entity_id,)
            ).fetchone()
            if row:
                extra["inn"] = row["inn"]
                extra["ogrn"] = row["ogrn"]
                extra["description"] = row["description"]

            if self._table_exists("investigative_materials"):
                inv = self.conn.execute(
                    "SELECT material_type, title, publication_date, verification_status, involved_entities "
                    "FROM investigative_materials "
                    "WHERE involved_entities IS NOT NULL AND involved_entities != '' "
                    "ORDER BY publication_date DESC LIMIT 500"
                ).fetchall()
                filtered_inv = []
                for item in inv:
                    refs = self._extract_involved_entities(item["involved_entities"])
                    if any(ref["entity_id"] == entity_id for ref in refs):
                        filtered_inv.append({
                            "material_type": item["material_type"],
                            "title": item["title"],
                            "publication_date": item["publication_date"],
                            "verification_status": item["verification_status"],
                        })
                    if len(filtered_inv) >= 10:
                        break
                if filtered_inv:
                    extra["investigative"] = filtered_inv

        return extra

    def _find_all_connections(self, entity_id: int, relation_types: Optional[List[str]] = None) -> List[Dict]:
        virtual = self._decode_virtual_node_id(entity_id)
        if virtual:
            kind, source_id = virtual
            if kind == "vote_session":
                return self._find_vote_session_node_connections(entity_id, source_id)
            if kind == "bill":
                return self._find_bill_node_connections(entity_id, source_id)
            if kind == "contract":
                return self._find_contract_node_connections(entity_id, source_id)
            return []

        connections: List[Dict] = []
        connections.extend(self._find_entity_relations(entity_id, relation_types))
        connections.extend(self._find_bill_connections(entity_id))
        connections.extend(self._find_position_connections(entity_id))
        connections.extend(self._find_content_connections(entity_id))
        connections.extend(self._find_investigative_connections(entity_id))
        connections.extend(self._find_inn_connections(entity_id))
        connections.extend(self._find_vote_connections(entity_id))
        return connections

    def _find_entity_relations(self, entity_id: int, relation_types: Optional[List[str]] = None) -> List[Dict]:
        if not self._table_exists("entity_relations") or not self._table_exists("entities"):
            return []
        connections = []
        query = (
            "SELECT er.from_entity_id, er.to_entity_id, er.relation_type, er.strength, "
            "er.evidence_item_id, er.detected_by, "
            "e_from.canonical_name as from_name, e_from.entity_type as from_type, "
            "e_to.canonical_name as to_name, e_to.entity_type as to_type "
            "FROM entity_relations er "
            "JOIN entities e_from ON er.from_entity_id = e_from.id "
            "JOIN entities e_to ON er.to_entity_id = e_to.id "
            "WHERE (er.from_entity_id=? OR er.to_entity_id=?) "
            "AND er.relation_type NOT IN ('associated_with_location', 'located_in', 'mentioned_together')"
        )
        params: list = [entity_id, entity_id]
        if relation_types:
            placeholders = ",".join("?" * len(relation_types))
            query += f" AND er.relation_type IN ({placeholders})"
            params.extend(relation_types)

        rows = self.conn.execute(query, params).fetchall()
        for row in rows:
            if row["from_entity_id"] == entity_id:
                other_id = row["to_entity_id"]
                other_name = row["to_name"]
                other_type = row["to_type"]
            else:
                other_id = row["from_entity_id"]
                other_name = row["from_name"]
                other_type = row["from_type"]

            connections.append({
                "other_entity_id": other_id,
                "other_name": other_name,
                "other_type": other_type,
                "relation_type": row["relation_type"],
                "bidirectional": self._is_bidirectional_relation(row["relation_type"]),
                "source": "entity_relations",
                "strength": row["strength"],
                "original_from_id": row["from_entity_id"],
                "original_to_id": row["to_entity_id"],
                "metadata": {
                    "strength": row["strength"],
                    "detected_by": row["detected_by"],
                    "evidence_item_id": row["evidence_item_id"],
                    "context_type": "content_item" if row["evidence_item_id"] else "entity_relation",
                    "context_id": row["evidence_item_id"] or f"{row['from_entity_id']}:{row['to_entity_id']}:{row['relation_type']}",
                },
            })

        if relation_types is None or "mentioned_together" in relation_types:
            rows = self.conn.execute(
                "SELECT er.from_entity_id, er.to_entity_id, er.relation_type, er.strength, "
                "er.evidence_item_id, er.detected_by, "
                "e_from.canonical_name as from_name, e_from.entity_type as from_type, "
                "e_to.canonical_name as to_name, e_to.entity_type as to_type "
                "FROM entity_relations er "
                "JOIN entities e_from ON er.from_entity_id = e_from.id "
                "JOIN entities e_to ON er.to_entity_id = e_to.id "
                "WHERE (er.from_entity_id=? OR er.to_entity_id=?) "
                "AND er.relation_type = 'mentioned_together' "
                "AND e_to.entity_type NOT IN ('location') "
                "AND e_from.entity_type NOT IN ('location') "
                "LIMIT 30",
                (entity_id, entity_id),
            ).fetchall()
            for row in rows:
                if row["from_entity_id"] == entity_id:
                    other_id = row["to_entity_id"]
                    other_name = row["to_name"]
                    other_type = row["to_type"]
                else:
                    other_id = row["from_entity_id"]
                    other_name = row["from_name"]
                    other_type = row["from_type"]
                connections.append({
                    "other_entity_id": other_id,
                    "other_name": other_name,
                    "other_type": other_type,
                    "relation_type": row["relation_type"],
                    "bidirectional": True,
                    "source": "entity_relations",
                    "strength": row["strength"],
                    "original_from_id": row["from_entity_id"],
                    "original_to_id": row["to_entity_id"],
                    "metadata": {
                        "strength": row["strength"],
                        "detected_by": row["detected_by"],
                        "evidence_item_id": row["evidence_item_id"],
                        "context_type": "content_item" if row["evidence_item_id"] else "entity_relation",
                        "context_id": row["evidence_item_id"] or f"{row['from_entity_id']}:{row['to_entity_id']}:mentioned_together",
                    },
                })

        return connections

    def _find_bill_connections(self, entity_id: int) -> List[Dict]:
        if not self._table_exists("bill_sponsors") or not self._table_exists("bills"):
            return []
        connections = []
        rows = self.conn.execute(
            "SELECT bs.bill_id, bs.sponsor_name, bs.sponsor_role, bs.faction, "
            "b.number, b.title, b.status "
            "FROM bill_sponsors bs JOIN bills b ON bs.bill_id = b.id "
            "WHERE bs.entity_id=?",
            (entity_id,),
        ).fetchall()
        for row in rows:
            bill_node_id, bill_name, bill_type, bill_meta = self._resolve_bill_node(
                row["bill_id"],
                row["number"],
                row["title"],
                row["status"],
                {"sponsor_role": row["sponsor_role"]},
            )

            connections.append({
                "other_entity_id": bill_node_id,
                "other_name": bill_name,
                "other_type": bill_type,
                "relation_type": "sponsored_bill",
                "source": "bill_sponsors",
                "bidirectional": False,
                "metadata": {
                    "bill_id": row["bill_id"],
                    "bill_number": row["number"],
                    "bill_status": row["status"],
                    "sponsor_role": row["sponsor_role"],
                    "context_type": "bill",
                    "context_id": row["bill_id"],
                    **bill_meta,
                },
            })

            co_sponsors = self.conn.execute(
                "SELECT bs2.entity_id, bs2.sponsor_name, e.entity_type "
                "FROM bill_sponsors bs2 "
                "LEFT JOIN entities e ON bs2.entity_id = e.id "
                "WHERE bs2.bill_id=? AND bs2.entity_id!=? AND bs2.entity_id IS NOT NULL",
                (row["bill_id"], entity_id),
            ).fetchall()
            for cs in co_sponsors:
                if cs["entity_id"]:
                    connections.append({
                        "other_entity_id": cs["entity_id"],
                        "other_name": cs["sponsor_name"],
                        "other_type": cs["entity_type"] or "person",
                        "relation_type": "co_sponsor",
                        "source": "bill_sponsors_co",
                        "bidirectional": True,
                        "metadata": {
                            "bill_id": row["bill_id"],
                            "bill_number": row["number"],
                            "bill_title": row["title"][:100] if row["title"] else "",
                            "context_type": "bill",
                            "context_id": row["bill_id"],
                        },
                    })
        return connections

    def _find_vote_connections(self, entity_id: int) -> List[Dict]:
        if not self._table_exists("bill_votes") or not self._table_exists("bill_vote_sessions"):
            return []
        connections = []
        rows = self.conn.execute(
            "SELECT bv.vote_session_id, bv.vote_result, bv.faction, bvs.bill_id, bvs.vote_date, bvs.vote_stage, "
            "bvs.result as session_result, b.number, b.title "
            "FROM bill_votes bv "
            "JOIN bill_vote_sessions bvs ON bv.vote_session_id = bvs.id "
            "LEFT JOIN bills b ON bvs.bill_id = b.id "
            "WHERE bv.entity_id=? "
            "ORDER BY bvs.vote_date DESC LIMIT 200",
            (entity_id,),
        ).fetchall()
        seen_vote_edges: Set[Tuple] = set()
        for row in rows:
            vote_type = {
                "за": "voted_for", "против": "voted_against",
                "воздержался": "voted_abstained", "не голосовал": "voted_absent",
                "отсутствовал": "voted_absent",
            }.get(row["vote_result"], "voted_for")
            vote_session_id = self._virtual_node_id("vote_session", row["vote_session_id"])
            vote_label = self._vote_session_label(row)
            edge_key = (vote_session_id, vote_type, row["vote_session_id"])
            if edge_key in seen_vote_edges:
                continue
            seen_vote_edges.add(edge_key)
            connections.append({
                "other_entity_id": vote_session_id,
                "other_name": vote_label,
                "other_type": "vote_session",
                "relation_type": vote_type,
                "source": "bill_votes",
                "bidirectional": False,
                "metadata": {
                    "bill_id": row["bill_id"],
                    "bill_number": row["number"],
                    "bill_title": row["title"],
                    "vote_session_id": row["vote_session_id"],
                    "vote_result": row["vote_result"],
                    "vote_date": row["vote_date"],
                    "vote_stage": row["vote_stage"],
                    "session_result": row["session_result"],
                    "context_type": "vote_session",
                    "context_id": row["vote_session_id"],
                    "virtual_node_type": "vote_session",
                    "virtual_node_label": vote_label,
                    "virtual_node_extra": {
                        "vote_session_id": row["vote_session_id"],
                        "bill_id": row["bill_id"],
                        "bill_number": row["number"],
                        "bill_title": row["title"],
                        "vote_result": row["vote_result"],
                        "vote_date": row["vote_date"],
                        "vote_stage": row["vote_stage"],
                        "session_result": row["session_result"],
                    },
                },
            })

        co_voters = self.conn.execute(
            """
            SELECT bv2.entity_id, bv2.deputy_name, e.entity_type,
                   COUNT(DISTINCT bv.vote_session_id) AS same_vote_count,
                   MIN(bvs.vote_date) AS first_vote_date,
                   MAX(bvs.vote_date) AS last_vote_date,
                   GROUP_CONCAT(DISTINCT bv.vote_session_id) AS vote_session_ids
            FROM bill_votes bv
            JOIN bill_votes bv2
              ON bv2.vote_session_id = bv.vote_session_id
             AND bv2.entity_id IS NOT NULL
             AND bv2.entity_id != bv.entity_id
             AND bv2.vote_result = bv.vote_result
            JOIN bill_vote_sessions bvs ON bvs.id = bv.vote_session_id
            LEFT JOIN entities e ON bv2.entity_id = e.id
            WHERE bv.entity_id = ?
            GROUP BY bv2.entity_id, bv2.deputy_name, e.entity_type
            HAVING COUNT(DISTINCT bv.vote_session_id) >= 3
            ORDER BY same_vote_count DESC
            LIMIT 50
            """,
            (entity_id,),
        ).fetchall()
        for row in co_voters:
            vote_session_ids = []
            if row["vote_session_ids"]:
                vote_session_ids = [int(v) for v in str(row["vote_session_ids"]).split(",") if v][:20]
            connections.append({
                "other_entity_id": row["entity_id"],
                "other_name": row["deputy_name"],
                "other_type": row["entity_type"] or "person",
                "relation_type": "co_voter",
                "source": "bill_votes_co",
                "bidirectional": True,
                "support_count": row["same_vote_count"],
                "metadata": {
                    "same_vote_count": row["same_vote_count"],
                    "first_vote_date": row["first_vote_date"],
                    "last_vote_date": row["last_vote_date"],
                    "vote_session_ids": vote_session_ids,
                    "context_type": "vote_pattern",
                    "context_id": f"{entity_id}:{row['entity_id']}",
                },
            })

        return connections

    def _find_position_connections(self, entity_id: int) -> List[Dict]:
        if not self._table_exists("official_positions"):
            return []
        connections = []
        rows = self.conn.execute(
            "SELECT position_title, organization, region, faction FROM official_positions WHERE entity_id=?",
            (entity_id,),
        ).fetchall()
        for row in rows:
            org_name = row["organization"]
            if org_name:
                org = self.conn.execute(
                    "SELECT id, entity_type FROM entities WHERE canonical_name=? LIMIT 1",
                    (org_name,),
                ).fetchone()
                if org:
                    connections.append({
                        "other_entity_id": org["id"],
                        "other_name": org_name,
                        "other_type": org["entity_type"],
                        "relation_type": "works_at",
                        "source": "official_positions",
                        "metadata": {"position": row["position_title"]},
                    })

        return connections

    def _find_content_connections(self, entity_id: int) -> List[Dict]:
        if not self._table_exists("entity_mentions") or not self._table_exists("content_items") or not self._table_exists("entities"):
            return []
        connections = []
        co_mentions = self.conn.execute(
            "SELECT em2.entity_id, e.canonical_name, e.entity_type, "
            "COUNT(DISTINCT em1.content_item_id) as cnt, "
            "COUNT(DISTINCT COALESCE(ci.source_id, -1)) as source_cnt "
            "FROM entity_mentions em1 "
            "JOIN entity_mentions em2 ON em1.content_item_id = em2.content_item_id "
            "JOIN content_items ci ON ci.id = em1.content_item_id "
            "JOIN entities e ON em2.entity_id = e.id "
            "WHERE em1.entity_id=? AND em2.entity_id!=? "
            "GROUP BY em2.entity_id "
            "HAVING COUNT(DISTINCT em1.content_item_id) >= 2 "
            "ORDER BY source_cnt DESC, cnt DESC LIMIT 50",
            (entity_id, entity_id),
        ).fetchall()
        for row in co_mentions:
            connections.append({
                "other_entity_id": row["entity_id"],
                "other_name": row["canonical_name"],
                "other_type": row["entity_type"],
                "relation_type": "mentioned_together",
                "source": "co_mentions",
                "co_mention_count": row["cnt"],
                "metadata": {
                    "distinct_content_count": row["cnt"],
                    "distinct_source_count": row["source_cnt"],
                    "context_type": "co_mentions",
                    "context_id": f"{entity_id}:{row['entity_id']}",
                },
            })
        return connections

    def _find_investigative_connections(self, entity_id: int) -> List[Dict]:
        connections = []
        entity = self._load_entity(entity_id)
        if not entity:
            return connections

        if self._table_exists("investigative_materials"):
            rows = self.conn.execute(
                "SELECT id, material_type, title, publication_date, source_org, verification_status, "
                "involved_entities FROM investigative_materials "
                "WHERE involved_entities IS NOT NULL AND involved_entities != '' "
                "ORDER BY publication_date DESC LIMIT 1000"
            ).fetchall()
            for row in rows:
                refs = self._extract_involved_entities(row["involved_entities"])
                if not any(ref["entity_id"] == entity_id for ref in refs):
                    continue
                for ref in refs:
                    inv_id = ref["entity_id"]
                    if inv_id != entity_id:
                        other_name = ref["name"]
                        other_type = ref["type"]
                        if not other_name or not other_type:
                            other = self._load_entity(inv_id)
                            if other:
                                other_name = other["canonical_name"]
                                other_type = other["entity_type"]
                        connections.append({
                            "other_entity_id": inv_id,
                            "other_name": other_name or f"ID:{inv_id}",
                            "other_type": other_type or "organization",
                            "relation_type": "involved_in",
                            "source": "investigative_materials",
                            "bidirectional": True,
                            "metadata": {
                                "material_id": row["id"],
                                "material_type": row["material_type"],
                                "material_title": row["title"],
                                "source_org": row["source_org"],
                                "publication_date": row["publication_date"],
                                "role": ref["role"],
                                "context_type": "material",
                                "context_id": row["id"],
                            },
                        })

        if self._table_exists("risk_patterns"):
            risk_rows = self.conn.execute(
                "SELECT id, pattern_type, description, risk_level, entity_ids, evidence_ids "
                "FROM risk_patterns ORDER BY detected_at DESC LIMIT 500"
            ).fetchall()
            for row in risk_rows:
                eids = self._entity_ids_from_json(row["entity_ids"])
                if entity_id not in eids:
                    continue
                for eid in eids:
                    if eid != entity_id:
                        other = self._load_entity(eid)
                        if other:
                            connections.append({
                                "other_entity_id": eid,
                                "other_name": other["canonical_name"],
                                "other_type": other["entity_type"],
                                "relation_type": "has_risk",
                                "source": "risk_patterns",
                                "bidirectional": True,
                                "metadata": {
                                    "risk_pattern_id": row["id"],
                                    "risk_type": row["pattern_type"],
                                    "risk_level": row["risk_level"],
                                    "description": row["description"],
                                    "context_type": "risk_pattern",
                                    "context_id": row["id"],
                                },
                            })
        return connections

    def _find_inn_connections(self, entity_id: int) -> List[Dict]:
        connections = []
        entity = self._load_entity(entity_id)
        if not entity:
            return connections
        entity_inn = self._normalize_inn(entity.get("inn"))

        if entity_inn:
            rows = self.conn.execute(
                "SELECT id, canonical_name, entity_type FROM entities WHERE inn=? AND id!=?",
                (entity_inn, entity_id),
            ).fetchall()
            for row in rows:
                connections.append({
                    "other_entity_id": row["id"],
                    "other_name": row["canonical_name"],
                    "other_type": row["entity_type"],
                    "relation_type": "same_inn",
                    "source": "inn_match",
                    "metadata": {"inn": entity_inn},
                })

        if self._table_exists("contracts"):
            contract_rows: List[sqlite3.Row] = []
            if self._table_exists("contract_parties"):
                contract_rows.extend(
                    self.conn.execute(
                        """
                        SELECT DISTINCT c.id, c.contract_number, c.title, c.publication_date, c.source_org,
                               c.customer_inn, c.supplier_inn, c.raw_data,
                               cp.party_role AS self_role
                        FROM contracts c
                        JOIN contract_parties cp ON cp.contract_id = c.id
                        WHERE cp.entity_id = ?
                        ORDER BY c.publication_date DESC, c.id DESC
                        LIMIT 500
                        """,
                        (entity_id,),
                    ).fetchall()
                )
                if entity_inn:
                    contract_rows.extend(
                        self.conn.execute(
                            """
                            SELECT DISTINCT c.id, c.contract_number, c.title, c.publication_date, c.source_org,
                                   c.customer_inn, c.supplier_inn, c.raw_data,
                                   cp.party_role AS self_role
                            FROM contracts c
                            LEFT JOIN contract_parties cp
                              ON cp.contract_id = c.id
                             AND cp.inn = ?
                            WHERE cp.id IS NOT NULL OR c.customer_inn = ? OR c.supplier_inn = ?
                            ORDER BY c.publication_date DESC, c.id DESC
                            LIMIT 500
                            """,
                            (entity_inn, entity_inn, entity_inn),
                        ).fetchall()
                    )
            elif entity_inn:
                contract_rows = self.conn.execute(
                    """
                    SELECT DISTINCT c.id, c.contract_number, c.title, c.publication_date, c.source_org,
                           c.customer_inn, c.supplier_inn, c.raw_data,
                           '' AS self_role
                    FROM contracts c
                    WHERE c.customer_inn = ? OR c.supplier_inn = ?
                    ORDER BY c.publication_date DESC, c.id DESC
                    LIMIT 500
                    """,
                    (entity_inn, entity_inn),
                ).fetchall()
            else:
                contract_rows = []
        elif self._table_exists("investigative_materials"):
            contract_rows = self.conn.execute(
                """
                SELECT id, '' AS contract_number, title, publication_date, source_org,
                       '' AS customer_inn, '' AS supplier_inn, raw_data, '' AS self_role
                FROM investigative_materials
                WHERE material_type='government_contract'
                  AND raw_data IS NOT NULL AND raw_data != ''
                ORDER BY publication_date DESC, id DESC
                LIMIT 500
                """
            ).fetchall()
        else:
            contract_rows = []

        seen_contracts: Set[int] = set()
        for row in contract_rows:
            contract_id = row["id"]
            if contract_id in seen_contracts:
                continue

            raw = self._parse_json(row["raw_data"], {})
            if not isinstance(raw, dict):
                raw = {}
            customer_inn = self._normalize_inn(row["customer_inn"] or raw.get("customer_inn"))
            supplier_inn = self._normalize_inn(row["supplier_inn"] or raw.get("supplier_inn"))

            if row["self_role"]:
                contract_role = row["self_role"]
            elif entity_inn and customer_inn == entity_inn:
                contract_role = "customer"
            elif entity_inn and supplier_inn == entity_inn:
                contract_role = "supplier"
            else:
                party_row = None
                if self._table_exists("contract_parties"):
                    party_row = self.conn.execute(
                        """
                        SELECT party_role
                        FROM contract_parties
                        WHERE contract_id=? AND entity_id=?
                        ORDER BY id
                        LIMIT 1
                        """,
                        (contract_id, entity_id),
                    ).fetchone()
                contract_role = party_row["party_role"] if party_row else ""

            if not contract_role:
                continue

            seen_contracts.add(contract_id)
            contract_number = row["contract_number"] or raw.get("contract_number", "")
            contract_label = self._contract_label(contract_number, row["title"])
            contract_node_id = self._virtual_node_id("contract", contract_id)
            connections.append({
                "other_entity_id": contract_node_id,
                "other_name": contract_label,
                "other_type": "contract",
                "relation_type": "government_contract",
                "source": "contracts",
                "bidirectional": False,
                "metadata": {
                    "contract_id": contract_id,
                    "contract_title": row["title"],
                    "contract_number": contract_number,
                    "publication_date": row["publication_date"],
                    "source_org": row["source_org"],
                    "role": contract_role,
                    "context_type": "contract",
                    "context_id": contract_id,
                    "virtual_node_type": "contract",
                    "virtual_node_label": contract_label,
                    "virtual_node_extra": {
                        "contract_id": contract_id,
                        "contract_title": row["title"],
                        "contract_number": contract_number,
                        "publication_date": row["publication_date"],
                        "source_org": row["source_org"],
                    },
                },
            })
        return connections

    def _find_vote_session_node_connections(self, node_id: int, vote_session_id: int) -> List[Dict]:
        if not self._table_exists("bill_vote_sessions"):
            return []
        row = self.conn.execute(
            """
            SELECT bvs.id AS vote_session_id, bvs.bill_id, bvs.vote_date, bvs.vote_stage, bvs.result,
                   b.number, b.title, b.status
            FROM bill_vote_sessions bvs
            LEFT JOIN bills b ON b.id = bvs.bill_id
            WHERE bvs.id=?
            """,
            (vote_session_id,),
        ).fetchone()
        if not row or row["bill_id"] is None:
            return []

        bill_node_id, bill_name, bill_type, bill_meta = self._resolve_bill_node(
            row["bill_id"],
            row["number"],
            row["title"],
            row["status"],
        )
        return [{
            "other_entity_id": bill_node_id,
            "other_name": bill_name,
            "other_type": bill_type,
            "relation_type": "about_bill",
            "source": "bill_vote_sessions",
            "bidirectional": False,
            "metadata": {
                "vote_session_id": vote_session_id,
                "vote_date": row["vote_date"],
                "vote_stage": row["vote_stage"],
                "session_result": row["result"],
                "context_type": "vote_session",
                "context_id": vote_session_id,
                **bill_meta,
            },
        }]

    def _find_bill_node_connections(self, node_id: int, bill_id: int) -> List[Dict]:
        connections: List[Dict] = []
        if self._table_exists("bill_sponsors"):
            sponsors = self.conn.execute(
                """
                SELECT bs.entity_id, bs.sponsor_name, e.entity_type, b.number, b.title, b.status
                FROM bill_sponsors bs
                LEFT JOIN entities e ON e.id = bs.entity_id
                LEFT JOIN bills b ON b.id = bs.bill_id
                WHERE bs.bill_id=? AND bs.entity_id IS NOT NULL
                ORDER BY bs.id
                """,
                (bill_id,),
            ).fetchall()
            for sponsor in sponsors:
                connections.append({
                    "other_entity_id": sponsor["entity_id"],
                    "other_name": sponsor["sponsor_name"],
                    "other_type": sponsor["entity_type"] or "person",
                    "relation_type": "sponsored_bill",
                    "source": "bill_sponsors",
                    "bidirectional": False,
                    "original_from_id": sponsor["entity_id"],
                    "original_to_id": node_id,
                    "metadata": {
                        "bill_id": bill_id,
                        "bill_number": sponsor["number"],
                        "bill_title": sponsor["title"],
                        "bill_status": sponsor["status"],
                        "context_type": "bill",
                        "context_id": bill_id,
                    },
                })

        if self._table_exists("bill_vote_sessions"):
            vote_rows = self.conn.execute(
                """
                SELECT id, vote_date, vote_stage, result, bill_id
                FROM bill_vote_sessions
                WHERE bill_id=?
                ORDER BY vote_date DESC, id DESC
                LIMIT 100
                """,
                (bill_id,),
            ).fetchall()
            for vote in vote_rows:
                vote_label = self._vote_session_label(
                    {
                        "vote_session_id": vote["id"],
                        "vote_date": vote["vote_date"],
                        "number": "",
                        "title": "",
                    }
                )
                vote_node_id = self._virtual_node_id("vote_session", vote["id"])
                connections.append({
                    "other_entity_id": vote_node_id,
                    "other_name": vote_label,
                    "other_type": "vote_session",
                    "relation_type": "about_bill",
                    "source": "bill_vote_sessions",
                    "bidirectional": False,
                    "original_from_id": vote_node_id,
                    "original_to_id": node_id,
                    "metadata": {
                        "vote_session_id": vote["id"],
                        "vote_date": vote["vote_date"],
                        "vote_stage": vote["vote_stage"],
                        "session_result": vote["result"],
                        "bill_id": bill_id,
                        "context_type": "bill",
                        "context_id": bill_id,
                        "virtual_node_type": "vote_session",
                        "virtual_node_label": vote_label,
                        "virtual_node_extra": {
                            "vote_session_id": vote["id"],
                            "vote_date": vote["vote_date"],
                            "vote_stage": vote["vote_stage"],
                            "session_result": vote["result"],
                            "bill_id": bill_id,
                        },
                    },
                })
        return connections

    def _find_contract_node_connections(self, node_id: int, contract_id: int) -> List[Dict]:
        connections: List[Dict] = []
        party_rows = []
        contract_row = None

        if self._table_exists("contracts"):
            contract_row = self.conn.execute("SELECT * FROM contracts WHERE id=?", (contract_id,)).fetchone()
            if contract_row and self._table_exists("contract_parties"):
                party_rows = self.conn.execute(
                    """
                    SELECT cp.entity_id, cp.party_name, cp.party_role, cp.inn, e.canonical_name, e.entity_type
                    FROM contract_parties cp
                    LEFT JOIN entities e ON e.id = cp.entity_id
                    WHERE cp.contract_id=?
                    ORDER BY cp.id
                    """,
                    (contract_id,),
                ).fetchall()

        if contract_row is None and self._table_exists("investigative_materials"):
            contract_row = self.conn.execute(
                """
                SELECT id, title, summary, publication_date, source_org, raw_data, involved_entities
                FROM investigative_materials
                WHERE id=? AND material_type='government_contract'
                """,
                (contract_id,),
            ).fetchone()
            if contract_row:
                raw = self._parse_json(contract_row["raw_data"], {})
                refs = self._extract_involved_entities(contract_row["involved_entities"])
                for ref in refs:
                    entity = self._load_entity(ref["entity_id"])
                    party_rows.append(
                        {
                            "entity_id": ref["entity_id"],
                            "party_name": ref.get("name") or (entity["canonical_name"] if entity else ""),
                            "party_role": ref.get("role") or "party",
                            "inn": "",
                            "canonical_name": entity["canonical_name"] if entity else "",
                            "entity_type": entity["entity_type"] if entity else "organization",
                        }
                    )
                if isinstance(raw, dict):
                    for party_role, inn_key in (("customer", "customer_inn"), ("supplier", "supplier_inn")):
                        party_inn = self._normalize_inn(raw.get(inn_key))
                        if not party_inn:
                            continue
                        entity = self.conn.execute(
                            "SELECT id, canonical_name, entity_type FROM entities WHERE inn=? LIMIT 1",
                            (party_inn,),
                        ).fetchone()
                        if entity:
                            party_rows.append(
                                {
                                    "entity_id": entity["id"],
                                    "party_name": entity["canonical_name"],
                                    "party_role": party_role,
                                    "inn": party_inn,
                                    "canonical_name": entity["canonical_name"],
                                    "entity_type": entity["entity_type"],
                                }
                            )

        if not contract_row:
            return []

        if "contract_number" in contract_row.keys():
            contract_number = contract_row["contract_number"]
        else:
            raw = self._parse_json(contract_row["raw_data"], {})
            contract_number = raw.get("contract_number", "") if isinstance(raw, dict) else ""

        seen_entities: Set[int] = set()
        for party in party_rows:
            entity_id = party["entity_id"]
            if entity_id is None and party["inn"]:
                entity = self.conn.execute(
                    "SELECT id, canonical_name, entity_type FROM entities WHERE inn=? LIMIT 1",
                    (party["inn"],),
                ).fetchone()
                if entity:
                    entity_id = entity["id"]
                    party = dict(party)
                    party["canonical_name"] = entity["canonical_name"]
                    party["entity_type"] = entity["entity_type"]
            if entity_id is None or entity_id in seen_entities:
                continue
            seen_entities.add(entity_id)
            entity = self._load_entity(entity_id)
            if not entity:
                continue
            connections.append({
                "other_entity_id": entity_id,
                "other_name": entity["canonical_name"],
                "other_type": entity["entity_type"],
                "relation_type": "government_contract",
                "source": "contracts",
                "bidirectional": False,
                "original_from_id": node_id,
                "original_to_id": entity_id,
                "metadata": {
                    "contract_id": contract_id,
                    "contract_title": contract_row["title"],
                    "contract_number": contract_number,
                    "publication_date": contract_row["publication_date"],
                    "source_org": contract_row["source_org"],
                    "role": party["party_role"],
                    "context_type": "contract",
                    "context_id": contract_id,
                },
            })

        return connections

    def _deduplicate_connections(self, connections: List[Dict]) -> List[Dict]:
        deduped_by_key: Dict[Tuple, Dict] = {}
        for conn in connections:
            key = (
                conn["other_entity_id"],
                conn["relation_type"],
                self._connection_context_key(conn),
            )
            if key in deduped_by_key:
                deduped_by_key[key] = self._merge_connection(deduped_by_key[key], conn)
            else:
                deduped_by_key[key] = conn
        return list(deduped_by_key.values())

    def _verify_connection(self, conn: Dict) -> Confidence:
        if conn["relation_type"] == "co_voter":
            same_vote_count = conn.get("metadata", {}).get("same_vote_count", conn.get("support_count", 0))
            return Confidence.LIKELY if same_vote_count >= 20 else Confidence.UNCONFIRMED

        base = RELATION_CONFIDENCE.get(conn["relation_type"])
        if base is not None:
            return base

        if conn["relation_type"] == "mentioned_together":
            count = conn.get("metadata", {}).get("distinct_content_count", conn.get("co_mention_count", 1))
            source_count = conn.get("metadata", {}).get("distinct_source_count", 1)
            if count >= 5 and source_count >= 3:
                return Confidence.LIKELY
            elif count >= 3 and source_count >= 2:
                return Confidence.LIKELY
            else:
                return Confidence.UNCONFIRMED

        if conn["relation_type"] == "involved_in":
            mat_type = conn.get("metadata", {}).get("material_type", "")
            official_types = {"fas_decision", "audit_report", "investigation_report",
                             "foreign_agent", "undesirable_org", "presidential_act"}
            if mat_type in official_types:
                return Confidence.CONFIRMED
            return Confidence.LIKELY

        source = conn.get("source", "")
        if source in ("entity_relations", "bill_sponsors", "bill_sponsors_co",
                       "official_positions", "inn_match", "zakupki", "contracts", "bill_vote_sessions"):
            return Confidence.CONFIRMED
        if source in ("co_mentions",):
            return Confidence.UNCONFIRMED
        if source in ("risk_patterns",):
            return Confidence.LIKELY

        return Confidence.UNCONFIRMED

    def _build_edge(self, from_id: int, conn: Dict, hop: int) -> InvestigationEdge:
        confidence = conn.get("confidence", Confidence.UNCONFIRMED)
        if isinstance(confidence, str):
            confidence = Confidence.from_name(confidence)

        evidence = self._build_evidence(conn)
        edge_from_id = conn.get("original_from_id", from_id)
        edge_to_id = conn.get("original_to_id", conn["other_entity_id"])
        metadata = dict(conn.get("metadata", {}) or {})
        metadata.setdefault("source", conn.get("source", ""))
        return InvestigationEdge(
            from_id=edge_from_id,
            to_id=edge_to_id,
            relation_type=conn["relation_type"],
            confidence=confidence,
            evidence=evidence,
            bidirectional=conn.get("bidirectional", False),
            hop=hop,
            metadata=metadata,
        )

    def _build_evidence(self, conn: Dict) -> List[EvidenceItem]:
        evidence = []
        source = conn.get("source", "")
        meta = conn.get("metadata", {})

        if source == "entity_relations":
            evidence.append(EvidenceItem(
                source_type="entity_relation",
                source_url="",
                source_name="Связь сущностей (БД)",
                description=(
                    f"Связь: {RELATION_LABELS.get(conn['relation_type'], conn['relation_type'])}"
                    + (f" | evidence_item_id={meta.get('evidence_item_id')}" if meta.get("evidence_item_id") else "")
                ),
                confidence=Confidence.CONFIRMED,
            ))

        elif source == "bill_sponsors":
            evidence.append(EvidenceItem(
                source_type="duma_registry",
                source_url=f"https://sozd.duma.gov.ru/bill/{meta.get('bill_number', '')}",
                source_name="Государственная Дума",
                description=f"Спонсировал законопроект {meta.get('bill_number', '')}",
                confidence=Confidence.CONFIRMED,
            ))

        elif source == "bill_sponsors_co":
            evidence.append(EvidenceItem(
                source_type="duma_registry",
                source_url=f"https://sozd.duma.gov.ru/bill/{meta.get('bill_number', '')}",
                source_name="Государственная Дума",
                description=f"Соавтор законопроекта {meta.get('bill_number', '')}",
                confidence=Confidence.CONFIRMED,
            ))

        elif source == "bill_votes_co":
            same_vote_count = meta.get("same_vote_count", 0)
            evidence.append(EvidenceItem(
                source_type="vote_record",
                source_url="https://vote.duma.gov.ru",
                source_name="Голосования ГД",
                description=f"Совпадающие голосования: {same_vote_count}",
                confidence=Confidence.LIKELY,
            ))

        elif source == "bill_votes":
            evidence.append(EvidenceItem(
                source_type="vote_record",
                source_url="https://vote.duma.gov.ru",
                source_name="Голосования ГД",
                description=(
                    f"{RELATION_LABELS.get(conn['relation_type'], conn['relation_type'])}: "
                    f"{meta.get('bill_number', '') or meta.get('vote_session_id', '')}"
                ),
                confidence=Confidence.CONFIRMED,
            ))

        elif source == "bill_vote_sessions":
            evidence.append(EvidenceItem(
                source_type="vote_session",
                source_url="https://vote.duma.gov.ru",
                source_name="Голосования ГД",
                description=(
                    f"Сессия голосования {meta.get('vote_date', '')}: "
                    f"{meta.get('bill_number', '') or meta.get('bill_title', '') or meta.get('bill_id', '')}"
                ),
                confidence=Confidence.CONFIRMED,
            ))

        elif source == "official_positions":
            evidence.append(EvidenceItem(
                source_type="official_record",
                source_url="",
                source_name="Официальная должность",
                description=f"Должность: {meta.get('position', '')}",
                confidence=Confidence.CONFIRMED,
            ))

        elif source == "co_mentions":
            count = conn.get("co_mention_count", 1)
            evidence.append(EvidenceItem(
                source_type="content_co_mention",
                source_url="",
                source_name="Совместные упоминания в контенте",
                description=f"Упомянуты вместе в {count} материал(ах)",
                confidence=Confidence.LIKELY if count >= 3 else Confidence.UNCONFIRMED,
            ))

        elif source == "investigative_materials":
            evidence.append(EvidenceItem(
                source_type="investigative_material",
                source_url="",
                source_name=meta.get("source_org", "Расследовательский материал"),
                description=f"{meta.get('material_type', '')}: {meta.get('material_title', '')[:100]}",
                confidence=Confidence.CONFIRMED,
            ))

        elif source == "risk_patterns":
            evidence.append(EvidenceItem(
                source_type="risk_pattern",
                source_url="",
                source_name="Детектор рисков",
                description=f"Риск: {meta.get('risk_type', '')} — {meta.get('description', '')[:100]}",
                confidence=Confidence.LIKELY,
            ))

        elif source == "inn_match":
            evidence.append(EvidenceItem(
                source_type="inn_crossref",
                source_url="",
                source_name="Совпадение ИНН",
                description=f"Совпадение ИНН: {meta.get('inn', '')}",
                confidence=Confidence.CONFIRMED,
            ))

        elif source == "zakupki":
            evidence.append(EvidenceItem(
                source_type="government_procurement",
                source_url="https://zakupki.gov.ru",
                source_name="Госзакупки",
                description=f"Госконтракт: {meta.get('contract_title', '')[:100]}",
                confidence=Confidence.CONFIRMED,
            ))

        elif source == "contracts":
            evidence.append(EvidenceItem(
                source_type="government_procurement",
                source_url="https://zakupki.gov.ru",
                source_name=meta.get("source_org") or "Госзакупки",
                description=(
                    f"Госконтракт {meta.get('contract_number') or ''}: "
                    f"{meta.get('contract_title', '')[:100]}"
                ).strip(),
                confidence=Confidence.CONFIRMED,
            ))

        return evidence

    def _find_contradictions(self, result: InvestigationResult) -> List[Dict]:
        if not self._table_exists("entity_relations") or not self._table_exists("entities"):
            return []
        contradictions = []
        seed_id = result.seed_entity_id
        rows = self.conn.execute(
            "SELECT er.from_entity_id, er.to_entity_id, er.relation_type, e1.canonical_name, e2.canonical_name "
            "FROM entity_relations er "
            "JOIN entities e1 ON er.from_entity_id = e1.id "
            "JOIN entities e2 ON er.to_entity_id = e2.id "
            "WHERE er.relation_type='contradicts' AND (er.from_entity_id=? OR er.to_entity_id=?)",
            (seed_id, seed_id),
        ).fetchall()
        for row in rows:
            contradictions.append(dict(row))

        for node_id in result.nodes:
            if node_id == seed_id:
                continue
            rows2 = self.conn.execute(
                "SELECT er.from_entity_id, er.to_entity_id, e1.canonical_name, e2.canonical_name "
                "FROM entity_relations er "
                "JOIN entities e1 ON er.from_entity_id = e1.id "
                "JOIN entities e2 ON er.to_entity_id = e2.id "
                "WHERE er.relation_type='contradicts' AND (er.from_entity_id=? OR er.to_entity_id=?)",
                (node_id, node_id),
            ).fetchall()
            for row in rows2:
                contradictions.append(dict(row))

        return contradictions

    def _find_risk_patterns(self, result: InvestigationResult) -> List[Dict]:
        if not self._table_exists("risk_patterns"):
            return []
        patterns = []
        entity_ids = list(result.nodes.keys())
        rows = self.conn.execute(
            "SELECT id, pattern_type, description, risk_level, entity_ids "
            "FROM risk_patterns ORDER BY detected_at DESC LIMIT 500"
        ).fetchall()
        seen_pairs: Set[Tuple[int, int]] = set()

        for eid in entity_ids[:50]:
            for row in rows:
                linked_ids = self._entity_ids_from_json(row["entity_ids"])
                if eid in linked_ids:
                    pair = (eid, row["id"])
                    if pair in seen_pairs:
                        continue
                    seen_pairs.add(pair)
                    patterns.append({
                        "entity_id": eid,
                        "risk_pattern_id": row["id"],
                        "pattern_type": row["pattern_type"],
                        "description": row["description"],
                        "risk_level": row["risk_level"],
                        "entity_ids": row["entity_ids"],
                    })

        return patterns

    def _chain_confidence(self, edges: List[InvestigationEdge]) -> Confidence:
        if not edges:
            return Confidence.UNCONFIRMED
        return min(edges, key=lambda edge: edge.confidence.weight).confidence

    def _chain_description(
        self,
        result: InvestigationResult,
        entity_path: List[int],
        edges: List[InvestigationEdge],
    ) -> str:
        parts: List[str] = []
        for index, entity_id in enumerate(entity_path):
            node = result.nodes.get(entity_id)
            parts.append(node.canonical_name if node else str(entity_id))
            if index < len(edges):
                parts.append(f"—{edges[index].label_from(entity_id)}→")
        return " ".join(parts)

    def _edge_chain_weight(self, edge: InvestigationEdge) -> float:
        return CHAIN_RELATION_WEIGHTS.get(edge.relation_type, 0.9)

    def _edge_source_quality(self, edge: InvestigationEdge) -> float:
        source_name = edge.metadata.get("source", "")
        if source_name:
            return CHAIN_SOURCE_QUALITY.get(source_name, 1.0)
        if edge.evidence:
            return CHAIN_SOURCE_QUALITY.get(edge.evidence[0].source_type, 1.0)
        return 1.0

    def _edge_evidence_strength(self, edge: InvestigationEdge) -> float:
        strength = 1.0
        evidence_count = len(edge.evidence)
        if evidence_count:
            strength += min(0.24, 0.06 * evidence_count)
            distinct_sources = len({(item.source_type, item.source_name) for item in edge.evidence})
            if distinct_sources > 1:
                strength += min(0.16, 0.04 * (distinct_sources - 1))

        meta = edge.metadata or {}
        if edge.relation_type == "mentioned_together":
            strength += min(0.15, 0.03 * max(0, int(meta.get("distinct_source_count", 1)) - 1))
        elif edge.relation_type == "co_voter":
            strength += min(0.2, 0.01 * max(0, int(meta.get("same_vote_count", 0)) - 3))
        return strength

    def _chain_score(
        self,
        result: InvestigationResult,
        entity_path: List[int],
        edges: List[InvestigationEdge],
    ) -> float:
        if not edges:
            return 0.0

        edge_scores: List[float] = []
        for edge in edges:
            step_score = (
                edge.confidence.weight
                * self._edge_chain_weight(edge)
                * self._edge_source_quality(edge)
                * self._edge_evidence_strength(edge)
            )
            if edge.relation_type in NON_EXPANDING_CHAIN_RELATIONS:
                step_score *= 0.7
            edge_scores.append(step_score)

        base_score = sum(edge_scores) / len(edge_scores)
        terminal_node = result.nodes.get(entity_path[-1])
        terminal_bonus = 1.0 + CHAIN_NODE_BONUS.get(terminal_node.node_type, 0.0) if terminal_node else 1.0

        distinct_relations = len({edge.relation_type for edge in edges})
        novelty_bonus = 1.0 + min(0.2, 0.05 * max(0, distinct_relations - 1))
        evidence_items = sum(len(edge.evidence) for edge in edges)
        evidence_bonus = 1.0 + min(0.18, 0.03 * evidence_items)
        length_bonus = 1.0 + min(0.15, 0.05 * max(0, len(edges) - 1))

        noise_penalty = 1.0
        weak_edges = sum(1 for edge in edges if edge.relation_type in NON_EXPANDING_CHAIN_RELATIONS)
        if weak_edges:
            noise_penalty *= max(0.55, 1.0 - 0.15 * weak_edges)

        return round(base_score * terminal_bonus * novelty_bonus * evidence_bonus * length_bonus * noise_penalty, 4)

    def _chain_is_interesting(
        self,
        result: InvestigationResult,
        entity_path: List[int],
        edges: List[InvestigationEdge],
    ) -> bool:
        if not edges:
            return False
        interesting_relations = {
            "contradicts",
            "has_risk",
            "foreign_agent",
            "government_contract",
            "same_inn",
            "investigated_by",
            "about_bill",
            "sponsored_bill",
            "involved_in",
        }
        if any(edge.relation_type in interesting_relations for edge in edges):
            return True
        terminal_node = result.nodes.get(entity_path[-1])
        if terminal_node and terminal_node.node_type in CHAIN_NODE_BONUS:
            return True
        return len(edges) >= 2 and all(edge.confidence >= Confidence.LIKELY for edge in edges)

    def _should_expand_chain(self, edges: List[InvestigationEdge]) -> bool:
        if not edges:
            return True
        return edges[-1].relation_type not in NON_EXPANDING_CHAIN_RELATIONS

    def _build_evidence_chains(self, result: InvestigationResult) -> List[EvidenceChain]:
        chains: List[EvidenceChain] = []
        adj: Dict[int, List[InvestigationEdge]] = defaultdict(list)
        for edge in result.edges:
            adj[edge.from_id].append(edge)
            if edge.bidirectional:
                adj[edge.to_id].append(edge)

        seed_id = result.seed_entity_id
        max_depth = 4
        max_frontier = 256
        emitted_signatures: Set[Tuple[Tuple[int, ...], Tuple[Tuple, ...]]] = set()
        best_seen: Dict[Tuple[Tuple[int, ...], Tuple[Tuple, ...]], float] = {}
        frontier: List[Tuple[float, int, List[int], List[InvestigationEdge]]] = []
        sequence = count()

        for edge in adj.get(seed_id, []):
            if edge.confidence < Confidence.LIKELY:
                continue
            next_id = edge.to_id if edge.from_id == seed_id else edge.from_id
            path = [seed_id, next_id]
            edge_list = [edge]
            score = self._chain_score(result, path, edge_list)
            heapq.heappush(frontier, (-score, next(sequence), path, edge_list))

        while frontier and len(chains) < 40:
            neg_score, _, path, edge_list = heapq.heappop(frontier)
            score = -neg_score
            signature = (tuple(path), tuple(edge.key for edge in edge_list))
            if best_seen.get(signature, -1.0) > score:
                continue

            if self._chain_is_interesting(result, path, edge_list):
                emit_key = signature
                if emit_key not in emitted_signatures:
                    emitted_signatures.add(emit_key)
                    chains.append(EvidenceChain(
                        description=self._chain_description(result, path, edge_list),
                        entity_path=path,
                        edge_path=[edge.key for edge in edge_list],
                        confidence=self._chain_confidence(edge_list),
                        pattern_type=edge_list[-1].relation_type,
                        score=score,
                    ))

            if len(edge_list) >= max_depth or not self._should_expand_chain(edge_list):
                continue

            current = path[-1]
            for next_edge in adj.get(current, []):
                if next_edge.confidence < Confidence.LIKELY:
                    continue
                next_id = next_edge.to_id if next_edge.from_id == current else next_edge.from_id
                if next_id in path:
                    continue
                new_path = path + [next_id]
                new_edges = edge_list + [next_edge]
                new_score = self._chain_score(result, new_path, new_edges)
                new_signature = (tuple(new_path), tuple(edge.key for edge in new_edges))
                if best_seen.get(new_signature, -1.0) >= new_score:
                    continue
                best_seen[new_signature] = new_score
                heapq.heappush(frontier, (-new_score, next(sequence), new_path, new_edges))

            if len(frontier) > max_frontier:
                frontier = heapq.nsmallest(max_frontier, frontier)
                heapq.heapify(frontier)

        chains.sort(key=lambda chain: (chain.score, chain.confidence.weight, len(chain.edge_path)), reverse=True)
        return chains[:20]

    def _detect_leads(self, result: InvestigationResult) -> List[Lead]:
        leads: List[Lead] = []
        seed_id = result.seed_entity_id
        seed_type = result.seed_type

        seed_edges = result.edges_for(seed_id)
        for edge in seed_edges:
            other_id = edge.to_id if edge.from_id == seed_id else edge.from_id
            other_node = result.nodes.get(other_id)

            if not other_node:
                continue

            if edge.relation_type == "mentioned_together" and edge.confidence == Confidence.UNCONFIRMED:
                co_count = edge.metadata.get("co_mention_count", 0)
                if co_count >= 3:
                    leads.append(Lead(
                        entity_id=other_id,
                        entity_name=other_node.canonical_name,
                        entity_type=other_node.entity_type,
                        description=f"Совместные упоминания с {result.seed_name} ({co_count} раз)",
                        reason="co_mention_unverified",
                        confidence=Confidence.UNCONFIRMED,
                        source_entity_ids=[seed_id],
                        interestingness=0.3 + co_count * 0.1,
                    ))

            if other_node.entity_type == "organization":
                org_edges = result.edges_for(other_id)
                has_contract = any(e.relation_type == "government_contract" for e in org_edges)
                has_foreign_agent = any(e.relation_type == "foreign_agent" for e in org_edges)
                has_investigation = any(e.relation_type in ("involved_in", "investigated_by", "has_risk")
                                        for e in org_edges)

                if has_contract and seed_type == "person":
                    seed_extra = result.nodes[seed_id].extra
                    if seed_extra.get("deputy_profile"):
                        leads.append(Lead(
                            entity_id=other_id,
                            entity_name=other_node.canonical_name,
                            entity_type=other_node.entity_type,
                            description=f"Организация с госконтрактом, связанная с депутатом",
                            reason="deputy_org_contract",
                            confidence=Confidence.LIKELY,
                            source_entity_ids=[seed_id, other_id],
                            interestingness=0.7,
                        ))

                if has_foreign_agent:
                    leads.append(Lead(
                        entity_id=other_id,
                        entity_name=other_node.canonical_name,
                        entity_type=other_node.entity_type,
                        description="Связанная организация — иностранный агент",
                        reason="connected_foreign_agent",
                        confidence=Confidence.CONFIRMED,
                        source_entity_ids=[seed_id, other_id],
                        interestingness=0.9,
                    ))

                if has_investigation:
                    leads.append(Lead(
                        entity_id=other_id,
                        entity_name=other_node.canonical_name,
                        entity_type=other_node.entity_type,
                        description="Организация под расследованием",
                        reason="connected_investigation",
                        confidence=Confidence.CONFIRMED,
                        source_entity_ids=[seed_id, other_id],
                        interestingness=0.8,
                    ))

        for edge in seed_edges:
            if edge.relation_type in ("voted_for", "sponsored_bill"):
                bill_num = edge.metadata.get("bill_number", "")
                if bill_num:
                    other_edges = result.edges_for(edge.to_id if edge.from_id == seed_id else edge.from_id)
                    for oe in other_edges:
                        if oe.relation_type == "government_contract":
                            target_id = oe.to_id if oe.from_id != seed_id else oe.from_id
                            target_node = result.nodes.get(target_id)
                            if target_node and target_node.entity_type == "organization":
                                leads.append(Lead(
                                    entity_id=target_id,
                                    entity_name=target_node.canonical_name,
                                    entity_type=target_node.entity_type,
                                    description=f"Организация получила контракт после закона {bill_num}",
                                    reason="bill_beneficiary_contract",
                                    confidence=Confidence.LIKELY,
                                    source_entity_ids=[seed_id, edge.to_id if edge.from_id == seed_id else edge.from_id, target_id],
                                    interestingness=0.85,
                                ))

        for c in result.contradictions:
            leads.append(Lead(
                entity_id=c.get("from_entity_id", c.get("to_entity_id", 0)),
                entity_name=c.get("canonical_name", ""),
                entity_type="person",
                description=f"Противоречие: {c.get('canonical_name', 'Неизвестно')}",
                reason="contradiction",
                confidence=Confidence.CONFIRMED,
                source_entity_ids=[seed_id],
                interestingness=0.75,
            ))

        leads.sort(key=lambda l: l.interestingness, reverse=True)
        return leads[:30]

    def _compute_stats(self, result: InvestigationResult) -> Dict:
        type_counts: Dict[str, int] = defaultdict(int)
        for node in result.nodes.values():
            type_counts[node.entity_type] += 1

        rel_counts: Dict[str, int] = defaultdict(int)
        for edge in result.edges:
            rel_counts[edge.relation_type] += 1

        hop_counts: Dict[int, int] = defaultdict(int)
        for node in result.nodes.values():
            hop_counts[node.hop] += 1

        return {
            "total_nodes": len(result.nodes),
            "total_edges": len(result.edges),
            "confirmed_edges": result.total_confirmed,
            "likely_edges": result.total_likely,
            "unconfirmed_edges": result.total_unconfirmed,
            "entity_types": dict(type_counts),
            "relation_types": dict(rel_counts),
            "hop_distribution": dict(hop_counts),
            "contradictions": len(result.contradictions),
            "risk_patterns": len(result.risk_patterns),
            "evidence_chains": len(result.evidence_chains),
            "leads": len(result.leads),
        }
