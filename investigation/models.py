from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class Confidence(Enum):
    CONFIRMED = "confirmed"
    LIKELY = "likely"
    UNCONFIRMED = "unconfirmed"
    DISPUTED = "disputed"

    @property
    def weight(self) -> float:
        return {"confirmed": 1.0, "likely": 0.6, "unconfirmed": 0.3, "disputed": 0.1}[self.value]

    @property
    def symbol(self) -> str:
        return {"confirmed": "[+]", "likely": "[~]", "unconfirmed": "[?]", "disputed": "[!]"}[self.value]

    def __ge__(self, other):
        if not isinstance(other, Confidence):
            return NotImplemented
        order = [Confidence.DISPUTED, Confidence.UNCONFIRMED, Confidence.LIKELY, Confidence.CONFIRMED]
        return order.index(self) >= order.index(other)

    def __gt__(self, other):
        if not isinstance(other, Confidence):
            return NotImplemented
        order = [Confidence.DISPUTED, Confidence.UNCONFIRMED, Confidence.LIKELY, Confidence.CONFIRMED]
        return order.index(self) > order.index(other)

    def __le__(self, other):
        return not self > other

    def __lt__(self, other):
        return not self >= other

    @classmethod
    def from_name(cls, name: str) -> "Confidence":
        return {"confirmed": cls.CONFIRMED, "likely": cls.LIKELY,
                "unconfirmed": cls.UNCONFIRMED, "disputed": cls.DISPUTED}.get(name, cls.UNCONFIRMED)


class NodeType(Enum):
    PERSON = "person"
    ORGANIZATION = "organization"
    LAW = "law"
    BILL = "bill"
    CASE = "case"
    REGION = "region"
    PARTY = "party"
    COMMITTEE = "committee"
    VOTE_SESSION = "vote_session"
    CONTRACT = "contract"
    INVESTIGATIVE = "investigative"
    CLAIM = "claim"
    RISK = "risk"
    LOCATION = "location"
    OGRN = "ogrn"
    CONTENT = "content"

    @property
    def color_rgb(self) -> tuple:
        colors = {
            NodeType.PERSON: (70, 130, 220),
            NodeType.ORGANIZATION: (60, 180, 80),
            NodeType.LAW: (200, 180, 60),
            NodeType.BILL: (210, 190, 50),
            NodeType.CASE: (220, 110, 70),
            NodeType.REGION: (160, 100, 200),
            NodeType.PARTY: (220, 80, 80),
            NodeType.COMMITTEE: (100, 180, 200),
            NodeType.VOTE_SESSION: (180, 140, 200),
            NodeType.CONTRACT: (255, 140, 0),
            NodeType.INVESTIGATIVE: (200, 60, 60),
            NodeType.CLAIM: (220, 180, 100),
            NodeType.RISK: (255, 50, 50),
            NodeType.LOCATION: (130, 160, 180),
            NodeType.OGRN: (180, 180, 180),
            NodeType.CONTENT: (150, 200, 150),
        }
        return colors.get(self, (180, 180, 180))

    @property
    def color_hex(self) -> int:
        r, g, b = self.color_rgb
        return (r << 16) | (g << 8) | b

    @property
    def label(self) -> str:
        labels = {
            NodeType.PERSON: "Персона",
            NodeType.ORGANIZATION: "Организация",
            NodeType.LAW: "Закон",
            NodeType.BILL: "Законопроект",
            NodeType.CASE: "Дело",
            NodeType.REGION: "Регион",
            NodeType.PARTY: "Партия",
            NodeType.COMMITTEE: "Комитет",
            NodeType.VOTE_SESSION: "Голосование",
            NodeType.CONTRACT: "Контракт",
            NodeType.INVESTIGATIVE: "Расследование",
            NodeType.CLAIM: "Заявление",
            NodeType.RISK: "Риск",
            NodeType.LOCATION: "Местоположение",
            NodeType.OGRN: "ОГРН",
            NodeType.CONTENT: "Контент",
        }
        return labels.get(self, "Другое")

    @classmethod
    def from_entity_type(cls, entity_type: str) -> "NodeType":
        mapping = {
            "person": cls.PERSON,
            "organization": cls.ORGANIZATION,
            "law": cls.LAW,
            "region": cls.REGION,
            "party": cls.PARTY,
            "committee": cls.COMMITTEE,
            "location": cls.LOCATION,
            "ogrn": cls.OGRN,
        }
        return mapping.get(entity_type, cls.CONTENT)

    @classmethod
    def from_material_type(cls, material_type: str) -> "NodeType":
        mapping = {
            "government_contract": cls.CONTRACT,
            "fas_decision": cls.INVESTIGATIVE,
            "audit_report": cls.INVESTIGATIVE,
            "investigation_report": cls.INVESTIGATIVE,
            "presidential_act": cls.LAW,
            "foreign_agent": cls.INVESTIGATIVE,
            "undesirable_org": cls.INVESTIGATIVE,
            "government_decision": cls.LAW,
            "legal_act_publication": cls.LAW,
            "ach_news": cls.INVESTIGATIVE,
        }
        return mapping.get(material_type, cls.INVESTIGATIVE)


RELATION_LABELS = {
    "voted_for": "Голосовал ЗА",
    "voted_against": "Голосовал ПРОТИВ",
    "voted_abstained": "Воздержался",
    "voted_absent": "Не голосовал",
    "sponsored_bill": "Спонсировал",
    "works_at": "Работает в",
    "head_of": "Руководит",
    "party_member": "Член партии",
    "member_of": "Член",
    "represents_region": "Представляет регион",
    "member_of_committee": "Член комитета",
    "mentioned_together": "Упомянут вместе",
    "associated_with_location": "Связан с местом",
    "located_in": "Находится в",
    "contradicts": "Противоречит",
    "government_contract": "Госконтракт",
    "foreign_agent": "Иноагент",
    "co_sponsor": "Соавтор законопроекта",
    "co_voter": "Соголосовавший",
    "investigated_by": "Расследуется",
    "same_inn": "Совпадение ИНН",
    "involved_in": "Причастен к",
    "has_claim": "Заявление",
    "has_risk": "Риск-паттерн",
    "same_region_contract": "Контракт в регионе",
    "bill_beneficiary": "Бенефициар закона",
    "about_bill": "По законопроекту",
    "reported_in": "Зафиксировано в",
    "supported_by": "Подкреплено доказательством",
    "mentions_entity": "Упоминает",
    "involved_in_case": "Фигурант дела",
    "part_of_case": "Входит в дело",
    "documents_case": "Документирует дело",
}

RELATION_INVERSE_LABELS = {
    "works_at": "Сотрудник",
    "head_of": "Имеет руководителя",
    "party_member": "Включает члена партии",
    "member_of": "Содержит участника",
    "represents_region": "Представлен",
    "member_of_committee": "Имеет члена комитета",
    "sponsored_bill": "Спонсируется",
    "voted_for": "Поддержан голосом",
    "voted_against": "Отклонён голосом",
    "voted_abstained": "Имеет воздержавшегося",
    "voted_absent": "Имеет отсутствовавшего",
    "government_contract": "Связан контрактом с",
    "investigated_by": "Фигурант расследования",
    "involved_in": "Содержит фигуранта",
    "has_claim": "Фигурант заявления",
    "has_risk": "Фигурант риска",
    "bill_beneficiary": "Даёт выгоду",
    "about_bill": "Имеет голосование",
    "reported_in": "Содержит заявление",
    "supported_by": "Служит доказательством для",
    "mentions_entity": "Упомянут в",
    "involved_in_case": "Дело включает фигуранта",
    "part_of_case": "Дело опирается на заявление",
    "documents_case": "Используется в деле",
}

RELATION_CONFIDENCE = {
    "voted_for": Confidence.CONFIRMED,
    "voted_against": Confidence.CONFIRMED,
    "voted_abstained": Confidence.CONFIRMED,
    "voted_absent": Confidence.CONFIRMED,
    "sponsored_bill": Confidence.CONFIRMED,
    "works_at": Confidence.CONFIRMED,
    "head_of": Confidence.CONFIRMED,
    "party_member": Confidence.CONFIRMED,
    "member_of": Confidence.CONFIRMED,
    "represents_region": Confidence.CONFIRMED,
    "member_of_committee": Confidence.CONFIRMED,
    "contradicts": Confidence.CONFIRMED,
    "government_contract": Confidence.CONFIRMED,
    "foreign_agent": Confidence.CONFIRMED,
    "same_inn": Confidence.CONFIRMED,
    "located_in": Confidence.LIKELY,
    "associated_with_location": Confidence.LIKELY,
    "co_sponsor": Confidence.CONFIRMED,
    "co_voter": Confidence.LIKELY,
    "investigated_by": Confidence.CONFIRMED,
    "involved_in": Confidence.LIKELY,
    "same_region_contract": Confidence.LIKELY,
    "bill_beneficiary": Confidence.LIKELY,
    "about_bill": Confidence.CONFIRMED,
    "reported_in": Confidence.CONFIRMED,
    "mentions_entity": Confidence.CONFIRMED,
    "involved_in_case": Confidence.CONFIRMED,
    "part_of_case": Confidence.CONFIRMED,
    "documents_case": Confidence.CONFIRMED,
    "mentioned_together": None,
    "has_claim": Confidence.LIKELY,
    "supported_by": None,
    "has_risk": Confidence.LIKELY,
    "member_of": Confidence.CONFIRMED,
}


@dataclass
class EvidenceItem:
    source_type: str
    source_url: str
    source_name: str
    description: str
    confidence: Confidence
    raw_data: Optional[Dict] = None

    def to_dict(self) -> Dict:
        return {
            "source_type": self.source_type,
            "source_url": self.source_url,
            "source_name": self.source_name,
            "description": self.description,
            "confidence": self.confidence.value,
        }

    @classmethod
    def from_dict(cls, d: Dict) -> "EvidenceItem":
        return cls(
            source_type=d["source_type"],
            source_url=d.get("source_url", ""),
            source_name=d.get("source_name", ""),
            description=d.get("description", ""),
            confidence=Confidence(d.get("confidence", "unconfirmed")),
        )


@dataclass
class InvestigationEdge:
    from_id: int
    to_id: int
    relation_type: str
    confidence: Confidence
    evidence: List[EvidenceItem] = field(default_factory=list)
    bidirectional: bool = False
    hop: int = 0
    metadata: Dict = field(default_factory=dict)

    @property
    def label(self) -> str:
        return RELATION_LABELS.get(self.relation_type, self.relation_type)

    def label_from(self, perspective_id: Optional[int] = None) -> str:
        if self.bidirectional or perspective_id is None or perspective_id == self.from_id:
            return self.label
        if perspective_id == self.to_id:
            return RELATION_INVERSE_LABELS.get(self.relation_type, self.label)
        return self.label

    @property
    def context_key(self) -> tuple:
        meta = self.metadata or {}
        context_parts = []
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
            "organization",
        ):
            if key in meta and meta[key] not in (None, "", [], {}):
                context_parts.append((key, self._freeze_value(meta[key])))
        return tuple(context_parts)

    @property
    def key(self) -> tuple:
        return (self.from_id, self.to_id, self.relation_type, self.context_key)

    def _freeze_value(self, value):
        if isinstance(value, dict):
            return tuple(sorted((k, self._freeze_value(v)) for k, v in value.items()))
        if isinstance(value, (list, tuple, set)):
            return tuple(self._freeze_value(v) for v in value)
        return value

    def to_dict(self) -> Dict:
        return {
            "from_id": self.from_id,
            "to_id": self.to_id,
            "relation_type": self.relation_type,
            "confidence": self.confidence.value,
            "bidirectional": self.bidirectional,
            "hop": self.hop,
            "metadata": self.metadata,
            "evidence": [e.to_dict() for e in self.evidence],
        }

    @classmethod
    def from_dict(cls, d: Dict) -> "InvestigationEdge":
        return cls(
            from_id=d["from_id"],
            to_id=d["to_id"],
            relation_type=d["relation_type"],
            confidence=Confidence(d["confidence"]),
            bidirectional=d.get("bidirectional", False),
            hop=d.get("hop", 0),
            metadata=d.get("metadata", {}),
            evidence=[EvidenceItem.from_dict(e) for e in d.get("evidence", [])],
        )


@dataclass
class InvestigationNode:
    entity_id: int
    canonical_name: str
    entity_type: str
    node_type: NodeType
    extra: Dict = field(default_factory=dict)
    hop: int = 0
    is_expanded: bool = False

    @property
    def short_name(self) -> str:
        name = self.canonical_name
        if len(name) > 50:
            return name[:47] + "..."
        return name

    def to_dict(self) -> Dict:
        return {
            "entity_id": self.entity_id,
            "canonical_name": self.canonical_name,
            "entity_type": self.entity_type,
            "node_type": self.node_type.value,
            "extra": self.extra,
            "hop": self.hop,
            "is_expanded": self.is_expanded,
        }

    @classmethod
    def from_dict(cls, d: Dict) -> "InvestigationNode":
        return cls(
            entity_id=d["entity_id"],
            canonical_name=d["canonical_name"],
            entity_type=d["entity_type"],
            node_type=NodeType(d["node_type"]),
            extra=d.get("extra", {}),
            hop=d.get("hop", 0),
            is_expanded=d.get("is_expanded", False),
        )


@dataclass
class EvidenceChain:
    description: str
    entity_path: List[int] = field(default_factory=list)
    edge_path: List[tuple] = field(default_factory=list)
    confidence: Confidence = Confidence.LIKELY
    pattern_type: str = ""
    score: float = 0.0

    def to_dict(self) -> Dict:
        return {
            "description": self.description,
            "entity_path": self.entity_path,
            "edge_path": list(self.edge_path),
            "confidence": self.confidence.value,
            "pattern_type": self.pattern_type,
            "score": self.score,
        }


@dataclass
class Lead:
    entity_id: int
    entity_name: str
    entity_type: str
    description: str
    reason: str
    confidence: Confidence
    source_entity_ids: List[int] = field(default_factory=list)
    interestingness: float = 0.0

    def to_dict(self) -> Dict:
        return {
            "entity_id": self.entity_id,
            "entity_name": self.entity_name,
            "entity_type": self.entity_type,
            "description": self.description,
            "reason": self.reason,
            "confidence": self.confidence.value,
            "source_entity_ids": self.source_entity_ids,
            "interestingness": self.interestingness,
        }


@dataclass
class InvestigationResult:
    seed_entity_id: int
    seed_name: str = ""
    seed_type: str = ""
    nodes: Dict[int, InvestigationNode] = field(default_factory=dict)
    edges: List[InvestigationEdge] = field(default_factory=list)
    max_hop: int = 0
    contradictions: List[Dict] = field(default_factory=list)
    risk_patterns: List[Dict] = field(default_factory=list)
    evidence_chains: List[EvidenceChain] = field(default_factory=list)
    leads: List[Lead] = field(default_factory=list)
    stats: Dict = field(default_factory=dict)

    @property
    def total_confirmed(self) -> int:
        return sum(1 for e in self.edges if e.confidence == Confidence.CONFIRMED)

    @property
    def total_likely(self) -> int:
        return sum(1 for e in self.edges if e.confidence == Confidence.LIKELY)

    @property
    def total_unconfirmed(self) -> int:
        return sum(1 for e in self.edges if e.confidence == Confidence.UNCONFIRMED)

    @property
    def edge_keys(self) -> set:
        return {e.key for e in self.edges}

    def has_edge(self, from_id: int, to_id: int, relation_type: str) -> bool:
        return (from_id, to_id, relation_type) in self.edge_keys

    def edges_for(self, entity_id: int) -> List[InvestigationEdge]:
        return [e for e in self.edges if e.from_id == entity_id or e.to_id == entity_id]

    def neighbors(self, entity_id: int) -> List[int]:
        ids = set()
        for e in self.edges:
            if e.from_id == entity_id:
                ids.add(e.to_id)
            elif e.to_id == entity_id:
                ids.add(e.from_id)
        return list(ids)

    def to_json(self) -> str:
        return json.dumps({
            "seed_entity_id": self.seed_entity_id,
            "seed_name": self.seed_name,
            "seed_type": self.seed_type,
            "nodes": {str(k): v.to_dict() for k, v in self.nodes.items()},
            "edges": [e.to_dict() for e in self.edges],
            "contradictions": self.contradictions,
            "risk_patterns": self.risk_patterns,
            "evidence_chains": [ec.to_dict() for ec in self.evidence_chains],
            "leads": [l.to_dict() for l in self.leads],
            "max_hop": self.max_hop,
            "stats": self.stats,
        }, ensure_ascii=False, default=str)

    @classmethod
    def from_json(cls, data: str) -> "InvestigationResult":
        d = json.loads(data)
        result = cls(
            seed_entity_id=d["seed_entity_id"],
            seed_name=d.get("seed_name", ""),
            seed_type=d.get("seed_type", ""),
            max_hop=d.get("max_hop", 0),
            contradictions=d.get("contradictions", []),
            risk_patterns=d.get("risk_patterns", []),
            stats=d.get("stats", {}),
        )
        for k, v in d.get("nodes", {}).items():
            result.nodes[int(k)] = InvestigationNode.from_dict(v)
        for e in d.get("edges", []):
            result.edges.append(InvestigationEdge.from_dict(e))
        for ec in d.get("evidence_chains", []):
            result.evidence_chains.append(EvidenceChain(
                description=ec["description"],
                entity_path=ec.get("entity_path", []),
                edge_path=[tuple(ep) for ep in ec.get("edge_path", [])],
                confidence=Confidence(ec.get("confidence", "likely")),
                pattern_type=ec.get("pattern_type", ""),
                score=float(ec.get("score", 0.0)),
            ))
        for l in d.get("leads", []):
            result.leads.append(Lead(
                entity_id=l["entity_id"],
                entity_name=l["entity_name"],
                entity_type=l["entity_type"],
                description=l["description"],
                reason=l["reason"],
                confidence=Confidence(l.get("confidence", "unconfirmed")),
                source_entity_ids=l.get("source_entity_ids", []),
                interestingness=l.get("interestingness", 0.0),
            ))
        return result

    def merge(self, other: "InvestigationResult") -> "InvestigationResult":
        existing_keys = self.edge_keys
        for nid, node in other.nodes.items():
            if nid not in self.nodes:
                self.nodes[nid] = node
        for edge in other.edges:
            if edge.key not in existing_keys:
                self.edges.append(edge)
        self.contradictions.extend(other.contradictions)
        self.risk_patterns.extend(other.risk_patterns)
        self.evidence_chains.extend(other.evidence_chains)
        self.leads.extend(other.leads)
        self.max_hop = max(self.max_hop, other.max_hop)
        return self
