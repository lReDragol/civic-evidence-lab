from __future__ import annotations

from typing import Dict, List

from .models import (
    Confidence,
    EvidenceChain,
    InvestigationEdge,
    InvestigationNode,
    InvestigationResult,
    Lead,
    RELATION_LABELS,
)


class DossierGenerator:
    def __init__(self, result: InvestigationResult):
        self.result = result
        self._lines: List[str] = []

    def generate(self) -> str:
        self._lines = []
        self._header()
        self._subject()
        self._direct_connections()
        self._extended_network()
        self._evidence_chains()
        self._contradictions()
        self._risks()
        self._leads()
        self._statistics()
        return "\n".join(self._lines)

    def _w(self, text: str = ""):
        self._lines.append(text)

    def _header(self):
        r = self.result
        self._w("=" * 72)
        self._w(f"  ДОСЬЕ РАССЛЕДОВАНИЯ")
        self._w(f"  {r.seed_name}")
        self._w("=" * 72)
        self._w()

    def _subject(self):
        r = self.result
        seed_node = r.nodes.get(r.seed_entity_id)
        if not seed_node:
            return
        extra = seed_node.extra

        self._w(f"{'─' * 3} СУБЪЕКТ {'─' * 60}")
        self._w(f"  Имя:           {r.seed_name}")
        self._w(f"  Тип:            {seed_node.node_type.label}")
        if extra.get("inn"):
            self._w(f"  ИНН:            {extra['inn']}")
        if extra.get("ogrn"):
            self._w(f"  ОГРН:           {extra['ogrn']}")

        dp = extra.get("deputy_profile")
        if dp:
            if dp.get("position"):
                self._w(f"  Должность:      {dp['position']}")
            if dp.get("faction"):
                self._w(f"  Фракция:        {dp['faction']}")
            if dp.get("region"):
                self._w(f"  Регион:         {dp['region']}")
            if dp.get("committee"):
                self._w(f"  Комитет:        {dp['committee']}")
            if dp.get("income_latest"):
                self._w(f"  Доход:          {dp['income_latest']}")
            if dp.get("is_active") is not None:
                self._w(f"  Активный:       {'Да' if dp['is_active'] else 'Нет'}")

        acc = extra.get("accountability")
        if acc:
            self._w(f"  Индекс подотч.: {acc.get('calculated_score', 'N/A')}")
            self._w(f"  Противоречий:   {acc.get('confirmed_contradictions', 0)}")
            self._w(f"  Флагов:         {acc.get('flagged_statements_count', 0)}")
            self._w(f"  Связанных дел:  {acc.get('linked_cases_count', 0)}")

        positions = extra.get("positions", [])
        if positions:
            self._w(f"  Должности:")
            for p in positions[:3]:
                active = " (активная)" if p.get("is_active") else ""
                self._w(f"    - {p.get('position_title', '')} @ {p.get('organization', '')}{active}")

        parties = extra.get("parties", [])
        if parties:
            self._w(f"  Партии:")
            for p in parties[:3]:
                current = " (текущая)" if p.get("is_current") else ""
                self._w(f"    - {p.get('party_name', '')} — {p.get('role', '')}{current}")

        self._w()

    def _direct_connections(self):
        r = self.result
        self._w(f"{'─' * 3} ПРЯМЫЕ СВЯЗИ ({len(r.edges)}) {'─' * 52}")

        by_type: Dict[str, List[InvestigationEdge]] = {}
        for edge in r.edges:
            by_type.setdefault(edge.relation_type, []).append(edge)

        priority_types = [
            "voted_for", "voted_against", "voted_abstained",
            "sponsored_bill", "co_sponsor",
            "party_member", "works_at", "head_of",
            "member_of_committee", "represents_region",
            "government_contract", "foreign_agent",
            "involved_in", "investigated_by", "has_risk",
            "same_inn", "contradicts",
            "mentioned_together", "co_voter",
            "bill_beneficiary", "same_region_contract",
        ]
        seen_types = set()

        for rt in priority_types:
            edges = by_type.get(rt, [])
            if not edges:
                continue
            seen_types.add(rt)
            label = RELATION_LABELS.get(rt, rt)
            self._w(f"  {label} ({len(edges)}):")
            for edge in edges[:10]:
                other_id = edge.to_id
                other_node = r.nodes.get(other_id)
                name = other_node.short_name if other_node else f"ID:{other_id}"
                conf = edge.confidence.symbol
                meta_info = ""
                if edge.metadata.get("bill_number"):
                    meta_info = f" [{edge.metadata['bill_number']}]"
                elif edge.metadata.get("position"):
                    meta_info = f" ({edge.metadata['position']})"
                elif edge.metadata.get("inn"):
                    meta_info = f" (ИНН:{edge.metadata['inn']})"
                elif edge.metadata.get("co_mention_count"):
                    meta_info = f" ({edge.metadata['co_mention_count']} упомин.)"
                self._w(f"    {conf} → {name}{meta_info}")
            if len(edges) > 10:
                self._w(f"    ... и ещё {len(edges) - 10}")

        for rt, edges in by_type.items():
            if rt in seen_types:
                continue
            label = RELATION_LABELS.get(rt, rt)
            self._w(f"  {label} ({len(edges)}):")
            for edge in edges[:5]:
                other_id = edge.to_id
                other_node = r.nodes.get(other_id)
                name = other_node.short_name if other_node else f"ID:{other_id}"
                conf = edge.confidence.symbol
                self._w(f"    {conf} → {name}")
            if len(edges) > 5:
                self._w(f"    ... и ещё {len(edges) - 5}")

        self._w()

    def _extended_network(self):
        r = self.result
        if r.max_hop <= 1:
            return

        self._w(f"{'─' * 3} РАСШИРЕННАЯ СЕТЬ {'─' * 55}")

        hop_nodes: Dict[int, List[InvestigationNode]] = {}
        for node in r.nodes.values():
            hop_nodes.setdefault(node.hop, []).append(node)

        for hop in sorted(hop_nodes.keys()):
            if hop == 0:
                continue
            nodes = hop_nodes[hop]
            type_counts: Dict[str, int] = {}
            for n in nodes:
                type_counts[n.entity_type] = type_counts.get(n.entity_type, 0) + 1
            type_str = ", ".join(f"{t}: {c}" for t, c in sorted(type_counts.items(), key=lambda x: -x[1]))
            self._w(f"  Хоп {hop}: {len(nodes)} сущностей ({type_str})")

        self._w()

    def _evidence_chains(self):
        r = self.result
        if not r.evidence_chains:
            return

        self._w(f"{'─' * 3} ДОКАЗАТЕЛЬСТВЕННЫЕ ЦЕПИ ({len(r.evidence_chains)}) {'─' * 48}")
        for i, chain in enumerate(r.evidence_chains[:10], 1):
            conf = chain.confidence.symbol
            self._w(f"  Цепь {i} {conf} [{chain.pattern_type}]:")
            parts = []
            for eid in chain.entity_path:
                node = r.nodes.get(eid)
                if node:
                    parts.append(node.short_name)
                else:
                    parts.append(f"ID:{eid}")
            if parts:
                self._w(f"    {' → '.join(parts)}")
            if chain.description:
                self._w(f"    {chain.description[:100]}")

        if len(r.evidence_chains) > 10:
            self._w(f"  ... и ещё {len(r.evidence_chains) - 10} цепей")

        self._w()

    def _contradictions(self):
        r = self.result
        if not r.contradictions:
            return

        self._w(f"{'─' * 3} ПРОТИВОРЕЧИЯ ({len(r.contradictions)}) {'─' * 53}")
        for c in r.contradictions[:10]:
            name1 = c.get("canonical_name", c.get("from_name", "?"))
            name2 = c.get("canonical_name", c.get("to_name", "?"))
            self._w(f"  [!] {name1} ↔ {name2}")
        self._w()

    def _risks(self):
        r = self.result
        if not r.risk_patterns:
            return

        self._w(f"{'─' * 3} РИСКИ ({len(r.risk_patterns)}) {'─' * 60}")
        risk_symbols = {"critical": "[!!!]", "high": "[!!]", "medium": "[!]", "low": "[~]"}
        for rp in r.risk_patterns[:10]:
            level = rp.get("risk_level", "medium")
            sym = risk_symbols.get(level, "[?]")
            ptype = rp.get("pattern_type", "")
            desc = rp.get("description", "")[:80]
            self._w(f"  {sym} {ptype}: {desc}")
        self._w()

    def _leads(self):
        r = self.result
        if not r.leads:
            return

        self._w(f"{'─' * 3} НАВОДКИ ({len(r.leads)}) {'─' * 59}")
        for i, lead in enumerate(r.leads[:15], 1):
            conf = lead.confidence.symbol
            score = f"{lead.interestingness:.2f}"
            self._w(f"  {i}. {conf} [{score}] {lead.entity_name} ({lead.entity_type})")
            self._w(f"     {lead.description}")
            self._w(f"     Причина: {lead.reason}")
        self._w()

    def _statistics(self):
        r = self.result
        s = r.stats
        if not s:
            return

        self._w(f"{'─' * 3} СТАТИСТИКА {'─' * 59}")
        self._w(f"  Узлов:         {s.get('total_nodes', 0)}")
        self._w(f"  Рёбер:         {s.get('total_edges', 0)}")
        self._w(f"  Подтверждено:  {s.get('confirmed_edges', 0)}")
        self._w(f"  Вероятно:      {s.get('likely_edges', 0)}")
        self._w(f"  Требует пров.: {s.get('unconfirmed_edges', 0)}")
        self._w(f"  Противоречия:  {s.get('contradictions', 0)}")
        self._w(f"  Риски:         {s.get('risk_patterns', 0)}")
        self._w(f"  Цепи док-в:    {s.get('evidence_chains', 0)}")
        self._w(f"  Наводки:       {s.get('leads', 0)}")
        self._w()

        et = s.get("entity_types", {})
        if et:
            self._w(f"  По типам:")
            for t, c in sorted(et.items(), key=lambda x: -x[1]):
                self._w(f"    {t}: {c}")

        hd = s.get("hop_distribution", {})
        if hd:
            self._w(f"  По хопам:")
            for h in sorted(hd.keys()):
                self._w(f"    Хоп {h}: {hd[h]}")

        self._w()
        self._w("=" * 72)
