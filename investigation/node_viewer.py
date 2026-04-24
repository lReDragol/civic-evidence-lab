from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

import dearpygui.dearpygui as dpg

from .models import (
    Confidence,
    InvestigationEdge,
    InvestigationNode,
    InvestigationResult,
    NodeType,
    RELATION_LABELS,
)

NODE_PREFIX = "node_"
ATTR_PREFIX = "attr_"
LINK_PREFIX = "link_"
DETAIL_WINDOW = "detail_window"
DOSSIER_WINDOW = "dossier_window"


class InvestigationGraphViewer:
    def __init__(self, result: InvestigationResult, dossier_text: str = ""):
        self.result = result
        self.dossier_text = dossier_text
        self._node_positions: Dict[int, Dict] = {}
        self._link_ids: Dict[tuple, int] = {}
        self._expanded_nodes: Set[int] = set()
        self._selected_entity: Optional[int] = None
        self._detail_fields: List[str] = []

    def show(self):
        dpg.create_context()

        with dpg.window(tag="primary_window", no_resize=False):
            with dpg.menu_bar():
                dpg.add_menu_item(label="Показать досье", callback=self._show_dossier)
                dpg.add_menu_item(label="Фильтр: подтверждённые", callback=lambda: self._filter_by_confidence(Confidence.CONFIRMED))
                dpg.add_menu_item(label="Фильтр: все", callback=self._show_all_links)
                dpg.add_menu_item(label="Сбросить позицию", callback=self._reset_layout)

            with dpg.splitter(direction="horizontal"):
                with dpg.group(width=800):
                    dpg.add_text(f"Расследование: {self.result.seed_name}")
                    dpg.add_text(f"Узлов: {len(self.result.nodes)} | Рёбер: {len(self.result.edges)} | "
                                 f"Подтверждено: {self.result.total_confirmed} | "
                                 f"Наводок: {len(self.result.leads)}")

                    with dpg.node_editor(
                        callback=self._on_link,
                        delink_callback=self._on_delink,
                        tag="node_editor",
                        minimap=True,
                        minimap_location=dpg.mvNodeMiniMap_Location_BottomRight,
                    ):
                        self._create_all_nodes()
                        self._create_all_links()

                with dpg.group(width=350):
                    dpg.add_text("Детали узла")
                    dpg.add_separator()
                    with dpg.group(tag="detail_panel"):
                        dpg.add_text("Выберите узел для просмотра деталей", color=(150, 150, 150))

        dpg.create_viewport(title=f"Расследование — {self.result.seed_name}", width=1400, height=900)
        dpg.setup_dearpygui()
        dpg.show_viewport()
        dpg.set_primary_window("primary_window", True)
        self._auto_layout()
        dpg.start_dearpygui()
        dpg.destroy_context()

    def _create_all_nodes(self):
        for eid, node in self.result.nodes.items():
            self._create_node(node)

    def _create_node(self, node: InvestigationNode):
        tag = f"{NODE_PREFIX}{node.entity_id}"
        color = node.node_type.color_rgb
        header_color = (color[0] // 2 + 30, color[1] // 2 + 30, color[2] // 2 + 30, 200)

        with dpg.node(tag=tag, label=f"{node.short_name} [{node.node_type.label}]",
                       parent="node_editor"):
            dpg.bind_item_theme(tag, self._create_node_theme(color))

            with dpg.node_attribute(tag=f"{ATTR_PREFIX}{node.entity_id}_out",
                                     attribute_type=dpg.mvNode_Attr_Output):
                edges = self.result.edges_for(node.entity_id)
                edge_summary = {}
                for e in edges:
                    rt = e.relation_type
                    edge_summary[rt] = edge_summary.get(rt, 0) + 1
                for rt, count in sorted(edge_summary.items(), key=lambda x: -x[1])[:5]:
                    label = RELATION_LABELS.get(rt, rt)
                    conf_color = self._confidence_color(
                        next((e.confidence for e in edges if e.relation_type == rt), Confidence.UNCONFIRMED)
                    )
                    dpg.add_text(f"{label}: {count}", color=conf_color)

            with dpg.node_attribute(tag=f"{ATTR_PREFIX}{node.entity_id}_in",
                                     attribute_type=dpg.mvNode_Attr_Input):
                if node.entity_type == "person":
                    dp = node.extra.get("deputy_profile", {})
                    if dp.get("faction"):
                        dpg.add_text(f"Фракция: {dp['faction']}", color=(200, 200, 200))
                    if dp.get("region"):
                        dpg.add_text(f"Регион: {dp['region']}", color=(200, 200, 200))
                elif node.entity_type == "organization":
                    inn = node.extra.get("inn")
                    if inn:
                        dpg.add_text(f"ИНН: {inn}", color=(200, 200, 200))

            with dpg.node_attribute(tag=f"{ATTR_PREFIX}{node.entity_id}_btn",
                                     attribute_type=dpg.mvNode_Attr_Static):
                dpg.add_button(label="Подробнее", callback=lambda s, a, u: self._show_detail(node.entity_id))
                if node.entity_id != self.result.seed_entity_id:
                    dpg.add_button(label="Раскрыть связи", callback=lambda s, a, u: self._expand_node(node.entity_id))

    def _create_all_links(self):
        for i, edge in enumerate(self.result.edges):
            self._create_link(edge)

    def _link_tag(self, edge: InvestigationEdge) -> str:
        return f"{LINK_PREFIX}{abs(hash(edge.key))}"

    def _create_link(self, edge: InvestigationEdge):
        from_attr = f"{ATTR_PREFIX}{edge.from_id}_out"
        to_attr = f"{ATTR_PREFIX}{edge.to_id}_in"

        from_node = self.result.nodes.get(edge.from_id)
        to_node = self.result.nodes.get(edge.to_id)
        if not from_node or not to_node:
            return

        tag = self._link_tag(edge)
        if dpg.does_item_exist(tag):
            return

        color = self._confidence_color(edge.confidence)
        try:
            dpg.add_node_link(from_attr, to_attr, tag=tag, parent="node_editor")
            dpg.bind_item_theme(tag, self._create_link_theme(color, edge.confidence.weight))
        except Exception:
            pass

    def _create_node_theme(self, color: tuple):
        with dpg.theme() as theme:
            with dpg.theme_component(dpg.mvNode):
                r, g, b = color
                dpg.add_theme_color(dpg.mvNodeCol_TitleBar, (r, g, b, 220))
                dpg.add_theme_color(dpg.mvNodeCol_Background, (40, 40, 40, 200))
                dpg.add_theme_color(dpg.mvNodeCol_Border, (r, g, b, 150))
                dpg.add_theme_color(dpg.mvNodeCol_BorderHovered, (r, g, b, 255))
                dpg.add_theme_color(dpg.mvNodeCol_BorderSelected, (255, 255, 100, 255))
        return theme

    def _create_link_theme(self, color: tuple, strength: float):
        thickness = max(1, int(strength * 4))
        with dpg.theme() as theme:
            with dpg.theme_component(dpg.mvNodeLink):
                r, g, b = color
                dpg.add_theme_color(dpg.mvLinkCol_Hovered, (255, 255, 100, 255))
                dpg.add_theme_color(dpg.mvLinkCol_Selected, (255, 255, 100, 255))
                dpg.add_theme_color(dpg.mvLinkCol_Base, (r, g, b, 180))
                dpg.add_theme_style(dpg.mvNodeLink_Style_Thickness, thickness)
        return theme

    def _confidence_color(self, confidence: Confidence) -> tuple:
        colors = {
            Confidence.CONFIRMED: (80, 220, 80),
            Confidence.LIKELY: (220, 220, 80),
            Confidence.UNCONFIRMED: (180, 180, 180),
            Confidence.DISPUTED: (220, 80, 80),
        }
        return colors.get(confidence, (180, 180, 180))

    def _auto_layout(self):
        node_ids = list(self.result.nodes.keys())
        if not node_ids:
            return

        hop_groups: Dict[int, List[int]] = {}
        for eid, node in self.result.nodes.items():
            hop_groups.setdefault(node.hop, []).append(eid)

        x_offset = 0
        for hop in sorted(hop_groups.keys()):
            eids = hop_groups[hop]
            y_offset = 0
            for eid in eids:
                tag = f"{NODE_PREFIX}{eid}"
                if dpg.does_item_exist(tag):
                    dpg.set_item_pos(tag, [x_offset, y_offset])
                y_offset += 160
            x_offset += 400

    def _reset_layout(self):
        self._auto_layout()

    def _filter_by_confidence(self, min_confidence: Confidence):
        for edge in self.result.edges:
            tag = self._link_tag(edge)
            if dpg.does_item_exist(tag):
                if edge.confidence >= min_confidence:
                    dpg.show_item(tag)
                else:
                    dpg.hide_item(tag)

    def _show_all_links(self):
        for edge in self.result.edges:
            tag = self._link_tag(edge)
            if dpg.does_item_exist(tag):
                dpg.show_item(tag)

    def _show_detail(self, entity_id: int):
        self._selected_entity = entity_id
        node = self.result.nodes.get(entity_id)
        if not node:
            return

        if dpg.does_item_exist(DETAIL_WINDOW):
            dpg.delete_item(DETAIL_WINDOW)

        with dpg.window(tag=DETAIL_WINDOW, label=node.canonical_name, width=500, height=600,
                         modal=False, no_resize=False):
            dpg.add_text(f"{node.canonical_name}", color=node.node_type.color_rgb)
            dpg.add_text(f"Тип: {node.node_type.label}")
            dpg.add_separator()

            extra = node.extra
            dp = extra.get("deputy_profile")
            if dp:
                for key, val in dp.items():
                    if val and key not in ("entity_id",):
                        label_map = {
                            "full_name": "Имя", "position": "Должность", "faction": "Фракция",
                            "region": "Регион", "committee": "Комитет", "duma_id": "ID Думы",
                            "date_elected": "Дата избрания", "income_latest": "Доход",
                        }
                        dpg.add_text(f"{label_map.get(key, key)}: {val}")

            positions = extra.get("positions", [])
            if positions:
                dpg.add_separator()
                dpg.add_text("Должности:")
                for p in positions[:5]:
                    active = " [Активная]" if p.get("is_active") else ""
                    dpg.add_text(f"  {p.get('position_title', '')} @ {p.get('organization', '')}{active}")

            acc = extra.get("accountability")
            if acc:
                dpg.add_separator()
                dpg.add_text(f"Индекс подотчётности: {acc.get('calculated_score', 'N/A')}")

            generic_fields = {
                "vote_session_id": "Сессия",
                "bill_id": "Bill ID",
                "bill_number": "Номер",
                "bill_title": "Название",
                "bill_status": "Статус",
                "vote_result": "Результат голоса",
                "vote_date": "Дата",
                "vote_stage": "Стадия",
                "session_result": "Итог сессии",
                "sponsor_role": "Роль",
                "source": "Источник",
            }
            generic_rows = []
            for key, label in generic_fields.items():
                value = extra.get(key)
                if value not in (None, "", [], {}, False):
                    generic_rows.append((label, value))
            if generic_rows:
                dpg.add_separator()
                dpg.add_text("Контекст:")
                for label, value in generic_rows:
                    dpg.add_text(f"  {label}: {value}")

            dpg.add_separator()
            dpg.add_text("Связи:")
            edges = self.result.edges_for(entity_id)
            for edge in edges[:15]:
                other_id = edge.to_id if edge.from_id == entity_id else edge.from_id
                other = self.result.nodes.get(other_id)
                name = other.short_name if other else f"ID:{other_id}"
                conf_color = self._confidence_color(edge.confidence)
                dpg.add_text(f"  {edge.confidence.symbol} {edge.label_from(entity_id)} → {name}", color=conf_color)

    def _expand_node(self, entity_id: int):
        if entity_id in self._expanded_nodes:
            return
        self._expanded_nodes.add(entity_id)

        try:
            from .engine import InvestigationEngine
            from config.db_utils import load_settings

            settings = load_settings()
            engine = InvestigationEngine(settings["db_path"])
            old_node_ids = set(self.result.nodes.keys())
            old_edge_keys = set(self.result.edge_keys)
            engine.expand(self.result, entity_id, hops=1)
            engine.close()
        except Exception as e:
            log_msg = f"Expansion failed: {e}"
            if dpg.does_item_exist("status_text"):
                dpg.set_value("status_text", log_msg)
            return

        new_node_ids = set(self.result.nodes.keys()) - old_node_ids
        new_edges = [e for e in self.result.edges if e.key not in old_edge_keys]

        for nid in new_node_ids:
            if nid in self.result.nodes:
                self._create_node(self.result.nodes[nid])

        for edge in new_edges:
            self._create_link(edge)

        self._auto_layout()

    def _show_dossier(self):
        if dpg.does_item_exist(DOSSIER_WINDOW):
            dpg.delete_item(DOSSIER_WINDOW)

        with dpg.window(tag=DOSSIER_WINDOW, label="Досье", width=700, height=700,
                         modal=False, no_resize=False):
            dpg.add_input_text(multiline=True, default_value=self.dossier_text,
                                height=650, width=680, readonly=True)

    def _on_link(self, sender, app_data):
        pass

    def _on_delink(self, sender, app_data):
        pass


def show_investigation(result_json_path: str):
    with open(result_json_path, "r", encoding="utf-8") as f:
        data = f.read()

    result = InvestigationResult.from_json(data)

    from .dossier import DossierGenerator
    dossier_text = DossierGenerator(result).generate()

    viewer = InvestigationGraphViewer(result, dossier_text)
    viewer.show()


def launch_viewer(result: InvestigationResult, dossier_text: str = ""):
    import multiprocessing

    temp_path = Path(__file__).parent / "_temp_investigation.json"
    with open(temp_path, "w", encoding="utf-8") as f:
        f.write(result.to_json())

    p = multiprocessing.Process(
        target=show_investigation,
        args=(str(temp_path),),
        daemon=False,
    )
    p.start()
    return p
