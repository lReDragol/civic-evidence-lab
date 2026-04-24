import json
import sqlite3

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QComboBox, QHBoxLayout, QHeaderView, QLabel, QLineEdit,
    QPushButton, QSplitter, QTableWidget, QTableWidgetItem,
    QTextEdit, QVBoxLayout, QWidget,
)

STRENGTH_COLORS = {
    "strong": "#a6e3a1",
    "moderate": "#f9e2af",
    "weak": "#6c7086",
}


class RelationsTab(QWidget):
    def __init__(self, db: sqlite3.Connection, settings: dict, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings

        layout = QVBoxLayout(self)

        toolbar = QHBoxLayout()
        self._strength_filter = QComboBox()
        self._strength_filter.addItem("Все силы", "")
        for s in ["strong", "moderate", "weak"]:
            self._strength_filter.addItem(s, s)
        self._strength_filter.currentIndexChanged.connect(self._load_relations)
        toolbar.addWidget(QLabel("Сила:"))
        toolbar.addWidget(self._strength_filter)

        self._type_filter = QComboBox()
        self._type_filter.addItem("Все типы", "")
        type_labels = {
            "mentioned_together": "Упомянуты вместе",
            "associated_with_location": "Связь с локацией",
            "located_in": "Находится в",
            "sponsored_bill": "Спонсировал закон",
            "works_at": "Работает в",
            "represents_region": "Представляет регион",
            "party_member": "Член партии",
            "member_of": "Член (орг.)",
            "member_of_committee": "Член комитета",
            "head_of": "Руководитель",
        }
        for r in self.db.execute("SELECT DISTINCT relation_type FROM entity_relations ORDER BY relation_type"):
            label = type_labels.get(r[0], r[0])
            self._type_filter.addItem(label, r[0])
        self._type_filter.currentIndexChanged.connect(self._load_relations)
        toolbar.addWidget(QLabel("Тип:"))
        toolbar.addWidget(self._type_filter)

        self._search_input = QLineEdit()
        self._search_input.setPlaceholderText("Поиск по имени сущности...")
        self._search_input.returnPressed.connect(self._load_relations)
        toolbar.addWidget(self._search_input, stretch=2)

        btn_refresh = QPushButton("Обновить")
        btn_refresh.clicked.connect(self._load_relations)
        toolbar.addWidget(btn_refresh)
        layout.addLayout(toolbar)

        splitter = QSplitter(Qt.Horizontal)

        self._table = QTableWidget(0, 6)
        self._table.setHorizontalHeaderLabels(["Откуда", "Тип", "Куда", "Сила", "Метод", "Типы"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionBehavior(QTableWidget.SelectRows)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.setSortingEnabled(True)
        self._table.currentCellChanged.connect(self._on_row_selected)
        splitter.addWidget(self._table)

        right = QVBoxLayout()
        self._detail_title = QLabel("")
        self._detail_title.setStyleSheet("font-size: 14px; font-weight: bold; color: #cba6f7;")
        self._detail_title.setWordWrap(True)
        right.addWidget(self._detail_title)

        self._detail_info = QTextEdit()
        self._detail_info.setReadOnly(True)
        self._detail_info.setMaximumHeight(300)
        right.addWidget(self._detail_info)

        right_widget = QWidget()
        right_widget.setLayout(right)
        splitter.addWidget(right_widget)

        splitter.setSizes([800, 400])
        layout.addWidget(splitter)

        self._load_relations()

    def _load_relations(self):
        strength = self._strength_filter.currentData() or ""
        rtype = self._type_filter.currentData() or ""
        search = self._search_input.text().strip()

        where_parts = ["1=1"]
        params = []
        if strength:
            where_parts.append("er.strength = ?")
            params.append(strength)
        if rtype:
            where_parts.append("er.relation_type = ?")
            params.append(rtype)
        if search:
            where_parts.append("(e1.canonical_name LIKE ? OR e2.canonical_name LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])

        where = " AND ".join(where_parts)

        rows = self.db.execute(
            f"""
            SELECT e1.canonical_name, er.relation_type, e2.canonical_name,
                   er.strength, er.detected_by, e1.entity_type, e2.entity_type
            FROM entity_relations er
            JOIN entities e1 ON e1.id = er.from_entity_id
            JOIN entities e2 ON e2.id = er.to_entity_id
            WHERE {where}
            ORDER BY
                CASE er.strength
                    WHEN 'strong' THEN 0
                    WHEN 'moderate' THEN 1
                    WHEN 'weak' THEN 2
                END
            LIMIT 300
            """,
            params,
        ).fetchall()

        self._table.setRowCount(len(rows))
        for i, r in enumerate(rows):
            self._table.setItem(i, 0, QTableWidgetItem(str(r[0])))
            self._table.setItem(i, 1, QTableWidgetItem(str(r[1])))

            self._table.setItem(i, 2, QTableWidgetItem(str(r[2])))

            strength_item = QTableWidgetItem(r[3] or "")
            strength_item.setForeground(QColor(STRENGTH_COLORS.get(r[3], "#cdd6f4")))
            self._table.setItem(i, 3, strength_item)

            self._table.setItem(i, 4, QTableWidgetItem(str(r[4] or "")[:30]))
            self._table.setItem(i, 5, QTableWidgetItem(f"{r[5]}->{r[6]}"))

    def _on_row_selected(self, row, col, prev_row, prev_col):
        if row < 0:
            return
        name1_item = self._table.item(row, 0)
        rel_type_item = self._table.item(row, 1)
        name2_item = self._table.item(row, 2)
        if not name1_item or not name2_item:
            return

        n1 = name1_item.text()
        n2 = name2_item.text()
        rt = rel_type_item.text() if rel_type_item else ""

        self._detail_title.setText(f"{n1} —[{rt}]— {n2}")

        e1 = self.db.execute("SELECT * FROM entities WHERE canonical_name = ? AND entity_type = 'person' LIMIT 1", (n1,)).fetchone()
        e2 = self.db.execute("SELECT * FROM entities WHERE canonical_name = ? LIMIT 1", (n2,)).fetchone()

        info_parts = []
        if e1:
            m1 = self.db.execute("SELECT COUNT(*) FROM entity_mentions WHERE entity_id = ?", (e1["id"],)).fetchone()[0]
            info_parts.append(f"Сущность 1: {e1['canonical_name']} [{e1['entity_type']}] — {m1} упоминаний")
            if e1["inn"]:
                info_parts.append(f"  ИНН: {e1['inn']}")
        if e2:
            m2 = self.db.execute("SELECT COUNT(*) FROM entity_mentions WHERE entity_id = ?", (e2["id"],)).fetchone()[0]
            info_parts.append(f"Сущность 2: {e2['canonical_name']} [{e2['entity_type']}] — {m2} упоминаний")
            if e2["inn"]:
                info_parts.append(f"  ИНН: {e2['inn']}")

        all_rels = self.db.execute(
            """
            SELECT er.relation_type, er.strength, er.detected_by
            FROM entity_relations er
            JOIN entities e1 ON e1.id = er.from_entity_id
            JOIN entities e2 ON e2.id = er.to_entity_id
            WHERE e1.canonical_name = ? AND e2.canonical_name = ?
            """,
            (n1, n2),
        ).fetchall()
        if all_rels:
            info_parts.append("\nВсе связи:")
            for r in all_rels:
                info_parts.append(f"  [{r[1]}] {r[0]} ({r[2]})")

        self._detail_info.setPlainText("\n".join(info_parts))
