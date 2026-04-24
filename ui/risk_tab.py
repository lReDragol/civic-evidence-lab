import json
import sqlite3

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QComboBox, QHBoxLayout, QHeaderView, QLabel, QLineEdit,
    QPushButton, QSplitter, QTableWidget, QTableWidgetItem,
    QTextEdit, QVBoxLayout, QWidget,
)

RISK_COLORS = {
    "critical": "#f38ba8",
    "high": "#fab387",
    "medium": "#f9e2af",
    "low": "#a6e3a1",
}

RISK_LABELS = {
    "corruption_risk": "Коррупция",
    "rhetoric_risk": "Риторика давления",
    "contradiction_risk": "Противоречия",
    "suppression_risk": "Подавление/Цензура",
    "procurement_risk": "Закупки",
    "conflict_of_interest": "Конфликт интересов",
}


class RiskTab(QWidget):
    def __init__(self, db: sqlite3.Connection, settings: dict, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings

        layout = QVBoxLayout(self)

        toolbar = QHBoxLayout()
        self._type_filter = QComboBox()
        self._type_filter.addItem("Все типы", "")
        for t in self.db.execute("SELECT DISTINCT pattern_type FROM risk_patterns ORDER BY pattern_type"):
            label = RISK_LABELS.get(t[0], t[0])
            self._type_filter.addItem(label, t[0])
        self._type_filter.currentIndexChanged.connect(self._load_patterns)
        toolbar.addWidget(QLabel("Тип:"))
        toolbar.addWidget(self._type_filter)

        self._risk_filter = QComboBox()
        self._risk_filter.addItem("Все уровни", "")
        risk_labels = {"critical": "Критический", "high": "Высокий", "medium": "Средний", "low": "Низкий"}
        for r in self.db.execute("SELECT DISTINCT risk_level FROM risk_patterns WHERE risk_level IS NOT NULL ORDER BY risk_level"):
            label = risk_labels.get(r[0], r[0])
            self._risk_filter.addItem(label, r[0])
        self._risk_filter.currentIndexChanged.connect(self._load_patterns)
        toolbar.addWidget(QLabel("Риск:"))
        toolbar.addWidget(self._risk_filter)

        self._search_input = QLineEdit()
        self._search_input.setPlaceholderText("Поиск по описанию...")
        self._search_input.returnPressed.connect(self._load_patterns)
        toolbar.addWidget(self._search_input, stretch=2)

        btn_refresh = QPushButton("Обновить")
        btn_refresh.clicked.connect(self._load_patterns)
        toolbar.addWidget(btn_refresh)
        layout.addLayout(toolbar)

        stats_bar = QHBoxLayout()
        self._stats_label = QLabel("")
        self._stats_label.setStyleSheet("font-size: 12px; color: #a6adc8; padding: 4px;")
        stats_bar.addWidget(self._stats_label)
        stats_bar.addStretch()
        layout.addLayout(stats_bar)

        splitter = QSplitter(Qt.Horizontal)

        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(["ID", "Тип", "Уровень", "Сущности", "Описание"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionBehavior(QTableWidget.SelectRows)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.setSortingEnabled(True)
        self._table.currentCellChanged.connect(self._on_row_selected)
        splitter.addWidget(self._table)

        right = QVBoxLayout()
        self._detail_title = QLabel("")
        self._detail_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #f38ba8;")
        self._detail_title.setWordWrap(True)
        right.addWidget(self._detail_title)

        self._detail_info = QTextEdit()
        self._detail_info.setReadOnly(True)
        self._detail_info.setMaximumHeight(150)
        right.addWidget(self._detail_info)

        self._detail_entities = QTextEdit()
        self._detail_entities.setReadOnly(True)
        self._detail_entities.setMaximumHeight(200)
        right.addWidget(QLabel("Сущности:"))
        right.addWidget(self._detail_entities)

        self._detail_evidence = QTextEdit()
        self._detail_evidence.setReadOnly(True)
        self._detail_evidence.setMaximumHeight(200)
        right.addWidget(QLabel("Свидетельства:"))
        right.addWidget(self._detail_evidence)

        actions = QHBoxLayout()
        btn_dismiss = QPushButton("Отклонить")
        btn_dismiss.clicked.connect(lambda: self._set_review(False))
        actions.addWidget(btn_dismiss)
        btn_confirm = QPushButton("Подтвердить")
        btn_confirm.setStyleSheet("QPushButton { background-color: #5b3d3d; } QPushButton:hover { background-color: #7b5d5d; }")
        btn_confirm.clicked.connect(lambda: self._set_review(True))
        actions.addWidget(btn_confirm)
        right.addLayout(actions)

        right_widget = QWidget()
        right_widget.setLayout(right)
        splitter.addWidget(right_widget)

        splitter.setSizes([600, 600])
        layout.addWidget(splitter)

        self._current_pattern_id = None
        self._load_patterns()

    def _load_patterns(self):
        ptype = self._type_filter.currentData() or ""
        risk = self._risk_filter.currentData() or ""
        search = self._search_input.text().strip()

        where_parts = ["1=1"]
        params = []
        if ptype:
            where_parts.append("rp.pattern_type = ?")
            params.append(ptype)
        if risk:
            where_parts.append("rp.risk_level = ?")
            params.append(risk)
        if search:
            where_parts.append("rp.description LIKE ?")
            params.append(f"%{search}%")

        where = " AND ".join(where_parts)

        rows = self.db.execute(
            f"""
            SELECT rp.id, rp.pattern_type, rp.risk_level, rp.entity_ids, rp.description
            FROM risk_patterns rp
            WHERE {where}
            ORDER BY
                CASE rp.risk_level
                    WHEN 'critical' THEN 0
                    WHEN 'high' THEN 1
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 3
                END,
                rp.id DESC
            LIMIT 200
            """,
            params,
        ).fetchall()

        self._table.setRowCount(len(rows))
        for i, r in enumerate(rows):
            self._table.setItem(i, 0, QTableWidgetItem(str(r[0])))
            type_label = RISK_LABELS.get(r[1], r[1])
            self._table.setItem(i, 1, QTableWidgetItem(type_label))

            risk_item = QTableWidgetItem(r[2] or "")
            risk_item.setForeground(QColor(RISK_COLORS.get(r[2], "#cdd6f4")))
            self._table.setItem(i, 2, risk_item)

            try:
                eids = json.loads(r[3]) if r[3] else []
                entity_count = len(eids)
                self._table.setItem(i, 3, QTableWidgetItem(str(entity_count)))
            except Exception:
                self._table.setItem(i, 3, QTableWidgetItem("?"))

            self._table.setItem(i, 4, QTableWidgetItem(str(r[4] or "")[:120]))

        total = self.db.execute(f"SELECT COUNT(*) FROM risk_patterns WHERE {where}", params).fetchone()[0]
        critical = self.db.execute("SELECT COUNT(*) FROM risk_patterns WHERE risk_level = 'critical'").fetchone()[0]
        high = self.db.execute("SELECT COUNT(*) FROM risk_patterns WHERE risk_level = 'high'").fetchone()[0]
        self._stats_label.setText(
            f"Всего: {total} | Критических: {critical} | Высоких: {high}"
        )

    def _on_row_selected(self, row, col, prev_row, prev_col):
        if row < 0:
            return
        item = self._table.item(row, 0)
        if not item:
            return
        pid = int(item.text())
        self._current_pattern_id = pid

        pattern = self.db.execute("SELECT * FROM risk_patterns WHERE id = ?", (pid,)).fetchone()
        if not pattern:
            return

        self._detail_title.setText(
            f"{RISK_LABELS.get(pattern['pattern_type'], pattern['pattern_type'])} — {pattern['risk_level']}"
        )

        info_parts = [
            f"Тип: {RISK_LABELS.get(pattern['pattern_type'], pattern['pattern_type'])}",
            f"Уровень риска: {pattern['risk_level']}",
            f"Требует проверки: {'Да' if pattern['needs_review'] else 'Нет'}",
            f"Обнаружено: {pattern['detected_at']}",
        ]
        if pattern["case_id"]:
            case = self.db.execute("SELECT title FROM cases WHERE id = ?", (pattern["case_id"],)).fetchone()
            if case:
                info_parts.append(f"Связанное дело: {case[0]}")
        self._detail_info.setPlainText("\n".join(info_parts))

        try:
            eids = json.loads(pattern["entity_ids"]) if pattern["entity_ids"] else []
        except Exception:
            eids = []

        if eids:
            ph = ",".join("?" * len(eids))
            entities = self.db.execute(
                f"SELECT id, canonical_name, entity_type FROM entities WHERE id IN ({ph})",
                eids,
            ).fetchall()
            lines = []
            for e in entities:
                mcount = self.db.execute(
                    "SELECT COUNT(*) FROM entity_mentions WHERE entity_id = ?", (e[0],)
                ).fetchone()[0]
                lines.append(f"[{e[2]}] {e[1]} ({mcount} упоминаний)")
            self._detail_entities.setPlainText("\n".join(lines))
        else:
            self._detail_entities.setPlainText("Нет сущностей")

        try:
            evidence_ids = json.loads(pattern["evidence_ids"]) if pattern["evidence_ids"] else []
        except Exception:
            evidence_ids = []

        if evidence_ids:
            ph = ",".join("?" * min(len(evidence_ids), 20))
            items = self.db.execute(
                f"SELECT id, title, content_type FROM content_items WHERE id IN ({ph})",
                evidence_ids[:20],
            ).fetchall()
            lines = [f"Показано {min(len(evidence_ids), 20)} из {len(evidence_ids)}:"]
            for it in items:
                lines.append(f"  [{it[2]}] {it[1] or '?'}")
            self._detail_evidence.setPlainText("\n".join(lines))
        else:
            self._detail_evidence.setPlainText("Нет свидетельств")

    def _set_review(self, confirmed: bool):
        if self._current_pattern_id is None:
            return
        self.db.execute(
            "UPDATE risk_patterns SET needs_review = 0 WHERE id = ?",
            (self._current_pattern_id,),
        )
        self.db.commit()
        self._load_patterns()
