import sqlite3

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


class ReviewTab(QWidget):
    def __init__(self, db: sqlite3.Connection, settings: dict, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings
        self._results = []

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        toolbar = QHBoxLayout()
        toolbar.setSpacing(8)

        self._mode = QComboBox()
        self._mode.addItem("Заявления на проверку", "claims")
        self._mode.addItem("Риски на проверку", "risks")
        self._mode.addItem("Цитаты с флагом", "quotes")
        self._mode.currentIndexChanged.connect(self._load)
        toolbar.addWidget(self._mode)

        self._level_filter = QComboBox()
        self._level_filter.addItem("Все уровни", "")
        self._level_filter.addItem("critical", "critical")
        self._level_filter.addItem("high", "high")
        self._level_filter.addItem("medium", "medium")
        self._level_filter.currentIndexChanged.connect(self._load)
        toolbar.addWidget(self._level_filter)

        self._type_filter = QComboBox()
        self._type_filter.addItem("Все типы", "")
        self._type_filter.addItem("corruption", "corruption")
        self._type_filter.addItem("rhetoric", "rhetoric")
        self._type_filter.addItem("contradiction", "contradiction")
        self._type_filter.addItem("suppression", "suppression")
        self._type_filter.currentIndexChanged.connect(self._load)
        toolbar.addWidget(self._type_filter)

        refresh_button = QPushButton("Обновить")
        refresh_button.clicked.connect(self._load)
        toolbar.addWidget(refresh_button)
        toolbar.addStretch()
        layout.addLayout(toolbar)

        self._count_label = QLabel("")
        self._count_label.setObjectName("mutedLabel")
        layout.addWidget(self._count_label)

        splitter = QSplitter(Qt.Horizontal)

        self._table = QTableWidget(0, 6)
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionBehavior(QTableWidget.SelectRows)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.setShowGrid(False)
        self._table.setWordWrap(False)
        self._table.verticalHeader().setVisible(False)
        self._table.verticalHeader().setDefaultSectionSize(34)
        self._table.currentCellChanged.connect(self._on_row_selected)
        splitter.addWidget(self._table)

        detail = QWidget()
        detail_layout = QVBoxLayout(detail)
        detail_layout.setContentsMargins(0, 0, 0, 0)
        detail_layout.setSpacing(8)

        detail_title = QLabel("Панель проверки")
        detail_title.setObjectName("sectionLabel")
        detail_layout.addWidget(detail_title)

        self._detail_title = QLabel("Элемент не выбран")
        self._detail_title.setStyleSheet("font-size: 16px; font-weight: 700; color: #eef5ff;")
        self._detail_title.setWordWrap(True)
        detail_layout.addWidget(self._detail_title)

        self._detail_meta = QLabel("После выбора строки здесь появятся контекст и действия по модерации.")
        self._detail_meta.setObjectName("mutedLabel")
        self._detail_meta.setWordWrap(True)
        detail_layout.addWidget(self._detail_meta)

        action_row = QHBoxLayout()
        action_row.setSpacing(8)

        self._btn_approve = QPushButton("Подтвердить")
        self._btn_approve.setObjectName("startBtn")
        self._btn_approve.clicked.connect(self._action_approve)
        action_row.addWidget(self._btn_approve)

        self._btn_reject = QPushButton("Опровергнуть")
        self._btn_reject.setObjectName("stopBtn")
        self._btn_reject.clicked.connect(self._action_reject)
        action_row.addWidget(self._btn_reject)

        self._btn_skip = QPushButton("Пропустить")
        self._btn_skip.clicked.connect(self._action_skip)
        action_row.addWidget(self._btn_skip)
        detail_layout.addLayout(action_row)

        self._detail_body = QTextEdit()
        self._detail_body.setReadOnly(True)
        self._detail_body.setPlaceholderText("Текст заявления или риска появится здесь.")
        detail_layout.addWidget(self._detail_body, stretch=1)

        splitter.addWidget(detail)
        splitter.setSizes([760, 460])
        layout.addWidget(splitter, stretch=1)

        self._load()

    def _load(self):
        mode = self._mode.currentData() or "claims"
        if mode == "claims":
            self._load_claims()
        elif mode == "risks":
            self._load_risks()
        elif mode == "quotes":
            self._load_quotes()

    def _load_claims(self):
        self._table.setColumnCount(6)
        self._table.setHorizontalHeaderLabels(["ID", "Тип", "Уверенность", "Статус", "Контент ID", "Текст заявления"])
        for index, mode in enumerate(
            [QHeaderView.ResizeToContents, QHeaderView.ResizeToContents, QHeaderView.ResizeToContents,
             QHeaderView.ResizeToContents, QHeaderView.ResizeToContents, QHeaderView.Stretch]
        ):
            self._table.horizontalHeader().setSectionResizeMode(index, mode)

        rows = self.db.execute(
            """
            SELECT id, claim_type, confidence_auto, status, content_item_id, claim_text
            FROM claims
            WHERE needs_review = 1
            ORDER BY confidence_auto DESC
            LIMIT 200
            """
        ).fetchall()

        self._results = [
            {
                "id": row[0],
                "claim_type": row[1],
                "confidence_auto": row[2],
                "status": row[3],
                "content_item_id": row[4],
                "claim_text": row[5],
            }
            for row in rows
        ]

        self._table.setRowCount(len(rows))
        for index, row in enumerate(rows):
            self._table.setItem(index, 0, QTableWidgetItem(str(row[0])))
            self._table.setItem(index, 1, QTableWidgetItem(str(row[1] or "")))
            self._table.setItem(index, 2, QTableWidgetItem(f"{row[2]:.2f}" if row[2] else ""))
            self._table.setItem(index, 3, self._status_item(row[3]))
            self._table.setItem(index, 4, QTableWidgetItem(str(row[4] or "")))
            self._table.setItem(index, 5, QTableWidgetItem(str(row[5] or "")[:160]))

        self._count_label.setText(f"Заявлений на ручную проверку: {len(rows)}")
        self._reset_detail("Выберите заявление для проверки.")

    def _load_risks(self):
        level = self._level_filter.currentData() or ""
        pattern_type = self._type_filter.currentData() or ""

        self._table.setColumnCount(6)
        self._table.setHorizontalHeaderLabels(["ID", "Тип", "Уровень", "Дата", "Дело ID", "Описание"])
        for index, mode in enumerate(
            [QHeaderView.ResizeToContents, QHeaderView.ResizeToContents, QHeaderView.ResizeToContents,
             QHeaderView.ResizeToContents, QHeaderView.ResizeToContents, QHeaderView.Stretch]
        ):
            self._table.horizontalHeader().setSectionResizeMode(index, mode)

        where_parts = ["needs_review = 1"]
        params = []
        if level:
            where_parts.append("risk_level = ?")
            params.append(level)
        if pattern_type:
            where_parts.append("pattern_type = ?")
            params.append(pattern_type)

        where_sql = " AND ".join(where_parts)
        rows = self.db.execute(
            f"""
            SELECT id, pattern_type, risk_level, detected_at, case_id, description
            FROM risk_patterns
            WHERE {where_sql}
            ORDER BY CASE risk_level WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END
            LIMIT 200
            """,
            params,
        ).fetchall()

        self._results = [
            {
                "id": row[0],
                "pattern_type": row[1],
                "risk_level": row[2],
                "detected_at": row[3],
                "case_id": row[4],
                "description": row[5],
            }
            for row in rows
        ]

        self._table.setRowCount(len(rows))
        for index, row in enumerate(rows):
            self._table.setItem(index, 0, QTableWidgetItem(str(row[0])))
            self._table.setItem(index, 1, QTableWidgetItem(str(row[1] or "")))
            self._table.setItem(index, 2, self._level_item(row[2]))
            self._table.setItem(index, 3, QTableWidgetItem(str(row[3] or "")[:16]))
            self._table.setItem(index, 4, QTableWidgetItem(str(row[4] or "")))
            self._table.setItem(index, 5, QTableWidgetItem(str(row[5] or "")[:160]))

        self._count_label.setText(f"Рисков на ручную проверку: {len(rows)}")
        self._reset_detail("Выберите риск для проверки.")

    def _load_quotes(self):
        self._table.setColumnCount(6)
        self._table.setHorizontalHeaderLabels(["ID", "Сущность ID", "Риторика", "Флаг", "Контент ID", "Цитата"])
        for index, mode in enumerate(
            [QHeaderView.ResizeToContents, QHeaderView.ResizeToContents, QHeaderView.ResizeToContents,
             QHeaderView.ResizeToContents, QHeaderView.ResizeToContents, QHeaderView.Stretch]
        ):
            self._table.horizontalHeader().setSectionResizeMode(index, mode)

        rows = self.db.execute(
            """
            SELECT id, entity_id, rhetoric_class, is_flagged, content_item_id, quote_text
            FROM quotes
            WHERE is_flagged = 1
            ORDER BY id DESC
            LIMIT 200
            """
        ).fetchall()

        self._results = [
            {
                "id": row[0],
                "entity_id": row[1],
                "rhetoric_class": row[2],
                "is_flagged": row[3],
                "content_item_id": row[4],
                "quote_text": row[5],
            }
            for row in rows
        ]

        self._table.setRowCount(len(rows))
        for index, row in enumerate(rows):
            self._table.setItem(index, 0, QTableWidgetItem(str(row[0])))
            self._table.setItem(index, 1, QTableWidgetItem(str(row[1] or "")))
            self._table.setItem(index, 2, QTableWidgetItem(str(row[2] or "")))
            self._table.setItem(index, 3, QTableWidgetItem("⚠" if row[3] else ""))
            self._table.setItem(index, 4, QTableWidgetItem(str(row[4] or "")))
            self._table.setItem(index, 5, QTableWidgetItem(str(row[5] or "")[:160]))

        self._count_label.setText(f"Цитат с флагом: {len(rows)}")
        self._reset_detail("Выберите цитату для проверки.")

    def _reset_detail(self, message: str):
        self._detail_title.setText("Элемент не выбран")
        self._detail_meta.setText(message)
        self._detail_body.clear()

    def _on_row_selected(self, row, col, prev_row, prev_col):
        if row < 0 or row >= len(self._results):
            return
        result = self._results[row]
        mode = self._mode.currentData() or "claims"

        if mode == "claims":
            self._detail_title.setText(result.get("claim_text", "")[:220] or "Заявление")
            self._detail_body.setPlainText(result.get("claim_text", ""))

            content_id = result.get("content_item_id")
            if content_id:
                content = self.db.execute(
                    "SELECT title, body_text FROM content_items WHERE id = ?", (content_id,)
                ).fetchone()
                if content:
                    body = self._detail_body.toPlainText()
                    body += f"\n\n--- Связанный материал [{content_id}] ---\n{content[0] or ''}\n{(content[1] or '')[:700]}"
                    self._detail_body.setPlainText(body)

            self._detail_meta.setText(
                " | ".join(
                    [
                        f"ID: {result.get('id')}",
                        f"Тип: {result.get('claim_type')}",
                        f"Уверенность: {result.get('confidence_auto', 0):.2f}",
                        f"Статус: {result.get('status')}",
                    ]
                )
            )
        elif mode == "risks":
            self._detail_title.setText(result.get("description", "")[:220] or "Риск")
            self._detail_body.setPlainText(result.get("description", ""))
            self._detail_meta.setText(
                " | ".join(
                    [
                        f"ID: {result.get('id')}",
                        f"Тип: {result.get('pattern_type')}",
                        f"Уровень: {result.get('risk_level')}",
                        f"Дело: {result.get('case_id') or '—'}",
                    ]
                )
            )
        elif mode == "quotes":
            self._detail_title.setText("Цитата с флагом")
            self._detail_body.setPlainText(result.get("quote_text", ""))
            self._detail_meta.setText(
                " | ".join(
                    [
                        f"ID: {result.get('id')}",
                        f"Риторика: {result.get('rhetoric_class')}",
                        f"Сущность: {result.get('entity_id')}",
                        f"Контент: {result.get('content_item_id')}",
                    ]
                )
            )

    def _action_approve(self):
        self._set_review_status("verified")

    def _action_reject(self):
        self._set_review_status("disproven")

    def _action_skip(self):
        self._set_review_status("unverified")

    def _set_review_status(self, status: str):
        row = self._table.currentRow()
        if row < 0 or row >= len(self._results):
            return

        result = self._results[row]
        mode = self._mode.currentData() or "claims"
        item_id = result.get("id")

        try:
            if mode == "claims":
                self.db.execute(
                    "UPDATE claims SET needs_review = 0, status = ?, reviewed_at = datetime('now') WHERE id = ?",
                    (status, item_id),
                )
            elif mode == "risks":
                self.db.execute("UPDATE risk_patterns SET needs_review = 0 WHERE id = ?", (item_id,))
            elif mode == "quotes":
                self.db.execute("UPDATE quotes SET is_flagged = 0 WHERE id = ?", (item_id,))
            self.db.commit()
        except Exception as error:
            import logging

            logging.getLogger(__name__).error("Review action failed: %s", error)
            return

        self._results.pop(row)
        self._table.removeRow(row)
        prefix = self._count_label.text().split(":")[0]
        self._count_label.setText(f"{prefix}: {len(self._results)}")
        self._reset_detail("Элемент обновлён. Выберите следующий.")

    def _status_item(self, status: str) -> QTableWidgetItem:
        colors = {
            "verified": "#8fe0b0",
            "disproven": "#f2a8b6",
            "unverified": "#ead18a",
            "partially_verified": "#f0ba8d",
            "raw_signal": "#8ebfff",
        }
        item = QTableWidgetItem(status or "")
        item.setForeground(QColor(colors.get(status, "#d9e2f2")))
        return item

    def _level_item(self, level: str) -> QTableWidgetItem:
        colors = {
            "critical": "#f2a8b6",
            "high": "#f0ba8d",
            "medium": "#ead18a",
            "low": "#8fe0b0",
        }
        item = QTableWidgetItem(level or "")
        item.setForeground(QColor(colors.get(level, "#d9e2f2")))
        return item
