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


class ClaimsTab(QWidget):
    def __init__(self, db: sqlite3.Connection, settings: dict, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings
        self._current_claim_id = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        toolbar = QHBoxLayout()
        toolbar.setSpacing(8)

        self._status_filter = QComboBox()
        self._status_filter.addItem("Все статусы", "")
        for status in ["unverified", "verified", "disproven", "partially_verified"]:
            self._status_filter.addItem(status, status)
        self._status_filter.currentIndexChanged.connect(self._load_claims)
        toolbar.addWidget(self._status_filter)

        self._type_filter = QComboBox()
        self._type_filter.addItem("Все типы", "")
        for claim_type in [
            "public_statement",
            "censorship_action",
            "mobilization_claim",
            "court_decision",
            "detention",
            "ownership_claim",
            "vote_record",
        ]:
            self._type_filter.addItem(claim_type, claim_type)
        self._type_filter.currentIndexChanged.connect(self._load_claims)
        toolbar.addWidget(self._type_filter)

        self._review_filter = QComboBox()
        self._review_filter.addItem("Все записи", "")
        self._review_filter.addItem("Требует проверки", "1")
        self._review_filter.addItem("Проверено", "0")
        self._review_filter.currentIndexChanged.connect(self._load_claims)
        toolbar.addWidget(self._review_filter)

        refresh_button = QPushButton("Обновить")
        refresh_button.clicked.connect(self._load_claims)
        toolbar.addWidget(refresh_button)
        toolbar.addStretch()
        layout.addLayout(toolbar)

        self._summary_label = QLabel("")
        self._summary_label.setObjectName("mutedLabel")
        layout.addWidget(self._summary_label)

        splitter = QSplitter(Qt.Horizontal)

        self._table = QTableWidget(0, 7)
        self._table.setHorizontalHeaderLabels(["ID", "Контент", "Тип", "Статус", "Увер.", "Риск", "Текст"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(6, QHeaderView.Stretch)
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

        detail_header = QLabel("Карточка заявления")
        detail_header.setObjectName("sectionLabel")
        detail_layout.addWidget(detail_header)

        self._detail_text = QTextEdit()
        self._detail_text.setReadOnly(True)
        self._detail_text.setPlaceholderText("Описание выбранного заявления появится здесь.")
        detail_layout.addWidget(self._detail_text, stretch=1)

        evidence_label = QLabel("Свидетельства")
        evidence_label.setObjectName("sectionLabel")
        detail_layout.addWidget(evidence_label)

        self._evidence_text = QTextEdit()
        self._evidence_text.setReadOnly(True)
        self._evidence_text.setMinimumHeight(140)
        detail_layout.addWidget(self._evidence_text)

        actions = QHBoxLayout()
        actions.setSpacing(8)

        self._btn_verified = QPushButton("Подтверждено")
        self._btn_verified.setObjectName("startBtn")
        self._btn_verified.clicked.connect(lambda: self._set_status("verified"))
        actions.addWidget(self._btn_verified)

        self._btn_disproven = QPushButton("Опровергнуто")
        self._btn_disproven.setObjectName("stopBtn")
        self._btn_disproven.clicked.connect(lambda: self._set_status("disproven"))
        actions.addWidget(self._btn_disproven)

        self._btn_partial = QPushButton("Частично")
        self._btn_partial.clicked.connect(lambda: self._set_status("partially_verified"))
        actions.addWidget(self._btn_partial)
        detail_layout.addLayout(actions)

        splitter.addWidget(detail)
        splitter.setSizes([760, 480])
        layout.addWidget(splitter, stretch=1)

        self._load_claims()

    def _load_claims(self):
        status = self._status_filter.currentData() or ""
        claim_type = self._type_filter.currentData() or ""
        review = self._review_filter.currentData() or ""

        where_parts = ["1=1"]
        params = []
        if status:
            where_parts.append("c.status = ?")
            params.append(status)
        if claim_type:
            where_parts.append("c.claim_type = ?")
            params.append(claim_type)
        if review:
            where_parts.append("c.needs_review = ?")
            params.append(int(review))

        where_sql = " AND ".join(where_parts)
        rows = self.db.execute(
            f"""
            SELECT c.id, c.content_item_id, c.claim_type, c.status,
                   c.confidence_auto, c.manipulation_risk, c.claim_text
            FROM claims c
            WHERE {where_sql}
            ORDER BY c.needs_review DESC, c.id DESC
            LIMIT 200
            """,
            params,
        ).fetchall()

        self._table.setRowCount(len(rows))
        for index, row in enumerate(rows):
            self._table.setItem(index, 0, QTableWidgetItem(str(row[0])))
            self._table.setItem(index, 1, QTableWidgetItem(str(row[1])))
            self._table.setItem(index, 2, QTableWidgetItem(str(row[2] or "")))
            self._table.setItem(index, 3, self._status_item(row[3]))
            self._table.setItem(index, 4, QTableWidgetItem(f"{row[4]:.2f}" if row[4] else ""))
            self._table.setItem(index, 5, QTableWidgetItem(f"{row[5]:.2f}" if row[5] else ""))
            self._table.setItem(index, 6, QTableWidgetItem(str(row[6] or "")[:160]))

        self._summary_label.setText(f"Показано {len(rows)} заявлений по текущим фильтрам.")
        self._current_claim_id = None
        self._detail_text.clear()
        self._evidence_text.clear()

    def _status_item(self, status: str) -> QTableWidgetItem:
        colors = {
            "verified": "#8fe0b0",
            "disproven": "#f2a8b6",
            "unverified": "#ead18a",
            "partially_verified": "#f0ba8d",
        }
        item = QTableWidgetItem(status or "")
        item.setForeground(QColor(colors.get(status, "#d9e2f2")))
        return item

    def _on_row_selected(self, row, col, prev_row, prev_col):
        if row < 0:
            return
        item = self._table.item(row, 0)
        if not item:
            return
        claim_id = int(item.text())
        self._current_claim_id = claim_id

        claim = self.db.execute(
            """
            SELECT claim_text, claim_type, status, confidence_auto, manipulation_risk, needs_review
            FROM claims
            WHERE id = ?
            """,
            (claim_id,),
        ).fetchone()
        if claim:
            self._detail_text.setPlainText(
                f"Тип: {claim[1]}\nСтатус: {claim[2]}\nУверенность: {claim[3]}\n"
                f"Риск манипуляции: {claim[4]}\nТребует проверки: {'Да' if claim[5] else 'Нет'}\n\n{claim[0]}"
            )

        evidence_rows = self.db.execute(
            """
            SELECT el.evidence_type, el.strength, el.notes, ci.title
            FROM evidence_links el
            LEFT JOIN content_items ci ON ci.id = el.evidence_item_id
            WHERE el.claim_id = ?
            """,
            (claim_id,),
        ).fetchall()
        if evidence_rows:
            lines = []
            for evidence in evidence_rows:
                lines.append(
                    f"[{evidence[0]}] сила {evidence[1]} | {evidence[3] or '?'}\n{evidence[2] or ''}"
                )
            self._evidence_text.setPlainText("\n\n".join(lines))
        else:
            self._evidence_text.setPlainText("Нет привязанных свидетельств.")

    def _set_status(self, status: str):
        if self._current_claim_id is None:
            return
        old_status_row = self.db.execute(
            "SELECT status FROM claims WHERE id = ?",
            (self._current_claim_id,),
        ).fetchone()
        old_status = old_status_row[0] if old_status_row else None
        self.db.execute(
            "UPDATE claims SET status = ?, needs_review = 0, reviewed_at = datetime('now') WHERE id = ?",
            (status, self._current_claim_id),
        )
        self.db.execute(
            """
            INSERT INTO verifications(claim_id, verifier_type, old_status, new_status, verified_by)
            VALUES(?, 'editor', ?, ?, 'editor')
            """,
            (self._current_claim_id, old_status, status),
        )
        self.db.commit()
        self._load_claims()
