import sqlite3

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QComboBox, QHBoxLayout, QHeaderView, QLabel, QLineEdit,
    QPushButton, QSplitter, QTableWidget, QTableWidgetItem,
    QTextEdit, QVBoxLayout, QWidget,
)


class CasesTab(QWidget):
    def __init__(self, db: sqlite3.Connection, settings: dict, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings

        layout = QVBoxLayout(self)

        toolbar = QHBoxLayout()
        self._type_filter = QComboBox()
        self._type_filter.addItem("Все типы", "")
        for t in ["entity_group", "topic_group", "corruption_risk", "contradiction"]:
            self._type_filter.addItem(t, t)
        self._type_filter.currentIndexChanged.connect(self._load_cases)
        toolbar.addWidget(QLabel("Тип дела:"))
        toolbar.addWidget(self._type_filter)

        self._search_input = QLineEdit()
        self._search_input.setPlaceholderText("Поиск по названию...")
        self._search_input.returnPressed.connect(self._load_cases)
        toolbar.addWidget(self._search_input, stretch=2)

        btn_refresh = QPushButton("Обновить")
        btn_refresh.clicked.connect(self._load_cases)
        toolbar.addWidget(btn_refresh)
        layout.addLayout(toolbar)

        splitter = QSplitter(Qt.Horizontal)

        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(["ID", "Название", "Тип", "Статус", "Заявлений"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionBehavior(QTableWidget.SelectRows)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.setSortingEnabled(True)
        self._table.currentCellChanged.connect(self._on_row_selected)
        splitter.addWidget(self._table)

        right = QVBoxLayout()
        self._detail_title = QLabel("")
        self._detail_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #cba6f7;")
        self._detail_title.setWordWrap(True)
        right.addWidget(self._detail_title)

        self._detail_info = QTextEdit()
        self._detail_info.setReadOnly(True)
        self._detail_info.setMaximumHeight(100)
        right.addWidget(self._detail_info)

        self._detail_claims = QTextEdit()
        self._detail_claims.setReadOnly(True)
        self._detail_claims.setMaximumHeight(200)
        right.addWidget(QLabel("Связанные заявления:"))
        right.addWidget(self._detail_claims)

        right.addWidget(QLabel("Хронология (события дела):"))
        self._detail_timeline = QTextEdit()
        self._detail_timeline.setReadOnly(True)
        self._detail_timeline.setMaximumHeight(150)
        right.addWidget(self._detail_timeline)

        right.addWidget(QLabel("Связанные законы / голосования:"))
        self._detail_laws = QTextEdit()
        self._detail_laws.setReadOnly(True)
        self._detail_laws.setMaximumHeight(120)
        right.addWidget(self._detail_laws)

        right.addWidget(QLabel("Участники кейса:"))
        self._detail_participants = QTextEdit()
        self._detail_participants.setReadOnly(True)
        self._detail_participants.setMaximumHeight(120)
        right.addWidget(self._detail_participants)

        right_widget = QWidget()
        right_widget.setLayout(right)
        splitter.addWidget(right_widget)

        splitter.setSizes([600, 600])
        layout.addWidget(splitter)

        self._load_cases()

    def _load_cases(self):
        ctype = self._type_filter.currentData() or ""
        search = self._search_input.text().strip()

        where_parts = ["1=1"]
        params = []
        if ctype:
            where_parts.append("c.case_type = ?")
            params.append(ctype)
        if search:
            where_parts.append("c.title LIKE ?")
            params.append(f"%{search}%")

        where = " AND ".join(where_parts)

        rows = self.db.execute(
            f"""
            SELECT c.id, c.title, c.case_type, c.status,
                   (SELECT COUNT(*) FROM case_claims WHERE case_id = c.id)
            FROM cases c
            WHERE {where}
            ORDER BY c.updated_at DESC
            LIMIT 200
            """,
            params,
        ).fetchall()

        self._table.setRowCount(len(rows))
        for i, r in enumerate(rows):
            self._table.setItem(i, 0, QTableWidgetItem(str(r[0])))
            self._table.setItem(i, 1, QTableWidgetItem(str(r[1] or "")[:100]))
            self._table.setItem(i, 2, QTableWidgetItem(str(r[2] or "")))
            self._table.setItem(i, 3, QTableWidgetItem(str(r[3] or "")))
            self._table.setItem(i, 4, QTableWidgetItem(str(r[4])))

    def _on_row_selected(self, row, col, prev_row, prev_col):
        if row < 0:
            return
        item = self._table.item(row, 0)
        if not item:
            return
        case_id = int(item.text())

        case = self.db.execute(
            "SELECT * FROM cases WHERE id = ?", (case_id,)
        ).fetchone()
        if not case:
            return

        self._detail_title.setText(case["title"] or "")
        info = [
            f"Тип: {case['case_type'] or '?'}",
            f"Статус: {case['status'] or '?'}",
            f"Регион: {case['region'] or '?'}",
            f"Начало: {case['started_at'] or '?'}",
            f"Обновлено: {case['updated_at'] or '?'}",
        ]
        if case["description"]:
            info.append(f"\n{case['description'][:300]}")
        self._detail_info.setPlainText("\n".join(info))

        claims = self.db.execute(
            """
            SELECT cl.claim_text, cl.claim_type, cl.status, cc.role
            FROM case_claims cc
            JOIN claims cl ON cl.id = cc.claim_id
            WHERE cc.case_id = ?
            ORDER BY cl.id DESC
            LIMIT 30
            """,
            (case_id,),
        ).fetchall()
        if claims:
            lines = []
            for c in claims:
                role = f"[{c[3]}]" if c[3] else ""
                lines.append(f"{role}[{c[1]}/{c[2]}] {c[0][:150]}")
            self._detail_claims.setPlainText("\n".join(lines))
        else:
            self._detail_claims.setPlainText("Нет заявлений")

        events = self.db.execute(
            """
            SELECT event_date, event_title, event_description
            FROM case_events
            WHERE case_id = ?
            ORDER BY event_date ASC, event_order ASC
            """,
            (case_id,),
        ).fetchall()
        if events:
            tl_lines = []
            for ev in events:
                desc = f": {ev[2][:80]}" if ev[2] else ""
                tl_lines.append(f"  {ev[0]} — {ev[1]}{desc}")
            self._detail_timeline.setPlainText("\n".join(tl_lines))
        else:
            self._detail_timeline.setPlainText("Событий нет.")

        law_refs = self.db.execute(
            """
            SELECT DISTINCT lr.law_type, lr.law_number, lr.article
            FROM law_references lr
            WHERE lr.content_item_id IN (
                SELECT cl.content_item_id FROM claims cl
                JOIN case_claims cc ON cc.claim_id = cl.id
                WHERE cc.case_id = ?
            )
            ORDER BY lr.law_type, lr.law_number
            LIMIT 20
            """,
            (case_id,),
        ).fetchall()
        bill_refs = self.db.execute(
            """
            SELECT DISTINCT b.number, b.title, b.status
            FROM bill_sponsors bs
            JOIN bills b ON b.id = bs.bill_id
            WHERE bs.entity_id IN (
                SELECT DISTINCT em.entity_id FROM entity_mentions em
                WHERE em.content_item_id IN (
                    SELECT cl.content_item_id FROM claims cl
                    JOIN case_claims cc ON cc.claim_id = cl.id
                    WHERE cc.case_id = ?
                )
            )
            ORDER BY b.registration_date DESC
            LIMIT 15
            """,
            (case_id,),
        ).fetchall()
        law_lines = []
        if law_refs:
            law_lines.append("Упомянутые статьи:")
            for lr in law_refs:
                art = f" {lr[2]}" if lr[2] else ""
                law_lines.append(f"  {lr[0]} {lr[1] or ''}{art}")
        if bill_refs:
            law_lines.append("\nЗаконопроекты участников:")
            for br in bill_refs:
                law_lines.append(f"  {br[0]} [{br[2] or '?'}] {br[1][:60] if br[1] else ''}")
        self._detail_laws.setPlainText("\n".join(law_lines) if law_lines else "Связанных законов нет.")

        participants = self.db.execute(
            """
            SELECT DISTINCT e.id, e.canonical_name, e.entity_type
            FROM entities e
            JOIN entity_mentions em ON em.entity_id = e.id
            WHERE em.content_item_id IN (
                SELECT cl.content_item_id FROM claims cl
                JOIN case_claims cc ON cc.claim_id = cl.id
                WHERE cc.case_id = ?
            )
            ORDER BY e.canonical_name
            LIMIT 30
            """,
            (case_id,),
        ).fetchall()
        if participants:
            self._detail_participants.setPlainText(
                "\n".join(f"  [{p[0]}] {p[1]} ({p[2]})" for p in participants)
            )
        else:
            self._detail_participants.setPlainText("Участники не определены.")
