import json
import sqlite3

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


class BillsTab(QWidget):
    def __init__(self, db: sqlite3.Connection, settings: dict, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        toolbar = QHBoxLayout()
        toolbar.setSpacing(8)

        self._status_filter = QComboBox()
        self._status_filter.addItem("Все статусы", "")
        for s in [r[0] for r in self.db.execute("SELECT DISTINCT status FROM bills WHERE status IS NOT NULL AND status != '' ORDER BY status").fetchall()]:
            self._status_filter.addItem(s, s)
        self._status_filter.currentIndexChanged.connect(self._load_bills)
        toolbar.addWidget(QLabel("Статус:"))
        toolbar.addWidget(self._status_filter)

        self._type_filter = QComboBox()
        self._type_filter.addItem("Все типы", "")
        for t in [r[0] for r in self.db.execute("SELECT DISTINCT bill_type FROM bills WHERE bill_type IS NOT NULL AND bill_type != '' ORDER BY bill_type").fetchall()]:
            self._type_filter.addItem(t, t)
        self._type_filter.currentIndexChanged.connect(self._load_bills)
        toolbar.addWidget(QLabel("Тип:"))
        toolbar.addWidget(self._type_filter)

        self._search_input = QLineEdit()
        self._search_input.setPlaceholderText("Поиск по номеру/названию...")
        self._search_input.returnPressed.connect(self._load_bills)
        toolbar.addWidget(self._search_input, stretch=2)

        btn_refresh = QPushButton("Обновить")
        btn_refresh.clicked.connect(self._load_bills)
        toolbar.addWidget(btn_refresh)
        layout.addLayout(toolbar)

        self._summary_label = QLabel("")
        self._summary_label.setObjectName("mutedLabel")
        layout.addWidget(self._summary_label)

        splitter = QSplitter(Qt.Horizontal)

        self._table = QTableWidget(0, 7)
        self._table.setHorizontalHeaderLabels(["ID", "Номер", "Название", "Тип", "Статус", "Дата рег.", "Спонсоры"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeToContents)
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionBehavior(QTableWidget.SelectRows)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.setSortingEnabled(True)
        self._table.setShowGrid(False)
        self._table.setWordWrap(False)
        self._table.verticalHeader().setVisible(False)
        self._table.verticalHeader().setDefaultSectionSize(34)
        self._table.currentCellChanged.connect(self._on_row_selected)
        splitter.addWidget(self._table)

        detail = QWidget()
        detail_layout = QVBoxLayout(detail)
        detail_layout.setContentsMargins(8, 0, 0, 0)
        detail_layout.setSpacing(8)

        self._detail_title = QLabel("Закон не выбран")
        self._detail_title.setStyleSheet("font-size: 16px; font-weight: 700; color: #eef5ff;")
        self._detail_title.setWordWrap(True)
        detail_layout.addWidget(self._detail_title)

        self._detail_info = QTextEdit()
        self._detail_info.setReadOnly(True)
        self._detail_info.setMaximumHeight(140)
        self._detail_info.setPlaceholderText("Аннотация и метаинформация закона.")
        detail_layout.addWidget(self._detail_info)

        sponsors_label = QLabel("Спонсоры")
        sponsors_label.setObjectName("sectionLabel")
        detail_layout.addWidget(sponsors_label)

        self._sponsors_table = QTableWidget(0, 3)
        self._sponsors_table.setHorizontalHeaderLabels(["Спонсор", "Роль", "Фракция"])
        self._sponsors_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self._sponsors_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._sponsors_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self._sponsors_table.setAlternatingRowColors(True)
        self._sponsors_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._sponsors_table.setShowGrid(False)
        self._sponsors_table.verticalHeader().setVisible(False)
        self._sponsors_table.verticalHeader().setDefaultSectionSize(30)
        self._sponsors_table.setMaximumHeight(200)
        detail_layout.addWidget(self._sponsors_table)

        votes_label = QLabel("Голосования")
        votes_label.setObjectName("sectionLabel")
        detail_layout.addWidget(votes_label)

        self._votes_table = QTableWidget(0, 7)
        self._votes_table.setHorizontalHeaderLabels(["Дата", "Стадия", "За", "Против", "Воздерж.", "Отсут.", "Результат"])
        self._votes_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self._votes_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        for c in range(2, 6):
            self._votes_table.horizontalHeader().setSectionResizeMode(c, QHeaderView.ResizeToContents)
        self._votes_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.Stretch)
        self._votes_table.setAlternatingRowColors(True)
        self._votes_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._votes_table.setShowGrid(False)
        self._votes_table.verticalHeader().setVisible(False)
        self._votes_table.verticalHeader().setDefaultSectionSize(30)
        detail_layout.addWidget(self._votes_table, stretch=1)

        faction_label = QLabel("Голосование по фракциям (последняя сессия)")
        faction_label.setObjectName("sectionLabel")
        detail_layout.addWidget(faction_label)

        self._faction_text = QTextEdit()
        self._faction_text.setReadOnly(True)
        self._faction_text.setMaximumHeight(160)
        self._faction_text.setPlaceholderText("Распределение голосов по фракциям.")
        detail_layout.addWidget(self._faction_text)

        cases_btn = QPushButton("Найти связанные кейсы")
        cases_btn.setObjectName("runBtn")
        cases_btn.clicked.connect(self._find_linked_cases)
        detail_layout.addWidget(cases_btn)

        self._cases_text = QTextEdit()
        self._cases_text.setReadOnly(True)
        self._cases_text.setMaximumHeight(100)
        self._cases_text.setPlaceholderText("Связанные кейсы появятся здесь.")
        detail_layout.addWidget(self._cases_text)

        splitter.addWidget(detail)
        splitter.setSizes([700, 560])
        layout.addWidget(splitter, stretch=1)

        self._current_bill_id = None
        self._load_bills()

    def _load_bills(self):
        status = self._status_filter.currentData() or ""
        bill_type = self._type_filter.currentData() or ""
        search = self._search_input.text().strip()

        where_parts = ["1=1"]
        params = []
        if status:
            where_parts.append("b.status = ?")
            params.append(status)
        if bill_type:
            where_parts.append("b.bill_type = ?")
            params.append(bill_type)
        if search:
            where_parts.append("(b.number LIKE ? OR b.title LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])

        where = " AND ".join(where_parts)
        rows = self.db.execute(
            f"""
            SELECT b.id, b.number, b.title, b.bill_type, b.status, b.registration_date,
                   (SELECT COUNT(*) FROM bill_sponsors WHERE bill_id = b.id)
            FROM bills b
            WHERE {where}
            ORDER BY b.registration_date DESC
            LIMIT 300
            """,
            params,
        ).fetchall()

        self._table.setRowCount(len(rows))
        for i, r in enumerate(rows):
            for c, v in enumerate(r):
                self._table.setItem(i, c, QTableWidgetItem("" if v is None else str(v)))

        total = self.db.execute("SELECT COUNT(*) FROM bills").fetchone()[0]
        self._summary_label.setText(f"Всего законопроектов: {total} | Показано: {len(rows)}")

    def _on_row_selected(self, row, col, prev_row, prev_col):
        if row < 0:
            return
        item = self._table.item(row, 0)
        if not item:
            return
        bill_id = int(item.text())
        self._current_bill_id = bill_id
        self._show_bill_detail(bill_id)

    def _show_bill_detail(self, bill_id):
        bill = self.db.execute("SELECT * FROM bills WHERE id = ?", (bill_id,)).fetchone()
        if not bill:
            return

        self._detail_title.setText(f"{bill['number']} — {bill['title'] or 'Без названия'}")

        info_lines = [
            f"Тип: {bill['bill_type'] or '—'}",
            f"Статус: {bill['status'] or '—'}",
            f"Дата регистрации: {bill['registration_date'] or '—'}",
            f"Комитет: {bill['committee'] or '—'}",
            f"URL: {bill['duma_url'] or '—'}",
        ]
        if bill["annotation"]:
            info_lines.append(f"\nАннотация:\n{bill['annotation'][:500]}")
        if bill["keywords"]:
            try:
                kw = json.loads(bill["keywords"])
                info_lines.append(f"Ключевые слова: {', '.join(kw[:15])}")
            except Exception:
                info_lines.append(f"Ключевые слова: {bill['keywords'][:200]}")
        self._detail_info.setPlainText("\n".join(info_lines))

        sponsors = self.db.execute(
            """
            SELECT sponsor_name, sponsor_role, faction, is_collective
            FROM bill_sponsors
            WHERE bill_id = ?
            ORDER BY is_collective, sponsor_name
            """,
            (bill_id,),
        ).fetchall()
        self._sponsors_table.setRowCount(len(sponsors))
        for i, s in enumerate(sponsors):
            name = s[0]
            if s[3]:
                name += " (коллективный)"
            self._sponsors_table.setItem(i, 0, QTableWidgetItem(name))
            self._sponsors_table.setItem(i, 1, QTableWidgetItem(s[1] or ""))
            self._sponsors_table.setItem(i, 2, QTableWidgetItem(s[2] or ""))

        sessions = self.db.execute(
            """
            SELECT id, vote_date, vote_stage, total_for, total_against,
                   total_abstained, total_absent, result
            FROM bill_vote_sessions
            WHERE bill_id = ?
            ORDER BY vote_date DESC
            """,
            (bill_id,),
        ).fetchall()
        self._votes_table.setRowCount(len(sessions))
        for i, s in enumerate(sessions):
            self._votes_table.setItem(i, 0, QTableWidgetItem(s[1] or ""))
            self._votes_table.setItem(i, 1, QTableWidgetItem(s[2] or ""))
            self._votes_table.setItem(i, 2, QTableWidgetItem(str(s[3] or 0)))
            self._votes_table.setItem(i, 3, QTableWidgetItem(str(s[4] or 0)))
            self._votes_table.setItem(i, 4, QTableWidgetItem(str(s[5] or 0)))
            self._votes_table.setItem(i, 5, QTableWidgetItem(str(s[6] or 0)))
            self._votes_table.setItem(i, 6, QTableWidgetItem(s[7] or ""))

        if sessions:
            latest_session_id = sessions[0][0]
            faction_rows = self.db.execute(
                """
                SELECT faction, vote_result, COUNT(*) as cnt
                FROM bill_votes
                WHERE vote_session_id = ?
                GROUP BY faction, vote_result
                ORDER BY faction, vote_result
                """,
                (latest_session_id,),
            ).fetchall()
            factions = {}
            for fr in faction_rows:
                fac = fr[0] or "Без фракции"
                if fac not in factions:
                    factions[fac] = {}
                factions[fac][fr[1]] = fr[2]

            if factions:
                lines = []
                for fac, votes in factions.items():
                    parts = [f"  {v}: {c}" for v, c in sorted(votes.items())]
                    lines.append(f"{fac}:\n" + "\n".join(parts))
                self._faction_text.setPlainText("\n\n".join(lines))
            else:
                self._faction_text.setPlainText("Данные по фракциям отсутствуют.")
        else:
            self._faction_text.setPlainText("Сессий голосования нет.")

        self._cases_text.setPlainText("")

    def _find_linked_cases(self):
        if self._current_bill_id is None:
            return
        bill = self.db.execute("SELECT number, title FROM bills WHERE id = ?", (self._current_bill_id,)).fetchone()
        if not bill:
            return

        bill_number = bill[0] or ""
        bill_title = bill[1] or ""
        search_term = f"%{bill_number}%"

        rows = self.db.execute(
            """
            SELECT DISTINCT c.id, c.title, c.case_type, c.status
            FROM cases c
            JOIN case_claims cc ON cc.case_id = c.id
            JOIN claims cl ON cl.id = cc.claim_id
            WHERE cl.claim_text LIKE ?
            ORDER BY c.updated_at DESC
            LIMIT 20
            """,
            (search_term,),
        ).fetchall()

        if rows:
            lines = [f"[{r[0]}] {r[1]} ({r[2]}/{r[3]})" for r in rows]
            self._cases_text.setPlainText("\n".join(lines))
        else:
            self._cases_text.setPlainText("Связанных кейсов не найдено.")
