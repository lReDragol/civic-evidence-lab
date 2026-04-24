import sqlite3

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QHeaderView,
    QLabel,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


class DeputiesTab(QWidget):
    def __init__(self, db: sqlite3.Connection, settings: dict, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        intro = QLabel("Индекс подотчётности по депутатским профилям, цитатам и связанным делам.")
        intro.setObjectName("mutedLabel")
        intro.setWordWrap(True)
        layout.addWidget(intro)

        splitter = QSplitter(Qt.Horizontal)

        self._table = QTableWidget(0, 8)
        self._table.setHorizontalHeaderLabels(["ID", "ФИО", "Фракция", "Регион", "Балл", "Выступления", "Флаги", "Дела"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        for column in range(4, 8):
            self._table.horizontalHeader().setSectionResizeMode(column, QHeaderView.ResizeToContents)
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
        detail_layout.setContentsMargins(0, 0, 0, 0)
        detail_layout.setSpacing(8)

        detail_title = QLabel("Карточка депутата")
        detail_title.setObjectName("sectionLabel")
        detail_layout.addWidget(detail_title)

        self._detail_name = QLabel("Депутат не выбран")
        self._detail_name.setStyleSheet("font-size: 16px; font-weight: 700; color: #eef5ff;")
        self._detail_name.setWordWrap(True)
        detail_layout.addWidget(self._detail_name)

        self._detail_info = QTextEdit()
        self._detail_info.setReadOnly(True)
        self._detail_info.setPlaceholderText("Основная информация по профилю появится здесь.")
        detail_layout.addWidget(self._detail_info)

        quotes_label = QLabel("Последние цитаты")
        quotes_label.setObjectName("sectionLabel")
        detail_layout.addWidget(quotes_label)

        self._detail_quotes = QTextEdit()
        self._detail_quotes.setReadOnly(True)
        detail_layout.addWidget(self._detail_quotes)

        claims_label = QLabel("Связанные заявления")
        claims_label.setObjectName("sectionLabel")
        detail_layout.addWidget(claims_label)

        self._detail_claims = QTextEdit()
        self._detail_claims.setReadOnly(True)
        detail_layout.addWidget(self._detail_claims, stretch=1)

        splitter.addWidget(detail)
        splitter.setSizes([760, 500])
        layout.addWidget(splitter, stretch=1)

        self._load_deputies()

    def _load_deputies(self):
        rows = self.db.execute(
            """
            SELECT dp.id, dp.full_name, dp.faction, dp.region,
                   ai.calculated_score, ai.public_speeches_count,
                   ai.flagged_statements_count, ai.linked_cases_count
            FROM deputy_profiles dp
            LEFT JOIN accountability_index ai ON ai.deputy_id = dp.id
                AND ai.period = (SELECT MAX(period) FROM accountability_index)
            WHERE dp.is_active = 1
            ORDER BY ai.calculated_score ASC
            """
        ).fetchall()

        self._table.setRowCount(len(rows))
        for index, row in enumerate(rows):
            for column, value in enumerate(row):
                self._table.setItem(index, column, QTableWidgetItem("" if value is None else str(value)))

    def _on_row_selected(self, row, col, prev_row, prev_col):
        if row < 0:
            return
        item = self._table.item(row, 0)
        if not item:
            return
        deputy_id = int(item.text())

        profile = self.db.execute(
            """
            SELECT full_name, faction, region, committee, biography_url, entity_id
            FROM deputy_profiles
            WHERE id = ?
            """,
            (deputy_id,),
        ).fetchone()
        if profile:
            self._detail_name.setText(profile[0] or "Без имени")
            self._detail_info.setPlainText(
                f"Фракция: {profile[1] or '—'}\n"
                f"Регион: {profile[2] or '—'}\n"
                f"Комитет: {profile[3] or '—'}\n"
                f"Биография: {profile[4] or '—'}"
            )
            entity_id = profile[5]
        else:
            entity_id = None

        quotes = []
        claims = []
        if entity_id:
            quotes = self.db.execute(
                """
                SELECT quote_text
                FROM quotes
                WHERE entity_id = ?
                ORDER BY id DESC
                LIMIT 10
                """,
                (entity_id,),
            ).fetchall()
            claims = self.db.execute(
                """
                SELECT DISTINCT c.claim_text
                FROM claims c
                JOIN entity_mentions em ON em.content_item_id = c.content_item_id
                WHERE em.entity_id = ?
                ORDER BY c.id DESC
                LIMIT 10
                """,
                (entity_id,),
            ).fetchall()
        if quotes:
            self._detail_quotes.setPlainText("\n\n".join(quote[0] for quote in quotes if quote[0]))
        else:
            self._detail_quotes.setPlainText("Цитаты не найдены.")

        if claims:
            self._detail_claims.setPlainText("\n\n".join(claim[0] for claim in claims if claim[0]))
        else:
            self._detail_claims.setPlainText("Связанные заявления не найдены.")
