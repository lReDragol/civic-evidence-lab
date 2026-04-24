import sqlite3
import sys
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
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

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from search.search_engine import search, search_entities, search_quotes


class SearchTab(QWidget):
    def __init__(self, db: sqlite3.Connection, settings: dict, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings
        self._results = []
        self._page = 0
        self._page_size = 50
        self._total = 0

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        toolbar = QHBoxLayout()
        toolbar.setSpacing(8)

        self._query_input = QLineEdit()
        self._query_input.setPlaceholderText("Поиск по контенту, сущностям и цитатам...")
        self._query_input.returnPressed.connect(self._do_search)
        toolbar.addWidget(self._query_input, stretch=3)

        self._mode = QComboBox()
        self._mode.addItem("Контент", "content")
        self._mode.addItem("Сущности", "entities")
        self._mode.addItem("Цитаты", "quotes")
        self._mode.currentIndexChanged.connect(self._load_results)
        toolbar.addWidget(self._mode)

        self._type_filter = QComboBox()
        self._type_filter.addItem("Все типы", "")
        for content_type in [
            "post",
            "article",
            "video",
            "bill",
            "registry_record",
            "court_record",
            "enforcement",
            "transcript",
            "deputy_profile",
            "procurement",
        ]:
            self._type_filter.addItem(content_type, content_type)
        self._type_filter.currentIndexChanged.connect(self._load_results)
        toolbar.addWidget(self._type_filter)

        self._status_filter = QComboBox()
        self._status_filter.addItem("Все статусы", "")
        for status in ["raw_signal", "verified", "disproven", "unverified", "partially_verified"]:
            self._status_filter.addItem(status, status)
        self._status_filter.currentIndexChanged.connect(self._load_results)
        toolbar.addWidget(self._status_filter)

        search_button = QPushButton("Найти")
        search_button.clicked.connect(self._do_search)
        toolbar.addWidget(search_button)
        layout.addLayout(toolbar)

        self._result_label = QLabel("Введите запрос и выберите тип данных для поиска.")
        self._result_label.setObjectName("mutedLabel")
        self._result_label.setWordWrap(True)
        layout.addWidget(self._result_label)

        splitter = QSplitter(Qt.Horizontal)

        self._table = QTableWidget(0, 6)
        self._table.setHorizontalHeaderLabels(["ID", "Дата", "Тип", "Статус", "Источник", "Фрагмент"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(5, QHeaderView.Stretch)
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

        detail_title = QLabel("Детали результата")
        detail_title.setObjectName("sectionLabel")
        detail_layout.addWidget(detail_title)

        self._detail_title = QLabel("Результат не выбран")
        self._detail_title.setStyleSheet("font-size: 16px; font-weight: 700; color: #eef5ff;")
        self._detail_title.setWordWrap(True)
        detail_layout.addWidget(self._detail_title)

        self._detail_meta = QLabel("После выбора строки здесь появятся тип, источник, дата и служебные поля.")
        self._detail_meta.setObjectName("mutedLabel")
        self._detail_meta.setWordWrap(True)
        detail_layout.addWidget(self._detail_meta)

        self._detail_body = QTextEdit()
        self._detail_body.setReadOnly(True)
        self._detail_body.setPlaceholderText("Подробности выбранного результата появятся здесь.")
        detail_layout.addWidget(self._detail_body, stretch=1)

        splitter.addWidget(detail)
        splitter.setSizes([760, 460])
        layout.addWidget(splitter, stretch=1)

        pager = QHBoxLayout()
        pager.setSpacing(8)
        self._btn_prev = QPushButton("Назад")
        self._btn_prev.clicked.connect(self._prev_page)
        self._btn_next = QPushButton("Вперёд")
        self._btn_next.clicked.connect(self._next_page)
        self._page_label = QLabel("")
        self._page_label.setObjectName("mutedLabel")
        self._page_label.setAlignment(Qt.AlignCenter)
        pager.addStretch()
        pager.addWidget(self._btn_prev)
        pager.addWidget(self._page_label)
        pager.addWidget(self._btn_next)
        layout.addLayout(pager)

    def _do_search(self):
        self._page = 0
        self._load_results()

    def _load_results(self):
        query = self._query_input.text().strip()
        mode = self._mode.currentData() or "content"

        if not query:
            self._result_label.setText("Введите запрос и выберите тип данных для поиска.")
            self._table.setRowCount(0)
            self._results = []
            self._page_label.setText("")
            self._detail_title.setText("Результат не выбран")
            self._detail_meta.setText("После выбора строки здесь появятся тип, источник, дата и служебные поля.")
            self._detail_body.clear()
            return

        if mode == "content":
            self._load_content(query)
        elif mode == "entities":
            self._load_entities(query)
        elif mode == "quotes":
            self._load_quotes(query)

    def _load_content(self, query):
        content_type = self._type_filter.currentData() or ""
        status = self._status_filter.currentData() or ""

        result = search(
            query,
            conn=self.db,
            content_type=content_type,
            status=status,
            limit=self._page_size,
            offset=self._page * self._page_size,
        )
        self._total = result["total"]
        self._results = result["results"]

        self._table.setColumnCount(6)
        self._table.setHorizontalHeaderLabels(["ID", "Дата", "Тип", "Статус", "Источник", "Фрагмент"])
        self._table.setRowCount(len(self._results))
        for index, row in enumerate(self._results):
            self._table.setItem(index, 0, QTableWidgetItem(str(row["id"])))
            self._table.setItem(index, 1, QTableWidgetItem(str(row["published_at"] or "")[:16]))
            self._table.setItem(index, 2, QTableWidgetItem(str(row["content_type"] or "")))
            self._table.setItem(index, 3, self._status_item(row["status"]))
            self._table.setItem(index, 4, QTableWidgetItem(str(row["source_name"] or "")[:32]))
            snippet = row.get("title_snippet") or row.get("title", "")
            self._table.setItem(index, 5, QTableWidgetItem(snippet[:140]))

        self._result_label.setText(
            f"Найдено {self._total} записей по запросу «{result.get('fts_query', query)}»."
        )
        self._update_pager()

    def _load_entities(self, query):
        self._total = 0
        self._results = search_entities(query, conn=self.db)
        self._total = len(self._results)

        self._table.setColumnCount(5)
        self._table.setHorizontalHeaderLabels(["ID", "Тип", "Имя", "Упоминания", "Алиасы"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self._table.setRowCount(len(self._results))

        for index, row in enumerate(self._results):
            self._table.setItem(index, 0, QTableWidgetItem(str(row["id"])))
            self._table.setItem(index, 1, QTableWidgetItem(row["entity_type"]))
            self._table.setItem(index, 2, QTableWidgetItem(row["canonical_name"]))
            self._table.setItem(index, 3, QTableWidgetItem(str(row["mention_count"])))
            self._table.setItem(index, 4, QTableWidgetItem(row["aliases"][:120]))

        self._result_label.setText(f"Найдено {self._total} сущностей по запросу «{query}».")
        self._update_pager()

    def _load_quotes(self, query):
        self._total = 0
        self._results = search_quotes(query, conn=self.db)
        self._total = len(self._results)

        self._table.setColumnCount(5)
        self._table.setHorizontalHeaderLabels(["ID", "Сущность", "Риторика", "Флаг", "Цитата"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self._table.setRowCount(len(self._results))

        for index, row in enumerate(self._results):
            self._table.setItem(index, 0, QTableWidgetItem(str(row["id"])))
            self._table.setItem(index, 1, QTableWidgetItem(row["entity_name"] or ""))
            self._table.setItem(index, 2, QTableWidgetItem(row["rhetoric_class"] or ""))
            self._table.setItem(index, 3, QTableWidgetItem("⚠" if row["is_flagged"] else ""))
            self._table.setItem(index, 4, QTableWidgetItem(row["quote_text"][:140]))

        self._result_label.setText(f"Найдено {self._total} цитат по запросу «{query}».")
        self._update_pager()

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

    def _on_row_selected(self, row, col, prev_row, prev_col):
        if row < 0 or row >= len(self._results):
            return
        result = self._results[row]
        mode = self._mode.currentData() or "content"

        if mode == "content":
            self._detail_title.setText(result.get("title_snippet") or result.get("title", ""))
            body = result.get("body_snippet") or result.get("body_text", "")
            self._detail_body.setPlainText(body)
            meta = " | ".join(
                [
                    f"ID: {result.get('id')}",
                    f"Тип: {result.get('content_type')}",
                    f"Статус: {result.get('status')}",
                    f"Источник: {result.get('source_name', '?')}",
                    f"Опубликовано: {result.get('published_at', '?')}",
                ]
            )
            if result.get("url"):
                meta += f" | URL: {result['url']}"
            self._detail_meta.setText(meta)
        elif mode == "entities":
            self._detail_title.setText(result.get("canonical_name", ""))
            self._detail_meta.setText(f"ID: {result.get('id')} | Тип: {result.get('entity_type')}")
            self._detail_body.setPlainText(
                f"Упоминаний: {result.get('mention_count')}\n\nАлиасы:\n{result.get('aliases', '-')}"
            )
        elif mode == "quotes":
            self._detail_title.setText(result.get("entity_name") or "Цитата без связанной сущности")
            self._detail_meta.setText(
                f"ID: {result.get('id')} | Риторика: {result.get('rhetoric_class')} | "
                f"Флаг: {'Да' if result.get('is_flagged') else 'Нет'} | "
                f"Контент: {result.get('content_item_id')}"
            )
            self._detail_body.setPlainText(result.get("quote_text", ""))

    def _update_pager(self):
        total_pages = max(1, (self._total + self._page_size - 1) // self._page_size)
        self._page_label.setText(f"Страница {self._page + 1} из {total_pages} · {self._total} записей")

    def _prev_page(self):
        if self._page > 0:
            self._page -= 1
            self._load_results()

    def _next_page(self):
        total_pages = max(1, (self._total + self._page_size - 1) // self._page_size)
        if self._page < total_pages - 1:
            self._page += 1
            self._load_results()
