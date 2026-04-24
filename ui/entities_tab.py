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


class EntitiesTab(QWidget):
    def __init__(self, db: sqlite3.Connection, settings: dict, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        toolbar = QHBoxLayout()
        toolbar.setSpacing(8)

        self._type_filter = QComboBox()
        self._type_filter.addItem("Все типы", "")
        for entity_type in [r[0] for r in self.db.execute("SELECT DISTINCT entity_type FROM entities ORDER BY entity_type")]:
            self._type_filter.addItem(entity_type, entity_type)
        self._type_filter.currentIndexChanged.connect(self._load_entities)
        toolbar.addWidget(self._type_filter)

        self._search_input = QLineEdit()
        self._search_input.setPlaceholderText("Поиск по имени или алиасу...")
        self._search_input.returnPressed.connect(self._load_entities)
        toolbar.addWidget(self._search_input, stretch=2)

        refresh_button = QPushButton("Обновить")
        refresh_button.clicked.connect(self._load_entities)
        toolbar.addWidget(refresh_button)
        toolbar.addStretch()
        layout.addLayout(toolbar)

        self._summary_label = QLabel("")
        self._summary_label.setObjectName("mutedLabel")
        layout.addWidget(self._summary_label)

        splitter = QSplitter(Qt.Horizontal)

        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(["ID", "Тип", "Имя", "Упоминания", "Алиасы"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
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

        detail_title = QLabel("Карточка сущности")
        detail_title.setObjectName("sectionLabel")
        detail_layout.addWidget(detail_title)

        self._detail_name = QLabel("Сущность не выбрана")
        self._detail_name.setStyleSheet("font-size: 16px; font-weight: 700; color: #eef5ff;")
        self._detail_name.setWordWrap(True)
        detail_layout.addWidget(self._detail_name)

        self._detail_info = QTextEdit()
        self._detail_info.setReadOnly(True)
        self._detail_info.setPlaceholderText("Служебная информация появится после выбора строки.")
        detail_layout.addWidget(self._detail_info)

        mentions_label = QLabel("Последние упоминания")
        mentions_label.setObjectName("sectionLabel")
        detail_layout.addWidget(mentions_label)

        self._detail_mentions = QTextEdit()
        self._detail_mentions.setReadOnly(True)
        detail_layout.addWidget(self._detail_mentions, stretch=1)

        splitter.addWidget(detail)
        splitter.setSizes([760, 480])
        layout.addWidget(splitter, stretch=1)

        self._load_entities()

    def _load_entities(self):
        entity_type = self._type_filter.currentData() or ""
        search_value = self._search_input.text().strip()

        where_parts = ["1=1"]
        params = []
        if entity_type:
            where_parts.append("entity_type = ?")
            params.append(entity_type)
        if search_value:
            where_parts.append("(canonical_name LIKE ? OR aliases LIKE ?)")
            params.extend([f"%{search_value}%", f"%{search_value}%"])

        where_sql = " AND ".join(where_parts)
        rows = self.db.execute(
            f"""
            SELECT
                e.id,
                e.entity_type,
                e.canonical_name,
                (SELECT COUNT(*) FROM entity_mentions em WHERE em.entity_id = e.id) AS mention_count,
                GROUP_CONCAT(ea.alias, ', ') AS aliases
            FROM entities e
            LEFT JOIN entity_aliases ea ON ea.entity_id = e.id
            WHERE {where_sql}
            GROUP BY e.id
            ORDER BY mention_count DESC, e.canonical_name
            LIMIT 300
            """,
            params,
        ).fetchall()

        self._table.setRowCount(len(rows))
        for index, row in enumerate(rows):
            self._table.setItem(index, 0, QTableWidgetItem(str(row[0])))
            self._table.setItem(index, 1, QTableWidgetItem(str(row[1] or "")))
            self._table.setItem(index, 2, QTableWidgetItem(str(row[2] or "")))
            self._table.setItem(index, 3, QTableWidgetItem(str(row[3] or 0)))
            self._table.setItem(index, 4, QTableWidgetItem(str(row[4] or "")[:160]))

        self._summary_label.setText(f"Показано {len(rows)} сущностей.")
        self._detail_name.setText("Сущность не выбрана")
        self._detail_info.clear()
        self._detail_mentions.clear()

    def _on_row_selected(self, row, col, prev_row, prev_col):
        if row < 0:
            return
        item = self._table.item(row, 0)
        if not item:
            return
        entity_id = int(item.text())

        entity = self.db.execute(
            """
            SELECT
                e.canonical_name,
                e.entity_type,
                (SELECT COUNT(*) FROM entity_mentions em WHERE em.entity_id = e.id) AS mention_count,
                GROUP_CONCAT(ea.alias, ', ') AS aliases,
                e.description,
                e.created_at,
                e.extra_data
            FROM entities e
            LEFT JOIN entity_aliases ea ON ea.entity_id = e.id
            WHERE e.id = ?
            GROUP BY e.id
            """,
            (entity_id,),
        ).fetchone()
        if entity:
            self._detail_name.setText(entity[0] or "Без имени")
            self._detail_info.setPlainText(
                f"ID: {entity_id}\n"
                f"Тип: {entity[1]}\n"
                f"Упоминаний: {entity[2]}\n"
                f"Создано: {entity[5] or '?'}\n\n"
                f"Алиасы:\n{entity[3] or '—'}\n\n"
                f"Описание:\n{entity[4] or '—'}\n\n"
                f"Extra data:\n{entity[6] or '—'}"
            )

        mentions = self.db.execute(
            """
            SELECT c.published_at, c.title
            FROM entity_mentions em
            JOIN content_items c ON c.id = em.content_item_id
            WHERE em.entity_id = ?
            ORDER BY c.published_at DESC, c.id DESC
            LIMIT 20
            """,
            (entity_id,),
        ).fetchall()
        if mentions:
            self._detail_mentions.setPlainText(
                "\n".join(f"{mention[0] or '?'} · {mention[1] or '(без заголовка)'}" for mention in mentions)
            )
        else:
            self._detail_mentions.setPlainText("Упоминания не найдены.")
