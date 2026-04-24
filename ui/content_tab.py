import sqlite3
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QPixmap
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


class ContentTab(QWidget):
    def __init__(self, db: sqlite3.Connection, settings: dict, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings
        self._page = 0
        self._page_size = 50
        self._total = 0
        self._current_image_path = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        toolbar = QHBoxLayout()
        toolbar.setSpacing(8)

        self._search_input = QLineEdit()
        self._search_input.setPlaceholderText("Поиск по заголовку и тексту...")
        self._search_input.returnPressed.connect(self._search)
        toolbar.addWidget(self._search_input, stretch=3)

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
        self._type_filter.currentIndexChanged.connect(self._search)
        toolbar.addWidget(self._type_filter)

        self._status_filter = QComboBox()
        self._status_filter.addItem("Все статусы", "")
        for status in ["raw_signal", "verified", "disproven", "unverified", "partially_verified"]:
            self._status_filter.addItem(status, status)
        self._status_filter.currentIndexChanged.connect(self._search)
        toolbar.addWidget(self._status_filter)

        self._tag_input = QLineEdit()
        self._tag_input.setPlaceholderText("Тег...")
        self._tag_input.returnPressed.connect(self._search)
        toolbar.addWidget(self._tag_input, stretch=1)

        search_button = QPushButton("Найти")
        search_button.clicked.connect(self._search)
        toolbar.addWidget(search_button)
        layout.addLayout(toolbar)

        self._summary_label = QLabel("")
        self._summary_label.setObjectName("mutedLabel")
        layout.addWidget(self._summary_label)

        splitter = QSplitter(Qt.Horizontal)

        self._table = QTableWidget(0, 7)
        self._table.setHorizontalHeaderLabels(["ID", "Дата", "Тип", "Статус", "Источник", "Заголовок", "Теги"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(5, QHeaderView.Stretch)
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

        detail_header = QLabel("Карточка материала")
        detail_header.setObjectName("sectionLabel")
        detail_layout.addWidget(detail_header)

        self._detail_title = QLabel("Материал не выбран")
        self._detail_title.setStyleSheet("font-size: 16px; font-weight: 700; color: #eef5ff;")
        self._detail_title.setWordWrap(True)
        detail_layout.addWidget(self._detail_title)

        self._detail_meta = QLabel("Выберите запись в таблице, чтобы открыть её текст и вложения.")
        self._detail_meta.setObjectName("mutedLabel")
        self._detail_meta.setWordWrap(True)
        detail_layout.addWidget(self._detail_meta)

        self._detail_body = QTextEdit()
        self._detail_body.setReadOnly(True)
        self._detail_body.setPlaceholderText("Текст выбранного материала появится здесь.")
        detail_layout.addWidget(self._detail_body, stretch=1)

        self._image_preview = QLabel("Нет изображения")
        self._image_preview.setAlignment(Qt.AlignCenter)
        self._image_preview.setMinimumHeight(220)
        self._image_preview.setStyleSheet(
            "border: 1px solid #263041; border-radius: 12px; background-color: #11161f; color: #8191a8; padding: 12px;"
        )
        detail_layout.addWidget(self._image_preview)

        attachments_title = QLabel("Вложения")
        attachments_title.setObjectName("sectionLabel")
        detail_layout.addWidget(attachments_title)

        self._attachments_text = QTextEdit()
        self._attachments_text.setReadOnly(True)
        self._attachments_text.setMinimumHeight(100)
        detail_layout.addWidget(self._attachments_text)

        splitter.addWidget(detail)
        splitter.setSizes([760, 520])
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

        self._search()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self._current_image_path:
            self._render_image(self._current_image_path)

    def _search(self):
        self._page = 0
        self._load_page()

    def _load_page(self):
        query = self._search_input.text().strip()
        content_type = self._type_filter.currentData() or ""
        status = self._status_filter.currentData() or ""
        tag = self._tag_input.text().strip()

        where_parts = ["1=1"]
        params = []

        if query:
            where_parts.append("(c.title LIKE ? OR c.body_text LIKE ?)")
            params.extend([f"%{query}%", f"%{query}%"])
        if content_type:
            where_parts.append("c.content_type = ?")
            params.append(content_type)
        if status:
            where_parts.append("c.status = ?")
            params.append(status)
        if tag:
            where_parts.append(
                "EXISTS (SELECT 1 FROM content_tags ct WHERE ct.content_item_id = c.id AND ct.tag_name LIKE ?)"
            )
            params.append(f"%{tag}%")

        where_sql = " AND ".join(where_parts)

        self._total = self.db.execute(
            f"SELECT COUNT(*) FROM content_items c WHERE {where_sql}", params
        ).fetchone()[0]

        offset = self._page * self._page_size
        rows = self.db.execute(
            f"""
            SELECT c.id, c.published_at, c.content_type, c.status,
                   s.name, c.title,
                   (SELECT GROUP_CONCAT(ct.tag_name, ', ') FROM content_tags ct WHERE ct.content_item_id = c.id LIMIT 5)
            FROM content_items c
            LEFT JOIN sources s ON s.id = c.source_id
            WHERE {where_sql}
            ORDER BY c.id DESC
            LIMIT ? OFFSET ?
            """,
            params + [self._page_size, offset],
        ).fetchall()

        self._table.setRowCount(len(rows))
        for index, row in enumerate(rows):
            self._table.setItem(index, 0, QTableWidgetItem(str(row[0])))
            self._table.setItem(index, 1, QTableWidgetItem(str(row[1] or "")[:16]))
            self._table.setItem(index, 2, QTableWidgetItem(str(row[2] or "")))
            self._table.setItem(index, 3, self._status_item(row[3]))
            self._table.setItem(index, 4, QTableWidgetItem(str(row[4] or "")[:32]))
            self._table.setItem(index, 5, QTableWidgetItem(str(row[5] or "")[:120]))
            self._table.setItem(index, 6, QTableWidgetItem(str(row[6] or "")[:100]))

        total_pages = max(1, (self._total + self._page_size - 1) // self._page_size)
        self._page_label.setText(f"Страница {self._page + 1} из {total_pages} · {self._total} записей")
        self._summary_label.setText(
            f"Фильтр: {content_type or 'все типы'} · {status or 'все статусы'} · найдено {self._total} материалов"
        )

        if rows:
            self._table.setCurrentCell(0, 0)
        else:
            self._detail_title.setText("Материалы не найдены")
            self._detail_meta.setText("Измените фильтры или поисковый запрос.")
            self._detail_body.clear()
            self._attachments_text.clear()
            self._current_image_path = None
            self._image_preview.setPixmap(QPixmap())
            self._image_preview.setText("Нет изображения")

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
        if row < 0:
            return
        item_id = self._table.item(row, 0)
        if not item_id:
            return
        content_id = int(item_id.text())

        record = self.db.execute(
            """
            SELECT c.title, c.body_text, c.published_at, c.url, c.content_type, c.status,
                   s.name, c.collected_at
            FROM content_items c
            LEFT JOIN sources s ON s.id = c.source_id
            WHERE c.id = ?
            """,
            (content_id,),
        ).fetchone()
        if not record:
            return

        self._detail_title.setText(record[0] or "(без заголовка)")
        self._detail_body.setPlainText(record[1] or "")
        meta_parts = [
            f"ID: {content_id}",
            f"Тип: {record[4]}",
            f"Статус: {record[5]}",
            f"Источник: {record[6] or '?'}",
            f"Опубликовано: {record[2] or '?'}",
            f"Собрано: {record[7] or '?'}",
        ]
        if record[3]:
            meta_parts.append(f"URL: {record[3]}")
        self._detail_meta.setText(" | ".join(meta_parts))
        self._render_attachments(content_id)

    def _render_attachments(self, content_id: int):
        rows = self.db.execute(
            """
            SELECT id, file_path, attachment_type, file_size, mime_type
            FROM attachments
            WHERE content_item_id = ?
            ORDER BY
                CASE
                    WHEN mime_type LIKE 'image/%' THEN 0
                    WHEN attachment_type IN ('photo', 'scan', 'keyframe', 'thumbnail') THEN 1
                    ELSE 2
                END,
                id
            """,
            (content_id,),
        ).fetchall()

        if not rows:
            self._attachments_text.setPlainText("Нет вложений")
            self._current_image_path = None
            self._image_preview.setPixmap(QPixmap())
            self._image_preview.setText("Нет изображения")
            return

        lines = []
        first_image = None
        for attachment_id, file_path, attachment_type, size, mime_type in rows:
            path = Path(file_path or "")
            exists = path.exists() and path.is_file()
            size_text = f"{size or 0} bytes"
            lines.append(
                f"#{attachment_id} · {attachment_type or 'file'} · {mime_type or 'unknown'} · "
                f"{size_text} · {'OK' if exists else 'нет файла'}\n{file_path or ''}"
            )
            is_image = (mime_type or "").startswith("image/") or attachment_type in {
                "photo",
                "scan",
                "keyframe",
                "thumbnail",
            }
            if first_image is None and is_image and exists:
                first_image = path

        self._attachments_text.setPlainText("\n\n".join(lines))
        self._current_image_path = first_image
        if first_image:
            self._render_image(first_image)
        else:
            self._image_preview.setPixmap(QPixmap())
            self._image_preview.setText("Нет доступного изображения")

    def _render_image(self, image_path: Path):
        pixmap = QPixmap(str(image_path))
        if pixmap.isNull():
            self._image_preview.setPixmap(QPixmap())
            self._image_preview.setText(f"Не удалось загрузить: {image_path.name}")
            return

        target_size = self._image_preview.size()
        if target_size.width() < 80 or target_size.height() < 80:
            target_size = self._image_preview.minimumSize()
        scaled = pixmap.scaled(target_size, Qt.KeepAspectRatio, Qt.SmoothTransformation)
        self._image_preview.setPixmap(scaled)
        self._image_preview.setText("")

    def _prev_page(self):
        if self._page > 0:
            self._page -= 1
            self._load_page()

    def _next_page(self):
        total_pages = max(1, (self._total + self._page_size - 1) // self._page_size)
        if self._page < total_pages - 1:
            self._page += 1
            self._load_page()
