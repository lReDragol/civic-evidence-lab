from PySide6.QtCore import Qt, Signal, QThread
from PySide6.QtGui import QColor, QFont
from PySide6.QtWidgets import (
    QComboBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSpinBox,
    QTextEdit,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

JOB_DEFS = [
    ("watch_folder", "Inbox-сканер", "Сбор", 60),
    ("telegram", "Telegram", "Сбор", 300),
    ("youtube", "YouTube", "Сбор", 86400),
    ("rss", "RSS/СМИ", "Сбор", 3600),
    ("official", "Офиц. реестры", "Сбор", 86400),
    ("playwright_official", "Офиц. JS-сайты", "Сбор", 86400),
    ("tagger", "Тегирование", "Анализ", 21600),
    ("llm", "LLM-классификатор", "Анализ", 43200),
    ("asr", "ASR (Whisper)", "Медиа", 3600),
    ("ocr", "OCR (PaddleOCR)", "Медиа", 3600),
    ("ner", "NER (Natasha)", "Анализ", 7200),
    ("entity_resolve", "Разрешение сущностей", "Анализ", 43200),
    ("quotes", "Извлечение цитат", "Анализ", 7200),
    ("claims", "Заявления/верификация", "Анализ", 21600),
    ("evidence_link", "Привязка свидетельств", "Анализ", 43200),
    ("cases", "Построение дел", "Дела", 86400),
    ("accountability", "Индекс подотчётности", "Дела", 86400),
    ("risk_patterns", "Детекция рисков", "Дела", 86400),
    ("relations", "Связи сущностей", "Анализ", 86400),
    ("backup", "Бэкап БД", "Система", 86400),
]

INTERVAL_KEYS = {
    "watch_folder": "watch_folder_interval_seconds",
    "telegram": "telegram_collect_interval_seconds",
    "youtube": "youtube_interval_seconds",
    "rss": "rss_interval_seconds",
    "official": "official_interval_seconds",
    "playwright_official": "playwright_interval_seconds",
    "tagger": "classification_interval_seconds",
    "llm": "llm_interval_seconds",
    "asr": None,
    "ocr": None,
    "ner": "ner_interval_seconds",
    "entity_resolve": "entity_resolve_interval_seconds",
    "quotes": "quotes_interval_seconds",
    "claims": "claims_interval_seconds",
    "evidence_link": "evidence_link_interval_seconds",
    "cases": "cases_interval_seconds",
    "accountability": "accountability_interval_seconds",
    "risk_patterns": "risk_interval_seconds",
    "relations": "relations_interval_seconds",
    "backup": "backup_interval_seconds",
}

SOURCE_GROUP_LABELS = {
    "pinned": "Закреплённые",
    "official": "Официальные",
    "telegram": "Telegram",
    "media": "СМИ",
    "youtube": "YouTube",
    "other": "Другое",
}

CATEGORY_LABELS = {
    "official": "официальный",
    "telegram": "telegram",
    "media": "сми",
    "youtube": "youtube",
}


class WorkerThread(QThread):
    finished_signal = Signal(str, bool, str)

    def __init__(self, job_id: str, func, parent=None):
        super().__init__(parent)
        self.job_id = job_id
        self.func = func
        self._cancelled = False

    def run(self):
        try:
            self.func()
            self.finished_signal.emit(self.job_id, True, "")
        except Exception as e:
            if not self._cancelled:
                self.finished_signal.emit(self.job_id, False, str(e))

    def cancel(self):
        self._cancelled = True


class SidebarPanel(QWidget):
    source_selected = Signal(int, str)

    def __init__(self, db, settings, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings
        self._selected_source_id = None
        self._selected_source_name = ""
        self._source_items = {}

        self.setMinimumWidth(260)
        self.setMaximumWidth(360)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        title = QLabel("Источники")
        title.setObjectName("sectionLabel")
        layout.addWidget(title)

        subtitle = QLabel("Поиск, фильтрация и закрепление часто используемых источников.")
        subtitle.setObjectName("mutedLabel")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        self._search_input = QLineEdit()
        self._search_input.setPlaceholderText("Поиск по названию источника...")
        self._search_input.textChanged.connect(self._load_sources)
        layout.addWidget(self._search_input)

        filter_row = QHBoxLayout()
        filter_row.setSpacing(8)

        self._src_filter = QComboBox()
        self._src_filter.addItem("Все источники", "")
        self._src_filter.addItem("Официальные", "official")
        self._src_filter.addItem("Telegram", "telegram")
        self._src_filter.addItem("YouTube", "youtube")
        self._src_filter.addItem("СМИ", "media")
        self._src_filter.currentIndexChanged.connect(self._load_sources)
        filter_row.addWidget(self._src_filter, stretch=1)

        self._pin_button = QPushButton("Закрепить")
        self._pin_button.clicked.connect(self._toggle_pin_selected)
        self._pin_button.setEnabled(False)
        filter_row.addWidget(self._pin_button)
        layout.addLayout(filter_row)

        self._src_tree = QTreeWidget()
        self._src_tree.setHeaderHidden(True)
        self._src_tree.setIndentation(14)
        self._src_tree.setUniformRowHeights(True)
        self._src_tree.itemSelectionChanged.connect(self._on_src)
        layout.addWidget(self._src_tree, stretch=1)

        hint = QLabel("Группы можно сворачивать. Закреплённые источники всегда показываются сверху.")
        hint.setObjectName("mutedLabel")
        hint.setWordWrap(True)
        layout.addWidget(hint)

        self._load_sources()

    def current_source_name(self) -> str:
        return self._selected_source_name

    def _get_pinned_ids(self):
        pinned = self.settings.get("pinned_sources", [])
        result = set()
        for value in pinned:
            try:
                result.add(int(value))
            except (TypeError, ValueError):
                continue
        return result

    def _set_pinned_ids(self, values):
        self.settings["pinned_sources"] = sorted(values)

    def _load_sources(self):
        selected_id = self._selected_source_id
        self._source_items = {}
        self._src_tree.clear()

        category = self._src_filter.currentData() or ""
        search_text = self._search_input.text().strip().lower()

        where_parts = ["is_active = 1"]
        params = []
        if category == "official":
            where_parts.append("is_official = 1")
        elif category:
            where_parts.append("category = ?")
            params.append(category)
        if search_text:
            where_parts.append("LOWER(name) LIKE ?")
            params.append(f"%{search_text}%")

        where_sql = " AND ".join(where_parts)
        rows = self.db.execute(
            f"""
            SELECT id, name, category, is_official, credibility_tier
            FROM sources
            WHERE {where_sql}
            ORDER BY is_official DESC, name
            """,
            params,
        ).fetchall()

        pinned_ids = self._get_pinned_ids()
        grouped = {key: [] for key in SOURCE_GROUP_LABELS}
        for row in rows:
            source_id, name, src_category, is_official, tier = row
            if source_id in pinned_ids:
                grouped["pinned"].append(row)
                continue
            if is_official:
                grouped["official"].append(row)
            elif src_category in ("telegram", "media", "youtube"):
                grouped[src_category].append(row)
            else:
                grouped["other"].append(row)

        for group_key, label in SOURCE_GROUP_LABELS.items():
            items = grouped[group_key]
            if not items:
                continue
            header = QTreeWidgetItem([f"{label} ({len(items)})"])
            header.setFlags(header.flags() & ~Qt.ItemIsSelectable)
            header.setForeground(0, QColor("#8ea3c8"))
            header.setFont(0, QFont("Segoe UI", 10, QFont.Bold))
            self._src_tree.addTopLevelItem(header)
            header.setExpanded(group_key in {"pinned", "official", "telegram"})

            for source_id, name, src_category, is_official, tier in items:
                meta = []
                if is_official:
                    meta.append("официальный")
                elif src_category:
                    meta.append(CATEGORY_LABELS.get(src_category, str(src_category)))
                if tier:
                    meta.append(f"надёжность {tier}")
                prefix = "★ " if source_id in pinned_ids else ""
                text = f"{prefix}{name}"
                if meta:
                    text += "  ·  " + " · ".join(meta)

                child = QTreeWidgetItem([text])
                child.setData(0, Qt.UserRole, source_id)
                child.setData(0, Qt.UserRole + 1, name)
                child.setToolTip(0, text)
                if is_official:
                    child.setForeground(0, QColor("#8fe0b0"))
                elif src_category == "telegram":
                    child.setForeground(0, QColor("#92c7ff"))
                header.addChild(child)
                self._source_items[source_id] = child

        if selected_id in self._source_items:
            self._src_tree.setCurrentItem(self._source_items[selected_id])
        else:
            self._selected_source_id = None
            self._selected_source_name = ""
            self._pin_button.setEnabled(False)
            self._pin_button.setText("Закрепить")

    def _toggle_pin_selected(self):
        if self._selected_source_id is None:
            return
        pinned_ids = self._get_pinned_ids()
        if self._selected_source_id in pinned_ids:
            pinned_ids.remove(self._selected_source_id)
        else:
            pinned_ids.add(self._selected_source_id)
        self._set_pinned_ids(pinned_ids)
        self._load_sources()

    def _on_src(self):
        item = self._src_tree.currentItem()
        if not item:
            return
        source_id = item.data(0, Qt.UserRole)
        if source_id is None:
            return
        self._selected_source_id = int(source_id)
        self._selected_source_name = item.data(0, Qt.UserRole + 1) or item.text(0)
        self._pin_button.setEnabled(True)
        self._pin_button.setText(
            "Открепить" if self._selected_source_id in self._get_pinned_ids() else "Закрепить"
        )
        self.source_selected.emit(self._selected_source_id, self._selected_source_name)


class JobDetailPanel(QWidget):
    run_requested = Signal(str)
    stop_requested = Signal(str)
    scheduler_toggle = Signal(bool)
    job_selected = Signal(str)

    def __init__(self, db, settings, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings
        self._current_job = None
        self._job_items = {}

        self.setMinimumWidth(290)
        self.setMaximumWidth(420)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        title = QLabel("Рабочая панель")
        title.setObjectName("sectionLabel")
        layout.addWidget(title)

        subtitle = QLabel("Выберите задачу, настройте интервал и запускайте её вручную или по планировщику.")
        subtitle.setObjectName("mutedLabel")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        self._btn_sched = QPushButton("Запустить планировщик")
        self._btn_sched.setObjectName("startBtn")
        self._btn_sched.clicked.connect(lambda: self.scheduler_toggle.emit(True))
        layout.addWidget(self._btn_sched)

        self._job_tree = QTreeWidget()
        self._job_tree.setHeaderHidden(True)
        self._job_tree.setIndentation(14)
        self._job_tree.setUniformRowHeights(True)
        self._job_tree.itemSelectionChanged.connect(self._on_job_item_selected)
        layout.addWidget(self._job_tree, stretch=1)

        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        sep.setStyleSheet("color: #263041;")
        layout.addWidget(sep)

        self._title = QLabel("Задача не выбрана")
        self._title.setObjectName("sectionLabel")
        self._title.setWordWrap(True)
        layout.addWidget(self._title)

        self._status = QLabel("Выберите задачу из списка справа.")
        self._status.setObjectName("mutedLabel")
        self._status.setWordWrap(True)
        layout.addWidget(self._status)

        row = QHBoxLayout()
        row.setSpacing(8)

        self._btn_run = QPushButton("Запустить")
        self._btn_run.setObjectName("runBtn")
        self._btn_run.clicked.connect(
            lambda: self.run_requested.emit(self._current_job) if self._current_job else None
        )
        self._btn_run.setEnabled(False)
        row.addWidget(self._btn_run)

        self._btn_stop = QPushButton("Стоп")
        self._btn_stop.setObjectName("stopBtn")
        self._btn_stop.clicked.connect(
            lambda: self.stop_requested.emit(self._current_job) if self._current_job else None
        )
        self._btn_stop.setEnabled(False)
        row.addWidget(self._btn_stop)
        layout.addLayout(row)

        interval_row = QHBoxLayout()
        interval_row.setSpacing(8)
        interval_row.addWidget(QLabel("Интервал"))
        self._iv_spin = QSpinBox()
        self._iv_spin.setRange(30, 604800)
        self._iv_spin.setSuffix(" сек")
        self._iv_spin.valueChanged.connect(self._on_iv)
        interval_row.addWidget(self._iv_spin, stretch=1)
        layout.addLayout(interval_row)

        log_label = QLabel("Последние сообщения")
        log_label.setObjectName("sectionLabel")
        layout.addWidget(log_label)

        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setMinimumHeight(150)
        layout.addWidget(self._log)

        self.refresh_jobs()

    def refresh_jobs(self):
        selected_job = self._current_job
        self._job_items = {}
        self._job_tree.clear()

        groups = {}
        for job_id, name, group, interval in JOB_DEFS:
            groups.setdefault(group, []).append((job_id, name, interval))

        for group_name, jobs in groups.items():
            header = QTreeWidgetItem([group_name])
            header.setFlags(header.flags() & ~Qt.ItemIsSelectable)
            header.setForeground(0, QColor("#8ea3c8"))
            header.setFont(0, QFont("Segoe UI", 10, QFont.Bold))
            self._job_tree.addTopLevelItem(header)
            header.setExpanded(True)
            for job_id, name, interval in jobs:
                running = self.settings.get(f"job_{job_id}_running", False)
                interval_value = self._interval_for_job(job_id, interval)
                dot = "●" if running else "○"
                item = QTreeWidgetItem([f"{dot} {name}  ·  {self._human_interval(interval_value)}"])
                item.setData(0, Qt.UserRole, job_id)
                item.setToolTip(0, f"{name}\nГруппа: {group_name}\nИнтервал: {self._human_interval(interval_value)}")
                item.setForeground(0, QColor("#8fe0b0" if running else "#cdd6f4"))
                header.addChild(item)
                self._job_items[job_id] = item

        if selected_job and selected_job in self._job_items:
            self._job_tree.setCurrentItem(self._job_items[selected_job])
        else:
            self._set_placeholder()

    def set_job(self, job_id):
        self._current_job = job_id
        if job_id in self._job_items and self._job_tree.currentItem() is not self._job_items[job_id]:
            self._job_tree.setCurrentItem(self._job_items[job_id])
            return

        job_def = next((job for job in JOB_DEFS if job[0] == job_id), None)
        if not job_def:
            self._set_placeholder()
            return

        _, name, group, default_interval = job_def
        running = self.settings.get(f"job_{job_id}_running", False)
        interval_value = self._interval_for_job(job_id, default_interval)
        self._title.setText(name)
        self._status.setText(
            f"{group} · {'выполняется' if running else 'ожидает запуска'} · {self._human_interval(interval_value)}"
        )
        self._iv_spin.setValue(interval_value)
        self._btn_run.setEnabled(not running)
        self._btn_stop.setEnabled(running)

    def set_job_status(self, job_id, running, msg=""):
        if job_id == self._current_job:
            interval = self._iv_spin.value() if self._current_job else 0
            self._status.setText(
                f"{'выполняется' if running else 'ожидает запуска'}"
                + (f" · {self._human_interval(interval)}" if interval else "")
            )
            self._btn_run.setEnabled(not running)
            self._btn_stop.setEnabled(running)
        if msg:
            self._log.append(msg)
        self.refresh_jobs()

    def set_scheduler_running(self, running: bool):
        self._btn_sched.setText("Остановить планировщик" if running else "Запустить планировщик")
        self._btn_sched.setObjectName("stopBtn" if running else "startBtn")
        self._btn_sched.style().unpolish(self._btn_sched)
        self._btn_sched.style().polish(self._btn_sched)

    def _on_job_item_selected(self):
        item = self._job_tree.currentItem()
        if not item:
            return
        job_id = item.data(0, Qt.UserRole)
        if not job_id:
            return
        self._current_job = job_id
        self.set_job(job_id)
        self.job_selected.emit(job_id)

    def _set_placeholder(self):
        self._current_job = None
        self._title.setText("Задача не выбрана")
        self._status.setText("Выберите задачу из списка справа.")
        self._btn_run.setEnabled(False)
        self._btn_stop.setEnabled(False)
        self._iv_spin.setValue(30)

    def _interval_for_job(self, job_id: str, default_value: int) -> int:
        interval_key = INTERVAL_KEYS.get(job_id)
        if not interval_key:
            return default_value
        return int(self.settings.get(interval_key, default_value))

    def _human_interval(self, seconds: int) -> str:
        if seconds < 60:
            return f"{seconds} сек"
        if seconds < 3600:
            return f"{seconds // 60} мин"
        if seconds < 86400:
            return f"{seconds // 3600} ч"
        return f"{seconds // 86400} д"

    def _on_iv(self, value):
        if not self._current_job:
            return
        interval_key = INTERVAL_KEYS.get(self._current_job)
        if interval_key:
            self.settings[interval_key] = value
            self.refresh_jobs()
