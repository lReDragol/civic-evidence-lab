import json
from pathlib import Path

from PySide6.QtWidgets import (
    QComboBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)


class SettingsPanel(QWidget):
    def __init__(self, db, settings, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings
        self._widgets = {}

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        intro = QLabel("Основные параметры моделей, путей, интервалов и логирования.")
        intro.setObjectName("mutedLabel")
        intro.setWordWrap(True)
        layout.addWidget(intro)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)

        inner = QWidget()
        grid = QGridLayout(inner)
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(12)
        grid.setVerticalSpacing(12)

        sections = [
            (
                "Модели AI",
                [
                    ("ollama_model", "Модель Ollama", "text"),
                    ("ollama_base_url", "URL Ollama", "text"),
                    ("whisper_model", "Модель Whisper", "text"),
                    ("whisper_device", "Устройство Whisper", "text"),
                    ("whisper_compute_type", "Тип вычислений", "text"),
                    ("ocr_engine", "OCR движок", "text"),
                    ("ocr_lang", "OCR язык", "text"),
                ],
            ),
            (
                "Пути",
                [
                    ("db_path", "Путь к БД", "text"),
                    ("inbox_tiktok", "Входящие TikTok", "text"),
                    ("inbox_documents", "Входящие документы", "text"),
                    ("inbox_youtube", "Входящие YouTube", "text"),
                    ("log_file", "Файл лога", "text"),
                ],
            ),
            (
                "Интервалы (секунды)",
                [
                    ("watch_folder_interval_seconds", "Inbox-сканер", "int"),
                    ("telegram_collect_interval_seconds", "Telegram", "int"),
                    ("youtube_interval_seconds", "YouTube", "int"),
                    ("rss_interval_seconds", "RSS", "int"),
                    ("classification_interval_seconds", "Тегирование", "int"),
                    ("llm_interval_seconds", "LLM", "int"),
                    ("ner_interval_seconds", "NER", "int"),
                    ("quotes_interval_seconds", "Цитаты", "int"),
                    ("claims_interval_seconds", "Заявления", "int"),
                    ("evidence_link_interval_seconds", "Свидетельства", "int"),
                    ("cases_interval_seconds", "Дела", "int"),
                    ("risk_interval_seconds", "Риски", "int"),
                    ("backup_interval_seconds", "Бэкап", "int"),
                ],
            ),
            (
                "Прокси",
                [
                    ("http_proxy", "HTTP прокси", "text"),
                    ("https_proxy", "HTTPS прокси", "text"),
                ],
            ),
            (
                "Логирование",
                [("log_level", "Уровень лога", "combo:DEBUG,INFO,WARNING,ERROR")],
            ),
        ]

        for index, (section_name, fields) in enumerate(sections):
            group = QGroupBox(section_name)
            form = QFormLayout(group)
            form.setSpacing(8)
            form.setContentsMargins(12, 14, 12, 12)
            form.setHorizontalSpacing(12)
            for key, label, widget_type in fields:
                widget = self._build_widget(key, widget_type)
                self._widgets[key] = widget
                form.addRow(label, widget)
            grid.addWidget(group, index // 2, index % 2)

        scroll.setWidget(inner)
        layout.addWidget(scroll, stretch=1)

        button_row = QHBoxLayout()
        button_row.setSpacing(8)
        button_row.addStretch()

        save_button = QPushButton("Сохранить настройки")
        save_button.setObjectName("startBtn")
        save_button.clicked.connect(self._save)
        button_row.addWidget(save_button)

        reset_button = QPushButton("Сбросить")
        reset_button.clicked.connect(self._restore)
        button_row.addWidget(reset_button)
        layout.addLayout(button_row)

    def _build_widget(self, key: str, widget_type: str):
        if widget_type == "text":
            return QLineEdit(str(self.settings.get(key, "")))
        if widget_type == "int":
            widget = QSpinBox()
            widget.setRange(10, 604800)
            widget.setValue(int(self.settings.get(key, 3600)))
            return widget
        if widget_type.startswith("combo:"):
            widget = QComboBox()
            for option in widget_type.split(":")[1].split(","):
                widget.addItem(option)
            index = widget.findText(str(self.settings.get(key, "")))
            if index >= 0:
                widget.setCurrentIndex(index)
            return widget
        return QLineEdit(str(self.settings.get(key, "")))

    def _save(self):
        for key, widget in self._widgets.items():
            if isinstance(widget, QLineEdit):
                self.settings[key] = widget.text()
            elif isinstance(widget, QSpinBox):
                self.settings[key] = widget.value()
            elif isinstance(widget, QComboBox):
                self.settings[key] = widget.currentText()

        settings_path = Path(self.settings.get("project_root", ".")) / "config" / "settings.json"
        safe_keys = set(self._widgets.keys())
        public_values = {key: value for key, value in self.settings.items() if key in safe_keys}

        try:
            existing = json.loads(settings_path.read_text(encoding="utf-8"))
            existing.update(public_values)
            settings_path.write_text(
                json.dumps(existing, indent=4, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as error:
            import logging

            logging.getLogger(__name__).error("Failed to save settings: %s", error)

    def _restore(self):
        for key, widget in self._widgets.items():
            value = self.settings.get(key, "")
            if isinstance(widget, QLineEdit):
                widget.setText(str(value))
            elif isinstance(widget, QSpinBox):
                widget.setValue(int(value) if value else 3600)
            elif isinstance(widget, QComboBox):
                index = widget.findText(str(value))
                if index >= 0:
                    widget.setCurrentIndex(index)
