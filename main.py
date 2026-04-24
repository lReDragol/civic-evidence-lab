import logging
import sys
from pathlib import Path

sys_path = str(Path(__file__).resolve().parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QAction
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from config.db_utils import get_db, load_settings, ensure_dirs, setup_logging

log = logging.getLogger(__name__)

STYLESHEET = """
QMainWindow, QWidget {
    background-color: #0f131a;
    color: #d9e2f2;
    font-family: 'Segoe UI', sans-serif;
    font-size: 13px;
}
QMenuBar {
    background-color: #11161f;
    color: #d9e2f2;
    border-bottom: 1px solid #263041;
    padding: 2px 6px;
}
QMenuBar::item {
    padding: 5px 12px;
    border-radius: 8px;
}
QMenuBar::item:selected { background-color: #182233; }
QMenu {
    background-color: #11161f;
    color: #d9e2f2;
    border: 1px solid #263041;
}
QMenu::item:selected { background-color: #182233; }
QTabWidget::pane {
    border: none;
    background-color: transparent;
    margin-top: 8px;
}
QTabBar::tab {
    background-color: transparent;
    color: #91a0b8;
    padding: 8px 14px;
    border: 1px solid transparent;
    border-radius: 10px;
    margin-right: 6px;
    font-size: 12px;
    font-weight: 600;
}
QTabBar::tab:selected {
    background-color: #17283a;
    color: #eef5ff;
    border-color: #315176;
}
QTabBar::tab:hover {
    background-color: #142131;
    color: #eef5ff;
}
QTableWidget {
    background-color: #11161f;
    alternate-background-color: #151c27;
    color: #d9e2f2;
    gridline-color: transparent;
    border: 1px solid #263041;
    border-radius: 12px;
    selection-background-color: #21486d;
    selection-color: #f8fbff;
}
QTableWidget::item { padding: 0 10px; }
QHeaderView::section {
    background-color: #101722;
    color: #9eb0cb;
    padding: 8px 10px;
    border: none;
    border-bottom: 1px solid #263041;
    font-weight: 600;
    font-size: 12px;
}
QLineEdit {
    background-color: #11161f;
    color: #d9e2f2;
    border: 1px solid #2d3a4f;
    padding: 7px 10px;
    border-radius: 10px;
}
QLineEdit:focus { border-color: #4e7db7; }
QPushButton {
    background-color: #182233;
    color: #d9e2f2;
    border: 1px solid #2d3a4f;
    padding: 7px 14px;
    border-radius: 10px;
    min-height: 18px;
}
QPushButton:hover {
    background-color: #203149;
    color: #f8fbff;
}
QPushButton:pressed {
    background-color: #315176;
    color: #f8fbff;
}
QPushButton:disabled {
    background-color: #121821;
    color: #617088;
    border-color: #1f2a39;
}
QPushButton#startBtn {
    background-color: #183824;
    color: #a8e2bf;
    border-color: #2f6d43;
}
QPushButton#startBtn:hover { background-color: #1e452d; }
QPushButton#stopBtn {
    background-color: #3b1f24;
    color: #f4b7c4;
    border-color: #8b3949;
}
QPushButton#stopBtn:hover { background-color: #4a252c; }
QPushButton#runBtn {
    background-color: #1b3044;
    color: #a9d1ff;
    border-color: #396086;
}
QPushButton#runBtn:hover { background-color: #22405a; }
QPushButton#groupNav {
    background-color: transparent;
    color: #91a0b8;
    border: 1px solid transparent;
    padding: 7px 14px;
    border-radius: 10px;
}
QPushButton#groupNav:hover {
    background-color: #142131;
    color: #eef5ff;
    border-color: #263041;
}
QPushButton#groupNav:checked {
    background-color: #17283a;
    color: #eef5ff;
    border-color: #315176;
}
QTextEdit, QPlainTextEdit {
    background-color: #11161f;
    color: #d9e2f2;
    border: 1px solid #263041;
    border-radius: 12px;
    padding: 10px;
    font-family: 'Consolas', monospace;
    font-size: 12px;
}
QLabel { color: #d9e2f2; }
QLabel#sectionLabel {
    color: #eef5ff;
    font-size: 15px;
    font-weight: 700;
    padding: 0;
}
QLabel#pageTitle {
    color: #eef5ff;
    font-size: 24px;
    font-weight: 700;
}
QLabel#contextLabel {
    color: #8ea3c8;
    font-size: 12px;
}
QLabel#mutedLabel {
    color: #8191a8;
    font-size: 12px;
}
QWidget#OverviewCard {
    background-color: #11161f;
    border: 1px solid #263041;
    border-radius: 12px;
}
QLabel#statValue { font-size: 24px; font-weight: 700; color: #eef5ff; }
QLabel#statSubtitle { font-size: 11px; color: #8ea3c8; }
QComboBox {
    background-color: #11161f;
    color: #d9e2f2;
    border: 1px solid #2d3a4f;
    padding: 6px 10px;
    border-radius: 10px;
}
QComboBox::drop-down { border: none; }
QComboBox QAbstractItemView {
    background-color: #11161f;
    color: #d9e2f2;
    selection-background-color: #21486d;
}
QTreeWidget {
    background-color: #11161f;
    color: #d9e2f2;
    border: 1px solid #263041;
    border-radius: 12px;
    outline: none;
}
QTreeWidget::item {
    padding: 7px 10px;
    border-radius: 8px;
}
QTreeWidget::item:selected {
    background-color: #21486d;
    color: #f8fbff;
}
QTreeWidget::item:hover {
    background-color: #172233;
}
QGroupBox {
    color: #eef5ff;
    border: 1px solid #263041;
    border-radius: 12px;
    margin-top: 8px;
    padding-top: 14px;
    font-size: 12px;
}
QGroupBox::title { subcontrol-origin: margin; padding: 0 6px; }
QSpinBox, QDoubleSpinBox {
    background-color: #11161f;
    color: #d9e2f2;
    border: 1px solid #2d3a4f;
    padding: 6px 10px;
    border-radius: 10px;
}
QScrollBar:vertical { background-color: #0f131a; width: 10px; }
QScrollBar::handle:vertical { background-color: #263041; border-radius: 5px; min-height: 30px; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
QStatusBar {
    background-color: #11161f;
    color: #8191a8;
    font-size: 11px;
    border-top: 1px solid #263041;
}
QCheckBox { color: #d9e2f2; spacing: 6px; }
QCheckBox::indicator {
    width: 16px;
    height: 16px;
    border-radius: 4px;
    border: 1px solid #2d3a4f;
    background-color: #11161f;
}
QCheckBox::indicator:checked { background-color: #315176; border-color: #315176; }
QSplitter::handle { background-color: #182233; }
QSplitter::handle:horizontal { width: 4px; }
QSplitter::handle:vertical { height: 3px; }
QScrollArea { border: none; }
"""

JOB_DEFS = [
    ("watch_folder", "Inbox-сканер", "Сбор", 60),
    ("telegram", "Telegram", "Сбор", 300),
    ("youtube", "YouTube", "Сбор", 86400),
    ("rss", "RSS/СМИ", "Сбор", 3600),
    ("official", "Офиц. реестры", "Сбор", 86400),
    ("playwright_official", "Офиц. JS-сайты", "Сбор", 86400),
    ("duma_bills", "Законопроекты Думы", "Сбор", 86400),
    ("minjust", "Минюст (иноагенты)", "Сбор", 86400),
    ("zakupki", "Госзакупки", "Сбор", 86400),
    ("gov", "Кремль/Правительство", "Сбор", 86400),
    ("votes", "Голосования Думы", "Сбор", 86400),
    ("senators", "Сенаторы", "Сбор", 604800),
    ("fas_ach_sk", "ФАС/Счётная/СК", "Сбор", 86400),
    ("tagger", "Тегирование", "Анализ", 21600),
    ("llm", "LLM-классификатор", "Анализ", 43200),
    ("asr", "ASR (Whisper)", "Медиа", 3600),
    ("ocr", "OCR (PaddleOCR)", "Медиа", 3600),
    ("ner", "NER (Natasha)", "Анализ", 7200),
    ("entity_resolve", "Разрешение сущностей", "Анализ", 43200),
    ("quotes", "Извлечение цитат", "Анализ", 7200),
    ("claims", "Заявления/верификация", "Анализ", 21600),
    ("evidence_link", "Привязка свидетельств", "Анализ", 43200),
    ("negation", "Негация/опровержения", "Анализ", 43200),
    ("authenticity", "Модель подлинности", "Верификация", 86400),
    ("structural_links", "Структурные связи", "Аналитика", 86400),
    ("entity_relation_builder", "Построение связей", "Аналитика", 86400),
    ("l4_tags", "L4 аналитические теги", "Анализ", 43200),
    ("re_verifier", "Повторная верификация", "Верификация", 43200),
    ("contradiction_detector", "Детекция противоречий", "Верификация", 86400),
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
    "duma_bills": "duma_bills_interval_seconds",
    "minjust": "minjust_interval_seconds",
    "zakupki": "zakupki_interval_seconds",
    "gov": "gov_interval_seconds",
    "votes": "votes_interval_seconds",
    "senators": "senators_interval_seconds",
    "fas_ach_sk": "fas_ach_sk_interval_seconds",
    "tagger": "classification_interval_seconds",
    "llm": "llm_interval_seconds",
    "asr": None, "ocr": None,
    "ner": "ner_interval_seconds",
    "entity_resolve": "entity_resolve_interval_seconds",
    "quotes": "quotes_interval_seconds",
    "claims": "claims_interval_seconds",
    "evidence_link": "evidence_link_interval_seconds",
    "negation": "negation_interval_seconds",
    "authenticity": "authenticity_interval_seconds",
    "structural_links": "structural_links_interval_seconds",
    "entity_relation_builder": "entity_relation_builder_interval_seconds",
    "l4_tags": "l4_tags_interval_seconds",
    "re_verifier": "re_verifier_interval_seconds",
    "contradiction_detector": None,
    "cases": "cases_interval_seconds",
    "accountability": "accountability_interval_seconds",
    "risk_patterns": "risk_interval_seconds",
    "relations": "relations_interval_seconds",
    "backup": "backup_interval_seconds",
}

JOB_FUNC_MAP = {
    "watch_folder": lambda: __import__("collectors.watch_folder", fromlist=["scan_all_inboxes"]).scan_all_inboxes(),
    "telegram": lambda: __import__("asyncio").run(__import__("collectors.telegram_collector", fromlist=["run_collect"]).run_collect()),
    "youtube": lambda: __import__("collectors.youtube_collector", fromlist=["collect_youtube"]).collect_youtube(),
    "rss": lambda: __import__("collectors.rss_collector", fromlist=["collect_rss"]).collect_rss(),
    "official": lambda: __import__("collectors.official_scraper", fromlist=["collect_all_official"]).collect_all_official(),
    "playwright_official": lambda: __import__("collectors.playwright_scraper", fromlist=["collect_all_playwright"]).collect_all_playwright(),
    "duma_bills": lambda: __import__("collectors.playwright_scraper_v2", fromlist=["collect_bills_playwright"]).collect_bills_playwright(pages=2, detail_limit=20, headless=True),
    "minjust": lambda: (__import__("collectors.minjust_scraper", fromlist=["collect_foreign_agents"]).collect_foreign_agents(), __import__("collectors.minjust_scraper", fromlist=["collect_undesirable_orgs"]).collect_undesirable_orgs()),
    "zakupki": lambda: __import__("collectors.zakupki_scraper", fromlist=["collect_contracts_recent"]).collect_contracts_recent(pages=3, per_page=20),
    "gov": lambda: (__import__("collectors.gov_scraper", fromlist=["collect_kremlin_acts"]).collect_kremlin_acts(pages=5), __import__("collectors.gov_scraper", fromlist=["collect_government_news"]).collect_government_news(pages=3)),
    "votes": lambda: __import__("collectors.vote_scraper", fromlist=["collect_votes"]).collect_votes(pages=3, fetch_details=True),
    "senators": lambda: __import__("collectors.senators_scraper", fromlist=["collect_senators"]).collect_senators(fetch_profiles=True),
    "fas_ach_sk": lambda: (__import__("collectors.fas_ach_sk_scraper", fromlist=["collect_fas"]).collect_fas(pages=3, fetch_details=True, detail_limit=20), __import__("collectors.fas_ach_sk_scraper", fromlist=["collect_ach"]).collect_ach(fetch_details=True, detail_limit=15), __import__("collectors.fas_ach_sk_scraper", fromlist=["collect_sk"]).collect_sk(pages=2, fetch_details=True, detail_limit=15)),
    "tagger": lambda: __import__("classifier.tagger_v2", fromlist=["tag_content_items"]).tag_content_items(),
    "llm": lambda: __import__("classifier.llm_classifier", fromlist=["classify_content"]).classify_content(batch_size=20),
    "asr": lambda: __import__("media_pipeline.asr", fromlist=["process_untranscribed_videos"]).process_untranscribed_videos(),
    "ocr": lambda: __import__("media_pipeline.ocr", fromlist=["process_unprocessed_ocr"]).process_unprocessed_ocr(),
    "ner": lambda: __import__("ner.extractor", fromlist=["process_content_entities"]).process_content_entities(batch_size=200),
    "entity_resolve": lambda: (__import__("ner.entity_resolver", fromlist=["resolve_deputies"]).resolve_deputies(), __import__("ner.entity_resolver", fromlist=["resolve_all_persons"]).resolve_all_persons()),
    "quotes": lambda: __import__("claims.quote_extractor", fromlist=["process_content_quotes"]).process_content_quotes(batch_size=200),
    "claims": lambda: __import__("verification.engine", fromlist=["process_claims_for_content"]).process_claims_for_content(),
    "evidence_link": lambda: (__import__("verification.evidence_linker", fromlist=["auto_link_evidence"]).auto_link_evidence(), __import__("verification.evidence_linker", fromlist=["auto_link_by_content_type"]).auto_link_by_content_type()),
    "negation": lambda: __import__("classifier.negation_handler", fromlist=["process_negations"]).process_negations(),
    "authenticity": lambda: __import__("verification.authenticity_model", fromlist=["recompute_all"]).recompute_all(),
    "structural_links": lambda: __import__("cases.structural_links", fromlist=["run_all_structural_links"]).run_all_structural_links(__import__("config.db_utils", fromlist=["load_settings"]).load_settings()),
    "entity_relation_builder": lambda: __import__("analysis.entity_relation_builder", fromlist=["run_all"]).run_all(),
    "l4_tags": lambda: __import__("classifier.analytical_tags", fromlist=["compute_l4_tags_batch"]).compute_l4_tags_batch(__import__("config.db_utils", fromlist=["get_db"]).get_db(__import__("config.db_utils", fromlist=["load_settings"]).load_settings()), limit=1000),
    "re_verifier": lambda: __import__("verification.re_verifier", fromlist=["run_reverification"]).run_reverification(limit=200),
    "contradiction_detector": lambda: __import__("verification.contradiction_detector", fromlist=["run_contradiction_detection"]).run_contradiction_detection(entity_limit=200),
    "cases": lambda: __import__("cases.builder", fromlist=["build_cases_from_entities"]).build_cases_from_entities(min_claims=2),
    "accountability": lambda: __import__("cases.accountability", fromlist=["compute_all_indices"]).compute_all_indices(),
    "risk_patterns": lambda: __import__("cases.risk_detector", fromlist=["detect_all_patterns"]).detect_all_patterns(),
    "relations": lambda: (__import__("ner.relation_extractor", fromlist=["extract_co_occurrence_relations"]).extract_co_occurrence_relations(), __import__("ner.relation_extractor", fromlist=["extract_head_role_relations"]).extract_head_role_relations()),
    "backup": lambda: __import__("db.backup", fromlist=["backup_database"]).backup_database(),
}

from ui.panels import SidebarPanel, JobDetailPanel, WorkerThread
from ui.settings_panel import SettingsPanel


class MainWindow(QMainWindow):
    def __init__(self, settings=None):
        super().__init__()
        self.settings = settings or load_settings()
        self.setWindowTitle("Факт-документирование — Панель управления")
        self.setMinimumSize(1400, 850)
        self.resize(1800, 1000)

        self.db = get_db(self.settings)
        self.scheduler = None
        self._workers = {}

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QHBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        self._lazy_loaded = set()
        self._job_panel_visible = True
        self._current_source_name = ""
        self._current_group = None
        self._group_buttons = {}
        self._tab_groups = {
            "Мониторинг": ["Обзор", "Контент", "Поиск"],
            "Проверка": ["Заявления", "Ревью"],
            "Аналитика": ["Депутаты", "Сущности", "Дела", "Риски", "Связи"],
            "Законы": ["Законопроекты", "Причастность", "Следствие"],
            "Система": ["Настройки"],
        }

        splitter = QSplitter(Qt.Horizontal)
        self._main_splitter = splitter

        self.sidebar = SidebarPanel(self.db, self.settings)
        self.sidebar.source_selected.connect(self._on_source_selected)
        splitter.addWidget(self.sidebar)

        center = QWidget()
        center_layout = QVBoxLayout(center)
        center_layout.setContentsMargins(16, 16, 16, 16)
        center_layout.setSpacing(12)

        header = QHBoxLayout()
        header.setSpacing(12)

        title_block = QVBoxLayout()
        title_block.setSpacing(4)

        self._view_title = QLabel("Обзор")
        self._view_title.setObjectName("pageTitle")
        title_block.addWidget(self._view_title)

        self._context_label = QLabel("")
        self._context_label.setObjectName("contextLabel")
        title_block.addWidget(self._context_label)
        header.addLayout(title_block, stretch=1)

        self._toggle_jobs_btn = QPushButton("Скрыть задачи")
        self._toggle_jobs_btn.clicked.connect(self._toggle_job_panel)
        header.addWidget(self._toggle_jobs_btn)
        center_layout.addLayout(header)

        group_row = QHBoxLayout()
        group_row.setSpacing(8)
        for group_name in self._tab_groups:
            button = QPushButton(group_name)
            button.setObjectName("groupNav")
            button.setCheckable(True)
            button.clicked.connect(lambda checked, name=group_name: self._set_tab_group(name))
            self._group_buttons[group_name] = button
            group_row.addWidget(button)
        group_row.addStretch()
        center_layout.addLayout(group_row)

        self.tabs = QTabWidget()
        self._tab_widgets = {
            "Обзор": self._make_overview(),
            "Контент": self._lazy_tab("Контент"),
            "Поиск": self._lazy_tab("Поиск"),
            "Заявления": self._lazy_tab("Заявления"),
            "Ревью": self._lazy_tab("Ревью"),
            "Депутаты": self._lazy_tab("Депутаты"),
            "Сущности": self._lazy_tab("Сущности"),
            "Дела": self._lazy_tab("Дела"),
            "Риски": self._lazy_tab("Риски"),
            "Связи": self._lazy_tab("Связи"),
            "Законопроекты": self._lazy_tab("Законопроекты"),
            "Причастность": self._lazy_tab("Причастность"),
            "Следствие": self._lazy_tab("Следствие"),
            "Настройки": self._lazy_tab("Настройки"),
        }
        self.overview_tab = self._tab_widgets["Обзор"]
        self._lazy_loaded.add(id(self.overview_tab))
        self.tabs.currentChanged.connect(self._on_tab_changed)
        center_layout.addWidget(self.tabs, stretch=1)
        splitter.addWidget(center)

        self.right_panel = JobDetailPanel(self.db, self.settings)
        self.right_panel.run_requested.connect(self._run_job)
        self.right_panel.stop_requested.connect(self._stop_job)
        self.right_panel.scheduler_toggle.connect(self._toggle_scheduler)
        self.right_panel.job_selected.connect(self._on_job_selected)
        splitter.addWidget(self.right_panel)

        splitter.setSizes([320, 1180, 360])
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setStretchFactor(2, 0)

        main_layout.addWidget(splitter)

        self._build_statusbar()
        self._build_menubar()

        self._timer = QTimer(self)
        self._timer.timeout.connect(self._periodic)
        self._timer.start(30000)

        self._set_tab_group("Мониторинг")
        self.right_panel.set_scheduler_running(False)
        self._update_status()

    def _lazy_tab(self, name):
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setContentsMargins(0, 0, 0, 0)
        lbl = QLabel(f"Загрузка экрана «{name}» произойдёт при первом открытии.")
        lbl.setAlignment(Qt.AlignCenter)
        lbl.setObjectName("mutedLabel")
        layout.addWidget(lbl)
        w._lazy_name = name
        return w

    def _make_overview(self):
        from ui.overview_tab import OverviewTab
        return OverviewTab(self.db, self.settings)

    def _set_tab_group(self, group_name):
        if group_name == self._current_group:
            return
        self._current_group = group_name
        for name, button in self._group_buttons.items():
            button.setChecked(name == group_name)

        current_name = None
        if self.tabs.currentIndex() >= 0:
            current_name = self.tabs.tabText(self.tabs.currentIndex())

        self.tabs.blockSignals(True)
        self.tabs.clear()
        for tab_name in self._tab_groups[group_name]:
            self.tabs.addTab(self._tab_widgets[tab_name], tab_name)
        self.tabs.blockSignals(False)

        target_name = current_name if current_name in self._tab_groups[group_name] else self._tab_groups[group_name][0]
        self.tabs.setCurrentIndex(self._tab_groups[group_name].index(target_name))
        self._on_tab_changed(self.tabs.currentIndex())

    def _on_tab_changed(self, idx):
        if idx < 0:
            return
        tab = self.tabs.widget(idx)
        name = self.tabs.tabText(idx)
        if id(tab) not in self._lazy_loaded:
            real = self._build_real_tab(name)
            if real is not None:
                layout = tab.layout()
                while layout.count():
                    item = layout.takeAt(0)
                    if item.widget():
                        item.widget().deleteLater()
                layout.addWidget(real)
                tab._real = real
                self._lazy_loaded.add(id(tab))
        self._view_title.setText(name)
        self._update_context()

    def _build_real_tab(self, name):
        if name == "Контент":
            from ui.content_tab import ContentTab
            return ContentTab(self.db, self.settings)
        if name == "Поиск":
            from ui.search_tab import SearchTab
            return SearchTab(self.db, self.settings)
        if name == "Заявления":
            from ui.claims_tab import ClaimsTab
            return ClaimsTab(self.db, self.settings)
        if name == "Ревью":
            from ui.review_tab import ReviewTab
            return ReviewTab(self.db, self.settings)
        if name == "Депутаты":
            from ui.deputies_tab import DeputiesTab
            return DeputiesTab(self.db, self.settings)
        if name == "Сущности":
            from ui.entities_tab import EntitiesTab
            return EntitiesTab(self.db, self.settings)
        if name == "Дела":
            from ui.cases_tab import CasesTab
            return CasesTab(self.db, self.settings)
        if name == "Риски":
            from ui.risk_tab import RiskTab
            return RiskTab(self.db, self.settings)
        if name == "Связи":
            from ui.relations_tab import RelationsTab
            return RelationsTab(self.db, self.settings)
        if name == "Законопроекты":
            from ui.bills_tab import BillsTab
            return BillsTab(self.db, self.settings)
        if name == "Причастность":
            from ui.involvement_tab import InvolvementTab
            return InvolvementTab(self.db, self.settings)
        if name == "Следствие":
            from ui.investigative_tab import InvestigativeTab
            return InvestigativeTab(self.db, self.settings)
        if name == "Настройки":
            return SettingsPanel(self.db, self.settings)
        return None

    def _update_context(self):
        current_tab = self.tabs.tabText(self.tabs.currentIndex()) if self.tabs.currentIndex() >= 0 else "—"
        source_name = self._current_source_name or "Все источники"
        self._context_label.setText(f"{source_name}  /  {self._current_group or '—'}  /  {current_tab}")

    def _toggle_job_panel(self):
        self._job_panel_visible = not self._job_panel_visible
        self.right_panel.setVisible(self._job_panel_visible)
        self._toggle_jobs_btn.setText("Скрыть задачи" if self._job_panel_visible else "Показать задачи")
        if self._job_panel_visible:
            self._main_splitter.setSizes([320, 1180, 360])
        else:
            self._main_splitter.setSizes([320, 1540, 0])

    def _on_source_selected(self, source_id, source_name):
        self._current_source_name = source_name or ""
        self._update_context()

    def _on_job_selected(self, job_id):
        if not self._job_panel_visible:
            self._toggle_job_panel()

    def _run_job(self, job_id):
        func = JOB_FUNC_MAP.get(job_id)
        if not func:
            self.right_panel.set_job_status(job_id, False, f"Нет функции для {job_id}")
            return
        self.settings[f"job_{job_id}_running"] = True
        self.right_panel.set_job_status(job_id, True, f"Запуск {job_id}...")
        self.right_panel.refresh_jobs()

        worker = WorkerThread(job_id, func, self)
        worker.finished_signal.connect(self._on_job_finished)
        self._workers[job_id] = worker
        worker.start()

    def _stop_job(self, job_id):
        worker = self._workers.get(job_id)
        if worker and worker.isRunning():
            worker.cancel()
            worker.quit()
            worker.wait(3000)
        self.settings[f"job_{job_id}_running"] = False
        self.right_panel.set_job_status(job_id, False, "Остановлена")
        self.right_panel.refresh_jobs()

    def _on_job_finished(self, job_id, success, error):
        self.settings[f"job_{job_id}_running"] = False
        if success:
            self.right_panel.set_job_status(job_id, False, "Завершено успешно")
        else:
            self.right_panel.set_job_status(job_id, False, f"Ошибка: {error[:200]}")
        self.right_panel.refresh_jobs()
        self._workers.pop(job_id, None)

    def _toggle_scheduler(self, _=None):
        if self.scheduler is None:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.interval import IntervalTrigger
            self.scheduler = BackgroundScheduler()
            for jid, name, grp, default_iv in JOB_DEFS:
                func = JOB_FUNC_MAP.get(jid)
                if not func:
                    continue
                ikey = INTERVAL_KEYS.get(jid)
                iv = self.settings.get(ikey, default_iv) if ikey else default_iv
                self.scheduler.add_job(func, IntervalTrigger(seconds=iv), id=jid, name=name)
            self.scheduler.start()
            self.right_panel.set_scheduler_running(True)
            log.info("Scheduler started")
        else:
            self.scheduler.shutdown(wait=False)
            self.scheduler = None
            self.right_panel.set_scheduler_running(False)
            log.info("Scheduler stopped")
        self._update_status()

    def _build_statusbar(self):
        self.statusBar().showMessage("Загрузка...")

    def _build_menubar(self):
        view_menu = self.menuBar().addMenu("Вид")

        action_toggle_jobs = QAction("Показать / скрыть рабочую панель", self)
        action_toggle_jobs.triggered.connect(self._toggle_job_panel)
        view_menu.addAction(action_toggle_jobs)

        export_menu = self.menuBar().addMenu("Экспорт")

        action_obsidian = QAction("Выгрузить в Obsidian...", self)
        action_obsidian.setStatusTip("Создать Markdown-архив и скопировать вложения в Obsidian vault")
        action_obsidian.triggered.connect(self._choose_obsidian_export_dir)
        export_menu.addAction(action_obsidian)

    def _choose_obsidian_export_dir(self):
        default_dir = self.settings.get("obsidian_export_dir", str(Path(__file__).resolve().parent / "obsidian_export"))
        target = QFileDialog.getExistingDirectory(self, "Папка Obsidian export", default_dir)
        if not target:
            return
        self._run_obsidian_export(target)

    def _run_obsidian_export(self, target_dir: str):
        if "obsidian_export" in self._workers:
            QMessageBox.information(self, "Obsidian export", "Выгрузка уже выполняется.")
            return

        db_path = self.settings.get("db_path", str(Path(__file__).resolve().parent / "db" / "news_unified.db"))

        def _export():
            from tools.export_obsidian import export_obsidian
            export_obsidian(Path(db_path), Path(target_dir), copy_media=True)

        self.statusBar().showMessage(f"Obsidian export запущен: {target_dir}")
        worker = WorkerThread("obsidian_export", _export, self)
        worker.finished_signal.connect(self._on_obsidian_export_finished)
        self._workers["obsidian_export"] = worker
        worker.start()

    def _on_obsidian_export_finished(self, job_id, success, error):
        self._workers.pop(job_id, None)
        if success:
            self.statusBar().showMessage("Obsidian export завершён")
            QMessageBox.information(self, "Obsidian export", "Выгрузка в Obsidian завершена.")
        else:
            self.statusBar().showMessage("Obsidian export завершился ошибкой")
            QMessageBox.critical(self, "Obsidian export", error or "Неизвестная ошибка")

    def _update_status(self):
        try:
            items = self.db.execute("SELECT COUNT(*) FROM content_items").fetchone()[0]
            claims = self.db.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
            entities = self.db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
            cases = self.db.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
            bills = self.db.execute("SELECT COUNT(*) FROM bills").fetchone()[0]
            votes = self.db.execute("SELECT COUNT(*) FROM bill_vote_sessions").fetchone()[0]
            sched = "ON" if self.scheduler else "OFF"
            self.statusBar().showMessage(
                f"Контент: {items} | Заявления: {claims} | Сущности: {entities} | "
                f"Дела: {cases} | Законы: {bills} | Голосования: {votes} | Планировщик: {sched}"
            )
        except Exception:
            self.statusBar().showMessage("Ошибка подключения к БД")

    def _periodic(self):
        self._update_status()
        if hasattr(self.overview_tab, 'refresh_stats'):
            self.overview_tab.refresh_stats()

    def closeEvent(self, event):
        self._timer.stop()
        if self.scheduler:
            self.scheduler.shutdown(wait=False)
        for w in self._workers.values():
            if w.isRunning():
                w.cancel()
                w.quit()
                w.wait(2000)
        try:
            self.db.close()
        except Exception:
            pass
        event.accept()


def main():
    settings = load_settings()
    setup_logging(settings)
    ensure_dirs(settings)

    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setStyleSheet(STYLESHEET)

    window = MainWindow(settings)
    window.show()
    app.exec()


if __name__ == "__main__":
    main()
