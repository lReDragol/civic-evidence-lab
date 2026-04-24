import sys
from pathlib import Path

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtGui import QColor, QFont, QIcon
from PySide6.QtWidgets import (
    QApplication, QHBoxLayout, QLabel, QMainWindow, QPushButton,
    QStackedWidget, QTabWidget, QVBoxLayout, QWidget,
)

from config.db_utils import get_db, load_settings

from ui.overview_tab import OverviewTab
from ui.content_tab import ContentTab
from ui.search_tab import SearchTab
from ui.claims_tab import ClaimsTab
from ui.review_tab import ReviewTab
from ui.deputies_tab import DeputiesTab
from ui.entities_tab import EntitiesTab
from ui.cases_tab import CasesTab
from ui.risk_tab import RiskTab
from ui.relations_tab import RelationsTab


DARK_STYLE = """
QMainWindow, QWidget {
    background-color: #1e1e2e;
    color: #cdd6f4;
    font-family: 'Segoe UI', sans-serif;
    font-size: 13px;
}
QTabWidget::pane {
    border: 1px solid #45475a;
    background-color: #1e1e2e;
}
QTabBar::tab {
    background-color: #313244;
    color: #bac2de;
    padding: 8px 18px;
    border: 1px solid #45475a;
    border-bottom: none;
    margin-right: 2px;
}
QTabBar::tab:selected {
    background-color: #1e1e2e;
    color: #cba6f7;
    border-bottom: 2px solid #cba6f7;
}
QTabBar::tab:hover {
    color: #f5c2e7;
}
QTableWidget {
    background-color: #181825;
    alternate-background-color: #1e1e2e;
    color: #cdd6f4;
    gridline-color: #45475a;
    border: 1px solid #45475a;
    selection-background-color: #585b70;
    selection-color: #f5e0dc;
}
QTableWidget::item {
    padding: 4px 8px;
}
QHeaderView::section {
    background-color: #313244;
    color: #bac2de;
    padding: 6px 8px;
    border: 1px solid #45475a;
    font-weight: bold;
}
QLineEdit {
    background-color: #313244;
    color: #cdd6f4;
    border: 1px solid #585b70;
    padding: 6px 10px;
    border-radius: 4px;
}
QLineEdit:focus {
    border-color: #cba6f7;
}
QPushButton {
    background-color: #45475a;
    color: #cdd6f4;
    border: 1px solid #585b70;
    padding: 6px 16px;
    border-radius: 4px;
}
QPushButton:hover {
    background-color: #585b70;
    color: #f5e0dc;
}
QPushButton:pressed {
    background-color: #cba6f7;
    color: #1e1e2e;
}
QTextEdit, QPlainTextEdit {
    background-color: #181825;
    color: #cdd6f4;
    border: 1px solid #45475a;
    padding: 8px;
    font-family: 'Consolas', monospace;
    font-size: 12px;
}
QLabel {
    color: #cdd6f4;
}
QComboBox {
    background-color: #313244;
    color: #cdd6f4;
    border: 1px solid #585b70;
    padding: 5px 10px;
    border-radius: 4px;
}
QComboBox::drop-down {
    border: none;
}
QComboBox QAbstractItemView {
    background-color: #313244;
    color: #cdd6f4;
    selection-background-color: #585b70;
}
QScrollBar:vertical {
    background-color: #1e1e2e;
    width: 12px;
}
QScrollBar::handle:vertical {
    background-color: #45475a;
    border-radius: 6px;
    min-height: 30px;
}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0;
}
QGroupBox {
    color: #cba6f7;
    border: 1px solid #45475a;
    border-radius: 6px;
    margin-top: 12px;
    padding-top: 16px;
}
QGroupBox::title {
    subcontrol-origin: margin;
    padding: 0 6px;
}
QStatusBar {
    background-color: #181825;
    color: #a6adc8;
}
"""


class MainWindow(QMainWindow):
    def __init__(self, settings: dict = None):
        super().__init__()
        self.settings = settings or load_settings()
        self.setWindowTitle("Система документирования фактов — Редактор")
        self.setMinimumSize(1200, 800)
        self.resize(1600, 900)

        self.db = get_db(self.settings)

        self.tab_widget = QTabWidget()
        self.setCentralWidget(self.tab_widget)

        self.overview_tab = OverviewTab(self.db, self.settings)
        self.content_tab = ContentTab(self.db, self.settings)
        self.search_tab = SearchTab(self.db, self.settings)
        self.claims_tab = ClaimsTab(self.db, self.settings)
        self.review_tab = ReviewTab(self.db, self.settings)
        self.deputies_tab = DeputiesTab(self.db, self.settings)
        self.entities_tab = EntitiesTab(self.db, self.settings)
        self.cases_tab = CasesTab(self.db, self.settings)
        self.risk_tab = RiskTab(self.db, self.settings)
        self.relations_tab = RelationsTab(self.db, self.settings)

        self.tab_widget.addTab(self.overview_tab, "📊 Обзор")
        self.tab_widget.addTab(self.content_tab, "📰 Контент")
        self.tab_widget.addTab(self.search_tab, "🔍 Поиск")
        self.tab_widget.addTab(self.claims_tab, "✅ Заявления")
        self.tab_widget.addTab(self.review_tab, "🔎 Ревью")
        self.tab_widget.addTab(self.deputies_tab, "🏛️ Депутаты")
        self.tab_widget.addTab(self.entities_tab, "👤 Сущности")
        self.tab_widget.addTab(self.cases_tab, "📁 Дела")
        self.tab_widget.addTab(self.risk_tab, "⚠️ Риски")
        self.tab_widget.addTab(self.relations_tab, "🔗 Связи")

        self._build_statusbar()

        self._refresh_timer = QTimer(self)
        self._refresh_timer.timeout.connect(self._periodic_refresh)
        self._refresh_timer.start(30000)

        self._update_status()

    def _build_statusbar(self):
        self.statusBar().showMessage("Загрузка...")

    def _update_status(self):
        try:
            items = self.db.execute("SELECT COUNT(*) FROM content_items").fetchone()[0]
            claims = self.db.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
            entities = self.db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
            cases = self.db.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
            self.statusBar().showMessage(
                f"Контент: {items} | Заявления: {claims} | Сущности: {entities} | Дела: {cases}"
            )
        except Exception:
            self.statusBar().showMessage("Ошибка подключения к БД")

    def _periodic_refresh(self):
        self._update_status()
        self.overview_tab.refresh_stats()

    def closeEvent(self, event):
        self._refresh_timer.stop()
        try:
            self.db.close()
        except Exception:
            pass
        event.accept()


def run_dashboard(settings: dict = None):
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setStyleSheet(DARK_STYLE)

    window = MainWindow(settings)
    window.show()

    return app


def main():
    settings = load_settings()
    app = run_dashboard(settings)
    app.exec()


if __name__ == "__main__":
    main()
