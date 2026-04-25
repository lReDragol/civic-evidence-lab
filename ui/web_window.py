from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from PySide6.QtCore import QThread, QUrl, Signal
from PySide6.QtWidgets import QApplication, QFileDialog, QMainWindow, QMessageBox
from PySide6.QtWebChannel import QWebChannel
from PySide6.QtWebEngineWidgets import QWebEngineView

from config.db_utils import CONFIG_DIR, SETTINGS_PATH, ensure_dirs, get_db, load_settings, setup_logging
from ui.job_registry import INTERVAL_KEYS, JOB_DEFS, JOB_FUNC_MAP, get_job_def, interval_for_job
from ui.web_bridge import DashboardBridge, DashboardDataService


log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WEB_ROOT = PROJECT_ROOT / "ui_web"


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
            if not self._cancelled:
                self.finished_signal.emit(self.job_id, True, "")
        except Exception as error:
            if not self._cancelled:
                self.finished_signal.emit(self.job_id, False, str(error))

    def cancel(self):
        self._cancelled = True


class WebDashboardWindow(QMainWindow):
    job_requested = Signal(str)

    def __init__(self, settings: dict | None = None):
        super().__init__()
        self.settings = settings or load_settings()
        self.db = get_db(self.settings)
        self.scheduler = None
        self._workers: dict[str, WorkerThread] = {}
        self._job_logs: list[dict] = []

        self.setWindowTitle("Civic Evidence Lab")
        self.setMinimumSize(1440, 900)
        self.resize(1820, 1040)

        self.service = DashboardDataService(self.db, self.settings)
        self.bridge = DashboardBridge(self.service, self)
        self.channel = QWebChannel(self)
        self.channel.registerObject("dashboardBridge", self.bridge)

        self.view = QWebEngineView(self)
        self.view.page().setWebChannel(self.channel)
        self.view.loadFinished.connect(self._on_load_finished)
        self.setCentralWidget(self.view)
        self.menuBar().hide()
        self.statusBar().hide()

        self.job_requested.connect(self.run_job)
        self._load_bundle()

    # Controller API used by DashboardBridge
    def running_jobs(self):
        return set(self._workers.keys())

    def scheduler_running(self):
        return self.scheduler is not None

    def logs(self):
        return list(self._job_logs[-80:])

    def toggle_pin_source(self, source_id: int):
        pinned = set()
        for value in self.settings.get("pinned_sources", []):
            try:
                pinned.add(int(value))
            except (TypeError, ValueError):
                continue
        if source_id in pinned:
            pinned.remove(source_id)
        else:
            pinned.add(source_id)
        self.settings["pinned_sources"] = sorted(pinned)
        self._persist_settings({"pinned_sources"})

    def update_job_interval(self, job_id: str, seconds: int):
        interval_key = INTERVAL_KEYS.get(job_id)
        if not interval_key:
            return
        self.settings[interval_key] = int(seconds)
        self._persist_settings({interval_key})
        self._append_log(f"Интервал {job_id}: {seconds} сек", level="info", notify=False)
        self.bridge.emit_bootstrap()

    def run_job(self, job_id: str):
        if not job_id or job_id in self._workers:
            return
        func = JOB_FUNC_MAP.get(job_id)
        if not func:
            self._append_log(f"Нет функции для задачи {job_id}", level="error")
            return
        self.settings[f"job_{job_id}_running"] = True
        worker = WorkerThread(job_id, func, self)
        worker.finished_signal.connect(self._on_job_finished)
        self._workers[job_id] = worker
        self._append_log(f"Запуск задачи: {job_id}", level="info")
        worker.start()
        self.bridge.emit_bootstrap()
        self._update_statusbar()

    def stop_job(self, job_id: str):
        worker = self._workers.get(job_id)
        if worker and worker.isRunning():
            worker.cancel()
            worker.quit()
            worker.wait(3000)
        self._workers.pop(job_id, None)
        self.settings[f"job_{job_id}_running"] = False
        self._append_log(f"Остановлена задача: {job_id}", level="warning")
        self.bridge.emit_bootstrap()
        self._update_statusbar()

    def toggle_scheduler(self):
        if self.scheduler is None:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.interval import IntervalTrigger

            self.scheduler = BackgroundScheduler()
            for job in JOB_DEFS:
                job_id = job["id"]
                if job_id not in JOB_FUNC_MAP:
                    continue
                interval_seconds = interval_for_job(self.settings, job_id)
                self.scheduler.add_job(
                    lambda jid=job_id: self.job_requested.emit(jid),
                    IntervalTrigger(seconds=interval_seconds),
                    id=job_id,
                    name=job["name"],
                    replace_existing=True,
                )
            self.scheduler.start()
            self._append_log("Планировщик запущен", level="success")
        else:
            self.scheduler.shutdown(wait=False)
            self.scheduler = None
            self._append_log("Планировщик остановлен", level="warning")
        self.bridge.emit_bootstrap()
        self._update_statusbar()

    def export_obsidian(self):
        if "obsidian_export" in self._workers:
            self._append_log("Выгрузка в Obsidian уже выполняется", level="warning")
            return

        default_dir = self.settings.get(
            "obsidian_export_dir",
            str(PROJECT_ROOT / "obsidian_export_graph"),
        )
        target_dir = QFileDialog.getExistingDirectory(self, "Папка Obsidian export", default_dir)
        if not target_dir:
            return

        self.settings["obsidian_export_dir"] = target_dir
        self._persist_settings({"obsidian_export_dir"})

        db_path = self.settings.get("db_path", str(PROJECT_ROOT / "db" / "news_unified.db"))

        def _export():
            from tools.export_obsidian import export_obsidian

            export_obsidian(Path(db_path), Path(target_dir), copy_media=True, mode="graph")

        worker = WorkerThread("obsidian_export", _export, self)
        worker.finished_signal.connect(self._on_export_finished)
        self._workers["obsidian_export"] = worker
        self._append_log(f"Запущена выгрузка Obsidian: {target_dir}", level="info")
        worker.start()
        self.bridge.emit_bootstrap()
        self._update_statusbar()

    def _load_bundle(self):
        index_path = WEB_ROOT / "index.html"
        if not index_path.exists():
            self.view.setHtml("<h1>ui_web/index.html not found</h1>")
            return
        self.view.load(QUrl.fromLocalFile(str(index_path)))

    def _on_load_finished(self, ok: bool):
        if not ok:
            self.view.setHtml("<h1>Не удалось загрузить web dashboard</h1>")
            return
        self._refresh_frontend()
        self._update_statusbar()

    def _refresh_frontend(self):
        try:
            self.bridge.emit_bootstrap()
            self.view.page().runJavaScript(
                "window.__dashboardApp && window.__dashboardApp.manualRefresh && window.__dashboardApp.manualRefresh();"
            )
        except Exception as error:
            log.warning("Failed to refresh frontend: %s", error)

    def _append_log(self, message: str, *, level: str = "info", notify: bool = True):
        entry = {
            "message": message,
            "level": level,
        }
        self._job_logs.append(entry)
        self._job_logs = self._job_logs[-100:]
        if notify:
            self.bridge.emit_toast(message, level=level)

    def _on_job_finished(self, job_id: str, success: bool, error: str):
        self._workers.pop(job_id, None)
        self.settings[f"job_{job_id}_running"] = False
        if success:
            self._append_log(f"Задача завершена: {job_id}", level="success")
        else:
            self._append_log(f"Ошибка {job_id}: {error[:260]}", level="error")
        self.bridge.emit_bootstrap()
        self._refresh_frontend()
        self._update_statusbar()

    def _on_export_finished(self, job_id: str, success: bool, error: str):
        self._workers.pop(job_id, None)
        if success:
            self._append_log("Выгрузка в Obsidian завершена", level="success")
            QMessageBox.information(self, "Obsidian export", "Выгрузка в Obsidian завершена.")
        else:
            self._append_log(f"Obsidian export error: {error[:260]}", level="error")
            QMessageBox.critical(self, "Obsidian export", error or "Неизвестная ошибка")
        self.bridge.emit_bootstrap()
        self._update_statusbar()

    def _update_statusbar(self):
        running = len(self.running_jobs())
        title = "Civic Evidence Lab"
        if running:
            title = f"{title} · jobs {running}"
        if self.scheduler_running():
            title = f"{title} · scheduler on"
        self.setWindowTitle(title)

    def _persist_settings(self, keys: set[str]):
        settings_path = SETTINGS_PATH
        try:
            existing = json.loads(settings_path.read_text(encoding="utf-8")) if settings_path.exists() else {}
        except json.JSONDecodeError:
            existing = {}
        for key in keys:
            if key in self.settings:
                existing[key] = self.settings[key]
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        settings_path.write_text(
            json.dumps(existing, indent=4, ensure_ascii=False),
            encoding="utf-8",
        )

    def closeEvent(self, event):
        if self.scheduler:
            self.scheduler.shutdown(wait=False)
        for worker in list(self._workers.values()):
            if worker.isRunning():
                worker.cancel()
                worker.quit()
                worker.wait(2000)
        try:
            self.db.close()
        except Exception:
            pass
        super().closeEvent(event)


def run_app(argv: list[str] | None = None):
    argv = argv or sys.argv
    settings = load_settings()
    setup_logging(settings)
    ensure_dirs(settings)

    app = QApplication.instance() or QApplication(argv)
    window = WebDashboardWindow(settings)
    window.show()
    return app.exec()
