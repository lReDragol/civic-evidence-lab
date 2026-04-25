from __future__ import annotations

import json
import logging
import subprocess
import sys
from pathlib import Path

from PySide6.QtCore import QThread, QUrl, Signal
from PySide6.QtWidgets import QApplication, QFileDialog, QMainWindow, QMessageBox
from PySide6.QtWebChannel import QWebChannel
from PySide6.QtWebEngineWidgets import QWebEngineView

from config.db_utils import CONFIG_DIR, SETTINGS_PATH, ensure_dirs, get_db, load_settings, setup_logging
from ui.job_registry import INTERVAL_KEYS, JOB_DEFS, JOB_FUNC_MAP, get_job_def, interval_for_job
from ui.web_bridge import DashboardBridge, DashboardDataService
from runtime.state import DAEMON_JOB_ID, active_job_lease, request_daemon_stop


log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WEB_ROOT = PROJECT_ROOT / "ui_web"


class WorkerThread(QThread):
    finished_signal = Signal(str, bool, str, str)

    def __init__(self, job_id: str, command: list[str], parent=None):
        super().__init__(parent)
        self.job_id = job_id
        self.command = command
        self._cancelled = False
        self._process = None

    def run(self):
        try:
            self._process = subprocess.Popen(
                self.command,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            stdout, stderr = self._process.communicate()
            if not self._cancelled:
                ok = self._process.returncode == 0
                payload = (stdout or stderr or "").strip()
                self.finished_signal.emit(self.job_id, ok, payload, "")
        except Exception as error:
            if not self._cancelled:
                self.finished_signal.emit(self.job_id, False, "", str(error))

    def cancel(self):
        self._cancelled = True
        try:
            if self._process and self._process.poll() is None:
                self._process.terminate()
        except Exception:
            pass


class WebDashboardWindow(QMainWindow):
    job_requested = Signal(str)

    def __init__(self, settings: dict | None = None):
        super().__init__()
        self.settings = settings or load_settings()
        self.db = get_db(self.settings)
        self._workers: dict[str, WorkerThread] = {}

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
        active = set()
        try:
            for row in self.db.execute(
                """
                SELECT job_id
                FROM job_leases
                WHERE job_id != ?
                """
                ,
                (DAEMON_JOB_ID,),
            ).fetchall():
                lease = active_job_lease(self.db, row[0])
                if lease:
                    active.add(str(row[0]))
        except Exception:
            pass
        active.update(self._workers.keys())
        return active

    def scheduler_running(self):
        try:
            return active_job_lease(self.db, DAEMON_JOB_ID) is not None
        except Exception:
            return False

    def logs(self):
        entries = []
        try:
            rows = self.db.execute(
                """
                SELECT job_id, status, started_at, finished_at, items_new, error_summary
                FROM job_runs
                ORDER BY id DESC
                LIMIT 80
                """
            ).fetchall()
            for row in rows:
                level = "success" if row["status"] == "ok" else "warning" if row["status"] == "abandoned" else "error" if row["status"] == "failed" else "info"
                message = f"{row['job_id']}: {row['status']}"
                if row["items_new"]:
                    message += f" · new {row['items_new']}"
                if row["error_summary"]:
                    message += f" · {row['error_summary']}"
                entries.append({"message": message, "level": level})
        except Exception:
            return []
        return entries

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
        if job_id not in JOB_FUNC_MAP:
            self._append_log(f"Нет функции для задачи {job_id}", level="error")
            return
        self.settings[f"job_{job_id}_running"] = True
        worker = WorkerThread(
            job_id,
            [
                sys.executable,
                "-m",
                "runtime.run_job",
                "--job",
                job_id,
                "--trigger-mode",
                "manual",
                "--requested-by",
                "ui",
            ],
            self,
        )
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
            self._append_log(f"Остановлена задача: {job_id}", level="warning")
        else:
            self._append_log(f"Для {job_id} доступна только мягкая остановка через runtime recovery", level="warning")
        self._workers.pop(job_id, None)
        self.settings[f"job_{job_id}_running"] = False
        self.bridge.emit_bootstrap()
        self._update_statusbar()

    def toggle_scheduler(self):
        if not self.scheduler_running():
            flags = 0
            if sys.platform.startswith("win"):
                flags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            subprocess.Popen(
                [sys.executable, "-m", "runtime.daemon"],
                cwd=str(PROJECT_ROOT),
                creationflags=flags,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self._append_log("Фоновый daemon запускается", level="success")
        else:
            request_daemon_stop(self.db, True)
            self._append_log("Отправлен запрос на остановку daemon", level="warning")
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

        worker = WorkerThread(
            "obsidian_export",
            [
                sys.executable,
                "-m",
                "runtime.run_job",
                "--job",
                "obsidian_export",
                "--trigger-mode",
                "manual",
                "--requested-by",
                "ui",
            ],
            self,
        )
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
        if notify:
            self.bridge.emit_toast(message, level=level)

    def _on_job_finished(self, job_id: str, success: bool, payload: str, error: str):
        self._workers.pop(job_id, None)
        self.settings[f"job_{job_id}_running"] = False
        try:
            data = json.loads(payload) if payload else {}
        except json.JSONDecodeError:
            data = {}
        if success and data.get("ok", True):
            suffix = f" · new {data.get('items_new')}" if data.get("items_new") else ""
            self._append_log(f"Задача завершена: {job_id}{suffix}", level="success")
        else:
            message = error[:260] if error else (payload[:260] if payload else f"Ошибка {job_id}")
            self._append_log(f"Ошибка {job_id}: {message}", level="error")
        self.bridge.emit_bootstrap()
        self._refresh_frontend()
        self._update_statusbar()

    def _on_export_finished(self, job_id: str, success: bool, payload: str, error: str):
        self._workers.pop(job_id, None)
        try:
            data = json.loads(payload) if payload else {}
        except json.JSONDecodeError:
            data = {}
        if success and data.get("ok", True):
            self._append_log("Выгрузка в Obsidian завершена", level="success")
            QMessageBox.information(self, "Obsidian export", "Выгрузка в Obsidian завершена.")
        else:
            message = error or payload or "Неизвестная ошибка"
            self._append_log(f"Obsidian export error: {message[:260]}", level="error")
            QMessageBox.critical(self, "Obsidian export", message)
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
