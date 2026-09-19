"""Civic workbench with profile-scoped collection and read-only archive access."""
import argparse
import importlib
import json
from pathlib import Path
import sqlite3
import sys

from PySide6.QtCore import QObject, QRunnable, QThreadPool, QTimer, QUrl, Signal, Slot
from PySide6.QtWidgets import QApplication, QMainWindow
from PySide6.QtWebChannel import QWebChannel
from PySide6.QtWebEngineWidgets import QWebEngineView
from ui.web_bridge import DashboardBridge, DashboardDataService


DEFAULT_PROFILE = Path(__file__).resolve().parents[1] / "config/civic_collection.json"


def resolve_db_dir(profile_path, db_dir=None):
    if db_dir is not None:
        return Path(db_dir).resolve()
    try:
        runtime = importlib.import_module("runtime.civic_service")
        profile = runtime.load_profile(Path(profile_path).resolve())
        return Path(profile["db_dir"]).resolve()
    except Exception as exc:
        raise ValueError(
            f"Cannot resolve the selected profile database ({type(exc).__name__}). "
            "Check --profile or provide --db-dir explicitly."
        ) from None


def _collection_call(method, profile_path):
    try:
        runtime = importlib.import_module("runtime.civic_service")
        result = getattr(runtime, method)(profile_path)
        if not isinstance(result, dict):
            raise TypeError("Collection service returned an invalid result")
        return result
    except Exception as exc:
        return {"ok": False, "status": "unavailable", "error_type": type(exc).__name__,
                "error": "Collection operation failed. Check the selected profile and runtime availability."}


class _ReportSignals(QObject):
    finished = Signal(dict)


class _ReportWorker(QRunnable):
    def __init__(self, profile_path):
        super().__init__()
        # Only an immutable path crosses threads, never the viewer's DB/service.
        self.profile_path = profile_path
        self.signals = _ReportSignals()

    def run(self):
        self.signals.finished.emit(_collection_call("export_collection_report", self.profile_path))


class CivicCollectionController(QObject):
    def __init__(self, profile_path=DEFAULT_PROFILE, parent=None):
        super().__init__(parent)
        self.profile_path = Path(profile_path).resolve()
        self._report = {"status": "idle"}
        self._report_worker = None

    def _with_profile(self, result):
        return {**result, "profile_path": str(self.profile_path), "report": dict(self._report)}

    def civic_start_collection(self):
        return self._with_profile(_collection_call("launch_collection", self.profile_path))

    def civic_collection_status(self):
        result = _collection_call("collection_status", self.profile_path)
        if "domain_modules" in result:
            # Older profiles omit this field, while the runtime defaults to [].
            # Do not confuse unknown configuration with explicit generic mode.
            try:
                with self.profile_path.open("rb") as stream:
                    raw = stream.read(65537)
                profile = json.loads(raw) if len(raw) <= 65536 else {}
                modules = profile.get("domain_modules") if isinstance(profile, dict) else None
                known = isinstance(modules, list) and all(isinstance(value, str) for value in modules)
            except (OSError, ValueError):
                known = False
            if not known:
                result = {**result, "domain_modules": None}
        return self._with_profile(result)

    def civic_stop_collection(self):
        return self._with_profile(_collection_call("request_stop", self.profile_path))

    def civic_export_report(self):
        if self._report_worker is None:
            self._report = {"status": "running"}
            self._report_worker = _ReportWorker(self.profile_path)
            self._report_worker.signals.finished.connect(self._report_finished)
            QThreadPool.globalInstance().start(self._report_worker)
        return self._with_profile({"ok": True, "status": "report_pending"})

    @Slot(dict)
    def _report_finished(self, result):
        # The runtime may include the full snapshot. Keep it out of WebChannel
        # polling; the UI needs only completion metadata and output paths.
        self._report = {key: result[key] for key in ("ok", "status", "error", "error_type", "run_id", "is_final") if key in result}
        paths = result.get("paths") or result.get("report_paths") or {}
        if isinstance(paths, str):
            paths = {"report": paths}
        elif isinstance(paths, list):
            paths = {str(i): value for i, value in enumerate(paths[:20])}
        elif not isinstance(paths, dict):
            paths = {}
        paths = dict(list(paths.items())[:20])
        for key in ("json_path", "markdown_path"):
            if result.get(key):
                paths[key] = result[key]
        self._report["paths"] = {str(key)[:100]: str(value)[:4096] for key, value in list(paths.items())[:20]}
        self._report.setdefault("status", "ready" if result.get("ok", True) else "failed")
        self._report_worker = None


class CivicWindow(QMainWindow):
    def __init__(self, db_dir, legacy_db, profile_path=DEFAULT_PROFILE):
        super().__init__()
        self.collection = CivicCollectionController(profile_path, self)
        self.conn=sqlite3.connect(legacy_db.resolve().as_uri()+"?mode=ro",uri=True)
        self.conn.row_factory=sqlite3.Row
        self.conn.execute("PRAGMA query_only=ON")
        settings={"civic_workbench_enabled":True,"reactor_db_dir":str(db_dir.resolve()),
                  "reactor_knowledge_db":str((db_dir/"reactor_v2.db").resolve()),
                  "reactor_ops_db":str((db_dir/"reactor_ops.db").resolve())}
        self.service=DashboardDataService(self.conn,settings)
        self.bridge=DashboardBridge(self.service,self)
        self.channel=QWebChannel(self)
        self.channel.registerObject("dashboardBridge",self.bridge)
        self.view=QWebEngineView(self)
        self.view.page().setWebChannel(self.channel)
        self.setCentralWidget(self.view)
        self.setWindowTitle("Civic Evidence Lab 2.0 - Collection workbench")
        self.resize(1440,950)
        self.view.load(QUrl.fromLocalFile(str(Path(__file__).resolve().parents[1]/"ui_web/index.html")))

    def running_jobs(self):
        return []

    def scheduler_running(self):
        return False

    def logs(self):
        return [{"level":"info","message":"Profile collection controls are in Civic workbench; legacy jobs and publication are disabled."}]

    def civic_start_collection(self):
        return self.collection.civic_start_collection()

    def civic_collection_status(self):
        return self.collection.civic_collection_status()

    def civic_stop_collection(self):
        return self.collection.civic_stop_collection()

    def civic_export_report(self):
        return self.collection.civic_export_report()

    def disabled(self,*args):
        self.bridge.emit_toast("Read-only shadow viewer: runtime and publication controls are disabled.","warning")

    run_job=stop_job=start_247=toggle_scheduler=update_job_interval=export_obsidian=toggle_pin_source=disabled

    def closeEvent(self,event):
        # The detached collector is intentionally not stopped with this viewer.
        self.conn.close()
        super().closeEvent(event)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-dir",type=Path,default=None)
    parser.add_argument("--legacy-db",type=Path,default=Path("db/news_unified.db"))
    parser.add_argument("--profile",type=Path,default=DEFAULT_PROFILE)
    parser.add_argument("--smoke-screenshot",type=Path)
    args=parser.parse_args()
    try:
        db_dir = resolve_db_dir(args.profile, args.db_dir)
    except ValueError as exc:
        parser.error(str(exc))
    app=QApplication.instance() or QApplication(sys.argv[:1])
    window=CivicWindow(db_dir,args.legacy_db,args.profile)
    window.show()
    if args.smoke_screenshot:
        def capture():
            args.smoke_screenshot.parent.mkdir(parents=True,exist_ok=True)
            if not window.view.grab().save(str(args.smoke_screenshot)):
                app.exit(1)
                return
            window.close()
            app.quit()
        QTimer.singleShot(8000,capture)
    return app.exec()


if __name__=="__main__":
    raise SystemExit(main())
