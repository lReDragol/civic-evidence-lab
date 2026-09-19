"""Profile collection sidecar contracts. Never starts a real collector."""
import json
import io
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
os.environ.setdefault("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-gpu")

from PySide6.QtCore import QCoreApplication, QEvent, QEventLoop, QThreadPool, QTimer
from PySide6.QtWidgets import QApplication

from ui.civic_window import CivicCollectionController, CivicWindow, DEFAULT_PROFILE, main, resolve_db_dir
from ui.web_bridge import DashboardBridge, DashboardDataService
from ui import civic_window


ROOT = Path(__file__).resolve().parents[1]
APP = None


class CivicControlsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global APP
        APP = QApplication.instance() or QApplication([])

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.profile = Path(self.temp.name) / "custom-collection.json"
        self.profile.write_text('{"domain_modules": []}', encoding="utf-8")
        self.runtime = SimpleNamespace(
            launch_collection=Mock(return_value={"ok": True, "status": "starting"}),
            collection_status=Mock(return_value={"ok": True, "status": "running", "pid": 123}),
            request_stop=Mock(return_value={"ok": True, "status": "stop_requested"}),
            export_collection_report=Mock(return_value={"ok": True, "status": "ready", "paths": {"markdown": "fixture/report.md"}}),
        )
        self.importer = self.enterContext(patch("ui.civic_window.importlib.import_module", return_value=self.runtime))
        self.controller = CivicCollectionController(self.profile)
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.service = DashboardDataService(self.db, {"civic_workbench_enabled": True})
        self.bridge = DashboardBridge(self.service, self.controller)

    def pump_until(self, predicate, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            APP.processEvents()
            if predicate():
                return
            loop = QEventLoop()
            QTimer.singleShot(20, loop.quit)
            loop.exec()
        self.fail("Timed out waiting for Qt state")

    def test_bridge_slots_delegate_only_to_profile_controller(self):
        for slot, method, status in (
            ("civicStartCollection", "launch_collection", "starting"),
            ("civicCollectionStatus", "collection_status", "running"),
            ("civicStopCollection", "request_stop", "stop_requested"),
        ):
            with self.subTest(slot=slot):
                result = json.loads(getattr(self.bridge, slot)())
                self.assertEqual(result["status"], status)
                self.assertEqual(result["profile_path"], str(self.profile.resolve()))
                getattr(self.runtime, method).assert_called_once_with(self.profile.resolve())
                self.assertGreaterEqual(self.bridge.metaObject().indexOfMethod(f"{slot}()"), 0)
        self.runtime.export_collection_report.assert_not_called()
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()[0], 0)

    def test_default_profile_and_lazy_runtime_failure(self):
        self.assertEqual(DEFAULT_PROFILE, ROOT / "config/civic_collection.json")
        self.importer.assert_not_called()
        self.importer.side_effect = ModuleNotFoundError("runtime not installed yet")
        result = json.loads(self.bridge.civicStartCollection())
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["error_type"], "ModuleNotFoundError")
        self.assertNotIn("not installed", result["error"])

    def test_disabled_or_missing_controller_never_launches(self):
        for controller, enabled in ((None, True), (self.controller, False)):
            self.service.settings["civic_workbench_enabled"] = enabled
            bridge = DashboardBridge(self.service, controller)
            for method in ("civicStartCollection", "civicCollectionStatus", "civicStopCollection", "civicExportReport"):
                self.assertFalse(json.loads(getattr(bridge, method)())["ok"])
        self.importer.assert_not_called()

    def test_report_runs_off_ui_thread_and_duplicate_is_bounded(self):
        entered, release = threading.Event(), threading.Event()
        worker_threads = []
        ui_thread = threading.get_ident()

        def export(profile):
            self.assertEqual(profile, self.profile.resolve())
            worker_threads.append(threading.get_ident())
            entered.set()
            if not release.wait(5):
                raise TimeoutError("Fixture release timed out")
            return {"ok": True, "status": "ready", "paths": {"json": "snapshot.json"}}

        self.runtime.export_collection_report.side_effect = export
        try:
            started = time.monotonic()
            result = json.loads(self.bridge.civicExportReport())
            self.assertLess(time.monotonic() - started, 0.5)
            self.assertEqual(result["report"]["status"], "running")
            self.assertTrue(entered.wait(2))
            self.bridge.civicExportReport()
            self.runtime.export_collection_report.assert_called_once()
            tick = []
            QTimer.singleShot(0, lambda: tick.append(True))
            self.pump_until(lambda: bool(tick))
            self.assertEqual(json.loads(self.bridge.civicCollectionStatus())["status"], "running")
            self.assertNotEqual(worker_threads[0], ui_thread)
            self.runtime.request_stop.assert_not_called()
        finally:
            release.set()
            QThreadPool.globalInstance().waitForDone(5000)
            self.pump_until(lambda: self.controller._report_worker is None)
        report = json.loads(self.bridge.civicCollectionStatus())["report"]
        self.assertEqual(report["paths"]["json"], "snapshot.json")

    def test_report_error_is_visible_and_retryable(self):
        self.runtime.export_collection_report.side_effect = RuntimeError("snapshot locked")
        self.bridge.civicExportReport()
        self.pump_until(lambda: self.controller._report_worker is None)
        result = json.loads(self.bridge.civicCollectionStatus())
        self.assertFalse(result["report"]["ok"])
        self.assertEqual(result["report"]["error_type"], "RuntimeError")
        self.assertNotIn("snapshot locked", result["report"]["error"])
        self.runtime.export_collection_report.side_effect = None
        self.bridge.civicExportReport()
        self.pump_until(lambda: self.controller._report_worker is None)
        self.assertEqual(json.loads(self.bridge.civicCollectionStatus())["report"]["status"], "ready")

    def test_runtime_report_shape_is_compact_and_paths_are_preserved(self):
        self.runtime.export_collection_report.return_value = {
            "status": "exported", "is_final": False,
            "json_path": "fixture/snapshot.json", "markdown_path": "fixture/snapshot.md",
            "report": {"private_large_snapshot": "x" * 100000},
        }
        self.bridge.civicExportReport()
        self.pump_until(lambda: self.controller._report_worker is None)
        result = self.bridge.civicCollectionStatus()
        self.assertLess(len(result), 2000)
        report = json.loads(result)["report"]
        self.assertEqual(report["status"], "exported")
        self.assertFalse(report["is_final"])
        self.assertEqual(report["paths"]["markdown_path"], "fixture/snapshot.md")
        self.assertNotIn("private_large_snapshot", result)

    def test_absent_profile_modules_are_unknown_not_generic(self):
        self.runtime.collection_status.return_value["domain_modules"] = []
        self.profile.write_text('{}', encoding="utf-8")
        self.assertIsNone(json.loads(self.bridge.civicCollectionStatus())["domain_modules"])
        self.profile.write_text('{"domain_modules": []}', encoding="utf-8")
        self.assertEqual(json.loads(self.bridge.civicCollectionStatus())["domain_modules"], [])

    def test_resolve_uses_actual_temp_profile_and_creates_no_database(self):
        from runtime.civic_service import load_profile

        profile = {
            "version": 1, "profile_id": "generic-fixture", "domain_modules": [],
            "db_dir": "other-db", "archive_root": "archive", "report_dir": "reports",
            "max_http_requests_24h": 1, "max_model_requests_24h": 0,
            "max_pending_tasks": 1, "report_interval_seconds": 10,
            "archive_max_bytes": 1024, "archive_reserve_bytes": 0,
            "sources": [{"url": "https://example.invalid/", "allowed_hosts": ["example.invalid"], "interval_seconds": 30}],
        }
        self.profile.write_text(json.dumps(profile), encoding="utf-8")
        self.runtime.load_profile = Mock(side_effect=load_profile)
        expected = self.profile.parent / "other-db"
        self.assertEqual(resolve_db_dir(self.profile), expected.resolve())
        self.runtime.load_profile.assert_called_once_with(self.profile.resolve())
        self.assertFalse(expected.exists())
        self.runtime.launch_collection.assert_not_called()

    def test_explicit_database_bypasses_profile_and_runtime_import(self):
        explicit = self.profile.parent / "override-db"
        self.assertEqual(resolve_db_dir(self.profile, explicit), explicit.resolve())
        self.importer.assert_not_called()
        self.assertFalse(explicit.exists())

    def test_invalid_profile_fails_visibly_without_fallback_or_secret(self):
        self.runtime.load_profile = Mock(side_effect=ValueError("secret-config-value"))
        with self.assertRaises(ValueError) as caught:
            resolve_db_dir(self.profile)
        self.assertIn("Cannot resolve", str(caught.exception))
        self.assertNotIn("secret-config-value", str(caught.exception))
        stderr = io.StringIO()
        with patch.object(sys, "argv", ["civic", "--profile", str(self.profile)]), patch.object(sys, "stderr", stderr), patch.object(civic_window, "CivicWindow") as window:
            with self.assertRaises(SystemExit) as exit_result:
                main()
            self.assertEqual(exit_result.exception.code, 2)
            window.assert_not_called()
        self.assertIn("Cannot resolve", stderr.getvalue())
        self.assertNotIn("secret-config-value", stderr.getvalue())

    def test_control_exception_text_never_enters_webchannel(self):
        self.runtime.launch_collection.side_effect = json.JSONDecodeError("secret-config-value", "secret-config-value", 0)
        payload = self.bridge.civicStartCollection()
        self.assertNotIn("secret-config-value", payload)
        self.assertEqual(json.loads(payload)["error_type"], "JSONDecodeError")
        self.controller.civic_start_collection = Mock(side_effect=ValueError("secret-config-value"))
        payload = self.bridge.civicStartCollection()
        self.assertNotIn("secret-config-value", payload)
        self.assertEqual(json.loads(payload)["error_type"], "ValueError")

    @unittest.skipUnless(os.environ.get("CIVIC_CONTROLS_QT_SMOKE") == "1", "Opt-in actual WebEngine button smoke")
    def test_real_qt_buttons_report_while_running_and_close(self):
        legacy = Path(self.temp.name) / "legacy.db"
        sqlite3.connect(legacy).close()
        window = CivicWindow(Path(self.temp.name), legacy, self.profile)
        window.show()
        self.runtime.export_collection_report.return_value = {
            "status": "exported", "json_path": "fixture/report.json",
            "markdown_path": "fixture/report.md", "is_final": False, "report": {"fixture_only": True},
        }

        def js(expression):
            loop = QEventLoop()
            result = []
            window.view.page().runJavaScript(expression, lambda value: (result.append(value), loop.quit()))
            QTimer.singleShot(3000, loop.quit)
            loop.exec()
            return result[0] if result else None

        def until(expression):
            self.pump_until(lambda: js(expression), timeout=12)

        def click(action):
            until(f"document.querySelector('[data-civic-control={action}]')?.disabled === false")
            js(f"document.querySelector('[data-civic-control={action}]').click()")

        try:
            self.runtime.collection_status.return_value = {"ok": True, "status": "stopped"}
            until("document.querySelector('.civic-runtime-status')?.textContent.includes('stopped')")
            self.assertEqual(js("document.querySelector('.civic-profile code').textContent"), str(self.profile.resolve()))
            self.assertTrue(js("document.getElementById('app-shell').hidden"))
            with self.assertRaises(sqlite3.OperationalError):
                window.conn.execute("CREATE TABLE forbidden(id)")
            click("start")
            self.pump_until(lambda: self.runtime.launch_collection.call_count == 1)
            self.runtime.collection_status.return_value = {
                "status": "running", "running": True, "pid": 123,
                "profile_id": "fixture-universal", "title": "Universal fixture",
                "model_state": "not_configured", "heartbeat_age_seconds": 2, "domain_modules": [],
            }
            click("status")
            until("document.querySelector('.civic-runtime-status').textContent.includes('running')")
            self.assertTrue(js("document.querySelector('[data-civic-screen=elections]').hidden"))
            self.assertEqual(js("getComputedStyle(document.querySelector('[data-civic-screen=elections]')).display"), "none")
            self.runtime.collection_status.return_value["domain_modules"] = ["elections"]
            click("status")
            until("document.querySelector('[data-civic-screen=elections]').hidden === false")
            self.runtime.collection_status.return_value.update(status="unavailable", domain_modules=[])
            click("status")
            until("document.querySelector('.civic-runtime-status').textContent.includes('unavailable')")
            self.assertFalse(js("document.querySelector('[data-civic-screen=elections]').hidden"))
            self.runtime.collection_status.return_value.update(status="running", domain_modules=[])
            js("document.querySelector('[data-civic-screen=elections]').click()")
            click("status")
            until("document.querySelector('[data-civic-screen=monitoring]').getAttribute('aria-current') === 'page'")
            click("report")
            self.pump_until(lambda: self.runtime.export_collection_report.call_count == 1)
            until("document.querySelector('.civic-report-paths')?.textContent.includes('fixture/report.md')")
            self.assertFalse(js("document.querySelector('[data-civic-control=report]').disabled"))
            self.assertTrue(js("document.querySelector('.civic-runtime-status').textContent.includes('running')"))
            self.runtime.request_stop.assert_not_called()
            js("document.querySelector('[data-civic-screen=claims]').click()")
            until("document.querySelector('.civic-report-paths')?.textContent.includes('fixture/report.md')")
            output = os.environ.get("CIVIC_CONTROLS_SCREENSHOT")
            if output:
                # Offscreen Chromium can retain stale pixels until a resize.
                size = window.size()
                window.resize(size.width() + 1, size.height())
                loop = QEventLoop()
                QTimer.singleShot(300, loop.quit)
                loop.exec()
                window.resize(size)
                loop = QEventLoop()
                QTimer.singleShot(1500, loop.quit)
                loop.exec()
                path = Path(output).resolve()
                path.parent.mkdir(parents=True, exist_ok=True)
                for _ in range(4):
                    window.view.repaint()
                    frame = window.view.grab()
                    loop = QEventLoop()
                    QTimer.singleShot(400, loop.quit)
                    loop.exec()
                self.assertTrue(frame.save(str(path)))
            window.resize(480, 900)
            until("document.querySelector('.civic-collection').getBoundingClientRect().right <= innerWidth + 1")
            click("stop")
            self.pump_until(lambda: self.runtime.request_stop.call_count == 1)
            self.runtime.request_stop.reset_mock()
        finally:
            window.close()
            QThreadPool.globalInstance().waitForDone(5000)
            window.deleteLater()
            QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
            APP.processEvents()
        self.runtime.request_stop.assert_not_called()


if __name__ == "__main__":
    unittest.main()
