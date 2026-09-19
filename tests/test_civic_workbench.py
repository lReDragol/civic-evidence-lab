from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
import unittest

from ui.query_service import CivicQueryService


ROOT = Path(__file__).resolve().parents[1]


class CivicFixtureSetup:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "reactor # fixture.db"
        conn = sqlite3.connect(self.path)
        for migration in ("0001_core.sql", "0003_election.sql", "0004_investigations.sql"):
            conn.executescript((ROOT / "db/reactor_migrations/knowledge" / migration).read_text(encoding="utf-8"))
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("INSERT INTO source_systems(id,source_key,source_type,title) VALUES(1,'fixture','test','Fixture')")
        conn.execute("INSERT INTO source_objects(id,source_system_id,external_id,first_seen_at,last_seen_at) VALUES(1,1,'source-one','2026-01-01','2026-09-20')")
        conn.execute("INSERT INTO source_revisions(id,source_object_id,revision_no,payload_hash,payload_json,fetched_at) VALUES(1,1,1,'fixture',?,'2026-09-20')", (json.dumps({"body": "x" * 100000}),))
        conn.executemany(
            "INSERT INTO civic_claims(id,source_revision_id,claim_key,claim_text,polarity,modality,attribution,locator_json,status,created_at) VALUES(?,1,?,?,'affirmed','asserted','Fixture','{}',?,'2026-09-20')",
            [(i, f"claim-{i}", "needle 100%_ " + "x" * 20000 if i <= 6 else f"ordinary-{i}", "supported" if i <= 6 else "unreviewed") for i in range(1, 84)],
        )
        conn.executemany("INSERT INTO civic_evidence_links(claim_id,source_revision_id,stance,locator_json,created_at) VALUES(6,1,'supports',?,'2026-09-20')", [(str(i),) for i in range(30)])
        conn.execute("INSERT INTO investigation_threads(id,thread_key,question,created_at) VALUES(1,'t1','Fixture question','2026-09-20')")
        conn.execute("INSERT INTO thread_revisions(id,thread_id,revision_no,operation,reason,actor,membership_json,created_at) VALUES(1,1,1,'create','Fixture','tester','{}','2026-09-20')")
        conn.execute("UPDATE investigation_threads SET current_revision_id=1 WHERE id=1")
        conn.execute("INSERT INTO election_campaigns VALUES(1,'campaign','Fixture campaign')")
        conn.execute("INSERT INTO election_ballots VALUES(1,1,'ballot','Fixture ballot')")
        conn.execute("INSERT INTO election_precincts VALUES(1,1,'region','123','deg')")
        conn.execute("INSERT INTO election_ballot_scopes VALUES(1,1,1,1)")
        conn.execute("INSERT INTO election_protocol_versions(id,scope_id,protocol_type,version_no,source_revision_id,provenance_group,document_locator,validation_state,validation_json) VALUES(1,1,'official',1,1,'origin','page:1','incomplete','{}')")
        conn.execute("INSERT INTO election_protocol_numbers(protocol_id,field_key,value,verification_state) VALUES(1,'ballots_cast',NULL,'missing')")
        conn.execute("INSERT INTO election_incidents(scope_id,candidate_key,kind,description,basis_json) VALUES(1,'c','difference','Unreviewed candidate','{}')")
        conn.execute("INSERT INTO election_claims(scope_id,source_revision_id,locator,attributed_to,stance,claim_text) VALUES(1,1,'page:1','Witness','alleges','Allegation, not guilt')")
        conn.execute("INSERT INTO entities(id,entity_type,canonical_name,canonical_key) VALUES(1,'person','One','one'),(2,'person','Two','two')")
        conn.execute("INSERT INTO projection_generations(id,projection_type,generation_key,status,started_at) VALUES(1,'relations','active','active','now'),(2,'relations','building','building','now')")
        conn.execute("INSERT INTO projection_generations(id,projection_type,generation_key,status,started_at) VALUES(3,'events','events','active','now'),(4,'facts','facts','active','now')")
        conn.execute("INSERT INTO events(id,generation_id,event_key,event_type,canonical_title,recorded_at) VALUES(1,3,'event','fixture','Fixture event','now')")
        conn.execute("INSERT INTO facts(id,generation_id,event_id,fact_key,fact_type,predicate,canonical_text,recorded_at) VALUES(1,4,1,'fact','fixture','knows','Fixture fact','now')")
        for i in (1, 2):
            conn.execute("INSERT INTO relation_assertions(id,generation_id,natural_key,subject_entity_id,predicate,object_entity_id,fact_id,event_id,polarity,state) VALUES(?,?,?,1,'knows',2,1,1,'positive','promoted')", (i, i, str(i)))
        conn.commit()
        conn.close()
        self.ops_path = Path(self.temp.name) / "reactor_ops.db"
        ops = sqlite3.connect(self.ops_path)
        for migration in ("0001_core.sql", "0002_task_fencing.sql"):
            ops.executescript((ROOT / "db/reactor_migrations/ops" / migration).read_text(encoding="utf-8"))
        now = datetime.now(timezone.utc).replace(microsecond=0)
        self.heartbeat = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        future, past = (now + timedelta(hours=1)).isoformat(), (now - timedelta(hours=1)).isoformat()
        for key, status, owner, token, expires, deadline in (
            ("active", "running", "worker", "token", future, future),
            ("expired", "running", "worker", "old", past, future),
            ("unfenced", "running", "worker", None, future, future),
            ("deadline", "running", "worker", "late", future, past),
            ("pending", "pending", None, None, None, None),
            ("retry", "needs_retry", None, None, None, None),
            ("failure", "failed", None, None, None, None),
        ):
            ops.execute("""INSERT INTO agent_tasks(task_key,task_type,requester_group,target_group,
                subject_type,subject_key,input_hash,status,lease_owner,lease_token,lease_expires_at,
                deadline_at,heartbeat_at,created_at,updated_at) VALUES(?,'fixture','test','test',
                'fixture','fixture','fixture',?,?,?,?,?,?,?,?)""",
                (key, status, owner, token, expires, deadline, self.heartbeat if key == "active" else None, self.heartbeat, self.heartbeat))
        ops.commit()
        ops.close()
        self.settings = {"civic_workbench_enabled": True, "reactor_db_dir": self.temp.name,
                         "reactor_knowledge_db": str(self.path), "reactor_ops_db": str(self.ops_path)}
        self.service = CivicQueryService(self.settings)

    def page(self, resource, **filters):
        return self.service.request({"resource": resource, "filters": filters})

    def detail(self, resource, object_id):
        return self.service.request({"resource": resource, "id": object_id}, detail=True)


class CivicFixture(CivicFixtureSetup, unittest.TestCase):
    def test_monitoring_real_fenced_ops_and_global_knowledge_counts(self):
        before = hashlib.sha256(self.ops_path.read_bytes()).digest()
        page = self.page("monitoring", query="does-not-match")
        self.assertEqual(page["total"], 0)
        summary = page["summary"]
        self.assertEqual(summary["knowledge"]["values"], {
            "sources": 1, "revisions": 1, "claims": 83, "evidence": 0, "evidence_links": 30, "threads": 1})
        self.assertEqual(summary["ops"]["availability"], "available")
        self.assertEqual(summary["ops"]["values"], {
            "active_fenced": 1, "pending": 2, "failed": 1, "last_heartbeat": self.heartbeat})
        self.assertEqual(hashlib.sha256(self.ops_path.read_bytes()).digest(), before)
        self.assertNotIn("lease_token", json.dumps(summary))

    def test_missing_ops_is_unavailable_not_zero_and_connections_close(self):
        self.page("monitoring")
        moved = self.ops_path.with_suffix(".saved")
        self.ops_path.rename(moved)
        summary = self.page("monitoring")["summary"]
        self.assertEqual(summary["ops"]["availability"], "unavailable")
        self.assertTrue(all(v is None for v in summary["ops"]["values"].values()))
        self.assertEqual(summary["knowledge"]["values"]["claims"], 83)
        self.assertFalse(self.ops_path.exists())
        moved.rename(self.ops_path)

    def test_empty_ops_is_known_zero_with_unknown_heartbeat(self):
        conn = sqlite3.connect(self.ops_path)
        conn.execute("DELETE FROM agent_tasks")
        conn.commit()
        conn.close()
        ops = self.page("monitoring")["summary"]["ops"]
        self.assertEqual(ops["availability"], "available")
        self.assertEqual(ops["values"], {"active_fenced": 0, "pending": 0, "failed": 0, "last_heartbeat": None})

    def test_ops_without_fencing_and_missing_knowledge_are_independent(self):
        old = Path(self.temp.name) / "old_ops.db"
        conn = sqlite3.connect(old)
        conn.executescript((ROOT / "db/reactor_migrations/ops/0001_core.sql").read_text(encoding="utf-8"))
        conn.close()
        self.settings["reactor_ops_db"] = str(old)
        self.assertEqual(self.page("monitoring")["summary"]["ops"]["availability"], "unavailable")
        self.settings["reactor_ops_db"] = str(self.ops_path)
        self.settings["reactor_knowledge_db"] = str(Path(self.temp.name) / "absent.db")
        summary = self.page("monitoring")["summary"]
        self.assertTrue(all(v is None for v in summary["knowledge"]["values"].values()))
        self.assertEqual(summary["ops"]["values"]["active_fenced"], 1)

    def test_default_off_and_missing_path_does_not_create_database(self):
        missing = Path(self.temp.name) / "not-created" / "absent.db"
        service = CivicQueryService({"reactor_knowledge_db": str(missing), "reactor_db_dir": self.temp.name})
        self.assertFalse(service.config()["enabled"])
        self.assertEqual(service.request({"resource": "monitoring"})["availability"], "unavailable")
        service.settings["civic_workbench_enabled"] = True
        self.assertEqual(service.request({"resource": "monitoring"})["availability"], "unavailable")
        self.assertFalse(missing.parent.exists())
        service.settings["civic_workbench_enabled"] = "true"
        self.assertFalse(service.enabled)

    def test_filters_precede_pagination_and_escape_like(self):
        first = self.page("claims", query="NEEDLE 100%_", status="supported", limit=2)
        self.assertEqual(first["total"], 6)
        self.assertEqual([r["id"] for r in first["items"]], [6, 5])
        second = self.page("claims", query="needle", status="supported", limit=2, cursor=first["next_cursor"])
        self.assertEqual([r["id"] for r in second["items"]], [4, 3])
        self.assertEqual(self.page("claims", date_from="2026-09-21")["total"], 0)
        self.assertEqual(self.page("claims", date_to="2026-09-20")["total"], 83)
        self.assertEqual(self.page("claims", revision_id=2)["total"], 0)

    def test_payload_caps_and_lazy_related_data(self):
        page = self.page("claims", limit=99999)
        self.assertEqual(len(page["items"]), 50)
        self.assertNotIn("detail", page)
        selected = self.page("claims", query="needle")
        self.assertLessEqual(len(selected["items"][0]["claim_text"]), 512)
        detail = self.detail("claims", 6)
        self.assertEqual(len(detail["detail"]["claim_text"]), 4096)
        links = detail["related"]["evidence_links"]
        self.assertEqual(len(links["items"]), 20)
        self.assertTrue(links["has_more"])
        self.assertLess(len(json.dumps(detail).encode()), 40000)
        self.assertNotIn("payload_json", self.page("monitoring")["items"][0])

    def test_empty_unavailable_and_bad_input_are_distinct(self):
        self.assertEqual(self.page("claims", query="does-not-exist")["availability"], "empty")
        self.assertEqual(self.detail("claims", 9999)["availability"], "empty")
        for payload in ({"resource": "claims", "filters": {"limit": "bad"}},
                        {"resource": "claims", "filters": {"cursor": "invalid"}},
                        {"resource": "claims", "filters": {"query": "x" * 257}},
                        {"resource": "claims", "filters": {"unsupported": "ignored?"}},
                        {"resource": "claims", "filters": [1]},
                        {"resource": "claims; DROP TABLE civic_claims"}):
            self.assertEqual(self.service.request(payload)["availability"], "unavailable")
        self.assertEqual(self.service.request({"resource": "claims"}, detail=True)["availability"], "unavailable")
        conn = sqlite3.connect(self.path)
        conn.execute("DROP TABLE investigation_threads")
        conn.close()
        self.assertEqual(self.page("threads")["availability"], "unavailable")

    def test_read_queries_leave_database_bytes_unchanged(self):
        before = hashlib.sha256(self.path.read_bytes()).digest()
        for resource in self.service.RESOURCES:
            self.page(resource)
            self.detail(resource, 1)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).digest(), before)

    def test_actual_schema_threads_and_election_provenance(self):
        thread = self.detail("threads", 1)
        self.assertEqual(thread["related"]["current_revision"]["items"][0]["operation"], "create")
        scope = self.detail("scopes", 1)
        self.assertEqual(scope["related"]["precinct"]["items"][0]["category"], "deg")
        protocol = self.detail("protocols", 1)
        self.assertIsNone(protocol["related"]["numbers"]["items"][0]["value"])
        self.assertEqual(protocol["related"]["acceptance"]["items"], [])
        self.assertEqual(self.page("incidents", status="candidate")["total"], 1)
        self.assertEqual(self.page("election_claims", status="alleges")["total"], 1)

    def test_graph_only_uses_active_promoted_assertions(self):
        page = self.page("graph")
        self.assertEqual([r["id"] for r in page["items"]], [1])
        self.assertEqual(len(page["graph"]["nodes"]), 2)
        self.assertEqual(page["graph"]["edges"][0]["source"], 1)
        self.assertEqual(page["items"][0]["subject_name"], "One")
        self.assertEqual(self.page("claim_evidence", claim_id=6)["total"], 30)

    def test_bridge_contract_and_oversized_request(self):
        try:
            from ui.web_bridge import DashboardBridge, DashboardDataService
        except ImportError:
            self.skipTest("PySide6 unavailable")
        legacy = sqlite3.connect(":memory:")
        self.addCleanup(legacy.close)
        bridge = DashboardBridge(DashboardDataService(legacy, self.settings), None)
        # Enabled refresh must not touch controller/legacy bootstrap work.
        bridge.emit_bootstrap()
        self.assertTrue(json.loads(bridge.getCivicConfig())["enabled"])
        page = json.loads(bridge.getCivicPage(json.dumps({"resource": "claims", "filters": {"limit": 1}})))
        self.assertEqual(len(page["items"]), 1)
        self.assertEqual(json.loads(bridge.getCivicPage("x" * 17000))["availability"], "unavailable")
        self.assertEqual(json.loads(bridge.getCivicDetail('{"resource":"claims","id":1}'))["detail"]["id"], 1)


class CivicStaticTests(unittest.TestCase):
    def test_assets_and_actual_backend_gate_are_wired(self):
        html = (ROOT / "ui_web/index.html").read_text(encoding="utf-8")
        app = (ROOT / "ui_web/app.js").read_text(encoding="utf-8")
        js = (ROOT / "ui_web/reactor_v2.js").read_text(encoding="utf-8")
        self.assertLess(html.index('./reactor_v2.js'), html.index('./app.js'))
        self.assertIn('bridgeCall("getCivicConfig")', app)
        self.assertIn('civicConfig?.enabled === true', app)
        self.assertIn('mountCivic', app)
        self.assertIn('getCivicPage', js)
        self.assertIn('getCivicDetail', js)
        self.assertNotIn('["Pipeline", "Live"]', js)

    @unittest.skipUnless(shutil.which("node"), "Node unavailable")
    def test_node_syntax_and_out_of_order_responses(self):
        for name in ("reactor_v2.js", "app.js"):
            subprocess.run(["node", "--check", str(ROOT / "ui_web" / name)], check=True, capture_output=True)
        script = r"""
const fs = require('fs'), vm = require('vm'), assert = require('assert');
const window = {};
vm.runInNewContext(fs.readFileSync(process.argv[1], 'utf8'), {window, document:{}, AbortController});
const Coordinator = window.CELReactorV2.RequestCoordinator;
(async () => {
  const c = new Coordinator();
  let oldResolve, newResolve, oldReject;
  const old = c.run('page', () => new Promise(r => oldResolve=r));
  const recent = c.run('page', () => new Promise(r => newResolve=r));
  newResolve('new'); assert.equal((await recent).value, 'new');
  oldResolve('old'); assert.equal((await old).accepted, false);
  const detail = c.run('detail', () => new Promise(r => oldResolve=r));
  c.cancelAll(); oldResolve('gone'); assert.equal((await detail).accepted, false);
  const failed = c.run('page', () => new Promise((r, reject) => oldReject=reject));
  await c.run('page', async () => 'newer'); oldReject(new Error('stale failure'));
  assert.equal((await failed).accepted, false);
  console.log('sequencing: OK');
})().catch(error => { console.error(error); process.exitCode=1; });
"""
        result = subprocess.run(["node", "-e", script, str(ROOT / "ui_web/reactor_v2.js")], capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("sequencing: OK", result.stdout)


@unittest.skipUnless(os.environ.get("CIVIC_QT_SMOKE") == "1", "Set CIVIC_QT_SMOKE=1 for isolated offscreen WebChannel smoke")
class CivicQtSmoke(CivicFixtureSetup, unittest.TestCase):
    def test_actual_webchannel_fixture_screen_filter_and_inspector(self):
        os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
        os.environ.setdefault("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-gpu")
        from PySide6.QtCore import QEventLoop, QTimer, QUrl
        from PySide6.QtWebChannel import QWebChannel
        from PySide6.QtWebEngineCore import QWebEnginePage
        from PySide6.QtWebEngineWidgets import QWebEngineView
        from PySide6.QtWidgets import QApplication
        from ui.web_bridge import DashboardBridge, DashboardDataService

        app = QApplication.instance() or QApplication([])
        legacy = sqlite3.connect(":memory:")
        self.addCleanup(legacy.close)
        # Override the default-off flag only for this temporary fixture service.
        fixture_settings = {**self.settings, "civic_workbench_enabled": True}
        bridge = DashboardBridge(DashboardDataService(legacy, fixture_settings), None)
        screenshot_dir = os.environ.get("CIVIC_UI_SCREENSHOT_DIR")
        output_dir = Path(screenshot_dir).resolve() if screenshot_dir else None
        report = {"started_at": datetime.now(timezone.utc).isoformat(),
                  "fixture_only": True, "feature_flag_override": True,
                  "screenshots": [], "console_messages": [], "errors": [], "passed": False}
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)

        class SmokePage(QWebEnginePage):
            def javaScriptConsoleMessage(self, level, message, line, source):
                report["console_messages"].append({"level": str(level), "message": message,
                                                   "line": line, "source": source})

        view = QWebEngineView()
        view.setPage(SmokePage(view))
        view.resize(1280, 900)
        channel = QWebChannel(view.page())
        channel.registerObject("dashboardBridge", bridge)
        view.page().setWebChannel(channel)
        view.load(QUrl.fromLocalFile(str(ROOT / "ui_web/index.html")))
        view.show()

        def js(expression):
            loop = QEventLoop()
            result = []
            view.page().runJavaScript(expression, lambda value: (result.append(value), loop.quit()))
            QTimer.singleShot(3000, loop.quit)
            loop.exec()
            return result[0] if result else None

        def until(expression):
            for _ in range(100):
                if js(expression):
                    return
                loop = QEventLoop()
                QTimer.singleShot(50, loop.quit)
                loop.exec()
            self.fail(f"Qt fixture did not reach state: {expression}; body={js('document.body.innerText')}")

        def capture(name, expected_state):
            if not output_dir:
                return
            # Wait for layout and Chromium paint before grabbing the viewport.
            js("window.__civicCaptureReady=false; requestAnimationFrame(() => requestAnimationFrame(() => {window.__civicCaptureReady=true;}))")
            until("window.__civicCaptureReady === true")
            def paint_wait(milliseconds):
                loop = QEventLoop()
                QTimer.singleShot(milliseconds, loop.quit)
                loop.exec()

            # Software/offscreen WebEngine may acknowledge rAF before updating
            # QWidget's backing store. Invalidate its size, then wait for stable
            # pixels at the original viewport rather than saving a stale frame.
            size = view.size()
            view.resize(size.width() + 1, size.height())
            paint_wait(300)
            view.resize(size)
            view.update()
            paint_wait(1200)
            path = output_dir / f"{name}.png"
            previous_hash, stable = None, 0
            for _ in range(12):
                view.repaint()
                pixmap = view.grab()
                frame = pixmap.toImage()
                frame_hash = hashlib.sha256(bytes(frame.constBits())).hexdigest()
                stable = stable + 1 if frame_hash == previous_hash else 0
                previous_hash = frame_hash
                if stable >= 2:
                    break
                paint_wait(400)
            self.assertGreaterEqual(stable, 2, f"Viewport did not settle: {name}")
            self.assertFalse(pixmap.isNull(), f"Empty Qt viewport: {name}")
            self.assertTrue(pixmap.save(str(path)), f"Could not save {path}")
            state = js("JSON.stringify({title:document.querySelector('.civic-heading h1')?.textContent,status:document.querySelector('.civic-status')?.textContent,rows:document.querySelectorAll('.civic-table tbody tr').length,inspectorOpen:!document.querySelector('.civic-inspector')?.hidden,width:innerWidth,height:innerHeight})")
            report["screenshots"].append({"path": str(path), "expected_state": expected_state,
                                          "pixel_sha256": frame_hash,
                                          "viewport": json.loads(state or "{}")})

        try:
            until("document.querySelector('#civic-workbench .civic-table tbody tr') !== null")
            self.assertTrue(js("document.getElementById('app-shell').hidden"))
            capture("01-monitoring", "available")
            self.assertEqual(js("document.querySelector('[data-metric=active_fenced] strong').textContent"), "1")
            self.assertGreaterEqual(js("parseFloat(getComputedStyle(document.querySelector('[data-civic-screen=monitoring]')).fontSize)"), 12)
            self.assertGreaterEqual(js("document.querySelector('.civic-shell .reactor-v2-nav-rail').getBoundingClientRect().width"), 120)
            self.assertTrue(js("document.querySelector('[data-summary=ops]').getBoundingClientRect().right <= innerWidth"))
            self.assertGreaterEqual(js("parseFloat(getComputedStyle(document.querySelector('.civic-table td')).fontSize)"), 12)
            fixture_settings["reactor_ops_db"] = str(Path(self.temp.name) / "missing-ops.db")
            try:
                js("document.querySelector('[data-civic-screen=monitoring]').click()")
                until("document.querySelector('[data-summary=ops]')?.dataset.availability === 'unavailable'")
                self.assertNotEqual(js("document.querySelector('[data-metric=active_fenced] strong').textContent"), "0")
                capture("13-monitoring-ops-unavailable", "knowledge available; ops unavailable, not zeros")
            finally:
                fixture_settings["reactor_ops_db"] = str(self.ops_path)
            for number, section, expected in ((2, "elections", "available"), (3, "threads", "available"),
                                               (4, "evidence", "empty"), (5, "claims", "available"),
                                               (6, "graph", "available"), (7, "review", "empty")):
                js(f"document.querySelector('[data-civic-screen={section}]').click()")
                until("/matching records|^Empty:/.test(document.querySelector('.civic-status').textContent)")
                capture(f"{number:02d}-{section}", expected)
            js("document.querySelector('[data-civic-screen=claims]').click()")
            until("document.querySelector('.civic-status').textContent.includes('83 matching')")
            js("document.querySelector('.civic-filters [name=query]').value='needle'; document.querySelector('.civic-filters').requestSubmit()")
            until("document.querySelector('.civic-status').textContent.includes('6 matching')")
            self.assertEqual(js("document.querySelectorAll('.civic-table tbody tr').length"), 6)
            capture("08-claims-filtered", "available")
            self.assertTrue(js("document.querySelector('.civic-inspector').hidden"))
            js("document.querySelector('.civic-table tbody button').click()")
            until("document.querySelector('.civic-inspector').textContent.includes('evidence links')")
            self.assertFalse(js("document.querySelector('.civic-inspector').hidden"))
            self.assertTrue(js("document.querySelector('.civic-inspector dl') !== null"))
            self.assertTrue(js("Array.from(document.querySelectorAll('.civic-text-preview')).every(node => node.textContent.length <= 300)"))
            self.assertFalse(js("document.querySelector('.civic-debug-json').open"))
            self.assertEqual(js("document.querySelectorAll('.civic-debug-json pre').length"), 0)
            self.assertEqual(js("document.querySelector('[data-related=evidence_links] [data-field=stance] dd').textContent"), "supports")
            self.assertFalse(js("document.querySelector('.civic-related-more').open"))
            js("document.querySelector('.civic-long-text').open=true")
            until("document.querySelector('.civic-long-text pre') !== null")
            self.assertEqual(js("document.querySelector('.civic-long-text pre').textContent.length"), 4096)
            js("document.querySelector('.civic-long-text').open=false")
            capture("09-claims-inspector", "available")
            js("document.querySelector('[data-civic-screen=threads]').click()")
            until("document.querySelector('.civic-table')?.textContent.includes('Fixture question')")
            js("document.querySelector('[data-civic-screen=graph]').click()")
            until("document.querySelector('canvas') !== null")
            view.resize(480, 800)
            until("document.querySelector('.civic-shell .main-panel').getBoundingClientRect().right <= innerWidth + 1")
            capture("10-graph-mobile", "available")
            view.resize(1280, 900)
            js("document.querySelector('[data-civic-screen=claims]').click()")
            until("document.querySelector('.civic-status').textContent.includes('83 matching')")
            js("document.querySelector('.civic-filters [name=query]').value='no-fixture-match'; document.querySelector('.civic-filters').requestSubmit()")
            until("document.querySelector('.civic-status').textContent.startsWith('Empty:')")
            capture("11-claims-empty", "empty: deliberate nonmatching filter")
            # Simulate missing storage only in the service's in-memory settings.
            # No live settings, DB migrations, or application launcher are used.
            fixture_settings["reactor_knowledge_db"] = str(Path(self.temp.name) / "missing.db")
            try:
                js("document.querySelector('[data-civic-screen=monitoring]').click()")
                until("document.querySelector('.civic-status').textContent.startsWith('Unavailable:')")
                capture("12-monitoring-unavailable", "unavailable: deliberate missing fixture DB")
            finally:
                fixture_settings["reactor_knowledge_db"] = str(self.path)
            self.assertEqual(legacy.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()[0], 0)
            self.assertFalse((Path(self.temp.name) / "missing.db").exists())
            report["passed"] = True
        except Exception as exc:
            report["errors"].append(f"{type(exc).__name__}: {exc}")
            try:
                capture("99-failure", "unexpected failure")
            except Exception as capture_error:
                report["errors"].append(f"Capture failure: {capture_error}")
            raise
        finally:
            if output_dir:
                report["finished_at"] = datetime.now(timezone.utc).isoformat()
                (output_dir / "manifest.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            view.close()
            view.deleteLater()
            app.processEvents()


if __name__ == "__main__":
    unittest.main()
