import re
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CSS_PATH = PROJECT_ROOT / "ui_web" / "reactor_v2.css"
JS_PATH = PROJECT_ROOT / "ui_web" / "reactor_v2.js"


class ReactorV2StaticContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.css = CSS_PATH.read_text(encoding="utf-8")
        cls.js = JS_PATH.read_text(encoding="utf-8")

    def test_feature_flagged_initializer_is_opt_in(self):
        self.assertIn('const FLAG_NAME = "reactor_v2"', self.js)
        self.assertIn('const STORAGE_KEY = "cel.feature.reactor_v2"', self.js)
        self.assertIn("function isFeatureEnabled", self.js)
        self.assertIn("if (isFeatureEnabled())", self.js)
        self.assertIn("global.CELReactorV2 = api", self.js)

    def test_shell_workspace_and_inspector_contracts_exist(self):
        for contract in (
            "reactor-v2-nav-rail",
            "reactor-v2-workspace",
            "reactor-v2-workbench",
            "reactor-v2-inspector-host",
        ):
            self.assertIn(contract, self.js)
            self.assertIn(contract, self.css)
        self.assertIn('setAttribute("role", "dialog")', self.js)
        self.assertIn('setAttribute("aria-modal", "false")', self.js)
        self.assertIn("--rv2-inspector-width", self.css)

    def test_review_ops_and_operations_cockpit_are_actionable(self):
        for action in ("approve", "reject", "merge", "split", "defer", "request_evidence"):
            self.assertIn(f'["{action}"', self.js)
        self.assertIn("reactor-v2:review-action", self.js)
        self.assertIn("reactor-v2-operations-cockpit", self.js)
        self.assertIn("reactor-v2:refresh-operations", self.js)

    def test_request_coordinator_sequences_and_cancels_stale_requests(self):
        self.assertIn("class RequestCoordinator", self.js)
        self.assertIn("new AbortController()", self.js)
        self.assertIn("requestId = ++this.sequence", self.js)
        self.assertIn("active.requestId !== requestId", self.js)
        self.assertIn("controller.signal.aborted", self.js)
        self.assertIn("cancelAll()", self.js)

    def test_relation_viewport_uses_one_canvas_not_dom_or_svg_nodes(self):
        self.assertIn("class RelationCanvasViewport", self.js)
        self.assertIn('document.createElement("canvas")', self.js)
        self.assertIn('this.canvas.getContext("2d")', self.js)
        self.assertIn("context.arc(", self.js)
        self.assertIn("host.append(this.root)", self.js)
        self.assertIn("this.returnMarker.before(this.root)", self.js)
        self.assertNotIn("cloneNode", self.js)
        self.assertNotIn("createElementNS", self.js)
        self.assertNotRegex(self.js.lower(), re.compile(r"<\s*svg\b"))
        self.assertNotIn("relation-node-element", self.js)

    def test_fullscreen_moves_the_single_viewport_and_is_accessible(self):
        self.assertIn("enterFullscreen()", self.js)
        self.assertIn("exitFullscreen()", self.js)
        self.assertIn('host.setAttribute("aria-modal", "true")', self.js)
        self.assertIn('if (event.key === "Escape")', self.js)
        self.assertIn('if (event.key === "Tab")', self.js)
        self.assertIn("host.append(this.root)", self.js)
        self.assertIn("this.lastFocusedElement?.focus?.()", self.js)

    def test_canvas_resize_has_a_webview_fallback(self):
        self.assertIn('typeof global.ResizeObserver === "function"', self.js)
        self.assertIn('global.addEventListener("resize", this.resizeFallback)', self.js)
        self.assertIn('global.removeEventListener("resize", this.resizeFallback)', self.js)

    def test_responsive_and_reduced_motion_contracts_exist(self):
        self.assertIn("@media (max-width: 760px)", self.css)
        self.assertIn("@container reactor-workspace", self.css)
        self.assertIn("@media (prefers-reduced-motion: reduce)", self.css)
        self.assertIn(":focus-visible", self.css)


if __name__ == "__main__":
    unittest.main()
