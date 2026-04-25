import re
import unittest
from pathlib import Path

from playwright.sync_api import sync_playwright


PROJECT_ROOT = Path(__file__).resolve().parents[1]
UI_WEB_INDEX = (PROJECT_ROOT / "ui_web" / "index.html").resolve().as_uri()
CUBIC_PATH_RE = re.compile(
    r"^M\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+C\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)"
    r",\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)"
    r",\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)$"
)


def parse_cubic_path(path_d: str) -> tuple[float, float, float, float, float, float, float, float]:
    match = CUBIC_PATH_RE.match(path_d.strip())
    if not match:
        raise AssertionError(f"Unexpected SVG cubic path: {path_d!r}")
    return tuple(float(value) for value in match.groups())


def cubic_is_monotonic(path_d: str) -> bool:
    sx, sy, c1x, c1y, c2x, c2y, ex, ey = parse_cubic_path(path_d)
    if abs(ex - sx) >= abs(ey - sy):
        low, high = sorted((sx, ex))
        return low <= c1x <= high and low <= c2x <= high
    low, high = sorted((sy, ey))
    return low <= c1y <= high and low <= c2y <= high


class UiWebGraphSmokeTests(unittest.TestCase):
    def test_relation_detail_graph_uses_monotonic_curves(self):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1600, "height": 1000})
                page.goto(UI_WEB_INDEX)
                page.wait_for_timeout(700)
                page.get_by_role("button", name="Аналитика").click()
                page.get_by_role("button", name="Связи").click()
                page.wait_for_timeout(250)
                page.locator("[data-row-id]").first.click()
                page.wait_for_timeout(400)

                path_d = page.locator(
                    "[data-graph-root] .node-graph-edge-group[data-edge-label='источник связи'] .node-graph-edge"
                ).first.get_attribute("d")
                self.assertIsNotNone(path_d)
                self.assertTrue(
                    cubic_is_monotonic(path_d or ""),
                    msg=f"Evidence curve should stay monotonic after layout, got: {path_d}",
                )

                context_node = page.locator(
                    "[data-graph-root] .node-graph-node[data-node-label='Контекст']"
                ).first
                context_node.hover()
                page.mouse.down()
                page.mouse.move(300, 300, steps=16)
                page.mouse.up()
                page.wait_for_timeout(220)

                dragged_path_d = page.locator(
                    "[data-graph-root] .node-graph-edge-group[data-edge-label='контекст'] .node-graph-edge"
                ).first.get_attribute("d")
                self.assertIsNotNone(dragged_path_d)
                self.assertTrue(
                    cubic_is_monotonic(dragged_path_d or ""),
                    msg=f"Evidence curve should stay monotonic after drag, got: {dragged_path_d}",
                )
            finally:
                browser.close()


if __name__ == "__main__":
    unittest.main()
