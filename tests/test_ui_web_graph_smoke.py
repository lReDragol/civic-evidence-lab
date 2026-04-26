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

    def test_relation_map_overlay_matches_screen_panel_and_exposes_group_filters(self):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1760, "height": 1040})
                page.goto(UI_WEB_INDEX)
                page.wait_for_timeout(700)
                page.get_by_role("button", name="Аналитика").click()
                page.get_by_role("button", name="Связи").click()
                page.get_by_role("button", name="Карта").click()
                page.wait_for_timeout(450)

                overlay_box = page.locator("#relation-map-overlay-host").bounding_box()
                main_panel_box = page.locator(".main-panel").bounding_box()
                screen_panel_box = page.locator(".screen-panel").bounding_box()
                self.assertIsNotNone(overlay_box)
                self.assertIsNotNone(main_panel_box)
                self.assertIsNotNone(screen_panel_box)
                self.assertLessEqual(abs((overlay_box or {})["x"] - (screen_panel_box or {})["x"]), 4)
                self.assertLessEqual(abs((overlay_box or {})["y"] - (screen_panel_box or {})["y"]), 4)
                self.assertLessEqual(abs((overlay_box or {})["width"] - (screen_panel_box or {})["width"]), 4)
                expected_height = ((main_panel_box or {})["y"] + (main_panel_box or {})["height"]) - (screen_panel_box or {})["y"]
                self.assertLessEqual(abs((overlay_box or {})["height"] - expected_height), 6)

                chips = page.locator("[data-map-group]")
                self.assertGreaterEqual(chips.count(), 3)
                self.assertTrue(page.locator("[data-map-group='documents']").first.is_visible())
            finally:
                browser.close()

    def test_entity_drawer_uses_wide_overlay(self):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1760, "height": 1040})
                page.goto(UI_WEB_INDEX)
                page.wait_for_timeout(700)
                page.get_by_role("button", name="Аналитика").click()
                page.get_by_role("button", name="Сущности").click()
                page.wait_for_timeout(250)
                page.locator("[data-row-id]").first.click()
                page.wait_for_timeout(350)

                drawer_box = page.locator("[data-detail-drawer]").bounding_box()
                viewport_width = page.viewport_size["width"]
                self.assertIsNotNone(drawer_box)
                self.assertGreater((drawer_box or {})["width"], viewport_width * 0.47)
            finally:
                browser.close()


if __name__ == "__main__":
    unittest.main()
