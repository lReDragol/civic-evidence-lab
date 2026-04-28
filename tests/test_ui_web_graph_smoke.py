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
    def test_events_screen_uses_full_height_and_split_drawer(self):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1920, "height": 1040})
                page.goto(UI_WEB_INDEX)
                page.wait_for_timeout(700)
                page.get_by_role("button", name="Аналитика").click()
                page.get_by_role("button", name="События").click()
                page.wait_for_timeout(350)
                page.locator("[data-row-id]").first.click()
                page.wait_for_timeout(350)

                main_panel_box = page.locator(".main-panel").bounding_box()
                screen_panel_box = page.locator(".screen-panel").bounding_box()
                list_wrap_box = page.locator(".master-list-wrap").bounding_box()
                drawer_box = page.locator("[data-detail-drawer]").bounding_box()

                self.assertIsNotNone(main_panel_box)
                self.assertIsNotNone(screen_panel_box)
                self.assertIsNotNone(list_wrap_box)
                self.assertIsNotNone(drawer_box)

                self.assertGreater((screen_panel_box or {})["height"], (main_panel_box or {})["height"] * 0.68)
                self.assertGreater((list_wrap_box or {})["height"], (screen_panel_box or {})["height"] * 0.68)
                self.assertGreater((drawer_box or {})["width"], (list_wrap_box or {})["width"] * 0.46)
                self.assertLess((drawer_box or {})["width"], (list_wrap_box or {})["width"] * 0.60)
            finally:
                browser.close()

    def test_claims_screen_uses_full_height_and_split_drawer(self):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1920, "height": 1040})
                page.goto(UI_WEB_INDEX)
                page.wait_for_timeout(700)
                page.get_by_role("button", name="Проверка").click()
                page.get_by_role("button", name="Заявления").click()
                page.wait_for_timeout(350)
                page.locator("[data-row-id]").first.click()
                page.wait_for_timeout(350)

                main_panel_box = page.locator(".main-panel").bounding_box()
                screen_panel_box = page.locator(".screen-panel").bounding_box()
                list_wrap_box = page.locator(".master-list-wrap").bounding_box()
                drawer_box = page.locator("[data-detail-drawer]").bounding_box()

                self.assertIsNotNone(main_panel_box)
                self.assertIsNotNone(screen_panel_box)
                self.assertIsNotNone(list_wrap_box)
                self.assertIsNotNone(drawer_box)

                self.assertGreater((screen_panel_box or {})["height"], (main_panel_box or {})["height"] * 0.68)
                self.assertGreater((list_wrap_box or {})["height"], (screen_panel_box or {})["height"] * 0.68)
                self.assertGreater((drawer_box or {})["width"], (list_wrap_box or {})["width"] * 0.46)
                self.assertLess((drawer_box or {})["width"], (list_wrap_box or {})["width"] * 0.60)
            finally:
                browser.close()

    def test_cases_screen_uses_full_height_and_split_drawer(self):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1920, "height": 1040})
                page.goto(UI_WEB_INDEX)
                page.wait_for_timeout(700)
                page.get_by_role("button", name="Проверка").click()
                page.get_by_role("button", name="Дела").click()
                page.wait_for_timeout(350)
                page.locator("[data-row-id]").first.click()
                page.wait_for_timeout(350)

                main_panel_box = page.locator(".main-panel").bounding_box()
                screen_panel_box = page.locator(".screen-panel").bounding_box()
                list_wrap_box = page.locator(".master-list-wrap").bounding_box()
                drawer_box = page.locator("[data-detail-drawer]").bounding_box()

                self.assertIsNotNone(main_panel_box)
                self.assertIsNotNone(screen_panel_box)
                self.assertIsNotNone(list_wrap_box)
                self.assertIsNotNone(drawer_box)

                self.assertGreater(
                    (screen_panel_box or {})["height"],
                    (main_panel_box or {})["height"] * 0.68,
                    msg=f"screen-panel should occupy the main working area, got {screen_panel_box} vs {main_panel_box}",
                )
                self.assertGreater(
                    (list_wrap_box or {})["height"],
                    (screen_panel_box or {})["height"] * 0.72,
                    msg=f"master list area should keep full working height, got {list_wrap_box} vs {screen_panel_box}",
                )
                self.assertGreater(
                    (drawer_box or {})["width"],
                    (list_wrap_box or {})["width"] * 0.46,
                    msg=f"detail drawer should occupy about the right half of the work area, got {drawer_box} vs {list_wrap_box}",
                )
                self.assertLess(
                    (drawer_box or {})["width"],
                    (list_wrap_box or {})["width"] * 0.60,
                    msg=f"detail drawer should stay close to a half split, got {drawer_box} vs {list_wrap_box}",
                )
            finally:
                browser.close()

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
                page = browser.new_page(viewport={"width": 1920, "height": 1040})
                page.goto(UI_WEB_INDEX)
                page.wait_for_timeout(700)
                page.get_by_role("button", name="Аналитика").click()
                page.get_by_role("button", name="Связи").click()
                page.get_by_role("button", name="Карта").click()
                page.wait_for_timeout(450)

                overlay_box = page.locator("#relation-map-overlay-host").bounding_box()
                main_panel_box = page.locator(".main-panel").bounding_box()
                screen_panel_box = page.locator(".screen-panel").bounding_box()
                stage_box = page.locator(".relation-map-stage").bounding_box()
                surface_box = page.locator(".relation-map-surface").bounding_box()
                viewport_box = page.locator(".relation-map-viewport").bounding_box()
                self.assertIsNotNone(overlay_box)
                self.assertIsNotNone(main_panel_box)
                self.assertIsNotNone(screen_panel_box)
                self.assertIsNotNone(stage_box)
                self.assertIsNotNone(surface_box)
                self.assertIsNotNone(viewport_box)

                self.assertGreater(
                    (screen_panel_box or {})["height"],
                    (main_panel_box or {})["height"] * 0.68,
                    msg=f"screen-panel should keep full working height in map mode, got {screen_panel_box} vs {main_panel_box}",
                )
                self.assertLessEqual(abs((overlay_box or {})["x"] - (screen_panel_box or {})["x"]), 4)
                self.assertLessEqual(abs((overlay_box or {})["y"] - (screen_panel_box or {})["y"]), 4)
                self.assertLessEqual(abs((overlay_box or {})["width"] - (screen_panel_box or {})["width"]), 4)
                self.assertLessEqual(abs((overlay_box or {})["height"] - (screen_panel_box or {})["height"]), 6)
                self.assertLessEqual(abs((stage_box or {})["x"] - (overlay_box or {})["x"]), 4)
                self.assertLessEqual(abs((stage_box or {})["width"] - (overlay_box or {})["width"]), 4)
                stage_bottom = (stage_box or {})["y"] + (stage_box or {})["height"]
                overlay_bottom = (overlay_box or {})["y"] + (overlay_box or {})["height"]
                self.assertLessEqual(abs(stage_bottom - overlay_bottom), 4)
                self.assertGreater((stage_box or {})["height"], (overlay_box or {})["height"] * 0.72)
                self.assertLessEqual(abs((surface_box or {})["height"] - (stage_box or {})["height"]), 24)
                self.assertGreater((viewport_box or {})["height"], (surface_box or {})["height"] * 0.55)

                chips = page.locator("[data-map-group]")
                self.assertGreaterEqual(chips.count(), 3)
                self.assertTrue(page.locator("[data-map-group='documents']").first.is_visible())
            finally:
                browser.close()

    def test_relation_map_interaction_keeps_layout_bounds_stable(self):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1920, "height": 1040})
                page.goto(UI_WEB_INDEX)
                page.wait_for_timeout(700)
                page.get_by_role("button", name="Аналитика").click()
                page.get_by_role("button", name="Связи").click()
                page.get_by_role("button", name="Карта").click()
                page.wait_for_timeout(700)

                host_before = page.locator("#relation-map-overlay-host").bounding_box()
                top_panel_before = page.locator(".top-panel").bounding_box()
                self.assertIsNotNone(host_before)
                self.assertIsNotNone(top_panel_before)

                node = page.locator(".relation-map-viewport .node-graph-node").first
                node.click()
                page.wait_for_timeout(220)
                self.assertTrue(page.locator(".node-graph-popover.open").first.is_visible())

                node.hover()
                page.mouse.down()
                page.mouse.move(1080, 540, steps=18)
                page.mouse.up()
                page.wait_for_timeout(320)

                host_after = page.locator("#relation-map-overlay-host").bounding_box()
                top_panel_after = page.locator(".top-panel").bounding_box()
                self.assertIsNotNone(host_after)
                self.assertIsNotNone(top_panel_after)

                for key in ("x", "y", "width", "height"):
                    self.assertLessEqual(
                        abs((host_after or {})[key] - (host_before or {})[key]),
                        4,
                        msg=f"relation map overlay bounds should stay stable after interaction for {key}: {host_before} -> {host_after}",
                    )
                    self.assertLessEqual(
                        abs((top_panel_after or {})[key] - (top_panel_before or {})[key]),
                        4,
                        msg=f"top panel bounds should stay stable after map interaction for {key}: {top_panel_before} -> {top_panel_after}",
                    )
            finally:
                browser.close()

    def test_relation_map_preserves_viewport_when_shell_panels_toggle(self):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1920, "height": 1040})
                page.goto(UI_WEB_INDEX)
                page.wait_for_timeout(700)
                page.get_by_role("button", name="Аналитика").click()
                page.get_by_role("button", name="Связи").click()
                page.get_by_role("button", name="Карта").click()
                page.wait_for_timeout(700)

                zoom_in = page.locator(
                    ".relation-map-stage [data-graph-root][data-graph-mode='relation-map'] [data-graph-action='zoom-in']"
                ).first
                zoom_in.click()
                zoom_in.click()
                page.wait_for_timeout(180)
                transform_before = page.locator(".relation-map-stage [data-graph-scene]").first.get_attribute("style")

                page.get_by_role("button", name="Панель").click()
                page.wait_for_timeout(320)
                transform_after_tasks = page.locator(".relation-map-stage [data-graph-scene]").first.get_attribute("style")
                page.get_by_role("button", name="Источники").click()
                page.wait_for_timeout(320)
                transform_after_sources = page.locator(".relation-map-stage [data-graph-scene]").first.get_attribute("style")

                self.assertEqual(
                    transform_after_tasks,
                    transform_before,
                    msg="opening the task drawer should resize map bounds without resetting user zoom/pan",
                )
                self.assertEqual(
                    transform_after_sources,
                    transform_before,
                    msg="opening the sources drawer should resize map bounds without resetting user zoom/pan",
                )
            finally:
                browser.close()

    def test_entity_drawer_uses_full_height_and_wide_overlay(self):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1920, "height": 1040})
                page.goto(UI_WEB_INDEX)
                page.wait_for_timeout(700)
                page.get_by_role("button", name="Аналитика").click()
                page.get_by_role("button", name="Сущности").click()
                page.wait_for_timeout(250)
                page.locator("[data-row-id]").first.click()
                page.wait_for_timeout(350)

                main_panel_box = page.locator(".main-panel").bounding_box()
                screen_panel_box = page.locator(".screen-panel").bounding_box()
                list_wrap_box = page.locator(".master-list-wrap").bounding_box()
                drawer_box = page.locator("[data-detail-drawer]").bounding_box()
                self.assertIsNotNone(main_panel_box)
                self.assertIsNotNone(screen_panel_box)
                self.assertIsNotNone(list_wrap_box)
                self.assertIsNotNone(drawer_box)
                self.assertGreater((screen_panel_box or {})["height"], (main_panel_box or {})["height"] * 0.68)
                self.assertGreater((list_wrap_box or {})["height"], (screen_panel_box or {})["height"] * 0.72)
                self.assertGreater((drawer_box or {})["width"], (list_wrap_box or {})["width"] * 0.46)
                self.assertLess((drawer_box or {})["width"], (list_wrap_box or {})["width"] * 0.60)
            finally:
                browser.close()


if __name__ == "__main__":
    unittest.main()
