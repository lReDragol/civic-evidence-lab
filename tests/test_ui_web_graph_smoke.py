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
    def assert_full_height_drawer(self, page):
        screen_panel_box = page.locator(".screen-panel").bounding_box()
        list_wrap_box = page.locator(".master-list-wrap").bounding_box()
        drawer_box = page.locator("[data-detail-drawer]").bounding_box()

        self.assertIsNotNone(screen_panel_box)
        self.assertIsNotNone(list_wrap_box)
        self.assertIsNotNone(drawer_box)

        screen_bottom = (screen_panel_box or {})["y"] + (screen_panel_box or {})["height"]
        drawer_bottom = (drawer_box or {})["y"] + (drawer_box or {})["height"]
        self.assertLessEqual(
            abs((drawer_box or {})["y"] - (screen_panel_box or {})["y"]),
            18,
            msg=f"detail drawer should start at the screen-panel top, got {drawer_box} vs {screen_panel_box}",
        )
        self.assertLessEqual(
            abs(drawer_bottom - screen_bottom),
            18,
            msg=f"detail drawer should extend to screen-panel bottom, got {drawer_box} vs {screen_panel_box}",
        )
        self.assertGreater(
            (drawer_box or {})["width"],
            (screen_panel_box or {})["width"] * 0.50,
            msg=f"detail drawer should occupy the right half of screen-panel, got {drawer_box} vs {screen_panel_box}",
        )
        self.assertLess(
            (drawer_box or {})["width"],
            (screen_panel_box or {})["width"] * 0.56,
            msg=f"detail drawer should stay close to 52% of screen-panel, got {drawer_box} vs {screen_panel_box}",
        )
        self.assertLess(
            (list_wrap_box or {})["x"] + (list_wrap_box or {})["width"],
            (drawer_box or {})["x"] + 12,
            msg=f"master list should stay in the left split, got {list_wrap_box} vs {drawer_box}",
        )

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

                self.assertIsNotNone(main_panel_box)
                self.assertIsNotNone(screen_panel_box)

                self.assertGreater((screen_panel_box or {})["height"], (main_panel_box or {})["height"] * 0.68)
                self.assert_full_height_drawer(page)
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

                self.assertIsNotNone(main_panel_box)
                self.assertIsNotNone(screen_panel_box)

                self.assertGreater((screen_panel_box or {})["height"], (main_panel_box or {})["height"] * 0.68)
                self.assert_full_height_drawer(page)
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

                self.assertIsNotNone(main_panel_box)
                self.assertIsNotNone(screen_panel_box)

                self.assertGreater(
                    (screen_panel_box or {})["height"],
                    (main_panel_box or {})["height"] * 0.68,
                    msg=f"screen-panel should occupy the main working area, got {screen_panel_box} vs {main_panel_box}",
                )
                self.assert_full_height_drawer(page)
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

    def test_relation_map_stays_inside_screen_root_and_exposes_fullscreen(self):
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

                screen_panel_box = page.locator(".screen-panel").bounding_box()
                screen_heading_box = page.locator(".screen-heading").bounding_box()
                screen_root_box = page.locator("#screen-root").bounding_box()
                map_screen_box = page.locator("#screen-root .relation-map-screen").bounding_box()
                stage_box = page.locator(".relation-map-stage").bounding_box()
                surface_box = page.locator(".relation-map-surface").bounding_box()
                viewport_box = page.locator(".relation-map-viewport").bounding_box()
                self.assertIsNotNone(screen_panel_box)
                self.assertIsNotNone(screen_heading_box)
                self.assertIsNotNone(screen_root_box)
                self.assertIsNotNone(map_screen_box)
                self.assertIsNotNone(stage_box)
                self.assertIsNotNone(surface_box)
                self.assertIsNotNone(viewport_box)

                overlay_hidden = page.locator("#relation-map-overlay-host").evaluate(
                    "el => el.hidden || !el.classList.contains('open')"
                )
                self.assertTrue(overlay_hidden, "normal relation map should not use the fullscreen overlay host")
                self.assertGreaterEqual(
                    (map_screen_box or {})["y"],
                    (screen_root_box or {})["y"] - 2,
                    msg=f"normal map should start inside screen-root, got {map_screen_box} vs {screen_root_box}",
                )
                self.assertGreaterEqual(
                    (map_screen_box or {})["y"],
                    (screen_heading_box or {})["y"] + (screen_heading_box or {})["height"] - 2,
                    msg=f"normal map must not overlap screen heading, got {map_screen_box} vs {screen_heading_box}",
                )
                self.assertLessEqual(abs((stage_box or {})["x"] - (map_screen_box or {})["x"]), 12)
                self.assertLessEqual(abs((stage_box or {})["width"] - (map_screen_box or {})["width"]), 24)
                stage_bottom = (stage_box or {})["y"] + (stage_box or {})["height"]
                map_bottom = (map_screen_box or {})["y"] + (map_screen_box or {})["height"]
                self.assertLessEqual(abs(stage_bottom - map_bottom), 24)
                self.assertGreater((stage_box or {})["height"], (map_screen_box or {})["height"] * 0.68)
                self.assertLessEqual(abs((surface_box or {})["height"] - (stage_box or {})["height"]), 24)
                self.assertGreater((viewport_box or {})["height"], (surface_box or {})["height"] * 0.55)

                chips = page.locator("[data-map-group]")
                self.assertGreaterEqual(chips.count(), 3)
                self.assertTrue(page.locator("[data-map-group='documents']").first.is_visible())

                page.get_by_role("button", name="На весь экран").click()
                page.wait_for_timeout(350)
                overlay_box = page.locator("#relation-map-overlay-host").bounding_box()
                app_shell_box = page.locator("#app-shell").bounding_box()
                self.assertIsNotNone(overlay_box)
                self.assertIsNotNone(app_shell_box)
                self.assertLessEqual(abs((overlay_box or {})["x"] - (app_shell_box or {})["x"]), 18)
                self.assertLessEqual(abs((overlay_box or {})["y"] - (app_shell_box or {})["y"]), 18)
                self.assertGreater((overlay_box or {})["width"], (app_shell_box or {})["width"] * 0.94)
                self.assertGreater((overlay_box or {})["height"], (app_shell_box or {})["height"] * 0.94)
                self.assertTrue(page.get_by_role("button", name="Закрыть карту").is_visible())
                page.keyboard.press("Escape")
                page.wait_for_timeout(220)
                self.assertTrue(
                    page.locator("#relation-map-overlay-host").evaluate(
                        "el => el.hidden || !el.classList.contains('open')"
                    )
                )
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

                host_before = page.locator("#screen-root .relation-map-screen").bounding_box()
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

                host_after = page.locator("#screen-root .relation-map-screen").bounding_box()
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
                self.assertIsNotNone(main_panel_box)
                self.assertIsNotNone(screen_panel_box)
                self.assertGreater((screen_panel_box or {})["height"], (main_panel_box or {})["height"] * 0.68)
                self.assert_full_height_drawer(page)
            finally:
                browser.close()

    def test_shell_drawers_open_below_workspace_panel(self):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1920, "height": 1040})
                page.goto(UI_WEB_INDEX)
                page.wait_for_timeout(700)

                top_panel_box = page.locator(".top-panel").bounding_box()
                self.assertIsNotNone(top_panel_box)
                top_panel_bottom = (top_panel_box or {})["y"] + (top_panel_box or {})["height"]

                page.get_by_role("button", name="Источники").click()
                page.wait_for_timeout(260)
                sidebar_box = page.locator("#sidebar-panel").bounding_box()
                self.assertIsNotNone(sidebar_box)
                self.assertGreaterEqual(
                    (sidebar_box or {})["y"],
                    top_panel_bottom + 8,
                    msg=f"sources drawer should open below top panel, got {sidebar_box} vs top panel {top_panel_box}",
                )

                page.get_by_role("button", name="Панель").click()
                page.wait_for_timeout(260)
                tasks_box = page.locator("#tasks-panel").bounding_box()
                self.assertIsNotNone(tasks_box)
                self.assertGreaterEqual(
                    (tasks_box or {})["y"],
                    top_panel_bottom + 8,
                    msg=f"task drawer should open below top panel, got {tasks_box} vs top panel {top_panel_box}",
                )
            finally:
                browser.close()

    def test_screen_text_filter_keeps_focus_after_debounced_reload(self):
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(viewport={"width": 1920, "height": 1040})
                page.goto(UI_WEB_INDEX)
                page.wait_for_timeout(700)
                page.get_by_role("button", name="Контент").click()
                page.wait_for_timeout(350)

                input_box = page.locator("#screen-query-input")
                input_box.click()
                input_box.type("abc")
                page.wait_for_timeout(900)
                self.assertEqual(input_box.input_value(), "abc")
                self.assertEqual(
                    page.evaluate("document.activeElement && document.activeElement.id"),
                    "screen-query-input",
                    msg="screen search input should keep focus after debounce reload",
                )
            finally:
                browser.close()


if __name__ == "__main__":
    unittest.main()
