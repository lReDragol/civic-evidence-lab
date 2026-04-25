# Web Shell Executive Graph Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Добавить case-layer в investigation, расширить executive collection для руководства госорганов и заменить текущий desktop shell на встроенный HTML/CSS/JS dashboard.

**Architecture:** Python/SQLite остаются backend-слоем. UI переносится в локальный web bundle внутри `QWebEngineView` с `QWebChannel` bridge. Investigation и executive collectors дорабатываются так, чтобы новые данные сразу отражались в graph/export/UI.

**Tech Stack:** Python 3.13, PySide6 + QtWebEngine + QWebChannel, SQLite, requests, BeautifulSoup, local HTML/CSS/JS.

---

### Task 1: Investigation Case Layer

**Files:**
- Modify: `F:\новости\investigation\models.py`
- Modify: `F:\новости\investigation\engine.py`
- Test: `F:\новости\tests\test_investigation_evidence_graph.py`

- [ ] Добавить failing tests для `Case`-узлов и `Case -> CaseEvent/Content` traversal.
- [ ] Прогнать targeted tests и убедиться, что они падают по отсутствующему case-layer.
- [ ] Добавить `NodeType.CASE`, relation labels/inverses/confidence для case edges.
- [ ] Реализовать `case` virtual nodes и traversal в engine.
- [ ] Прогнать tests, затем общий `unittest discover`.

### Task 2: Procurement Counterparty Enrichment

**Files:**
- Modify: `F:\новости\collectors\zakupki_scraper.py`
- Modify: `F:\новости\tools\build_analysis_snapshot.py`
- Test: `F:\новости\tests\test_investigation_evidence_graph.py`

- [ ] Добавить failing test на нормализацию supplier/counterparty из enriched contract raw data.
- [ ] Реализовать detail-fetch phase для закупок и извлечение второй стороны контракта, когда она доступна на detail page.
- [ ] Обновить snapshot normalization так, чтобы supplier/customer попадали в `contract_parties`.
- [ ] Прогнать targeted tests и rebuild snapshot smoke.

### Task 3: Executive Directory Collector

**Files:**
- Create: `F:\новости\config\executive_sources.json`
- Create: `F:\новости\collectors\executive_directory_scraper.py`
- Modify: `F:\новости\main.py`
- Test: `F:\новости\tests\test_executive_directory_scraper.py`

- [ ] Добавить fixture-based failing tests для executive parser.
- [ ] Описать стартовые leadership sources в config.
- [ ] Реализовать generic collector и DB upsert в `entities / entity_aliases / official_positions`.
- [ ] Подключить job в desktop backend.
- [ ] Прогнать unit tests и live smoke на стартовом наборе органов.

### Task 4: Web Dashboard Bridge

**Files:**
- Create: `F:\новости\ui\web_bridge.py`
- Create: `F:\новости\ui\web_window.py`
- Create: `F:\новости\ui\job_registry.py`
- Modify: `F:\новости\main.py`
- Test: `F:\новости\tests\test_web_bridge.py`

- [ ] Добавить failing tests на основные JSON payloads bridge.
- [ ] Вынести registry jobs из `main.py` в shared module.
- [ ] Реализовать `DashboardBridge` для summary, sources, jobs, search, claims, cases, entities, officials.
- [ ] Переключить `main.py` на web window shell.
- [ ] Прогнать bridge tests и offscreen smoke.

### Task 5: HTML/CSS/JS Dashboard

**Files:**
- Create: `F:\новости\ui_web\index.html`
- Create: `F:\новости\ui_web\app.css`
- Create: `F:\новости\ui_web\app.js`
- Modify: `F:\новости\ui\web_window.py`

- [ ] Собрать glass-dashboard shell: sidebar, top context, segmented nav, resizable/collapsible task panel.
- [ ] Реализовать screens `Overview`, `Search`, `Claims`, `Cases`, `Entities`, `Relations`, `Officials`.
- [ ] Подключить bridge actions: pinned sources, run/stop jobs, scheduler, export.
- [ ] Прогнать offscreen/web shell smoke и сохранить скриншоты.

### Task 6: Snapshot, Export, Docs, Cleanup

**Files:**
- Modify: `F:\новости\tools\export_obsidian_graph.py`
- Modify: `F:\новости\todo.md`
- Modify: `F:\новости\README.md`

- [ ] Проверить, что новые `Case`/executive links доходят до graph-export.
- [ ] Обновить `todo.md` и README по новому shell/executive collector.
- [ ] Прогнать `py_compile`, `unittest`, snapshot rebuild и minimal export smoke.
- [ ] Удалить временный мусор и подготовить чистый diff.
