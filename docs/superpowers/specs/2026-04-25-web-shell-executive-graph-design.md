# Web Shell Executive Graph Design

**Date:** 2026-04-25

## Goal

Перевести desktop shell проекта на встроенный HTML/CSS/JS dashboard без смены Python/SQLite backend, закрыть оставшиеся backend-разрывы investigation graph, и добавить сбор руководителей/заместителей госорганов из официальных источников в нормализованный слой `official_positions`.

## Scope

В этот цикл входят три связанных блока:

1. `investigation`:
   - добавить `Case`-узлы и traversal `Entity -> Claim -> Case -> Event/Content`;
   - улучшить контрактный слой так, чтобы при появлении контрагента в raw/detail-данных он нормализовался в `contracts / contract_parties`;
   - не ломать текущий evidence-graph и Obsidian export.
2. `collectors`:
   - добавить конфигурируемый сбор executive directory для стартового набора госорганов;
   - писать найденных людей в `entities`, `entity_aliases`, `official_positions`, при наличии партийного контекста — в `party_memberships`;
   - сохранять сырой снимок leadership pages как `content_items`, чтобы источник был проверяемым.
3. `ui`:
   - заменить текущий widget shell на `QWebEngineView + QWebChannel`;
   - реализовать тёмный emerald glass dashboard shell;
   - перенести навигацию, source sidebar, job panel и ключевые data screens в HTML/CSS/JS.

## Architecture

### 1. Web shell

- `main.py` перестаёт собирать shell из `QWidget`-панелей.
- Новый shell строится как `QMainWindow`, внутри которого центральный контент — `QWebEngineView`.
- Python остаётся orchestration/backend слоем:
  - загрузка настроек;
  - запуск/остановка job workers;
  - экспорт в Obsidian;
  - SQL-запросы к snapshot/live DB.
- В браузерную часть пробрасывается `DashboardBridge` через `QWebChannel`.
- HTML/CSS/JS assets живут локально в репозитории и грузятся как `file://` bundle.

### 2. Investigation graph

- В `investigation.models` добавляется `NodeType.CASE`.
- В `investigation.engine` добавляется виртуальный `case`-node offset и методы:
  - `_find_case_connections(entity_id)`
  - `_find_case_node_connections(node_id, case_id)`
- Основной traversal получает новые цепочки:
  - `Entity -> Claim -> Case`
  - `Entity -> Case`
  - `Case -> CaseEvent -> Content`
  - `Case -> Claim -> Entity`
- Контрактное обогащение делается в two-step модели:
  - если `contracts.raw_data` уже содержит supplier/counterparty fields, они нормализуются;
  - если нет, scraper detail-phase должен получать их с detail page.

### 3. Executive directory collection

- Добавляется конфигурационный файл с leadership sources и parser hints.
- Новый collector проходит по leadership pages, выделяет:
  - ФИО;
  - должность;
  - организацию;
  - profile URL;
  - photo URL;
  - статус `head`/`deputy`/`minister`/`deputy_minister`.
- Стартовый набор органов:
  - Правительство РФ
  - Минфин
  - Роскомнадзор
  - Минюст
  - ФНС
  - Федеральное казначейство
  - ФАС
- Органы с нестабильной сетью/markup должны деградировать в `warning + raw snapshot`, а не ломать весь прогон.

## Data Flow

### Executive data

1. Collector загружает leadership page.
2. Сохраняет raw snapshot в `raw_source_items/content_items`.
3. Парсит персон и profile links.
4. Для каждой персоны:
   - upsert в `entities(person)`;
   - aliases из короткой/полной формы имени;
   - upsert в `official_positions`;
   - при наличии старых active positions той же организации/роли — аккуратное закрытие/деактивация.

### Web dashboard

1. JS shell запрашивает grouped app snapshot у `DashboardBridge`.
2. Для экранов `Overview / Search / Claims / Cases / Entities / Relations / Officials` bridge отдаёт JSON payload.
3. JS строит master-detail layout локально, без server roundtrip HTML templates.
4. Actions (`run job`, `stop job`, `toggle scheduler`, `obsidian export`) вызывают bridge slots.

## Error Handling

- Leadership collector:
  - network/SSL/reset не должны прерывать общий batch;
  - для каждого org сохраняется health result.
- Web shell:
  - если `QWebEngineView` не загрузил bundle, окно показывает fallback error panel;
  - bridge methods возвращают error payload вместо исключения в JS.
- Investigation:
  - malformed JSON в `raw_data` / `involved_entities` / `entity_ids` продолжает обрабатываться через safe Python parsing;
  - `Case` traversal не должен расширять отсутствующие virtual nodes.

## Testing

- Unit tests:
  - `Case` nodes and case-event traversal;
  - executive parser normalization on fixture HTML;
  - bridge JSON payload smoke.
- Runtime checks:
  - `py_compile` по изменённым модулям;
  - `unittest discover -s tests -v`;
  - offscreen smoke запуска main window/web shell;
  - snapshot rebuild + minimal export smoke.

## Non-Goals For This Cycle

- Отдельный HTTP backend или браузерное standalone web app.
- Полная миграция каждого старого PySide widget screen 1:1 по всем micro-features.
- Автоматический парсинг всех федеральных органов сразу. В этом цикле делается расширяемый framework + стартовый набор органов.
