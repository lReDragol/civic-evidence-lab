# TODO / План реализации

## 0) Что это за система

Цель проекта — **не "агитка" и не потоковый пересказ слухов**, а система **документирования публично значимых фактов** о действиях власти, депутатов, ведомств и связанных с ними структур.

Рабочая формулировка:

> Создать единую систему сбора, оцифровки, нормализации, классификации и проверки публичной информации о политике, законах, госуправлении, судебной практике, госзакупках, публичных высказываниях должностных лиц и связанных с ними событиях. Система должна отделять сигналы от подтвержденных фактов, хранить доказательную базу, показывать происхождение каждого утверждения, поддерживать ручную редакторскую проверку и обеспечивать воспроизводимость выводов.

Ключевой принцип:

- **источник ≠ факт**
- **пост/ролик/новость = сигнал**
- **документ/реестр/судебный акт/официальная запись = доказательство**

### 0.1. Инфраструктура (утверждено)

| Компонент | Решение |
|-----------|---------|
| Сервер | Домашний, 24/7, VPN настроен |
| Диск | 5 ТБ локальный + S3 (позже) |
| БД | SQLite (MVP) → PostgreSQL (прод) |
| Очередь | APScheduler / Celery (позже) |
| ASR | Whisper large-v3, локально (G:\ollama) |
| LLM | Ollama, модели на G:\ollama |
| OCR | PaddleOCR (локально) |
| Frontend | PySide6 (локальная панель) |
| Хранение файлов | Локальный диск (D:\novosti_storage или аналог) |
| Docker | Не нужен для MVP (всё локально) |

### 0.2. Подход к TikTok

TikTok-сбор через **watch-folder**:
1. Пользователь вручную скачивает ролики (yt-dlp / браузер)
2. Кидает в папку `inbox/tiktok/`
3. Программа раз в минуту проверяет папку
4. Если есть файлы — обрабатывает (ASR, OCR, keyframes, метаданные)
5. После успешной обработки — перемещает в `processed/tiktok/` (оригинал хранится), из inbox удаляет

### 0.3. Подход к YouTube

Аналогично TikTok, но + автоматическое скачивание:
1. Allowlist YouTube-каналов в `sources`
2. yt-dlp по расписанию проверяет новые видео
3. Скачивает видео + субтитры (если есть) + метаданные
4. То же, что TikTok: ASR, OCR, keyframes, цитаты
5. Оригинал хранится в `processed/youtube/`

### 0.4. Актуальный план работ по репозиторию

- [x] Изучить текущую структуру проекта, старую БД `news_unified.db` и новую БД `db/news_unified.db`
- [x] Создать резервную копию рабочей БД перед миграцией
- [x] Привести файловую модель БД к структуре `raw_source_items` → `raw_blobs` → `attachments` → `content_items`
- [x] Добавить общий модуль файлового хранилища `db/file_store.py`
- [x] Расширить `db/schema.sql` полями для имени файла, относительного пути, метаданных и признака отсутствия файла на диске
- [x] Исправить `db/migrate.py`, чтобы он импортировал легаси-сообщения и фото/BLOB из таблиц `exports`, `messages`, `photos`
- [x] Заполнить `raw_blobs` существующими картинками из легаси-БД
- [x] Исправить запись вложений в `collectors/telegram_collector.py`
- [x] Подключить `raw_blobs` к `collectors/watch_folder.py`
- [x] Подключить `raw_blobs` к `collectors/youtube_collector.py`
- [x] Исправить SQL-ошибку вставки цитат в `media_pipeline/asr.py`
- [x] Исправить SQL-ошибку вставки депутатского профиля в `collectors/playwright_scraper.py`
- [x] Исправить запуск `db/backup.py` как отдельного скрипта
- [x] Добавить скрипт полного экспорта БД и файлов в Obsidian `tools/export_obsidian.py`
- [x] Проверить ограниченный Obsidian smoke-export
- [x] Проверить полный Obsidian export в `F:\новости\obsidian_export`
- [x] Проверить Python-компиляцию проекта через `python -m compileall -q .`
- [x] Проверить SQLite `PRAGMA integrity_check` и `PRAGMA foreign_key_check`
- [x] Проверить smoke-загрузку PySide6 UI в offscreen-режиме
- [x] Добавить `README.md`, `requirements.txt` и обновить `.gitignore`
- [x] Провести ревью качества выполненных пунктов и сверить `[x]` с фактическим состоянием
- [x] Довести `sources_seed.json` до полного seed: 47 Telegram, 10 TikTok, 14 YouTube, 17 СМИ и официальные источники раздела 3.1
- [x] Исправить ошибочную привязку легаси-импорта к `Госдума РФ (Telegram)` и перевязать 16 157 сообщений на отдельный `Telegram legacy export`
- [x] Исключить `Telegram legacy export` из живого Telegram collector
- [x] Исправить `watch_folder`, чтобы `content_items.url` указывал на сохранённый файл в `processed/*`, а не на удаляемый inbox-файл
- [x] Улучшить `setup_logging`, чтобы повторный запуск не дублировал console/file handlers
- [x] Повторно проверить Obsidian export после исправлений источников
- [x] Почистить временный мусор: удалить smoke-экспорты Obsidian, `app.log` и сгенерированные `__pycache__`
- [x] Добавить выгрузку Obsidian в меню PySide6: `Экспорт` -> `Выгрузить в Obsidian...`
- [x] Проверить smoke-загрузку меню экспорта в offscreen-режиме
- [x] Исправить отображение вложений и превью картинок в карточке контента
- [x] Проверить рендер превью на реальном attachment из БД
- [x] Исправить RSS collector: добавить реальные RSS endpoints для TASS, РИА, РБК, Коммерсантъ, Ведомости, Интерфакс, Meduza, Известия
- [x] Проверить RSS parser на реальных сайтах: добавлено 16 новых статей
- [x] Улучшить verification engine: извлекать полные утверждения, искать локальные официальные подтверждения и не падать на дубликатах claims
- [x] Запустить локальную проверку достоверности без внешних запросов: обработано 200 материалов, добавлено 127 claims
- [x] Запустить `verification/evidence_linker.py --all`: 194 claims получили 602 связи с evidence
- [x] Добавить `PRAGMA busy_timeout` для снижения риска `database is locked`
- [x] Описать стратегию автоматической классификации тегов в `classifier/TAGGING_STRATEGY.md`
- [x] Исправить шумные теги: контекстные `duma/regional` больше не попадают в уровень 3, `regional` не срабатывает внутри `блокировках`, `international` не срабатывает внутри `арестовал`
- [x] Повторно проверить Python-компиляцию измененных модулей, SQLite integrity/foreign keys, Obsidian smoke-export и PySide6 UI
- [x] Добавить изолированный Telethon-сбор Telegram в тестовую БД `db/news_telegram_test.db`
- [x] Проверить тестовую session-копию `campus5197` без изменения рабочей БД
- [x] Засеять в тестовую БД 47 Telegram-источников из `config/sources_seed.json`
- [x] Исправить подтвержденные Telegram handles: Госдума, Совет Федерации, СРЗП, Единая Россия, Минобороны, Генпрокуратура
- [x] Запустить тестовый Telegram parsing: собрано 114 постов в отдельную БД, отчет сохранен в `reports/telegram_test_collect_latest.json`
- [x] Добавить первичную маркировку качества Telegram-постов: `quality:probable_news`, `quality:uncertain_signal`, `quality:low_signal`, `promo_risk`
- [x] Проверить SQLite integrity/foreign keys тестовой Telegram-БД
- [x] Реализовать фильтры Telegram-сбора: пустые сообщения, короткий низкосигнальный текст, giveaway/розыгрыши, link-dump без новостного контекста, promo/ad-score
- [x] Перезапустить Telegram parsing в режиме `--store-mode filtered`: просмотрено 218 сообщений, записано 184, отфильтровано 34
- [x] Добавить опциональную подписку тестового аккаунта через `--join-channels`
- [x] Подписать `campus5197` на 23 доступных Telegram-канала и проверить `left=false` через Telegram API
- [x] Встроить запуск verification после Telegram-сбора через `--run-verification`
- [x] Запустить verification по тестовой Telegram-БД: создано 71 claims, все оставлены `unverified` из-за отсутствия официального корпуса/evidence
- [x] Проверить точность тегов на тестовой БД и исправить ложные короткие совпадения `суд`, `СВО`, `НАТО`, `ГРУ`, `лож`
- [x] Сузить контекстный тег `duma`: фамилии депутатов учитываются только при парламентском контексте
- [x] Отключить неверный Telegram seed `t.me/memorial`, который резолвился не в канал Мемориала
- [x] Исправить `db/migrate.py`, чтобы seed importer уважал `is_active` из `sources_seed.json`
- [x] Сохранить отчеты `reports/telegram_test_collect_latest.json`, `reports/telegram_test_join_retry.json`, `reports/telegram_test_subscription_check.json`, `reports/telegram_test_audit_latest.json`
- [x] Временно подключить `db/news_unified.db` как внешний verification corpus для `db/news_telegram_test.db`, убрать ложные совпадения по общим словам и проверить результат: `reports/telegram_test_main_corpus_verify.json`, reliable hits на текущем корпусе = 0
- [x] Создать отдельную evidence-БД `db/news_evidence.db` и перенести в неё текущий официальный/media корпус из `db/news_unified.db`
- [x] Исправить official parsers для backfill evidence corpus: `minjust` переведен на `reestrs.minjust.gov.ru` API, `duma` переведен на `https://sozd.duma.gov.ru/oz/b`, `zakupki` дедуплицируется по `reestrNumber` и умеет режим последних контрактов без поискового мусора
- [x] Прогнать backfill в `db/news_evidence.db`: `minjust=1184`, `zakupki=20`, `duma=192` запросов, итоговый corpus = `1413 official_registry` + `16 media`
- [x] Исправить качество Duma bill parser: устранено смещение колонок, `published_at` снова дата регистрации, `title/body_text` содержат номер, статус, СПЗИ и последнее событие
- [x] Ужесточить claim extraction для Telegram test DB и пересобрать claims: `71 -> 53`, мусорные фрагменты типа `собственников.` и общие лозунги отфильтрованы
- [x] Проверить Telegram verification против `db/news_evidence.db`: `53` claims обработано, `0` reliable hits; причина — текущий corpus все еще слабо пересекается с темами тестовой Telegram-выборки
- [x] Добавить health-check официальных источников `tools/check_official_sources.py` и отчет `reports/source_health_latest.json`: доступно `6/15`, недоступны `government.ru`, `kremlin.ru/special.kremlin.ru`, часть `publication.pravo.gov.ru` по HTTPS и `rosreestr.gov.ru/lk.rosreestr.ru`
- [x] Добавить `ГИС ЖКХ` (`dom.gosuslugi.ru`) в `config/sources_seed.json` и автоматическое создание missing official sources внутри collectors
- [x] Расширить official backfill collectors: `gis_gkh`, `government`, `pravo`, `rosreestr`, `kremlin` fallback; фактически собрано `gis_gkh=1`, `pravo=1`, а `government/rosreestr/kremlin=0` помечаются как `ok=false` с сетевой диагностикой
- [x] Расширить `tools/build_evidence_db.py`: per-source `ok/collected/duration/error`, встроенный `source_health`, новые Duma-запросы `росреестр` и `гис жкх`
- [x] Перевести `verification.external_corpus` с простого term-match на сопоставление по датам, органам, ссылкам на нормы/законы и тематическим осям; в notes добавлен `match_reason`
- [x] Убрать ложные подтверждения verifier: год `2026` + `Государственная Дума` больше не подтверждают claim, `суд` не срабатывает внутри `государственную`, слабые нерелевантные candidates не пишутся в `evidence_links`
- [x] Пересобрать `db/news_evidence.db` после расширения collectors: `1186 registry_record`, `189 bill`, `40 procurement`, `16 article`; `PRAGMA integrity_check=ok`, foreign keys без ошибок
- [x] Пересобрать и проверить Telegram test verification после нового verifier: `53` claims, `0` reliable hits, `0` evidence_links, все claims остаются `unverified`; отчет `reports/telegram_test_evidence_verify.json`
- [x] Добавить строгий негативный фильтр `classifier/negative_filter.py`: corruption/fraud, election manipulation, censorship/blocking, repression/courts, mobilization harm, economic harm, state coercion/surveillance, covid restrictions, social harm
- [x] Ужесточить партийные и депутатские материалы: party threshold `5.5`, `Единая Россия` threshold `6.0`, депутатский контекст `5.5`, self-promo партий без негативного сигнала фильтруется
- [x] Переключить Telegram test collector на режим по умолчанию `--store-mode negative_only`; старые режимы `all`, `filtered`, `news_only` сохранены
- [x] Добавить риск-теги для новых фильтров: `filter:negative_public_interest`, `negative:<category>`, `review:party_source_strict`, `review:united_russia_strict`, `review:deputy_claim_strict`
- [x] Добавить аудит фильтра `tools/audit_negative_filter.py` и отчет `reports/negative_filter_audit_latest.json`: на старой тестовой БД `184` поста проверено, `25` прошли бы `negative_only`, `159` были бы отфильтрованы
- [x] Исправить шумные negative-patterns: `ранен` больше не срабатывает внутри чужих слов, `призыв` не ловит обычное "призывает", `повестка` ограничена военкоматом/электронными повестками, MAX считается только в контексте принуждения/обязательной установки
- [x] Исправить критичные баги расследования в `investigation/engine.py`: `accountability_index` теперь ищется по `deputy_profiles.id`, а не по `entities.id`
- [x] Исправить дедупликацию и контекст связей: `mentioned_together` исключен из общего SQL, ключ ребра учитывает `context_id/bill_id/vote_session_id/material_id/evidence_item_id`, а не только `other_entity_id + relation_type`
- [x] Починить интерактивное раскрытие в `investigation/node_viewer.py`: diff новых узлов/рёбер считается до мутации, `merge()` больше не вызывается на том же объекте, режим `Фильтр: все` показывает и `DISPUTED`
- [x] Починить `InvolvementTab`: `QTimer` останавливается после завершения потока, ошибка расследования не теряется, риск-паттерны больше не ищутся через хрупкий SQL `LIKE/json_each`
- [x] Перевести `investigation` на безопасный Python-парсинг JSON для `involved_entities`, `risk_patterns.entity_ids` и `government_contract.raw_data`, чтобы расследование не падало на грязных строках SQLite
- [x] Материализовать evidence-узлы для голосований и законопроектов: подтвержденные `bill_votes` теперь создают `VoteSession`-узлы даже при отсутствии соответствующей записи в `entities`, а `sponsored_bill` умеет использовать синтетический `Bill`-узел как fallback
- [x] Добавить `tools/build_analysis_snapshot.py`: производная БД `db/news_analysis.db` теперь собирается из `db/news_unified.db` с фиксированным pipeline (`structural -> relations -> contradictions/evidence -> risks`) и отчетом `reports/analysis_snapshot_latest.json`
- [x] Перевести `tools/export_obsidian.py` в два режима: `graph` по умолчанию и `archive` как совместимый старый режим
- [x] Реализовать полноценный graph-export в `tools/export_obsidian_graph.py`: отдельные заметки `Entity`, `Content`, `Claim`, `Case`, `Bill`, `VoteSession`, `Contract`, `Risk`, `WeakLinks`, `Tags`, `Files`
- [x] Убрать source-hub схему из Obsidian graph: `Content`-заметки больше не линкуют `Sources/*` как центральные узлы, а weak co-occurrence вынесен в `WeakLinks/*`
- [x] Добавить YAML tags + inline hashtags в `Content` и namespace-индексы `Tags/*`, чтобы теги работали как навигация, а не как мусорные wikilink-хабы
- [x] Прогнать полный live snapshot и full graph-export: `db/news_analysis.db` собрана, `obsidian_export_graph` создан, `Content`-заметки не содержат `[[Sources/`, а top wikilink roots теперь `Entities/Content/Cases/Claims`, а не `Sources`
- [x] Нормализовать закупки в отдельные таблицы `contracts` / `contract_parties`: `db/news_analysis.db` теперь материализует госконтракты отдельно от `raw_data` JSON, а `investigation` умеет входить в `Contract`-узлы через `contract_parties.entity_id` даже без ИНН
- [x] Повторно прогнать live snapshot + graph-export после нормализации закупок: `contract_parties=30`, seed-организация `16205` теперь раскрывается в `Contract`-узел, а соответствующая `Entity`-заметка в Obsidian содержит секцию `Contracts` с wikilink на контракт
- [x] Перевести граф расследования с `entity -> entity` BFS на полноценный evidence-graph: движок теперь поддерживает `Person -> VoteSession -> Bill`, `Organization -> Contract -> Counterparty` (если обе стороны есть в `contract_parties`) и `Entity -> Claim -> SourceItem -> Entity`; добавлены виртуальные `Claim`/`Content`-узлы, live-граф ограничивается `max_nodes/max_edges` без переполнения
- [x] Заменить DFS-first цепочки в `EvidenceChain` на weighted/best-first scoring: цепочки теперь ранжируются по `confidence + relation_weight + source_quality + evidence_strength`, слабые `mentioned_together/co_voter/co_sponsor` не расширяются как основной доказательный маршрут, а тесты закрепляют приоритет `Person -> VoteSession -> Bill` и `Organization -> Contract`
- [x] Добавить `Case`-узлы в `investigation`: `case_claims/case_events` теперь материализуются как отдельный evidence-layer `Entity -> Claim -> Case -> Event/Content`, тесты и live-дым подтверждают наличие `Case`-узлов в графе расследования
- [x] Обогатить `contracts/contract_parties` второй стороной сделки: после починки `zakupki` search-result parser и detail-fetch в live snapshot получено `67 contracts`, `97 contract_parties`, из них `30` уже двусторонние (`customer + supplier`) и пригодны для `Organization -> Contract -> Counterparty`
- [x] Добавить collector руководства госорганов `collectors/executive_directory_scraper.py` + `config/executive_sources.json`: в рабочую БД пишутся официальные профили руководителей и заместителей `Правительства РФ`, `Минфина`, `РКН`, `Минюста`, `ФНС`, `Федерального казначейства`, `ФАС`
- [x] Переписать основной desktop shell на встроенный HTML/CSS/JS dashboard через `QWebEngineView + QWebChannel`: новый `main.py` запускает web-shell `ui_web/*`, а bridge `ui/web_bridge.py` отдаёт summary, sources, jobs, entities, cases, relations и officials
- [x] Ввести runtime/orchestration слой для 24/7 работы: добавлены `runtime/*` entrypoints (`python -m runtime.daemon`, `runtime.run_job`, `runtime.run_pipeline`, `runtime.healthcheck`, `runtime.recover`) и единый контракт результата job'а с `job_runs / job_leases / pipeline_runs / runtime_metadata`
- [x] Вынести scheduler из UI: `scheduler.py` теперь только прокси к `runtime.daemon`, а `ui/web_window.py` запускает/останавливает внешний daemon и ручные job'ы как отдельные subprocess'ы, не держа long-running APScheduler внутри desktop-процесса
- [x] Добавить persistent runtime/state tables в `db/schema.sql`: `source_health_checks`, `source_sync_state`, `dead_letter_items`, `relation_candidates`, `relation_support`, `classifier_audit_samples`, `investigation_leads`; `config/db_utils.get_db()` теперь гарантирует наличие актуальной схемы
- [x] Перевести weak-layer на отдельный candidate backlog: raw `mentioned_together` больше не пишется напрямую в `entity_relations`, а `graph/relation_candidates.py` хранит weak/review state отдельно и продвигает только promoted edges
- [x] Нормализовать контрактный слой и встроить его в live graph-stage: `runtime.registry._relations` теперь пересобирает `contracts / contract_parties` и создаёт structural review backlog `same_contract_cluster` прямо в `db/news_unified.db`
- [x] Прогнать live smoke нового runtime-контура: `runtime.healthcheck`, `runtime.run_job --job analysis_snapshot`, `runtime.run_job --job obsidian_export`, отдельный daemon start/lease/stop и полный `runtime.run_pipeline --mode nightly`; итоговый `pipeline_version=nightly-20260425011232`, `weak_similarity=53 review candidates`, metadata синхронизированы между `news_unified.db`, `news_analysis.db` и Obsidian export
- [ ] Расширить structural seeding beyond contracts: добавить осмысленные `same_vote_pattern`, `same_bill_cluster` и `same_case_cluster` без возврата старого co-occurrence шума
- [x] Интегрировать `source_sync_state`/cursor updates и `dead_letter_items` на item-level в `telegram`, `watch_folder` и OCR media pipeline; incremental jobs теперь возвращают нормальные runtime payload'ы, а битые OCR-вложения уходят в `ocr_runtime`/`ocr_missing_attachment`
- [ ] Добавить Windows Task Scheduler bootstrap/install script для автозапуска `runtime.daemon` при старте машины
- [ ] Построить gold-set / `classifier_audit_samples` pipeline и automatic drift gate, чтобы nightly snapshot/export не публиковались после деградации precision
- [ ] Добить live OCR runtime-совместимость на этой машине: текущий `PaddleOCR` больше не ломает job-контракт и не уходит в бесконечный retry, но реальное распознавание части изображений падает в `ocr_runtime` из-за `oneDNN/PIR` incompatibility
- [ ] Найти внешний источник для 4 отсутствующих легаси-фото, которых нет ни на диске, ни BLOB-ом в старой БД
- [ ] Разобраться с сетевыми таймаутами/недоступностью `kremlin.ru` и других недостающих official sources из текущей сети
- [ ] Добавить сетевой workaround или альтернативные leadership endpoints для `digital.gov.ru` и `rosreestr.gov.ru`: сейчас executive collector по ним упирается в timeout/TLS reset из текущей сети
- [ ] Повторно проверить `pnp.ru` RSS после сетевой ошибки `WinError 10054`
- [ ] Провести ручной аудит нерезолвящихся Telegram handles из отчета `reports/telegram_test_collect_latest.json`
- [ ] Настроить строгий режим Telegram-сбора `--store-mode news_only` после ручной проверки качества фильтра
- [x] Подключить официальный корпус документов через общий verification-index `db/news_evidence.db`; evidence_links теперь создаются только при надежном совпадении, на текущей Telegram-выборке надежных совпадений нет
- [ ] Расширить доказательный корпус в `db/news_unified.db`: сейчас там только 20 `official_registry` и 16 `media`, этого недостаточно для надежной внешней Telegram verification
- [ ] Расширить `db/news_evidence.db` источниками, которые пересекаются с текущими Telegram claims: `kremlin.ru`, `government.ru`, Росреестр/ЖКХ, судебные и ограничительные реестры
- [ ] Добавить сетевой workaround для `government.ru`, `kremlin.ru/special.kremlin.ru`, `rosreestr.gov.ru` и HTTPS `publication.pravo.gov.ru`: proxy/VPN profile, альтернативный endpoint или headless browser с тем же health-report контрактом
- [ ] Прогнать реальный Telegram-сбор после настройки API/session
- [ ] Прогнать реальный watch-folder тест с видеофайлом в `inbox/tiktok`
- [ ] Прогнать реальный OCR/ASR на тестовом PDF/картинке/видео после проверки установленных моделей

---

## 1) Границы проекта

### 1.1. Что система должна делать

1. Собирать данные из нескольких типов источников:
   - Telegram
   - TikTok (watch-folder)
   - YouTube (авто-скачивание + watch-folder)
   - официальные государственные реестры и порталы
   - официальные сайты органов власти
   - новостные сайты
   - приложенные документы
   - видео заседаний / обращений / интервью
2. Приводить всё к единой модели данных.
3. Выделять сущности:
   - люди
   - ведомства
   - партии
   - компании
   - суды
   - дела
   - законы
   - закупки
   - адреса
   - ИНН / ОГРН / кадастровые номера / номера дел
4. Автоматически присваивать теги и тип события.
5. Строить связи между источниками и доказательствами.
6. Оценивать достоверность по формальным правилам.
7. Отправлять материалы на ручную проверку, если доказательств недостаточно.
8. Хранить архив первоисточников и их хэши.
9. Давать редактору карточку кейса: что произошло, кто участники, какие есть доказательства, что подтверждено, что спорно.
10. Вести отдельный модуль по депутатам/чиновникам и их публичным высказываниям.

### 1.2. Что система НЕ должна делать

1. Не публиковать обвинения без доказательной базы.
2. Не подменять факт эмоциональной оценкой.
3. Не считать Telegram-пост или TikTok-ролик доказательством сам по себе.
4. Не автоматически делать вывод "коррупция" без документов, цепочки связей и ручной верификации.
5. Не стирать противоречащие источники — наоборот, хранить их как часть кейса.

---

## 2) Архитектура проекта

### 2.1. Слои

1. **Ingestion layer** — сбор данных.
2. **Normalization layer** — приведение к единому формату.
3. **Extraction layer** — OCR, ASR, NER, извлечение фактов и цитат.
4. **Verification layer** — оценка достоверности, кросс-проверка, поиск подтверждений.
5. **Case layer** — сборка кейсов/досье/историй.
6. **Editorial layer** — ручная модерация и публикация.
7. **Audit layer** — хранение происхождения, хэшей, версий и статусов.

### 2.2. Основные сервисы

- `source_registry` — реестр источников
- `collector_telegram` — TDLib / Telethon
- `collector_tiktok` — watch-folder + yt-dlp
- `collector_youtube` — yt-dlp по расписанию
- `collector_web` — HTTP scraping / API
- `collector_documents` — watch-folder для PDF/фото
- `media_pipeline` (OCR/ASR/keyframes)
- `entity_resolution`
- `classifier`
- `claim_extractor`
- `verification_engine`
- `case_builder`
- `search_index`
- `editor_dashboard` (PySide6)
- `deputy_monitor`

---

## 3) Полный список источников (по типам и приоритету)

> Важно: ниже не просто "перечень сайтов", а **реестр, который надо заводить в БД**. Для каждого источника нужны поля: `source_id`, `name`, `category`, `type`, `url`, `access_method`, `legal_status`, `credibility_tier`, `update_frequency`, `notes`.

## 3.1. Приоритет A — первичные доказательные источники

### A1. Нормативные акты и законы

| # | Источник | URL | access_method | Примечание |
|---|----------|-----|---------------|------------|
| 1 | Официальное опубликование правовых актов | publication.pravo.gov.ru | HTML scraping + RSS | Изменения законов в реальном времени |
| 2 | Портал правовой информации | pravo.gov.ru | HTML scraping | Официальные тексты |
| 3 | Законопроекты Госдумы | sozd.duma.gov.ru | API + HTML | Карточки, стадии, документы, отзывы |
| 4 | API Госдумы | api.duma.gov.ru | REST API | Депутаты, стенограммы, голосования, запросы |
| 5 | Конституционный суд | ksrf.ru | HTML + PDF | Решения, определения, практика |
| 6 | Верховный суд | vsrf.ru | HTML scraping | Судебная практика, обзоры |

### A2. Парламент и публичные выступления

| # | Источник | URL | access_method | Примечание |
|---|----------|-----|---------------|------------|
| 7 | Карточки депутатов | duma.gov.ru/duma/deputies/ | API + HTML | Биографии, фракции, комитеты |
| 8 | Пленарные заседания Госдумы | duma.gov.ru/multimedia/video/meetings/ | HTML + video | Видеозаписи |
| 9 | Видеоматериалы Госдумы | video.duma.gov.ru | HTML + video | Архив видео |
| 10 | Трансляции Совета Федерации | council.gov.ru/events/streams/ | HTML + video | Заседания, слушания |
| 11 | Совет Федерации | council.gov.ru | HTML + API | Карточки сенаторов, документы |
| 12 | Стенограммы Президента | kremlin.ru/events/president/transcripts/ | HTML scraping | Речи, совещания, обращения |
| 13 | Правительство РФ | government.ru | HTML + RSS | Заседания, распоряжения |
| 14 | ЦИК России | cikrf.ru | HTML + API | Выборы, итоги, нарушения, протоколы |
| 15 | Счётная палата | ach.gov.ru | HTML + PDF | Аудиторские отчёты, проверки бюджета |
| 16 | Общественная палата | oprf.ru | HTML | Заключения, слушания |

### A3. Судебная система

| # | Источник | URL | access_method | Примечание |
|---|----------|-----|---------------|------------|
| 17 | ГАС Правосудие | sudrf.ru | HTML scraping | Суды общей юрисдикции, нет API |
| 18 | Арбитражные суды | arbitr.ru | HTML | Главная страница |
| 19 | Картотека арбитражных дел | kad.arbitr.ru | REST API | Есть API, карточки дел, участники |
| 20 | Судебные решения (суд.рф) | sudrf.ru | HTML scraping | Поиск по делам |
| 21 | Банк судебных решений | sudsrf.ru | HTML scraping | Обзоры практики |

### A4. Юрлица, ИП, финансы, банкротства, взыскания

| # | Источник | URL | access_method | Примечание |
|---|----------|-----|---------------|------------|
| 22 | ЕГРЮЛ/ЕГРИП выписки | egrul.nalog.ru | REST API | Есть официальный API, бесплатно |
| 23 | Прозрачный бизнес | pb.nalog.ru | HTML scraping | Связи юрлиц, директора, адреса |
| 24 | Открытые данные ФНС | nalog.gov.ru/opendata/ | CSV/JSON/XLSX | Датасеты |
| 25 | ЕФРСФДЮЛ | fedresurs.ru | API (платный) / HTML | Юридически значимые сведения |
| 26 | ЕФРСБ (банкротства) | bankrot.fedresurs.ru | HTML + API | Банкротства, торги |
| 27 | Банк данных исполнительных производств | fssp.gov.ru/iss/ip/ | HTML scraping | Поиск по ФИО/юрлицу |
| 28 | Кредитные организации ЦБ | cbr.ru/banking_sector/credit/ | HTML + API | Отзывы лицензий |
| 29 | Участники финрынка ЦБ | cbr.ru/fmp_check/ | HTML | Проверка организаций |
| 30 | Единый реестр субъектов МСП | rmsp.nalog.ru | API | Малый и средний бизнес |

### A5. Госрасходы, учреждения, закупки

| # | Источник | URL | access_method | Примечание |
|---|----------|-----|---------------|------------|
| 31 | ЕИС закупки | zakupki.gov.ru | API + HTML | Госзакупки, контракты, подрядчики |
| 32 | Сведения об учреждениях | bus.gov.ru | HTML | Гос/муниципальные учреждения |
| 33 | Открытые данные РФ | data.gov.ru | API + CSV/JSON | Портал открытых данных |
| 34 | Открытые данные Росстата | rosstat.gov.ru/folder/12793 | CSV/XLSX | Статистика |
| 35 | Госпрограммы и расходы | budget.gov.ru | HTML + API | Бюджет, расходы по статьям |
| 36 | Электронный бюджет | budget.roskazna.gov.ru | HTML | Казначейство |

### A6. Реестры ограничений / статусов / контроля

| # | Источник | URL | access_method | Примечание |
|---|----------|-----|---------------|------------|
| 37 | Реестр иноагентов | minjust.gov.ru/.../reestr-inostrannykh-agentov/ | HTML + PDF | Список, изменения |
| 38 | Единый реестр запрещённой информации | eais.rkn.gov.ru | HTML | РКН — заблокированные ресурсы |
| 39 | Реестр организаторов распространения информации | rkn.gov.ru/communication/register/ | HTML | ОРВ, мессенджеры |
| 40 | Реестр повесток | реестрповесток.рф | HTML | Только при релевантности кейсу |
| 41 | Госуслуги | gosuslugi.ru | HTML (ограниченно) | Справочная информация |
| 42 | Росреестр — объекты недвижимости | lk.rosreestr.ru/eservices/real-estate-objects-online | HTML | Справочная информация по объектам |
| 43 | Росфинмониторинг — перечень экстремистов | fedsfm.ru/documents/terrorists-list | HTML + PDF | Список организаций и лиц |
| 44 | Перечень экстремистских материалов | minjust.gov.ru/.../extremist-materials/ | HTML + PDF | Запрещённые материалы |

### A7. Антикоррупционные/имущественные разделы

| # | Источник | URL | access_method | Примечание |
|---|----------|-----|---------------|------------|
| 45 | Сведения о доходах депутатов | duma.gov.ru/anticorruption/... | HTML + PDF | Декларации |
| 46 | Сведения о доходах сенаторов | council.gov.ru/.../property/ | HTML + PDF | Декларации |
| 47 | Сведения о доходах Президента | kremlin.ru/structure/president/income-reports | HTML | Ежегодные данные |

### A8. Правоохранительные и надзорные органы

| # | Источник | URL | access_method | Примечание |
|---|----------|-----|---------------|------------|
| 48 | Генпрокуратура | genproc.gov.ru | HTML + RSS | Пресс-релизы, статистика |
| 49 | Следственный комитет | sledcom.ru | HTML + RSS | Сообщения о расследованиях |
| 50 | ФАС | fas.gov.ru | API + HTML | Антимонопольные дела, штрафы |
| 51 | Минфин | minfin.gov.ru | HTML + PDF | Бюджет, госдолг, политика |
| 52 | ФССП — подробнее | fssp.gov.ru | HTML scraping | Исполнительные производства по ФИО |

### A9. Кадастры и регистрация

| # | Источник | URL | access_method | Примечание |
|---|----------|-----|---------------|------------|
| 53 | Роспатент (ФИПС) | fips.ru | HTML | Товарные знаки, патенты — связи компаний |
| 54 | Росреестр (портал) | rosreestr.gov.ru | HTML + API | Недвижимость, кадастры |

---

## 3.2. Приоритет B — публичные сигнальные источники

> Это НЕ конечные доказательства. Это **источники сигналов**, которые запускают проверку.

### B1. Telegram — полный seed-список каналов

Нужен **allowlist**, а не хаотический парсинг.

#### Категория 1. Официальные органы

| # | Хэндл | Название | примечание |
|---|--------|----------|------------|
| 1 | @dumaofficial | Госдума РФ | Официальный канал |
| 2 | @council_rf | Совет Федерации | Официальный канал |
| 3 | @government_rus | Правительство РФ | Официальный канал |
| 4 | @kremlin | Президент России | Официальный канал (если есть) |
| 5 | @mil | Министерство обороны | Официальный канал |
| 6 | @mchs_official | МЧС России | Официальный канал |
| 7 | @mvd_official | МВД России | Официальный канал |
| 8 | @prokuratura_rf | Генпрокуратура | Официальный канал |

#### Категория 2. Политические партии и движения

| # | Хэндл | Название | примечание |
|---|--------|----------|------------|
| 9 | @yabloko_party | Яблоко | Официальный |
| 10 | @partynewpeople | Новые люди | Официальный |
| 11 | @movementbudushee | Движение Будущего | Официальный |
| 12 | @cprf_ru | КПРФ | Официальный |
| 13 | @ldpr_ru | ЛДПР | Официальный |
| 14 | @sr_ru | Справедливая Россия | Официальный |
| 15 | @rodina_ru | Родина | Официальный |
| 16 | @er_ru | Единая Россия | Официальный |

#### Категория 3. Публичные политики

| # | Хэндл | Название | примечание |
|---|--------|----------|------------|
| 17 | @navalny | Алексей Навальный | (если доступен) |
| 18 | @dmilov | Дмитрий Гудков | Политик |
| 19 | @maxkatz | Максим Кац | Политик / блогер |
| 20 | @varlamov | Илья Варламов | Блогер / политик |
| 21 | @vashmarkov | Марков | Политик |
| 22 | @putin_com | (актуальные прокремлёвские) | Для контраста |

#### Категория 4. Блогеры / расследователи / активисты

| # | Хэндл | Название | примечание |
|---|--------|----------|------------|
| 23 | @lagoda1337 | Лагода | Расследователь |
| 24 | @feigin | Марк Фейгин | Адвокат / блогер |
| 25 | @aaplushev | Александр Плющев | Журналист |
| 26 | @alburov | Георгий Албуров | Расследователь (ФБК) |
| 27 | @pevchikh | Мария Певчих | Расследователь (ФБК) |
| 28 | @vovan_x | Vovan / пранки | (проверять осторожно) |
| 29 | @buzhinsky | Антон Бужинский | Юрист |
| 30 | @antimon | Антимонопольщики | Профильный канал |

#### Категория 5. Правозащитные и наблюдательные организации

| # | Хэндл | Название | примечание |
|---|--------|----------|------------|
| 31 | @ovdinfo | ОВД-Инфо | Правозащита, задержания |
| 32 | @memorial | Международный Мемориал | Правозащита |
| 33 | @agora_legal | Агора | Юридическая помощь |
| 34 | @zona_prava | Зона Права | Правозащита |
| 35 | @apologia_protesta | Апология протеста | Мониторинг задержаний |

#### Категория 6. Региональные и локальные сообщества

| # | Хэндл | Название | примечание |
|---|--------|----------|------------|
| 36 | @moscow_live | Москва лайв | Городские новости |
| 37 | @spb_live | СПб лайв | Городские новости |
| 38 | @kazan_live | Казань лайв | Городские новости |
| 39 | @ekb_live | Екатеринбург лайв | Городские новости |
| 40 | @novosibirsk_live | Новосибирск лайв | Городские новости |

#### Категория 7. Профильные тематические каналы

| # | Хэндл | Название | примечание |
|---|--------|----------|------------|
| 41 | @sud_media | Суды | Судебная хроника |
| 42 | @mobilization_news | Мобилизация | Повестки, мобилизация |
| 43 | @jkh_news | ЖКХ | Коммунальные проблемы |
| 44 | @ecology_rf | Экология | Экологические проблемы |
| 45 | @zakupki_monitor | Госзакупки | Мониторинг закупок |
| 46 | @elections_monitor | Выборы | Нарушения на выборах |
| 47 | @censorship_rf | Цензура / блокировки | РКН, заблокированные ресурсы |

> **Важно**: для каждого канала хранить `telegram_handle`, `display_name`, `source_group`, `political_alignment`, `region`, `is_official`, `trust_base`, `requires_manual_review`.

### B2. TikTok — seed-список (watch-folder)

TikTok-аккаунты — пользователь скачивает вручную через yt-dlp или браузер.

#### Начальный allowlist

| # | Username | Владелец | Тип |
|---|----------|----------|-----|
| 1 | @novye_lyudi | Новые люди | Партия |
| 2 | @yabloko_ru | Яблоко | Партия |
| 3 | @maxkatz | Максим Кац | Политик |
| 4 | @milov_d | Дмитрий Гудков | Политик |
| 5 | @navalny_live | Навальный (связанные) | Политика |
| 6 | @lagoda_live | Лагода | Блогер |
| 7 | @varlamov | Варламов | Блогер |
| 8 | @ovdinfo | ОВД-Инфо | Правозащита |
| 9 | @fedortv | Фёдор (политический) | Блогер |
| 10 | @politika_tiktok | Политика РФ | Агрегатор |

> Этот список будет расширяться. Для TikTok используем watch-folder — **не автоматический scrape**.

### B3. YouTube — seed-список

| # | Канал | Владелец | Тип |
|---|-------|----------|-----|
| 1 | youtube.com/@gosdumaRF | Госдума РФ | Официальный |
| 2 | youtube.com/@councilrf | Совет Федерации | Официальный |
| 3 | youtube.com/@kremlin | Кремль | Официальный |
| 4 | youtube.com/@navalny | Алексей Навальный | Политик |
| 5 | youtube.com/@maxkatz | Максим Кац | Политик |
| 6 | youtube.com/@populjarnajapolitika | Популярная политика | Медиа |
| 7 | youtube.com/@feigin | Марк Фейгин | Адвокат |
| 8 | youtube.com/@redakciya | Редакция | Журналистика |
| 9 | youtube.com/@ovdinfo | ОВД-Инфо | Правозащита |
| 10 | youtube.com/@thimble | Напёрсток | Расследования |
| 11 | youtube.com/@lagoda | Лагода | Расследователь |
| 12 | youtube.com/@echomsk | Эхо Москвы | Архив (заблокирован) |
| 13 | youtube.com/@tjournal | TJ | Медиа |
| 14 | youtube.com/@meduzaproject | Медуза | Медиа (VPN) |

> yt-dlp скачивает автоматически по расписанию для allowlist-каналов. Субтитры (auto-generated) сохраняются как черновой ASR-результат.

---

## 3.3. Приоритет C — вторичные СМИ и новостные сайты

> Использовать для контекста, таймлайна и обнаружения кейсов, но не как окончательное доказательство без первичных документов.

| # | СМИ | URL | ownership_type | bias_notes | access_method |
|---|-----|-----|----------------|-----------|---------------|
| 1 | ТАСС | tass.ru | государственное | Проправительственное | RSS + HTML |
| 2 | РИА Новости | ria.ru | государственное | Проправительственное | RSS + HTML |
| 3 | Парламентская газета | pnp.ru | государственное | Проправительственное | RSS + HTML |
| 4 | РБК | rbc.ru | частное | Нейтрально-деловое | RSS + HTML |
| 5 | Коммерсантъ | kommersant.ru | частное | Нейтрально-критическое | RSS + HTML |
| 6 | Ведомости | vedomosti.ru | частное | Деловое | RSS + HTML |
| 7 | Известия | iz.ru | окологосударственное | Лояльное | RSS + HTML |
| 8 | Интерфакс | interfax.ru | частное | Нейтральное | RSS + HTML |
| 9 | Новая газета | novayagazeta.ru | частное | Критическое | HTML (VPN) |
| 10 | Медуза | meduza.io | частное (Латвия) | Критическое, расследования | RSS + HTML (VPN) |
| 11 | Медиазона | mediazona.ru | частное | Критическое, суды/силовики | HTML (VPN) |
| 12 | Проект | proekt.media | частное | Расследовательское | HTML (VPN) |
| 13 | Вёрстка | verstka.media | частное | Расследовательское | HTML (VPN) |
| 14 | iStories | istories.media | частное | Расследовательское | HTML (VPN) |
| 15 | Русская правда | russkayapravda.ru | — | Правовая тематика | HTML |
| 16 | Агентство социальных новостей | asninfo.ru | частное | Социальная тематика | HTML |
| 17 | Забега.ру / Заик.рф | zaik.ru | — | Правовые новости | HTML |

> Для СМИ хранить: `ownership_type`, `region`, `bias_notes`, `archived_copy_required`, `primary_evidence_required = true`.

---

## 3.4. Приоритет D — приложенные пользователями материалы

1. PDF
2. фото документов
3. сканы судебных решений
4. аудио
5. видео
6. архивы выгрузок
7. таблицы
8. письма / ответы ведомств

Это часто самый ценный слой, потому что именно он превращает сигнал в доказуемый кейс.

**Watch-folder**: `inbox/documents/` — та же модель, что TikTok. Программа проверяет раз в минуту.

---

## 4) Схема БД — полный DDL

### 4.1. Миграция из текущей схемы

Текущая БД (`news_unified.db`) содержит:
- `exports` — привязка к папкам экспорта
- `messages` — посты Telegram с тегами
- `photos` — фото с blob и SHA-256

Новый DDL должен:
- Создать все новые таблицы
- Сохранить возможность импорта старых данных
- Старые таблицы не удалять сразу — миграция через `import_legacy_data()`

### 4.2. Полный schema.sql

```sql
-- ============================================================
-- СХЕМА БД: система документирования публичных фактов
-- ============================================================

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

-- ----------------------------------------------------------
-- РЕЕСТР ИСТОЧНИКОВ
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS sources (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL,  -- official_registry, official_site, telegram, tiktok, youtube, media, user_upload
    subcategory     TEXT,
    url             TEXT,
    access_method   TEXT,           -- api, rss, html, manual_upload, telegram_tdlib, yt_dlp, watch_folder, headless_capture
    is_official     INTEGER DEFAULT 0,
    credibility_tier TEXT DEFAULT 'C',  -- A, B, C, D
    region          TEXT,
    country         TEXT DEFAULT 'RU',
    owner           TEXT,
    bias_notes      TEXT,
    political_alignment TEXT,
    is_active       INTEGER DEFAULT 1,
    update_frequency TEXT,          -- hourly, daily, weekly, manual
    last_checked_at TEXT,
    notes           TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(url, category)
);

CREATE INDEX idx_sources_category ON sources(category);
CREATE INDEX idx_sources_tier ON sources(credibility_tier);
CREATE INDEX idx_sources_active ON sources(is_active);

-- ----------------------------------------------------------
-- СЫРЫЕ ОБЪЕКТЫ ИЗ ИСТОЧНИКОВ
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_source_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       INTEGER NOT NULL,
    external_id     TEXT,           -- ID поста / видео / карточки на стороне источника
    raw_payload     TEXT,           -- JSON: полные данные как получили
    collected_at    TEXT DEFAULT (datetime('now')),
    hash_sha256     TEXT,           -- хэш payload для детекции дублей
    is_processed    INTEGER DEFAULT 0,
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE CASCADE,
    UNIQUE(source_id, external_id)
);

CREATE INDEX idx_raw_processed ON raw_source_items(is_processed);
CREATE INDEX idx_raw_source ON raw_source_items(source_id);

-- ----------------------------------------------------------
-- ФАЙЛЫ / БЛОБЫ
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_blobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_item_id     INTEGER NOT NULL,
    blob_type       TEXT NOT NULL,  -- photo, video, audio, pdf, docx, xlsx, other
    file_path       TEXT NOT NULL,  -- путь к файлу на диске
    original_url    TEXT,
    mime_type       TEXT,
    file_size       INTEGER,
    hash_sha256     TEXT NOT NULL,
    downloaded_at   TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (raw_item_id) REFERENCES raw_source_items(id) ON DELETE CASCADE,
    UNIQUE(raw_item_id, original_url)
);

-- ----------------------------------------------------------
-- ЕДИНЫЕ ЗАПИСИ КОНТЕНТА
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS content_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       INTEGER NOT NULL,
    raw_item_id     INTEGER,
    external_id     TEXT,
    content_type    TEXT NOT NULL,  -- post, video, document, news_article, registry_record,
                                    -- speech, court_case, procurement, law_record, quote
    title           TEXT,
    body_text       TEXT,
    published_at    TEXT,
    collected_at    TEXT DEFAULT (datetime('now')),
    url             TEXT,
    language        TEXT DEFAULT 'ru',
    status          TEXT DEFAULT 'raw_signal',  -- raw_signal, unverified, partially_confirmed,
                                                 -- confirmed, contradicted, false, archived
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE CASCADE,
    FOREIGN KEY (raw_item_id) REFERENCES raw_source_items(id) ON DELETE SET NULL
);

CREATE INDEX idx_content_type ON content_items(content_type);
CREATE INDEX idx_content_status ON content_items(status);
CREATE INDEX idx_content_source ON content_items(source_id);
CREATE INDEX idx_content_published ON content_items(published_at);

-- ----------------------------------------------------------
-- ВЛОЖЕНИЯ / ПРИЛОЖЕНИЯ
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS attachments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL,
    blob_id         INTEGER,
    file_path       TEXT NOT NULL,
    attachment_type TEXT NOT NULL,  -- photo, video, audio, pdf, scan, thumbnail, keyframe
    hash_sha256     TEXT NOT NULL,
    file_size       INTEGER,
    mime_type       TEXT,
    ocr_text        TEXT,           -- результат OCR (черновой)
    is_original     INTEGER DEFAULT 1,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE,
    FOREIGN KEY (blob_id) REFERENCES raw_blobs(id) ON DELETE SET NULL
);

-- ----------------------------------------------------------
-- СУЩНОСТИ
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS entities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type     TEXT NOT NULL,  -- person, organization, government_body, party, company,
                                    -- court, law, procurement, address, document_number, case_number
    canonical_name  TEXT NOT NULL,
    inn             TEXT,           -- для юрлиц
    ogrn            TEXT,
    description     TEXT,
    extra_data      TEXT,           -- JSON: дополнительные поля
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(entity_type, canonical_name, COALESCE(inn, ''), COALESCE(ogrn, ''))
);

CREATE INDEX idx_entities_type ON entities(entity_type);
CREATE INDEX idx_entities_inn ON entities(inn);
CREATE INDEX idx_entities_ogrn ON entities(ogrn);
CREATE INDEX idx_entities_name ON entities(canonical_name);

-- ----------------------------------------------------------
-- АЛИАСЫ СУЩНОСТЕЙ
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS entity_aliases (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    alias           TEXT NOT NULL,
    alias_type      TEXT DEFAULT 'spelling',  -- spelling, nickname, short_name, former_name
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    UNIQUE(entity_id, alias)
);

-- ----------------------------------------------------------
-- СВЯЗИ СУЩНОСТЕЙ С КОНТЕНТОМ
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS entity_mentions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    content_item_id INTEGER NOT NULL,
    mention_type    TEXT NOT NULL,  -- subject, object, mentioned, author, judge, plaintiff, defendant
    confidence      REAL DEFAULT 1.0,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE
);

CREATE INDEX idx_entity_mentions_entity ON entity_mentions(entity_id);
CREATE INDEX idx_entity_mentions_content ON entity_mentions(content_item_id);

-- ----------------------------------------------------------
-- ПРОВЕРЯЕМЫЕ УТВЕРЖДЕНИЯ (CLAIMS)
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS claims (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL,
    claim_text      TEXT NOT NULL,  -- формулировка утверждения
    claim_type      TEXT,           -- accusation, statement, promise, vote_record, ownership, connection, censorship, etc.
    confidence_auto REAL,           -- автоматическая оценка 0-1
    confidence_final REAL,          -- итоговая после ручной проверки
    status          TEXT DEFAULT 'unverified',  -- raw_signal, unverified, partially_confirmed,
                                                 -- confirmed, contradicted, false_or_manipulated, archived_unresolved
    source_score    REAL DEFAULT 0,
    document_score  REAL DEFAULT 0,
    corroboration_score REAL DEFAULT 0,
    consistency_score   REAL DEFAULT 0,
    manipulation_risk   REAL DEFAULT 0,
    editor_review_score REAL DEFAULT 0,
    needs_review    INTEGER DEFAULT 1,
    reviewed_by     TEXT,
    reviewed_at     TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE
);

CREATE INDEX idx_claims_status ON claims(status);
CREATE INDEX idx_claims_content ON claims(content_item_id);
CREATE INDEX idx_claims_review ON claims(needs_review);

-- ----------------------------------------------------------
-- СВЯЗИ УТВЕРЖДЕНИЙ С ДОКАЗАТЕЛЬСТВАМИ
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS evidence_links (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_id        INTEGER NOT NULL,
    evidence_item_id INTEGER NOT NULL,  -- content_item_id доказательства
    evidence_type   TEXT NOT NULL,  -- primary_document, registry_record, court_decision,
                                    -- transcript, official_video, cross_source, user_document
    strength        TEXT DEFAULT 'moderate',  -- strong, moderate, weak, contradicts
    notes           TEXT,
    linked_by       TEXT,
    linked_at       TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (claim_id) REFERENCES claims(id) ON DELETE CASCADE,
    FOREIGN KEY (evidence_item_id) REFERENCES content_items(id) ON DELETE CASCADE
);

CREATE INDEX idx_evidence_claim ON evidence_links(claim_id);
CREATE INDEX idx_evidence_item ON evidence_links(evidence_item_id);

-- ----------------------------------------------------------
-- КЕЙСЫ / ИСТОРИИ
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS cases (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    title           TEXT NOT NULL,
    description     TEXT,
    case_type       TEXT,           -- corruption_risk, judicial, censorship, procurement_fraud,
                                    -- abuse_of_power, property_trace, repression, election_violation
    status          TEXT DEFAULT 'open',  -- draft, open, under_review, confirmed, partially_confirmed,
                                          -- contradicted, closed, archived
    region          TEXT,
    started_at      TEXT,
    closed_at       TEXT,
    created_by      TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX idx_cases_status ON cases(status);
CREATE INDEX idx_cases_type ON cases(case_type);

-- ----------------------------------------------------------
-- СВЯЗИ КЕЙС — УТВЕРЖДЕНИЕ
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS case_claims (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id         INTEGER NOT NULL,
    claim_id        INTEGER NOT NULL,
    role            TEXT DEFAULT 'central',  -- central, supporting, contextual, contradicting
    added_at        TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE CASCADE,
    FOREIGN KEY (claim_id) REFERENCES claims(id) ON DELETE CASCADE,
    UNIQUE(case_id, claim_id)
);

-- ----------------------------------------------------------
-- ТАЙМЛАЙН КЕЙСА
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS case_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id         INTEGER NOT NULL,
    event_date      TEXT NOT NULL,
    event_title     TEXT NOT NULL,
    event_description TEXT,
    content_item_id INTEGER,
    event_order     INTEGER DEFAULT 0,
    FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE CASCADE,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL
);

CREATE INDEX idx_case_events_case ON case_events(case_id);
CREATE INDEX idx_case_events_date ON case_events(event_date);

-- ----------------------------------------------------------
-- ЦИТАТЫ С ТАЙМКОДАМИ
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS quotes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL,
    entity_id       INTEGER,        -- кто сказал
    quote_text      TEXT NOT NULL,
    timecode_start  TEXT,           -- HH:MM:SS или секунды
    timecode_end    TEXT,
    context         TEXT,           -- что обсуждалось
    rhetoric_class  TEXT,           -- neutral, offensive, dehumanizing, discriminatory,
                                    -- threatening, manipulative, contradictory, promise
    is_flagged      INTEGER DEFAULT 0,  -- требует ручной проверки
    verified_by     TEXT,
    verified_at     TEXT,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE SET NULL
);

CREATE INDEX idx_quotes_entity ON quotes(entity_id);
CREATE INDEX idx_quotes_flagged ON quotes(is_flagged);
CREATE INDEX idx_quotes_rhetoric ON quotes(rhetoric_class);

-- ----------------------------------------------------------
-- ПРОФИЛИ ДЕПУТАТОВ / ЧИНОВНИКОВ
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS deputy_profiles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    full_name       TEXT NOT NULL,
    position        TEXT,
    faction         TEXT,
    region          TEXT,
    committee       TEXT,
    duma_id         INTEGER,        -- ID на api.duma.gov.ru
    date_elected    TEXT,
    income_latest   TEXT,           -- последняя декларация (JSON)
    biography_url   TEXT,
    photo_url       TEXT,
    is_active       INTEGER DEFAULT 1,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    UNIQUE(entity_id)
);

CREATE INDEX idx_deputy_faction ON deputy_profiles(faction);
CREATE INDEX idx_deputy_region ON deputy_profiles(region);
CREATE INDEX idx_deputy_active ON deputy_profiles(is_active);

-- ----------------------------------------------------------
-- ACCOUNTABILITY INDEX (рейтинг подотчётности)
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS accountability_index (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    deputy_id       INTEGER NOT NULL,
    period          TEXT NOT NULL,  -- 2024, 2024-Q1, etc.
    public_speeches_count   INTEGER DEFAULT 0,
    verifiable_claims_count INTEGER DEFAULT 0,
    confirmed_contradictions INTEGER DEFAULT 0,
    flagged_statements_count INTEGER DEFAULT 0,  -- после ручной проверки
    votes_tracked_count     INTEGER DEFAULT 0,
    linked_cases_count      INTEGER DEFAULT 0,  -- только подтверждённые
    promises_made_count     INTEGER DEFAULT 0,
    promises_kept_count     INTEGER DEFAULT 0,
    calculated_score  REAL DEFAULT 0,
    FOREIGN KEY (deputy_id) REFERENCES deputy_profiles(id) ON DELETE CASCADE,
    UNIQUE(deputy_id, period)
);

-- ----------------------------------------------------------
-- ВЕРИФИКАЦИЯ — ИСТОРИЯ ПРОВЕРОК
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS verifications (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_id        INTEGER NOT NULL,
    verifier_type   TEXT NOT NULL,  -- auto_crosscheck, auto_registry, manual_editor, external_factcheck
    old_status      TEXT,
    new_status      TEXT,
    notes           TEXT,
    evidence_added  INTEGER DEFAULT 0,
    verified_by     TEXT,
    verified_at     TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (claim_id) REFERENCES claims(id) ON DELETE CASCADE
);

CREATE INDEX idx_verifications_claim ON verifications(claim_id);

-- ----------------------------------------------------------
-- ТЕГИ КОНТЕНТА (3-уровневая таксономия)
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS content_tags (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL,
    tag_level       INTEGER NOT NULL,  -- 1=тип_события, 2=тема, 3=риск/оценка
    tag_name        TEXT NOT NULL,
    confidence      REAL DEFAULT 1.0,
    tag_source      TEXT DEFAULT 'rule',  -- rule, ml, llm, manual
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE,
    UNIQUE(content_item_id, tag_level, tag_name)
);

CREATE INDEX idx_content_tags_item ON content_tags(content_item_id);
CREATE INDEX idx_content_tags_name ON content_tags(tag_name);
CREATE INDEX idx_content_tags_level ON content_tags(tag_level);

-- ----------------------------------------------------------
-- СВЯЗИ СУЩНОСТЕЙ (ГРАФ)
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS entity_relations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    from_entity_id  INTEGER NOT NULL,
    to_entity_id    INTEGER NOT NULL,
    relation_type   TEXT NOT NULL,  -- owns, directs, founded, related_to, contracted_with,
                                    -- judged, investigated, lobbied, same_address, same_director
    evidence_item_id INTEGER,
    strength        TEXT DEFAULT 'moderate',  -- strong, moderate, weak, unverified
    detected_at     TEXT DEFAULT (datetime('now')),
    detected_by     TEXT,           -- auto_pattern, manual, registry_match
    FOREIGN KEY (from_entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (to_entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (evidence_item_id) REFERENCES content_items(id) ON DELETE SET NULL
);

CREATE INDEX idx_entity_relations_from ON entity_relations(from_entity_id);
CREATE INDEX idx_entity_relations_to ON entity_relations(to_entity_id);
CREATE INDEX idx_entity_relations_type ON entity_relations(relation_type);

-- ----------------------------------------------------------
-- СИГНАЛЫ О РИСК-ПАТТЕРНАХ
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk_patterns (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern_type    TEXT NOT NULL,  -- repeat_contractor, address_overlap, director_overlap,
                                    -- budget_spike, bankruptcy_chain, income_mismatch
    description     TEXT NOT NULL,
    entity_ids      TEXT NOT NULL,  -- JSON: [id1, id2, ...]
    evidence_ids    TEXT,           -- JSON: [content_item_id, ...]
    risk_level      TEXT DEFAULT 'low',  -- low, medium, high, critical
    case_id         INTEGER,
    detected_at     TEXT DEFAULT (datetime('now')),
    needs_review    INTEGER DEFAULT 1,
    FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE SET NULL
);

CREATE INDEX idx_risk_patterns_type ON risk_patterns(pattern_type);
CREATE INDEX idx_risk_patterns_risk ON risk_patterns(risk_level);

-- ----------------------------------------------------------
-- ЛЕГАС-ТАБЛИЦЫ (из текущей БД, для миграции)
-- ----------------------------------------------------------
-- exports, messages, photos — оставляем как есть
-- данные переносятся через import_legacy_data()
```

---

## 5) Алгоритм сбора данных

## 5.1. Telegram pipeline

### Вариант доступа

- предпочтительно: **Telethon** (MTProto) для публичных каналов — работает надёжнее TDLib в Python
- для собственных каналов/ботов: Bot API
- не строить архитектуру, завязанную только на Bot API для чужих публичных каналов

### Шаги

1. Загрузить список утверждённых каналов из `sources`.
2. Для каждого канала получить:
   - новые посты
   - медиа
   - дату/время
   - id поста
   - количество просмотров/репостов/реакций (если доступно)
   - ссылки / вложения
3. Сохранить сырой объект в `raw_source_items`.
4. Скачать вложения в `raw_blobs` → файлы на диске.
5. Построить нормализованную запись `content_items`.
6. Извлечь сущности и утверждения.
7. Выставить статус `raw_signal`.
8. Запустить процедуру верификации.

### Что считать полезным сигналом

- обвинение
- сообщение о задержании / обыске / приговоре
- публикация договора / выписки / суда / повестки / закупки
- видео с высказыванием чиновника
- утверждение о собственности / компании / связи / голосовании

### Конкретная реализация

```python
# telegram_collector.py
# Telethon, mtproto session
# 1. Загрузка source_id из БД где category='telegram' AND is_active=1
# 2. Для каждого: client.get_messages(channel, limit=50, offset_id=last_saved_id)
# 3. Скачать медиа: client.download_media(msg, file=storage_path)
# 4. Сохранить в raw_source_items + raw_blobs
# 5. Запустить normalization → content_items
```

---

## 5.2. TikTok pipeline (watch-folder)

### Принцип

1. Папка `inbox/tiktok/` на диске
2. APScheduler: раз в 60 секунд проверять папку
3. Если есть `.mp4` файлы:
   - определить имя аккаунта из метаданных или имени файла
   - извлечь метаданные (ffprobe)
   - ASR (Whisper large-v3)
   - keyframe extraction (ffmpeg -vf fps=1/30)
   - OCR с ключевых кадров (PaddleOCR)
   - сохранить результат в content_items
   - переместить оригинал в `processed/tiktok/YYYY-MM/`
   - удалить из inbox

### Нормализованный результат

- `video_id` (из имени файла или метаданных)
- `account_id` (match по allowlist)
- `published_at` (из метаданных или file_mtime)
- `caption` (из описания, если есть)
- `hashtags`
- `audio_transcript` (Whisper)
- `screen_text` (OCR)
- `detected_entities` (NER)
- `claims` (claim_extractor)
- `quote_segments` (с таймкодами)

---

## 5.3. YouTube pipeline

### Принцип

1. Allowlist YouTube-каналов в `sources`
2. yt-dlp по расписанию (раз в 4 часа):
   - проверить новые видео
   - скачать видео + субтитры (auto-generated) + метаданные + thumbnail
   - сохранить в `processed/youtube/CHANNEL_ID/`
3. Если субтитров нет — ASR (Whisper)
4. Speaker diarization (pyannote) для заседаний с несколькими спикерами
5. OCR с ключевых кадров (если есть текст на экране)

### Команда yt-dlp

```bash
yt-dlp --write-auto-sub --sub-lang ru --write-info-json \
       --write-thumbnail --format "bestvideo[height<=720]+bestaudio/best[height<=720]" \
       -o "processed/youtube/%(channel_id)s/%(upload_date)s_%(id)s.%(ext)s" \
       CHANNEL_URL
```

---

## 5.4. Официальные сайты / реестры

Для каждого официального источника определить один из методов (см. таблицы в 3.1):

- API (api.duma.gov.ru, kad.arbitr.ru, egrul.nalog.ru)
- RSS (ТАСС, РБК, и т.д.)
- sitemap + change detection
- HTML polling (sudrf.ru, fssp.gov.ru)
- file watcher для публикуемых PDF/ZIP/XLSX (rosstat)
- ручная загрузка архивных массивов

### Общий цикл

1. Проверить обновления.
2. Скачать новый документ / карточку / запись.
3. Сохранить оригинал.
4. Вычислить hash.
5. Вытащить текст и метаданные.
6. Сопоставить с сущностями.
7. При необходимости создать или обновить кейс.

---

## 5.5. Приложенные документы (watch-folder)

Папка `inbox/documents/`

Пайплайн:

1. Получить файл.
2. Посчитать SHA-256.
3. Сохранить оригинал в `processed/documents/YYYY-MM/`.
4. Определить тип:
   - PDF text → text extraction (PyMuPDF)
   - PDF scan → OCR (PaddleOCR)
   - image → OCR (PaddleOCR)
   - doc/docx → python-docx
   - spreadsheet → openpyxl / pandas
   - audio → ASR (Whisper)
   - video → ASR + keyframes + OCR
5. Выделить:
   - номера дел
   - ФИО
   - должности
   - суммы
   - даты
   - адреса
   - реквизиты (ИНН, ОГРН)
6. Связать документ с кейсом.
7. Выставить уровень аутентичности.

---

## 5.6. Очередь задач и расписание

| Задача | Интервал | Примечание |
|--------|----------|------------|
| Проверка inbox/tiktok/ | 60 сек | APScheduler |
| Проверка inbox/documents/ | 60 сек | APScheduler |
| Telegram — новые посты | 5 мин | Telethon |
| YouTube — новые видео | 4 часа | yt-dlp |
| Официальные реестры — обновления | 24 часа | HTTP/API |
| СМИ — RSS | 30 мин | RSS parser |
| Переклассификация тегов | 6 часов | Rule-based + LLM |
| Верификация claims | 1 час | Cross-source |

---

## 6) Нормализация и единая модель данных

Модель описана в schema.sql (раздел 4.2). Ключевые сущности:

- `content_items` — единая запись о любой единице контента
- `attachments` — файлы, медиаматериалы, thumbnails, scans
- `entities` — люди, организации, партии, компании, суды, законы, etc.
- `entity_aliases` — алиасы (одна сущность = много написаний)
- `entity_mentions` — связь сущности с контентом
- `claims` — проверяемое утверждение
- `evidence_links` — связь утверждения с доказательствами
- `cases` — карточка истории / инцидента
- `case_events` — таймлайн кейса
- `quotes` — цитаты с привязкой к источнику и таймкоду
- `verifications` — история проверки и итоговый статус

---

## 7) Классификация

## 7.1. Базовые классы контента

1. закон / нормативный акт
2. законопроект
3. заседание / выступление
4. суд / приговор / апелляция / арест
5. силовое действие (обыск, задержание, этапирование и т.п.)
6. закупка / контракт / подрядчик
7. имущественный след
8. регистрационное действие юрлица
9. банкротство / взыскание / исполнительное производство
10. цензура / блокировка / РКН
11. мобилизационный кейс / повестка
12. публичное заявление / обещание / цитата
13. опровержение / противоречие / изменение позиции
14. коррупционный риск
15. региональная проблема / локальный конфликт

## 7.2. Таксономия тегов (3 уровня)

### Уровень 1 — тип события (tag_level=1)

| Тег | Описание |
|-----|----------|
| `court` | Суд / приговор / арест |
| `procurement` | Закупка / контракт |
| `speech` | Выступление / заявление |
| `law` | Закон / нормативный акт |
| `detention` | Задержание / обыск / арест |
| `property` | Имущество / недвижимость |
| `registry` | Регистрационное действие |
| `censorship` | Цензура / блокировка |
| `mobilization` | Мобилизация / повестка |
| `election` | Выборы / кампания |
| `protest` | Протест / митинг |
| `sanctions` | Санкции |
| `bankruptcy` | Банкротство / взыскание |
| `conflict` | Военный конфликт / силовая операция |
| `economic` | Экономика / цены / бюджет |
| `accident` | Происшествие / катастрофа |

### Уровень 2 — тема (tag_level=2)

| Тег | Описание |
|-----|----------|
| `housing` | ЖКХ / жильё |
| `healthcare` | Здравоохранение |
| `education` | Образование |
| `military` | Военная тематика |
| `media` | СМИ / медиа |
| `transport` | Транспорт |
| `construction` | Строительство |
| `ecology` | Экология |
| `finance` | Финансы / банки |
| `technology` | Технологии / интернет |
| `human_rights` | Права человека |
| `corruption` | Коррупционные риски |
| `law_enforcement` | Силовики / правоохранительные |
| `regional` | Региональная политика |
| `international` | Международная политика |

### Уровень 3 — риск/оценка (tag_level=3)

| Тег | Описание |
|-----|----------|
| `needs_verification` | Требует проверки |
| `official_confirmation` | Подтверждено официальными источниками |
| `contradicted` | Противоречит другим данным |
| `document_attached` | Приложен документ-доказательство |
| `possible_disinformation` | Возможная дезинформация |
| `possible_corruption` | Возможный коррупционный риск |
| `possible_conflict_of_interest` | Возможный конфликт интересов |
| `flagged_rhetoric` | Оскорбительная/опасная риторика (требует ручной проверки) |
| `pattern_detected` | Обнаружен риск-паттерн |

## 7.3. Как делать классификацию

Порядок:

1. **Rule-based pre-tagging** — regex + словари + справочники сущностей
2. **ML/LLM multi-label classifier** — Ollama (модель на G:\ollama)
3. **confidence score** — для каждого тега
4. **human review** для спорных кейсов

Не пытаться сразу сделать один "умный классификатор на всё".

Сначала:
- правила
- словари
- regex
- справочники сущностей (депутаты, партии, ведомства)
- затем LLM через Ollama

### 7.4. Миграция из news_tagging.py

Старый классификатор (TAG_RULES) — плоский, смешивает тип события, тему и платформу.

План миграции:
1. Разобрать TAG_RULES на 3 уровня
2. Убрать платформенные теги (Telegram, YouTube, Twitch, Discord) — это не тип события
3. Убрать финансовые теги узкого назначения (Крипта, Рынки/акции) → обобщить до `economic` + `finance`
4. Добавить `tag_level` в структуру
5. Переписать `infer_tags()` → `infer_tags_v2()` с 3-уровневым выходом
6. Хранить результаты в `content_tags` вместо строки в `messages.tags`

---

## 8) Алгоритм подтверждения / верификации

Это центральная часть проекта.

## 8.1. Статусы достоверности

Каждое утверждение и каждый кейс должны иметь статус:

1. `raw_signal` — исходный сигнал, не проверялся
2. `unverified` — проходит автоматическую проверку
3. `partially_confirmed` — есть частичные подтверждения
4. `confirmed` — подтверждено первичными источниками
5. `contradicted` — опровергнуто
6. `false_or_manipulated` — выявлена манипуляция
7. `archived_unresolved` — не удалось проверить

## 8.2. Правила верификации

### Правило 1. Разделять сигнал и доказательство

- Telegram/TikTok/YouTube/новость → **сигнал**
- судебный акт / выписка / карточка закупки / официальный реестр / официальная стенограмма → **доказательство**

### Правило 2. Для сильного утверждения нужны минимум 2 слоя

Пример:
- пост о задержании + карточка суда
- ролик с обвинением + выписка из ЕГРЮЛ + закупка / договор
- новость о высказывании + видео + стенограмма

### Правило 3. Документ выше пересказа

### Правило 4. Видео выше перепечатки, но только при наличии таймкода/контекста

### Правило 5. Любой OCR/ASR-фрагмент считается черновым, пока не проверен

### Правило 6. Один источник-сигнал = один сигнал. Несколько перепечаток одной новости ≠ корроборация

## 8.3. Формальная модель оценки

Для каждого claim считать:

```
source_score:
  - tier A источник: 0.5
  - tier B источник: 0.3
  - tier C источник: 0.15
  - tier D источник: 0.05

document_score:
  - 0: нет документов = 0
  - 1: один вторичный документ = 0.2
  - 2: один первичный документ (реестр/суд) = 0.5
  - 3: два+ первичных документа = 0.8

corroboration_score:
  - 0: только один источник = 0
  - 1: два независимых сигнала = 0.2
  - 2: три+ независимых сигнала = 0.3
  - 3: сигнал + документ = 0.5

consistency_score:
  - противоречий нет: 0.1
  - мелкие несовпадения: 0.0
  - серьёзные противоречия: -0.3

manipulation_risk:
  - нет признаков: 0
  - сомнительное происхождение: 0.1
  - следы монтажа/подделки: 0.3
  - противоречащие данные: 0.5

editor_review_score:
  - редактор подтвердил: 0.3
  - редактор отклонил: -0.5
  - не проверялось: 0
```

Формула:

```
final_confidence = source_score + document_score + corroboration_score
                   + consistency_score + editor_review_score - manipulation_risk
```

| final_confidence | Статус |
|-----------------|--------|
| >= 1.0 | confirmed |
| 0.6 – 0.99 | partially_confirmed |
| 0.2 – 0.59 | unverified |
| -0.2 – 0.19 | raw_signal / needs_review |
| < -0.2 | contradicted / false_or_manipulated |

Итоговый статус всегда зависит не только от числа, а от правил. Если `manipulation_risk >= 0.3` — всегда `needs_review`.

## 8.4. Аутентичность документов

Проверять:

1. происхождение файла
2. hash (SHA-256)
3. наличие подписи / QR / реквизитов
4. совпадение метаданных с текстом
5. наличие документа в официальном реестре
6. визуальные следы монтажа / подделки
7. соответствие шрифтов / печатей / структуры типовым бланкам

---

## 9) Модуль по депутатам и чиновникам

Это отдельный контур, не смешивать с общим новостным потоком.

## 9.1. Что собирать

1. карточки депутата/сенатора/чиновника
2. партия / фракция / регион
3. официальные биографические данные
4. публичные выступления
5. стенограммы
6. видео заседаний
7. голосования
8. депутатские запросы
9. официальные заявления в соцсетях
10. имущественные/антикоррупционные сведения, если доступны

## 9.2. Что извлекать

1. цитаты
2. обещания
3. фактические утверждения
4. оскорбительные / уничижительные формулировки
5. призывы / угрозы / дискриминационные высказывания
6. противоречия с собственными более ранними словами
7. связь слов с голосованиями и законопроектами

## 9.3. Как делать модуль цитат

Пайплайн:

1. скачать видео/стенограмму (yt-dlp / api.duma.gov.ru)
2. ASR (Whisper large-v3) — получить текст
3. speaker diarization (pyannote-audio) — разделить по спикерам
4. выровнять текст по таймкодам
5. определить говорящего (match с deputy_profiles)
6. сохранить цитаты длиной 1–3 предложения в `quotes`
7. привязать к:
   - видео URL
   - таймкоду
   - заседанию
   - дате
   - теме
8. прогнать по классификатору риторики (LLM через Ollama)
9. обязательно отправить флаги на ручную проверку (`is_flagged=1`)

### ASR для заседаний

Для заседаний Госдумы — сначала проверить наличие стенограммы на api.duma.gov.ru.
Стенограмма = готовый текст, ASR не нужен.
ASR нужен только если стенограммы нет, а есть только видео.

### Speaker identification

1. pyannote-audio — diarization (разделение спикеров)
2. Для каждого сегмента — embedding (x-vectors)
3. Сравнить с референсными embedding депутатов (построить заранее из известных видео)
4. Результат — `entity_id` в `quotes`

## 9.4. Accountability Index

Не делать "рейтинг гнид". Делать **Accountability Index**:

| Метрика | Вес | Описание |
|---------|-----|----------|
| public_speeches_count | 1 | Число публичных выступлений |
| verifiable_claims_count | 1 | Число проверяемых утверждений |
| confirmed_contradictions | 3 | Число подтверждённых противоречий |
| flagged_statements_count | 5 | Число зафиксированных оскорбительных фраз (после ручной проверки) |
| votes_tracked_count | 1 | Число голосований по выбранным темам |
| linked_cases_count | 3 | Связанные кейсы (только подтверждённые) |
| promises_made_count | 1 | Обещания |
| promises_kept_count | -2 | Невыполненные обещания = promises_made - promises_kept |

```
score = (confirmed_contradictions * 3 + flagged_statements_count * 5 +
         linked_cases_count * 3 + (promises_made - promises_kept) * 2) /
        max(public_speeches_count, 1)
```

Чем выше score — тем хуже подотчётность. Но это **не обвинение**, а индекс проверяемости.

---

## 10) Поиск коррупционных схем

Система не должна автоматически писать "коррупция доказана". Она должна строить **risk graph**.

## 10.1. Какие паттерны искать

1. повторяющиеся подрядчики у одного заказчика
2. группа компаний с совпадающими:
   - адресами
   - директорами
   - учредителями
   - телефонами
   - доменами
3. резкий рост контрактов после назначения чиновника
4. цепочка банкротств / ликвидаций / переоформлений
5. суды и взыскания вокруг подрядчиков
6. связи между депутатом/чиновником и коммерческими субъектами через родственников/партнёров/адреса
7. несоответствие публичных слов и голосований/решений
8. бюджетные деньги → подрядчик → связанное лицо

## 10.2. Какие данные нужны для графа

- ФИО, роли, должности
- компании, ИНН/ОГРН
- адреса
- госзаказчики, контракты, суммы, даты
- судебные дела, исполнительные производства
- публичные заявления

## 10.3. Результат

Не "обвинение", а:

- `pattern_detected`
- `evidence_bundle`
- `risk_level`
- `needs_editor_review`

---

## 11) Индексация и поиск

Нужен гибридный поиск:

1. full-text search (SQLite FTS5 на MVP → OpenSearch на проде)
2. фильтры по сущностям
3. фильтры по времени
4. фильтры по регионам
5. фильтры по статусу верификации
6. графовые связи (entity_relations)
7. поиск по цитатам
8. поиск по номеру дела / ИНН / ОГРН / закону / закупке

Для поиска важно хранить:

- лемматизированный текст
- оригинальный текст
- транскрипт
- OCR-текст
- алиасы сущностей

### SQLite FTS5 (MVP)

```sql
CREATE VIRTUAL TABLE content_search USING fts5(
    title, body_text, ocr_text, transcript,
    content='content_items',
    content_rowid='id'
);
```

---

## 12) OCR / ASR / NLP — стек

| Компонент | Инструмент | Путь / Примечание |
|-----------|------------|-------------------|
| ASR | Whisper large-v3 | `G:\ollama\models\whisper-large-v3` или через huggingface |
| OCR | PaddleOCR (server) | Локально, лучший для русского |
| NER | Natasha + spaCy | Локально |
| LLM (классификация/claim extraction) | Ollama | `G:\ollama`, модель qwen2.5:14b или llama3.1:8b |
| Speaker diarization | pyannote-audio | Локально, нужен HF token |
| Keyframe extraction | ffmpeg + OpenCV | Локально |
| Text extraction PDF | PyMuPDF (fitz) | Локально |
| Text extraction DOCX | python-docx | Локально |
| Text extraction XLSX | openpyxl | Локально |

### Конкретные модели для Ollama

```bash
# Установить Ollama на G:\ollama
OLLAMA_MODELS=G:\ollama\models ollama pull qwen2.5:14b    # для классификации и claim extraction
OLLAMA_MODELS=G:\ollama\models ollama pull nomic-embed-text # для эмбеддингов (поиск)
```

### Whisper large-v3

```bash
# Через huggingface / faster-whisper
pip install faster-whisper
# Модель скачается автоматически или укажем путь:
# G:\ollama\models\faster-whisper-large-v3
```

---

## 13) Структура папок проекта

```
F:\новости\                          — код проекта
F:\новости\inbox\tiktok\             — входящие TikTok-ролики
F:\новости\inbox\documents\          — входящие документы
F:\новости\inbox\youtube\            — входящие YouTube-видео (если вручную)
F:\новости\processed\tiktok\         — обработанные TikTok (по YYYY-MM/)
F:\новости\processed\youtube\        — обработанные YouTube (по CHANNEL/YYYY-MM/)
F:\новости\processed\documents\      — обработанные документы (по YYYY-MM/)
F:\новости\processed\telegram\       — медиа из Telegram (по CHANNEL/YYYY-MM/)
F:\новости\processed\keyframes\      — ключевые кадры из видео
F:\новости\db\news_unified.db        — основная БД (SQLite)
F:\новости\db\schema.sql             — DDL
F:\новости\config\sources_seed.json  — начальный список источников
F:\новости\config\settings.json      — настройки (пути, интервалы, ключи)

G:\ollama\                           — модели Ollama
G:\ollama\models\                    — файлы моделей
```

---

## 14) Техническая реализация по этапам

## Этап 1. Каркас проекта и миграция БД
**Оценка: 3–5 дней**

- [x] Создать `config/settings.json` с путями и интервалами
- [x] Создать `config/sources_seed.json` с начальным списком источников
- [x] Описать `db/schema.sql` — полный DDL из раздела 4.2
- [x] Написать `db/migrate.py` — создание новых таблиц + импорт легас-данных
- [x] Импортировать текущие `exports/messages/photos` в новую схему
- [ ] Настроить APScheduler для периодических задач
- [x] Настроить логирование (файл + консоль)

## Этап 2. Реестр источников
**Оценка: 1–2 дня**

- [ ] Завести все источники приоритета A из раздела 3.1 в sources_seed.json
- [ ] Завести seed-список Telegram (47 каналов из раздела 3.2.B1)
- [ ] Завести allowlist TikTok (10 аккаунтов из раздела 3.2.B2)
- [ ] Завести YouTube (14 каналов из раздела 3.2.B3)
- [ ] Завести СМИ (17 из раздела 3.3)
- [ ] Определить для каждого `access_method`
- [ ] Написать `source_registry.py` — CRUD для источников

## Этап 3. Сбор данных — Telegram
**Оценка: 3–5 дней**

- [ ] Установить Telethon
- [ ] Написать `collectors/telegram_collector.py`
- [ ] Авторизация через MTProto (session файл)
- [ ] Загрузка новых постов по allowlist
- [ ] Скачивание медиа (фото, видео, документы)
- [ ] Сохранение в raw_source_items + raw_blobs
- [ ] Нормализация → content_items
- [ ] Настройка расписания (5 мин)

## Этап 4. Сбор данных — Watch-folder (TikTok / Documents / YouTube manual)
**Оценка: 2–3 дня**

- [ ] Написать `collectors/watch_folder.py`
- [ ] Проверка inbox/tiktok/ раз в 60 сек
- [ ] Проверка inbox/documents/ раз в 60 сек
- [ ] Проверка inbox/youtube/ раз в 60 сек
- [ ] Определение типа файла (ffprobe)
- [ ] Перемещение в processed/ после обработки
- [ ] Обработка ошибок (битые файлы, неподдерживаемые форматы)

## Этап 5. Сбор данных — YouTube (авто)
**Оценка: 2–3 дня**

- [ ] Установить yt-dlp
- [ ] Написать `collectors/youtube_collector.py`
- [ ] Проверка новых видео по allowlist (раз в 4 часа)
- [ ] Скачивание: видео + субтитры + метаданные + thumbnail
- [ ] Сохранение в raw_source_items + raw_blobs
- [ ] Нормализация → content_items

## Этап 6. Извлечение текста
**Оценка: 5–7 дней**

- [ ] Установить Whisper (faster-whisper), PaddleOCR, PyMuPDF, python-docx, openpyxl
- [ ] Написать `media_pipeline/asr.py` — Whisper large-v3, русский
- [ ] Написать `media_pipeline/ocr.py` — PaddleOCR для русского
- [ ] Написать `media_pipeline/pdf_extract.py` — PyMuPDF
- [ ] Написать `media_pipeline/keyframes.py` — ffmpeg keyframe extraction
- [ ] Написать `media_pipeline/speaker_diarization.py` — pyannote-audio (для заседаний)
- [ ] Интеграция: content_item → media_pipeline → attachments (ocr_text) + quotes

## Этап 7. Сущности и теги (3-уровневая таксономия)
**Оценка: 5–7 дней**

- [ ] Написать `ner/extractor.py` — Natasha + spaCy для русского NER
- [ ] Справочники депутатов, партий, ведомств (из api.duma.gov.ru)
- [ ] Написать `ner/entity_resolver.py` — matching + alias
- [ ] Написать `classifier/rule_based.py` — 3-уровневая таксономия
  - level 1: тип события (16 тегов)
  - level 2: тема (15 тегов)
  - level 3: риск/оценка (9 тегов)
- [ ] Переписать news_tagging.py → classifier/tagger_v2.py
- [ ] Написать `classifier/llm_classifier.py` — Ollama (qwen2.5:14b)
- [ ] Хранение результатов в content_tags
- [ ] LLM fallback для нераспознанных тегов

## Этап 8. Claim extraction
**Оценка: 3–5 дней**

- [ ] Написать `claims/extractor.py` — извлечение утверждений из текста
- [ ] Rule-based: паттерны ("X заявил", "X голосовал за", "суд вынес", и т.д.)
- [ ] LLM-based: Ollama для сложных случаев
- [ ] Сохранение в claims
- [ ] Привязка к entity_mentions

## Этап 9. Верификация
**Оценка: 5–7 дней**

- [ ] Реализовать таблицы: claims, evidence_links, verifications
- [ ] Написать `verification/engine.py`
  - source_score по credibility_tier
  - document_score по наличию первичных документов
  - corroboration_score по числу независимых источников
  - consistency_score по противоречиям
  - manipulation_risk по признакам
- [ ] Кросс-проверка: поиск evidence в content_items по entity/claim
- [ ] Автоматический поиск в реестрах (ЕГРЮЛ, kad.arbitr, ФССП) по извлечённым ИНН/ФИО
- [ ] Формула final_confidence → статус

## Этап 10. Карточки кейсов
**Оценка: 3–5 дней**

- [ ] Реализовать cases, case_claims, case_events
- [ ] Написать `cases/builder.py` — автоматическая сборка кейса по связанным claims
- [ ] Таймлайн кейса (case_events)
- [ ] Связанная доказательная база (evidence_links через case_claims)
- [ ] Статус кейса
- [ ] Экспорт кейса в markdown/pdf/html

## Этап 11. Модуль депутатов
**Оценка: 7–10 дней**

- [ ] Импорт списка депутатов из api.duma.gov.ru → deputy_profiles
- [ ] Импорт стенограмм → content_items (type=speech)
- [ ] Импорт голосований → content_items (type=vote_record)
- [ ] Импорт видео заседаний (yt-dlp с youtube.com/@gosdumaRF)
- [ ] Speaker identification для видео заседаний
- [ ] Модуль цитат: quotes с таймкодами и rhetoric_class
- [ ] Классификатор риторики (LLM через Ollama)
- [ ] Поиск по высказываниям
- [ ] Accountability Index (расчёт и обновление)
- [ ] Ручная верификация оскорбительных фраз (is_flagged=1)

## Этап 12. Аналитика и граф связей
**Оценка: 7–10 дней**

- [ ] Реализовать entity_relations
- [ ] Написать `graph/builder.py` — построение графа связей
- [ ] Поиск связей по закупкам (ЕГРЮЛ + zakupki.gov.ru)
- [ ] Поиск связей по адресам/директорам/учредителям
- [ ] Эвристики конфликта интересов
- [ ] Реализовать risk_patterns
- [ ] Визуализация графа (PySide6 + graph layout)

## Этап 13. Панель редактора (PySide6)
**Оценка: 7–10 дней**

- [ ] Расширить news_calendar_pyside6.py → editor_dashboard.py
- [ ] Вкладки: Лента / Кейсы / Депутаты / Поиск / Настройки
- [ ] Лента: фильтры по типу, тегу, статусу, дате, источнику
- [ ] Карточка кейса: таймлайн, доказательства, статус, claims
- [ ] Карточка депутата: цитаты, голосования, accountability index
- [ ] Очередь на проверку (needs_review=1)
- [ ] Действия редактора: подтвердить / отклонить / добавить evidence
- [ ] История изменений (verifications)
- [ ] Полный текстовый поиск (FTS5)

## Этап 14. Сбор данных — официальные реестры (v1)
**Оценка: 10–15 дней**

- [ ] Написать `collectors/web_collector.py` — универсальный HTTP-scraper
- [ ] Реализовать сбор с API-источников:
  - api.duma.gov.ru (депутаты, стенограммы, голосования)
  - egrul.nalog.ru API (выписки ЕГРЮЛ)
  - kad.arbitr.ru API (арбитражные дела)
  - zakupki.gov.ru API (закупки)
- [ ] Реализовать HTML-scraping:
  - sudrf.ru (суды общей юрисдикции)
  - fssp.gov.ru (исполнительные производства)
  - minjust.gov.ru (иноагенты, экстремисты)
  - eais.rkn.gov.ru (запрещённая информация)
- [ ] Реализовать RSS:
  - ТАСС, РБК, Коммерсантъ, Интерфакс, etc.
- [ ] Нормализация → content_items
- [ ] Связь с entity_mentions

---

## 15) Безопасность и юридическая защита

### 15.1. Формулировки

| Не говорить | Говорить |
|-------------|----------|
| "Коррупция доказана" | "Обнаружен риск-паттерн, подтверждённый N документами" |
| "X — вор" | "Имущество X не соответствует задекларированному доходу (документы: ...)" |
| "X — гнида" | "Accountability Index X: N подтверждённых противоречий, N оскорбительных высказываний" |
| "Власть скрывает" | "Документ Y не найден в открытом доступе; запрос Z оставлен без ответа" |
| "Это ложь" | "Утверждение X противоречит данным из [источник A] и [документ B]" |

### 15.2. Техническая безопасность

- БД на локальном диске (не в облаке для MVP)
- Ролевая модель: admin / editor / viewer (позже)
- Резервное копирование БД ежедневно (sqlite3 backup API)
- Шифрование диска — на усмотрение (BitLocker / VeraCrypt)
- VPN для заблокированных источников
- Не хранить токены/пароли в коде — в config/secrets.json (.gitignore)

### 15.3. Домены для VPN

Если что-то не открывается — добавить в исключения VPN:

```
meduza.io
mediazona.ru
proekt.media
verstka.media
istories.media
novayagazeta.ru
dw.com
bbc.com/russian
reuters.com
youtube.com (если заблокирован)
tass.ru (иногда)
```

---

## 16) Миграция из текущего кода

### 16.1. Что есть сейчас

- `news_unified.db` — SQLite с таблицами exports, messages, photos
- `news_tagging.py` — плоский классификатор TAG_RULES
- `news_unified_pipeline.py` — парсинг HTML-экспортов Telegram + sync-db
- `news_calendar_pyside6.py` — UI календарь

### 16.2. Что сделать с текущим кодом

1. **news_tagging.py** — не удалять. Переписать TAG_RULES на 3-уровневую таксономию. Оставить `infer_tags()` как совместимый, добавить `infer_tags_v2()`.
2. **news_unified_pipeline.py** — оставить для импорта HTML-экспортов. Добавить `import_legacy_to_new_schema()`.
3. **news_calendar_pyside6.py** — расширить до editor_dashboard.
4. **news_unified.db** — не удалять. Новые таблицы создаются рядом. Миграция данных.

### 16.3. Почему прошлый классификатор не взлетел

1. не было жёсткой таксономии
2. смешивались тема, тон, тип события и оценка достоверности
3. одна запись пыталась получить слишком много тегов без иерархии
4. не было separation between source_item / claim / case
5. не было слоя ручной проверки

### 16.4. Как правильно теперь

Разделить на 4 уровня:

1. `source_item` — пост как он есть
2. `claim` — проверяемое утверждение
3. `evidence` — документ/реестр/видео/стенограмма
4. `case` — собранная история

---

## 17) MVP-план

### MVP-1 (4–6 недель)

Система должна уметь:

1. Собирать 20–50 Telegram-каналов (Telethon)
2. Принимать документы через watch-folder (inbox/documents/)
3. Принимать TikTok через watch-folder (inbox/tiktok/)
4. Извлекать текст (ASR, OCR, PDF)
5. Ставить 3-уровневые теги (rule-based)
6. Создавать claims (rule-based)
7. Прикладывать evidence
8. Давать редактору список материалов needs_review
9. PySide6 панель: лента + карточки + фильтры

### MVP-2 (+4–6 недель)

Добавить:

1. YouTube collector (yt-dlp)
2. Модуль депутатов (api.duma.gov.ru + стенограммы)
3. Цитаты + таймкоды + rhetoric_class
4. Простую оценку достоверности (verification engine)
5. Полный текстовый поиск (FTS5)
6. Сбор с официальных реестров (API: ЕГРЮЛ, kad.arbitr, api.duma)

### MVP-3 (+4–6 недель)

Добавить:

1. Граф связей (entity_relations + визуализация)
2. Risk patterns по закупкам/юрлицам
3. Accountability Index для депутатов
4. Автоматическую сборку досье (case_builder)
5. Экспорт кейсов
6. Сбор с HTML-scraping источников (суды, ФССП, РКН)
7. LLM-классификацию (Ollama)

---

## 18) Сроки и приоритеты — сводная таблица

| Этап | Описание | Дней | Приоритет | Зависимость |
|------|----------|------|-----------|-------------|
| 1 | Каркас + миграция БД | 3–5 | P0 | — |
| 2 | Реестр источников | 1–2 | P0 | Этап 1 |
| 3 | Telegram collector | 3–5 | P0 | Этап 1–2 |
| 4 | Watch-folder | 2–3 | P0 | Этап 1 |
| 5 | YouTube collector | 2–3 | P1 | Этап 4 |
| 6 | OCR/ASR pipeline | 5–7 | P0 | Этап 1 |
| 7 | NER + теги (3 уровня) | 5–7 | P0 | Этап 6 |
| 8 | Claim extraction | 3–5 | P1 | Этап 7 |
| 9 | Верификация | 5–7 | P1 | Этап 8 |
| 10 | Карточки кейсов | 3–5 | P1 | Этап 9 |
| 11 | Модуль депутатов | 7–10 | P2 | Этап 6–7 |
| 12 | Граф связей | 7–10 | P2 | Этап 7–9 |
| 13 | Панель редактора | 7–10 | P0 | Этап 7 |
| 14 | Официальные реестры | 10–15 | P1 | Этап 2 |

**MVP-1**: этапы 1–7 + 13 ≈ 25–40 дней
**MVP-2**: этапы 5, 8–10, 14 ≈ 20–35 дней
**MVP-3**: этапы 11–12 ≈ 15–20 дней

---

## 19) Главная методологическая мысль

Лучший вариант для этого проекта:

- не "парсер новостей"
- а **система документирования сигналов, утверждений и доказательств**

То есть архитектура должна быть ближе не к новостному агрегатору, а к смеси:

- OSINT-пайплайна
- доказательного архива
- расследовательской CRM
- поисковой системы по публичным фактам

---

## 20) Что делать дальше прямо сейчас

### Шаг 1 — каркас
- [x] Создать `config/settings.json` (пути, интервалы)
- [x] Создать `config/sources_seed.json` (все источники из раздела 3)
- [x] Создать `db/schema.sql` (из раздела 4.2)
- [x] Написать `db/migrate.py` (создание таблиц + импорт легаса)

### Шаг 2 — Telegram
- [ ] Установить Telethon: `pip install telethon`
- [x] Написать `collectors/telegram_collector.py`
- [ ] Авторизоваться (создать session)
- [ ] Протестировать на 2–3 каналах

### Шаг 3 — Watch-folder
- [x] Создать папки inbox/ и processed/
- [x] Написать `collectors/watch_folder.py`
- [ ] Протестировать: кинуть mp4 в inbox/tiktok/

### Шаг 4 — OCR/ASR
- [ ] Установить faster-whisper, PaddleOCR, PyMuPDF
- [ ] Скачать Whisper large-v3 (на G:\ollama\models\)
- [ ] Установить Ollama, скачать qwen2.5:14b
- [x] Написать media_pipeline/asr.py
- [x] Написать media_pipeline/ocr.py

### Шаг 5 — Теги v2
- [x] Переписать TAG_RULES на 3 уровня
- [x] Написать classifier/tagger_v2.py
- [ ] Протестировать на текущих данных из БД

### Шаг 6 — Панель
- [ ] Расширить news_calendar_pyside6.py
- [ ] Добавить вкладку "Кейсы" и "На проверку"
- [ ] Добавить фильтры по статусу/тегу/источнику

### Шаг 7 — Obsidian-архив
- [x] Добавить `tools/export_obsidian.py` для выгрузки БД в Obsidian
- [x] Скопировать медиа/файлы из `raw_blobs` в `Attachments`
- [x] Проверить полный экспорт: `F:\новости\obsidian_export`
