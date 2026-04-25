# Civic Evidence Lab

Локальная система для сбора публичных сигналов, файлов, фото, документов и последующей сборки проверяемых утверждений, доказательств и кейсов.

## Быстрый старт

```powershell
python db\migrate.py
python main.py
```

Основная SQLite-БД: `db/news_unified.db`.
Старый импорт Telegram хранится в корневой `news_unified.db` и используется миграцией как источник легаси-данных.

Локальные машинные настройки хранятся в `config/settings.json`.
В репозитории лежит шаблон `config/settings.example.json` без абсолютных путей и локальных секретов.

## 24/7 runtime

Фоновый сбор и nightly pipeline теперь живут вне UI в отдельном daemon-процессе.
Desktop dashboard только показывает состояние и вручную запускает job'ы; владельцем long-running scheduler больше не является.

Полезные команды:

```powershell
python -m runtime.healthcheck
python -m runtime.run_job --job relations
python -m runtime.run_job --job classifier_audit
python -m runtime.run_pipeline --mode nightly
python -m runtime.daemon
python -m runtime.recover --request-daemon-stop
python -m runtime.task_scheduler
```

Runtime пишет состояние прямо в `db/news_unified.db`:
- `job_runs`, `job_leases`, `pipeline_runs`
- `source_health_checks`, `source_sync_state`, `dead_letter_items`
- `relation_candidates`, `relation_support`, `classifier_audit_samples`, `runtime_metadata`

Nightly pipeline собирает `pipeline_version`, прогоняет `classifier_audit`/drift gate, пересобирает `db/news_analysis.db` и затем запускает Obsidian export.
Локальный путь выгрузки берётся из `config/settings.json` через `obsidian_export_dir`; шаблон в `config/settings.example.json` по умолчанию указывает на `obsidian_export_graph`.

Для автозапуска daemon при логоне есть bootstrap:

```powershell
python -m runtime.task_scheduler
python -m runtime.task_scheduler --query
python -m runtime.task_scheduler --remove
```

Скрипт сначала пытается зарегистрировать задачу в Windows Task Scheduler. Если текущая сессия не имеет прав на `schtasks /Create`, он автоматически пишет launcher в user Startup folder и сообщает это в результате (`install_mode=startup_folder`).

## UI

`main.py` запускает встроенный HTML/CSS/JS dashboard внутри PySide6 через `QWebEngineView + QWebChannel`.
Web bundle лежит в `ui_web/`, bridge и controller-логика — в `ui/web_bridge.py` и `ui/web_window.py`.

## Executive directories

Официальный сбор руководителей и заместителей госорганов живёт в `collectors/executive_directory_scraper.py`.
Активные leadership sources сейчас описаны в `config/executive_sources.json`.

Быстрый ручной прогон:

```powershell
python -m collectors.executive_directory_scraper
```

## Файловая модель

- `raw_source_items` хранит исходный сигнал и сырой JSON.
- `raw_blobs` хранит канонический реестр файлов на диске: путь, имя, MIME, размер, SHA-256 и служебные метаданные.
- `attachments` связывает файлы из `raw_blobs` с нормализованными `content_items`.
- Файлы физически лежат в `processed/*`; БД хранит метаданные и связи, а не только плоский список SQL-записей.

## Экспорт в Obsidian

Smoke-test:

```powershell
python tools\export_obsidian.py --limit 5 --vault .\obsidian_export_smoke
```

Полный graph-export:

```powershell
python -m runtime.run_pipeline --mode nightly
```

Ручной вариант по-прежнему доступен:

```powershell
python tools\build_analysis_snapshot.py
python tools\export_obsidian.py --db .\db\news_analysis.db --vault .\obsidian_export_graph --mode graph
```

Экспорт создаёт разделы `Sources`, `Content`, `Claims`, `Cases`, `Entities`, `Bills`, `VoteSessions`, `Contracts`, `Risks`, `WeakLinks`, `Tags`, `Files` и копирует медиа в `Attachments`. В `graph`-режиме index note также хранит `built_from_pipeline_version`.

## OCR fallback

`media_pipeline/ocr.py` теперь работает в режиме `auto`: сначала пробует `PaddleOCR`, но при несовместимостях вида `oneDNN/PIR` автоматически переключается на `RapidOCR + ONNXRuntime`.
После первого успешного fallback backend фиксируется в `source_sync_state.metadata_json`, и следующий batch с `ocr_engine=auto` стартует уже с `rapidocr`, не прогревая снова сломанный Paddle runtime.

## Проверка

```powershell
python -m unittest discover -s tests -v
python -m py_compile main.py ui\web_window.py ui\web_bridge.py runtime\daemon.py runtime\runner.py runtime\state.py runtime\task_scheduler.py graph\relation_candidates.py classifier\audit.py media_pipeline\ocr.py tools\build_analysis_snapshot.py tools\export_obsidian.py tools\export_obsidian_graph.py
```
