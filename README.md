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
python tools\build_analysis_snapshot.py
python tools\export_obsidian.py --db .\db\news_analysis.db --vault .\obsidian_export_graph --mode graph
```

Экспорт создаёт разделы `Sources`, `Content`, `Claims`, `Cases`, `Entities`, `Bills`, `VoteSessions`, `Contracts`, `Risks`, `WeakLinks`, `Tags`, `Files` и копирует медиа в `Attachments`.

## Проверка

```powershell
python -m unittest discover -s tests -v
python -m py_compile main.py ui\web_window.py ui\web_bridge.py collectors\executive_directory_scraper.py collectors\zakupki_scraper.py tools\build_analysis_snapshot.py
```
