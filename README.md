# Новостной доказательный архив

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

Полный экспорт:

```powershell
python tools\export_obsidian.py --vault .\obsidian_export
```

Экспорт создаёт разделы `Sources`, `Content`, `Claims`, `Cases`, `Entities`, `Files` и копирует медиа в `Attachments`.

## Проверка

```powershell
python -m compileall -q .
python db\migrate.py --no-legacy
```
