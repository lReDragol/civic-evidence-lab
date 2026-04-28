# Civic Evidence Lab

Local evidence pipeline for collecting public signals, preserving source
materials, extracting claims and events, and building explainable dossiers over a
SQLite-backed corpus.

The project is designed for local Windows operation: collectors and runtime jobs
run on the machine, generated databases and media stay outside git, and the UI is
an operator console over the local store.

## What It Does

- Collects public materials from feeds, official sources, Telegram exports,
  watch folders, documents, and media.
- Stores raw source records, file blobs, attachments, normalized content, claims,
  events, facts, entities, and evidence links in SQLite.
- Separates source signals from reviewed or evidence-backed facts.
- Builds relation candidates and bridge paths so dossier views can explain why
  two entities or events are connected.
- Runs quality gates before publishing derived snapshots or Obsidian exports.
- Provides a PySide6/Web dashboard for monitoring jobs, reviewing queues, and
  exploring events, relations, and source health.

## What It Is Not

- Not a news repost bot.
- Not a hosted SaaS service.
- Not a repository of collected databases, media, API keys, or Telegram
  sessions.
- Not an automatic truth oracle: weak or ambiguous material is kept as a signal
  or routed to review.

## Repository Layout

| Path | Purpose |
| --- | --- |
| `analysis/` | AI sweep, event pipeline, and relation analysis |
| `cases/` | Accountability, involvement, and risk case builders |
| `claims/` | Claim extraction helpers |
| `classifier/` | Tagging, semantic index, audit, and classification logic |
| `collectors/` | Telegram, RSS, official-source, registry, and watch-folder collectors |
| `config/` | Example settings, source manifests, and seed data |
| `db/` | Schema, migrations, backups, and file-store helpers |
| `enrichment/` | Deduplication, profiles, disclosures, assets, and restrictions |
| `graph/` | Relation candidate logic |
| `investigation/` | Dossier and graph exploration APIs |
| `llm/` | Provider key pool and routing |
| `media_pipeline/` | OCR and ASR integrations |
| `ner/` | Entity extraction and resolution |
| `quality/` | Pipeline gate checks |
| `runtime/` | Job registry, daemon, scheduler, state, and pipeline orchestration |
| `search/` | Search helpers |
| `tests/` | Unit and smoke tests |
| `tools/` | Maintained CLI utilities for snapshots, exports, audits, and imports |
| `ui/`, `ui_web/` | Desktop shell and embedded web dashboard |
| `verification/` | Evidence linking, contradiction checks, and re-verification |

## Data Boundary

The repository intentionally excludes:

- SQLite databases and WAL/SHM files;
- source exports, processed media, inbox folders, generated reports, and
  Obsidian vault output;
- API keys, provider key dumps, local settings, Telegram sessions, and secrets;
- runtime logs, caches, archives, temporary scripts, and one-off debug probes.

Use `config/settings.example.json` as the public template. Keep real
`config/settings.json`, `key.json`, databases, and collected files local.

## Quick Start

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -r requirements.txt
playwright install
Copy-Item config\settings.example.json config\settings.json
```

Create or update the local schema without importing a legacy root database:

```powershell
python -m db.migrate --no-legacy
```

Launch the desktop dashboard:

```powershell
python main.py
```

Run the background daemon:

```powershell
python -m runtime.daemon
```

Run one job manually:

```powershell
python -m runtime.run_job --job source_health
python -m runtime.run_job --job event_pipeline
python -m runtime.run_job --job quality_gate
```

Run an orchestrated pipeline:

```powershell
python -m runtime.run_pipeline --mode nightly
```

## Common Jobs

| Job | Purpose |
| --- | --- |
| `source_health` | Check live/archive/fixture state for configured sources |
| `watch_folder` | Ingest files from configured inbox folders |
| `telegram`, `rss`, `official` | Collect configured source families |
| `tagger`, `semantic_index` | Classify and index normalized content |
| `event_pipeline` | Build derived events, timelines, and event facts |
| `relations` | Build entity relation candidates |
| `quality_gate` | Validate whether derived publication can proceed |
| `analysis_snapshot` | Build the derived analysis database |
| `obsidian_export` | Export graph notes and attachments to the configured vault |
| `ai_full_sweep` | Run the high-cost AI processing queue manually |

Registered jobs are defined in `runtime/registry.py`.

## Configuration

Important settings in `config/settings.json`:

- `db_path`: local live SQLite database, usually `db/news_unified.db`.
- `analysis_db_path`: derived analysis database.
- `legacy_db_path`: optional old import database used by `db.migrate`.
- `obsidian_export_dir`: local export destination.
- `telegram_api_id`, `telegram_api_hash`, `telegram_session_dir`: Telegram
  collection settings.
- `ai_sweep.key_file`: local provider-key import file.
- `http_proxy`, `https_proxy`: optional proxy settings for collectors.

## Verification

Run the maintained test suite:

```powershell
python -m unittest discover -s tests -v
```

Useful lightweight checks:

```powershell
python -m py_compile db\file_store.py db\migrate.py runtime\registry.py
python -m runtime.run_job --job source_health
python -m runtime.run_job --job quality_gate
```

Database integrity checks should be run against the local live database before
large migrations or exports.

## Public Repository Policy

Commit source code, tests, schemas, example configuration, and maintained tools.
Do not commit local databases, generated evidence stores, reports, copied media,
provider keys, Telegram session files, Obsidian exports, or one-off exploratory
scripts.
