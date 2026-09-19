# Civic Evidence Lab 2.0: implementation and acceptance

Date: 2026-09-20. Mode: **shadow**, not production cutover.
Current checklist: [todo.md](../todo.md). No commit, push, branch switch or live
legacy database migration was performed. Existing dirty work was retained.

## Safety and observed baseline

- Consistent SQLite/settings/session/log/source snapshot:
  `E:/CivicEvidence/backups/pre-civic-20260920-implementation`.
- Legacy migration verification used a fresh copy at
  `E:/CivicEvidence/verification/legacy-migration-20260920.db`, not the live DB.
- `reports/civic-migration-verification.json`: quick_check=ok, zero FK violations,
  repeat migration is a no-op. Counts unchanged: 179 sources, 27467 raw items,
  26945 content items, 3200 relations, 27892 claims, two MAS tasks.
- Shadow databases: `db/civic-shadow/reactor_v2.db`, `reactor_ops.db`,
  `reactor_search.db`. The old source of truth remains intact.
- Eleven supplied screenshots were archived by SHA-256. Visible captions are
  explicitly unverified manual transcriptions, not verified incidents or votes.
  See `docs/election_seed_leads.json` and `reports/civic-screenshot-import.json`.
- A bounded HTTP capture of the CEC Telegram public page succeeded and preserved
  original HTML/WARC. The CEC website timed out and produced a durable failed
  capture. This is not a complete official campaign crawl. TikTok access remains
  unresolved; no CAPTCHA bypass was attempted.

## Defects addressed

- Migration DDL and its checksum ledger now commit together. A failed statement
  or ledger write rolls back the migration rather than leaving a false ledger.
- MAS schema adapters support both legacy and separated Reactor stores. Every
  lease has an attempt token, owner, expiry and deadline. Stale workers cannot
  finalize accepted results/artifacts. Retry delay and attempt limits are durable.
- Cross-database work uses producer outbox plus idempotent receiver inbox.
  Receiver state and inbox receipt commit together; a crash before sender ACK is
  safe to replay. Search heads reject older observations arriving out of order.
- Source payloads stay immutable; A -> B -> A records three observations without
  losing the return to A. Capture timestamps/WARC container metadata no longer
  cause a new content revision for otherwise unchanged material.
- Alerts reuse stable identity. Monitoring SQL/schema failures are visible errors,
  not misleading empty or zero results.
- Rejected, fabricated, unreviewed or refuting evidence cannot promote supporting
  relations. Evidence must match the specific pair and revision; domain diversity
  alone is not independence. A graph rebuild preserves foreign-owned edges and
  does not remove the active graph before a valid replacement is ready.
- Telegram walks a bounded contiguous oldest-first range rather than advancing
  to MAX(id) after a partial newest-first page. Separately, a bounded recent tail
  is preserved without advancing that coverage checkpoint. Media must exist and
  match its hash. Failed media/SQL leaves the message checkpoint unadvanced.
- Post edits are stored as revision blobs rather than overwriting original raw
  text. Skip reasons are durable. Resolve errors do not poison healthy sessions.
- HTTP capture pins validated public DNS addresses, validates each redirect,
  bounds body/time, archives raw WARC and does not execute remote instructions.
  A real Windows `Connection: close` socket-lifecycle defect was reproduced and
  fixed with a local HTTP-server regression test.

## Delivered modules

| Area | Main files | Scope |
| --- | --- | --- |
| Storage | `db/migration_runner.py`, `db/reactor.py`, `knowledge/revisions.py`, `knowledge/reactor_import.py` | Atomic migrations, three stores, revision history |
| Tasks | `agents/bus.py`, `agents/search.py`, `agents/pipeline_runner.py`, `runtime/transfers.py` | Fencing, durable transfer, safe acceptance |
| Evidence | `collectors/evidence_archive.py`, `collectors/civic_capture.py`, `collectors/telegram_telethon_collector.py` | Originals, quota/reserve, WARC, checkpoints |
| Claims/threads | `knowledge/investigations.py`, `knowledge/relation_engine.py`, `graph/relation_candidates.py`, `knowledge/projection.py` | Stance, locators, versioned membership, guarded graph |
| Elections | `election_audit/` | Campaign/ballot/precinct identity, protocols, incidents, comparable totals |
| Gateway | `integrations/fcm_gateway.py`, `F:/ChatGPT-API-Scanner/civic_gateway/` | Isolated explicit grants, infer/health/metrics, no admin router |
| CLI | `runtime/civic.py`, `runtime/import_leads.py`, `runtime/civic_quality.py`, `runtime/safety_snapshot.py`, `runtime/verify_civic.py` | Shadow execution/import/acceptance/safety |
| UI | `ui/query_service.py`, `ui/web_bridge.py`, `ui/civic_window.py`, `ui_web/reactor_v2.js`, `ui_web/reactor_v2.css`, shell wiring | Seven read-only screens, bounded queries, inspector |
| Tests | `tests/test_civic_*`, `test_election_audit`, `test_fcm_gateway`, `test_migration_atomicity`, `test_relation_safety`, `test_telegram_checkpoint`, `test_telegram_evidence_archive` | Regression and isolated end-to-end fixtures |

FCM concrete transport currently permits only explicitly declared free routes;
paid routes remain fail-closed. The generic contract has cost ceilings but does
not prove prices. No actual provider call or secret provisioning was performed.
Deployment, restart journal and constraints: [FCM_GATEWAY.md](FCM_GATEWAY.md).

## Additive schema

- Legacy `0002_source_observations.sql`; MAS columns also added to the canonical
  bootstrap/additive column map. Existing legacy runtime requires its schema
  migration before it can use the new fenced bus. Do not start it unmigrated.
- Knowledge `0002_observations_transfer`, `0003_election`,
  `0004_investigations`, `0006_source_captures`.
- Ops `0002_task_fencing`, `0003_transfer`.
- Search `0002_transfer`, `0003_heads`.
- Applied SQL files are immutable/checksummed. Number 0005 is unused, not a
  missing prerequisite. Further changes require new additive migrations.

## Run and inspect

Run from `F:/новости`, with the tested Python 3.13 environment. The code uses
recent pathlib/hashlib APIs; older Python versions have not been certified.
`warcio` and `jsonschema` were checked/installed separately. The entire heavy
legacy requirements set was not reinstalled into the working environment.

```powershell
python -m runtime.civic --db-dir db\civic-shadow --init
python -m runtime.civic --db-dir db\civic-shadow --summary
python -m ui.civic_window --db-dir db\civic-shadow
python -m runtime.civic --db-dir db\civic-shadow --reconcile-search
python -m runtime.civic_quality --db-dir db\civic-shadow --report reports\civic-quality.json
```

Quality exits with code 2 while gold review is missing. This is an expected
release block, not an instruction to fabricate labels or bypass it.
The example `config/civic_profile.example.json` is documentation, not an active
settings merge. The viewer never starts collectors, providers or a daemon.

Bounded capture and explicit local file import:

```powershell
python -m runtime.civic --db-dir db\civic-shadow --capture-url https://t.me/s/cikrossii --allow-host t.me
python -m runtime.civic --db-dir db\civic-shadow --import-file <FILE> --source-url <ORIGINAL_URL>
python -m runtime.civic --db-dir db\civic-shadow --worker-once --gateway-config civic-gateway.local.json
```

The worker requires a reviewed client config and a matching live gateway grant;
it never discovers or starts Scanner. It extracts only from available source
text and validates every returned quote before inserting unreviewed claims.
Images/video without OCR/ASR are not substituted by filenames or metadata.

## Verification artifacts

- `reports/civic-unittest-release.log`: 526 tests, OK, one skipped.
- After recent Telegram preservation: 40 targeted collector/archive tests, OK.
- `reports/civic-unittest-acceptance.log`: final repeat covering that addition.
- `reports/civic-fcm-final.log`: 61 gateway tests with 90 table-driven subtests.
- `reports/civic-ui-smoke/manifest.json`: 13 actual Qt fixture screenshots,
  no JS errors; GPU GLES fallback warnings are retained in the log.
- `reports/civic-ui-live-shadow.png`: actual read-only shadow-store desktop view,
  distinct from synthetic UI fixture screenshots.
- `reports/civic-quality.json`: publication blocked, gold and numeric review
  outstanding. No positive finding was inferred from screenshots alone.

```powershell
python -m unittest discover -s tests -v
$files = @(rg --files -g '*.py')
python -m py_compile @files
node --check ui_web\app.js
node --check ui_web\reactor_v2.js
python -m ui.civic_window --db-dir db\civic-shadow --smoke-screenshot reports\civic-ui-live-shadow.png
```

## Not yet accepted / next implementation gates

### Universal collection follow-up

`runtime/civic_service.py` now supplies continuous, profile-driven collection.
`config/civic_collection.json` is the active election profile;
`config/civic_collection.generic.example.json` is the domain-neutral template.
Core ingestion, relevance selection, hashing, queues, reporting and budgets do
not contain election-specific tests. `domain_modules` controls optional screens.

Open `start_civic.cmd` or run `python main.py --civic`. The workbench has
**Start collection**, **Get report**, **Stop collection** and status controls.
The report worker uses its own read connections, and closing the UI does not
stop the detached collector. Without an explicit `--db-dir`, the UI follows the
chosen profile's database directory, not a hardcoded election database.

```powershell
python -m runtime.civic_service --profile config\civic_collection.json --start
python -m runtime.civic_service --profile config\civic_collection.json --status
python -m runtime.civic_service --profile config\civic_collection.json --report
python -m runtime.civic_service --profile config\civic_collection.json --stop
python -m runtime.civic_service --profile config\civic_collection.json --run --duration-seconds 900
```

Additional ops migrations: `0004_collection_service`, `0005_model_holds`.
An OS lock prevents duplicate collection instances for a DB directory. Requests
are reserved against a durable rolling 24-hour ceiling before dispatch. Source
polling/backoff survives restarts; inaccessible pages wait rather than looping.
Task backlog pressure pauses new requests. Relevance is configured by keywords;
nonmatching captures remain archived but are not dispatched to extraction.
The system never recursively follows URLs supplied by a page or model.

Reports include run start/end/heartbeat, source states, request reservations,
captures/errors, stored deltas, task states and model availability. Exported
reports use unique filenames so concurrent readers cannot overwrite newer/final
snapshots. A report may be requested while collection continues.

`integrations/civic_routes.py` supplies single-attempt selection within explicit
reviewed free-route grants. Runtime reuses the helper and persists account/provider
denials in `civic_model_holds`; refreshing configuration or restarting cannot clear
these holds. Daily request ceilings also survive restart. No matching provisioned
gateway was found. **The active profile therefore says `gateway_config: null`;
`not_configured` is visible rather than presenting capture success as AI success.**

The requested 15-minute capture trial is recorded separately under
`reports/civic-collection/` and `reports/civic-autonomous-15m.log`. It cannot certify
live models, two-day reliability, automatic approval of newly found credentials,
or complete coverage of the Internet. A crashed process/reboot still needs an
external approved supervisor; OS autostart was not silently installed.

Trial outcome and exact timestamps: [CIVIC_15_MINUTE_TRIAL.md](CIVIC_15_MINUTE_TRIAL.md).
Final regression checkpoints: `reports/civic-autonomy-full-suite.log` contains
550 unittest tests OK (two opt-in Qt skips); `reports/civic-autonomy-pytest.log`
contains 617 passed, two skipped and 183 subtests passed. The subsequently added
provider-quota deferral regression and service tests passed all 16 focused checks
in `reports/civic-final-runtime-tests.log`. Actual Qt controls were tested
separately and the live window was captured in `reports/civic-controls-live.png`.

### Remaining gates

This is a working, tested foundation and bounded shadow flow, **not completion of
the full national 24/7 system**. The following are still required:

1. Provision a reviewed live FCM route; verify actual capabilities, cancellation,
   account quotas and durable request-level usage across restarts. Do not renew
   snapshots simply to evade exhausted quotas.
2. Enforce a single writer process per store and replace remaining legacy bulk
   jobs with continuous revision-triggered queues for all four agent groups.
3. Add full independently scheduled Telegram forward/backfill ranges, deletion
   observations and delivery of Telegram edits into Reactor; bounded recent
   preservation is not proof of historical completeness.
4. Discover actual official campaign identifiers and published hierarchy. Add
   national gap accounting, real protocol OCR/ASR, field-level review and live
   official/observed comparisons. Fixtures do not constitute a recount.
5. Complete thread merge/split, corroboration/refutation loops, origin clustering,
   media fingerprints and three-round gap resolution.
6. Add bounded browser fallback, human-mediated TikTok access, historical RED
   fixtures and safe public package/redaction/release workflow.
7. Extend read-only UI to reviewed mutations, campaign coverage maps, useful
   claim/event graph navigation and full live session/model telemetry. Finish
   localization and visual density checks on real populated datasets.
8. Obtain real human gold (300 claims/incidents, 100 protocols, 200 links), report
   measured precision with uncertainty, and verify every included numeric field.
9. Observe shadow 5h and 24h runs with restart tests, then benchmark the same corpus
   and budget. Neither run has occurred; no fabricated report or efficiency gain
   is supplied. Only then consider canonical cutover.
