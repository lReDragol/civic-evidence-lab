# Civic Evidence Lab 2.0

## Current implementation (2026-09-20)
Legacy remains authoritative; Reactor is a shadow store until acceptance gates pass.
- [x] Inspect dirty main worktree without resetting or switching branches.
- [x] Consistent SQLite/settings/session/log backup: E:/CivicEvidence/backups/pre-civic-20260920-implementation.
- [x] Atomic DDL plus migration ledger; crash regression tests.
- [x] Fenced MAS attempts, deadline/backoff and atomic artifact acceptance on both schemas (test_civic_core, test_agent_mas).
- [x] Source observation history A -> B -> A and immutable payload reuse (test_civic_core, test_knowledge_spine).
- [x] Transactional outbox/inbox, crash replay and monotonic search heads (test_civic_core, test_civic_runtime).
- [ ] Single-writer process ownership per database; unify continuous scheduling and all four agent groups.
- [x] Frozen authorized Scanner route contract, schema/deadline/budget gates; no future-key auto approval (test_fcm_gateway).
- [ ] Provision reviewed live FCM configuration and verify actual provider capabilities; paid transport remains blocked.
- [x] Telegram contiguous checkpoints, bounded recent preservation, immutable edits and real archived media (test_telegram_checkpoint, test_telegram_evidence_archive).
- [ ] Explicit independently scheduled forward/backfill ranges, deletion observations and Telegram-to-Reactor revision delivery.
- [x] Election campaign/jurisdiction/ballot identity; DEG and overseas separated (test_election_audit).
- [x] Protocol numeric locators, validation and comparable-subset official/observed totals on fixtures (test_election_audit).
- [ ] Integrate real protocol OCR/ASR, numeric review and collector-to-election import.
- [x] Claim stance/modality/attribution; revision-scoped, pair-specific and reviewed provenance-backed relations (test_civic_runtime, test_relation_safety).
- [x] Versioned thread membership with conflict detection (test_civic_runtime).
- [ ] Thread merge/split workflow, agent gap follow-up and three-round clarification orchestration.
- [ ] Official hierarchy capture and national coverage gaps; no invented API identifiers.
- [x] Import 11 supplied screenshots as hashed originals/unverified leads; source access failures become durable tasks (reports/civic-screenshot-import.json).
- [ ] Live TikTok discovery after user access; original video download, OCR/ASR and provenance deduplication.
- [x] Read-only modular workbench, request sequencing, filter before pagination and lazy inspector (test_civic_workbench).
- [ ] Review mutations, full localized workbench, geographic coverage map and claim/event-centered graph navigation.
- [x] Bounded public-host HTTP capture with DNS pinning, redirect checks and WARC (test_civic_capture).
- [ ] Restricted browser capture fallback and historical RED fixtures.
- [ ] Reviewed sanitized public packages; no automatic publication.
- [x] Full unittest checkpoint: 526 passed, one skipped; 40 Telegram tests passed after recent-preservation addition.
- [x] Python/JS syntax, isolated legacy migration and real Qt smoke/screenshots (reports/civic-ui-smoke/manifest.json).
- [ ] Human gold: 300 claims/incidents, 100 protocols, 200 links; >=95% measured precision.
- [ ] Observed 5h/24h runs with restart and same-corpus efficiency benchmark.
- [ ] Operator-approved canonical cutover after all gates, never before.

## Universal collection and controls (follow-up)
- [x] Profile-driven continuous HTTP capture; election domain is configuration, not core ingestion logic.
- [x] Start/report/stop controls in Qt; reports run in the background without stopping collection.
- [x] Detached hidden launcher and a per-DB collection process lock.
- [x] Durable rolling request budgets, source backoff, access blockers and backlog pressure.
- [x] Bounded approved route selection; account/provider denials persist across worker recreation.
- [x] Immutable concurrent report snapshots in JSON/Markdown and periodic automatic reporting.
- [x] Unit coverage for generic profiles, live report generation, stop, request reservation and quota holds.
- [x] Final checkpoints: unittest 550 tests OK (2 opt-in Qt skips); pytest 617 passed, 2 skipped, 183 subtests; 16 focused runtime/service tests after quota-deferral addition. Real Qt smoke ran separately.
- [x] Real 15-minute capture trial: 01:36:13-01:51:13 MSK, 900 seconds; six requests, three captures, two timeouts, one access blocker, zero model calls (reports/civic-collection/collection-6c1a27288d9448a9965af9c0bda917c5.md).
- [x] Final code repeated for 900 seconds without changes: 01:57:49-02:12:49 MSK; five HTTP attempts, three captures, two timeouts; no repeated TikTok access request, no model calls (docs/CIVIC_15_MINUTE_TRIAL.md).
- [ ] Provision the authorized model gateway and repeat the unattended test with real extraction/verification.
- [ ] External crash/reboot supervision, two-day endurance and explicit owned-pool renewal policy.

## Earlier Reactor backlog (historical audit below)

Frozen historical checklist, not the current completion ledger. The checked items
above and docs/CIVIC_EVIDENCE_2_IMPLEMENTATION.md supersede overlapping entries.

## Baseline audit
- [x] Сделать safety snapshot БД, настроек, Telegram-сессий и dirty diff.
- [x] Проверить live schema и фактические распределения Event/Fact/Evidence/Relation/MAS.
- [x] Провести независимые аудиты runtime/data, relations и UI.
- [x] Зафиксировать корневые дефекты в `docs/REACTOR_V2.md`.

## P0: Knowledge spine
- [ ] Добавить migration ledger и versioned additive migration для Reactor v2.
- [ ] Добавить immutable source revisions и transform runs.
- [ ] Добавить projection generations и атомарный current generation switch.
- [ ] Добавить typed fact arguments with explicit source spans.
- [ ] Добавить relation signals отдельно от factual relation assertions.
- [ ] Добавить current/as-of views для событий, фактов и отношений.

## P0: Relation engine v2
- [ ] Строить relations только из explicit fact arguments или typed official adapters.
- [ ] Запретить Cartesian event participant expansion, generic `likely_association` и self/alias edges.
- [ ] Ввести evidence tiers E0-E3, entailment, source independence, temporal and hub penalties.
- [ ] Материализовать promoted assertion и public edge строго 1:1.
- [ ] Сделать hard gate для dangling provenance, unverifiable hard evidence и stale generation.
- [ ] Перенести `same_case/same_bill/co-mention/semantic` только в signal layer.

## P0: Runtime and automation
- [ ] Оставить один orchestration plane для scheduled jobs и pipelines.
- [ ] Добавить real deadline, retry/backoff, CAS leases and stale pipeline recovery.
- [ ] Сделать MAS leases owner-checked and atomic; добавить message lifecycle and artifacts.
- [ ] Подключить knowledge projection and relation v2 after collection/evidence stages.
- [ ] Сделать monitoring fail-visible on its own SQL/schema errors.

## P1: Event/Fact/Evidence
- [ ] Сохранить source revisions instead of overwriting changed external IDs.
- [ ] Заполнять actor/action/object/date/legal basis/polarity and exact evidence locator.
- [ ] Не считать OCR/Telegram screenshot hard evidence без authenticity verdict.
- [ ] Пересобрать events/facts in shadow generation and atomically activate.
- [ ] Покрыть missing roles: issuer, executor, regulator, court, voter, owner.

## P1: UI v2
- [ ] Ввести route/request sequencing and lazy detail API.
- [ ] Исправить search-before-limit, server-side sort/facets/cursor pagination.
- [ ] Перестроить shell: navigation rail, workspace, independent inspector.
- [ ] Сделать Review Ops рабочим inbox с approve/reject/merge/defer/need evidence.
- [ ] Разделить monitoring на Runtime/Sources/Jobs/AI/Errors with trends and drill-down.
- [ ] Заменить relation map на event/fact hubs, clustering and level-of-detail.
- [ ] Удалить CSS patch stack and `!important` cascade after parity verification.
- [ ] Добавить accessibility, keyboard navigation and responsive layouts.

## P1: Storage consolidation
- [ ] Сохранить legacy truth до сверки; новые knowledge/ops/search разнести по SQLite.
- [ ] Маркировать analysis/evidence DB as generated read models with manifest/watermark.
- [ ] Добавить archive/retention policy for raw revisions and large blobs.
- [ ] Добавить exact duplicate and near-duplicate reporting by canonical unit.

## Verification
- [ ] Clean migration on temp DB and additive migration on live DB backup.
- [ ] `python -m py_compile` for all Python files.
- [ ] Full unittest suite.
- [ ] Relation v2 integrity audit on live backup.
- [ ] Runtime/daemon/24x7 smoke with no stale leases.
- [ ] UI screenshot review for every group and section.
- [ ] Before/after quality report with precision sample and graph statistics.
