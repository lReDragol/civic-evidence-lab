import json
import logging
import os
import sqlite3
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
SETTINGS_PATH = CONFIG_DIR / "settings.json"
SECRETS_PATH = CONFIG_DIR / "secrets.json"
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"

ADDITIVE_COLUMNS = {
    "content_items": {
        "classification_v3_processed": "INTEGER DEFAULT 0",
    },
    "content_derivations": {
        "campaign_id": "INTEGER",
        "work_item_id": "INTEGER",
        "event_context_json": "TEXT",
        "fact_context_json": "TEXT",
        "temporal_window_json": "TEXT",
        "is_current": "INTEGER DEFAULT 0",
    },
    "content_tag_votes": {
        "signal_layer": "TEXT DEFAULT 'raw'",
        "abstain_reason": "TEXT",
    },
    "claims": {
        "canonical_text": "TEXT",
        "canonical_hash": "TEXT",
        "claim_cluster_id": "INTEGER",
    },
    "evidence_links": {
        "evidence_class": "TEXT DEFAULT 'support'",
    },
    "content_tags": {
        "namespace": "TEXT",
        "normalized_tag": "TEXT",
        "confidence_calibrated": "REAL",
        "decision_source": "TEXT",
    },
    "relation_candidates": {
        "seed_kind": "TEXT",
        "structural_score": "REAL DEFAULT 0",
        "semantic_score": "REAL DEFAULT 0",
        "support_score": "REAL DEFAULT 0",
        "calibrated_score": "REAL DEFAULT 0",
        "support_claim_cluster_count": "INTEGER DEFAULT 0",
        "support_hard_evidence_count": "INTEGER DEFAULT 0",
        "candidate_state": "TEXT DEFAULT 'pending'",
        "promotion_block_reason": "TEXT",
        "evidence_mix_json": "TEXT",
        "explain_path_json": "TEXT",
        "valid_from": "TEXT",
        "valid_to": "TEXT",
        "observed_at": "TEXT",
        "recorded_at": "TEXT",
        "superseded_at": "TEXT",
    },
    "relation_support": {
        "support_class": "TEXT DEFAULT 'seed'",
        "event_id": "INTEGER",
        "fact_id": "INTEGER",
    },
    "relation_features": {
        "entity_quality_score": "REAL DEFAULT 0",
        "dedupe_support_score": "REAL DEFAULT 0",
        "real_host_diversity_score": "REAL DEFAULT 0",
        "bridge_diversity_score": "REAL DEFAULT 0",
        "event_consistency_score": "REAL DEFAULT 0",
        "fact_support_score": "REAL DEFAULT 0",
        "official_bridge_score": "REAL DEFAULT 0",
        "telegram_penalty": "REAL DEFAULT 0",
    },
    "source_sync_state": {
        "quality_state": "TEXT DEFAULT 'unknown'",
        "quality_issue": "TEXT",
        "failure_class": "TEXT",
    },
    "content_clusters": {
        "representative_score": "REAL DEFAULT 0",
        "suppression_reason": "TEXT",
    },
    "official_positions": {
        "valid_from": "TEXT",
        "valid_to": "TEXT",
        "observed_at": "TEXT",
    },
    "company_affiliations": {
        "valid_from": "TEXT",
        "valid_to": "TEXT",
        "observed_at": "TEXT",
        "recorded_at": "TEXT",
        "superseded_at": "TEXT",
    },
    "restriction_events": {
        "valid_from": "TEXT",
        "valid_to": "TEXT",
        "observed_at": "TEXT",
        "recorded_at": "TEXT",
        "superseded_at": "TEXT",
    },
    "entity_relations": {
        "valid_from": "TEXT",
        "valid_to": "TEXT",
        "observed_at": "TEXT",
        "recorded_at": "TEXT",
        "superseded_at": "TEXT",
    },
    "ai_work_items": {
        "campaign_id": "INTEGER",
        "prompt_version": "TEXT NOT NULL DEFAULT 'ai-sweep-v1'",
        "input_hash": "TEXT",
        "sample_bucket": "TEXT",
    },
    "ai_task_attempts": {
        "failure_kind": "TEXT",
    },
}

ADDITIVE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS content_tag_votes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL,
    voter_name      TEXT NOT NULL,
    tag_name        TEXT NOT NULL,
    namespace       TEXT,
    normalized_tag  TEXT,
    vote_value      TEXT NOT NULL,
    signal_layer    TEXT DEFAULT 'raw',
    abstain_reason  TEXT,
    confidence_raw  REAL DEFAULT 0,
    evidence_text   TEXT,
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_content_tag_votes_item ON content_tag_votes(content_item_id);
CREATE INDEX IF NOT EXISTS idx_content_tag_votes_tag ON content_tag_votes(normalized_tag);
CREATE INDEX IF NOT EXISTS idx_content_tag_votes_vote ON content_tag_votes(vote_value);

CREATE TABLE IF NOT EXISTS llm_keys (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    provider        TEXT NOT NULL,
    api_key         TEXT NOT NULL,
    key_hash        TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL DEFAULT 'active',
    failure_count   INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    last_failure_kind TEXT,
    last_used_at    TEXT,
    metadata_json   TEXT,
    removed_at      TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_llm_keys_provider ON llm_keys(provider);
CREATE INDEX IF NOT EXISTS idx_llm_keys_status ON llm_keys(status);

CREATE TABLE IF NOT EXISTS llm_key_failures (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    key_id          INTEGER NOT NULL,
    provider        TEXT NOT NULL,
    failure_kind    TEXT NOT NULL,
    failure_code    TEXT,
    error_text      TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (key_id) REFERENCES llm_keys(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_llm_key_failures_key ON llm_key_failures(key_id);
CREATE INDEX IF NOT EXISTS idx_llm_key_failures_provider ON llm_key_failures(provider);

CREATE TABLE IF NOT EXISTS llm_provider_models (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    provider        TEXT NOT NULL,
    model_name      TEXT NOT NULL,
    capability_tier INTEGER NOT NULL DEFAULT 1,
    stage_roles_json TEXT,
    supports_web_search INTEGER NOT NULL DEFAULT 0,
    supports_reasoning INTEGER NOT NULL DEFAULT 0,
    supports_background INTEGER NOT NULL DEFAULT 0,
    is_active       INTEGER NOT NULL DEFAULT 1,
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(provider, model_name)
);
CREATE INDEX IF NOT EXISTS idx_llm_provider_models_provider ON llm_provider_models(provider);
CREATE INDEX IF NOT EXISTS idx_llm_provider_models_web ON llm_provider_models(supports_web_search, is_active);

CREATE TABLE IF NOT EXISTS llm_provider_health (
    provider        TEXT PRIMARY KEY,
    status          TEXT NOT NULL DEFAULT 'unknown',
    active_key_count INTEGER NOT NULL DEFAULT 0,
    last_checked_at TEXT,
    last_success_at TEXT,
    metadata_json   TEXT,
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ai_sweep_campaigns (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_key    TEXT NOT NULL UNIQUE,
    campaign_seed   TEXT NOT NULL,
    mode            TEXT NOT NULL DEFAULT 'pilot',
    provider_mode   TEXT NOT NULL DEFAULT 'conservative',
    sample_size     INTEGER NOT NULL DEFAULT 0,
    selection_json  TEXT,
    prompt_versions_json TEXT,
    status          TEXT NOT NULL DEFAULT 'planned',
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    last_run_at     TEXT,
    completed_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_ai_sweep_campaigns_status ON ai_sweep_campaigns(status);

CREATE TABLE IF NOT EXISTS ai_work_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_run_id INTEGER,
    campaign_id     INTEGER,
    unit_kind       TEXT NOT NULL,
    unit_key        TEXT NOT NULL,
    stage           TEXT NOT NULL,
    unit_ref_id     INTEGER,
    canonical_content_id INTEGER,
    event_id        INTEGER,
    review_task_id  INTEGER,
    prompt_version  TEXT NOT NULL DEFAULT 'ai-sweep-v1',
    input_hash      TEXT,
    sample_bucket   TEXT,
    priority        INTEGER NOT NULL DEFAULT 50,
    status          TEXT NOT NULL DEFAULT 'pending',
    lease_owner     TEXT,
    lease_expires_at TEXT,
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    provider        TEXT,
    model_name      TEXT,
    payload_json    TEXT,
    result_json     TEXT,
    error_text      TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    completed_at    TEXT,
    FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id) ON DELETE SET NULL,
    FOREIGN KEY (campaign_id) REFERENCES ai_sweep_campaigns(id) ON DELETE SET NULL,
    UNIQUE(unit_kind, unit_key, stage)
);
CREATE INDEX IF NOT EXISTS idx_ai_work_items_status ON ai_work_items(status);
CREATE INDEX IF NOT EXISTS idx_ai_work_items_stage ON ai_work_items(stage);
CREATE INDEX IF NOT EXISTS idx_ai_work_items_pipeline ON ai_work_items(pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_ai_work_items_campaign ON ai_work_items(campaign_id, status);

CREATE TABLE IF NOT EXISTS ai_task_attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    work_item_id    INTEGER NOT NULL,
    provider        TEXT,
    model_name      TEXT,
    llm_key_id      INTEGER,
    status          TEXT NOT NULL,
    failure_kind    TEXT,
    error_text      TEXT,
    output_json     TEXT,
    started_at      TEXT DEFAULT (datetime('now')),
    finished_at     TEXT,
    FOREIGN KEY (work_item_id) REFERENCES ai_work_items(id) ON DELETE CASCADE,
    FOREIGN KEY (llm_key_id) REFERENCES llm_keys(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_ai_task_attempts_work_item ON ai_task_attempts(work_item_id);
CREATE INDEX IF NOT EXISTS idx_ai_task_attempts_key ON ai_task_attempts(llm_key_id);

CREATE TABLE IF NOT EXISTS event_candidates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    unit_kind       TEXT NOT NULL,
    unit_key        TEXT NOT NULL,
    content_item_id INTEGER,
    content_cluster_id INTEGER,
    suggested_event_id INTEGER,
    candidate_state TEXT NOT NULL DEFAULT 'suggested',
    confidence      REAL DEFAULT 0,
    suggestion_json TEXT,
    model_provider  TEXT,
    model_name      TEXT,
    prompt_version  TEXT,
    status          TEXT NOT NULL DEFAULT 'open',
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL,
    FOREIGN KEY (content_cluster_id) REFERENCES content_clusters(id) ON DELETE SET NULL,
    FOREIGN KEY (suggested_event_id) REFERENCES events(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_event_candidates_state ON event_candidates(candidate_state, status);
CREATE INDEX IF NOT EXISTS idx_event_candidates_event ON event_candidates(suggested_event_id);

CREATE TABLE IF NOT EXISTS event_merge_reviews (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_a_id      INTEGER NOT NULL,
    event_b_id      INTEGER NOT NULL,
    suggested_action TEXT NOT NULL DEFAULT 'merge',
    confidence      REAL DEFAULT 0,
    machine_reason  TEXT,
    payload_json    TEXT,
    status          TEXT NOT NULL DEFAULT 'open',
    reviewed_at     TEXT,
    reviewer        TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (event_a_id) REFERENCES events(id) ON DELETE CASCADE,
    FOREIGN KEY (event_b_id) REFERENCES events(id) ON DELETE CASCADE,
    UNIQUE(event_a_id, event_b_id)
);
CREATE INDEX IF NOT EXISTS idx_event_merge_reviews_status ON event_merge_reviews(status);

CREATE TABLE IF NOT EXISTS semantic_neighbors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_kind     TEXT NOT NULL,
    source_id       INTEGER NOT NULL,
    neighbor_kind   TEXT NOT NULL,
    neighbor_id     INTEGER NOT NULL,
    score           REAL DEFAULT 0,
    method          TEXT DEFAULT 'tfidf',
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(source_kind, source_id, neighbor_kind, neighbor_id, method)
);
CREATE INDEX IF NOT EXISTS idx_semantic_neighbors_source ON semantic_neighbors(source_kind, source_id);
CREATE INDEX IF NOT EXISTS idx_semantic_neighbors_neighbor ON semantic_neighbors(neighbor_kind, neighbor_id);

CREATE TABLE IF NOT EXISTS claim_clusters (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster_key     TEXT NOT NULL UNIQUE,
    canonical_text  TEXT NOT NULL,
    claim_type      TEXT,
    method          TEXT DEFAULT 'canonical',
    status          TEXT DEFAULT 'active',
    support_count   INTEGER DEFAULT 0,
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_claim_clusters_type ON claim_clusters(claim_type);
CREATE INDEX IF NOT EXISTS idx_claim_clusters_status ON claim_clusters(status);

CREATE TABLE IF NOT EXISTS claim_occurrences (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_cluster_id INTEGER NOT NULL,
    claim_id        INTEGER,
    content_item_id INTEGER,
    occurrence_text TEXT NOT NULL,
    occurrence_hash TEXT NOT NULL,
    source_kind     TEXT DEFAULT 'claim',
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (claim_cluster_id) REFERENCES claim_clusters(id) ON DELETE CASCADE,
    FOREIGN KEY (claim_id) REFERENCES claims(id) ON DELETE SET NULL,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL,
    UNIQUE(claim_cluster_id, content_item_id, occurrence_hash)
);
CREATE INDEX IF NOT EXISTS idx_claim_occurrences_cluster ON claim_occurrences(claim_cluster_id);
CREATE INDEX IF NOT EXISTS idx_claim_occurrences_claim ON claim_occurrences(claim_id);

CREATE TABLE IF NOT EXISTS relation_features (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id    INTEGER NOT NULL UNIQUE,
    structural_score REAL DEFAULT 0,
    content_support_score REAL DEFAULT 0,
    source_diversity_score REAL DEFAULT 0,
    semantic_support_score REAL DEFAULT 0,
    shared_claim_cluster_score REAL DEFAULT 0,
    evidence_quality_score REAL DEFAULT 0,
    entity_quality_score REAL DEFAULT 0,
    dedupe_support_score REAL DEFAULT 0,
    real_host_diversity_score REAL DEFAULT 0,
    bridge_diversity_score REAL DEFAULT 0,
    event_consistency_score REAL DEFAULT 0,
    fact_support_score REAL DEFAULT 0,
    official_bridge_score REAL DEFAULT 0,
    telegram_penalty REAL DEFAULT 0,
    temporal_score REAL DEFAULT 0,
    role_compatibility_score REAL DEFAULT 0,
    calibrated_score REAL DEFAULT 0,
    explain_path_json TEXT,
    metadata_json   TEXT,
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (candidate_id) REFERENCES relation_candidates(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_relation_features_candidate ON relation_features(candidate_id);

CREATE TABLE IF NOT EXISTS content_clusters (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster_key     TEXT NOT NULL UNIQUE,
    cluster_type    TEXT NOT NULL DEFAULT 'document_dedupe',
    canonical_content_id INTEGER,
    canonical_title TEXT,
    method          TEXT DEFAULT 'title_signature',
    similarity_score REAL DEFAULT 0,
    representative_score REAL DEFAULT 0,
    item_count      INTEGER DEFAULT 0,
    first_seen_at   TEXT,
    last_seen_at    TEXT,
    status          TEXT DEFAULT 'active',
    suppression_reason TEXT,
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (canonical_content_id) REFERENCES content_items(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_content_clusters_type ON content_clusters(cluster_type);
CREATE INDEX IF NOT EXISTS idx_content_clusters_status ON content_clusters(status);

CREATE TABLE IF NOT EXISTS content_cluster_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster_id      INTEGER NOT NULL,
    content_item_id INTEGER NOT NULL,
    similarity_score REAL DEFAULT 0,
    reason          TEXT,
    is_canonical    INTEGER DEFAULT 0,
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (cluster_id) REFERENCES content_clusters(id) ON DELETE CASCADE,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE,
    UNIQUE(cluster_id, content_item_id)
);
CREATE INDEX IF NOT EXISTS idx_content_cluster_items_cluster ON content_cluster_items(cluster_id);
CREATE INDEX IF NOT EXISTS idx_content_cluster_items_content ON content_cluster_items(content_item_id);

CREATE TABLE IF NOT EXISTS entity_merge_candidates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_a_id     INTEGER NOT NULL,
    entity_b_id     INTEGER NOT NULL,
    candidate_type  TEXT NOT NULL DEFAULT 'entity_merge',
    score           REAL DEFAULT 0,
    support_count   INTEGER DEFAULT 0,
    suggested_action TEXT DEFAULT 'merge',
    reason          TEXT,
    status          TEXT DEFAULT 'open',
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (entity_a_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_b_id) REFERENCES entities(id) ON DELETE CASCADE,
    UNIQUE(entity_a_id, entity_b_id, candidate_type)
);
CREATE INDEX IF NOT EXISTS idx_entity_merge_candidates_status ON entity_merge_candidates(status);
CREATE INDEX IF NOT EXISTS idx_entity_merge_candidates_pair ON entity_merge_candidates(entity_a_id, entity_b_id);

CREATE TABLE IF NOT EXISTS person_disclosures (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    disclosure_year INTEGER NOT NULL,
    source_content_id INTEGER,
    source_url      TEXT,
    source_type     TEXT,
    income_amount   REAL,
    income_currency TEXT DEFAULT 'RUB',
    raw_income_text TEXT,
    spouse_income_text TEXT,
    source_scope    TEXT,
    evidence_class  TEXT DEFAULT 'support',
    document_attachment_id INTEGER,
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (source_content_id) REFERENCES content_items(id) ON DELETE SET NULL,
    FOREIGN KEY (document_attachment_id) REFERENCES attachments(id) ON DELETE SET NULL,
    UNIQUE(entity_id, disclosure_year, source_url)
);
CREATE INDEX IF NOT EXISTS idx_person_disclosures_entity ON person_disclosures(entity_id);
CREATE INDEX IF NOT EXISTS idx_person_disclosures_year ON person_disclosures(disclosure_year);

CREATE TABLE IF NOT EXISTS declared_assets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    disclosure_id   INTEGER NOT NULL,
    entity_id       INTEGER,
    owner_role      TEXT DEFAULT 'self',
    asset_type      TEXT NOT NULL,
    asset_name      TEXT,
    asset_value_text TEXT,
    ownership_type  TEXT,
    area_text       TEXT,
    area_value      REAL,
    country         TEXT,
    usage_type      TEXT,
    source_url      TEXT,
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (disclosure_id) REFERENCES person_disclosures(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_declared_assets_disclosure ON declared_assets(disclosure_id);
CREATE INDEX IF NOT EXISTS idx_declared_assets_entity ON declared_assets(entity_id);
CREATE INDEX IF NOT EXISTS idx_declared_assets_type ON declared_assets(asset_type);

CREATE TABLE IF NOT EXISTS company_affiliations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    company_entity_id INTEGER,
    company_name    TEXT NOT NULL,
    role_type       TEXT NOT NULL,
    role_title      TEXT,
    period_start    TEXT,
    period_end      TEXT,
    source_content_id INTEGER,
    source_url      TEXT,
    evidence_class  TEXT DEFAULT 'support',
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (company_entity_id) REFERENCES entities(id) ON DELETE SET NULL,
    FOREIGN KEY (source_content_id) REFERENCES content_items(id) ON DELETE SET NULL,
    UNIQUE(entity_id, company_name, role_type, source_url)
);
CREATE INDEX IF NOT EXISTS idx_company_affiliations_entity ON company_affiliations(entity_id);
CREATE INDEX IF NOT EXISTS idx_company_affiliations_company ON company_affiliations(company_entity_id);
CREATE INDEX IF NOT EXISTS idx_company_affiliations_role ON company_affiliations(role_type);

CREATE TABLE IF NOT EXISTS compensation_facts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    employer_entity_id INTEGER,
    compensation_year INTEGER NOT NULL,
    amount          REAL,
    amount_text     TEXT,
    currency        TEXT DEFAULT 'RUB',
    role_title      TEXT,
    fact_type       TEXT DEFAULT 'income',
    source_content_id INTEGER,
    source_url      TEXT,
    evidence_class  TEXT DEFAULT 'support',
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (employer_entity_id) REFERENCES entities(id) ON DELETE SET NULL,
    FOREIGN KEY (source_content_id) REFERENCES content_items(id) ON DELETE SET NULL,
    UNIQUE(entity_id, compensation_year, fact_type, source_url)
);
CREATE INDEX IF NOT EXISTS idx_compensation_facts_entity ON compensation_facts(entity_id);
CREATE INDEX IF NOT EXISTS idx_compensation_facts_year ON compensation_facts(compensation_year);

CREATE TABLE IF NOT EXISTS restriction_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    issuer_entity_id INTEGER,
    target_entity_id INTEGER,
    target_name      TEXT,
    region          TEXT,
    restriction_type TEXT NOT NULL,
    right_category  TEXT,
    legal_basis     TEXT,
    stated_justification TEXT,
    event_date      TEXT,
    source_content_id INTEGER,
    source_url      TEXT,
    evidence_class  TEXT DEFAULT 'support',
    severity        TEXT DEFAULT 'moderate',
    status          TEXT DEFAULT 'open',
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (issuer_entity_id) REFERENCES entities(id) ON DELETE SET NULL,
    FOREIGN KEY (target_entity_id) REFERENCES entities(id) ON DELETE SET NULL,
    FOREIGN KEY (source_content_id) REFERENCES content_items(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_restriction_events_issuer ON restriction_events(issuer_entity_id);
CREATE INDEX IF NOT EXISTS idx_restriction_events_target ON restriction_events(target_entity_id);
CREATE INDEX IF NOT EXISTS idx_restriction_events_type ON restriction_events(restriction_type);
CREATE INDEX IF NOT EXISTS idx_restriction_events_category ON restriction_events(right_category);

CREATE TABLE IF NOT EXISTS entity_media (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    attachment_id   INTEGER NOT NULL,
    media_kind      TEXT NOT NULL DEFAULT 'photo',
    source_url      TEXT,
    is_primary      INTEGER DEFAULT 0,
    caption         TEXT,
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (attachment_id) REFERENCES attachments(id) ON DELETE CASCADE,
    UNIQUE(entity_id, attachment_id, media_kind)
);
CREATE INDEX IF NOT EXISTS idx_entity_media_entity ON entity_media(entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_media_kind ON entity_media(media_kind);

CREATE TABLE IF NOT EXISTS review_tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_key        TEXT NOT NULL UNIQUE,
    queue_key       TEXT NOT NULL,
    subject_type    TEXT NOT NULL,
    subject_id      INTEGER,
    related_id      INTEGER,
    candidate_payload TEXT,
    suggested_action TEXT NOT NULL,
    confidence      REAL DEFAULT 0,
    machine_reason  TEXT,
    source_links_json TEXT,
    status          TEXT DEFAULT 'open',
    review_pack_id  TEXT,
    reviewer        TEXT,
    reviewed_at     TEXT,
    resolution_notes TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_review_tasks_queue ON review_tasks(queue_key);
CREATE INDEX IF NOT EXISTS idx_review_tasks_status ON review_tasks(status);
CREATE INDEX IF NOT EXISTS idx_review_tasks_pack ON review_tasks(review_pack_id);

CREATE TABLE IF NOT EXISTS source_fixtures (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_key      TEXT NOT NULL,
    fixture_kind    TEXT NOT NULL,
    origin_url      TEXT,
    archive_url     TEXT,
    local_path      TEXT NOT NULL,
    checksum        TEXT,
    captured_at     TEXT DEFAULT (datetime('now')),
    is_active       INTEGER DEFAULT 1,
    metadata_json   TEXT
);
CREATE INDEX IF NOT EXISTS idx_source_fixtures_key ON source_fixtures(source_key);
CREATE INDEX IF NOT EXISTS idx_source_fixtures_active ON source_fixtures(is_active);

CREATE TABLE IF NOT EXISTS content_derivations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL,
    campaign_id     INTEGER,
    work_item_id    INTEGER,
    derivation_type TEXT NOT NULL,
    model_provider  TEXT NOT NULL DEFAULT 'deterministic',
    model_name      TEXT NOT NULL DEFAULT 'event-pipeline-v1',
    prompt_version  TEXT NOT NULL DEFAULT 'event-pipeline-v1',
    input_hash      TEXT NOT NULL,
    output_text     TEXT,
    output_json     TEXT,
    event_context_json TEXT,
    fact_context_json TEXT,
    temporal_window_json TEXT,
    confidence      REAL DEFAULT 0,
    status          TEXT DEFAULT 'ready',
    is_current      INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE,
    UNIQUE(content_item_id, derivation_type, model_provider, model_name, prompt_version, input_hash)
);
CREATE INDEX IF NOT EXISTS idx_content_derivations_item ON content_derivations(content_item_id);
CREATE INDEX IF NOT EXISTS idx_content_derivations_type ON content_derivations(derivation_type);

CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_title TEXT NOT NULL,
    event_type      TEXT,
    summary_short   TEXT,
    summary_long    TEXT,
    status          TEXT DEFAULT 'active',
    event_date_start TEXT,
    event_date_end  TEXT,
    first_observed_at TEXT,
    last_observed_at TEXT,
    importance_score REAL DEFAULT 0,
    confidence      REAL DEFAULT 0,
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
CREATE INDEX IF NOT EXISTS idx_events_date_start ON events(event_date_start);

CREATE TABLE IF NOT EXISTS event_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        INTEGER NOT NULL,
    content_item_id INTEGER,
    content_cluster_id INTEGER,
    item_role       TEXT NOT NULL DEFAULT 'origin',
    source_strength TEXT DEFAULT 'support',
    added_at        TEXT DEFAULT (datetime('now')),
    metadata_json   TEXT,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE,
    FOREIGN KEY (content_cluster_id) REFERENCES content_clusters(id) ON DELETE SET NULL,
    UNIQUE(event_id, content_item_id, item_role)
);
CREATE INDEX IF NOT EXISTS idx_event_items_event ON event_items(event_id);
CREATE INDEX IF NOT EXISTS idx_event_items_content ON event_items(content_item_id);

CREATE TABLE IF NOT EXISTS event_entities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        INTEGER NOT NULL,
    entity_id       INTEGER NOT NULL,
    role            TEXT NOT NULL,
    confidence      REAL DEFAULT 0,
    valid_from      TEXT,
    valid_to        TEXT,
    observed_at     TEXT,
    metadata_json   TEXT,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    UNIQUE(event_id, entity_id, role)
);
CREATE INDEX IF NOT EXISTS idx_event_entities_event ON event_entities(event_id);
CREATE INDEX IF NOT EXISTS idx_event_entities_entity ON event_entities(entity_id);
CREATE INDEX IF NOT EXISTS idx_event_entities_role ON event_entities(role);

CREATE TABLE IF NOT EXISTS event_timeline (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        INTEGER NOT NULL,
    timeline_date   TEXT,
    title           TEXT NOT NULL,
    description     TEXT,
    content_item_id INTEGER,
    document_content_id INTEGER,
    sort_order      INTEGER DEFAULT 0,
    metadata_json   TEXT,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL,
    FOREIGN KEY (document_content_id) REFERENCES content_items(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_event_timeline_event ON event_timeline(event_id);
CREATE INDEX IF NOT EXISTS idx_event_timeline_date ON event_timeline(timeline_date);

CREATE TABLE IF NOT EXISTS event_facts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        INTEGER NOT NULL,
    claim_id        INTEGER,
    fact_type       TEXT NOT NULL,
    canonical_text  TEXT NOT NULL,
    polarity        TEXT DEFAULT 'neutral',
    valid_from      TEXT,
    valid_to        TEXT,
    observed_at     TEXT,
    recorded_at     TEXT DEFAULT (datetime('now')),
    superseded_at   TEXT,
    confidence      REAL DEFAULT 0,
    metadata_json   TEXT,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
    FOREIGN KEY (claim_id) REFERENCES claims(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_event_facts_event ON event_facts(event_id);
CREATE INDEX IF NOT EXISTS idx_event_facts_type ON event_facts(fact_type);

CREATE TABLE IF NOT EXISTS fact_evidence (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fact_id         INTEGER NOT NULL,
    content_item_id INTEGER,
    document_content_id INTEGER,
    evidence_type   TEXT,
    evidence_class  TEXT DEFAULT 'support',
    source_strength TEXT DEFAULT 'support',
    added_at        TEXT DEFAULT (datetime('now')),
    metadata_json   TEXT,
    FOREIGN KEY (fact_id) REFERENCES event_facts(id) ON DELETE CASCADE,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL,
    FOREIGN KEY (document_content_id) REFERENCES content_items(id) ON DELETE SET NULL,
    UNIQUE(fact_id, content_item_id, document_content_id, evidence_type)
);
CREATE INDEX IF NOT EXISTS idx_fact_evidence_fact ON fact_evidence(fact_id);
CREATE INDEX IF NOT EXISTS idx_fact_evidence_content ON fact_evidence(content_item_id);
"""


def load_settings() -> dict:
    settings = {}
    if SETTINGS_PATH.exists():
        settings = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    if SECRETS_PATH.exists():
        secrets = json.loads(SECRETS_PATH.read_text(encoding="utf-8"))
        for k, v in secrets.items():
            if v is not None:
                settings[k] = v
    return settings


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    except sqlite3.DatabaseError:
        return set()


def _create_index_if_columns_exist(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    index_name: str,
    columns_sql: str,
    required_columns: tuple[str, ...],
):
    existing = _table_columns(conn, table_name)
    if not existing:
        return
    if any(column_name not in existing for column_name in required_columns):
        return
    conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({columns_sql})")


def ensure_additive_schema(conn: sqlite3.Connection):
    for table_name, columns in ADDITIVE_COLUMNS.items():
        existing = _table_columns(conn, table_name)
        if not existing:
            continue
        for column_name, column_sql in columns.items():
            if column_name in existing:
                continue
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")
    conn.executescript(ADDITIVE_SCHEMA_SQL)
    _create_index_if_columns_exist(
        conn,
        table_name="claims",
        index_name="idx_claims_canonical_hash",
        columns_sql="canonical_hash",
        required_columns=("canonical_hash",),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="claims",
        index_name="idx_claims_cluster",
        columns_sql="claim_cluster_id",
        required_columns=("claim_cluster_id",),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="evidence_links",
        index_name="idx_evidence_class",
        columns_sql="evidence_class",
        required_columns=("evidence_class",),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="content_tags",
        index_name="idx_content_tags_namespace",
        columns_sql="namespace",
        required_columns=("namespace",),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="content_tags",
        index_name="idx_content_tags_normalized",
        columns_sql="normalized_tag",
        required_columns=("normalized_tag",),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="relation_candidates",
        index_name="idx_relation_candidates_candidate_state",
        columns_sql="candidate_state",
        required_columns=("candidate_state",),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="relation_support",
        index_name="idx_relation_support_class",
        columns_sql="support_class",
        required_columns=("support_class",),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="relation_support",
        index_name="idx_relation_support_event_fact",
        columns_sql="event_id, fact_id",
        required_columns=("event_id", "fact_id"),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="content_derivations",
        index_name="idx_content_derivations_current",
        columns_sql="content_item_id, derivation_type, is_current",
        required_columns=("content_item_id", "derivation_type", "is_current"),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="ai_task_attempts",
        index_name="idx_ai_task_attempts_failure_kind",
        columns_sql="failure_kind",
        required_columns=("failure_kind",),
    )
    _create_index_if_columns_exist(
        conn,
        table_name="source_sync_state",
        index_name="idx_source_sync_state_quality",
        columns_sql="quality_state",
        required_columns=("quality_state",),
    )
    conn.commit()


def _execute_schema_sql(conn: sqlite3.Connection, sql: str):
    for raw_statement in sql.split(";"):
        statement = raw_statement.strip()
        if not statement:
            continue
        try:
            conn.execute(statement)
        except sqlite3.OperationalError as error:
            lowered = str(error).lower()
            normalized = statement.upper()
            if normalized.startswith("CREATE INDEX") and (
                "no such column" in lowered or "has no column named" in lowered
            ):
                log.warning("Skipping schema statement due to missing legacy column: %s", statement.splitlines()[0][:180])
                continue
            raise


def exec_schema(conn: sqlite3.Connection, schema_path: Path | None = None):
    target_schema = schema_path or SCHEMA_PATH
    sql = target_schema.read_text(encoding="utf-8")
    ensure_additive_schema(conn)
    _execute_schema_sql(conn, sql)
    ensure_additive_schema(conn)
    conn.commit()


def get_db(settings: dict = None) -> sqlite3.Connection:
    if settings is None:
        settings = load_settings()
    db_path = Path(settings.get("db_path", str(PROJECT_ROOT / "db" / "news_unified.db")))
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    if settings.get("ensure_schema_on_connect", True):
        exec_schema(conn, SCHEMA_PATH)
    return conn


def ensure_dirs(settings: dict = None):
    if settings is None:
        settings = load_settings()
    for key in [
        "inbox_tiktok", "inbox_documents", "inbox_youtube",
        "processed_tiktok", "processed_youtube", "processed_documents",
        "processed_telegram", "processed_keyframes",
    ]:
        p = Path(settings.get(key, str(PROJECT_ROOT / key.replace("_", "/", 1))))
        p.mkdir(parents=True, exist_ok=True)


def setup_logging(settings: dict = None):
    if settings is None:
        settings = load_settings()

    log_level = getattr(logging, settings.get("log_level", "INFO").upper(), logging.INFO)
    log_file = settings.get("log_file", str(PROJECT_ROOT / "app.log"))
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    managed_handlers = [
        h for h in root_logger.handlers
        if getattr(h, "_news_archive_handler", False)
    ]
    for handler in managed_handlers:
        root_logger.removeHandler(handler)
        handler.close()

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(log_level)
    ch.setFormatter(fmt)
    ch._news_archive_handler = True
    root_logger.addHandler(ch)

    fh = RotatingFileHandler(
        str(log_path), maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    fh.setLevel(log_level)
    fh.setFormatter(fmt)
    fh._news_archive_handler = True
    root_logger.addHandler(fh)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("pyrogram").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
