PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

-- ============================================================
-- СХЕМА БД: система документирования публичных фактов
-- ============================================================

CREATE TABLE IF NOT EXISTS sources (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL,
    subcategory     TEXT,
    url             TEXT,
    access_method   TEXT,
    is_official     INTEGER DEFAULT 0,
    credibility_tier TEXT DEFAULT 'C',
    region          TEXT,
    country         TEXT DEFAULT 'RU',
    owner           TEXT,
    bias_notes      TEXT,
    political_alignment TEXT,
    is_active       INTEGER DEFAULT 1,
    update_frequency TEXT,
    last_checked_at TEXT,
    notes           TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(url, category)
);

CREATE INDEX IF NOT EXISTS idx_sources_category ON sources(category);
CREATE INDEX IF NOT EXISTS idx_sources_tier ON sources(credibility_tier);
CREATE INDEX IF NOT EXISTS idx_sources_active ON sources(is_active);

CREATE TABLE IF NOT EXISTS raw_source_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       INTEGER NOT NULL,
    external_id     TEXT,
    raw_payload     TEXT,
    collected_at    TEXT DEFAULT (datetime('now')),
    hash_sha256     TEXT,
    is_processed    INTEGER DEFAULT 0,
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE CASCADE,
    UNIQUE(source_id, external_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_processed ON raw_source_items(is_processed);
CREATE INDEX IF NOT EXISTS idx_raw_source ON raw_source_items(source_id);

CREATE TABLE IF NOT EXISTS raw_blobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_item_id     INTEGER NOT NULL,
    blob_type       TEXT NOT NULL,
    file_path       TEXT NOT NULL,
    original_filename TEXT,
    storage_rel_path TEXT,
    original_url    TEXT,
    mime_type       TEXT,
    file_size       INTEGER,
    hash_sha256     TEXT NOT NULL,
    metadata_json   TEXT,
    missing_on_disk INTEGER DEFAULT 0,
    downloaded_at   TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (raw_item_id) REFERENCES raw_source_items(id) ON DELETE CASCADE,
    UNIQUE(raw_item_id, original_url)
);

CREATE INDEX IF NOT EXISTS idx_raw_blobs_raw_item ON raw_blobs(raw_item_id);
CREATE INDEX IF NOT EXISTS idx_raw_blobs_hash ON raw_blobs(hash_sha256);
CREATE INDEX IF NOT EXISTS idx_raw_blobs_type ON raw_blobs(blob_type);

CREATE TABLE IF NOT EXISTS content_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       INTEGER NOT NULL,
    raw_item_id     INTEGER,
    external_id     TEXT,
    content_type    TEXT NOT NULL,
    title           TEXT,
    body_text       TEXT,
    published_at    TEXT,
    collected_at    TEXT DEFAULT (datetime('now')),
    url             TEXT,
    language        TEXT DEFAULT 'ru',
    status          TEXT DEFAULT 'raw_signal',
    ner_processed   INTEGER DEFAULT 0,
    llm_processed   INTEGER DEFAULT 0,
    quotes_processed INTEGER DEFAULT 0,
    granular_processed INTEGER DEFAULT 0,
    classification_v3_processed INTEGER DEFAULT 0,
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE CASCADE,
    FOREIGN KEY (raw_item_id) REFERENCES raw_source_items(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_content_type ON content_items(content_type);
CREATE INDEX IF NOT EXISTS idx_content_status ON content_items(status);
CREATE INDEX IF NOT EXISTS idx_content_source ON content_items(source_id);
CREATE INDEX IF NOT EXISTS idx_content_published ON content_items(published_at);

CREATE VIRTUAL TABLE IF NOT EXISTS content_search USING fts5(
    title, body_text,
    content='content_items',
    content_rowid='id'
);

CREATE TABLE IF NOT EXISTS attachments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL,
    blob_id         INTEGER,
    file_path       TEXT NOT NULL,
    attachment_type TEXT NOT NULL,
    hash_sha256     TEXT NOT NULL,
    file_size       INTEGER,
    mime_type       TEXT,
    ocr_text        TEXT,
    is_original     INTEGER DEFAULT 1,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE,
    FOREIGN KEY (blob_id) REFERENCES raw_blobs(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_attachments_content ON attachments(content_item_id);
CREATE INDEX IF NOT EXISTS idx_attachments_blob ON attachments(blob_id);
CREATE INDEX IF NOT EXISTS idx_attachments_hash ON attachments(hash_sha256);
CREATE UNIQUE INDEX IF NOT EXISTS idx_attachments_content_blob_unique
    ON attachments(content_item_id, blob_id)
    WHERE blob_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS entities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type     TEXT NOT NULL,
    canonical_name  TEXT NOT NULL,
    inn             TEXT,
    ogrn            TEXT,
    description     TEXT,
    extra_data      TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(entity_type, canonical_name)
);

CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_inn ON entities(inn);
CREATE INDEX IF NOT EXISTS idx_entities_ogrn ON entities(ogrn);
CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(canonical_name);

CREATE TABLE IF NOT EXISTS entity_aliases (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    alias           TEXT NOT NULL,
    alias_type      TEXT DEFAULT 'spelling',
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    UNIQUE(entity_id, alias)
);

CREATE TABLE IF NOT EXISTS entity_mentions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    content_item_id INTEGER NOT NULL,
    mention_type    TEXT NOT NULL,
    confidence      REAL DEFAULT 1.0,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_entity_mentions_entity ON entity_mentions(entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_mentions_content ON entity_mentions(content_item_id);

CREATE TABLE IF NOT EXISTS claims (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL,
    claim_text      TEXT NOT NULL,
    canonical_text  TEXT,
    canonical_hash  TEXT,
    claim_cluster_id INTEGER,
    claim_type      TEXT,
    confidence_auto REAL,
    confidence_final REAL,
    status          TEXT DEFAULT 'unverified',
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
    -- claim_cluster_id FK added via additive schema helper for backward compatibility
);

CREATE INDEX IF NOT EXISTS idx_claims_status ON claims(status);
CREATE INDEX IF NOT EXISTS idx_claims_content ON claims(content_item_id);
CREATE INDEX IF NOT EXISTS idx_claims_review ON claims(needs_review);
CREATE INDEX IF NOT EXISTS idx_claims_canonical_hash ON claims(canonical_hash);
CREATE INDEX IF NOT EXISTS idx_claims_cluster ON claims(claim_cluster_id);

CREATE TABLE IF NOT EXISTS evidence_links (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_id        INTEGER NOT NULL,
    evidence_item_id INTEGER,
    evidence_type   TEXT NOT NULL,
    evidence_class  TEXT DEFAULT 'support',
    strength        TEXT DEFAULT 'moderate',
    notes           TEXT,
    linked_by       TEXT,
    linked_at       TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (claim_id) REFERENCES claims(id) ON DELETE CASCADE,
    FOREIGN KEY (evidence_item_id) REFERENCES content_items(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_evidence_claim ON evidence_links(claim_id);
CREATE INDEX IF NOT EXISTS idx_evidence_item ON evidence_links(evidence_item_id);
CREATE INDEX IF NOT EXISTS idx_evidence_class ON evidence_links(evidence_class);

CREATE TABLE IF NOT EXISTS cases (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    title           TEXT NOT NULL,
    description     TEXT,
    case_type       TEXT,
    status          TEXT DEFAULT 'open',
    region          TEXT,
    started_at      TEXT,
    closed_at       TEXT,
    created_by      TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_cases_status ON cases(status);
CREATE INDEX IF NOT EXISTS idx_cases_type ON cases(case_type);

CREATE TABLE IF NOT EXISTS case_claims (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id         INTEGER NOT NULL,
    claim_id        INTEGER NOT NULL,
    role            TEXT DEFAULT 'central',
    added_at        TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE CASCADE,
    FOREIGN KEY (claim_id) REFERENCES claims(id) ON DELETE CASCADE,
    UNIQUE(case_id, claim_id)
);

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

CREATE INDEX IF NOT EXISTS idx_case_events_case ON case_events(case_id);
CREATE INDEX IF NOT EXISTS idx_case_events_date ON case_events(event_date);

CREATE TABLE IF NOT EXISTS quotes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL,
    entity_id       INTEGER,
    quote_text      TEXT NOT NULL,
    timecode_start  TEXT,
    timecode_end    TEXT,
    context         TEXT,
    rhetoric_class  TEXT,
    is_flagged      INTEGER DEFAULT 0,
    verified_by     TEXT,
    verified_at     TEXT,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_quotes_entity ON quotes(entity_id);
CREATE INDEX IF NOT EXISTS idx_quotes_flagged ON quotes(is_flagged);
CREATE INDEX IF NOT EXISTS idx_quotes_rhetoric ON quotes(rhetoric_class);

CREATE TABLE IF NOT EXISTS deputy_profiles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    full_name       TEXT NOT NULL,
    position        TEXT,
    faction         TEXT,
    region          TEXT,
    committee       TEXT,
    duma_id         INTEGER,
    date_elected    TEXT,
    income_latest   TEXT,
    biography_url   TEXT,
    photo_url       TEXT,
    is_active       INTEGER DEFAULT 1,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    UNIQUE(entity_id)
);

CREATE INDEX IF NOT EXISTS idx_deputy_faction ON deputy_profiles(faction);
CREATE INDEX IF NOT EXISTS idx_deputy_region ON deputy_profiles(region);
CREATE INDEX IF NOT EXISTS idx_deputy_active ON deputy_profiles(is_active);

CREATE TABLE IF NOT EXISTS accountability_index (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    deputy_id       INTEGER NOT NULL,
    period          TEXT NOT NULL,
    public_speeches_count   INTEGER DEFAULT 0,
    verifiable_claims_count INTEGER DEFAULT 0,
    confirmed_contradictions INTEGER DEFAULT 0,
    flagged_statements_count INTEGER DEFAULT 0,
    votes_tracked_count     INTEGER DEFAULT 0,
    linked_cases_count      INTEGER DEFAULT 0,
    promises_made_count     INTEGER DEFAULT 0,
    promises_kept_count     INTEGER DEFAULT 0,
    calculated_score  REAL DEFAULT 0,
    FOREIGN KEY (deputy_id) REFERENCES deputy_profiles(id) ON DELETE CASCADE,
    UNIQUE(deputy_id, period)
);

CREATE TABLE IF NOT EXISTS verifications (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_id        INTEGER NOT NULL,
    verifier_type   TEXT NOT NULL,
    old_status      TEXT,
    new_status      TEXT,
    notes           TEXT,
    evidence_added  INTEGER DEFAULT 0,
    verified_by     TEXT,
    verified_at     TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (claim_id) REFERENCES claims(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_verifications_claim ON verifications(claim_id);

CREATE TABLE IF NOT EXISTS content_tags (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL,
    tag_level       INTEGER NOT NULL,
    tag_name        TEXT NOT NULL,
    namespace       TEXT,
    normalized_tag  TEXT,
    confidence      REAL DEFAULT 1.0,
    confidence_calibrated REAL,
    tag_source      TEXT DEFAULT 'rule',
    decision_source TEXT,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE,
    UNIQUE(content_item_id, tag_level, tag_name)
);

CREATE INDEX IF NOT EXISTS idx_content_tags_item ON content_tags(content_item_id);
CREATE INDEX IF NOT EXISTS idx_content_tags_name ON content_tags(tag_name);
CREATE INDEX IF NOT EXISTS idx_content_tags_level ON content_tags(tag_level);
CREATE INDEX IF NOT EXISTS idx_content_tags_namespace ON content_tags(namespace);
CREATE INDEX IF NOT EXISTS idx_content_tags_normalized ON content_tags(normalized_tag);

CREATE TABLE IF NOT EXISTS content_tag_votes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL,
    voter_name      TEXT NOT NULL,
    tag_name        TEXT NOT NULL,
    namespace       TEXT,
    normalized_tag  TEXT,
    vote_value      TEXT NOT NULL,
    confidence_raw  REAL DEFAULT 0,
    evidence_text   TEXT,
    metadata_json   TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_content_tag_votes_item ON content_tag_votes(content_item_id);
CREATE INDEX IF NOT EXISTS idx_content_tag_votes_tag ON content_tag_votes(normalized_tag);
CREATE INDEX IF NOT EXISTS idx_content_tag_votes_vote ON content_tag_votes(vote_value);

CREATE TABLE IF NOT EXISTS entity_relations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    from_entity_id  INTEGER NOT NULL,
    to_entity_id    INTEGER NOT NULL,
    relation_type   TEXT NOT NULL,
    evidence_item_id INTEGER,
    strength        TEXT DEFAULT 'moderate',
    detected_at     TEXT DEFAULT (datetime('now')),
    detected_by     TEXT,
    FOREIGN KEY (from_entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (to_entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (evidence_item_id) REFERENCES content_items(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_entity_relations_from ON entity_relations(from_entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_relations_to ON entity_relations(to_entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_relations_type ON entity_relations(relation_type);

CREATE TABLE IF NOT EXISTS risk_patterns (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern_type    TEXT NOT NULL,
    description     TEXT NOT NULL,
    entity_ids      TEXT NOT NULL,
    evidence_ids    TEXT,
    risk_level      TEXT DEFAULT 'low',
    case_id         INTEGER,
    detected_at     TEXT DEFAULT (datetime('now')),
    needs_review    INTEGER DEFAULT 1,
    FOREIGN KEY (case_id) REFERENCES cases(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_risk_patterns_type ON risk_patterns(pattern_type);
CREATE INDEX IF NOT EXISTS idx_risk_patterns_risk ON risk_patterns(risk_level);

CREATE TABLE IF NOT EXISTS official_positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    position_title  TEXT,
    organization    TEXT,
    region          TEXT,
    faction         TEXT,
    started_at      TEXT,
    ended_at        TEXT,
    source_url      TEXT,
    source_type     TEXT,
    is_active       INTEGER DEFAULT 1,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_official_positions_entity ON official_positions(entity_id);
CREATE INDEX IF NOT EXISTS idx_official_positions_active ON official_positions(is_active);

CREATE TABLE IF NOT EXISTS party_memberships (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    party_name      TEXT NOT NULL,
    role            TEXT,
    started_at      TEXT,
    ended_at        TEXT,
    source_url      TEXT,
    is_current      INTEGER DEFAULT 1,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_party_memberships_entity ON party_memberships(entity_id);
CREATE INDEX IF NOT EXISTS idx_party_memberships_current ON party_memberships(is_current);

CREATE TABLE IF NOT EXISTS bills (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    number          TEXT NOT NULL,
    title           TEXT NOT NULL,
    bill_type       TEXT,
    status          TEXT,
    registration_date TEXT,
    duma_url        TEXT,
    committee       TEXT,
    keywords        TEXT,
    annotation      TEXT,
    raw_data        TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_bills_number ON bills(number);
CREATE INDEX IF NOT EXISTS idx_bills_status ON bills(status);
CREATE INDEX IF NOT EXISTS idx_bills_registration_date ON bills(registration_date);

CREATE TABLE IF NOT EXISTS bill_sponsors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bill_id         INTEGER NOT NULL,
    entity_id       INTEGER,
    sponsor_name    TEXT,
    sponsor_role    TEXT,
    faction         TEXT,
    is_collective   INTEGER DEFAULT 0,
    FOREIGN KEY (bill_id) REFERENCES bills(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_bill_sponsors_bill ON bill_sponsors(bill_id);
CREATE INDEX IF NOT EXISTS idx_bill_sponsors_entity ON bill_sponsors(entity_id);

CREATE TABLE IF NOT EXISTS bill_vote_sessions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bill_id         INTEGER,
    vote_date       TEXT NOT NULL,
    vote_stage      TEXT,
    total_for       INTEGER DEFAULT 0,
    total_against   INTEGER DEFAULT 0,
    total_abstained INTEGER DEFAULT 0,
    total_absent    INTEGER DEFAULT 0,
    total_present   INTEGER DEFAULT 0,
    result          TEXT,
    duma_session    TEXT,
    raw_data        TEXT,
    FOREIGN KEY (bill_id) REFERENCES bills(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_bill_vote_sessions_bill ON bill_vote_sessions(bill_id);
CREATE INDEX IF NOT EXISTS idx_bill_vote_sessions_date ON bill_vote_sessions(vote_date);

CREATE TABLE IF NOT EXISTS bill_votes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vote_session_id INTEGER NOT NULL,
    entity_id       INTEGER,
    deputy_name     TEXT NOT NULL,
    faction         TEXT,
    vote_result     TEXT NOT NULL,
    raw_data        TEXT,
    FOREIGN KEY (vote_session_id) REFERENCES bill_vote_sessions(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_bill_votes_session ON bill_votes(vote_session_id);
CREATE INDEX IF NOT EXISTS idx_bill_votes_entity ON bill_votes(entity_id);

CREATE TABLE IF NOT EXISTS investigative_materials (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id     INTEGER,
    material_type       TEXT NOT NULL,
    title               TEXT NOT NULL,
    summary             TEXT,
    involved_entities   TEXT,
    referenced_laws     TEXT,
    referenced_cases    TEXT,
    publication_date    TEXT,
    source_org          TEXT,
    source_credibility  TEXT,
    verification_status TEXT,
    url                 TEXT,
    raw_data            TEXT,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_investigative_materials_type ON investigative_materials(material_type);
CREATE INDEX IF NOT EXISTS idx_investigative_materials_content ON investigative_materials(content_item_id);
CREATE INDEX IF NOT EXISTS idx_investigative_materials_date ON investigative_materials(publication_date);

CREATE TABLE IF NOT EXISTS contracts (
    id              INTEGER PRIMARY KEY,
    material_id     INTEGER,
    content_item_id INTEGER,
    contract_number TEXT,
    title           TEXT NOT NULL,
    summary         TEXT,
    publication_date TEXT,
    source_org      TEXT,
    customer_inn    TEXT,
    supplier_inn    TEXT,
    raw_data        TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (material_id) REFERENCES investigative_materials(id) ON DELETE SET NULL,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_contracts_number ON contracts(contract_number);
CREATE INDEX IF NOT EXISTS idx_contracts_material ON contracts(material_id);
CREATE INDEX IF NOT EXISTS idx_contracts_customer_inn ON contracts(customer_inn);
CREATE INDEX IF NOT EXISTS idx_contracts_supplier_inn ON contracts(supplier_inn);

CREATE TABLE IF NOT EXISTS contract_parties (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_id     INTEGER NOT NULL,
    entity_id       INTEGER,
    party_name      TEXT,
    party_role      TEXT NOT NULL,
    inn             TEXT,
    metadata_json   TEXT,
    FOREIGN KEY (contract_id) REFERENCES contracts(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_contract_parties_contract ON contract_parties(contract_id);
CREATE INDEX IF NOT EXISTS idx_contract_parties_entity ON contract_parties(entity_id);
CREATE INDEX IF NOT EXISTS idx_contract_parties_inn ON contract_parties(inn);

CREATE TABLE IF NOT EXISTS runtime_metadata (
    key             TEXT PRIMARY KEY,
    value_text      TEXT,
    value_json      TEXT,
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS job_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          TEXT NOT NULL,
    trigger_mode    TEXT DEFAULT 'manual',
    requested_by    TEXT,
    owner           TEXT,
    pipeline_version TEXT,
    pipeline_run_id INTEGER,
    attempt_no      INTEGER DEFAULT 1,
    status          TEXT NOT NULL,
    started_at      TEXT DEFAULT (datetime('now')),
    finished_at     TEXT,
    items_seen      INTEGER DEFAULT 0,
    items_new       INTEGER DEFAULT 0,
    items_updated   INTEGER DEFAULT 0,
    warnings_json   TEXT,
    retriable_errors_json TEXT,
    fatal_errors_json TEXT,
    next_cursor     TEXT,
    health_json     TEXT,
    artifacts_json  TEXT,
    error_summary   TEXT,
    FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_job_runs_job ON job_runs(job_id);
CREATE INDEX IF NOT EXISTS idx_job_runs_status ON job_runs(status);
CREATE INDEX IF NOT EXISTS idx_job_runs_started ON job_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_job_runs_pipeline ON job_runs(pipeline_run_id);

CREATE TABLE IF NOT EXISTS job_leases (
    job_id          TEXT PRIMARY KEY,
    lease_owner     TEXT NOT NULL,
    started_at      TEXT DEFAULT (datetime('now')),
    heartbeat_at    TEXT DEFAULT (datetime('now')),
    expires_at      TEXT NOT NULL,
    payload_json    TEXT
);

CREATE INDEX IF NOT EXISTS idx_job_leases_expires ON job_leases(expires_at);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_version TEXT NOT NULL UNIQUE,
    mode            TEXT NOT NULL,
    status          TEXT NOT NULL,
    requested_by    TEXT,
    started_at      TEXT DEFAULT (datetime('now')),
    finished_at     TEXT,
    stages_json     TEXT,
    result_json     TEXT,
    error_summary   TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_mode ON pipeline_runs(mode);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started ON pipeline_runs(started_at);

CREATE TABLE IF NOT EXISTS source_health_checks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_key      TEXT NOT NULL,
    source_id       INTEGER,
    checked_at      TEXT DEFAULT (datetime('now')),
    url             TEXT,
    ok              INTEGER DEFAULT 0,
    status_code     INTEGER,
    elapsed_sec     REAL,
    final_url       TEXT,
    content_type    TEXT,
    length          INTEGER DEFAULT 0,
    title           TEXT,
    link_count      INTEGER DEFAULT 0,
    transport_mode  TEXT,
    error           TEXT,
    payload_json    TEXT,
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_source_health_key ON source_health_checks(source_key);
CREATE INDEX IF NOT EXISTS idx_source_health_checked ON source_health_checks(checked_at);
CREATE INDEX IF NOT EXISTS idx_source_health_ok ON source_health_checks(ok);

CREATE TABLE IF NOT EXISTS source_sync_state (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_key      TEXT NOT NULL UNIQUE,
    source_id       INTEGER,
    state           TEXT DEFAULT 'unknown',
    quality_state   TEXT DEFAULT 'unknown',
    quality_issue   TEXT,
    failure_class   TEXT,
    last_success_at TEXT,
    last_attempt_at TEXT,
    consecutive_failures INTEGER DEFAULT 0,
    last_cursor     TEXT,
    last_external_id TEXT,
    last_etag       TEXT,
    last_hash       TEXT,
    last_http_status INTEGER,
    transport_mode  TEXT,
    last_error      TEXT,
    metadata_json   TEXT,
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_source_sync_state_id ON source_sync_state(source_id);
CREATE INDEX IF NOT EXISTS idx_source_sync_state_state ON source_sync_state(state);
CREATE INDEX IF NOT EXISTS idx_source_sync_state_quality ON source_sync_state(quality_state);
CREATE INDEX IF NOT EXISTS idx_source_sync_state_success ON source_sync_state(last_success_at);

CREATE TABLE IF NOT EXISTS dead_letter_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_key      TEXT,
    source_id       INTEGER,
    raw_item_id     INTEGER,
    external_id     TEXT,
    attachment_id   INTEGER,
    content_item_id INTEGER,
    failure_stage   TEXT NOT NULL,
    error_type      TEXT,
    error_message   TEXT,
    payload_json    TEXT,
    detected_at     TEXT DEFAULT (datetime('now')),
    resolved_at     TEXT,
    resolution_notes TEXT,
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE SET NULL,
    FOREIGN KEY (raw_item_id) REFERENCES raw_source_items(id) ON DELETE SET NULL,
    FOREIGN KEY (attachment_id) REFERENCES attachments(id) ON DELETE SET NULL,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_source ON dead_letter_items(source_key);
CREATE INDEX IF NOT EXISTS idx_dead_letters_stage ON dead_letter_items(failure_stage);
CREATE INDEX IF NOT EXISTS idx_dead_letters_detected ON dead_letter_items(detected_at);
CREATE INDEX IF NOT EXISTS idx_dead_letters_resolved ON dead_letter_items(resolved_at);

CREATE TABLE IF NOT EXISTS relation_candidates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_a_id     INTEGER NOT NULL,
    entity_b_id     INTEGER NOT NULL,
    candidate_type  TEXT NOT NULL,
    seed_kind       TEXT,
    origin          TEXT NOT NULL,
    score           REAL DEFAULT 0,
    structural_score REAL DEFAULT 0,
    semantic_score  REAL DEFAULT 0,
    support_score   REAL DEFAULT 0,
    calibrated_score REAL DEFAULT 0,
    source_independence REAL DEFAULT 0,
    evidence_overlap REAL DEFAULT 0,
    temporal_proximity REAL DEFAULT 0,
    role_compatibility REAL DEFAULT 0,
    tag_overlap     REAL DEFAULT 0,
    text_specificity REAL DEFAULT 0,
    support_items   INTEGER DEFAULT 0,
    support_sources INTEGER DEFAULT 0,
    support_domains INTEGER DEFAULT 0,
    support_categories INTEGER DEFAULT 0,
    support_claim_cluster_count INTEGER DEFAULT 0,
    support_hard_evidence_count INTEGER DEFAULT 0,
    first_seen_at   TEXT,
    last_seen_at    TEXT,
    sample_content_ids TEXT,
    candidate_state TEXT DEFAULT 'pending',
    promotion_state TEXT DEFAULT 'pending',
    promoted_relation_type TEXT,
    promoted_at     TEXT,
    explain_path_json TEXT,
    metadata_json   TEXT,
    FOREIGN KEY (entity_a_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_b_id) REFERENCES entities(id) ON DELETE CASCADE,
    UNIQUE(entity_a_id, entity_b_id, candidate_type, origin)
);

CREATE INDEX IF NOT EXISTS idx_relation_candidates_pair ON relation_candidates(entity_a_id, entity_b_id);
CREATE INDEX IF NOT EXISTS idx_relation_candidates_score ON relation_candidates(score);
CREATE INDEX IF NOT EXISTS idx_relation_candidates_state ON relation_candidates(promotion_state);
CREATE INDEX IF NOT EXISTS idx_relation_candidates_type ON relation_candidates(candidate_type);
CREATE INDEX IF NOT EXISTS idx_relation_candidates_candidate_state ON relation_candidates(candidate_state);

CREATE TABLE IF NOT EXISTS relation_support (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id    INTEGER NOT NULL,
    support_kind    TEXT NOT NULL,
    support_class   TEXT DEFAULT 'seed',
    content_item_id INTEGER,
    source_id       INTEGER,
    domain          TEXT,
    category        TEXT,
    tag_name        TEXT,
    evidence_item_id INTEGER,
    metric_value    REAL,
    metadata_json   TEXT,
    FOREIGN KEY (candidate_id) REFERENCES relation_candidates(id) ON DELETE CASCADE,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL,
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE SET NULL,
    FOREIGN KEY (evidence_item_id) REFERENCES content_items(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_relation_support_candidate ON relation_support(candidate_id);
CREATE INDEX IF NOT EXISTS idx_relation_support_content ON relation_support(content_item_id);
CREATE INDEX IF NOT EXISTS idx_relation_support_source ON relation_support(source_id);
CREATE INDEX IF NOT EXISTS idx_relation_support_class ON relation_support(support_class);

CREATE TABLE IF NOT EXISTS relation_features (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id    INTEGER NOT NULL UNIQUE,
    structural_score REAL DEFAULT 0,
    content_support_score REAL DEFAULT 0,
    source_diversity_score REAL DEFAULT 0,
    semantic_support_score REAL DEFAULT 0,
    shared_claim_cluster_score REAL DEFAULT 0,
    evidence_quality_score REAL DEFAULT 0,
    temporal_score REAL DEFAULT 0,
    role_compatibility_score REAL DEFAULT 0,
    calibrated_score REAL DEFAULT 0,
    explain_path_json TEXT,
    metadata_json   TEXT,
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (candidate_id) REFERENCES relation_candidates(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_relation_features_candidate ON relation_features(candidate_id);

CREATE TABLE IF NOT EXISTS classifier_audit_samples (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_kind     TEXT NOT NULL,
    target_id       INTEGER,
    target_ref      TEXT,
    expected_label  TEXT,
    actual_label    TEXT,
    review_status   TEXT DEFAULT 'pending',
    reviewed_by     TEXT,
    reviewed_at     TEXT,
    notes           TEXT,
    batch_name      TEXT,
    payload_json    TEXT
);

CREATE INDEX IF NOT EXISTS idx_classifier_audit_kind ON classifier_audit_samples(sample_kind);
CREATE INDEX IF NOT EXISTS idx_classifier_audit_status ON classifier_audit_samples(review_status);
CREATE INDEX IF NOT EXISTS idx_classifier_audit_batch ON classifier_audit_samples(batch_name);

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

CREATE TABLE IF NOT EXISTS investigation_leads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id       INTEGER NOT NULL,
    lead_type       TEXT NOT NULL,
    title           TEXT NOT NULL,
    score           REAL DEFAULT 0,
    chain_json      TEXT,
    pipeline_version TEXT,
    generated_at    TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_investigation_leads_entity ON investigation_leads(entity_id);
CREATE INDEX IF NOT EXISTS idx_investigation_leads_type ON investigation_leads(lead_type);
CREATE INDEX IF NOT EXISTS idx_investigation_leads_score ON investigation_leads(score);
