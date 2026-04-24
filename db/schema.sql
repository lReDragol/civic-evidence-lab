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
);

CREATE INDEX IF NOT EXISTS idx_claims_status ON claims(status);
CREATE INDEX IF NOT EXISTS idx_claims_content ON claims(content_item_id);
CREATE INDEX IF NOT EXISTS idx_claims_review ON claims(needs_review);

CREATE TABLE IF NOT EXISTS evidence_links (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_id        INTEGER NOT NULL,
    evidence_item_id INTEGER,
    evidence_type   TEXT NOT NULL,
    strength        TEXT DEFAULT 'moderate',
    notes           TEXT,
    linked_by       TEXT,
    linked_at       TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (claim_id) REFERENCES claims(id) ON DELETE CASCADE,
    FOREIGN KEY (evidence_item_id) REFERENCES content_items(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_evidence_claim ON evidence_links(claim_id);
CREATE INDEX IF NOT EXISTS idx_evidence_item ON evidence_links(evidence_item_id);

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
    confidence      REAL DEFAULT 1.0,
    tag_source      TEXT DEFAULT 'rule',
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE CASCADE,
    UNIQUE(content_item_id, tag_level, tag_name)
);

CREATE INDEX IF NOT EXISTS idx_content_tags_item ON content_tags(content_item_id);
CREATE INDEX IF NOT EXISTS idx_content_tags_name ON content_tags(tag_name);
CREATE INDEX IF NOT EXISTS idx_content_tags_level ON content_tags(tag_level);

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
