CREATE TABLE source_systems (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_key TEXT NOT NULL UNIQUE,
    source_type TEXT NOT NULL,
    title TEXT NOT NULL,
    canonical_url TEXT,
    policy_json TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE source_objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_system_id INTEGER NOT NULL REFERENCES source_systems(id),
    external_id TEXT NOT NULL,
    object_kind TEXT NOT NULL DEFAULT 'content',
    canonical_url TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    metadata_json TEXT,
    UNIQUE(source_system_id, external_id)
);

CREATE TABLE source_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_object_id INTEGER NOT NULL REFERENCES source_objects(id) ON DELETE CASCADE,
    revision_no INTEGER NOT NULL,
    payload_hash TEXT NOT NULL,
    text_hash TEXT,
    payload_json TEXT NOT NULL,
    observed_at TEXT,
    fetched_at TEXT NOT NULL,
    supersedes_revision_id INTEGER REFERENCES source_revisions(id),
    is_current INTEGER NOT NULL DEFAULT 1 CHECK(is_current IN (0,1)),
    metadata_json TEXT,
    UNIQUE(source_object_id, revision_no),
    UNIQUE(source_object_id, payload_hash)
);
CREATE UNIQUE INDEX source_revisions_one_current
ON source_revisions(source_object_id) WHERE is_current=1;
CREATE INDEX source_revisions_hash ON source_revisions(payload_hash);

CREATE TABLE blobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sha256 TEXT NOT NULL UNIQUE,
    media_type TEXT,
    byte_size INTEGER NOT NULL,
    storage_path TEXT NOT NULL,
    perceptual_hash TEXT,
    metadata_json TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE revision_blobs (
    revision_id INTEGER NOT NULL REFERENCES source_revisions(id) ON DELETE CASCADE,
    blob_id INTEGER NOT NULL REFERENCES blobs(id),
    role TEXT NOT NULL DEFAULT 'attachment',
    ordinal INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(revision_id, blob_id, role)
);

CREATE TABLE transform_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage TEXT NOT NULL,
    source_revision_id INTEGER REFERENCES source_revisions(id),
    subject_type TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    provider TEXT,
    model TEXT,
    prompt_version TEXT,
    transform_version TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    output_hash TEXT,
    status TEXT NOT NULL CHECK(status IN ('running','completed','failed','skipped')),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    latency_ms INTEGER,
    error_kind TEXT,
    error_text TEXT,
    usage_json TEXT,
    metadata_json TEXT,
    UNIQUE(stage, subject_type, subject_key, transform_version, input_hash)
);

CREATE TABLE derivation_outputs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transform_run_id INTEGER NOT NULL REFERENCES transform_runs(id) ON DELETE CASCADE,
    output_type TEXT NOT NULL,
    schema_name TEXT,
    schema_version TEXT,
    schema_valid INTEGER NOT NULL DEFAULT 0 CHECK(schema_valid IN (0,1)),
    output_text TEXT,
    output_json TEXT,
    confidence REAL NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(transform_run_id, output_type)
);

CREATE TABLE entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    canonical_key TEXT NOT NULL UNIQUE,
    resolution_state TEXT NOT NULL DEFAULT 'candidate',
    confidence REAL NOT NULL DEFAULT 0,
    metadata_json TEXT,
    recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
    superseded_at TEXT
);

CREATE TABLE entity_aliases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    alias_normalized TEXT NOT NULL,
    source_revision_id INTEGER REFERENCES source_revisions(id),
    confidence REAL NOT NULL DEFAULT 0,
    UNIQUE(entity_id, alias_normalized, source_revision_id)
);

CREATE TABLE projection_generations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    projection_type TEXT NOT NULL,
    generation_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'building'
        CHECK(status IN ('building','validated','active','superseded','failed')),
    source_watermark TEXT,
    started_at TEXT NOT NULL,
    validated_at TEXT,
    activated_at TEXT,
    finished_at TEXT,
    metrics_json TEXT,
    error_text TEXT
);
CREATE UNIQUE INDEX projection_one_active
ON projection_generations(projection_type) WHERE status='active';

CREATE TABLE projection_generation_members (
    generation_id INTEGER NOT NULL REFERENCES projection_generations(id) ON DELETE CASCADE,
    object_type TEXT NOT NULL,
    object_id INTEGER NOT NULL,
    natural_key TEXT NOT NULL,
    payload_hash TEXT,
    PRIMARY KEY(generation_id, object_type, natural_key)
);

CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    generation_id INTEGER NOT NULL REFERENCES projection_generations(id) ON DELETE CASCADE,
    event_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    canonical_title TEXT NOT NULL,
    summary_short TEXT,
    summary_long TEXT,
    confidence REAL NOT NULL DEFAULT 0,
    event_date_start TEXT,
    event_date_end TEXT,
    first_observed_at TEXT,
    last_observed_at TEXT,
    recorded_at TEXT NOT NULL,
    superseded_at TEXT,
    metadata_json TEXT,
    UNIQUE(generation_id, event_key)
);

CREATE TABLE event_members (
    event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    source_revision_id INTEGER NOT NULL REFERENCES source_revisions(id),
    member_role TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 0,
    PRIMARY KEY(event_id, source_revision_id, member_role)
);

CREATE TABLE facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    generation_id INTEGER NOT NULL REFERENCES projection_generations(id) ON DELETE CASCADE,
    event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    fact_key TEXT NOT NULL,
    fact_type TEXT NOT NULL,
    predicate TEXT NOT NULL,
    canonical_text TEXT NOT NULL,
    polarity TEXT NOT NULL DEFAULT 'positive'
        CHECK(polarity IN ('positive','negative','neutral','uncertain')),
    modality TEXT NOT NULL DEFAULT 'asserted',
    confidence REAL NOT NULL DEFAULT 0,
    valid_from TEXT,
    valid_to TEXT,
    observed_at TEXT,
    recorded_at TEXT NOT NULL,
    superseded_at TEXT,
    metadata_json TEXT,
    UNIQUE(generation_id, fact_key)
);

CREATE TABLE fact_arguments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fact_id INTEGER NOT NULL REFERENCES facts(id) ON DELETE CASCADE,
    entity_id INTEGER NOT NULL REFERENCES entities(id),
    argument_role TEXT NOT NULL,
    source_revision_id INTEGER NOT NULL REFERENCES source_revisions(id),
    evidence_locator TEXT NOT NULL,
    span_start INTEGER,
    span_end INTEGER,
    text_span TEXT,
    confidence REAL NOT NULL DEFAULT 0,
    inference_method TEXT NOT NULL,
    metadata_json TEXT,
    UNIQUE(fact_id, entity_id, argument_role, source_revision_id, evidence_locator)
);

CREATE TABLE evidence_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_revision_id INTEGER NOT NULL REFERENCES source_revisions(id),
    evidence_key TEXT NOT NULL UNIQUE,
    evidence_type TEXT NOT NULL,
    evidence_tier TEXT NOT NULL DEFAULT 'E0'
        CHECK(evidence_tier IN ('E0','E1','E2','E3')),
    source_owner TEXT,
    locator TEXT NOT NULL,
    text_span TEXT,
    entailment_score REAL NOT NULL DEFAULT 0,
    authenticity_score REAL NOT NULL DEFAULT 0,
    verification_state TEXT NOT NULL DEFAULT 'unverified',
    metadata_json TEXT,
    recorded_at TEXT NOT NULL
);

CREATE TABLE fact_evidence (
    fact_id INTEGER NOT NULL REFERENCES facts(id) ON DELETE CASCADE,
    evidence_item_id INTEGER NOT NULL REFERENCES evidence_items(id) ON DELETE CASCADE,
    evidence_class TEXT NOT NULL DEFAULT 'support',
    PRIMARY KEY(fact_id, evidence_item_id)
);

CREATE TABLE relation_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    generation_id INTEGER NOT NULL REFERENCES projection_generations(id) ON DELETE CASCADE,
    entity_a_id INTEGER NOT NULL REFERENCES entities(id),
    entity_b_id INTEGER NOT NULL REFERENCES entities(id),
    signal_type TEXT NOT NULL,
    signal_score REAL NOT NULL DEFAULT 0,
    source_unit_key TEXT NOT NULL,
    observed_at TEXT,
    metadata_json TEXT,
    CHECK(entity_a_id < entity_b_id),
    UNIQUE(generation_id, entity_a_id, entity_b_id, signal_type, source_unit_key)
);

CREATE TABLE relation_assertions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    generation_id INTEGER NOT NULL REFERENCES projection_generations(id) ON DELETE CASCADE,
    natural_key TEXT NOT NULL,
    subject_entity_id INTEGER NOT NULL REFERENCES entities(id),
    predicate TEXT NOT NULL,
    object_entity_id INTEGER NOT NULL REFERENCES entities(id),
    fact_id INTEGER NOT NULL REFERENCES facts(id),
    event_id INTEGER NOT NULL REFERENCES events(id),
    polarity TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'candidate'
        CHECK(state IN ('candidate','review','promoted','rejected')),
    confidence REAL NOT NULL DEFAULT 0,
    valid_from TEXT,
    valid_to TEXT,
    observed_at TEXT,
    promotion_reason TEXT,
    metadata_json TEXT,
    CHECK(subject_entity_id <> object_entity_id),
    UNIQUE(generation_id, natural_key)
);

CREATE TABLE relation_assertion_evidence (
    assertion_id INTEGER NOT NULL REFERENCES relation_assertions(id) ON DELETE CASCADE,
    evidence_item_id INTEGER NOT NULL REFERENCES evidence_items(id),
    PRIMARY KEY(assertion_id, evidence_item_id)
);

CREATE TABLE verification_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_type TEXT NOT NULL,
    subject_id INTEGER NOT NULL,
    decision TEXT NOT NULL DEFAULT 'open',
    reason TEXT,
    reviewer TEXT,
    created_at TEXT NOT NULL,
    decided_at TEXT,
    metadata_json TEXT,
    UNIQUE(subject_type, subject_id, decision)
);

CREATE VIEW current_events_v AS
SELECT e.* FROM events e
JOIN projection_generations g ON g.id=e.generation_id
WHERE g.projection_type='events' AND g.status='active' AND e.superseded_at IS NULL;

CREATE VIEW current_facts_v AS
SELECT f.* FROM facts f
JOIN projection_generations g ON g.id=f.generation_id
WHERE g.projection_type='facts' AND g.status='active' AND f.superseded_at IS NULL;

CREATE VIEW current_relation_assertions_v AS
SELECT r.* FROM relation_assertions r
JOIN projection_generations g ON g.id=r.generation_id
WHERE g.projection_type='relations' AND g.status='active' AND r.state='promoted';

CREATE INDEX source_objects_lookup ON source_objects(source_system_id, external_id);
CREATE INDEX facts_event ON facts(event_id, fact_type);
CREATE INDEX fact_arguments_fact ON fact_arguments(fact_id, argument_role);
CREATE INDEX evidence_revision ON evidence_items(source_revision_id, evidence_tier);
CREATE INDEX relation_assertions_subject ON relation_assertions(subject_entity_id, predicate);
CREATE INDEX relation_assertions_object ON relation_assertions(object_entity_id, predicate);
