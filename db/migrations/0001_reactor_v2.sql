CREATE TABLE IF NOT EXISTS source_objects (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id        INTEGER NOT NULL,
    external_id      TEXT NOT NULL,
    object_kind      TEXT NOT NULL DEFAULT 'content',
    canonical_url    TEXT,
    first_seen_at    TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at     TEXT NOT NULL DEFAULT (datetime('now')),
    metadata_json    TEXT,
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE CASCADE,
    UNIQUE(source_id, external_id)
);

CREATE TABLE IF NOT EXISTS source_revisions (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    source_object_id      INTEGER NOT NULL,
    raw_item_id           INTEGER,
    content_item_id       INTEGER,
    revision_no           INTEGER NOT NULL,
    payload_hash          TEXT NOT NULL,
    text_hash             TEXT,
    payload_json          TEXT,
    observed_at           TEXT,
    fetched_at            TEXT NOT NULL DEFAULT (datetime('now')),
    supersedes_revision_id INTEGER,
    is_current            INTEGER NOT NULL DEFAULT 1,
    metadata_json         TEXT,
    FOREIGN KEY (source_object_id) REFERENCES source_objects(id) ON DELETE CASCADE,
    FOREIGN KEY (raw_item_id) REFERENCES raw_source_items(id) ON DELETE SET NULL,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL,
    FOREIGN KEY (supersedes_revision_id) REFERENCES source_revisions(id) ON DELETE SET NULL,
    UNIQUE(source_object_id, revision_no),
    UNIQUE(source_object_id, payload_hash)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_source_revisions_current
ON source_revisions(source_object_id)
WHERE is_current=1;
CREATE INDEX IF NOT EXISTS idx_source_revisions_content ON source_revisions(content_item_id);
CREATE INDEX IF NOT EXISTS idx_source_revisions_hash ON source_revisions(payload_hash);

CREATE TABLE IF NOT EXISTS transform_runs (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    stage              TEXT NOT NULL,
    source_revision_id INTEGER,
    subject_type       TEXT,
    subject_id         INTEGER,
    provider           TEXT,
    model              TEXT,
    transform_version  TEXT NOT NULL,
    input_hash         TEXT NOT NULL,
    output_hash        TEXT,
    status             TEXT NOT NULL DEFAULT 'running',
    started_at         TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at        TEXT,
    latency_ms         INTEGER,
    error_kind         TEXT,
    error_text         TEXT,
    usage_json         TEXT,
    metadata_json      TEXT,
    FOREIGN KEY (source_revision_id) REFERENCES source_revisions(id) ON DELETE SET NULL,
    UNIQUE(stage, subject_type, subject_id, transform_version, input_hash)
);

CREATE INDEX IF NOT EXISTS idx_transform_runs_status ON transform_runs(status, stage);
CREATE INDEX IF NOT EXISTS idx_transform_runs_source_revision ON transform_runs(source_revision_id);

CREATE TABLE IF NOT EXISTS projection_generations (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    projection_type   TEXT NOT NULL,
    generation_key    TEXT NOT NULL UNIQUE,
    status            TEXT NOT NULL DEFAULT 'building',
    source_watermark  TEXT,
    started_at        TEXT NOT NULL DEFAULT (datetime('now')),
    validated_at      TEXT,
    activated_at      TEXT,
    finished_at       TEXT,
    metrics_json      TEXT,
    error_text        TEXT,
    CHECK(status IN ('building','validated','active','superseded','failed'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_projection_one_active
ON projection_generations(projection_type)
WHERE status='active';
CREATE INDEX IF NOT EXISTS idx_projection_generations_status
ON projection_generations(projection_type, status, id);

CREATE TABLE IF NOT EXISTS projection_generation_members (
    generation_id INTEGER NOT NULL,
    object_type   TEXT NOT NULL,
    object_id     INTEGER NOT NULL,
    natural_key   TEXT NOT NULL,
    payload_hash  TEXT,
    PRIMARY KEY (generation_id, object_type, natural_key),
    FOREIGN KEY (generation_id) REFERENCES projection_generations(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS entity_canonical_map (
    entity_id            INTEGER PRIMARY KEY,
    canonical_entity_id  INTEGER NOT NULL,
    resolution_state     TEXT NOT NULL DEFAULT 'resolved',
    confidence           REAL NOT NULL DEFAULT 1,
    method               TEXT,
    reviewed_at          TEXT,
    metadata_json        TEXT,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (canonical_entity_id) REFERENCES entities(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS fact_arguments (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    fact_id          INTEGER NOT NULL,
    entity_id        INTEGER NOT NULL,
    argument_role    TEXT NOT NULL,
    content_item_id  INTEGER,
    evidence_locator TEXT,
    span_start       INTEGER,
    span_end         INTEGER,
    text_span        TEXT,
    confidence       REAL NOT NULL DEFAULT 0,
    inference_method TEXT NOT NULL,
    generation_id    INTEGER,
    metadata_json    TEXT,
    FOREIGN KEY (fact_id) REFERENCES event_facts(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL,
    FOREIGN KEY (generation_id) REFERENCES projection_generations(id) ON DELETE CASCADE,
    UNIQUE(generation_id, fact_id, entity_id, argument_role, content_item_id, evidence_locator)
);

CREATE INDEX IF NOT EXISTS idx_fact_arguments_fact ON fact_arguments(fact_id, argument_role);
CREATE INDEX IF NOT EXISTS idx_fact_arguments_entity ON fact_arguments(entity_id, argument_role);

CREATE TABLE IF NOT EXISTS relation_signals (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    generation_id    INTEGER NOT NULL,
    entity_a_id      INTEGER NOT NULL,
    entity_b_id      INTEGER NOT NULL,
    signal_type      TEXT NOT NULL,
    signal_score     REAL NOT NULL DEFAULT 0,
    source_unit_key  TEXT NOT NULL,
    observed_at      TEXT,
    metadata_json    TEXT,
    FOREIGN KEY (generation_id) REFERENCES projection_generations(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_a_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_b_id) REFERENCES entities(id) ON DELETE CASCADE,
    CHECK(entity_a_id < entity_b_id),
    UNIQUE(generation_id, entity_a_id, entity_b_id, signal_type, source_unit_key)
);

CREATE INDEX IF NOT EXISTS idx_relation_signals_pair
ON relation_signals(generation_id, entity_a_id, entity_b_id);

CREATE TABLE IF NOT EXISTS relation_assertions (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    generation_id            INTEGER NOT NULL,
    natural_key              TEXT NOT NULL,
    subject_entity_id        INTEGER NOT NULL,
    predicate                TEXT NOT NULL,
    object_entity_id         INTEGER NOT NULL,
    fact_id                  INTEGER,
    event_id                 INTEGER,
    polarity                 TEXT NOT NULL DEFAULT 'positive',
    confidence               REAL NOT NULL DEFAULT 0,
    state                    TEXT NOT NULL DEFAULT 'candidate',
    valid_from               TEXT,
    valid_to                 TEXT,
    observed_at              TEXT,
    recorded_at              TEXT NOT NULL DEFAULT (datetime('now')),
    promotion_reason         TEXT,
    materialized_relation_id INTEGER,
    metadata_json            TEXT,
    FOREIGN KEY (generation_id) REFERENCES projection_generations(id) ON DELETE CASCADE,
    FOREIGN KEY (subject_entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (object_entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (fact_id) REFERENCES event_facts(id) ON DELETE SET NULL,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE SET NULL,
    FOREIGN KEY (materialized_relation_id) REFERENCES entity_relations(id) ON DELETE SET NULL,
    CHECK(subject_entity_id <> object_entity_id),
    CHECK(state IN ('candidate','review','promoted','rejected')),
    CHECK(polarity IN ('positive','negative','neutral','uncertain')),
    UNIQUE(generation_id, natural_key)
);

CREATE INDEX IF NOT EXISTS idx_relation_assertions_state
ON relation_assertions(generation_id, state, confidence);
CREATE INDEX IF NOT EXISTS idx_relation_assertions_subject
ON relation_assertions(subject_entity_id, predicate);
CREATE INDEX IF NOT EXISTS idx_relation_assertions_object
ON relation_assertions(object_entity_id, predicate);

CREATE TABLE IF NOT EXISTS relation_assertion_evidence (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    assertion_id        INTEGER NOT NULL,
    fact_evidence_id    INTEGER,
    content_item_id     INTEGER,
    source_revision_id  INTEGER,
    evidence_tier       TEXT NOT NULL DEFAULT 'E0',
    entailment_score    REAL NOT NULL DEFAULT 0,
    authenticity_score  REAL NOT NULL DEFAULT 0,
    source_reliability  REAL NOT NULL DEFAULT 0,
    source_owner        TEXT,
    evidence_locator    TEXT,
    text_span           TEXT,
    metadata_json       TEXT,
    FOREIGN KEY (assertion_id) REFERENCES relation_assertions(id) ON DELETE CASCADE,
    FOREIGN KEY (fact_evidence_id) REFERENCES fact_evidence(id) ON DELETE SET NULL,
    FOREIGN KEY (content_item_id) REFERENCES content_items(id) ON DELETE SET NULL,
    FOREIGN KEY (source_revision_id) REFERENCES source_revisions(id) ON DELETE SET NULL,
    CHECK(evidence_tier IN ('E0','E1','E2','E3')),
    UNIQUE(assertion_id, fact_evidence_id, content_item_id, source_revision_id, evidence_locator)
);

CREATE INDEX IF NOT EXISTS idx_relation_assertion_evidence_assertion
ON relation_assertion_evidence(assertion_id, evidence_tier);

CREATE TABLE IF NOT EXISTS relation_assertion_reviews (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    assertion_id   INTEGER NOT NULL,
    decision       TEXT NOT NULL DEFAULT 'open',
    reason         TEXT,
    reviewer       TEXT,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    decided_at     TEXT,
    metadata_json  TEXT,
    FOREIGN KEY (assertion_id) REFERENCES relation_assertions(id) ON DELETE CASCADE,
    CHECK(decision IN ('open','approve','reject','defer','need_evidence'))
);

CREATE VIEW IF NOT EXISTS current_events_v AS
SELECT * FROM events WHERE superseded_at IS NULL;

CREATE VIEW IF NOT EXISTS current_event_facts_v AS
SELECT * FROM event_facts WHERE superseded_at IS NULL;

CREATE VIEW IF NOT EXISTS current_event_entities_v AS
SELECT * FROM event_entities WHERE superseded_at IS NULL;

CREATE VIEW IF NOT EXISTS current_relation_assertions_v AS
SELECT ra.*
FROM relation_assertions ra
JOIN projection_generations pg ON pg.id=ra.generation_id
WHERE pg.projection_type='relations'
  AND pg.status='active'
  AND ra.state='promoted';
