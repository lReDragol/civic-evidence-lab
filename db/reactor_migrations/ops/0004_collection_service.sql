CREATE TABLE collection_runs (
    run_id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL,
    profile_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    pid INTEGER NOT NULL,
    started_at REAL NOT NULL,
    heartbeat_at REAL NOT NULL,
    finished_at REAL,
    model_state TEXT NOT NULL DEFAULT 'not_configured',
    baseline_json TEXT NOT NULL,
    error_type TEXT
);
CREATE TABLE collection_events (
    id INTEGER PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES collection_runs(run_id),
    created_at REAL NOT NULL,
    kind TEXT NOT NULL,
    source_key TEXT,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE INDEX collection_events_window ON collection_events(kind,created_at);
CREATE INDEX collection_events_run ON collection_events(run_id,id);
CREATE TABLE collection_sources (
    profile_id TEXT NOT NULL,
    source_key TEXT NOT NULL,
    next_due REAL NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    last_success REAL,
    last_error TEXT,
    last_run_id TEXT,
    PRIMARY KEY(profile_id,source_key)
);
