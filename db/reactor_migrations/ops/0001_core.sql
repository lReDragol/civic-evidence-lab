CREATE TABLE pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name TEXT NOT NULL,
    trigger_mode TEXT NOT NULL,
    requested_by TEXT,
    status TEXT NOT NULL,
    trace_id TEXT NOT NULL UNIQUE,
    started_at TEXT NOT NULL,
    heartbeat_at TEXT,
    finished_at TEXT,
    result_json TEXT,
    error_text TEXT
);

CREATE TABLE job_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_run_id INTEGER REFERENCES pipeline_runs(id) ON DELETE SET NULL,
    job_key TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    owner TEXT,
    attempt_no INTEGER NOT NULL DEFAULT 1,
    started_at TEXT NOT NULL,
    heartbeat_at TEXT,
    deadline_at TEXT,
    finished_at TEXT,
    counters_json TEXT,
    result_json TEXT,
    error_kind TEXT,
    error_text TEXT
);
CREATE INDEX job_runs_active ON job_runs(status, heartbeat_at);

CREATE TABLE work_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    work_key TEXT NOT NULL UNIQUE,
    pipeline_run_id INTEGER REFERENCES pipeline_runs(id) ON DELETE SET NULL,
    stage TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 50,
    lease_owner TEXT,
    lease_expires_at TEXT,
    heartbeat_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    available_at TEXT,
    payload_json TEXT,
    result_json TEXT,
    failure_kind TEXT,
    error_text TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX work_items_queue ON work_items(status, available_at, priority, id);

CREATE TABLE agent_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_key TEXT NOT NULL UNIQUE,
    task_type TEXT NOT NULL,
    requester_group TEXT NOT NULL,
    target_group TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 50,
    lease_owner TEXT,
    lease_expires_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT,
    acceptance_json TEXT,
    result_json TEXT,
    failure_kind TEXT,
    error_text TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX agent_tasks_queue ON agent_tasks(status, target_group, priority, id);

CREATE TABLE agent_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL REFERENCES agent_tasks(id) ON DELETE CASCADE,
    message_type TEXT NOT NULL,
    sender_group TEXT NOT NULL,
    recipient_group TEXT NOT NULL,
    payload_json TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE agent_artifacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER REFERENCES agent_tasks(id) ON DELETE SET NULL,
    artifact_type TEXT NOT NULL,
    subject_type TEXT,
    subject_key TEXT,
    payload_json TEXT,
    confidence REAL NOT NULL DEFAULT 0,
    source_links_json TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE runtime_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id TEXT,
    span_id TEXT,
    parent_span_id TEXT,
    created_at TEXT NOT NULL,
    level TEXT NOT NULL,
    event_type TEXT NOT NULL,
    stage TEXT,
    job_run_id INTEGER REFERENCES job_runs(id) ON DELETE SET NULL,
    source_key TEXT,
    provider TEXT,
    model TEXT,
    subject_key TEXT,
    message TEXT NOT NULL,
    error_type TEXT,
    payload_json TEXT
);
CREATE INDEX runtime_events_trace ON runtime_events(trace_id, created_at);
CREATE INDEX runtime_events_type ON runtime_events(event_type, created_at);

CREATE TABLE provider_health (
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    circuit_open_until TEXT,
    requests INTEGER NOT NULL DEFAULT 0,
    successes INTEGER NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0,
    avg_latency_ms REAL NOT NULL DEFAULT 0,
    last_success_at TEXT,
    last_failure_at TEXT,
    failure_breakdown_json TEXT,
    PRIMARY KEY(provider, model, stage)
);

CREATE TABLE dead_letters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    work_item_id INTEGER REFERENCES work_items(id) ON DELETE SET NULL,
    stage TEXT NOT NULL,
    subject_type TEXT,
    subject_key TEXT,
    failure_kind TEXT NOT NULL,
    error_text TEXT,
    payload_json TEXT,
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    resolution TEXT
);
