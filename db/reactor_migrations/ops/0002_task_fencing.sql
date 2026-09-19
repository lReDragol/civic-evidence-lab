ALTER TABLE agent_tasks ADD COLUMN heartbeat_at TEXT;
ALTER TABLE agent_tasks ADD COLUMN lease_token TEXT;
ALTER TABLE agent_tasks ADD COLUMN available_at TEXT;
ALTER TABLE agent_tasks ADD COLUMN deadline_at TEXT;
ALTER TABLE agent_tasks ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3;
ALTER TABLE agent_tasks ADD COLUMN parent_task_id INTEGER REFERENCES agent_tasks(id);
ALTER TABLE agent_tasks ADD COLUMN trace_id TEXT;
CREATE TABLE search_evidence (
 id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL REFERENCES agent_tasks(id),
 query_hash TEXT NOT NULL, query_text TEXT, provider TEXT, model TEXT, url TEXT,
 title TEXT, snippet TEXT, citation_json TEXT, source_tier TEXT, confidence REAL,
 dedupe_key TEXT NOT NULL, created_at TEXT DEFAULT (datetime('now')),
 UNIQUE(task_id, query_hash, dedupe_key)
);
