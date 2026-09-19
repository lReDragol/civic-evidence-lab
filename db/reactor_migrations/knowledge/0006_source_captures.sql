CREATE TABLE source_captures (
 id INTEGER PRIMARY KEY,
 source_revision_id INTEGER NOT NULL REFERENCES source_revisions(id),
 fetched_at TEXT NOT NULL,
 status TEXT NOT NULL,
 capture_json TEXT NOT NULL
);
CREATE INDEX source_captures_revision ON source_captures(source_revision_id,id);
