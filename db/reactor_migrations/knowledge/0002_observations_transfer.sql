CREATE TABLE source_observations (
 id INTEGER PRIMARY KEY, source_object_id INTEGER NOT NULL REFERENCES source_objects(id),
 source_revision_id INTEGER NOT NULL REFERENCES source_revisions(id),
 sequence_no INTEGER NOT NULL, observed_at TEXT, fetched_at TEXT NOT NULL,
 previous_observation_id INTEGER REFERENCES source_observations(id),
 UNIQUE(source_object_id, sequence_no)
);
CREATE INDEX source_observations_object ON source_observations(source_object_id, sequence_no);
CREATE TABLE transfer_outbox (
 message_id TEXT PRIMARY KEY, destination TEXT NOT NULL, event_type TEXT NOT NULL,
 subject_key TEXT NOT NULL, input_revision TEXT NOT NULL, payload_json TEXT NOT NULL,
 created_at TEXT NOT NULL, delivered_at TEXT,
 UNIQUE(destination,event_type,subject_key,input_revision)
);
CREATE TABLE transfer_inbox (
 message_id TEXT PRIMARY KEY, received_at TEXT NOT NULL, payload_hash TEXT NOT NULL
);
CREATE INDEX transfer_outbox_pending ON transfer_outbox(delivered_at,created_at);
