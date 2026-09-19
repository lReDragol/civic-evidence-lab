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
