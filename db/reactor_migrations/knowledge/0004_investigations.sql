CREATE TABLE civic_claims (
 id INTEGER PRIMARY KEY, source_revision_id INTEGER NOT NULL REFERENCES source_revisions(id),
 claim_key TEXT NOT NULL UNIQUE, claim_text TEXT NOT NULL,
 polarity TEXT NOT NULL CHECK(polarity IN ('affirmed','denied','unknown')),
 modality TEXT NOT NULL CHECK(modality IN ('asserted','alleged','possible','question')),
 attribution TEXT, locator_json TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'unreviewed' CHECK(status IN ('unreviewed','supported','refuted','contested','insufficient_evidence')),
 created_at TEXT NOT NULL
);
CREATE TABLE civic_evidence_links (
 id INTEGER PRIMARY KEY, claim_id INTEGER NOT NULL REFERENCES civic_claims(id),
 source_revision_id INTEGER NOT NULL REFERENCES source_revisions(id),
 stance TEXT NOT NULL CHECK(stance IN ('supports','refutes','context')),
 locator_json TEXT NOT NULL, origin_key TEXT,
 verification_state TEXT NOT NULL DEFAULT 'unverified',
 authenticity_state TEXT NOT NULL DEFAULT 'unknown', reviewed_by TEXT,
 created_at TEXT NOT NULL,
 UNIQUE(claim_id,source_revision_id,stance,locator_json)
);
CREATE TABLE investigation_threads (
 id INTEGER PRIMARY KEY, thread_key TEXT NOT NULL UNIQUE, question TEXT NOT NULL,
 workflow_state TEXT NOT NULL DEFAULT 'open' CHECK(workflow_state IN ('open','researching','waiting_access','review','closed')),
 current_revision_id INTEGER REFERENCES thread_revisions(id), created_at TEXT NOT NULL
);
CREATE TABLE thread_revisions (
 id INTEGER PRIMARY KEY, thread_id INTEGER NOT NULL REFERENCES investigation_threads(id),
 revision_no INTEGER NOT NULL, previous_revision_id INTEGER REFERENCES thread_revisions(id),
 operation TEXT NOT NULL, reason TEXT NOT NULL, actor TEXT NOT NULL,
 membership_json TEXT NOT NULL, created_at TEXT NOT NULL,
 UNIQUE(thread_id,revision_no)
);
CREATE TABLE investigation_gaps (
 id INTEGER PRIMARY KEY, thread_id INTEGER NOT NULL REFERENCES investigation_threads(id),
 gap_key TEXT NOT NULL, input_revision TEXT NOT NULL, question TEXT NOT NULL,
 rounds INTEGER NOT NULL DEFAULT 0 CHECK(rounds BETWEEN 0 AND 3),
 status TEXT NOT NULL DEFAULT 'open', UNIQUE(thread_id,gap_key,input_revision)
);
CREATE INDEX civic_claims_revision ON civic_claims(source_revision_id,id);
CREATE INDEX civic_evidence_claim ON civic_evidence_links(claim_id,stance);
