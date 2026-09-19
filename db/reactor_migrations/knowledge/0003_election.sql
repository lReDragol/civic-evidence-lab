-- Election evidence is separate from Reactor facts and relation assertions.
CREATE TABLE election_campaigns (
    id INTEGER PRIMARY KEY,
    campaign_key TEXT NOT NULL UNIQUE CHECK(length(trim(campaign_key)) > 0),
    title TEXT NOT NULL CHECK(length(trim(title)) > 0)
);

CREATE TABLE election_ballots (
    id INTEGER PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES election_campaigns(id),
    ballot_key TEXT NOT NULL CHECK(length(trim(ballot_key)) > 0),
    title TEXT NOT NULL CHECK(length(trim(title)) > 0),
    UNIQUE(campaign_id, ballot_key),
    UNIQUE(id, campaign_id)
);

CREATE TABLE election_precincts (
    id INTEGER PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES election_campaigns(id),
    jurisdiction TEXT NOT NULL CHECK(length(trim(jurisdiction)) > 0),
    official_id TEXT NOT NULL CHECK(length(trim(official_id)) > 0),
    category TEXT NOT NULL CHECK(category IN ('uik','deg','overseas','other')),
    UNIQUE(campaign_id, jurisdiction, official_id, category),
    UNIQUE(id, campaign_id)
);

CREATE TABLE election_ballot_scopes (
    id INTEGER PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES election_campaigns(id),
    ballot_id INTEGER NOT NULL,
    precinct_id INTEGER NOT NULL,
    FOREIGN KEY(ballot_id, campaign_id) REFERENCES election_ballots(id, campaign_id),
    FOREIGN KEY(precinct_id, campaign_id) REFERENCES election_precincts(id, campaign_id),
    UNIQUE(ballot_id, precinct_id)
);

CREATE TABLE election_protocol_versions (
    id INTEGER PRIMARY KEY,
    scope_id INTEGER NOT NULL REFERENCES election_ballot_scopes(id),
    protocol_type TEXT NOT NULL CHECK(protocol_type IN ('official','observed')),
    version_no INTEGER NOT NULL CHECK(typeof(version_no)='integer' AND version_no > 0),
    source_revision_id INTEGER NOT NULL REFERENCES source_revisions(id),
    provenance_group TEXT NOT NULL CHECK(length(trim(provenance_group)) > 0),
    document_locator TEXT NOT NULL CHECK(length(trim(document_locator)) > 0),
    validation_state TEXT NOT NULL CHECK(validation_state IN ('valid','incomplete','invalid')),
    validation_json TEXT NOT NULL,
    sealed INTEGER NOT NULL DEFAULT 0 CHECK(sealed IN (0,1)),
    recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
    reported_at TEXT CHECK(reported_at IS NULL OR length(trim(reported_at)) > 0),
    fetched_at TEXT CHECK(fetched_at IS NULL OR length(trim(fetched_at)) > 0),
    timezone TEXT CHECK(timezone IS NULL OR length(trim(timezone)) > 0),
    publication_status TEXT NOT NULL DEFAULT 'unknown'
        CHECK(publication_status IN ('unknown','preliminary','final')),
    UNIQUE(scope_id, protocol_type, version_no),
    UNIQUE(id, scope_id, protocol_type)
);

CREATE TABLE election_protocol_numbers (
    protocol_id INTEGER NOT NULL REFERENCES election_protocol_versions(id),
    field_key TEXT NOT NULL CHECK(length(trim(field_key)) > 0),
    value INTEGER CHECK(value IS NULL OR (typeof(value)='integer' AND value >= 0)),
    source_revision_id INTEGER REFERENCES source_revisions(id),
    locator TEXT,
    verification_state TEXT NOT NULL
        CHECK(verification_state IN ('missing','unverified','verified','disputed')),
    verified_by TEXT,
    PRIMARY KEY(protocol_id, field_key),
    CHECK((value IS NULL AND verification_state='missing') OR
          (value IS NOT NULL AND verification_state<>'missing'
           AND source_revision_id IS NOT NULL AND locator IS NOT NULL
           AND length(trim(locator)) > 0)),
    CHECK(verification_state<>'verified' OR
          (verified_by IS NOT NULL AND length(trim(verified_by)) > 0))
);

CREATE TABLE election_accepted_protocols (
    scope_id INTEGER NOT NULL,
    protocol_type TEXT NOT NULL,
    protocol_id INTEGER NOT NULL,
    reviewer TEXT NOT NULL CHECK(length(trim(reviewer)) > 0),
    reason TEXT NOT NULL CHECK(length(trim(reason)) > 0),
    accepted_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY(scope_id, protocol_type),
    FOREIGN KEY(protocol_id, scope_id, protocol_type)
        REFERENCES election_protocol_versions(id, scope_id, protocol_type)
);

CREATE TABLE election_acceptance_history (
    id INTEGER PRIMARY KEY,
    scope_id INTEGER NOT NULL REFERENCES election_ballot_scopes(id),
    protocol_type TEXT NOT NULL,
    protocol_id INTEGER NOT NULL REFERENCES election_protocol_versions(id),
    previous_protocol_id INTEGER REFERENCES election_protocol_versions(id),
    reviewer TEXT NOT NULL,
    reason TEXT NOT NULL,
    recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TRIGGER election_protocol_immutable_update
BEFORE UPDATE ON election_protocol_versions WHEN OLD.sealed=1
BEGIN SELECT RAISE(ABORT, 'sealed protocol is immutable'); END;
CREATE TRIGGER election_protocol_immutable_delete
BEFORE DELETE ON election_protocol_versions WHEN OLD.sealed=1
BEGIN SELECT RAISE(ABORT, 'sealed protocol is immutable'); END;
CREATE TRIGGER election_number_no_insert
BEFORE INSERT ON election_protocol_numbers
WHEN (SELECT sealed FROM election_protocol_versions WHERE id=NEW.protocol_id)=1
BEGIN SELECT RAISE(ABORT, 'sealed protocol numbers are immutable'); END;
CREATE TRIGGER election_number_no_update
BEFORE UPDATE ON election_protocol_numbers
WHEN (SELECT sealed FROM election_protocol_versions WHERE id=OLD.protocol_id)=1
  OR (SELECT sealed FROM election_protocol_versions WHERE id=NEW.protocol_id)=1
BEGIN SELECT RAISE(ABORT, 'sealed protocol numbers are immutable'); END;
CREATE TRIGGER election_number_no_delete
BEFORE DELETE ON election_protocol_numbers
WHEN (SELECT sealed FROM election_protocol_versions WHERE id=OLD.protocol_id)=1
BEGIN SELECT RAISE(ABORT, 'sealed protocol numbers are immutable'); END;

CREATE VIEW election_acceptable_protocols_v AS
SELECT p.* FROM election_protocol_versions p
WHERE p.sealed=1 AND p.validation_state='valid'
AND 4 = (SELECT COUNT(*) FROM election_protocol_numbers n
         WHERE n.protocol_id=p.id AND n.value IS NOT NULL
           AND n.verification_state='verified'
           AND n.field_key IN ('registered_voters','ballots_cast','valid_ballots','invalid_ballots'))
AND NOT EXISTS (SELECT 1 FROM election_protocol_numbers n
                WHERE n.protocol_id=p.id AND (n.value IS NULL OR n.verification_state<>'verified'))
AND NOT EXISTS (
    SELECT 1 FROM election_protocol_numbers c
    JOIN election_protocol_numbers v ON v.protocol_id=c.protocol_id AND v.field_key='valid_ballots'
    JOIN election_protocol_numbers i ON i.protocol_id=c.protocol_id AND i.field_key='invalid_ballots'
    WHERE c.protocol_id=p.id AND c.field_key='ballots_cast' AND c.value<>v.value+i.value
);

CREATE TRIGGER election_accept_insert_guard BEFORE INSERT ON election_accepted_protocols
WHEN NOT EXISTS (SELECT 1 FROM election_acceptable_protocols_v WHERE id=NEW.protocol_id)
BEGIN SELECT RAISE(ABORT, 'protocol is not acceptable'); END;
CREATE TRIGGER election_accept_update_guard BEFORE UPDATE ON election_accepted_protocols
WHEN NOT EXISTS (SELECT 1 FROM election_acceptable_protocols_v WHERE id=NEW.protocol_id)
BEGIN SELECT RAISE(ABORT, 'protocol is not acceptable'); END;
CREATE TRIGGER election_accept_insert_history AFTER INSERT ON election_accepted_protocols
BEGIN
    INSERT INTO election_acceptance_history(scope_id,protocol_type,protocol_id,reviewer,reason)
    VALUES(NEW.scope_id,NEW.protocol_type,NEW.protocol_id,NEW.reviewer,NEW.reason);
END;
CREATE TRIGGER election_accept_update_history AFTER UPDATE ON election_accepted_protocols
BEGIN
    INSERT INTO election_acceptance_history(
        scope_id,protocol_type,protocol_id,previous_protocol_id,reviewer,reason)
    VALUES(NEW.scope_id,NEW.protocol_type,NEW.protocol_id,OLD.protocol_id,NEW.reviewer,NEW.reason);
END;

CREATE TABLE election_incidents (
    id INTEGER PRIMARY KEY,
    scope_id INTEGER NOT NULL REFERENCES election_ballot_scopes(id),
    candidate_key TEXT NOT NULL CHECK(length(trim(candidate_key)) > 0),
    kind TEXT NOT NULL CHECK(length(trim(kind)) > 0),
    description TEXT NOT NULL CHECK(length(trim(description)) > 0),
    basis_json TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'candidate' CHECK(state IN ('candidate','confirmed','dismissed')),
    reviewer TEXT,
    review_reason TEXT,
    review_evidence_id INTEGER REFERENCES evidence_items(id),
    recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(scope_id, candidate_key),
    CHECK(state='candidate' OR (reviewer IS NOT NULL AND length(trim(reviewer)) > 0
          AND review_reason IS NOT NULL AND length(trim(review_reason)) > 0)),
    CHECK(state<>'confirmed' OR review_evidence_id IS NOT NULL)
);

CREATE TABLE election_claims (
    id INTEGER PRIMARY KEY,
    scope_id INTEGER NOT NULL REFERENCES election_ballot_scopes(id),
    incident_id INTEGER REFERENCES election_incidents(id),
    source_revision_id INTEGER NOT NULL REFERENCES source_revisions(id),
    locator TEXT NOT NULL CHECK(length(trim(locator)) > 0),
    attributed_to TEXT NOT NULL CHECK(length(trim(attributed_to)) > 0),
    subject_entity_id INTEGER REFERENCES entities(id),
    stance TEXT NOT NULL CHECK(stance IN ('alleges','denies','reports','uncertain')),
    claim_text TEXT NOT NULL CHECK(length(trim(claim_text)) > 0),
    recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TRIGGER election_claim_scope_insert BEFORE INSERT ON election_claims
WHEN NEW.incident_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM election_incidents WHERE id=NEW.incident_id AND scope_id=NEW.scope_id)
BEGIN SELECT RAISE(ABORT, 'claim incident scope mismatch'); END;
CREATE TRIGGER election_claim_scope_update BEFORE UPDATE ON election_claims
WHEN NEW.incident_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM election_incidents WHERE id=NEW.incident_id AND scope_id=NEW.scope_id)
BEGIN SELECT RAISE(ABORT, 'claim incident scope mismatch'); END;

CREATE VIEW election_incident_counts_v AS
SELECT s.id AS scope_id,
       COUNT(CASE WHEN i.state='candidate' THEN 1 END) AS candidate_count,
       COUNT(CASE WHEN i.state='confirmed' THEN 1 END) AS confirmed_count,
       COUNT(CASE WHEN i.state='dismissed' THEN 1 END) AS dismissed_count
FROM election_ballot_scopes s LEFT JOIN election_incidents i ON i.scope_id=s.id
GROUP BY s.id;
CREATE INDEX election_claims_scope ON election_claims(scope_id, incident_id);
