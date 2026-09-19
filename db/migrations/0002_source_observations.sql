CREATE TABLE source_observations (
 id INTEGER PRIMARY KEY, source_object_id INTEGER NOT NULL REFERENCES source_objects(id),
 source_revision_id INTEGER NOT NULL REFERENCES source_revisions(id),
 sequence_no INTEGER NOT NULL, observed_at TEXT, fetched_at TEXT NOT NULL,
 previous_observation_id INTEGER REFERENCES source_observations(id),
 UNIQUE(source_object_id, sequence_no)
);
CREATE INDEX source_observations_object ON source_observations(source_object_id, sequence_no);
