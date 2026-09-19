CREATE TABLE search_documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    projection_generation INTEGER NOT NULL,
    subject_type TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    title TEXT,
    body TEXT,
    tags TEXT,
    entities TEXT,
    event_date TEXT,
    metadata_json TEXT,
    UNIQUE(projection_generation, subject_type, subject_key)
);

CREATE VIRTUAL TABLE search_documents_fts USING fts5(
    title,
    body,
    tags,
    entities,
    content='search_documents',
    content_rowid='id',
    tokenize='unicode61 remove_diacritics 2'
);

CREATE TRIGGER search_documents_ai AFTER INSERT ON search_documents BEGIN
    INSERT INTO search_documents_fts(rowid, title, body, tags, entities)
    VALUES (new.id, new.title, new.body, new.tags, new.entities);
END;
CREATE TRIGGER search_documents_ad AFTER DELETE ON search_documents BEGIN
    INSERT INTO search_documents_fts(search_documents_fts, rowid, title, body, tags, entities)
    VALUES('delete', old.id, old.title, old.body, old.tags, old.entities);
END;
CREATE TRIGGER search_documents_au AFTER UPDATE ON search_documents BEGIN
    INSERT INTO search_documents_fts(search_documents_fts, rowid, title, body, tags, entities)
    VALUES('delete', old.id, old.title, old.body, old.tags, old.entities);
    INSERT INTO search_documents_fts(rowid, title, body, tags, entities)
    VALUES (new.id, new.title, new.body, new.tags, new.entities);
END;

CREATE INDEX search_documents_subject ON search_documents(subject_type, subject_key);
CREATE INDEX search_documents_date ON search_documents(event_date);
