CREATE TABLE search_heads (
 subject_type TEXT NOT NULL, subject_key TEXT NOT NULL, observation_id INTEGER NOT NULL,
 document_id INTEGER NOT NULL REFERENCES search_documents(id),
 PRIMARY KEY(subject_type,subject_key)
);
CREATE VIEW current_search_documents_v AS
 SELECT d.* FROM search_heads h JOIN search_documents d ON d.id=h.document_id;
