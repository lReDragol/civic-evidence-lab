from __future__ import annotations

import sqlite3
import unittest

from ui.query_service import UIQueryService


SCHEMA = """
CREATE TABLE sources(
    id INTEGER PRIMARY KEY,
    name TEXT,
    category TEXT,
    credibility_tier TEXT
);
CREATE TABLE content_items(
    id INTEGER PRIMARY KEY,
    source_id INTEGER,
    external_id TEXT,
    content_type TEXT,
    title TEXT,
    body_text TEXT,
    published_at TEXT,
    collected_at TEXT,
    url TEXT,
    language TEXT,
    status TEXT
);
CREATE TABLE attachments(
    id INTEGER PRIMARY KEY,
    content_item_id INTEGER,
    file_path TEXT,
    attachment_type TEXT,
    hash_sha256 TEXT,
    ocr_text TEXT
);
CREATE TABLE entities(
    id INTEGER PRIMARY KEY,
    entity_type TEXT,
    canonical_name TEXT,
    inn TEXT,
    ogrn TEXT,
    description TEXT
);
CREATE TABLE entity_aliases(id INTEGER PRIMARY KEY, entity_id INTEGER, alias TEXT, alias_type TEXT);
CREATE TABLE entity_mentions(
    id INTEGER PRIMARY KEY,
    entity_id INTEGER,
    content_item_id INTEGER,
    mention_type TEXT,
    confidence REAL
);
CREATE TABLE claims(id INTEGER PRIMARY KEY, content_item_id INTEGER, claim_text TEXT);
CREATE TABLE events(
    id INTEGER PRIMARY KEY,
    canonical_title TEXT,
    event_type TEXT,
    summary_short TEXT,
    summary_long TEXT,
    status TEXT,
    event_date_start TEXT,
    event_date_end TEXT,
    importance_score REAL,
    confidence REAL,
    superseded_at TEXT
);
CREATE TABLE event_timeline(
    id INTEGER PRIMARY KEY, event_id INTEGER, timeline_date TEXT, title TEXT,
    description TEXT, sort_order INTEGER, superseded_at TEXT
);
CREATE TABLE event_entities(
    id INTEGER PRIMARY KEY, event_id INTEGER, entity_id INTEGER, role TEXT,
    confidence REAL, superseded_at TEXT
);
CREATE TABLE event_facts(
    id INTEGER PRIMARY KEY, event_id INTEGER, fact_type TEXT, canonical_text TEXT,
    superseded_at TEXT
);
CREATE TABLE event_items(
    id INTEGER PRIMARY KEY, event_id INTEGER, content_item_id INTEGER,
    item_role TEXT, superseded_at TEXT
);
CREATE TABLE entity_relations(
    id INTEGER PRIMARY KEY,
    from_entity_id INTEGER,
    to_entity_id INTEGER,
    relation_type TEXT,
    strength TEXT,
    detected_by TEXT,
    evidence_item_id INTEGER,
    valid_from TEXT,
    valid_to TEXT,
    observed_at TEXT,
    superseded_at TEXT
);
CREATE TABLE review_tasks(
    id INTEGER PRIMARY KEY,
    task_key TEXT,
    queue_key TEXT,
    subject_type TEXT,
    subject_id INTEGER,
    related_id INTEGER,
    candidate_payload TEXT,
    suggested_action TEXT,
    confidence REAL,
    machine_reason TEXT,
    source_links_json TEXT,
    status TEXT,
    review_pack_id TEXT,
    created_at TEXT,
    updated_at TEXT
);
"""


class UIQueryServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(SCHEMA)
        self.conn.execute("INSERT INTO sources VALUES(1,'Archive','news','B')")
        self.conn.executemany(
            """
            INSERT INTO content_items(
                id, source_id, external_id, content_type, title, body_text,
                published_at, collected_at, url, language, status
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (
                    row_id,
                    1,
                    f"post-{row_id}",
                    "article",
                    "Very old exact investigation" if row_id == 5 else f"Newest item {row_id}",
                    "archived evidence" if row_id == 5 else "ordinary text",
                    f"2026-01-{(row_id % 28) + 1:02d}",
                    "2026-02-01",
                    f"https://example.test/{row_id}",
                    "ru",
                    "active",
                )
                for row_id in range(1, 301)
            ],
        )
        self.conn.executemany(
            "INSERT INTO entities VALUES(?,?,?,?,?,?)",
            [
                (1, "person", "Alpha", None, None, "first"),
                (2, "organization", "Beta", None, None, "second"),
                (3, "person", "Старый Свидетель", None, None, "архив"),
            ],
        )
        self.conn.execute(
            "INSERT INTO entity_aliases VALUES(1,3,'Old Witness','spelling')"
        )
        self.conn.executemany(
            "INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            [
                (1, "Old event", "decision", "archive anchor", "details", "active", "2024-01-01", None, 1, 0.9, None),
                (2, "New event", "statement", "recent", "details", "active", "2026-01-01", None, 2, 0.8, None),
            ],
        )
        self.conn.execute(
            "INSERT INTO entity_relations VALUES(1,1,2,'issued', 'strong','test',5,NULL,NULL,NULL,NULL)"
        )
        self.conn.executemany(
            "INSERT INTO review_tasks VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (1, "old-task", "documents", "content_item", 5, None, "{}", "verify", 0.9, "rare archive reason", "[]", "open", None, "2024-01-01", "2024-01-01"),
                (2, "new-task", "relations", "relation", 1, None, "{}", "reject", 0.5, "recent", "[]", "open", None, "2026-01-01", "2026-01-01"),
            ],
        )
        self.service = UIQueryService(self.conn, default_limit=20, max_limit=100)

    def tearDown(self) -> None:
        self.conn.close()

    def test_filter_runs_before_limit_and_finds_old_content(self) -> None:
        page = self.service.query_content(query="exact investigation", limit=10)

        self.assertEqual(page["total"], 1)
        self.assertEqual([item["id"] for item in page["items"]], [5])
        self.assertEqual(page["items"][0]["source_name"], "Archive")

    def test_cursor_pagination_is_stable_when_newer_rows_are_inserted(self) -> None:
        first = self.service.query_content(limit=25)
        first_ids = [item["id"] for item in first["items"]]
        self.assertEqual(first_ids, list(range(300, 275, -1)))

        self.conn.execute(
            """
            INSERT INTO content_items VALUES(
                301,1,'post-301','article','Inserted later','text','2026-08-01',
                '2026-08-01','https://example.test/301','ru','active'
            )
            """
        )
        second = self.service.query_content(limit=25, cursor=first["next_cursor"])

        self.assertEqual([item["id"] for item in second["items"]], list(range(275, 250, -1)))
        self.assertFalse(set(first_ids) & {item["id"] for item in second["items"]})

    def test_offset_pagination_and_facets_are_server_side(self) -> None:
        page = self.service.query_content(limit=3, offset=4, status="active")

        self.assertEqual([item["id"] for item in page["items"]], [296, 295, 294])
        self.assertEqual(page["total"], 300)
        self.assertEqual(page["facets"]["status"], [{"value": "active", "count": 300}])

    def test_all_resources_support_filtered_lists_and_lazy_details(self) -> None:
        self.conn.execute(
            "INSERT INTO attachments VALUES(1,5,'doc.png','image','abc','Роскомнадзор 152-ФЗ')"
        )

        self.assertEqual(self.service.query_events(query="archive")["items"][0]["id"], 1)
        self.assertEqual(self.service.query_entities(query="old witness")["items"][0]["id"], 3)
        self.assertEqual(self.service.query_documents(query="152-ФЗ")["items"][0]["id"], 5)
        self.assertEqual(self.service.query_relations(query="Alpha")["items"][0]["id"], 1)
        self.assertEqual(self.service.query_review_queues(query="rare archive")["items"][0]["id"], 1)

        self.assertEqual(self.service.content_detail(5)["attachments"][0]["id"], 1)
        self.assertEqual(self.service.document_detail(5)["document_reviews"][0]["id"], 1)
        self.assertEqual(self.service.event_detail(1)["canonical_title"], "Old event")
        self.assertEqual(self.service.entity_detail(3)["canonical_name"], "Старый Свидетель")
        self.assertEqual(self.service.relation_detail(1)["relation_type"], "issued")
        self.assertEqual(self.service.review_detail(1)["candidate_payload_json"], {})

    def test_active_projection_views_take_precedence_over_legacy_tables(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE projection_generations(
                id INTEGER PRIMARY KEY, projection_type TEXT, status TEXT
            );
            CREATE TABLE relation_assertions(
                id INTEGER PRIMARY KEY,
                generation_id INTEGER,
                subject_entity_id INTEGER,
                object_entity_id INTEGER,
                predicate TEXT,
                confidence REAL,
                state TEXT,
                polarity TEXT,
                event_id INTEGER,
                fact_id INTEGER,
                valid_from TEXT,
                valid_to TEXT,
                observed_at TEXT
            );
            INSERT INTO projection_generations VALUES(1,'relations','active');
            INSERT INTO relation_assertions VALUES(
                10,1,2,3,'verified_by',0.97,'promoted','positive',1,NULL,NULL,NULL,'2026-01-01'
            );
            CREATE VIEW current_relation_assertions_v AS
            SELECT r.* FROM relation_assertions r
            JOIN projection_generations p ON p.id=r.generation_id
            WHERE p.projection_type='relations' AND p.status='active' AND r.state='promoted';
            CREATE VIEW current_events_v AS SELECT * FROM events WHERE id=1;
            """
        )

        relations = self.service.query_relations()
        events = self.service.query_events()

        self.assertEqual(relations["source"], "current_relation_assertions_v")
        self.assertEqual([item["id"] for item in relations["items"]], [10])
        self.assertEqual(events["source"], "current_events_v")
        self.assertEqual([item["id"] for item in events["items"]], [1])

    def test_search_wildcards_are_literals_and_values_are_parameterized(self) -> None:
        literal = self.service.query_content(query="100%_missing")
        injection = self.service.query_content(query="' OR 1=1 --")

        self.assertEqual(literal["total"], 0)
        self.assertEqual(injection["total"], 0)

    def test_invalid_or_cross_resource_cursor_is_rejected(self) -> None:
        cursor = self.service.query_content(limit=2)["next_cursor"]
        with self.assertRaises(ValueError):
            self.service.query_events(cursor=cursor)
        with self.assertRaises(ValueError):
            self.service.query_content(cursor="not-base64")


if __name__ == "__main__":
    unittest.main()
