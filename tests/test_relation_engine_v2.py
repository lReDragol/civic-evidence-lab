from __future__ import annotations

import json
import sqlite3
import unittest

from db.migration_runner import apply_pending_migrations
from knowledge.projection import mark_generation_validated, start_generation
from knowledge.relation_engine import rebuild_relation_assertions, record_relation_signal


BASE_SCHEMA = """
CREATE TABLE sources(
    id INTEGER PRIMARY KEY, name TEXT, category TEXT, url TEXT, is_official INTEGER DEFAULT 0,
    credibility_tier TEXT DEFAULT 'C', owner TEXT
);
CREATE TABLE raw_source_items(id INTEGER PRIMARY KEY, source_id INTEGER, external_id TEXT);
CREATE TABLE content_items(
    id INTEGER PRIMARY KEY, source_id INTEGER, raw_item_id INTEGER, content_type TEXT,
    title TEXT, body_text TEXT, url TEXT
);
CREATE TABLE entities(id INTEGER PRIMARY KEY, entity_type TEXT, canonical_name TEXT);
CREATE TABLE events(id INTEGER PRIMARY KEY, canonical_title TEXT, superseded_at TEXT);
CREATE TABLE event_facts(
    id INTEGER PRIMARY KEY, event_id INTEGER, fact_type TEXT, canonical_text TEXT,
    polarity TEXT, valid_from TEXT, valid_to TEXT, observed_at TEXT, confidence REAL,
    metadata_json TEXT, superseded_at TEXT
);
CREATE TABLE event_entities(
    id INTEGER PRIMARY KEY, event_id INTEGER, entity_id INTEGER, role TEXT, superseded_at TEXT
);
CREATE TABLE fact_evidence(
    id INTEGER PRIMARY KEY, fact_id INTEGER, content_item_id INTEGER,
    document_content_id INTEGER, evidence_type TEXT, evidence_class TEXT,
    source_strength TEXT, metadata_json TEXT, superseded_at TEXT
);
CREATE TABLE entity_relations(id INTEGER PRIMARY KEY);
"""


class RelationEngineV2Tests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(BASE_SCHEMA)
        apply_pending_migrations(self.conn)
        self.conn.executemany(
            "INSERT INTO entities(id,entity_type,canonical_name) VALUES(?,?,?)",
            [
                (1, "organization", "Regulator"),
                (2, "organization", "Company"),
                (3, "person", "Unrelated participant"),
                (4, "location", "Moscow"),
            ],
        )
        self.conn.execute("INSERT INTO events(id,canonical_title) VALUES(10,'Restriction event')")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _source_evidence(
        self,
        *,
        source_id: int,
        content_id: int,
        evidence_id: int,
        owner: str,
        official: bool,
        tier: str,
        entailment: float,
        origin_key: str | None = None,
    ) -> None:
        category = "official_registry" if official else "media"
        content_type = "official_document" if official else "article"
        self.conn.execute(
            """
            INSERT INTO sources(id,name,category,url,is_official,credibility_tier,owner)
            VALUES(?,?,?,?,?,?,?)
            """,
            (source_id, owner, category, f"https://{owner}/", int(official), "A", owner),
        )
        self.conn.execute(
            "INSERT INTO content_items(id,source_id,content_type,title,url) VALUES(?,?,?,?,?)",
            (content_id, source_id, content_type, "Evidence", f"https://{owner}/doc"),
        )
        self.conn.execute(
            """
            INSERT INTO source_objects(id,source_id,external_id) VALUES(?,?,?)
            """,
            (source_id, source_id, f"doc:{content_id}"),
        )
        self.conn.execute(
            """
            INSERT INTO source_revisions(
                id,source_object_id,content_item_id,revision_no,payload_hash,is_current
            ) VALUES(?,?,?,?,?,1)
            """,
            (content_id, source_id, content_id, 1, f"hash-{content_id}"),
        )
        self.conn.execute(
            """
            INSERT INTO fact_evidence(
                id,fact_id,content_item_id,evidence_type,evidence_class,source_strength,metadata_json
            ) VALUES(?,100,?,'document','hard','strong',?)
            """,
            (
                evidence_id,
                content_id,
                json.dumps(
                    {
                        "authenticity_verdict": "confirmed",
                        "entailment_score": entailment,
                        "evidence_tier": tier,
                        "evidence_quote": "The regulator restricted the company.",
                        "independence_verified": origin_key is not None,
                        "origin_key": origin_key,
                    }
                ),
            ),
        )

    def _bind_arguments_to_revision(self, revision_id: int, *, copy: bool = False) -> None:
        rows = self.conn.execute(
            "SELECT id,entity_id,argument_role,metadata_json FROM fact_arguments WHERE fact_id=100"
        ).fetchall()
        for argument_id, entity_id, role, raw_metadata in rows:
            metadata = {**json.loads(raw_metadata), "source_revision_id": revision_id}
            if copy:
                self.conn.execute(
                    """INSERT INTO fact_arguments(fact_id,entity_id,argument_role,content_item_id,
                           evidence_locator,confidence,inference_method,metadata_json)
                       VALUES(100,?,?,?,'sentence:1',0.99,'explicit_extract',?)""",
                    (entity_id, role, revision_id, json.dumps(metadata)),
                )
            else:
                self.conn.execute(
                    "UPDATE fact_arguments SET content_item_id=?,metadata_json=? WHERE id=?",
                    (revision_id, json.dumps(metadata), argument_id),
                )

    def _fact_and_arguments(self, *, polarity: str = "positive", object_entity: int = 2) -> None:
        self.conn.execute(
            """
            INSERT INTO event_facts(
                id,event_id,fact_type,canonical_text,polarity,valid_from,valid_to,
                observed_at,confidence,metadata_json
            ) VALUES(100,10,'restriction','Regulator restricted Company',?,
                     '2026-01-01','2026-06-01','2026-01-02',0.98,'{}')
            """,
            (polarity,),
        )
        self.conn.executemany(
            """
            INSERT INTO fact_arguments(
                fact_id,entity_id,argument_role,content_item_id,evidence_locator,
                confidence,inference_method,metadata_json
            ) VALUES(100,?,?,50,'sentence:1',0.99,'explicit_extract',?)
            """,
            [
                (1, "regulator", json.dumps({"argument_group": "restriction:1"})),
                (object_entity, "target", json.dumps({"argument_group": "restriction:1"})),
            ],
        )
        # Event participants are deliberately broader than the explicit fact arguments.
        self.conn.execute(
            "INSERT INTO event_entities(id,event_id,entity_id,role) VALUES(1,10,3,'commentator')"
        )

    def test_official_e3_promotes_only_explicit_pair_and_preserves_time(self):
        self._fact_and_arguments()
        self._source_evidence(
            source_id=11,
            content_id=201,
            evidence_id=301,
            owner="regulator.gov.ru",
            official=True,
            tier="E3",
            entailment=0.97,
        )

        self._bind_arguments_to_revision(201)
        result = rebuild_relation_assertions(self.conn, generation_key="relations:test:official")
        row = self.conn.execute(
            """
            SELECT subject_entity_id,predicate,object_entity_id,polarity,state,
                   valid_from,valid_to,promotion_reason
            FROM current_relation_assertions_v
            """
        ).fetchone()

        self.assertEqual(1, result["assertions"])
        self.assertEqual(
            (1, "restricts", 2, "positive", "promoted", "2026-01-01", "2026-06-01", "single_primary_E3"),
            row,
        )
        self.assertEqual(
            (301, 201, 201, "E3"),
            self.conn.execute(
                """
                SELECT fact_evidence_id,content_item_id,source_revision_id,evidence_tier
                FROM relation_assertion_evidence
                """
            ).fetchone(),
        )
        self.assertEqual(
            0,
            self.conn.execute(
                "SELECT COUNT(*) FROM relation_assertions WHERE subject_entity_id=3 OR object_entity_id=3"
            ).fetchone()[0],
        )

    def test_negative_fact_remains_negative_instead_of_inverting_relation(self):
        self._fact_and_arguments(polarity="negative")
        self._source_evidence(
            source_id=12,
            content_id=202,
            evidence_id=302,
            owner="court.gov.ru",
            official=True,
            tier="E3",
            entailment=0.96,
        )
        self._bind_arguments_to_revision(202)
        rebuild_relation_assertions(self.conn, generation_key="relations:test:negative")

        self.assertEqual(
            (1, "restricts", 2, "negative", "promoted"),
            self.conn.execute(
                """
                SELECT subject_entity_id,predicate,object_entity_id,polarity,state
                FROM current_relation_assertions_v
                """
            ).fetchone(),
        )

    def test_location_argument_never_becomes_entity_relation(self):
        self._fact_and_arguments(object_entity=4)
        self._source_evidence(
            source_id=13,
            content_id=203,
            evidence_id=303,
            owner="regulator.gov.ru",
            official=True,
            tier="E3",
            entailment=0.99,
        )
        result = rebuild_relation_assertions(self.conn, generation_key="relations:test:location")

        self.assertEqual(0, result["assertions"])
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM relation_assertions").fetchone()[0])

    def test_same_case_signal_cannot_create_assertion(self):
        generation_id = start_generation(
            self.conn, projection_type="signals", generation_key="signals:test:same-case"
        )
        mark_generation_validated(self.conn, generation_id, metrics={"signals": 1})
        self.conn.execute(
            "UPDATE projection_generations SET status='active' WHERE id=?", (generation_id,)
        )
        self.conn.commit()
        record_relation_signal(
            self.conn,
            generation_id=generation_id,
            entity_a_id=1,
            entity_b_id=2,
            signal_type="same_case_cluster",
            source_unit_key="case:99",
            signal_score=1.0,
        )

        result = rebuild_relation_assertions(self.conn, generation_key="relations:test:signal-only")

        self.assertEqual(0, result["assertions"])
        self.assertEqual(1, self.conn.execute("SELECT COUNT(*) FROM relation_signals").fetchone()[0])

    def test_two_reviewed_independent_e2_origins_promote_and_generation_switch_is_atomic(self):
        self._fact_and_arguments()
        self._source_evidence(
            source_id=21,
            content_id=211,
            evidence_id=311,
            owner="media-one.ru",
            official=False,
            tier="E2",
            entailment=0.86,
            origin_key="reviewed-witness:one",
        )
        self._source_evidence(
            source_id=22,
            content_id=212,
            evidence_id=312,
            owner="media-two.ru",
            official=False,
            tier="E2",
            entailment=0.84,
            origin_key="reviewed-witness:two",
        )
        self._bind_arguments_to_revision(211)
        self._bind_arguments_to_revision(212, copy=True)
        first = rebuild_relation_assertions(self.conn, generation_key="relations:test:e2:first")
        second = rebuild_relation_assertions(self.conn, generation_key="relations:test:e2:second")

        self.assertEqual(1, first["promoted"])
        self.assertEqual(1, second["promoted"])
        self.assertEqual(
            [(first["generation_id"], "superseded"), (second["generation_id"], "active")],
            self.conn.execute(
                """
                SELECT id,status FROM projection_generations
                WHERE projection_type='relations' ORDER BY id
                """
            ).fetchall(),
        )
        self.assertEqual(
            "two_independent_E2_plus",
            self.conn.execute(
                "SELECT promotion_reason FROM current_relation_assertions_v"
            ).fetchone()[0],
        )

    def test_ambiguous_explicit_arguments_are_not_cartesian_expanded(self):
        self._fact_and_arguments()
        self.conn.execute(
            """
            INSERT INTO fact_arguments(
                fact_id,entity_id,argument_role,content_item_id,evidence_locator,
                confidence,inference_method,metadata_json
            ) VALUES(100,3,'target',50,'sentence:1',0.9,'explicit_extract',?)
            """,
            (json.dumps({"argument_group": "restriction:1"}),),
        )
        self._source_evidence(
            source_id=14,
            content_id=204,
            evidence_id=304,
            owner="regulator.gov.ru",
            official=True,
            tier="E3",
            entailment=0.99,
        )

        result = rebuild_relation_assertions(self.conn, generation_key="relations:test:ambiguous")

        self.assertEqual(0, result["assertions"])


if __name__ == "__main__":
    unittest.main()
