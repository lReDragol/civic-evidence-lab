from __future__ import annotations

import json
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from db.migration_runner import apply_pending_migrations
from graph import relation_candidates as candidates
from knowledge import relation_engine as engine
from tests import test_relation_engine_v2 as legacy


ROOT = Path(__file__).resolve().parents[1]


class ReactorRelationSafetyTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        self.conn.execute("PRAGMA foreign_keys=ON")
        apply_pending_migrations(
            self.conn, migrations_dir=ROOT / "db/reactor_migrations/knowledge"
        )
        self.conn.executescript("""
            INSERT INTO projection_generations(id,projection_type,generation_key,status,started_at)
            VALUES(1,'events','events:fixture','active','2026-01-01'),
                  (2,'facts','facts:fixture','active','2026-01-01');
            INSERT INTO entities(id,entity_type,canonical_name,canonical_key)
            VALUES(1,'organization','Regulator','regulator'),
                  (2,'organization','Company','company'),
                  (3,'organization','Other','other');
            INSERT INTO events(id,generation_id,event_key,event_type,canonical_title,recorded_at)
            VALUES(1,1,'event','restriction','Restriction','2026-01-01');
            INSERT INTO facts(id,generation_id,event_id,fact_key,fact_type,predicate,
                              canonical_text,confidence,recorded_at,valid_from,valid_to)
            VALUES(1,2,1,'fact','custom_import_type','restricts','Regulator restricts Company',
                   0.99,'2026-01-01','2026-01-01','2026-06-01');
        """)
        self.add_evidence(1)
        self.add_arguments(1)
        self.conn.commit()

    def add_evidence(self, key, *, tier="E3", owner=None, metadata=None):
        self.conn.execute(
            "INSERT INTO source_systems(id,source_key,source_type,title,canonical_url,policy_json) VALUES(?,?,'media',?,?,?)",
            (key, f"source:{key}", f"Source {key}", f"https://domain-{key}.test", '{"official":true}'),
        )
        self.conn.execute(
            "INSERT INTO source_objects(id,source_system_id,external_id,first_seen_at,last_seen_at) VALUES(?,?,?,'2026-01-01','2026-01-01')",
            (key, key, f"object:{key}"),
        )
        self.conn.execute(
            "INSERT INTO source_revisions(id,source_object_id,revision_no,payload_hash,payload_json,fetched_at) VALUES(?,?,1,?,'{}','2026-01-01')",
            (key, key, f"hash:{key}"),
        )
        self.conn.execute(
            """INSERT INTO evidence_items(id,source_revision_id,evidence_key,evidence_type,
                   evidence_tier,source_owner,locator,entailment_score,authenticity_score,
                   verification_state,metadata_json,recorded_at)
               VALUES(?,?,?,'document',?,?,'sentence:1',0.99,0.99,'verified',?,'2026-01-01')""",
            (key, key, f"evidence:{key}", tier, owner, json.dumps(metadata or {})),
        )
        self.conn.execute("INSERT INTO fact_evidence VALUES(1,?,'support')", (key,))

    def add_arguments(self, revision, *, subject=1, obj=2, metadata=None):
        for entity, role in ((subject, "regulator"), (obj, "target")):
            if entity is not None:
                self.conn.execute(
                    """INSERT INTO fact_arguments(fact_id,entity_id,argument_role,source_revision_id,
                           evidence_locator,confidence,inference_method,metadata_json)
                       VALUES(1,?,?,?,'sentence:1',0.99,'explicit_extract',?)""",
                    (entity, role, revision, json.dumps(metadata or {})),
                )

    def rebuild(self, key="test"):
        self.conn.commit()
        return engine.rebuild_relation_assertions(self.conn, generation_key=key)

    def review_origins(self):
        for key in (1, 2):
            metadata = json.loads(self.conn.execute("SELECT metadata_json FROM evidence_items WHERE id=?", (key,)).fetchone()[0])
            metadata.update(independence_verified=True, origin_key=f"witness:{key}")
            self.conn.execute("UPDATE evidence_items SET metadata_json=? WHERE id=?", (json.dumps(metadata), key))

    def test_actual_schema_predicate_and_generation_round_trip(self):
        first = self.rebuild("first")
        second = self.rebuild("second")
        self.assertEqual((1, 1), (first["promoted"], second["promoted"]))
        self.assertEqual(
            [(1, "restricts", 2, "positive", "2026-01-01", "2026-06-01")],
            self.conn.execute("SELECT subject_entity_id,predicate,object_entity_id,polarity,valid_from,valid_to FROM current_relation_assertions_v").fetchall(),
        )
        self.assertEqual([], self.conn.execute("PRAGMA foreign_key_check").fetchall())
        self.assertEqual("superseded", self.conn.execute("SELECT status FROM projection_generations WHERE id=?", (first["generation_id"],)).fetchone()[0])

    def test_unverified_rejected_and_inauthentic_evidence_never_promotes(self):
        cases = [
            ("verification_state", value) for value in ("unverified", "rejected", "disputed", "unknown")
        ] + [("authenticity_score", value) for value in (0, 0.79)] + [
            ("metadata_json", json.dumps({"authenticity_verdict": "fake"})),
            ("metadata_json", json.dumps({"verification_state": "rejected"})),
            ("metadata_json", json.dumps({"stance": "refutes"})),
        ]
        for index, (column, value) in enumerate(cases):
            with self.subTest(column=column, value=value):
                self.conn.execute("UPDATE evidence_items SET verification_state='verified',authenticity_score=0.99,metadata_json='{}'")
                self.conn.execute(f"UPDATE evidence_items SET {column}=?", (value,))
                result = self.rebuild(f"unsafe:{index}")
                self.assertEqual(0, result["promoted"])
                self.assertEqual(("review", 0.0), self.conn.execute("SELECT state,confidence FROM relation_assertions WHERE generation_id=?", (result["generation_id"],)).fetchone())
                self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM relation_assertion_evidence e JOIN relation_assertions a ON a.id=e.assertion_id WHERE a.generation_id=?", (result["generation_id"],)).fetchone()[0])

    def test_refuting_links_do_not_support_either_polarity(self):
        for polarity in ("positive", "negative"):
            for stance in ("refute", "refutes", "refuting", "context", "contradicts"):
                with self.subTest(polarity=polarity, stance=stance):
                    self.conn.execute("UPDATE facts SET polarity=?", (polarity,))
                    self.conn.execute("UPDATE fact_evidence SET evidence_class=?", (stance,))
                    self.assertEqual(0, self.rebuild(f"{polarity}:{stance}")["promoted"])

    def test_negative_support_is_not_rewritten_positive(self):
        self.conn.execute("UPDATE facts SET polarity='negative'")
        self.assertEqual(1, self.rebuild()["promoted"])
        self.assertEqual("negative", self.conn.execute("SELECT polarity FROM current_relation_assertions_v").fetchone()[0])

    def test_refuting_evidence_is_retained_on_fact_not_attached_as_support(self):
        self.add_evidence(2)
        self.conn.execute("UPDATE fact_evidence SET evidence_class='refutes' WHERE evidence_item_id=2")
        self.assertEqual(1, self.rebuild()["promoted"])
        self.assertEqual([(1,)], self.conn.execute("SELECT evidence_item_id FROM relation_assertion_evidence").fetchall())
        self.assertEqual(2, self.conn.execute("SELECT COUNT(*) FROM fact_evidence").fetchone()[0])

    def test_revision_scope_applies_to_sentence_and_explicit_group_keys(self):
        self.add_evidence(2)
        for metadata in ({}, {"argument_group": "same"}, {"relation_key": "same"}):
            with self.subTest(metadata=metadata):
                self.conn.execute("DELETE FROM fact_arguments")
                self.add_arguments(1, obj=None, metadata=metadata)
                self.add_arguments(2, subject=None, metadata=metadata)
                self.assertEqual(0, self.rebuild(str(metadata))["assertions"])

    def test_complete_repeated_pairs_across_revisions_are_deduplicated(self):
        self.add_evidence(2)
        self.add_arguments(2)
        self.assertEqual(1, self.rebuild()["assertions"])

    def test_two_e2_require_explicit_distinct_owners_not_domains(self):
        self.conn.execute("UPDATE evidence_items SET evidence_tier='E2'")
        self.add_evidence(2, tier="E2")
        self.add_arguments(2)
        self.assertEqual(0, self.rebuild("domains")["promoted"])
        self.conn.execute("UPDATE evidence_items SET source_owner='same owner'")
        self.assertEqual(0, self.rebuild("same-owner")["promoted"])
        self.conn.execute("UPDATE evidence_items SET source_owner='other owner' WHERE id=2")
        self.assertEqual(0, self.rebuild("distinct-owners")["promoted"])
        self.review_origins()
        self.assertEqual(1, self.rebuild("reviewed-origins")["promoted"])
        self.conn.execute("UPDATE evidence_items SET verification_state='rejected' WHERE id=2")
        self.assertEqual(0, self.rebuild("rejected-second")["promoted"])

    def test_origins_must_be_explicit_distinct_and_reviewed_on_both_items(self):
        self.conn.execute("UPDATE evidence_items SET evidence_tier='E2',source_owner='publisher one'")
        self.add_evidence(2, tier="E2", owner="publisher two")
        self.add_arguments(2)
        reviewed = {"independence_verified": True, "origin_key": "witness:one"}
        cases = [
            ({}, {}),
            ({"model": "model one"}, {"model": "model two"}),
            ({"origin_key": "one"}, {"origin_key": "two"}),
            (reviewed, {"independence_verified": True}),
            (reviewed, {"independence_verified": "true", "origin_key": "two"}),
            (reviewed, {"independence_verified": 1, "origin_key": "two"}),
            (reviewed, {"independence_verified": False, "origin_key": "two"}),
            (reviewed, {"independence_verified": True, "origin_key": "  "}),
            (reviewed, {"independence_verified": True, "origin_key": "unknown"}),
            (reviewed, {"independence_verified": True, "origin_key": 2}),
            (reviewed, {"independence_verified": True, "origin_key": "witness:one"}),
            (reviewed, {"independence_verified": True, "independence_group": "two"}),
        ]
        for index, metadata_pair in enumerate(cases):
            with self.subTest(metadata_pair=metadata_pair):
                for key, metadata in enumerate(metadata_pair, 1):
                    self.conn.execute("UPDATE evidence_items SET metadata_json=? WHERE id=?", (json.dumps(metadata), key))
                result = self.rebuild(f"no-independent-witnesses:{index}")
                self.assertEqual(0, result["promoted"])
                self.assertEqual("review", self.conn.execute("SELECT state FROM relation_assertions WHERE generation_id=?", (result["generation_id"],)).fetchone()[0])
        self.review_origins()
        self.assertEqual(1, self.rebuild("reviewed-distinct")["promoted"])

    def test_same_publisher_still_cannot_supply_independent_e2(self):
        self.conn.execute("UPDATE evidence_items SET evidence_tier='E2',source_owner='publisher'")
        self.add_evidence(2, tier="E2", owner=" PUBLISHER ")
        self.add_arguments(2)
        self.review_origins()
        self.assertEqual(0, self.rebuild()["promoted"])

    def test_reviewed_origin_groups_work_but_shared_group_vetoes_distinct_keys(self):
        self.conn.execute("UPDATE evidence_items SET evidence_tier='E2',source_owner='one'")
        self.add_evidence(2, tier="E2", owner="two")
        self.add_arguments(2)
        for key in (1, 2):
            self.conn.execute("UPDATE evidence_items SET metadata_json=? WHERE id=?",
                (json.dumps({"independence_verified": True, "independence_group": f"group:{key}"}), key))
        self.assertEqual(1, self.rebuild("groups")["promoted"])
        self.review_origins()
        self.conn.execute("UPDATE evidence_items SET metadata_json=json_set(metadata_json,'$.independence_group','same')")
        self.assertEqual(0, self.rebuild("shared-group")["promoted"])

    def test_other_revision_e3_does_not_support_unrelated_pair(self):
        self.conn.execute("UPDATE evidence_items SET verification_state='unverified' WHERE id=1")
        self.add_evidence(2)
        self.add_arguments(2, obj=3)
        result = self.rebuild()
        self.assertEqual(1, result["promoted"])
        self.assertEqual([(1, 3)], self.conn.execute("SELECT subject_entity_id,object_entity_id FROM current_relation_assertions_v").fetchall())
        self.assertEqual([], self.conn.execute("SELECT e.evidence_item_id FROM relation_assertion_evidence e JOIN relation_assertions a ON a.id=e.assertion_id WHERE a.object_entity_id=2").fetchall())

    def test_independent_e2_cannot_borrow_support_from_another_revision(self):
        self.conn.execute("UPDATE evidence_items SET evidence_tier='E2',source_owner='one'")
        self.add_evidence(2, tier="E2", owner="two")
        self.review_origins()
        self.assertEqual(0, self.rebuild("independent-but-unscoped")["promoted"])
        self.add_arguments(2)
        self.assertEqual(1, self.rebuild("independent-and-scoped")["promoted"])

    def test_repeated_pair_uses_only_evidence_from_its_own_occurrences(self):
        self.conn.execute("UPDATE evidence_items SET verification_state='unverified' WHERE id=1")
        self.add_evidence(2)
        self.add_arguments(2)
        self.assertEqual(1, self.rebuild()["promoted"])
        self.assertEqual([(2,)], self.conn.execute("SELECT evidence_item_id FROM relation_assertion_evidence").fetchall())

    def test_cross_revision_evidence_requires_exact_reviewed_pair_locator(self):
        self.conn.execute("UPDATE evidence_items SET verification_state='unverified' WHERE id=1")
        self.add_evidence(2)
        review = {
            "verified": True, "fact_id": 1,
            "subject_entity_id": 1, "object_entity_id": 2,
            "predicate": "restricts", "polarity": "positive", "argument_revision_id": 1,
            "subject_locator": "sentence:1", "object_locator": "sentence:1",
        }
        self.assertEqual(0, self.rebuild("unreviewed-cross-revision")["promoted"])
        invalid_changes = [
            {"verified": False}, {"verified": "true"}, {"verified": 1},
            {"fact_id": 2}, {"subject_entity_id": 2, "object_entity_id": 1},
            {"object_entity_id": 3}, {"predicate": "owns"}, {"polarity": "negative"},
            {"argument_revision_id": 2}, {"argument_revision_id": True},
            {"subject_locator": "sentence:2"}, {"object_locator": "sentence:2"},
        ]
        for index, changes in enumerate(invalid_changes):
            with self.subTest(changes=changes):
                self.conn.execute("UPDATE evidence_items SET metadata_json=? WHERE id=2",
                    (json.dumps({"pair_entailments": [{**review, **changes}]}),))
                self.assertEqual(0, self.rebuild(f"wrong-review:{index}")["promoted"])
        self.conn.execute("UPDATE evidence_items SET metadata_json=? WHERE id=2",
            (json.dumps({"pair_entailments": [review]}),))
        self.assertEqual(1, self.rebuild("verified-cross-revision")["promoted"])
        self.assertEqual([(2,)], self.conn.execute("SELECT e.evidence_item_id FROM relation_assertion_evidence e JOIN current_relation_assertions_v a ON a.id=e.assertion_id").fetchall())

    def test_validator_rechecks_pair_revision_scope(self):
        result = self.rebuild()
        self.add_evidence(2)
        self.conn.execute("UPDATE evidence_items SET source_revision_id=2 WHERE id=1")
        self.conn.row_factory = sqlite3.Row
        with self.assertRaisesRegex(RuntimeError, "failed validation"):
            engine._validate_generation(self.conn, result["generation_id"])

    def test_validator_rechecks_reviewed_independence(self):
        self.conn.execute("UPDATE evidence_items SET evidence_tier='E2',source_owner='one'")
        self.add_evidence(2, tier="E2", owner="two")
        self.add_arguments(2)
        self.review_origins()
        result = self.rebuild()
        self.conn.execute("UPDATE evidence_items SET metadata_json='{}' WHERE id=2")
        self.conn.row_factory = sqlite3.Row
        with self.assertRaisesRegex(RuntimeError, "failed validation"):
            engine._validate_generation(self.conn, result["generation_id"])

    def test_known_common_origin_blocks_e2_independence(self):
        self.conn.execute("UPDATE evidence_items SET evidence_tier='E2',source_owner='owner one'")
        self.add_evidence(2, tier="E2", owner="owner two")
        self.add_arguments(2)
        self.review_origins()
        self.conn.execute("UPDATE source_revisions SET payload_hash='same-hash'")
        self.assertEqual(0, self.rebuild("hash")["promoted"])
        self.conn.execute("UPDATE source_revisions SET payload_hash='different' WHERE id=2")
        self.conn.execute("UPDATE evidence_items SET metadata_json=json_set(metadata_json,'$.origin_id','wire-service:1')")
        self.assertEqual(0, self.rebuild("origin")["promoted"])

    def test_revisions_of_same_object_are_not_independent(self):
        self.conn.execute("UPDATE evidence_items SET evidence_tier='E2',source_owner='owner one'")
        self.add_evidence(2, tier="E2", owner="owner two")
        self.add_arguments(2)
        self.review_origins()
        self.conn.execute("UPDATE source_revisions SET source_object_id=1,revision_no=2,is_current=0 WHERE id=2")
        self.assertEqual(0, self.rebuild()["promoted"])

    def test_social_e3_cannot_use_primary_shortcut(self):
        self.conn.execute("UPDATE source_systems SET source_type='telegram'")
        self.assertEqual(0, self.rebuild()["promoted"])

    def test_unknown_predicate_and_nonassertive_modality_are_not_inferred(self):
        self.conn.execute("UPDATE facts SET predicate='unknown',fact_type='restriction'")
        self.assertEqual(0, self.rebuild("predicate")["assertions"])
        self.conn.execute("UPDATE facts SET predicate='restricts',modality='hypothetical'")
        self.assertEqual(0, self.rebuild("modality")["assertions"])

    def test_inactive_facts_are_not_projected(self):
        self.conn.execute("UPDATE projection_generations SET status='building' WHERE id=2")
        self.assertEqual(0, self.rebuild()["assertions"])

    def test_validation_failure_keeps_active_graph_and_restores_row_factory(self):
        first = self.rebuild("first")
        with patch.object(engine, "_validate_generation", side_effect=RuntimeError("invalid replacement")):
            with self.assertRaisesRegex(RuntimeError, "invalid replacement"):
                self.rebuild("failed")
        self.assertIsNone(self.conn.row_factory)
        self.assertEqual([(first["generation_id"],)], self.conn.execute("SELECT generation_id FROM current_relation_assertions_v").fetchall())
        self.assertEqual("failed", self.conn.execute("SELECT status FROM projection_generations WHERE generation_key='failed'").fetchone()[0])

    def test_validator_rechecks_verification_and_refuting_links(self):
        result = self.rebuild()
        self.conn.row_factory = sqlite3.Row
        for sql in ("UPDATE evidence_items SET verification_state='rejected'", "UPDATE fact_evidence SET evidence_class='refutes'"):
            self.conn.execute("UPDATE evidence_items SET verification_state='verified'")
            self.conn.execute(sql)
            with self.assertRaisesRegex(RuntimeError, "failed validation"):
                engine._validate_generation(self.conn, result["generation_id"])


class LegacyRelationSafetyTests(unittest.TestCase):
    def setUp(self):
        self.fixture = legacy.RelationEngineV2Tests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.conn = self.fixture.conn
        self.fixture._fact_and_arguments()

    def evidence(self, key, tier="E3"):
        self.fixture._source_evidence(source_id=key, content_id=key, evidence_id=key,
            owner=f"owner-{key}", official=tier == "E3", tier=tier, entailment=0.99)

    def test_confirmed_legacy_e3_still_promotes(self):
        self.evidence(101)
        self.fixture._bind_arguments_to_revision(101)
        self.assertEqual(1, engine.rebuild_relation_assertions(self.conn)["promoted"])

    def test_legacy_without_argument_revision_binding_does_not_promote(self):
        self.evidence(101)
        self.assertEqual(0, engine.rebuild_relation_assertions(self.conn)["promoted"])
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM relation_assertion_evidence").fetchone()[0])

    def test_legacy_different_publishers_without_reviewed_origins_do_not_promote(self):
        self.evidence(101, "E2")
        self.evidence(102, "E2")
        self.fixture._bind_arguments_to_revision(101)
        self.fixture._bind_arguments_to_revision(102, copy=True)
        self.assertEqual(0, engine.rebuild_relation_assertions(self.conn)["promoted"])

    def test_missing_owner_does_not_fall_back_to_domains(self):
        self.evidence(101, "E2")
        self.evidence(102, "E2")
        self.fixture._bind_arguments_to_revision(101)
        self.fixture._bind_arguments_to_revision(102, copy=True)
        self.conn.execute("UPDATE sources SET owner=NULL")
        self.assertEqual(0, engine.rebuild_relation_assertions(self.conn)["promoted"])

    def test_legacy_negative_stance_and_bad_authenticity_block(self):
        self.evidence(101)
        self.fixture._bind_arguments_to_revision(101)
        for index, extra in enumerate((
            {"verification_state": "unverified"}, {"authenticity_verdict": "fake"},
            {"authenticity_score": 0}, {"stance": "refutes"},
            {"evidence_class": "refutes"},
        )):
            metadata = {"authenticity_verdict": "confirmed", "evidence_tier": "E3", "entailment_score": 0.99, **extra}
            self.conn.execute("UPDATE fact_evidence SET metadata_json=?", (json.dumps(metadata),))
            self.assertEqual(0, engine.rebuild_relation_assertions(self.conn, generation_key=f"legacy:{index}")["promoted"])

    def test_legacy_refute_class_blocks_even_official_evidence(self):
        self.evidence(101)
        self.fixture._bind_arguments_to_revision(101)
        self.conn.execute("UPDATE fact_evidence SET evidence_class='refute'")
        self.assertEqual(0, engine.rebuild_relation_assertions(self.conn)["promoted"])

    def test_explicit_legacy_groups_still_require_same_content(self):
        self.evidence(101)
        self.conn.execute("UPDATE fact_arguments SET content_item_id=51 WHERE argument_role='target'")
        self.assertEqual(0, engine.rebuild_relation_assertions(self.conn)["assertions"])

    def test_explicit_legacy_revision_metadata_is_scoped(self):
        self.evidence(101)
        self.conn.execute("UPDATE fact_arguments SET metadata_json=? WHERE argument_role='target'",
            (json.dumps({"argument_group": "restriction:1", "source_revision_id": 999}),))
        self.assertEqual(0, engine.rebuild_relation_assertions(self.conn)["assertions"])


class CandidateReplacementSafetyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "fixture.db"
        self.conn = sqlite3.connect(self.path)
        self.addCleanup(self.conn.close)
        self.conn.executescript((ROOT / "db/schema.sql").read_text(encoding="utf-8"))
        self.conn.executescript("""
            INSERT INTO entities(id,entity_type,canonical_name) VALUES(1,'person','A'),(2,'organization','B');
            INSERT INTO relation_candidates(id,entity_a_id,entity_b_id,candidate_type,origin,
                    score,calibrated_score,candidate_state,promotion_state)
            VALUES(1,1,2,'likely_association','candidate_builder:test',0.99,0.99,'promoted','promoted'),
                  (2,1,2,'likely_association','external:test',0.99,0.99,'promoted','promoted');
            INSERT INTO relation_support(candidate_id,support_kind) VALUES(1,'content'),(2,'content');
            INSERT INTO relation_features(candidate_id) VALUES(1),(2);
            INSERT INTO entity_relations(id,from_entity_id,to_entity_id,relation_type,detected_by)
            VALUES(1,1,2,'likely_association','relation_candidate:1:score=0.990'),
                  (2,1,2,'likely_association','relation_candidate:2:score=0.990'),
                  (3,1,2,'mentioned_together','co_occurrence:2'),
                  (4,1,2,'same_case_cluster','manual');
        """)
        self.conn.commit()

    def snapshot(self):
        return {table: self.conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
                for table in ("relation_candidates", "relation_support", "relation_features", "entity_relations")}

    def test_cleanup_is_uncommitted_and_ownership_scoped(self):
        before = self.snapshot()
        candidates._delete_previous_candidate_state(self.conn)
        self.assertEqual([(2,)], self.conn.execute("SELECT id FROM relation_candidates").fetchall())
        self.assertEqual([(2,)], self.conn.execute("SELECT candidate_id FROM relation_support").fetchall())
        self.assertEqual([(2,)], self.conn.execute("SELECT candidate_id FROM relation_features").fetchall())
        self.assertEqual([(2,), (3,), (4,)], self.conn.execute("SELECT id FROM entity_relations ORDER BY id").fetchall())
        with closing(sqlite3.connect(self.path)) as reader:
            self.assertEqual(before["entity_relations"], reader.execute("SELECT * FROM entity_relations ORDER BY id").fetchall())
        self.conn.rollback()
        self.assertEqual(before, self.snapshot())

    def test_builder_failure_rolls_back_graph_and_candidate_rows(self):
        before = self.snapshot()
        with patch.object(candidates, "_load_tag_map", side_effect=RuntimeError("build failed")):
            with self.assertRaisesRegex(RuntimeError, "build failed"):
                candidates.rebuild_relation_candidates({"db_path": str(self.path)})
        self.assertEqual(before, self.snapshot())

    def test_missing_schema_does_not_delete_graph(self):
        self.conn.execute("DROP TABLE entity_mentions")
        self.conn.commit()
        before = self.snapshot()
        result = candidates.rebuild_relation_candidates({"db_path": str(self.path)})
        self.assertFalse(result["ok"])
        self.assertEqual(before, self.snapshot())

    def test_failed_promotion_restores_graph_without_committing_caller(self):
        self.conn.execute("UPDATE relation_candidates SET calibrated_score='invalid' WHERE id=1")
        before = self.snapshot()
        with self.assertRaises(ValueError):
            candidates.promote_relation_candidates(self.conn)
        self.assertTrue(self.conn.in_transaction)
        self.assertEqual(before, self.snapshot())

    def test_late_promotion_failure_restores_rebuild(self):
        before = self.snapshot()
        def fail_promotion(conn):
            # An independent reader still sees the old graph during replacement.
            with closing(sqlite3.connect(self.path)) as reader:
                self.assertEqual(before["entity_relations"], reader.execute("SELECT * FROM entity_relations ORDER BY id").fetchall())
            raise RuntimeError("promotion failed")
        with patch.object(candidates, "promote_relation_candidates", side_effect=fail_promotion):
            with self.assertRaisesRegex(RuntimeError, "promotion failed"):
                candidates.rebuild_relation_candidates({"db_path": str(self.path)})
        self.assertEqual(before, self.snapshot())

    def test_successful_promotion_does_not_commit_caller_writes(self):
        self.conn.execute("UPDATE relation_candidates SET metadata_json='{}' WHERE id=1")
        candidates.promote_relation_candidates(self.conn)
        self.assertTrue(self.conn.in_transaction)
        self.conn.rollback()
        self.assertIsNone(self.conn.execute("SELECT metadata_json FROM relation_candidates WHERE id=1").fetchone()[0])

    def test_successful_rebuild_preserves_external_rows(self):
        before = self.snapshot()
        candidates.rebuild_relation_candidates({"db_path": str(self.path)})
        after = self.snapshot()
        self.assertEqual(before["relation_candidates"][1:], after["relation_candidates"])
        self.assertEqual(before["relation_support"][1:], after["relation_support"])
        self.assertEqual(before["relation_features"][1:], after["relation_features"])
        self.assertEqual(before["entity_relations"][1:], after["entity_relations"])


if __name__ == "__main__":
    unittest.main()
