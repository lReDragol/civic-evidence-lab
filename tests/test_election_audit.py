from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import sqlite3
import unittest

from election_audit import AuditStore, NumericEvidence, validate_numbers


ROOT = Path(__file__).resolve().parents[1]


class ElectionAuditTests(unittest.TestCase):
    """Synthetic fixtures only: every database is a fresh in-memory connection."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        self.conn.execute("PRAGMA foreign_keys=ON")
        for name in ("0001_core.sql", "0003_election.sql"):
            self.conn.executescript(
                (ROOT / "db" / "reactor_migrations" / "knowledge" / name).read_text(encoding="utf-8")
            )
        self.audit = AuditStore(self.conn)
        self.official_revision = self.source("commission")
        self.observed_revision = self.source("observer")
        self.copy_revision = self.source("syndicator")
        self.campaign = self.audit.add_campaign("synthetic-2026-round-1", "Synthetic campaign")
        self.ballot = self.audit.add_ballot(self.campaign, "district-1-seat", "Single-seat ballot")
        self.ballot_two = self.audit.add_ballot(self.campaign, "party-list", "Party-list ballot")
        self.precinct = self.audit.add_precinct(self.campaign, "region-a/district-1", "001", "uik")
        self.other_precinct = self.audit.add_precinct(self.campaign, "region-b/district-1", "001", "uik")
        self.deg = self.audit.add_precinct(self.campaign, "region-a/district-1", "001", "deg")
        self.scope = self.audit.add_scope(self.campaign, self.ballot, self.precinct)
        self.other_scope = self.audit.add_scope(self.campaign, self.ballot, self.other_precinct)
        self.second_ballot_scope = self.audit.add_scope(self.campaign, self.ballot_two, self.precinct)
        self.deg_scope = self.audit.add_scope(self.campaign, self.ballot, self.deg)
        self.overseas = self.audit.add_precinct(self.campaign, "region-a/district-1", "001", "overseas")
        self.overseas_scope = self.audit.add_scope(self.campaign, self.ballot, self.overseas)

    def source(self, key):
        source = self.conn.execute(
            "INSERT INTO source_systems(source_key,source_type,title) VALUES(?,'fixture',?)", (key, key)
        ).lastrowid
        obj = self.conn.execute(
            "INSERT INTO source_objects(source_system_id,external_id,first_seen_at,last_seen_at) "
            "VALUES(?,'protocol','2026-01-01','2026-01-01')", (source,),
        ).lastrowid
        return self.conn.execute(
            "INSERT INTO source_revisions(source_object_id,revision_no,payload_hash,payload_json,fetched_at) "
            "VALUES(?,1,?,'{}','2026-01-01')", (obj, key),
        ).lastrowid

    def numbers(self, revision=None, *, cast=100, valid=98, invalid=2):
        revision = revision or self.official_revision
        return {key: NumericEvidence(value, revision, f"page:1/row:{index}", "verified", "fixture-reviewer")
                for index, (key, value) in enumerate({
                    "registered_voters": 200, "ballots_cast": cast,
                    "valid_ballots": valid, "invalid_ballots": invalid,
                }.items(), 1)}

    def protocol(self, kind="official", scope=None, version=1, numbers=None, revision=None, group=None,
                 **metadata):
        revision = revision or (self.official_revision if kind == "official" else self.observed_revision)
        return self.audit.record_protocol(
            scope or self.scope, kind, version, revision, group or kind, "page:1",
            numbers if numbers is not None else self.numbers(revision), **metadata,
        )

    def accept(self, protocol, previous=None):
        self.audit.accept_protocol(protocol, reviewer="human-reviewer", reason="Selected fixture copy",
                                   expected_previous_id=previous)

    def pair(self, scope=None, *, observed_cast=100):
        scope = scope or self.scope
        official = self.protocol(scope=scope)
        observed = self.protocol("observed", scope=scope,
                                 numbers=self.numbers(self.observed_revision,
                                                      cast=observed_cast, valid=observed_cast - 2))
        self.accept(official)
        self.accept(observed)
        return official, observed

    def evidence(self):
        return self.conn.execute(
            "INSERT INTO evidence_items(source_revision_id,evidence_key,evidence_type,locator,"
            "verification_state,recorded_at) VALUES(?,'review-proof','document','page:1','verified',"
            "'2026-01-01')", (self.observed_revision,),
        ).lastrowid

    def test_identity_includes_campaign_jurisdiction_official_id_category(self):
        self.assertEqual(3, len({self.precinct, self.other_precinct, self.deg}))
        with self.assertRaises(sqlite3.IntegrityError):
            self.audit.add_precinct(self.campaign, "region-a/district-1", "001", "uik")
        another = self.audit.add_campaign("synthetic-2026-round-2", "Another round")
        precinct = self.audit.add_precinct(another, "region-a/district-1", "001", "uik")
        self.assertNotEqual(precinct, self.precinct)
        with self.assertRaises(ValueError):
            self.audit.add_precinct(self.campaign, "", "001", "uik")
        with self.assertRaises(ValueError):
            self.audit.add_precinct(self.campaign, "region-a", 1, "uik")

    def test_two_ballots_and_deg_have_distinct_scopes(self):
        self.assertEqual(4, len({self.scope, self.other_scope, self.second_ballot_scope, self.deg_scope}))
        for scope in (self.scope, self.other_scope, self.second_ballot_scope, self.deg_scope):
            self.accept(self.protocol(scope=scope))
        self.assertEqual(4, self.conn.execute("SELECT COUNT(*) FROM election_accepted_protocols").fetchone()[0])

    def test_overseas_is_separate_from_domestic_and_deg_even_with_same_official_id(self):
        self.assertEqual(3, len({self.precinct, self.deg, self.overseas}))
        self.pair(self.overseas_scope, observed_cast=99)
        self.assertEqual("missing_protocol", self.audit.compare_scope(self.scope).status)
        totals = self.audit.comparable_totals([self.overseas_scope])
        self.assertEqual("overseas", totals["category"])
        self.assertEqual(-1, totals["fields"]["ballots_cast"]["delta"])
        for scope in (self.scope, self.deg_scope):
            with self.assertRaises(ValueError):
                self.audit.comparable_totals([scope, self.overseas_scope])
        with self.assertRaises(sqlite3.IntegrityError):
            self.audit.add_precinct(self.campaign, "region-a/district-1", "001", "overseas")

    def test_cross_campaign_scope_rejected(self):
        campaign = self.audit.add_campaign("other", "Other")
        ballot = self.audit.add_ballot(campaign, "seat", "Seat")
        precinct = self.audit.add_precinct(campaign, "region-a", "001", "uik")
        with self.assertRaises(sqlite3.IntegrityError):
            self.audit.add_scope(self.campaign, ballot, self.precinct)
        with self.assertRaises(sqlite3.IntegrityError):
            self.audit.add_scope(self.campaign, self.ballot, precinct)

    def test_validation_rejects_bad_counts_and_provenance(self):
        for value in (-1, 1.5, True, "12", 2**63):
            with self.subTest(value=value):
                numbers = self.numbers()
                numbers["ballots_cast"] = replace(numbers["ballots_cast"], value=value)
                self.assertEqual("invalid", validate_numbers(numbers).state)
                with self.assertRaises(ValueError):
                    self.protocol(numbers=numbers)
        numbers = self.numbers()
        for changes in ({"locator": None}, {"source_revision_id": None}, {"verified_by": " "}):
            with self.subTest(changes=changes):
                bad = dict(numbers, ballots_cast=replace(numbers["ballots_cast"], **changes))
                self.assertFalse(validate_numbers(bad).acceptable)
                with self.assertRaises(sqlite3.IntegrityError):
                    self.protocol(numbers=bad)
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM election_protocol_versions").fetchone()[0])

    def test_arithmetic_conflict_is_retained_but_not_accepted(self):
        protocol = self.protocol(numbers=self.numbers(cast=105))
        row = self.conn.execute("SELECT sealed,validation_state,validation_json "
                                "FROM election_protocol_versions WHERE id=?", (protocol,)).fetchone()
        self.assertEqual((1, "invalid"), row[:2])
        self.assertIn("ballots_cast !=", row[2])
        with self.assertRaises(sqlite3.IntegrityError):
            self.accept(protocol)

    def test_unverified_and_disputed_numbers_cannot_be_accepted(self):
        for version, state in enumerate(("unverified", "disputed"), 1):
            with self.subTest(state=state):
                numbers = self.numbers()
                numbers["ballots_cast"] = replace(numbers["ballots_cast"], verification=state)
                self.assertFalse(validate_numbers(numbers).acceptable)
                protocol = self.protocol(version=version, numbers=numbers)
                with self.assertRaises(sqlite3.IntegrityError):
                    self.accept(protocol)

    def test_missing_is_incomplete_and_zero_is_a_real_number(self):
        numbers = self.numbers()
        del numbers["registered_voters"]
        numbers["invalid_ballots"] = NumericEvidence(None, verification="missing")
        validation = validate_numbers(numbers)
        self.assertEqual("incomplete", validation.state)
        self.assertEqual(("invalid_ballots", "registered_voters"), validation.missing)
        self.assertFalse(validation.acceptable)
        incomplete = self.protocol(numbers=numbers)
        with self.assertRaises(sqlite3.IntegrityError):
            self.accept(incomplete)
        self.accept(self.protocol("observed", numbers=self.numbers(self.observed_revision, valid=100, invalid=0)))
        comparison = self.audit.compare_scope(self.scope)
        self.assertEqual("missing_protocol", comparison.status)
        self.assertIsNone(comparison.fields["invalid_ballots"]["official"])
        self.assertEqual(0, comparison.fields["invalid_ballots"]["observed"])
        self.assertIsNone(comparison.fields["invalid_ballots"]["delta"])
        self.assertIsNone(comparison.fields["registered_voters"]["official"])
        self.assertIsNone(comparison.fields["ballots_cast"]["delta"])
        stored = self.conn.execute("SELECT value,verification_state FROM election_protocol_numbers "
                                   "WHERE protocol_id=? AND field_key='invalid_ballots'", (incomplete,)).fetchone()
        self.assertEqual((None, "missing"), stored)

    def test_every_core_field_is_required_for_insert_and_replacement_acceptance(self):
        accepted = self.protocol()
        self.accept(accepted)
        for index, key in enumerate(self.numbers(), 2):
            for null in (False, True):
                with self.subTest(field=key, explicit_null=null):
                    numbers = self.numbers()
                    if null:
                        numbers[key] = NumericEvidence(None, verification="missing")
                    else:
                        del numbers[key]
                    self.assertFalse(validate_numbers(numbers).acceptable)
                    incomplete = self.protocol(version=index * 2 + int(null), numbers=numbers)
                    with self.assertRaises(sqlite3.IntegrityError):
                        self.accept(incomplete, previous=accepted)
        self.assertEqual([(accepted,)], self.conn.execute(
            "SELECT protocol_id FROM election_accepted_protocols").fetchall())
        self.assertEqual(1, self.conn.execute("SELECT COUNT(*) FROM election_acceptance_history").fetchone()[0])

    def test_sql_guard_checks_core_fields_even_if_validation_label_is_wrong(self):
        # Exercise the SQL gate independently of the Python validation label.
        protocol = self.conn.execute(
            "INSERT INTO election_protocol_versions(scope_id,protocol_type,version_no,source_revision_id,"
            "provenance_group,document_locator,validation_state,validation_json) "
            "VALUES(?,'official',1,?,'origin','page:1','valid','{}')",
            (self.scope, self.official_revision),
        ).lastrowid
        self.conn.execute("INSERT INTO election_protocol_numbers VALUES(?,'ballots_cast',100,?,"
                          "'json:/cast','verified','reviewer')", (protocol, self.official_revision))
        self.conn.execute("UPDATE election_protocol_versions SET sealed=1 WHERE id=?", (protocol,))
        with self.assertRaises(sqlite3.IntegrityError):
            self.accept(protocol)

    def test_optional_field_missing_on_one_accepted_side_stays_none_not_zero(self):
        numbers = self.numbers(self.observed_revision)
        numbers["candidate:17"] = replace(numbers["ballots_cast"], value=0)
        self.accept(self.protocol())
        self.accept(self.protocol("observed", numbers=numbers))
        comparison = self.audit.compare_scope(self.scope)
        self.assertEqual("incomplete", comparison.status)
        field = comparison.fields["candidate:17"]
        self.assertIsNone(field["official"])
        self.assertEqual(0, field["observed"])
        self.assertIsNone(field["delta"])
        totals = self.audit.comparable_totals([self.scope])["fields"]
        self.assertIsNone(totals["candidate:17"]["official"])
        self.assertIsNone(totals["candidate:17"]["observed"])
        self.assertEqual(0, totals["candidate:17"]["paired_scopes"])
        self.assertEqual(100, totals["ballots_cast"]["official"])

    def test_explicit_missing_extra_field_is_not_acceptable(self):
        numbers = self.numbers()
        numbers["candidate:17"] = NumericEvidence(None, verification="missing")
        self.assertFalse(validate_numbers(numbers).acceptable)
        with self.assertRaises(sqlite3.IntegrityError):
            self.accept(self.protocol(numbers=numbers))

    def test_structured_locators_are_preserved_but_do_not_imply_verification(self):
        for version, locator in enumerate(("json:/results/ballots_cast", "csv:row=12,column=cast"), 1):
            numbers = self.numbers()
            numbers["ballots_cast"] = NumericEvidence(100, self.official_revision, locator)
            self.assertFalse(validate_numbers(numbers).acceptable)
            protocol = self.protocol(version=version, numbers=numbers)
            self.assertEqual((locator, "unverified"), self.conn.execute(
                "SELECT locator,verification_state FROM election_protocol_numbers "
                "WHERE protocol_id=? AND field_key='ballots_cast'", (protocol,)).fetchone())
            with self.assertRaises(sqlite3.IntegrityError):
                self.accept(protocol)
        reviewed = self.numbers()
        reviewed["ballots_cast"] = replace(reviewed["ballots_cast"], locator="json:/results/ballots_cast")
        self.accept(self.protocol(version=3, numbers=reviewed))

    def test_protocol_timestamp_and_publication_metadata_are_optional_and_immutable(self):
        default = self.protocol()
        self.assertEqual((None, None, None, "unknown"), self.conn.execute(
            "SELECT reported_at,fetched_at,timezone,publication_status FROM election_protocol_versions WHERE id=?",
            (default,)).fetchone())
        for version, status in enumerate(("preliminary", "final"), 2):
            protocol = self.protocol(version=version, reported_at="2026-09-20T12:30:00+03:00",
                                     fetched_at="2026-09-20T09:31:00Z", timezone="Europe/Moscow",
                                     publication_status=status)
            self.assertEqual(("2026-09-20T12:30:00+03:00", "2026-09-20T09:31:00Z", "Europe/Moscow", status),
                             self.conn.execute("SELECT reported_at,fetched_at,timezone,publication_status "
                                               "FROM election_protocol_versions WHERE id=?", (protocol,)).fetchone())
            with self.assertRaises(sqlite3.IntegrityError):
                self.conn.execute("UPDATE election_protocol_versions SET publication_status='unknown' WHERE id=?",
                                  (protocol,))
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM election_accepted_protocols").fetchone()[0])

    def test_bad_or_naive_protocol_timestamps_and_unknown_status_are_rejected(self):
        for metadata in ({"reported_at": "2026-09-20T12:30:00"}, {"reported_at": "2026-02-30T12:00:00Z"},
                         {"reported_at": "2026-09-20T12:30:00+03:99"},
                         {"fetched_at": "yesterday"}, {"fetched_at": "2026-09-20"}, {"timezone": " "},
                         {"publication_status": "verified"}):
            with self.subTest(metadata=metadata), self.assertRaises(ValueError):
                self.protocol(**metadata)
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM election_protocol_versions").fetchone()[0])

    def test_empty_protocol_cannot_be_accepted(self):
        protocol = self.protocol(numbers={})
        with self.assertRaises(sqlite3.IntegrityError):
            self.accept(protocol)
        self.assertFalse(validate_numbers({}).acceptable)

    def test_each_number_preserves_locator_revision_and_verifier(self):
        self.pair()
        field = self.audit.compare_scope(self.scope).fields["ballots_cast"]
        self.assertEqual(self.official_revision, field["official_evidence"]["source_revision_id"])
        self.assertEqual("page:1/row:2", field["official_evidence"]["locator"])
        self.assertEqual("fixture-reviewer", field["observed_evidence"]["verified_by"])

    def test_conflicting_versions_require_explicit_reviewed_replacement(self):
        first = self.protocol()
        second = self.protocol(version=2, numbers=self.numbers(cast=120, valid=118))
        self.accept(first)
        with self.assertRaises(ValueError):
            self.accept(second)
        self.accept(second, previous=first)
        with self.assertRaises(ValueError):
            self.accept(first, previous=first)
        self.assertEqual([(second,)], self.conn.execute("SELECT protocol_id FROM election_accepted_protocols").fetchall())
        self.assertEqual(2, self.conn.execute("SELECT COUNT(*) FROM election_protocol_versions").fetchone()[0])
        self.assertEqual([(first, None), (second, first)], self.conn.execute(
            "SELECT protocol_id,previous_protocol_id FROM election_acceptance_history ORDER BY id").fetchall())

    def test_duplicate_version_and_wrong_scope_acceptance_rejected(self):
        protocol = self.protocol()
        with self.assertRaises(sqlite3.IntegrityError):
            self.protocol()
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute("INSERT INTO election_accepted_protocols VALUES(?,'official',?,'r','why','now')",
                              (self.other_scope, protocol))
        self.accept(protocol)
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute("INSERT INTO election_accepted_protocols VALUES(?,'official',?,'r','why','now')",
                              (self.scope, protocol))

    def test_sealed_protocol_and_numbers_are_immutable(self):
        protocol = self.protocol()
        statements = [
            ("UPDATE election_protocol_versions SET sealed=0 WHERE id=?", (protocol,)),
            ("DELETE FROM election_protocol_versions WHERE id=?", (protocol,)),
            ("UPDATE election_protocol_numbers SET value=999 WHERE protocol_id=?", (protocol,)),
            ("DELETE FROM election_protocol_numbers WHERE protocol_id=?", (protocol,)),
            ("INSERT INTO election_protocol_numbers(protocol_id,field_key,value,verification_state) "
             "VALUES(?,'extra',NULL,'missing')", (protocol,)),
        ]
        for sql, params in statements:
            with self.subTest(sql=sql), self.assertRaises(sqlite3.IntegrityError):
                self.conn.execute(sql, params)

    def test_independent_comparison_yields_signed_observed_minus_official_delta(self):
        official, observed = self.pair(observed_cast=97)
        comparison = self.audit.compare_scope(self.scope)
        self.assertEqual((official, observed), (comparison.official_protocol_id, comparison.observed_protocol_id))
        self.assertTrue(comparison.independent)
        self.assertEqual("different", comparison.status)
        self.assertEqual(-3, comparison.fields["ballots_cast"]["delta"])

    def test_same_origin_group_is_not_independent_even_across_publishers(self):
        self.accept(self.protocol(group="commission-origin"))
        self.accept(self.protocol("observed", revision=self.copy_revision, group="commission-origin"))
        comparison = self.audit.compare_scope(self.scope)
        self.assertFalse(comparison.independent)
        self.assertEqual("not_independent", comparison.status)
        self.assertIsNone(comparison.fields["ballots_cast"]["delta"])
        self.assertIsNone(self.audit.detect_discrepancy(self.scope))

    def test_same_source_system_is_not_independent_even_with_different_origin_labels(self):
        self.accept(self.protocol())
        self.accept(self.protocol("observed", revision=self.official_revision, group="different-label"))
        self.assertFalse(self.audit.compare_scope(self.scope).independent)

    def test_shared_per_number_source_blocks_independence(self):
        self.accept(self.protocol())
        numbers = self.numbers(self.observed_revision)
        numbers["ballots_cast"] = replace(numbers["ballots_cast"], source_revision_id=self.official_revision)
        self.accept(self.protocol("observed", numbers=numbers))
        self.assertFalse(self.audit.compare_scope(self.scope).independent)

    def test_missing_protocol_does_not_fall_back_to_another_ballot_or_precinct(self):
        self.accept(self.protocol())
        self.accept(self.protocol("observed", scope=self.second_ballot_scope))
        self.accept(self.protocol("observed", scope=self.other_scope))
        comparison = self.audit.compare_scope(self.scope)
        self.assertEqual("missing_protocol", comparison.status)
        self.assertIsNone(comparison.observed_protocol_id)
        self.assertIsNone(comparison.fields["ballots_cast"]["observed"])
        self.assertIsNone(self.audit.detect_discrepancy(self.scope))

    def test_totals_include_each_accepted_scope_once(self):
        self.pair(observed_cast=98)
        self.pair(self.other_scope, observed_cast=101)
        field = self.audit.comparable_totals([self.scope, self.other_scope])["fields"]["ballots_cast"]
        self.assertEqual({"official": 200, "observed": 199, "delta": -1,
                          "paired_scopes": 2, "expected_scopes": 2}, field)
        self.protocol(version=2, numbers=self.numbers(cast=500, valid=498))
        self.assertEqual(field, self.audit.comparable_totals([self.scope, self.other_scope])["fields"]["ballots_cast"])

    def test_totals_report_missing_coverage_without_partial_sum_or_zero(self):
        self.pair()
        field = self.audit.comparable_totals([self.scope, self.other_scope])["fields"]["ballots_cast"]
        self.assertEqual({"official": None, "observed": None, "delta": None,
                          "paired_scopes": 1, "expected_scopes": 2}, field)

    def test_totals_reject_ballot_deg_and_duplicate_scope_mixing(self):
        for scopes in ([], [self.scope, self.scope], [self.scope, self.second_ballot_scope],
                       [self.scope, self.deg_scope], [99999]):
            with self.subTest(scopes=scopes), self.assertRaises(ValueError):
                self.audit.comparable_totals(scopes)

    def test_discrepancies_are_idempotent_candidates_not_confirmed(self):
        self.pair(observed_cast=99)
        first = self.audit.detect_discrepancy(self.scope)
        self.assertIsNotNone(first)
        self.assertEqual(first, self.audit.detect_discrepancy(self.scope))
        self.assertEqual({"scope_id": self.scope, "candidate_count": 1, "confirmed_count": 0,
                          "dismissed_count": 0}, self.audit.incident_counts(self.scope))
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0])
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM relation_assertions").fetchone()[0])

    def test_claim_stance_and_attribution_never_confirm_incident_or_infer_guilt(self):
        self.pair(observed_cast=99)
        incident = self.audit.detect_discrepancy(self.scope)
        entity = self.conn.execute("INSERT INTO entities(entity_type,canonical_name,canonical_key) "
                                   "VALUES('person','Fixture subject','fixture-person')").lastrowid
        for stance in ("alleges", "denies", "reports", "uncertain"):
            self.audit.record_claim(
                self.scope, source_revision_id=self.observed_revision, locator="paragraph:2",
                attributed_to="Fixture speaker", stance=stance, claim_text="Attributed allegation, not a fact",
                incident_id=incident, subject_entity_id=entity,
            )
        self.assertEqual(4, self.conn.execute("SELECT COUNT(*) FROM election_claims").fetchone()[0])
        self.assertEqual(1, self.audit.incident_counts(self.scope)["candidate_count"])
        self.assertEqual(0, self.audit.incident_counts(self.scope)["confirmed_count"])
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0])
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM relation_assertions").fetchone()[0])
        with self.assertRaises(sqlite3.IntegrityError):
            self.audit.record_claim(self.scope, source_revision_id=self.observed_revision, locator="p:1",
                                    attributed_to="Speaker", stance="guilty", claim_text="Test")
        with self.assertRaises(ValueError):
            self.audit.record_claim(self.scope, source_revision_id=self.observed_revision, locator="p:1",
                                    attributed_to=" ", stance="alleges", claim_text="Test")
        with self.assertRaises(sqlite3.IntegrityError):
            self.audit.record_claim(self.other_scope, source_revision_id=self.observed_revision, locator="p:1",
                                    attributed_to="Speaker", stance="alleges", claim_text="Test", incident_id=incident)

    def test_confirmation_requires_explicit_reviewer_reason_and_evidence(self):
        self.pair(observed_cast=99)
        incident = self.audit.detect_discrepancy(self.scope)
        with self.assertRaises(ValueError):
            self.audit.review_incident(incident, decision="confirmed", reviewer="r", reason="why")
        with self.assertRaises(ValueError):
            self.audit.review_incident(incident, decision="confirmed", reviewer=" ", reason="why",
                                       evidence_item_id=self.evidence())
        with self.assertRaises(sqlite3.IntegrityError):
            self.audit.review_incident(incident, decision="confirmed", reviewer="r", reason="why",
                                       evidence_item_id=99999)
        evidence = self.conn.execute("SELECT id FROM evidence_items").fetchone()[0]
        self.audit.review_incident(incident, decision="confirmed", reviewer="r", reason="Reviewed discrepancy",
                                   evidence_item_id=evidence)
        self.assertEqual(0, self.audit.incident_counts(self.scope)["candidate_count"])
        self.assertEqual(1, self.audit.incident_counts(self.scope)["confirmed_count"])
        with self.assertRaises(ValueError):
            self.audit.review_incident(incident, decision="dismissed", reviewer="r", reason="again")
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0])

    def test_dismissed_and_empty_counts_are_separate(self):
        self.assertEqual(0, self.audit.incident_counts(self.scope)["candidate_count"])
        candidate = self.audit.add_incident_candidate(self.scope, "report:1", "reported_issue", "Unreviewed", {})
        self.audit.review_incident(candidate, decision="dismissed", reviewer="r", reason="Unsupported")
        self.assertEqual({"scope_id": self.scope, "candidate_count": 0, "confirmed_count": 0,
                          "dismissed_count": 1}, self.audit.incident_counts(self.scope))

    def test_candidate_key_cannot_silently_replace_evidence(self):
        self.audit.add_incident_candidate(self.scope, "x", "reported_issue", "Unreviewed", {"a": 1})
        with self.assertRaises(ValueError):
            self.audit.add_incident_candidate(self.scope, "x", "reported_issue", "Unreviewed", {"a": 2})

    def test_savepoint_failure_preserves_caller_transaction(self):
        self.conn.commit()
        self.audit.add_campaign("caller-uncommitted", "Caller work")
        numbers = self.numbers()
        numbers["ballots_cast"] = replace(numbers["ballots_cast"], source_revision_id=99999)
        with self.assertRaises(sqlite3.IntegrityError):
            self.protocol(numbers=numbers)
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM election_protocol_versions").fetchone()[0])
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM election_protocol_numbers").fetchone()[0])
        self.assertEqual(1, self.conn.execute("SELECT COUNT(*) FROM election_campaigns "
                                             "WHERE campaign_key='caller-uncommitted'").fetchone()[0])
        self.conn.rollback()
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM election_campaigns "
                                             "WHERE campaign_key='caller-uncommitted'").fetchone()[0])

    def test_operations_do_not_commit_when_caller_has_no_transaction(self):
        self.conn.commit()
        self.protocol()
        self.assertTrue(self.conn.in_transaction)
        self.conn.rollback()
        self.assertEqual(0, self.conn.execute("SELECT COUNT(*) FROM election_protocol_versions").fetchone()[0])

    def test_comparisons_use_one_caller_owned_snapshot(self):
        self.pair()
        self.pair(self.other_scope)
        self.conn.commit()
        statements = []
        self.conn.set_trace_callback(statements.append)
        totals = self.audit.comparable_totals([self.scope, self.other_scope])
        self.conn.set_trace_callback(None)
        self.assertEqual(200, totals["fields"]["ballots_cast"]["official"])
        self.assertEqual(1, sum(sql == "BEGIN" for sql in statements))
        self.assertFalse(any(sql == "COMMIT" for sql in statements))
        self.assertTrue(self.conn.in_transaction)
        self.conn.rollback()
        self.assertEqual("equal", self.audit.compare_scope(self.scope).status)
        self.assertTrue(self.conn.in_transaction)

    def test_row_factory_is_not_changed_and_rows_are_supported(self):
        self.conn.row_factory = sqlite3.Row
        self.pair()
        self.assertEqual("equal", self.audit.compare_scope(self.scope).status)
        self.assertEqual(0, self.audit.incident_counts(self.scope)["confirmed_count"])
        self.assertIs(sqlite3.Row, self.conn.row_factory)

    def test_custom_candidate_fields_are_compared_without_assuming_vote_sum_rules(self):
        left = self.numbers()
        right = self.numbers(self.observed_revision)
        # Multiple selections per ballot can exceed the valid-ballot count.
        left["candidate:official-17"] = replace(left["ballots_cast"], value=150)
        right["candidate:official-17"] = replace(right["ballots_cast"], value=140)
        self.assertEqual("valid", validate_numbers(left).state)
        self.accept(self.protocol(numbers=left))
        self.accept(self.protocol("observed", numbers=right))
        field = self.audit.compare_scope(self.scope).fields["candidate:official-17"]
        self.assertEqual(-10, field["delta"])

    def test_foreign_keys_are_mandatory_and_fixture_integrity_is_clean(self):
        self.pair()
        self.assertEqual([], self.conn.execute("PRAGMA foreign_key_check").fetchall())
        self.assertEqual("ok", self.conn.execute("PRAGMA integrity_check").fetchone()[0])
        other = sqlite3.connect(":memory:")
        self.addCleanup(other.close)
        with self.assertRaises(ValueError):
            AuditStore(other)


if __name__ == "__main__":
    unittest.main()
