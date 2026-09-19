"""No database discovery, commits, network, extraction, or guilt inference."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import json
import re
import sqlite3
from typing import Iterable, Mapping
from uuid import uuid4


CORE_FIELDS = ("registered_voters", "ballots_cast", "valid_ballots", "invalid_ballots")


def _text(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be nonblank text")
    return value.strip()


def _timestamp(value: str | None, label: str) -> str | None:
    if value is None:
        return None
    value = _text(value, label)
    pattern = (r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?"
               r"(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)")
    if not re.fullmatch(pattern, value):
        raise ValueError(f"{label} requires an ISO timestamp with UTC offset")
    datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


@dataclass(frozen=True)
class NumericEvidence:
    """Locator may address a page, JSON path, or table cell; it is not verification.

    A caller must review the exact source value before asserting verified status.
    OCR output or the existence of a nonblank locator is insufficient.
    """

    value: int | None
    source_revision_id: int | None = None
    locator: str | None = None
    verification: str = "unverified"
    verified_by: str | None = None


@dataclass(frozen=True)
class Validation:
    state: str
    errors: tuple[str, ...]
    missing: tuple[str, ...]
    unverified: tuple[str, ...]

    @property
    def acceptable(self) -> bool:
        return self.state == "valid" and not self.errors and not self.missing and not self.unverified


def validate_numbers(numbers: Mapping[str, NumericEvidence]) -> Validation:
    """Validate counts, provenance and the explicit cast=valid+invalid equation.

    Missing fields are incomplete, not zero or contradictory. No candidate-vote
    sum is assumed: multi-seat and multi-vote ballot rules are out of scope.
    """
    errors, unverified = [], []
    missing = {key for key in CORE_FIELDS if key not in numbers}
    values = {}
    for key, number in numbers.items():
        _text(key, "field key")
        if key != key.strip():
            raise ValueError("field keys must not have surrounding whitespace")
        if not isinstance(number, NumericEvidence):
            raise ValueError("numbers must contain NumericEvidence")
        if number.verification not in {"missing", "unverified", "verified", "disputed"}:
            errors.append(f"{key}: invalid verification state")
        if number.value is None:
            missing.add(key)
            if number.verification != "missing":
                errors.append(f"{key}: null requires missing verification")
            continue
        if type(number.value) is not int or not 0 <= number.value <= 2**63 - 1:
            errors.append(f"{key}: expected nonnegative SQLite integer")
        else:
            values[key] = number.value
        if (type(number.source_revision_id) is not int or number.source_revision_id <= 0
                or not isinstance(number.locator, str) or not number.locator.strip()):
            errors.append(f"{key}: source revision and per-number locator required")
        if number.verification == "missing":
            errors.append(f"{key}: populated value cannot be missing")
        if number.verification != "verified":
            unverified.append(key)
        elif not isinstance(number.verified_by, str) or not number.verified_by.strip():
            errors.append(f"{key}: verified_by required")
    if all(key in values for key in ("ballots_cast", "valid_ballots", "invalid_ballots")):
        if values["ballots_cast"] != values["valid_ballots"] + values["invalid_ballots"]:
            errors.append("ballots_cast != valid_ballots + invalid_ballots")
    if not values:
        errors.append("protocol contains no numeric values")
    state = "invalid" if errors else "incomplete" if missing else "valid"
    return Validation(state, tuple(errors), tuple(sorted(missing)), tuple(sorted(unverified)))


@dataclass(frozen=True)
class Comparison:
    scope_id: int
    official_protocol_id: int | None
    observed_protocol_id: int | None
    independent: bool
    status: str
    fields: dict[str, dict]


class AuditStore:
    """Operate only on a caller-owned migrated connection with FK enforcement.

    Caller owns commit/rollback. Multi-statement operations use nested savepoints
    and open an outer transaction if needed, never committing caller work.
    """

    def __init__(self, conn: sqlite3.Connection):
        if conn.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
            raise ValueError("foreign_keys must be enabled by the caller")
        self.conn = conn

    @contextmanager
    def _atomic(self):
        if not self.conn.in_transaction:
            self.conn.execute("BEGIN")
        name = "election_" + uuid4().hex
        self.conn.execute(f"SAVEPOINT {name}")
        try:
            yield
        except BaseException:
            self.conn.execute(f"ROLLBACK TO {name}")
            self.conn.execute(f"RELEASE {name}")
            raise
        else:
            self.conn.execute(f"RELEASE {name}")

    def _one(self, sql: str, params=()) -> dict | None:
        cursor = self.conn.execute(sql, params)
        row = cursor.fetchone()
        return None if row is None else dict(zip((c[0] for c in cursor.description), row))

    def add_campaign(self, campaign_key: str, title: str) -> int:
        return self.conn.execute(
            "INSERT INTO election_campaigns(campaign_key,title) VALUES(?,?)",
            (_text(campaign_key, "campaign_key"), _text(title, "title")),
        ).lastrowid

    def add_ballot(self, campaign_id: int, ballot_key: str, title: str) -> int:
        return self.conn.execute(
            "INSERT INTO election_ballots(campaign_id,ballot_key,title) VALUES(?,?,?)",
            (campaign_id, _text(ballot_key, "ballot_key"), _text(title, "title")),
        ).lastrowid

    def add_precinct(self, campaign_id: int, jurisdiction: str, official_id: str,
                     category: str) -> int:
        return self.conn.execute(
            "INSERT INTO election_precincts(campaign_id,jurisdiction,official_id,category) "
            "VALUES(?,?,?,?)",
            (campaign_id, _text(jurisdiction, "jurisdiction"),
             _text(official_id, "official_id"), _text(category, "category")),
        ).lastrowid

    def add_scope(self, campaign_id: int, ballot_id: int, precinct_id: int) -> int:
        return self.conn.execute(
            "INSERT INTO election_ballot_scopes(campaign_id,ballot_id,precinct_id) VALUES(?,?,?)",
            (campaign_id, ballot_id, precinct_id),
        ).lastrowid

    def record_protocol(self, scope_id: int, protocol_type: str, version_no: int,
                        source_revision_id: int, provenance_group: str,
                        document_locator: str, numbers: Mapping[str, NumericEvidence], *,
                        reported_at: str | None = None, fetched_at: str | None = None,
                        timezone: str | None = None, publication_status: str = "unknown") -> int:
        """Retain arithmetic conflicts/unverified versions without accepting them.

        Structurally malformed numbers fail the SQL constraints and roll back.
        provenance_group is a caller-reviewed upstream-origin cluster, NOT a URL
        or a unique identifier minted for every document.
        """
        if type(version_no) is not int or version_no <= 0:
            raise ValueError("version_no must be a positive integer")
        reported_at = _timestamp(reported_at, "reported_at")
        fetched_at = _timestamp(fetched_at, "fetched_at")
        timezone = _text(timezone, "timezone") if timezone is not None else None
        if publication_status not in {"unknown", "preliminary", "final"}:
            raise ValueError("publication_status must be unknown, preliminary, or final")
        result = validate_numbers(numbers)
        with self._atomic():
            protocol_id = self.conn.execute(
                "INSERT INTO election_protocol_versions(scope_id,protocol_type,version_no,"
                "source_revision_id,provenance_group,document_locator,validation_state,validation_json,"
                "reported_at,fetched_at,timezone,publication_status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (scope_id, protocol_type, version_no, source_revision_id,
                 _text(provenance_group, "provenance_group"), _text(document_locator, "document_locator"),
                 result.state, json.dumps(result.__dict__, sort_keys=True),
                 reported_at, fetched_at, timezone, publication_status),
            ).lastrowid
            for key, number in numbers.items():
                # Prevent SQLite's affinity from silently coercing floats/bools to integers.
                if number.value is not None and (type(number.value) is not int
                                                or not 0 <= number.value <= 2**63 - 1):
                    raise ValueError(f"{key}: invalid count")
                self.conn.execute(
                    "INSERT INTO election_protocol_numbers(protocol_id,field_key,value,source_revision_id,"
                    "locator,verification_state,verified_by) VALUES(?,?,?,?,?,?,?)",
                    (protocol_id, key, number.value, number.source_revision_id, number.locator,
                     number.verification, number.verified_by),
                )
            self.conn.execute("UPDATE election_protocol_versions SET sealed=1 WHERE id=?", (protocol_id,))
        return protocol_id

    def accept_protocol(self, protocol_id: int, *, reviewer: str, reason: str,
                        expected_previous_id: int | None = None) -> None:
        """Select one version per scope/type with explicit compare-and-swap replacement."""
        reviewer, reason = _text(reviewer, "reviewer"), _text(reason, "reason")
        with self._atomic():
            protocol = self._one("SELECT * FROM election_protocol_versions WHERE id=?", (protocol_id,))
            if protocol is None:
                raise ValueError("unknown protocol")
            key = (protocol["scope_id"], protocol["protocol_type"])
            old = self._one("SELECT protocol_id FROM election_accepted_protocols "
                            "WHERE scope_id=? AND protocol_type=?", key)
            old_id = old["protocol_id"] if old else None
            if old_id != expected_previous_id:
                raise ValueError("accepted protocol changed; explicit expected_previous_id required")
            self.conn.execute(
                "INSERT INTO election_accepted_protocols(scope_id,protocol_type,protocol_id,reviewer,reason) "
                "VALUES(?,?,?,?,?) ON CONFLICT(scope_id,protocol_type) DO UPDATE SET "
                "protocol_id=excluded.protocol_id,reviewer=excluded.reviewer,reason=excluded.reason,"
                "accepted_at=datetime('now')", (*key, protocol_id, reviewer, reason),
            )

    def _accepted(self, scope_id: int, kind: str) -> dict | None:
        return self._one(
            "SELECT p.* FROM election_accepted_protocols a JOIN election_protocol_versions p "
            "ON p.id=a.protocol_id WHERE a.scope_id=? AND a.protocol_type=?", (scope_id, kind),
        )

    def _numbers(self, protocol: dict | None) -> dict[str, dict]:
        if protocol is None:
            return {}
        cursor = self.conn.execute("SELECT * FROM election_protocol_numbers WHERE protocol_id=?",
                                   (protocol["id"],))
        columns = [c[0] for c in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor]
        return {row["field_key"]: row for row in rows}

    def _source_systems(self, protocol: dict, numbers: dict) -> set[int]:
        revisions = {protocol["source_revision_id"]}
        revisions.update(n["source_revision_id"] for n in numbers.values() if n["value"] is not None)
        return {self.conn.execute(
            "SELECT o.source_system_id FROM source_revisions r JOIN source_objects o "
            "ON o.id=r.source_object_id WHERE r.id=?", (revision,),
        ).fetchone()[0] for revision in revisions}

    def compare_scope(self, scope_id: int) -> Comparison:
        """Read both acceptance pointers and all evidence in one SQLite snapshot."""
        with self._atomic():
            return self._compare_scope(scope_id)

    def _compare_scope(self, scope_id: int) -> Comparison:
        if not self._one("SELECT id FROM election_ballot_scopes WHERE id=?", (scope_id,)):
            raise ValueError("unknown scope")
        official, observed = self._accepted(scope_id, "official"), self._accepted(scope_id, "observed")
        left, right = self._numbers(official), self._numbers(observed)
        independent = bool(official and observed
                           and official["provenance_group"] != observed["provenance_group"]
                           and self._source_systems(official, left).isdisjoint(
                               self._source_systems(observed, right)))
        fields = {}
        for key in sorted(set(CORE_FIELDS) | left.keys() | right.keys()):
            a, b = left.get(key), right.get(key)
            av, bv = a["value"] if a else None, b["value"] if b else None
            usable = (av is not None and bv is not None and independent
                      and a["verification_state"] == b["verification_state"] == "verified")
            fields[key] = {"official": av, "observed": bv,
                           "delta": bv - av if usable else None,
                           "status": ("missing" if av is None or bv is None else
                                      "not_independent" if not independent else
                                      "equal" if usable and av == bv else
                                      "different" if usable else "unverified"),
                           "official_evidence": a, "observed_evidence": b}
        status = ("missing_protocol" if not official or not observed else
                  "not_independent" if not independent else
                  "incomplete" if any(f["status"] == "missing" for f in fields.values()) else
                  "different" if any(f["status"] == "different" for f in fields.values()) else "equal")
        return Comparison(scope_id, official["id"] if official else None,
                          observed["id"] if observed else None, independent, status, fields)

    def comparable_totals(self, scope_ids: Iterable[int]) -> dict:
        """Full-coverage totals only; never mix ballots, rounds, or DEG with UIK."""
        with self._atomic():
            return self._comparable_totals(scope_ids)

    def _comparable_totals(self, scope_ids: Iterable[int]) -> dict:
        scope_ids = list(scope_ids)
        if not scope_ids or len(set(scope_ids)) != len(scope_ids):
            raise ValueError("provide nonempty, unique scope IDs")
        identities = []
        for scope_id in scope_ids:
            row = self._one("SELECT s.ballot_id,p.category FROM election_ballot_scopes s "
                            "JOIN election_precincts p ON p.id=s.precinct_id WHERE s.id=?", (scope_id,))
            if row is None:
                raise ValueError("unknown scope")
            identities.append((row["ballot_id"], row["category"]))
        if len(set(identities)) != 1:
            raise ValueError("totals require the same ballot and precinct category")
        comparisons = [self._compare_scope(scope_id) for scope_id in scope_ids]
        totals = {}
        for key in sorted(set().union(*(c.fields.keys() for c in comparisons))):
            rows = [c.fields.get(key, {}) for c in comparisons]
            paired = sum(row.get("delta") is not None for row in rows)
            complete = paired == len(rows)
            official = sum(row["official"] for row in rows) if complete else None
            observed = sum(row["observed"] for row in rows) if complete else None
            totals[key] = {"official": official, "observed": observed,
                           "delta": observed - official if complete else None,
                           "paired_scopes": paired, "expected_scopes": len(rows)}
        return {"ballot_id": identities[0][0], "category": identities[0][1], "fields": totals}

    def add_incident_candidate(self, scope_id: int, candidate_key: str, kind: str,
                               description: str, basis: dict) -> int:
        """Idempotent candidate creation; this operation can never confirm an incident."""
        candidate_key = _text(candidate_key, "candidate_key")
        payload = (_text(kind, "kind"), _text(description, "description"),
                   json.dumps(basis, sort_keys=True, allow_nan=False))
        with self._atomic():
            existing = self._one("SELECT * FROM election_incidents WHERE scope_id=? AND candidate_key=?",
                                 (scope_id, candidate_key))
            if existing:
                if tuple(existing[k] for k in ("kind", "description", "basis_json")) != payload:
                    raise ValueError("candidate key reused with different evidence")
                return existing["id"]
            return self.conn.execute(
                "INSERT INTO election_incidents(scope_id,candidate_key,kind,description,basis_json) "
                "VALUES(?,?,?,?,?)", (scope_id, candidate_key, *payload),
            ).lastrowid

    def detect_discrepancy(self, scope_id: int) -> int | None:
        with self._atomic():
            return self._detect_discrepancy(scope_id)

    def _detect_discrepancy(self, scope_id: int) -> int | None:
        comparison = self._compare_scope(scope_id)
        differences = {k: v for k, v in comparison.fields.items() if v["status"] == "different"}
        if not differences:
            return None
        key = f"comparison:{comparison.official_protocol_id}:{comparison.observed_protocol_id}"
        return self.add_incident_candidate(
            scope_id, key, "protocol_discrepancy", "Independent accepted protocol numbers differ",
            {"official_protocol_id": comparison.official_protocol_id,
             "observed_protocol_id": comparison.observed_protocol_id, "fields": differences},
        )

    def review_incident(self, incident_id: int, *, decision: str, reviewer: str,
                        reason: str, evidence_item_id: int | None = None) -> None:
        """Explicit human review of an incident, never an adjudication of guilt."""
        if decision not in {"confirmed", "dismissed"}:
            raise ValueError("decision must be confirmed or dismissed")
        reviewer, reason = _text(reviewer, "reviewer"), _text(reason, "reason")
        if decision == "confirmed" and evidence_item_id is None:
            raise ValueError("confirmation requires an evidence item")
        changed = self.conn.execute(
            "UPDATE election_incidents SET state=?,reviewer=?,review_reason=?,review_evidence_id=? "
            "WHERE id=? AND state='candidate'",
            (decision, reviewer, reason, evidence_item_id, incident_id),
        ).rowcount
        if changed != 1:
            raise ValueError("unknown or already reviewed incident")

    def incident_counts(self, scope_id: int) -> dict:
        row = self._one("SELECT * FROM election_incident_counts_v WHERE scope_id=?", (scope_id,))
        if row is None:
            raise ValueError("unknown scope")
        return row

    def record_claim(self, scope_id: int, *, source_revision_id: int, locator: str,
                     attributed_to: str, stance: str, claim_text: str,
                     incident_id: int | None = None, subject_entity_id: int | None = None) -> int:
        """Store attributed speech and stance only; do not create facts or confirm incidents."""
        return self.conn.execute(
            "INSERT INTO election_claims(scope_id,incident_id,source_revision_id,locator,attributed_to,"
            "subject_entity_id,stance,claim_text) VALUES(?,?,?,?,?,?,?,?)",
            (scope_id, incident_id, source_revision_id, _text(locator, "locator"),
             _text(attributed_to, "attributed_to"), subject_entity_id, stance,
             _text(claim_text, "claim_text")),
        ).lastrowid
