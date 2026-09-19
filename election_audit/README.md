# Election Audit Foundation

This standard-library-only package operates on an explicitly supplied Reactor
knowledge SQLite connection. It never locates or opens a database, connects to a
network, reads credentials, commits caller work, or publishes bus events.

## Integration Contract

The parent owns migration discovery/application, connection setup, transactions,
and bus integration. Apply `0001_core.sql` and `0003_election.sql` through that
runner and enable `PRAGMA foreign_keys=ON` before constructing `AuditStore(conn)`.
No dependency on a `0002` migration is introduced. Tests execute the two SQL files
directly against `:memory:` and do not exercise or change the parent runner.

Create a campaign, ballots, fully identified precincts, and ballot scopes with
`add_campaign`, `add_ballot`, `add_precinct`, and `add_scope`. A campaign key must
identify the election and round. A ballot key must identify the contest/district
and ballot kind. The precinct identity is the full tuple
`(campaign_id, jurisdiction, official_id, category)`, never a bare UIK number.
Jurisdiction is a canonical hierarchical identifier supplied by the caller;
official IDs are strings so leading zeros are retained. Categories are `uik`,
`deg`, `overseas`, or `other`; no automatic cross-category mapping exists and
cross-category totals are rejected, including domestic/overseas mixes.

`record_protocol` appends a sealed, immutable version for a scope and type
(`official` or `observed`). Each populated `NumericEvidence` has a source revision,
locator, verification state, and named verifier when verified. A null number must
use `verification="missing"`; an omitted field also stays missing. Corrections to
numbers, source locators, or verification require a new version. Version numbers
are positive, unique within scope/type, and caller-assigned, not arrival ordering.

Locators are opaque nonblank strings and support structured data, for example
`json:/results/ballots_cast` or `csv:row=12,column=valid_ballots`, as well as page
coordinates. Their syntax, resolution, and correspondence to the exact source
value require caller review. A locator or OCR-extracted value is NOT automatically
verified. Only an explicit reviewed assertion with `verified_by` is recorded as
verified; this package does not perform OCR verification or authenticate sources.

Optional keyword arguments on `record_protocol` are `reported_at`, `fetched_at`,
`timezone`, and `publication_status`. The timestamps are ISO strings with seconds
and explicit offsets (for example `2026-09-20T12:30:00+03:00`); absent values stay
SQL NULL, never fabricated from local time. `timezone` is an optional caller-supplied
zone label, not resolved or reconciled with timestamp offsets by this layer.
`publication_status` is `unknown` (default), `preliminary`, or `final`; it describes
the source's publication stage and does not imply verification or acceptance.
These metadata are sealed with the protocol; corrections require a new version.

Validation covers integer/nonnegative counts, provenance shape, and the explicit
equation `ballots_cast = valid_ballots + invalid_ballots` when all three are known.
These names mean counted ballot papers, not issued ballots or voter turnout.
`registered_voters` is a fourth core completeness field. Additional numeric keys
(for example stable `candidate:<official-id>` keys) retain the same provenance.
Candidate-vote sums and jurisdiction-specific electoral rules are not assumed.
Structurally malformed evidence is rejected; arithmetic conflicts are retained as
invalid versions. Unverified/disputed versions can be retained, not accepted.

`accept_protocol` requires an explicit reviewer and reason. Replacing an existing
acceptance also requires its `expected_previous_id`; stale selection fails rather
than silently overwriting it. Acceptance history retains old pointers. Incomplete
protocols are retained but cannot be accepted. All four core fields must be present
and verified, and no supplied field may be explicitly missing or unverified.
Both `Validation.acceptable` and the SQL acceptance guard enforce this. Acceptance
means selected for comparison, NOT authentic or legally valid. Optional fields may
be omitted entirely; a field present on only one accepted side remains `None` on
the other side in comparisons and totals, never zero.

`compare_scope` compares only the two accepted versions of the exact same scope.
`delta` is observed minus official. Missing numbers remain `None`. Independent
comparison requires disjoint Reactor source systems (including per-number sources)
AND different caller-reviewed `provenance_group` values. A group identifies the
upstream origin, so mirrors/reposts must share it. This is a conservative metadata
gate, not automatic proof of independence or authenticity; the caller must review
origin assignments. The comparator returns per-number provenance with the values.
`comparable_totals` accepts unique scopes for one ballot and category; per-field
totals are `None` unless all requested scopes have independent comparable pairs.
Coverage counts expose missing pairs. It never silently sums only the covered subset.

`detect_discrepancy` creates an idempotent incident candidate, even if other fields
are missing; missing fields alone are not discrepancies. `record_claim` stores
attributed speech and the explicit stance `alleges`, `denies`, `reports`, or
`uncertain`. It never confirms an incident, creates a Reactor fact/relation, or
infers guilt. Candidate counts, confirmed counts, and dismissed counts are separate
and count incidents, not claims. Confirmation is a separate `review_incident`
operation requiring reviewer, reason, and an existing Reactor evidence item.
Confirmation records a human decision about the incident, never anyone's guilt;
the package does not judge the evidence's legal sufficiency.

## Bounded Limitations

- No live migration, ingestion, OCR, web collectors, UI, bus handlers, or backfill.
- `0003` is the in-development foundation migration. If an earlier checksum has
  already been applied to a database, the parent must provide an explicit upgrade;
  do not reapply this edited migration or alter a migration ledger in place.
- No legal adjudication, statistical fraud inference, signature authentication,
  automatic independence discovery, or general electoral-rule engine.
- Campaign/ballot/precinct canonicalization and review authorization are parent
  responsibilities. This is an application API, not a security boundary against
  privileged SQL writers or trigger removal.
- Existing incident decisions are final through this API; appeals, review history,
  and supersession of candidates after new protocol acceptance need a later layer.
- Callers own transaction lifetime and retry SQLite lock errors. No implicit commit
  occurs, including on a connection initially outside a transaction. Comparison,
  totals, and discrepancy detection each use one SQLite snapshot; read operations
  also leave an opened outer transaction for the caller to finish.

## Offline Verification

From the repository root:

```powershell
python -B -m unittest discover -s tests -p test_election_audit.py -v
```

Fixtures cover duplicate UIK IDs across jurisdictions/campaigns, two ballots,
DEG/overseas isolation, conflicting versions, incomplete rejection, verified zero,
structured-data locators, optional timestamp/publication metadata,
independent comparison, copied-source exclusion, coverage-aware totals, claims,
review gates, immutability, foreign keys, and caller transaction preservation.
