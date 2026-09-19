# Bounded Civic Model Routes

## Audit Result (2026-09-20)

The current Scanner pool is user-authorized for this task. That authorization
does not extend to arbitrary future scanner findings. The inspected metadata is
insufficient to provision a stable, unattended **free inference** route. No live
connection or inference has been verified; no paid/provider calls were made.

Read-only aggregate queries against
`C:\Users\Drago\AppData\Local\APIWorkspace\workspace.sqlite` found 208 credential
records, 3,484 cached model rows, and cache timestamps from 2026-09-18 15:10-15:11
UTC. All 208 `trusted_source` fields were null. Catalog states were:

| Provider | State | Count |
| --- | --- | ---: |
| cerebras | catalog_ok | 2 |
| cohere | catalog_ok / cooldown / rate_limited | 40 / 124 / 1 |
| groq | catalog_ok | 1 |
| mistral | catalog_ok | 37 |
| moonshot | auth_rejected | 1 |
| openrouter | catalog_ok | 1 |
| perplexity | invalid_response | 1 |

These are historical catalog observations, not current inference-health results.
No credential blobs, hashes, labels, cookies, credential-file contents, or secret
values were selected or printed. The owned store was opened directly with SQLite
`mode=ro`; `OwnedStore()` was not instantiated because its constructor writes.

`F:\ChatGPT-API-Scanner\owned_store.py` stores credential IDs, provider, catalog
state, cache timestamps, and cooldowns. It has no provider **account identity**,
account allocation, explicit free pricing, or inference capability attestation.
Null provenance is not a revocation of the user's current authorization, but the
`model_rows()`/`models()` queries filter for local-files/git-history provenance and
therefore do not expose these current entries. Do not alter provenance to make
them eligible, treat a credential UUID as an account, or infer price from a model
name or successful catalog request.

`free-coding-models-main/src/core/router-daemon.js` explicitly tries up to five
keys on 429/401/403, then can fail over to other models. The general FCM config
also supports multiple provider keys and config merging. This is NOT a safe
transport behind Civic's pinned route contract. `inject_keys_to_fcm.py` imports
Scanner DB/export keys into that config; it is not an approval/provisioning path.

Existing file references were verified by filesystem metadata only:

- `C:\Users\Drago\.free-coding-models.json`: general FCM config, not a Civic grant.
- `F:\ChatGPT-API-Scanner\free-coding-models-main\config.json`: another general
  config; existence does not establish active use, ownership, pricing or safety.

No reviewed Civic launcher/client config was found in the inspected Civic config
and standalone gateway directories. The fixed gateway admission journal was also
absent at inspection. This does not prove no service exists elsewhere. Neither
general config is advertised as an authorized live Civic endpoint. Credential
files and running services were not queried to manufacture a readiness claim.

## Usable Client API

`integrations.civic_routes` depends only on the existing Civic gateway contract,
not Scanner core, catalogs, keys, exports, cookies or admin APIs:

```python
from integrations.civic_routes import CivicRoutes

# Keep this object for the supervised lifetime, NOT one object per poll/job.
routes = CivicRoutes(max_attempts=128, cooldown_seconds=30)

# Parent supplies the exact reviewed metadata-only path. Never glob for configs.
routes.reload(reviewed_client_config_path)
route = routes.select("extract", max_input_tokens=8192, max_output_tokens=256)
snapshot = routes.snapshot

# request is the existing complete FCM request (input refs, schema, ceilings,
# deadline, stream=False, max_cost_microusd=0 and a durable unique request_id).
request["snapshot_id"] = snapshot.snapshot_id
request["route_id"] = route.route_id
result = routes.infer(request)
```

`infer(request) -> dict` is compatible with `FCMGatewayClient.infer`: one exact
validated result, or a sanitized `GatewayError`. It performs at most ONE gateway
dispatch. It never changes the request's account/model/route, retries internally,
merges outputs, or contacts an alternative after failure. `select()` is for a new
operation; it does no network work. It prefers `route_id`, then the snapshot's
remaining route order, considering capability, ceilings and recorded health.
Selection is eligibility, not a promise of live availability or remaining quota.

The actual service contract is `POST /v1/civic/infer` on the configured isolated
gateway origin, with the dedicated Civic bearer token. For example,
`http://127.0.0.1:19390` is a supported loopback origin, **not a verified running
service**. Provider URLs/keys never enter the Civic client config. See
`docs/FCM_GATEWAY.md` for the full request/response schema and launcher contract.

`parse_client_config(mapping, now=...)` and `load_client_config(path, now=...)`
validate without resolving secrets or making calls. `CivicRoutes.configure(config,
environment=...)` resolves only the exact named, versioned gateway token; an
optional trusted `transport=` injection supports offline tests. It must implement
the single-attempt gateway contract, not Scanner's general router.

The flat client JSON matches the parent's existing `--gateway-config` shape:

```json
{
  "snapshot_id": "operator-issued-unique-id",
  "expires_at": 0,
  "routes": [],
  "origin": "http://127.0.0.1:19390",
  "gateway_token_env": "CIVIC_GATEWAY_TOKEN_V1",
  "route_id": "operator-approved-route",
  "max_cost_microusd": 0
}
```

This deliberately INVALID template cannot accidentally enable inference.
Populate `routes` with the complete `Route` fields documented in FCM_GATEWAY,
an explicit canonical account ID, free pricing with both rates zero, known token
allocation, and zero cost allocation. Expiry must be in the future and no more
than 24 hours away. At least one and at most 64 routes are allowed; duplicate IDs,
inconsistent same-account budgets, unknown fields, provider credentials, fallback
lists, nonfree prices, unknown quotas, and unversioned environment references fail.
`max_cost_microusd` may be omitted and is always zero. Parser validity is not an
ownership check: only the trusted operator/provisioner may publish this file.

## Polling, Health And Bounds

The parent owns file polling, request scheduling, durable operation identities,
service startup, reconciliation, and persistence. No parent runtime, gateway
package, bus, DB, or service was modified here.

- `reload()` replaces the entire config. Missing, partial, invalid, or expired
  replacements disable dispatch; the previous config is NOT used as fallback.
  Publish the approved file atomically with restrictive OS ACLs. Do not let the
  scanner, model output, or unaudited discovery process write this path.
- Reload preserves health, consumed request IDs, and the lifetime attempt count.
  Reusing a snapshot ID with changed routes/expiry/origin/token reference fails
  with `snapshot_changed`. History is bounded to 64 configurations. Attempt limits
  are 1..10,000 (default 128), across ALL snapshots for the instance, not per poll.
- Every dispatch attempt consumes one slot and its request ID, including an
  ambiguous failure or server-side rejection. Local preflight rejection does not.
  Concurrent calls fail busy. Calls use the gateway's existing total deadline,
  at most 120 seconds and no later than snapshot expiry. There are no probe calls.
- 429/rate limiting and `account_backoff` hold the whole provider conservatively:
  the current wire result does not include `backoff_scope`. Thus the helper cannot
  accidentally rotate to a same-account key alias or evade a provider-wide hold.
  `provider_unavailable` and gateway busy/unavailable also defer that provider.
- `defer(route_id, scope="account" | "provider", retry_after_seconds=N)` accepts
  explicit trusted health signals, not free-form Scanner status inference. Holds
  never shorten; a minimum 30-second default applies even to Retry-After zero.
  Longer delays are never truncated to snapshot expiry or capped downward.
- Quota exhaustion, auth rejection and missing credentials latch an account block
  across snapshot/model replacement and expiry. Provider quota/auth errors also
  latch the provider, since their wire scope is unknown; local `quota_exceeded`
  and `secret_unavailable` latch only the account. `defer(..., blocked=True)` can
  explicitly latch either scope too. No automatic clear/reset method exists.
  Timeout, malformed output, route mismatch and other ambiguous failures impose
  cooldown, consume the attempt, and never initiate another inference.
- `routes_exhausted`, `attempt_limit`, `config_history_full`, config errors and
  expired grants mean stop/defer and report a provisioning requirement. They do
  not trigger discovery, new keys, automatic approval, budget reset, or a hidden
  fallback. A newly reviewed independent owned route can be published, but grants
  cannot be minted merely to work around account limits.

The helper's health is **process-local**, not a durable quota ledger. It deliberately
does not claim stable unattended operation through restarts. Parent supervision
must preserve/reconcile holds and attempts before recreating an instance; never
restart it to refill a cap or forget a denial. The gateway's one-use persistent
snapshot claim and exact-account reservation remain authoritative. A failed poll
does not start or restart that service, and a client reload cannot hot-reload a
gateway snapshot. Coordinate service-side grants before publishing the client
file. Serialize parent select/snapshot/request construction with config reloads;
a changed snapshot between these steps fails closed rather than being retargeted.

## Required Provisioning

1. Freeze membership of the currently authorized pool. Resolve account identities
   independently of key/credential IDs, with all same-account aliases grouped.
   Do not automatically append future leaked/discovered keys.
2. Establish per-route free-only entitlement, exact provider-returned model ID,
   compatible strict JSON-schema responses, complete usage reporting, token
   ceilings and remaining account allocations. Catalog metadata proves none of
   these; no paid calls should be used as a readiness test.
3. Provision the Scanner launcher's exact reviewed HTTPS endpoint allowlist,
   account-to-versioned-environment binding, separate gateway token, matching
   snapshot and fixed journal. The current launcher resolves named environment
   variables only; it has no OwnedStore/DPAPI credential-ID resolver. The secure
   transfer of an approved existing owned credential into its exact named binding
   remains a provisioning gap, not implemented secret discovery.
4. Have the parent supervise the isolated gateway and poll one reviewed matching
   Civic client JSON. Reconcile consumed/ambiguous allocations before any new
   gateway snapshot or supervisor lifetime. Pause when capacity is exhausted.

Offline verification command:

```powershell
python -m pytest tests/test_civic_routes.py tests/test_fcm_gateway.py -q
```

Tests cover strict free-route parsing, file bounds, expiry, explicit token-name
resolution, single dispatch, account aliases, provider cooldown, refreshed grants,
invalid polling, no merging, sanitized errors, lifetime caps, duplicate IDs and
concurrency. All test credentials/transports are synthetic; passing tests are NOT
evidence of live provider connectivity.
