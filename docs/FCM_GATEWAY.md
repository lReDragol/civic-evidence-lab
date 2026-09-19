# Isolated Civic FCM Gateway

This is an opt-in integration, not a Scanner admin client or a replacement for
Scanner/FCM core. Imports perform no I/O. No existing Scanner source is changed.
The client and server never discover daemons, enumerate keys, ingest future keys,
start Scanner, rotate credentials, select fallback models, or automatically retry.

The shipped `python -m civic_gateway` launcher now works from an explicit JSON
config without Python injections. Its concrete OpenAI-compatible HTTPS transport
accepts **only explicit free routes**. See Operator Launch below. The lower-level
generic contract still has paid-budget fields, but concrete grants reject paid
routes regardless of configured budget.

## Operator Launch

1. Make both packages importable in the Scanner-side PowerShell terminal:

   ```powershell
   $env:PYTHONPATH = 'F:\новости;F:\ChatGPT-API-Scanner'
   ```

2. Prepare a reviewed JSON grant using the template below. Replace the `.invalid`
   endpoint, model/provider/account IDs and expiry with exact approved values.
   `expires_at` is UTC Unix seconds, between now and 24 hours from now. For a
   one-hour expiry, calculate `[DateTimeOffset]::UtcNow.ToUnixTimeSeconds() + 3600`.
   The operator must assert that this exact endpoint/model/account combination is
   free: the launcher does not discover or verify provider prices. Never authorize
   a billed route simply by setting its configured price to zero.

3. Validate the explicit config before provisioning any secrets:

   ```powershell
   python -m civic_gateway --config 'F:\ChatGPT-API-Scanner\civic_gateway\approved.json' --check
   ```

   `--check` reads only that bounded JSON file. It validates the frozen snapshot,
   endpoint allowlist, free pricing, quotas, expiry and secret-reference names.
   It does not read secret values, access the network, create a server, or read/
   write the admission journal. It does not prove credentials/endpoints work or
   that the snapshot ID is still unused. Invalid config exits with code 2.

4. Provision only the named environment variables into the Scanner-side process
   through the operator's credential-management mechanism. Never put values in
   JSON, command arguments, source control, documentation or logs. The gateway
   token must differ from every provider token. References are explicit versioned
   names matching `CIVIC_[A-Z0-9_]+_V<number>`, not generic `OPENAI_API_KEY` or key
   pools. Each named variable is read once and frozen. Changing the environment or
   discovering new keys cannot alter a running grant. No environment enumeration,
   prefix search, fallback variable or config hot reload occurs.

5. Start only the isolated gateway:

   ```powershell
   python -m civic_gateway --config 'F:\ChatGPT-API-Scanner\civic_gateway\approved.json' --serve
   ```

   This validates config and named secrets, constructs the transport, commits a
   one-use snapshot claim, then binds `127.0.0.1:<listen_port>`. No provider call
   occurs at startup; only authenticated, authorized inference can trigger one.
   Stop with Ctrl+C. Neither Scanner core nor its unrestricted admin router runs.

Template (intentionally non-runnable until the operator replaces the placeholder
endpoint/model and `expires_at: 1`; secret VALUES never belong in this file):

```json
{
  "version": 1,
  "auto_discover": false,
  "snapshot": {
    "snapshot_id": "operator-approved-run-001",
    "expires_at": 1,
    "routes": [{
      "route_id": "approved-extractor",
      "provider": "approved-provider",
      "account_id": "owned-account-1",
      "model": "exact-approved-model-id",
      "capabilities": ["extract"],
      "max_input_tokens": 8192,
      "max_output_tokens": 256,
      "pricing": "free",
      "input_microusd_per_million": 0,
      "output_microusd_per_million": 0,
      "quota_tokens": 100000,
      "quota_cost_microusd": 0
    }]
  },
  "endpoint_allowlist": ["https://api.provider.invalid/v1/chat/completions"],
  "bindings": [{
    "route_id": "approved-extractor",
    "endpoint": "https://api.provider.invalid/v1/chat/completions",
    "credential_env": "CIVIC_PROVIDER_ACCOUNT1_V1"
  }],
  "gateway_token_env": "CIVIC_GATEWAY_TOKEN_V1",
  "listen_port": 19390
}
```

All fields are required; unknown fields (including inline API keys, headers or a
journal-path override) are rejected. Every route has exactly one binding. URLs
must be exact canonical HTTPS DNS-host URLs on default port 443, with a path ending
`/chat/completions`: no wildcards, credentials, query, fragment, encoded path, dot
segments, IP literal or alternate port. A binding's URL must be in the explicit
allowlist. No endpoint is taken from request input. Use a direct provider endpoint:
a downstream router that silently switches accounts or billed models violates
the operator grant even if TLS is valid.

### Restart Journal

The launcher durably claims each snapshot ID once in
`F:\ChatGPT-API-Scanner\civic_gateway\state\admissions.sqlite3`, before opening
the listening socket. SQLite uses a unique snapshot-ID key, `BEGIN IMMEDIATE`,
and FULL synchronization. Simultaneous claims cannot both succeed. Records contain
only snapshot ID, config SHA-256 and claim time, never secrets, inputs or output.
Database corruption/unavailability fails closed.

Subsequent `--serve` with the same snapshot ID fails with
`snapshot_already_claimed`, even after clean exit, crash, zero inference calls,
failed port binding, or editing the config's quotas/endpoints. No reset flag,
automatic release or configurable journal location exists. Missing/invalid
secrets are rejected before claiming; failures after claiming consume the ID.

After reconciling prior activity and remaining account allocations, an operator
must issue a fresh approved snapshot ID and reviewed quotas. Never mint a new ID
to evade a quota, provider block, active process or unresolved call. Old IDs remain
consumed permanently. If precise usage cannot be recovered, treat the entire prior
grant allocation as consumed. This is a one-use admission barrier, NOT a per-request
billing ledger or automatic reconciliation system. Preserve/back up the entire
`state` directory and protect it with OS ACLs. Deleting/replacing it, copying a
deployment without its state, or using independent journals defeats local
coordination and is unsupported. No parent-owned Civic database is used.

## Concrete HTTPS Behavior

The transport sends one POST with the exact granted model, two fixed-shape system/
user messages, `response_format.type=json_schema`, `strict=true`, `stream=false`,
`n=1`, and `max_tokens` equal to the request's output ceiling. No tools, input refs,
files, arbitrary provider headers, fallback options or SDK retry machinery are
sent. TLS hostname/certificate verification uses the default SSL context.
Unsupported structured-output models or token-parameter dialects fail; there is
no downgrade to unstructured output or another token-limit parameter.

Input admission counts every UTF-8 byte in the serialized wire request (including
escaped Unicode, system text, schema and JSON wrappers), the schema again, and a
4096-token wrapper/special-token allowance. This intentionally overestimates
ordinary byte-based BPE input instead of using an optimistic characters-per-token
ratio. Requests below the bound fail before secret/provider dispatch. It is not a
certified bound for arbitrary server-side prompt expansion or non-byte-based
tokenizers; such routes must not be approved. Actual usage is still checked
against the request ceilings. Paid routes remain disabled because this bound is
not a billing guarantee.

A successful response requires exactly one assistant choice, index zero, finish
reason `stop`, string content, no tool/function calls or refusal, and explicit
integer `usage.prompt_tokens`, `completion_tokens`, `total_tokens` with a matching
sum. The response's actual `model` is retained and must exactly equal the grant:
model aliases/revisions are not rewritten. If a response reports `provider`, it is
retained and checked too; otherwise `actual_provider` identifies the approved
HTTPS endpoint, not independently verified downstream routing. Missing identity/
usage, streaming, truncation and schema errors are not repaired or retried.

Responses are capped at 1 MiB; content length, truncation and compression are
checked. Retry-After seconds/HTTP dates are normalized. A 429 with structured
error code/type `insufficient_quota`, `billing_hard_limit_reached`, or
`credits_exhausted` becomes exhausted quota (402 internally). Ordinary 429 stays
rate limiting. Provider error text is discarded. 3xx replies fail and are never
followed, even to another allowlisted URL.

The caller's total deadline includes DNS/TLS/connect, headers and body. A single
daemon worker bounds caller wait and aborts the active socket on timeout. If OS
DNS cannot be interrupted, that worker remains occupied and further calls fail
closed until it exits. Workers cannot accumulate and DNS completion after expiry
cannot trigger a late POST. An already accepted POST cannot be undone; ambiguous
completion retains the full token reservation.

## Files and Dependencies

- `integrations/fcm_gateway.py`: shared strict contract, immutable authorization
  snapshot, Civic client and fixed-path HTTP transport.
- `F:\ChatGPT-API-Scanner\civic_gateway\adapter.py`: single-attempt Scanner-side
  enforcement and injected provider/secret/token-counting contracts.
- `F:\ChatGPT-API-Scanner\civic_gateway\server.py`: opt-in authenticated loopback
  HTTP server. No service is started by this implementation or its imports.
- `tests/test_fcm_gateway.py`: mock-provider and ephemeral-loopback tests only.
- `F:\ChatGPT-API-Scanner\civic_gateway\openai_transport.py`: concrete bounded
  HTTPS transport, fixed renderer and conservative input counter.
- `F:\ChatGPT-API-Scanner\civic_gateway\launcher.py` and `__main__.py`: config,
  explicit environment resolution, SQLite claims, `--check` and `--serve`.

Python 3.10+ and `jsonschema` are required (`requirements-lock.txt` already includes
jsonschema). The deployment environment must have both checkout roots on
`PYTHONPATH`, or install/package the two packages equivalently. The standalone
Scanner adapter imports only Civic's contract, not Scanner core/configuration.
Tests locate the sibling Scanner directory; adapter/HTTP tests are explicitly
skipped when it is absent. Pure Civic contract tests still run.

## Authorization and Money

An operator supplies an `AuthorizedSnapshot(snapshot_id, expires_at, routes)` to
both ends through a trusted provisioning channel. It contains only non-secret
route/provider/account/model identifiers, allowed capabilities, token ceilings,
explicit token prices, and account quota allocations. The request must name an
exact route in that snapshot; the capability does not initiate route selection.
IDs are exact, case-sensitive strings. Snapshot objects copy route sequences and
are frozen. There is no snapshot refresh or key-catalog discovery endpoint.

`Route` fields are `route_id`, `provider`, `account_id`, `model`, `capabilities`,
`max_input_tokens`, `max_output_tokens`, `pricing`,
`input_microusd_per_million`, `output_microusd_per_million`, `quota_tokens`, and
`quota_cost_microusd`. Quotas are total allocations for this gateway instance and
snapshot, not inferred provider balances. All route aliases for the same
`(provider, account_id)` must have identical quota allocations and share spending
and backoff. An explicit `free` route requires both prices to be exactly zero.
Unknown paid rates or missing quota limits fail before secret resolution or I/O.

Money is integer micro-USD (1 USD = 1,000,000 micro-USD). Prices are micro-USD per
million tokens. Cost is rounded up once after summing input/output charges:

```text
ceil((input_tokens * input_rate + output_tokens * output_rate) / 1,000,000)
```

The full request input/output ceilings are reserved before provider dispatch.
Trusted, valid usage releases unused reservation, even if output schema validation
fails. Timeouts, transport errors, malformed usage, identity mismatches and other
ambiguous completions retain the full reservation. Unknown actual usage/cost is
reported as null, never zero. Paid providers with other billing dimensions,
unknown hidden/reasoning tokens, or non-authoritative price bounds must NOT be
authorized using this simple token-pricing contract.

## Request Contract

Only `POST /v1/civic/infer` is exposed. The HTTP transport takes an explicit origin
without paths, credentials, queries or fragments. HTTP is numeric loopback-only;
remote clients require HTTPS at a separately provisioned authenticated front door.
No redirects, environment proxy settings, admin paths or model-list requests.

```python
import time
from integrations.fcm_gateway import FCMGatewayClient, HTTPGatewayTransport

# approved_snapshot and civic_front_door_token are injected by the application.
# The front-door token is NOT a Scanner admin token or a provider API key.
client = FCMGatewayClient(
    approved_snapshot,
    HTTPGatewayTransport("http://127.0.0.1:19390", civic_front_door_token),
)
request = {
    "request_id": "extract-001",
    "snapshot_id": approved_snapshot.snapshot_id,
    "route_id": "approved-extractor",
    "capability": "extract",
    "input_refs": ["evidence:123", "revision:456"],
    "input": "Caller-supplied evidence text, not a fetch instruction.",
    "response_schema": {
        "type": "object",
        "properties": {"answer": {"type": "string"}},
        "required": ["answer"],
        "additionalProperties": False,
    },
    "deadline": time.time() + 30,
    "max_input_tokens": 8192,
    "max_output_tokens": 256,
    "max_cost_microusd": 0,
    "stream": False,
}
result = client.infer(request)
```

Every shown field is required; extra envelope fields are rejected. Input refs are
opaque provenance identifiers only: the adapter never reads files, DBs, URLs or
attachments referenced there. Deadlines are absolute Unix UTC seconds, must be
in the future, at most 120 seconds away, and within snapshot expiry. The server
also uses monotonic time after admission. Streaming, tool execution and markdown
JSON recovery are not supported.

Response schemas use a deliberately bounded subset of JSON Schema 2020-12:
`type`, `properties`, `required`, `additionalProperties`, `items`, `enum`,
`minimum`, `maximum`, `minLength`, `maxLength`, `minItems`, `maxItems`.
Root type must be object, every object must close additional properties, and every
array must declare an item schema. Types are single names, not type unions.
References, combinators, regexes, defaults and unknown keywords are rejected,
including nested references. No schema network resolution can occur. Limits:
1 MiB JSON messages, 16 KiB schema, 12 schema nesting levels, 32 JSON nesting
levels, 128 input refs, and 262,144 input characters. Duplicate JSON keys,
nonfinite numbers, boolean/inexact integer token counts and extra response fields
are rejected. JSON structured output is validated on BOTH sides.

## Optional Low-Level Injection

The launcher supplies all three implementations below. Injection remains an
extension/testing API and does NOT claim the launcher's journal. Production
operators should use `--serve`, not bypass its admission gate.

```python
from civic_gateway import Binding, Gateway
from civic_gateway.server import make_server

gateway = Gateway(
    approved_snapshot,
    (Binding("approved-extractor", "owned-account:credential-version-7"),),
    secret_resolver=resolve_exact_owned_credential,
    transport=single_attempt_provider_transport,
    count_input_tokens=count_complete_provider_input,
)
server = make_server(gateway, civic_front_door_token, port=19390)
try:
    server.serve_forever()
finally:
    server.server_close()
```

Custom functions are extension points, not needed by the launcher. Do not point
a transport at Scanner's unrestricted router: its fallback/credential-selection
behavior would violate this contract.

`secret_resolver(ref) -> str` must resolve an exact, operator-approved credential
version from owned credentials on the Scanner side. Never use `latest`, key pool
lookups, discovered exports, or a resolver that replaces failed keys. Binding
route IDs must exactly match the snapshot; account aliases must share one binding.
Generic resolution is lazy after admission and cached for the gateway lifetime.
The launcher additionally freezes all approved named environment values before
startup, so requests cannot pick up replacements. No credential value appears in
the Civic snapshot or request. Tests use synthetic environment mappings only;
no live provider key value or credential store was read.

`count_input_tokens(route, request) -> int` must count or conservatively bound the
complete provider input, including schema/system wrappers. It must use the same
rendering/tokenization as the provider transport. There is no approximate default
that could accidentally admit an over-budget paid request.

`transport(ProviderCall) -> ProviderReply` receives the fixed route, request, secret
and remaining timeout. It MUST perform at most one non-streaming inference, use
exactly that account/provider/model, enforce total connect/write/read timeout and
both token ceilings, and disable provider SDK retries/fallbacks. It must not log
credentials, request bodies, headers or exception strings. Return actual provider
metadata rather than copying requested model names to hide routing changes.
The normalized successful reply body is UTF-8 JSON bytes:

```json
{
  "provider": "actual-provider",
  "model": "actual-model",
  "output": {"answer": "structured output"},
  "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15}
}
```

`ProviderReply` also carries HTTP-like status, content type, optional normalized
`retry_after_seconds`, and `backoff_scope` (`account` by default or `provider`).
Map exhausted provider quota to 402, invalid credentials to 401/403, throttling
to 429, transient unavailability to 5xx. Convert Retry-After dates or SDK-specific
quota errors inside the trusted provider adapter. Error bodies are discarded.
No client request controls the secret reference, endpoint or backoff scope.

## Results, Backoff and Bounds

Results always include request/snapshot/route IDs, `ok`, schema-validated `output`,
`selected` provider/model, `actual_provider`, `actual_model`, `attempts` (0 or 1),
`usage`, `cost_microusd`, `reserved_cost_microusd`, total `latency_ms`, and `error`.
Actual identity stays null if no trustworthy provider metadata was received.
A mismatch is reported as an error with the observed identity, not a success.
Errors contain only a fixed `code`, `retryable` hint and `retry_after_seconds`.
Local client validation/HTTP failures raise `GatewayError` with a sanitized code;
valid gateway inference failures return the normalized error result.

Account/provider backoff is shared across routes, never across interchangeable
keys. 429/5xx failures honor a supplied nonnegative delay (30 seconds if absent).
Quota and auth failures block through snapshot expiry. No sleeping or retries
occur inside a request. Timeout/transport ambiguity also imposes account backoff.
Any later attempt requires an explicit caller decision and a new request ID;
retryable is not a promise that the previous operation was unbilled.

Dispatch is serial and concurrent direct calls fail with `gateway_busy`. The
loopback server has a backlog of eight and closes connections after responses.
Request header/body ingestion has a five-second wall-time cutoff; body size is
bounded, as are HTTP parser lines/header counts. The inference transport must
honor its remaining total deadline. Late replies are rejected, but Python cannot
forcibly cancel an arbitrary injected callable or undo an already billed call.

The request-ID set is capped at 10,000 admissions by default; IDs are never evicted
and reused for dispatch. Repeated admitted IDs fail closed, rather than replaying
output or rebilling. Quotas, backoff, duplicate tracking and credential caching
are in-memory and process-local. The launcher's persistent one-use claim prevents
restarting that snapshot and resetting these counters. Do not run independently
granted instances against the same allocation. Provision a fresh reviewed snapshot
with remaining budgets only after reconciliation. The parent-owned DB/bus must
provide any more advanced durable per-request quota/idempotency coordination.

Provider error details and exception strings are never forwarded/logged. Exact
resolved-secret occurrences in output/metadata are rejected rather than partially
redacted into schema-invalid data; the front-door token is also protected from
echo. This is not a general DLP filter for unknown or transformed/encoded secrets.
The token-authenticated loopback server is a minimal adapter, not production TLS,
multi-tenant authentication, an Internet-facing hardened HTTP server, or a secret
vault. Keep tokens distinct and inject them through a trusted launcher.

## Verification and Unimplemented Work

Run from Civic: `python -m pytest tests/test_fcm_gateway.py -q`.
Tests use synthetic credentials, fake clocks/transports, and new ephemeral
loopback servers; they do not contact a running Scanner or any provider.

Parent-owned Civic wiring is implemented: `runtime/civic.py` builds an
`FCMGatewayClient` for `worker_once`, commits MAS completion/artifact and the
extraction outbox atomically, then accepts claims through the replay-safe transfer
path. The parent reports mock end-to-end completion; `tests/test_civic_runtime.py`
covers source-to-task extraction, claim acceptance and replay with a mocked client.
This is no longer an entirely unimplemented bus/DB integration. Parent runtime,
bus, DB, election and collector code remain outside this gateway's edit scope.

Live readiness is still pending: no operator-approved live config/credentials
have been provisioned and no live provider inference has been verified. A passing
`--check` proves config validity only, not live readiness. A `--serve` startup
message proves the snapshot was claimed and the isolated listener started, not
provider availability. There is intentionally no health/catalog readiness probe;
do not send a live inference merely to establish readiness without authorization.

Not implemented: paid-provider support, exact model-specific tokenizers,
credential-vault adapters beyond environment references, signed/automated grant
provisioning, automatic reconciliation/restart, durable per-request billing ledger,
distributed gateway quota/idempotency coordination, installer packaging, and
production TLS/auth deployment. Only inference is exposed: health/catalog/
admin endpoints are intentionally absent. No live endpoint, key, pricing or
inference behavior has been verified. No existing Scanner core, key files or
services were modified or contacted.
