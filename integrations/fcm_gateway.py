"""Civic's credential-free, snapshot-pinned inference gateway contract.

No Scanner imports, discovery, admin endpoints, retries, or environment reads.
Money is integer micro-USD; deadlines are absolute Unix seconds in UTC.
"""

from __future__ import annotations

import http.client
import json
import math
import re
import socket
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol
from urllib.parse import urlsplit

from jsonschema import Draft202012Validator
from jsonschema.validators import extend

StrictValidator = extend(Draft202012Validator, type_checker=Draft202012Validator.TYPE_CHECKER.redefine(
    "integer", lambda checker, value: type(value) is int))

MAX_BYTES = 1_048_576
MAX_SCHEMA_BYTES = 16_384
MAX_SECONDS = 120.0
INFER_PATH = "/v1/civic/infer"
ID = r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$"


class GatewayError(Exception):
    """Only fixed, public error codes may cross the gateway boundary."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def encode(value: Any, limit: int = MAX_BYTES) -> bytes:
    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > 32:
            raise GatewayError("invalid_json")
        if isinstance(item, dict):
            if any(not isinstance(key, str) for key in item):
                raise GatewayError("invalid_json")
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
    try:
        data = json.dumps(value, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except (ValueError, TypeError, RecursionError):
        raise GatewayError("invalid_json") from None
    if len(data) > limit:
        raise GatewayError("payload_too_large")
    return data


def decode(data: bytes) -> Any:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        if len(data) > MAX_BYTES:
            raise GatewayError("payload_too_large")
        value = json.loads(data, object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        encode(value)
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise GatewayError("invalid_json") from None


def validate(schema: dict, value: Any, code: str) -> None:
    try:
        encode(value)
        if not StrictValidator(schema).is_valid(value):
            raise GatewayError(code)
    except (ValueError, TypeError, RecursionError):
        raise GatewayError(code) from None


def object_schema(properties: dict, *, optional: tuple = ()) -> dict:
    return {"type": "object", "properties": properties,
            "required": [key for key in properties if key not in optional],
            "additionalProperties": False}


IDENTIFIER = {"type": "string", "pattern": ID}
COUNT = {"type": "integer", "minimum": 0, "maximum": 10**15}
TOKENS = {"type": "integer", "minimum": 1, "maximum": 1_000_000}
REQUEST_SCHEMA = object_schema({
    "request_id": IDENTIFIER, "snapshot_id": IDENTIFIER, "route_id": IDENTIFIER,
    "capability": IDENTIFIER,
    "input_refs": {"type": "array", "minItems": 1, "maxItems": 128,
                   "uniqueItems": True, "items": IDENTIFIER},
    "input": {"type": "string", "minLength": 1, "maxLength": 262_144},
    "response_schema": {"type": "object"},
    "deadline": {"type": "number", "exclusiveMinimum": 0},
    "max_input_tokens": TOKENS, "max_output_tokens": TOKENS,
    "max_cost_microusd": COUNT, "stream": {"const": False},
})
USAGE_SCHEMA = object_schema({
    "input_tokens": COUNT, "output_tokens": COUNT, "total_tokens": COUNT,
})
ERROR_CODES = (
    "invalid_json", "payload_too_large", "invalid_request", "invalid_schema",
    "unknown_snapshot", "snapshot_expired", "unknown_route", "capability_denied",
    "deadline_exceeded", "token_limit", "cost_unknown", "limit_unknown",
    "cost_limit", "quota_exceeded", "account_backoff", "duplicate_request",
    "secret_unavailable", "secret_redacted", "provider_timeout", "transport_error",
    "rate_limited", "provider_quota", "provider_auth", "provider_unavailable",
    "provider_error", "provider_response_invalid", "route_mismatch",
    "usage_invalid", "output_schema_invalid", "gateway_response_invalid",
    "gateway_unavailable", "gateway_busy",
)
NULL_ID = {"anyOf": [IDENTIFIER, {"type": "null"}]}
RESULT_SCHEMA = object_schema({
    "request_id": NULL_ID, "snapshot_id": NULL_ID, "route_id": NULL_ID,
    "ok": {"type": "boolean"}, "output": {},
    "selected": {"anyOf": [object_schema({"provider": IDENTIFIER, "model": IDENTIFIER}),
                           {"type": "null"}]},
    "actual_provider": NULL_ID, "actual_model": NULL_ID,
    "attempts": {"type": "integer", "minimum": 0, "maximum": 1},
    "usage": {"anyOf": [USAGE_SCHEMA, {"type": "null"}]},
    "cost_microusd": {"anyOf": [COUNT, {"type": "null"}]},
    "reserved_cost_microusd": COUNT,
    "latency_ms": {"type": "number", "minimum": 0},
    "error": {"anyOf": [object_schema({
        "code": {"enum": list(ERROR_CODES)}, "retryable": {"type": "boolean"},
        "retry_after_seconds": {"type": "number", "minimum": 0},
    }), {"type": "null"}]},
})


def check_output_schema(schema: dict) -> None:
    """Bounded JSON Schema subset: no refs, regexes, combinators or remote I/O."""
    encode(schema, MAX_SCHEMA_BYTES)
    allowed = {"type", "properties", "required", "additionalProperties", "items",
               "enum", "minimum", "maximum", "minLength", "maxLength",
               "minItems", "maxItems"}

    def visit(node, depth=0):
        if not isinstance(node, dict) or depth > 12 or set(node) - allowed:
            raise GatewayError("invalid_schema")
        kind = node.get("type")
        if kind not in ("object", "array", "string", "integer", "number", "boolean", "null"):
            raise GatewayError("invalid_schema")
        if kind == "object":
            if node.get("additionalProperties") is not False:
                raise GatewayError("invalid_schema")
            for child in node.get("properties", {}).values():
                visit(child, depth + 1)
        elif kind == "array":
            visit(node.get("items"), depth + 1)
        # Reject schema-valued keywords outside their intended type, too.
        if kind != "object" and set(node) & {"properties", "required", "additionalProperties"}:
            raise GatewayError("invalid_schema")
        if kind != "array" and "items" in node:
            raise GatewayError("invalid_schema")

    try:
        Draft202012Validator.check_schema(schema)
        visit(schema)
        if schema["type"] != "object":
            raise GatewayError("invalid_schema")
    except GatewayError:
        raise
    except Exception:
        raise GatewayError("invalid_schema") from None


@dataclass(frozen=True)
class Route:
    route_id: str
    provider: str
    account_id: str
    model: str
    capabilities: tuple[str, ...]
    max_input_tokens: int
    max_output_tokens: int
    pricing: str  # "free" is an explicit operator assertion, never inferred.
    input_microusd_per_million: int | None
    output_microusd_per_million: int | None
    quota_tokens: int | None
    quota_cost_microusd: int | None

    def __post_init__(self):
        for value in (self.route_id, self.provider, self.account_id, self.model):
            if not isinstance(value, str) or not re.fullmatch(ID, value):
                raise ValueError("invalid route identifier")
        object.__setattr__(self, "capabilities", tuple(self.capabilities))
        if not self.capabilities or any(not isinstance(c, str) or not re.fullmatch(ID, c)
                                        for c in self.capabilities):
            raise ValueError("invalid capabilities")
        if self.pricing not in ("free", "paid"):
            raise ValueError("invalid pricing")
        for value in (self.max_input_tokens, self.max_output_tokens):
            if type(value) is not int or not 1 <= value <= 1_000_000:
                raise ValueError("invalid token ceiling")
        for value in (self.input_microusd_per_million, self.output_microusd_per_million,
                      self.quota_tokens, self.quota_cost_microusd):
            if value is not None and (type(value) is not int or not 0 <= value <= 10**15):
                raise ValueError("invalid budget")
        if self.pricing == "free" and (self.input_microusd_per_million,
                                       self.output_microusd_per_million) != (0, 0):
            raise ValueError("free route requires explicit zero prices")

    def cost(self, input_tokens: int, output_tokens: int) -> int:
        if self.input_microusd_per_million is None or self.output_microusd_per_million is None:
            raise GatewayError("cost_unknown")
        numerator = (input_tokens * self.input_microusd_per_million
                     + output_tokens * self.output_microusd_per_million)
        return (numerator + 999_999) // 1_000_000


@dataclass(frozen=True)
class AuthorizedSnapshot:
    snapshot_id: str
    expires_at: float
    routes: tuple[Route, ...]

    def __post_init__(self):
        object.__setattr__(self, "routes", tuple(self.routes))
        if not re.fullmatch(ID, self.snapshot_id) or not math.isfinite(self.expires_at):
            raise ValueError("invalid snapshot")
        if len({r.route_id for r in self.routes}) != len(self.routes):
            raise ValueError("duplicate route")
        budgets = {}
        for route in self.routes:
            account = (route.provider, route.account_id)
            budget = (route.quota_tokens, route.quota_cost_microusd)
            if account in budgets and budgets[account] != budget:
                raise ValueError("account aliases must share snapshot quotas")
            budgets[account] = budget

    def authorize(self, request: dict, now: float) -> Route:
        validate(REQUEST_SCHEMA, request, "invalid_request")
        check_output_schema(request["response_schema"])
        if request["snapshot_id"] != self.snapshot_id:
            raise GatewayError("unknown_snapshot")
        if now >= self.expires_at:
            raise GatewayError("snapshot_expired")
        if not now < request["deadline"] <= min(now + MAX_SECONDS, self.expires_at):
            raise GatewayError("deadline_exceeded")
        route = next((r for r in self.routes if r.route_id == request["route_id"]), None)
        if route is None:
            raise GatewayError("unknown_route")
        if request["capability"] not in route.capabilities:
            raise GatewayError("capability_denied")
        if (request["max_input_tokens"] > route.max_input_tokens
                or request["max_output_tokens"] > route.max_output_tokens):
            raise GatewayError("token_limit")
        reserve = route.cost(request["max_input_tokens"], request["max_output_tokens"])
        if route.quota_tokens is None or route.quota_cost_microusd is None:
            raise GatewayError("limit_unknown")
        if reserve > request["max_cost_microusd"]:
            raise GatewayError("cost_limit")
        if (request["max_input_tokens"] + request["max_output_tokens"] > route.quota_tokens
                or reserve > route.quota_cost_microusd):
            raise GatewayError("quota_exceeded")
        return route


def validate_result(result: dict, request: dict, route: Route) -> None:
    validate(RESULT_SCHEMA, result, "gateway_response_invalid")
    if any(result[key] != request[key] for key in ("request_id", "snapshot_id", "route_id")):
        raise GatewayError("gateway_response_invalid")
    if result["selected"] not in (None, {"provider": route.provider, "model": route.model}):
        raise GatewayError("gateway_response_invalid")
    if result["attempts"] == 0:
        if (any(result[key] is not None for key in (
                "actual_provider", "actual_model", "usage", "cost_microusd"))
                or result["reserved_cost_microusd"] != 0):
            raise GatewayError("gateway_response_invalid")
    elif (result["selected"] is None or result["reserved_cost_microusd"] != route.cost(
            request["max_input_tokens"], request["max_output_tokens"])):
        raise GatewayError("gateway_response_invalid")
    usage = result["usage"]
    if usage is not None:
        if (usage["total_tokens"] != usage["input_tokens"] + usage["output_tokens"]
                or usage["input_tokens"] > request["max_input_tokens"]
                or usage["output_tokens"] > request["max_output_tokens"]
                or result["cost_microusd"] != route.cost(usage["input_tokens"], usage["output_tokens"])
                or result["cost_microusd"] > request["max_cost_microusd"]):
            raise GatewayError("gateway_response_invalid")
    elif result["cost_microusd"] is not None:
        raise GatewayError("gateway_response_invalid")
    if result["ok"]:
        if (result["error"] is not None or result["attempts"] != 1
                or result["actual_provider"] != route.provider or result["actual_model"] != route.model
                or result["selected"] is None or result["usage"] is None):
            raise GatewayError("gateway_response_invalid")
        validate(request["response_schema"], result["output"], "output_schema_invalid")
    elif result["error"] is None or result["output"] is not None:
        raise GatewayError("gateway_response_invalid")


class GatewayTransport(Protocol):
    def __call__(self, request: dict, *, timeout: float) -> dict:
        """POST only to the dedicated gateway; no retries or alternate routes."""
        ...


class FCMGatewayClient:
    def __init__(self, snapshot: AuthorizedSnapshot, transport: GatewayTransport,
                 *, clock: Callable[[], float] = time.time):
        self.snapshot, self.transport, self.clock = snapshot, transport, clock

    def infer(self, request: dict) -> dict:
        request = decode(encode(request))
        now = self.clock()
        route = self.snapshot.authorize(request, now)
        try:
            result = self.transport(request, timeout=request["deadline"] - now)
        except GatewayError as exc:
            raise GatewayError(exc.code if exc.code in ERROR_CODES else "transport_error") from None
        except Exception:
            raise GatewayError("transport_error") from None
        if self.clock() >= request["deadline"]:
            raise GatewayError("deadline_exceeded")
        validate_result(result, request, route)
        return result


class HTTPGatewayTransport:
    """Explicit isolated origin, fixed path, no redirects, proxies or discovery.

    The bearer is a dedicated Civic gateway token, NEVER a provider/admin key.
    HTTP is loopback-only; remote deployment requires HTTPS and authentication.
    """

    def __init__(self, origin: str, gateway_token: str):
        parsed = urlsplit(origin)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.path not in ("", "/") or parsed.query or parsed.fragment
                or parsed.username is not None or parsed.password is not None
                or (parsed.scheme == "http" and parsed.hostname not in ("127.0.0.1", "::1"))):
            raise ValueError("dedicated gateway origin required")
        if not re.fullmatch(r"[A-Za-z0-9_-]{32,256}", gateway_token):
            raise ValueError("invalid gateway token")
        self._origin, self._token = parsed, gateway_token

    def __call__(self, request: dict, *, timeout: float) -> dict:
        if not 0 < timeout <= MAX_SECONDS:
            raise GatewayError("deadline_exceeded")
        end = time.monotonic() + timeout
        cls = http.client.HTTPSConnection if self._origin.scheme == "https" else http.client.HTTPConnection
        connection = cls(self._origin.hostname, self._origin.port, timeout=timeout)
        transport_socket = None

        def expire():
            active_socket = connection.sock or transport_socket
            if active_socket is not None:
                try:
                    active_socket.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

        # Socket timeouts alone allow a peer to keep a header read alive by
        # trickling bytes. Shut down the active socket at the total deadline.
        timer = threading.Timer(timeout, expire)
        timer.daemon = True
        timer.start()
        try:
            connection.request("POST", INFER_PATH, body=encode(request), headers={
                "Authorization": "Bearer " + self._token,
                "Content-Type": "application/json", "Accept": "application/json",
                "Connection": "close",
            })
            remaining = end - time.monotonic()
            if remaining <= 0:
                raise GatewayError("deadline_exceeded")
            connection.sock.settimeout(remaining)
            transport_socket = connection.sock
            response = connection.getresponse()
            if response.status != 200 or response.getheader("Content-Type", "").split(";")[0] != "application/json":
                raise GatewayError("gateway_unavailable")
            chunks, size = [], 0
            while not response.isclosed():
                remaining = end - time.monotonic()
                if remaining <= 0:
                    raise GatewayError("deadline_exceeded")
                transport_socket.settimeout(remaining)
                chunk = response.read1(min(65_536, MAX_BYTES + 1 - size))
                if not chunk:
                    break
                chunks.append(chunk)
                size += len(chunk)
                if size > MAX_BYTES:
                    raise GatewayError("payload_too_large")
            return decode(b"".join(chunks))
        except GatewayError:
            raise
        except TimeoutError:
            raise GatewayError("deadline_exceeded") from None
        except Exception:
            if time.monotonic() >= end:
                raise GatewayError("deadline_exceeded") from None
            raise GatewayError("transport_error") from None
        finally:
            timer.cancel()
            connection.close()
