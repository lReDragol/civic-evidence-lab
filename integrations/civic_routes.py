"""Explicit free-route grants and bounded client-side health, never key discovery.

Each infer call dispatches once at most. Selection is for a NEW operation, not a
retry/fallback chain. The isolated gateway remains the quota authority.
"""

from __future__ import annotations

import math
import os
from pathlib import Path
import re
import threading
import time
from dataclasses import asdict, dataclass
from typing import Callable, Mapping

from integrations.fcm_gateway import (
    COUNT, IDENTIFIER, MAX_BYTES, TOKENS, AuthorizedSnapshot, FCMGatewayClient,
    GatewayError, HTTPGatewayTransport, Route, decode, encode, object_schema,
    validate,
)

ENV_PATTERN = r"^CIVIC_[A-Z0-9_]{1,100}_V[1-9][0-9]{0,5}$"
FREE_ROUTE_SCHEMA = object_schema({
    "route_id": IDENTIFIER, "provider": IDENTIFIER, "account_id": IDENTIFIER,
    "model": IDENTIFIER,
    "capabilities": {"type": "array", "items": IDENTIFIER, "minItems": 1,
                     "maxItems": 32, "uniqueItems": True},
    "max_input_tokens": TOKENS, "max_output_tokens": TOKENS,
    "pricing": {"const": "free"},
    "input_microusd_per_million": {"type": "integer", "const": 0},
    "output_microusd_per_million": {"type": "integer", "const": 0},
    "quota_tokens": COUNT, "quota_cost_microusd": {"type": "integer", "const": 0},
})
CLIENT_CONFIG_SCHEMA = object_schema({
    "snapshot_id": IDENTIFIER,
    "expires_at": {"type": "number", "exclusiveMinimum": 0},
    "routes": {"type": "array", "items": FREE_ROUTE_SCHEMA, "minItems": 1, "maxItems": 64},
    "origin": {"type": "string", "minLength": 1, "maxLength": 2048},
    "gateway_token_env": {"type": "string", "pattern": ENV_PATTERN},
    "route_id": IDENTIFIER,
    "max_cost_microusd": {"type": "integer", "const": 0},
}, optional=("max_cost_microusd",))


def _now(clock: Callable[[], float]) -> float:
    try:
        value = clock()
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise ValueError()
        return value
    except Exception:
        raise GatewayError("invalid_clock") from None


@dataclass(frozen=True, repr=False)
class ClientConfig:
    snapshot: AuthorizedSnapshot
    origin: str
    gateway_token_env: str
    route_id: str

    def as_dict(self) -> dict:
        return dict(snapshot_id=self.snapshot.snapshot_id, expires_at=self.snapshot.expires_at,
                    routes=[asdict(route) for route in self.snapshot.routes], origin=self.origin,
                    gateway_token_env=self.gateway_token_env, route_id=self.route_id,
                    max_cost_microusd=0)


def parse_client_config(value: dict, *, now: float | None = None) -> ClientConfig:
    """Validate metadata only; approval is supplied externally, never inferred."""
    try:
        value = decode(encode(value))
        validate(CLIENT_CONFIG_SCHEMA, value, "invalid_client_config")
        snapshot = AuthorizedSnapshot(value["snapshot_id"], value["expires_at"],
                                      tuple(Route(**route) for route in value["routes"]))
        now = _now(time.time if now is None else lambda: now)
        if snapshot.expires_at <= now:
            raise GatewayError("snapshot_expired")
        if snapshot.expires_at > now + 86_400:
            raise GatewayError("snapshot_expiry_invalid")
        if value["route_id"] not in {r.route_id for r in snapshot.routes}:
            raise ValueError()
        if any(r.quota_tokens < r.max_input_tokens + r.max_output_tokens for r in snapshot.routes):
            raise ValueError()
        # Validate the origin without resolving an environment variable or doing I/O.
        HTTPGatewayTransport(value["origin"], "validation_only_not_a_secret_00000000")
        return ClientConfig(snapshot, value["origin"], value["gateway_token_env"], value["route_id"])
    except GatewayError as exc:
        if exc.code in ("snapshot_expired", "snapshot_expiry_invalid", "invalid_clock"):
            raise
        raise GatewayError("invalid_client_config") from None
    except Exception:
        raise GatewayError("invalid_client_config") from None


def load_client_config(path: str | Path, *, now: float | None = None) -> ClientConfig:
    """Read ONLY the exact reviewed file, with a byte limit and no fallback path."""
    try:
        with Path(path).open("rb") as stream:
            raw = stream.read(MAX_BYTES + 1)
        return parse_client_config(decode(raw), now=now)
    except GatewayError as exc:
        if exc.code in ("snapshot_expired", "snapshot_expiry_invalid", "invalid_clock"):
            raise
        raise GatewayError("invalid_client_config") from None
    except Exception:
        raise GatewayError("client_config_unavailable") from None


class CivicRoutes:
    """Serial single-attempt FCM client with health retained across config polls.

    No background work, sleeps, provider access, automatic retries, key resolution,
    or output merging. Reuse this instance across polls. State is process-local;
    parent supervision must not reset it to bypass a block or an attempt limit.
    """

    def __init__(self, *, clock: Callable[[], float] = time.time,
                 max_attempts: int = 128, cooldown_seconds: float = 30):
        if type(max_attempts) is not int or not 1 <= max_attempts <= 10_000:
            raise GatewayError("invalid_route_policy")
        if (type(cooldown_seconds) not in (int, float)
                or not math.isfinite(cooldown_seconds) or not 1 <= cooldown_seconds <= 86_400):
            raise GatewayError("invalid_route_policy")
        self._clock, self._max_attempts = clock, max_attempts
        self._cooldown_seconds = cooldown_seconds
        self._config: ClientConfig | None = None
        self._client: FCMGatewayClient | None = None
        self._lock = threading.RLock()
        self._history: dict[str, bytes] = {}
        self._seen: set[str] = set()
        self._attempts = 0
        self._cooldowns: dict[tuple[str, ...], float] = {}
        self._blocked: set[tuple[str, ...]] = set()

    def configure(self, config: ClientConfig, *, transport=None,
                  environment: Mapping[str, str] | None = None) -> None:
        """Install one complete approved snapshot; never merge with its predecessor.

        transport is trusted test/application injection with the gateway contract.
        Without it, resolve exactly the dedicated gateway token environment name.
        Provider credentials never enter this helper.
        """
        with self._lock:
            self._config = self._client = None
            try:
                config = parse_client_config(config.as_dict(), now=_now(self._clock))
                sid = config.snapshot.snapshot_id
                fingerprint = encode(config.as_dict())
                if sid in self._history and self._history[sid] != fingerprint:
                    raise GatewayError("snapshot_changed")
                if sid not in self._history and len(self._history) >= 64:
                    raise GatewayError("config_history_full")
                if transport is None:
                    env = os.environ if environment is None else environment
                    try:
                        token = env[config.gateway_token_env]
                        transport = HTTPGatewayTransport(config.origin, token)
                    except Exception:
                        raise GatewayError("secret_unavailable") from None
                client = FCMGatewayClient(config.snapshot, transport, clock=self._clock)
                self._history[sid] = fingerprint
                self._config, self._client = config, client
            except GatewayError:
                raise
            except Exception:
                raise GatewayError("invalid_client_config") from None

    def reload(self, path: str | Path, *, transport=None,
               environment: Mapping[str, str] | None = None) -> None:
        """A missing/partial/expired replacement disables dispatch, not health."""
        with self._lock:
            self._config = self._client = None
            config = load_client_config(path, now=_now(self._clock))
            self.configure(config, transport=transport, environment=environment)

    def _active(self) -> ClientConfig:
        if self._config is None:
            raise GatewayError("client_config_unavailable")
        if _now(self._clock) >= self._config.snapshot.expires_at:
            raise GatewayError("snapshot_expired")
        return self._config

    @property
    def snapshot(self) -> AuthorizedSnapshot:
        with self._lock:
            return self._active().snapshot

    @staticmethod
    def _keys(route: Route) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return (route.provider,), (route.provider, route.account_id)

    def _available(self, route: Route, now: float) -> bool:
        return all(key not in self._blocked and self._cooldowns.get(key, 0) <= now
                   for key in self._keys(route))

    def select(self, capability: str, *, max_input_tokens: int, max_output_tokens: int) -> Route:
        """Choose one eligible approved route for a new operation, without I/O."""
        with self._lock:
            config = self._active()
            if self._attempts >= self._max_attempts:
                raise GatewayError("attempt_limit")
            if (not isinstance(capability, str) or not re.fullmatch(IDENTIFIER["pattern"], capability)
                    or any(type(n) is not int or not 1 <= n <= 1_000_000
                           for n in (max_input_tokens, max_output_tokens))):
                raise GatewayError("invalid_request")
            now = _now(self._clock)
            ordered = sorted(config.snapshot.routes, key=lambda r: r.route_id != config.route_id)
            for route in ordered:
                if (capability in route.capabilities and self._available(route, now)
                        and max_input_tokens <= route.max_input_tokens
                        and max_output_tokens <= route.max_output_tokens
                        and max_input_tokens + max_output_tokens <= route.quota_tokens):
                    return route
            raise GatewayError("routes_exhausted")

    def defer(self, route_id: str, *, scope: str, retry_after_seconds: float,
              blocked: bool = False) -> None:
        """Apply trusted account/provider health; cannot clear or shorten a hold.

        blocked=True latches quota/auth denial across expiry and snapshot reloads.
        It requires external operator reconciliation, not an automatic reset.
        """
        with self._lock:
            config = self._active()
            route = next((r for r in config.snapshot.routes if r.route_id == route_id), None)
            if route is None:
                raise GatewayError("unknown_route")
            if (scope not in ("account", "provider") or type(blocked) is not bool
                    or type(retry_after_seconds) not in (int, float)
                    or not math.isfinite(retry_after_seconds) or retry_after_seconds < 0):
                raise GatewayError("invalid_route_health")
            key = (route.provider,) if scope == "provider" else (route.provider, route.account_id)
            if blocked:
                self._blocked.add(key)
            until = _now(self._clock) + max(self._cooldown_seconds, retry_after_seconds)
            self._cooldowns[key] = max(self._cooldowns.get(key, 0), until)

    def _failure(self, route: Route, code: str, retry_after: float = 0) -> None:
        # The wire error omits backoff_scope. Never guess that a 429 or existing
        # backoff is account-only; conservatively hold the whole provider.
        scope = "provider" if code in (
            "rate_limited", "account_backoff", "provider_unavailable",
            "gateway_unavailable", "gateway_busy", "provider_quota", "provider_auth",
        ) else "account"
        keys = self._keys(route)
        key = keys[0] if scope == "provider" else keys[1]
        if code in ("provider_quota", "quota_exceeded", "provider_auth", "secret_unavailable"):
            self._blocked.add(keys[1])
            if scope == "provider":
                self._blocked.add(keys[0])
        until = _now(self._clock) + max(self._cooldown_seconds, retry_after)
        self._cooldowns[key] = max(self._cooldowns.get(key, 0), until)

    def infer(self, request: dict) -> dict:
        """FCMGatewayClient-compatible API: exact route in, one exact result out.

        Failure never dispatches another key/account/model. All transport attempts
        consume the lifetime cap and request ID, even on ambiguous completion.
        """
        if not self._lock.acquire(blocking=False):
            raise GatewayError("gateway_busy")
        try:
            config = self._active()
            request = decode(encode(request))
            route = config.snapshot.authorize(request, _now(self._clock))
            if request["max_cost_microusd"] != 0:
                raise GatewayError("cost_limit")
            if not self._available(route, _now(self._clock)):
                raise GatewayError("route_cooldown")
            if request["request_id"] in self._seen:
                raise GatewayError("duplicate_request")
            if self._attempts >= self._max_attempts:
                raise GatewayError("attempt_limit")
            self._seen.add(request["request_id"])
            self._attempts += 1
            try:
                result = self._client.infer(request)
            except GatewayError as exc:
                self._failure(route, exc.code)
                raise
            if not result["ok"]:
                error = result["error"]
                self._failure(route, error["code"], error["retry_after_seconds"])
            return result
        finally:
            self._lock.release()
