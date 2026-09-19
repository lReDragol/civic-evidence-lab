"""Offline route policy tests; all credentials and transports are synthetic."""

from dataclasses import replace
import json
import threading
from unittest.mock import Mock, patch

import pytest

from integrations.civic_routes import (
    CivicRoutes, load_client_config, parse_client_config,
)
from integrations.fcm_gateway import GatewayError, MAX_BYTES


def route(route_id="primary", provider="mock", account_id="owned-1", **changes):
    return dict(route_id=route_id, provider=provider, account_id=account_id,
                model="exact-model", capabilities=["extract"], max_input_tokens=8192,
                max_output_tokens=256, pricing="free", input_microusd_per_million=0,
                output_microusd_per_million=0, quota_tokens=100_000,
                quota_cost_microusd=0) | changes


def config(*routes, **changes):
    return dict(snapshot_id="approved-1", expires_at=2000, routes=list(routes or (route(),)),
                origin="http://127.0.0.1:19390", gateway_token_env="CIVIC_GATEWAY_TOKEN_V1",
                route_id="primary", max_cost_microusd=0) | changes


def request(**changes):
    return dict(request_id="operation-1", snapshot_id="approved-1", route_id="primary",
                capability="extract", input_refs=["evidence:1"], input="Untrusted evidence",
                response_schema={"type": "object", "properties": {"answer": {"type": "string"}},
                                 "required": ["answer"], "additionalProperties": False},
                deadline=1030, max_input_tokens=8192, max_output_tokens=256,
                max_cost_microusd=0, stream=False) | changes


def result(req, *, code=None, retry_after=0):
    return dict(request_id=req["request_id"], snapshot_id=req["snapshot_id"],
                route_id=req["route_id"], ok=code is None,
                output={"answer": "one exact output"} if code is None else None,
                selected={"provider": "mock", "model": "exact-model"}, attempts=1,
                actual_provider="mock" if code is None else None,
                actual_model="exact-model" if code is None else None,
                usage={"input_tokens": 10, "output_tokens": 5, "total_tokens": 15}
                if code is None else None,
                cost_microusd=0 if code is None else None, reserved_cost_microusd=0,
                latency_ms=1,
                error=None if code is None else dict(code=code, retryable=True,
                                                     retry_after_seconds=retry_after))


def ready(raw=None, *, code=None, retry_after=0, **policy):
    now = [1000.0]
    transport = Mock(side_effect=lambda req, timeout: result(req, code=code, retry_after=retry_after))
    helper = CivicRoutes(clock=lambda: now[0], **policy)
    helper.configure(parse_client_config(raw or config(), now=now[0]), transport=transport)
    return helper, transport, now


def select(helper):
    return helper.select("extract", max_input_tokens=8192, max_output_tokens=256)


def test_parser_roundtrip_is_detached_and_free_only():
    raw = config()
    parsed = parse_client_config(raw, now=1000)
    raw["routes"][0]["model"] = "unapproved-change"
    assert parsed.snapshot.routes[0].model == "exact-model"
    assert parse_client_config(parsed.as_dict(), now=1000) == parsed
    assert "exact-model" not in repr(parsed)


@pytest.mark.parametrize("change", [
    {"api_key": "SYNTHETIC_SECRET"}, {"fallbacks": ["discovered"]},
    {"auto_discover": True}, {"max_cost_microusd": 1}, {"max_cost_microusd": False},
    {"route_id": "unknown"}, {"routes": []}, {"routes": [route()] * 65},
    {"expires_at": float("nan")}, {"expires_at": True},
    {"gateway_token_env": "OPENAI_API_KEY"}, {"gateway_token_env": "CIVIC_KEY_LATEST"},
    {"origin": "http://external.invalid"}, {"origin": "http://127.0.0.1/admin"},
    {"origin": "https://user:SYNTHETIC_SECRET@example.invalid"},
])
def test_invalid_configs_reject_without_echo(change):
    with pytest.raises(GatewayError) as exc:
        parse_client_config(config(**change), now=1000)
    assert str(exc.value) == "invalid_client_config"


@pytest.mark.parametrize("change", [
    {"pricing": "paid"}, {"input_microusd_per_million": 1}, {"quota_tokens": None},
    {"quota_tokens": 1}, {"quota_cost_microusd": None}, {"max_input_tokens": True},
    {"quota_cost_microusd": 1}, {"account_id": ""}, {"credential_env": "CIVIC_KEY_V1"},
])
def test_unprovisioned_routes_reject(change):
    with pytest.raises(GatewayError, match="^invalid_client_config$"):
        parse_client_config(config(route(**change)), now=1000)


def test_duplicate_routes_and_inconsistent_account_aliases_reject():
    for second in (route(), route("alias", quota_tokens=200_000)):
        with pytest.raises(GatewayError, match="invalid_client_config"):
            parse_client_config(config(route(), second), now=1000)


@pytest.mark.parametrize("expires,code", [(1000, "snapshot_expired"), (999, "snapshot_expired"),
                                         (87401, "snapshot_expiry_invalid")])
def test_expiry_is_bounded(expires, code):
    with pytest.raises(GatewayError, match=code):
        parse_client_config(config(expires_at=expires), now=1000)


@pytest.mark.parametrize("raw", [b"{", b'{"snapshot_id":1,"snapshot_id":2}',
                                 b" " * (MAX_BYTES + 1), b"\xff"],
                         ids=["partial", "duplicate", "oversized", "invalid-utf8"])
def test_file_loader_bounds_json_and_sanitizes(tmp_path, raw):
    path = tmp_path / "reviewed.json"
    path.write_bytes(raw)
    with pytest.raises(GatewayError, match="^invalid_client_config$"):
        load_client_config(path, now=1000)


def test_missing_file_no_search_and_no_secret_resolution(tmp_path):
    with patch("integrations.civic_routes.os.environ", {}) as env:
        with pytest.raises(GatewayError, match="^client_config_unavailable$"):
            load_client_config(tmp_path / "missing.json", now=1000)
        assert env == {}


def test_only_named_gateway_token_is_resolved_no_network():
    helper = CivicRoutes(clock=lambda: 1000)
    cfg = parse_client_config(config(), now=1000)
    env = Mock()
    env.__getitem__ = Mock(return_value="synthetic_gateway_token_000000000000")
    helper.configure(cfg, environment=env)
    env.__getitem__.assert_called_once_with("CIVIC_GATEWAY_TOKEN_V1")
    assert select(helper).route_id == "primary"
    with pytest.raises(GatewayError, match="^secret_unavailable$"):
        helper.configure(cfg, environment={})
    with pytest.raises(GatewayError, match="client_config_unavailable"):
        select(helper)


def test_preferred_then_stable_order_capability_and_ceiling():
    helper, transport, _ = ready(config(route("small", max_input_tokens=64),
                                       route("primary", capabilities=["classify"]),
                                       route("eligible")))
    assert select(helper).route_id == "eligible"
    transport.assert_not_called()


def test_single_success_matches_gateway_api_and_input_is_not_mutated():
    helper, transport, _ = ready()
    req = request()
    before = json.dumps(req)
    response = helper.infer(req)
    assert response["ok"] and response["output"] == {"answer": "one exact output"}
    assert response["attempts"] == 1
    assert json.dumps(req) == before
    transport.assert_called_once_with(req, timeout=30)


def test_429_blocks_same_account_alias_and_provider_without_any_fallback():
    helper, transport, now = ready(config(route(), route("alias"),
                                          route("same-provider", account_id="owned-2"),
                                          route("other-provider", provider="other", account_id="owned-3")),
                                   code="rate_limited", retry_after=90)
    response = helper.infer(request())
    assert not response["ok"] and response["output"] is None
    assert transport.call_count == 1
    for name in ("alias", "same-provider"):
        with pytest.raises(GatewayError, match="route_cooldown"):
            helper.infer(request(request_id="new-" + name, route_id=name))
    assert select(helper).route_id == "other-provider"
    now[0] = 1089
    assert select(helper).route_id == "other-provider"
    now[0] = 1090
    assert select(helper).route_id == "primary"
    assert transport.call_count == 1


def test_account_and_provider_health_are_distinct_and_never_shortened():
    helper, _, now = ready(config(route(), route("alias"), route("other", account_id="owned-2")))
    helper.defer("primary", scope="account", retry_after_seconds=90)
    helper.defer("primary", scope="account", retry_after_seconds=0)
    assert select(helper).route_id == "other"
    now[0] = 1089
    assert select(helper).route_id == "other"
    helper.defer("other", scope="provider", retry_after_seconds=40)
    now[0] = 1090
    with pytest.raises(GatewayError, match="routes_exhausted"):
        select(helper)
    now[0] = 1129
    assert select(helper).route_id == "primary"


@pytest.mark.parametrize("code", ["provider_quota", "quota_exceeded", "provider_auth", "secret_unavailable"])
def test_quota_auth_denials_survive_snapshot_refresh_and_expiry(code):
    helper, transport, now = ready(code=code)
    helper.infer(request())
    now[0] = 2100
    updated = config(route("primary", model="fresh-model"),
                     route("fresh-owned", provider="other", account_id="owned-2"),
                     snapshot_id="reviewed-2", expires_at=3000)
    helper.configure(parse_client_config(updated, now=now[0]), transport=transport)
    assert select(helper).route_id == "fresh-owned"
    with pytest.raises(GatewayError, match="route_cooldown"):
        helper.infer(request(snapshot_id="reviewed-2", request_id="new", deadline=2130))
    assert transport.call_count == 1


@pytest.mark.parametrize("code", ["provider_quota", "provider_auth"])
def test_unknown_provider_denial_scope_cannot_be_evaded_by_another_account(code):
    helper, transport, _ = ready(config(route(), route("other-account", account_id="owned-2")),
                                 code=code)
    helper.infer(request())
    with pytest.raises(GatewayError, match="routes_exhausted"):
        select(helper)
    assert transport.call_count == 1


def test_provider_cooldown_survives_snapshot_and_model_replacement():
    helper, transport, _ = ready(code="rate_limited", retry_after=300)
    helper.infer(request())
    updated = config(route(model="fresh"), snapshot_id="reviewed-2")
    helper.configure(parse_client_config(updated, now=1000), transport=transport)
    with pytest.raises(GatewayError, match="routes_exhausted"):
        select(helper)


def test_refresh_replaces_not_merges_and_changed_same_id_fails_closed():
    helper, transport, _ = ready(config(route(), route("old-fallback")))
    updated = parse_client_config(config(snapshot_id="reviewed-2"), now=1000)
    helper.configure(updated, transport=transport)
    with pytest.raises(GatewayError, match="unknown_route"):
        helper.infer(request(snapshot_id="reviewed-2", route_id="old-fallback"))
    with pytest.raises(GatewayError, match="snapshot_changed"):
        helper.configure(replace(updated, origin="http://127.0.0.1:19391"), transport=transport)
    with pytest.raises(GatewayError, match="client_config_unavailable"):
        select(helper)


def test_poll_failure_disables_dispatch_then_valid_poll_retains_health(tmp_path):
    helper, transport, _ = ready(code="rate_limited", retry_after=60)
    helper.infer(request())
    path = tmp_path / "approved.json"
    with pytest.raises(GatewayError, match="client_config_unavailable"):
        helper.reload(path, transport=transport)
    with pytest.raises(GatewayError, match="client_config_unavailable"):
        helper.infer(request(request_id="second"))
    path.write_text(json.dumps(config()), encoding="utf-8")
    helper.reload(path, transport=transport)
    with pytest.raises(GatewayError, match="routes_exhausted"):
        select(helper)
    assert transport.call_count == 1


@pytest.mark.parametrize("phase", ["select", "infer", "reload"])
def test_expired_snapshot_blocks_without_transport(tmp_path, phase):
    helper, transport, now = ready()
    now[0] = 2000
    path = tmp_path / "approved.json"
    path.write_text(json.dumps(config()), encoding="utf-8")
    action = {"select": lambda: select(helper), "infer": lambda: helper.infer(request()),
              "reload": lambda: helper.reload(path, transport=transport)}[phase]
    with pytest.raises(GatewayError, match="snapshot_expired"):
        action()
    transport.assert_not_called()


def test_total_attempt_budget_and_duplicate_ids_do_not_reset_on_reload():
    helper, transport, _ = ready(max_attempts=1)
    helper.infer(request())
    helper.configure(parse_client_config(config(snapshot_id="reviewed-2"), now=1000), transport=transport)
    with pytest.raises(GatewayError, match="duplicate_request"):
        helper.infer(request(snapshot_id="reviewed-2"))
    with pytest.raises(GatewayError, match="attempt_limit"):
        helper.infer(request(snapshot_id="reviewed-2", request_id="new-operation"))
    with pytest.raises(GatewayError, match="attempt_limit"):
        select(helper)
    assert transport.call_count == 1


@pytest.mark.parametrize("exception", [RuntimeError("SYNTHETIC_SECRET"), GatewayError("SYNTHETIC_SECRET")])
def test_ambiguous_failure_is_sanitized_single_attempt_and_keeps_reservation(exception):
    helper, transport, now = ready(max_attempts=1)
    transport.side_effect = exception
    with pytest.raises(GatewayError, match="^transport_error$"):
        helper.infer(request())
    now[0] += 31
    with pytest.raises(GatewayError, match="attempt_limit"):
        helper.infer(request(request_id="new", deadline=1060))
    assert transport.call_count == 1


def test_mismatch_and_invalid_output_never_merge_or_try_another_route():
    helper, transport, _ = ready(config(route(), route("alternate", account_id="owned-2")))
    transport.side_effect = lambda req, timeout: result(req) | {"actual_model": "hidden-fallback"}
    with pytest.raises(GatewayError, match="gateway_response_invalid"):
        helper.infer(request())
    assert transport.call_count == 1
    assert select(helper).route_id == "alternate"


@pytest.mark.parametrize("change", [{"max_cost_microusd": 1}, {"stream": True},
                                     {"snapshot_id": "unapproved"}, {"route_id": "discovered"},
                                     {"deadline": 5000}, {"max_output_tokens": 1000}])
def test_invalid_or_paid_request_has_no_attempt(change):
    helper, transport, _ = ready(max_attempts=1)
    with pytest.raises(GatewayError):
        helper.infer(request(**change))
    transport.assert_not_called()
    assert helper.infer(request())["ok"]


def test_concurrent_dispatch_fails_busy_without_extra_attempt():
    helper, transport, _ = ready()
    entered, release = threading.Event(), threading.Event()
    outcomes = []

    def wait(req, timeout):
        entered.set()
        assert release.wait(3)
        return result(req)

    transport.side_effect = wait
    thread = threading.Thread(target=lambda: outcomes.append(helper.infer(request())))
    thread.start()
    try:
        assert entered.wait(3)
        with pytest.raises(GatewayError, match="gateway_busy"):
            helper.infer(request(request_id="concurrent"))
    finally:
        release.set()
        thread.join(3)
    assert outcomes[0]["ok"] and transport.call_count == 1


@pytest.mark.parametrize("policy", [{"max_attempts": 0}, {"max_attempts": True},
                                     {"max_attempts": 10001}, {"cooldown_seconds": 0},
                                     {"cooldown_seconds": float("nan")}])
def test_policy_bounds(policy):
    with pytest.raises(GatewayError, match="invalid_route_policy"):
        CivicRoutes(**policy)


def test_invalid_clock_and_health_fail_closed():
    helper, transport, now = ready()
    for value in (-1, float("nan"), float("inf"), True):
        now[0] = value
        with pytest.raises(GatewayError, match="invalid_clock"):
            select(helper)
    now[0] = 1000
    for value in (-1, float("nan"), True):
        with pytest.raises(GatewayError, match="invalid_route_health"):
            helper.defer("primary", scope="account", retry_after_seconds=value)
    transport.assert_not_called()


def test_config_history_is_bounded():
    helper, transport, _ = ready()
    for index in range(1, 64):
        helper.configure(parse_client_config(config(snapshot_id=f"snapshot-{index}"), now=1000),
                         transport=transport)
    with pytest.raises(GatewayError, match="config_history_full"):
        helper.configure(parse_client_config(config(snapshot_id="snapshot-65"), now=1000),
                         transport=transport)
    with pytest.raises(GatewayError, match="client_config_unavailable"):
        select(helper)
