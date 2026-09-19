"""Offline contract and ephemeral-loopback tests. No live Scanner/provider I/O."""

import dataclasses
import contextlib
import http.client
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

from integrations.fcm_gateway import (
    MAX_BYTES, AuthorizedSnapshot, FCMGatewayClient, GatewayError,
    HTTPGatewayTransport, Route, check_output_schema, decode, encode,
)

# Load only the new standalone adapter, never Scanner's core or config modules.
SCANNER = Path(__file__).resolve().parents[2] / "ChatGPT-API-Scanner" / "civic_gateway"
if SCANNER.is_dir():
    spec = importlib.util.spec_from_file_location(
        "civic_gateway", SCANNER / "__init__.py", submodule_search_locations=[str(SCANNER)])
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    from civic_gateway import Binding, Gateway, ProviderCall, ProviderReply
    from civic_gateway.server import make_server
    from civic_gateway import launcher
    from civic_gateway.openai_transport import (
        HTTPSGrant, OpenAIHTTPS, conservative_input_tokens, normalize_response,
        render_request, retry_after,
    )

SCHEMA = {"type": "object", "properties": {"answer": {"type": "string"}},
          "required": ["answer"], "additionalProperties": False}
SECRET = "test-only-provider-secret"
TOKEN = "test_only_gateway_token_00000000000000"


def route(**changes):
    values = dict(route_id="approved", provider="mock", account_id="account-1",
                  model="model-1", capabilities=("extract",), max_input_tokens=100,
                  max_output_tokens=50, pricing="paid", input_microusd_per_million=1_000_000,
                  output_microusd_per_million=2_000_000, quota_tokens=1_000,
                  quota_cost_microusd=10_000)
    return Route(**(values | changes))


def snapshot(*routes, now=1000):
    return AuthorizedSnapshot("snapshot-1", now + 300, routes or (route(),))


def request(now=1000, **changes):
    return dict(request_id="request-1", snapshot_id="snapshot-1", route_id="approved",
                capability="extract", input_refs=["evidence:1"], input="Extract the evidence.",
                response_schema=SCHEMA, deadline=now + 30, max_input_tokens=100,
                max_output_tokens=50, max_cost_microusd=200, stream=False) | changes


def reply(**changes):
    body = dict(provider="mock", model="model-1", output={"answer": "verified"},
                usage={"input_tokens": 10, "output_tokens": 5, "total_tokens": 15}) | changes
    return ProviderReply(200, encode(body))


class ContractTests(unittest.TestCase):
    def test_unknown_route_rejected_without_transport(self):
        transport = Mock()
        client = FCMGatewayClient(snapshot(), transport, clock=lambda: 1000)
        with self.assertRaisesRegex(GatewayError, "unknown_route"):
            client.infer(request(route_id="future-key"))
        transport.assert_not_called()

    def test_invalid_requests_fail_closed(self):
        for changes in ({"stream": True}, {"api_key": "not-allowed"},
                        {"deadline": float("nan")}, {"max_input_tokens": 1.0},
                        {"max_output_tokens": True}, {"input_refs": []},
                        {"max_cost_microusd": None}, {"response_schema": {"$ref": "https://invalid"}}):
            with self.subTest(changes=changes):
                transport = Mock()
                with self.assertRaises(GatewayError):
                    FCMGatewayClient(snapshot(), transport, clock=lambda: 1000).infer(request(**changes))
                transport.assert_not_called()

    def test_json_rejects_duplicate_nonfinite_large_and_deep(self):
        for data in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}', b'\xff',
                     b' ' * (MAX_BYTES + 1), b'[' * 2000 + b']' * 2000):
            with self.subTest(data=data[:20]), self.assertRaises(GatewayError):
                decode(data)

    def test_schema_subset_has_no_remote_or_ignored_keywords(self):
        for schema in ({"$ref": "file:///private"}, SCHEMA | {"pattern": "x"},
                       SCHEMA | {"additionalProperties": True},
                       SCHEMA | {"properties": {"a": {"type": "array"}}},
                       SCHEMA | {"properties": {"a": {"type": "string", "$ref": "x"}}}):
            with self.subTest(schema=schema), self.assertRaises(GatewayError):
                check_output_schema(schema)

    def test_snapshot_is_detached_and_alias_budgets_match(self):
        routes = [route()]
        frozen = AuthorizedSnapshot("snapshot-1", 1300, routes)
        routes.append(route(route_id="future"))
        self.assertEqual(len(frozen.routes), 1)
        with self.assertRaises(ValueError):
            snapshot(route(), route(route_id="alias", quota_tokens=5))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            frozen.routes[0].model = "new-model"

    def test_http_origin_cannot_include_admin_path_credentials_or_remote_plaintext(self):
        for origin in ("http://127.0.0.1:99/admin", "http://user:pass@127.0.0.1",
                       "http://example.invalid", "http://127.0.0.1?route=admin"):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                HTTPGatewayTransport(origin, TOKEN)

    def test_client_transport_exceptions_are_sanitized(self):
        for exc in (RuntimeError(SECRET), GatewayError(SECRET)):
            with self.subTest(exc=type(exc)), self.assertRaisesRegex(GatewayError, "^transport_error$"):
                FCMGatewayClient(snapshot(), Mock(side_effect=exc), clock=lambda: 1000).infer(request())

    def test_cost_rounds_up_in_integer_microusd(self):
        self.assertEqual(route(input_microusd_per_million=1,
                               output_microusd_per_million=1).cost(1, 1), 1)


@unittest.skipUnless(SCANNER.is_dir(), "standalone Scanner civic_gateway checkout unavailable")
class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.now = 1000.0
        self.resolver = Mock(return_value=SECRET)
        self.provider = Mock(return_value=reply())
        self.counter = Mock(return_value=20)
        self.gateway = self.make_gateway()

    def make_gateway(self, *routes, **options):
        snap = snapshot(*routes)
        return Gateway(snap, tuple(Binding(r.route_id, "owned:account-1:version-1") for r in snap.routes),
                       self.resolver, self.provider, self.counter,
                       clock=lambda: self.now, monotonic=lambda: self.now, **options)

    def assert_error(self, result, code, attempts=0):
        self.assertFalse(result["ok"], result)
        self.assertEqual(result["error"]["code"], code, result)
        self.assertEqual(result["attempts"], attempts)
        self.assertIsNone(result["output"])
        self.assertNotIn(SECRET, json.dumps(result))

    def test_success_client_server_metadata_and_one_attempt(self):
        client = FCMGatewayClient(self.gateway.snapshot,
                                  lambda value, timeout: self.gateway.handle(value), clock=lambda: self.now)
        result = client.infer(request())
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["actual_provider"], "mock")
        self.assertEqual(result["actual_model"], "model-1")
        self.assertEqual(result["cost_microusd"], 20)
        self.assertEqual(result["reserved_cost_microusd"], 200)
        self.assertEqual(result["usage"]["total_tokens"], 15)
        call = self.provider.call_args.args[0]
        self.assertEqual(call.credential, SECRET)
        self.assertEqual(call.timeout, 30)
        self.assertFalse(call.request["stream"])
        self.assertNotIn(SECRET, repr(call))
        self.assertEqual(self.provider.call_count, 1)

    def test_rejections_precede_secret_resolution(self):
        for changes, code in (({"route_id": "unknown"}, "unknown_route"),
                              ({"snapshot_id": "new"}, "unknown_snapshot"),
                              ({"capability": "admin"}, "capability_denied"),
                              ({"deadline": 1000}, "deadline_exceeded"),
                              ({"deadline": 9999}, "deadline_exceeded"),
                              ({"max_input_tokens": 101}, "token_limit"),
                              ({"max_cost_microusd": 199}, "cost_limit"),
                              ({"stream": True}, "invalid_request")):
            with self.subTest(code=code):
                self.assert_error(self.gateway.handle(request(**changes)), code)
        self.resolver.assert_not_called()
        self.provider.assert_not_called()

    def test_unknown_paid_price_or_limits_denied(self):
        for changes, code in (({"input_microusd_per_million": None}, "cost_unknown"),
                              ({"output_microusd_per_million": None}, "cost_unknown"),
                              ({"quota_tokens": None}, "limit_unknown"),
                              ({"quota_cost_microusd": None}, "limit_unknown"),
                              ({"quota_tokens": 149}, "quota_exceeded")):
            with self.subTest(changes=changes):
                self.assert_error(self.make_gateway(route(**changes)).handle(request()), code)
        self.resolver.assert_not_called()

    def test_explicit_free_route_supports_zero_cost(self):
        gateway = self.make_gateway(route(pricing="free", input_microusd_per_million=0,
                                          output_microusd_per_million=0, quota_cost_microusd=0))
        result = gateway.handle(request(max_cost_microusd=0))
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["cost_microusd"], 0)

    def test_response_schema_failure_records_real_usage(self):
        self.provider.return_value = reply(output={"answer": 42})
        result = self.gateway.handle(request())
        self.assert_error(result, "output_schema_invalid", 1)
        self.assertEqual(result["cost_microusd"], 20)
        self.assertEqual(result["actual_model"], "model-1")

    def test_malformed_streamed_or_mismatched_provider_output(self):
        cases = [(ProviderReply(200, b"data: {}", "text/event-stream"), "provider_response_invalid"),
                 (ProviderReply(200, b"not-json"), "provider_response_invalid"),
                 (reply(model="other-model"), "route_mismatch"),
                 (reply(provider="other-provider"), "route_mismatch"),
                 (reply(usage={"input_tokens": 1, "output_tokens": 2, "total_tokens": 8}), "usage_invalid"),
                 (reply(usage={"input_tokens": 101, "output_tokens": 2, "total_tokens": 103}), "usage_invalid"),
                 (reply(usage=None), "provider_response_invalid"),
                 (reply(usage={"input_tokens": 1.0, "output_tokens": 2, "total_tokens": 3}), "provider_response_invalid")]
        for response, code in cases:
            with self.subTest(code=code):
                self.provider.return_value = response
                self.assert_error(self.make_gateway().handle(request()), code, 1)

    def test_secret_redaction_in_output_metadata_and_exceptions(self):
        for response in (reply(output={"answer": SECRET}), reply(model=SECRET),
                         reply(output={SECRET: "value"}), ProviderReply(401, SECRET.encode())):
            self.provider.return_value = response
            result = self.make_gateway().handle(request())
            self.assertNotIn(SECRET, json.dumps(result))
            self.assertFalse(result["ok"])
        self.provider.side_effect = RuntimeError("Authorization: Bearer " + SECRET)
        self.assert_error(self.make_gateway().handle(request()), "transport_error", 1)
        self.resolver.side_effect = RuntimeError(SECRET)
        self.assert_error(self.make_gateway().handle(request()), "secret_unavailable")

    def test_secret_resolution_cached_no_future_key_rotation(self):
        self.resolver.side_effect = [SECRET, "new-unauthorized-key"]
        self.gateway.handle(request())
        self.gateway.handle(request(request_id="second"))
        self.assertEqual(self.resolver.call_count, 1)
        self.assertEqual([c.args[0].credential for c in self.provider.call_args_list], [SECRET, SECRET])
        with self.assertRaises(ValueError):
            Gateway(snapshot(route(), route(route_id="alias")),
                    (Binding("approved", "v1"), Binding("alias", "v2")), self.resolver,
                    self.provider, self.counter)

    def test_account_backoff_is_shared_across_model_aliases(self):
        self.provider.return_value = ProviderReply(429, SECRET.encode(), retry_after_seconds=60)
        gateway = self.make_gateway(route(), route(route_id="alias", model="model-2"))
        self.assert_error(gateway.handle(request()), "rate_limited", 1)
        self.assert_error(gateway.handle(request(request_id="second", route_id="alias")), "account_backoff")
        self.assertEqual(self.provider.call_count, 1)
        self.now += 61
        self.provider.return_value = reply()
        self.assertTrue(gateway.handle(request(now=self.now, request_id="third"))["ok"])

    def test_provider_scope_blocks_other_accounts(self):
        self.provider.return_value = ProviderReply(503, backoff_scope="provider", retry_after_seconds=30)
        gateway = self.make_gateway(route(), route(route_id="account-2", account_id="account-2"))
        self.assert_error(gateway.handle(request()), "provider_unavailable", 1)
        self.assert_error(gateway.handle(request(request_id="second", route_id="account-2")), "account_backoff")
        self.assertEqual(self.provider.call_count, 1)

    def test_provider_quota_and_auth_block_until_snapshot_expiry(self):
        for status, code in ((402, "provider_quota"), (401, "provider_auth")):
            self.provider.return_value = ProviderReply(status)
            gateway = self.make_gateway()
            self.assert_error(gateway.handle(request()), code, 1)
            self.now = 1100
            self.assert_error(gateway.handle(request(now=self.now, request_id="second")), "account_backoff")
            self.now = 1000

    def test_deadline_timeout_and_late_reply_keep_reservation(self):
        def late(call):
            self.now += 31
            return reply()
        self.provider.side_effect = late
        result = self.gateway.handle(request())
        self.assert_error(result, "deadline_exceeded", 1)
        self.assertEqual(result["latency_ms"], 31_000)
        self.assertEqual(result["reserved_cost_microusd"], 200)
        self.assertIsNone(result["usage"])
        self.provider.side_effect = TimeoutError(SECRET)
        self.now = 1000
        self.assert_error(self.make_gateway().handle(request()), "provider_timeout", 1)

    def test_counter_and_slow_resolver_cannot_dispatch_past_limits(self):
        self.counter.return_value = 101
        self.assert_error(self.gateway.handle(request()), "token_limit")
        self.resolver.assert_not_called()
        self.counter.return_value = 20
        def slow(ref):
            self.now += 31
            return SECRET
        self.resolver.side_effect = slow
        self.assert_error(self.gateway.handle(request()), "deadline_exceeded")
        self.provider.assert_not_called()

    def test_cumulative_quota_and_ambiguous_failure_reservation(self):
        gateway = self.make_gateway(route(quota_tokens=160))
        self.assertTrue(gateway.handle(request())["ok"])
        self.assert_error(gateway.handle(request(request_id="second")), "quota_exceeded")
        self.provider.return_value = ProviderReply(400, SECRET.encode())
        gateway = self.make_gateway(route(quota_cost_microusd=200))
        self.assert_error(gateway.handle(request()), "provider_error", 1)
        self.assert_error(gateway.handle(request(request_id="second")), "quota_exceeded")

    def test_aliases_share_spent_quota_but_other_accounts_do_not(self):
        gateway = self.make_gateway(route(quota_tokens=160),
                                    route(route_id="alias", quota_tokens=160),
                                    route(route_id="other", account_id="other", quota_tokens=160))
        self.assertTrue(gateway.handle(request())["ok"])
        self.assert_error(gateway.handle(request(request_id="second", route_id="alias")), "quota_exceeded")
        self.assertTrue(gateway.handle(request(request_id="third", route_id="other"))["ok"])

    def test_snapshot_expiry_blocks_without_secrets(self):
        self.now = 1300
        self.assert_error(self.gateway.handle(request(now=self.now)), "snapshot_expired")
        self.resolver.assert_not_called()

    def test_secret_echo_in_correlation_is_removed(self):
        self.gateway.handle(request())
        result = self.gateway.handle(request(request_id=SECRET))
        self.assert_error(result, "secret_redacted", 1)
        self.assertIsNone(result["request_id"])

    def test_duplicate_request_and_bounded_cache_do_not_dispatch(self):
        gateway = self.make_gateway(max_requests=1)
        gateway.handle(request())
        self.assert_error(gateway.handle(request()), "duplicate_request")
        self.assert_error(gateway.handle(request(request_id="second")), "gateway_busy")
        self.assertEqual(self.provider.call_count, 1)

    def test_concurrent_request_is_bounded(self):
        entered, release = threading.Event(), threading.Event()
        def waiting(call):
            entered.set()
            release.wait(2)
            return reply()
        self.provider.side_effect = waiting
        worker = threading.Thread(target=self.gateway.handle, args=(request(),))
        worker.start()
        try:
            self.assertTrue(entered.wait(1))
            self.assert_error(self.gateway.handle(request(request_id="second")), "gateway_busy")
        finally:
            release.set()
            worker.join(2)

    def test_client_rejects_tampered_gateway_metadata_and_schema(self):
        good = self.gateway.handle(request())
        for changes in ({"request_id": "other"}, {"actual_model": "other"},
                        {"cost_microusd": 0}, {"attempts": 2}, {"output": {"answer": 9}},
                        {"reserved_cost_microusd": 0},
                        {"error": {"code": "raw-secret", "retryable": False, "retry_after_seconds": 0}}):
            with self.subTest(changes=changes), self.assertRaises(GatewayError):
                FCMGatewayClient(snapshot(), lambda r, timeout: good | changes,
                                 clock=lambda: 1000).infer(request())

    def test_client_rejects_late_gateway_reply(self):
        good = self.gateway.handle(request())
        clock = Mock(side_effect=[1000, 1031])
        with self.assertRaisesRegex(GatewayError, "deadline_exceeded"):
            FCMGatewayClient(snapshot(), lambda r, timeout: good, clock=clock).infer(request())


@unittest.skipUnless(SCANNER.is_dir(), "standalone Scanner civic_gateway checkout unavailable")
class HTTPTests(unittest.TestCase):
    def setUp(self):
        now = time.time()
        self.provider = Mock(return_value=reply())
        self.gateway = Gateway(snapshot(now=now), (Binding("approved", "version-1"),),
                               lambda ref: SECRET, self.provider, lambda route, req: 20)
        self.server = make_server(self.gateway, TOKEN)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.origin = "http://127.0.0.1:" + str(self.server.server_port)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)

    def post(self, path, body=b"{}", **headers):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=2)
        try:
            connection.request("POST", path, body=body, headers={
                "Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"} | headers)
            response = connection.getresponse()
            return response.status, decode(response.read())
        finally:
            connection.close()

    def test_ephemeral_loopback_end_to_end(self):
        client = FCMGatewayClient(self.gateway.snapshot, HTTPGatewayTransport(self.origin, TOKEN))
        result = client.infer(request(now=time.time()))
        self.assertTrue(result["ok"], result)
        self.assertEqual(self.provider.call_count, 1)

    def test_http_surface_auth_body_limits_and_browser_origin(self):
        for path, body, headers, expected in (
            ("/admin", b"{}", {}, 404), ("/v1/chat/completions", b"{}", {}, 404),
            ("/v1/civic/infer", b"{}", {"Authorization": "Bearer bad"}, 401),
            ("/v1/civic/infer", b"{}", {"Content-Length": str(MAX_BYTES + 1)}, 413),
            ("/v1/civic/infer", b"{}", {"Origin": "https://example.invalid"}, 400),
            ("/v1/civic/infer", b'{"a":1,"a":2}', {}, 400),
        ):
            with self.subTest(path=path, headers=headers):
                status, result = self.post(path, body, **headers)
                self.assertEqual(status, expected)
                self.assertNotIn(TOKEN, json.dumps(result))
        self.provider.assert_not_called()

    def test_http_client_normalizes_auth_error_without_token_echo(self):
        # Windows may reset an early-rejected connection with an unread body
        # before the caller receives the 401 headers. Neither path may retry.
        with self.assertRaises(GatewayError) as caught:
            HTTPGatewayTransport(self.origin, "wrong_gateway_token_00000000000000")(request(now=time.time()), timeout=2)
        self.assertIn(caught.exception.code, ("gateway_unavailable", "transport_error"))
        self.assertNotIn(TOKEN, str(caught.exception))
        self.provider.assert_not_called()

    def test_http_transport_never_follows_redirects_and_bounds_reply(self):
        from unittest.mock import patch
        for response_status, content_type, chunks, code in (
            (302, "application/json", [], "gateway_unavailable"),
            (200, "text/event-stream", [], "gateway_unavailable"),
            (200, "application/json", [b"x" * 65_536] * 17, "payload_too_large"),
        ):
            connection = Mock()
            response = connection.getresponse.return_value
            response.status = response_status
            response.getheader.return_value = content_type
            response.isclosed.return_value = False
            response.read1.side_effect = chunks
            with self.subTest(code=code), patch("http.client.HTTPConnection", return_value=connection) as cls:
                with self.assertRaisesRegex(GatewayError, code):
                    HTTPGatewayTransport(self.origin, TOKEN)(request(now=time.time()), timeout=2)
                self.assertEqual(cls.call_count, 1)
                connection.request.assert_called_once()
                connection.close.assert_called_once()

    def test_http_token_never_echoed_by_front_door(self):
        status, result = self.post("/v1/civic/infer", encode(request(now=time.time(), request_id=TOKEN)))
        self.assertEqual(status, 500)
        self.assertNotIn(TOKEN, json.dumps(result))

    def test_http_read_deadline_aborts_slow_gateway_without_retry(self):
        entered = threading.Event()
        release = threading.Event()
        original = self.gateway.handle
        def delayed(value):
            entered.set()
            release.wait(2)
            return original(value)
        self.gateway.handle = delayed
        try:
            start = time.monotonic()
            with self.assertRaisesRegex(GatewayError, "deadline_exceeded"):
                HTTPGatewayTransport(self.origin, TOKEN)(request(now=time.time()), timeout=0.1)
            self.assertLess(time.monotonic() - start, 1.5)
            self.assertTrue(entered.is_set())
        finally:
            release.set()


def approved_config():
    approved = route(pricing="free", input_microusd_per_million=0, output_microusd_per_million=0,
                     quota_cost_microusd=0, max_input_tokens=10_000, quota_tokens=100_000)
    return {
        "version": 1, "auto_discover": False,
        "snapshot": {"snapshot_id": "snapshot-1", "expires_at": time.time() + 3600,
                     "routes": [dataclasses.asdict(approved)]},
        "endpoint_allowlist": ["https://provider.example.invalid/v1/chat/completions"],
        "bindings": [{"route_id": "approved", "endpoint": "https://provider.example.invalid/v1/chat/completions",
                      "credential_env": "CIVIC_PROVIDER_ACCOUNT1_V1"}],
        "gateway_token_env": "CIVIC_GATEWAY_TOKEN_V1", "listen_port": 19390,
    }


def chat_body(**changes):
    return encode({"model": "model-1", "choices": [{"index": 0, "finish_reason": "stop",
        "message": {"role": "assistant", "content": '{"answer":"verified"}'}}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}} | changes)


class FakeHTTPSResponse:
    def __init__(self, body, *, status=200, headers=None):
        self.status = status
        self.body = io.BytesIO(body)
        self.headers = headers if headers is not None else [
            ("Content-Type", "application/json"), ("Content-Length", str(len(body)))]
        self.length = len(body)

    def getheaders(self):
        return self.headers

    def getheader(self, name, default=None):
        return next((v for k, v in self.headers if k.lower() == name.lower()), default)

    def isclosed(self):
        return self.body.tell() >= self.length

    def read1(self, size):
        return self.body.read(size)


@unittest.skipUnless(SCANNER.is_dir(), "standalone Scanner civic_gateway checkout unavailable")
class ConcreteTransportTests(unittest.TestCase):
    def setUp(self):
        self.config = launcher.parse_config(approved_config())
        self.route = self.config.snapshot.routes[0]
        self.grant = HTTPSGrant(self.route, self.config.bindings[0].endpoint)
        self.transport = OpenAIHTTPS((self.grant,), self.config.endpoint_allowlist)
        self.connection = Mock()
        self.connection.getresponse.return_value = FakeHTTPSResponse(chat_body())
        self.connection_patch = patch("civic_gateway.openai_transport.http.client.HTTPSConnection",
                                      return_value=self.connection)
        self.constructor = self.connection_patch.start()
        self.addCleanup(self.connection_patch.stop)

    def call(self):
        req = request(now=time.time(), max_input_tokens=10_000, max_cost_microusd=0)
        return ProviderCall(self.route, req, SECRET, 2)

    def gateway(self):
        return Gateway(self.config.snapshot, (Binding("approved", "fixed-version"),),
                       lambda ref: SECRET, self.transport, conservative_input_tokens)

    def test_actual_https_payload_pinned_identity_usage_and_no_retry(self):
        result = self.gateway().handle(self.call().request)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["actual_model"], "model-1")
        self.assertEqual(result["actual_provider"], "mock")
        self.assertEqual(result["usage"]["total_tokens"], 15)
        args, kwargs = self.connection.request.call_args
        self.assertEqual(args, ("POST", "/v1/chat/completions"))
        payload = decode(kwargs["body"])
        self.assertEqual(payload["model"], "model-1")
        self.assertFalse(payload["stream"])
        self.assertEqual(payload["n"], 1)
        self.assertEqual(payload["max_tokens"], 50)
        self.assertEqual(payload["response_format"]["json_schema"]["schema"], SCHEMA)
        self.assertNotIn("input_refs", payload)
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer " + SECRET)
        self.assertEqual(self.constructor.call_args.args, ("provider.example.invalid", 443))
        context = self.constructor.call_args.kwargs["context"]
        self.assertTrue(context.check_hostname)
        self.connection.request.assert_called_once()
        self.connection.close.assert_called_once()

    def test_endpoint_allowlist_and_models_cannot_be_overridden(self):
        with self.assertRaises(ValueError):
            OpenAIHTTPS((self.grant,), ("https://other.example.invalid/v1/chat/completions",))
        with self.assertRaisesRegex(GatewayError, "unknown_route"):
            self.transport(dataclasses.replace(self.call(), route=dataclasses.replace(self.route, model="other")))
        for field, value in (("endpoint", "https://evil.invalid/v1/chat/completions"),
                             ("model", "other"), ("stream", True)):
            with self.subTest(field=field), self.assertRaises(GatewayError):
                self.transport(dataclasses.replace(self.call(), request=self.call().request | {field: value}))
        self.constructor.assert_not_called()

    def test_byte_bound_includes_input_schema_wrappers_and_rejects_before_post(self):
        req = self.call().request | {"input": "\u0434\u0430\U0001f600" * 10}
        bound = conservative_input_tokens(self.route, req)
        self.assertGreaterEqual(bound, len(render_request(self.route, req)) + 4096)
        self.assertGreater(bound, len(req["input"].encode("utf-8")))
        req["max_input_tokens"] = bound - 1
        result = self.gateway().handle(req)
        self.assertEqual(result["error"]["code"], "token_limit")
        self.assertEqual(result["attempts"], 0)
        self.constructor.assert_not_called()

    def test_model_alias_and_provider_mismatch_fail_with_observed_metadata(self):
        for body, key, actual in ((chat_body(model="revision-other"), "actual_model", "revision-other"),
                                  (chat_body(provider="other"), "actual_provider", "other")):
            self.connection.getresponse.return_value = FakeHTTPSResponse(body)
            result = self.gateway().handle(self.call().request)
            self.assertEqual(result["error"]["code"], "route_mismatch")
            self.assertEqual(result[key], actual)

    def test_non_json_content_and_secret_echo_fail_without_repair(self):
        for content, code in (("```json\n{}\n```", "output_schema_invalid"),
                              ('{"answer":"' + SECRET + '"}', "secret_redacted")):
            body = chat_body(choices=[{"index": 0, "finish_reason": "stop",
                                      "message": {"role": "assistant", "content": content}}])
            self.connection.getresponse.return_value = FakeHTTPSResponse(body)
            result = self.gateway().handle(self.call().request)
            self.assertEqual(result["error"]["code"], code)
            self.assertNotIn(SECRET, json.dumps(result))

    def test_invalid_usage_tools_truncation_and_streams_rejected(self):
        bodies = [chat_body(usage=None), chat_body(usage={"prompt_tokens": True,
                  "completion_tokens": 5, "total_tokens": 6}), chat_body(choices=[])]
        for reason in ("length", "tool_calls", "content_filter"):
            bodies.append(chat_body(choices=[{"index": 0, "finish_reason": reason,
                "message": {"role": "assistant", "content": "{}"}}]))
        for body in bodies:
            with self.subTest(body=body[:25]):
                self.connection.getresponse.return_value = FakeHTTPSResponse(body)
                result = self.gateway().handle(self.call().request)
                self.assertFalse(result["ok"])
                self.assertIsNone(result["output"])
        self.connection.getresponse.return_value = FakeHTTPSResponse(b"data: {}", headers=[("Content-Type", "text/event-stream")])
        with self.assertRaisesRegex(GatewayError, "provider_response_invalid"):
            self.transport(self.call())

    def test_rate_and_quota_error_mapping_discards_error_body(self):
        for body, status, code in ((encode({"error": {"code": "insufficient_quota", "message": SECRET}}), 429, "provider_quota"),
                                   (encode({"error": {"message": SECRET}}), 429, "rate_limited")):
            self.connection.getresponse.return_value = FakeHTTPSResponse(body, status=status,
                headers=[("Content-Type", "application/json"), ("Retry-After", "60")])
            gateway = self.gateway()
            result = gateway.handle(self.call().request)
            self.assertEqual(result["error"]["code"], code)
            self.assertGreaterEqual(result["error"]["retry_after_seconds"], 60)
            self.assertNotIn(SECRET, json.dumps(result))
            second = gateway.handle(self.call().request | {"request_id": "next"})
            self.assertEqual(second["error"]["code"], "account_backoff")

    def test_redirect_never_followed(self):
        self.connection.getresponse.return_value = FakeHTTPSResponse(b"", status=307,
            headers=[("Location", "https://other.invalid/steal")])
        result = self.transport(self.call())
        self.assertEqual(result.status, 307)
        self.assertEqual(result.body, b"")
        self.constructor.assert_called_once()
        self.connection.request.assert_called_once()

    def test_body_size_length_and_encoding_bounded(self):
        for body, headers in ((b"x" * (MAX_BYTES + 1), [("Content-Type", "application/json")]),
                              (b"{}", [("Content-Length", str(MAX_BYTES + 1))]),
                              (b"{}", [("Content-Length", "20")]),
                              (b"{}", [("Content-Encoding", "gzip")]),
                              (b"{}", [("Content-Length", "2"), ("Content-Length", "2")])):
            with self.subTest(headers=headers), self.assertRaises(GatewayError):
                self.connection.getresponse.return_value = FakeHTTPSResponse(body, headers=headers)
                self.transport(self.call())

    def test_blocked_dns_and_read_are_deadline_bounded_and_workers_do_not_multiply(self):
        for phase in ("connect", "read"):
            entered, release = threading.Event(), threading.Event()
            def block(*args):
                entered.set()
                release.wait(2)
                return b""
            self.connection.connect.side_effect = block if phase == "connect" else None
            response = FakeHTTPSResponse(chat_body())
            if phase == "read":
                response.read1 = block
            self.connection.getresponse.return_value = response
            transport = OpenAIHTTPS((self.grant,), self.config.endpoint_allowlist)
            try:
                start = time.monotonic()
                with self.assertRaises(TimeoutError):
                    transport(dataclasses.replace(self.call(), timeout=0.05))
                self.assertLess(time.monotonic() - start, 1)
                self.assertTrue(entered.is_set())
                with self.assertRaisesRegex(GatewayError, "gateway_busy"):
                    transport(self.call())
                if phase == "connect":
                    self.connection.request.assert_not_called()
            finally:
                release.set()
                self.assertTrue(transport._worker_slot.acquire(timeout=2))
                transport._worker_slot.release()
            self.connection.reset_mock()

    def test_transport_exception_and_retry_after_are_sanitized(self):
        self.connection.connect.side_effect = RuntimeError(SECRET)
        with self.assertRaisesRegex(GatewayError, "^transport_error$"):
            self.transport(self.call())
        self.assertEqual(retry_after("60"), 60)
        self.assertIsNone(retry_after("nan"))
        self.assertIsNone(retry_after(SECRET))
        self.assertEqual(retry_after("Thu, 01 Jan 1970 00:00:00 GMT"), 0)

    def test_concrete_transport_error_imposes_account_backoff(self):
        self.connection.connect.side_effect = OSError(SECRET)
        gateway = self.gateway()
        first = gateway.handle(self.call().request)
        self.assertEqual(first["error"]["code"], "transport_error")
        second = gateway.handle(self.call().request | {"request_id": "second"})
        self.assertEqual(second["error"]["code"], "account_backoff")
        self.constructor.assert_called_once()

    def test_transport_refuses_paid_grant_even_with_known_prices(self):
        with self.assertRaisesRegex(ValueError, "only_explicit_free_routes_supported"):
            HTTPSGrant(route(), self.grant.endpoint)
        self.constructor.assert_not_called()


@unittest.skipUnless(SCANNER.is_dir(), "standalone Scanner civic_gateway checkout unavailable")
class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "approved.json"
        self.journal = Path(self.directory.name) / "state" / "admissions.sqlite3"
        self.value = approved_config()
        self.path.write_bytes(encode(self.value))
        self.config = launcher.load_config(self.path)
        self.env = {"CIVIC_GATEWAY_TOKEN_V1": TOKEN, "CIVIC_PROVIDER_ACCOUNT1_V1": SECRET}
        self.journal_patch = patch.object(launcher, "JOURNAL_PATH", self.journal)
        self.journal_patch.start()
        self.addCleanup(self.journal_patch.stop)

    def test_check_cli_reads_config_only_without_secrets_network_or_journal(self):
        with patch.object(launcher, "resolve_environment", side_effect=AssertionError("secret access")), \
                patch.object(launcher, "claim_snapshot", side_effect=AssertionError("journal access")), \
                patch("http.client.HTTPSConnection", side_effect=AssertionError("network")), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(launcher.main(["--config", str(self.path), "--check"]), 0)
        self.assertIn("configuration_valid", output.getvalue())
        self.assertFalse(self.journal.exists())

    def test_real_module_entrypoint_check_with_no_secret_environment(self):
        result = subprocess.run([sys.executable, "-m", "civic_gateway", "--config", str(self.path), "--check"],
            env={"PYTHONPATH": os.pathsep.join([str(SCANNER.parent), str(Path(__file__).resolve().parents[1])]),
                 "SystemRoot": "C:\\Windows"}, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("configuration_valid", result.stdout)
        self.assertFalse(self.journal.exists())

    def test_config_rejects_paid_discovery_unknown_fields_alias_keys_and_endpoints(self):
        changes = [lambda v: v.update(auto_discover=True), lambda v: v.update(api_key=SECRET),
                   lambda v: v["snapshot"]["routes"][0].update(pricing="paid"),
                   lambda v: v["bindings"][0].update(endpoint="https://evil.invalid/v1/chat/completions"),
                   lambda v: v["bindings"][0].update(credential_env="OPENAI_API_KEY"),
                   lambda v: v["bindings"][0].update(credential_env="CIVIC_PROVIDER_ACCOUNT1_V1\n"),
                   lambda v: v["snapshot"].update(expires_at=time.time() - 1),
                   lambda v: v["snapshot"].update(expires_at=time.time() + 172800)]
        for change in changes:
            value = decode(encode(self.value))
            change(value)
            with self.subTest(change=change), self.assertRaises(launcher.LaunchError):
                launcher.parse_config(value)

    def test_config_aliases_cannot_select_another_key(self):
        value = decode(encode(self.value))
        value["snapshot"]["routes"].append(value["snapshot"]["routes"][0] | {"route_id": "alias"})
        value["bindings"].append(value["bindings"][0] | {
            "route_id": "alias", "credential_env": "CIVIC_PROVIDER_ACCOUNT1_V2"})
        with self.assertRaises(launcher.LaunchError):
            launcher.parse_config(value)

    def test_invalid_check_never_echoes_json_secret_or_reads_environment(self):
        self.path.write_bytes(encode(self.value | {"api_key": SECRET}))
        with patch.object(launcher, "resolve_environment", side_effect=AssertionError("secret access")), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(launcher.main(["--config", str(self.path), "--check"]), 2)
        self.assertEqual(output.getvalue().strip(), "invalid_config")
        self.assertFalse(self.journal.exists())

    def test_noncanonical_endpoints_are_rejected(self):
        for endpoint in ("http://api.invalid/v1/chat/completions", "https://user:pass@api.invalid/v1/chat/completions",
                         "https://api.invalid/v1/chat/completions?key=x", "https://api.invalid:443/v1/chat/completions",
                         "https://api.invalid/../v1/chat/completions", "https://*.invalid/v1/chat/completions"):
            value = decode(encode(self.value))
            value["endpoint_allowlist"] = [endpoint]
            value["bindings"][0]["endpoint"] = endpoint
            with self.subTest(endpoint=endpoint), self.assertRaises(launcher.LaunchError):
                launcher.parse_config(value)

    def test_environment_is_explicit_frozen_no_discovered_or_future_values(self):
        env = Mock()
        env.get.side_effect = self.env.get
        with patch.object(launcher.os, "environ", env):
            secrets = launcher.resolve_environment(self.config)
        self.assertEqual([call.args[0] for call in env.get.call_args_list],
                         ["CIVIC_GATEWAY_TOKEN_V1", "CIVIC_PROVIDER_ACCOUNT1_V1"])
        self.env["CIVIC_PROVIDER_ACCOUNT1_V1"] = "replacement-secret"
        self.assertEqual(secrets.resolve("CIVIC_PROVIDER_ACCOUNT1_V1"), SECRET)
        with self.assertRaises(launcher.LaunchError):
            secrets.resolve("CIVIC_PROVIDER_ACCOUNT1_V2")
        self.assertNotIn(SECRET, repr(secrets))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            self.config.listen_port = 9

    def test_bad_or_missing_environment_values_are_not_echoed(self):
        for env in ({}, self.env | {"CIVIC_PROVIDER_ACCOUNT1_V1": SECRET + "\r\nheader: bad"},
                    self.env | {"CIVIC_PROVIDER_ACCOUNT1_V1": TOKEN}):
            with patch.object(launcher.os, "environ", env), self.assertRaisesRegex(launcher.LaunchError, "^secret_unavailable$"):
                launcher.resolve_environment(self.config)

    def test_durable_claim_refuses_same_snapshot_even_with_modified_budget(self):
        launcher.claim_snapshot(self.config)
        with contextlib.closing(sqlite3.connect(self.journal)) as db:
            self.assertEqual(db.execute("SELECT snapshot_id, config_sha256 FROM snapshot_claims").fetchall(),
                             [("snapshot-1", self.config.sha256)])
        changed = decode(encode(self.value))
        changed["snapshot"]["routes"][0]["quota_tokens"] += 1
        with self.assertRaisesRegex(launcher.LaunchError, "snapshot_already_claimed"):
            launcher.claim_snapshot(launcher.parse_config(changed))

    def test_concurrent_claims_allow_one_process_equivalent_winner(self):
        outcomes = []
        def claim():
            try:
                launcher.claim_snapshot(self.config)
                outcomes.append("claimed")
            except launcher.LaunchError as exc:
                outcomes.append(str(exc))
        workers = [threading.Thread(target=claim) for _ in range(2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(6)
        self.assertCountEqual(outcomes, ["claimed", "snapshot_already_claimed"])

    def test_journal_failure_never_listens_and_logs_no_details(self):
        self.journal.parent.mkdir()
        self.journal.write_bytes(b"not-a-sqlite-database " + SECRET.encode())
        with patch.object(launcher.os, "environ", self.env), patch.object(launcher, "make_server") as server, \
                contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(launcher.main(["--config", str(self.path), "--serve"]), 2)
        server.assert_not_called()
        self.assertEqual(output.getvalue().strip(), "journal_unavailable")

    def test_serve_cli_to_concrete_transport_then_restart_is_refused(self):
        server = Mock()
        connection = Mock()
        connection.getresponse.return_value = FakeHTTPSResponse(chat_body())
        captured = []
        def create(gateway, token, port):
            self.assertTrue(self.journal.exists())
            self.assertEqual(token, TOKEN)
            server.serve_forever.side_effect = lambda: captured.append(gateway.handle(request(
                now=time.time(), max_input_tokens=10_000, max_cost_microusd=0)))
            return server
        with patch.object(launcher.os, "environ", self.env), \
                patch.object(launcher, "make_server", side_effect=create) as create_server, \
                patch("http.client.HTTPSConnection", return_value=connection), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(launcher.main(["--config", str(self.path), "--serve"]), 0)
            self.assertEqual(launcher.main(["--config", str(self.path), "--serve"]), 2)
        self.assertTrue(captured[0]["ok"], captured)
        create_server.assert_called_once()
        server.server_close.assert_called_once()
        connection.request.assert_called_once()
        self.assertIn("snapshot_already_claimed", output.getvalue())
        self.assertNotIn(SECRET, output.getvalue())
        self.assertNotIn(TOKEN, output.getvalue())

    def test_failed_listen_keeps_snapshot_claim(self):
        with patch.object(launcher.os, "environ", self.env), \
                patch.object(launcher, "make_server", side_effect=OSError(SECRET)), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(launcher.main(["--config", str(self.path), "--serve"]), 2)
        self.assertNotIn(SECRET, output.getvalue())
        with self.assertRaisesRegex(launcher.LaunchError, "snapshot_already_claimed"):
            launcher.claim_snapshot(self.config)


if __name__ == "__main__":
    unittest.main()
