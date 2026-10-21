"""STEP control state machine and signed protocol, using only an in-memory port."""
from copy import deepcopy
from itertools import combinations
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "binance_momentum_bot"))
import step_lock_control as control


PROFILE = {"version": 1, "basis": "margin", "margin_usdt": "200", "leverage": 10}
SCOPE = "STEP_TEST"


class Clock:
    def __init__(self):
        self.value = 1792580400000

    def __call__(self):
        return self.value


class FakePort:
    """Models the adapter boundary; never imports or contacts an exchange."""
    def __init__(self, now_ms):
        self.now_ms = now_ms
        self.journal = []
        self.block_calls = 0
        self.enable_calls = []
        self.close_calls = []
        self.tick_calls = []
        self.protection_ticks = 0
        self.entry_enabled = False
        self.ready = True
        self.live_capable = True
        self.position_known = True
        self.position = None
        self.flat_confirmed = True
        self.protection_active = False
        self.active_profile = None
        self.observed_at_override = None
        self.reason = ""
        self.enable_ack = True
        self.enable_error = None

    def inspect(self):
        return {"observed_at_ms": self.now_ms() if self.observed_at_override is None else self.observed_at_override,
                "ready": self.ready, "live_capable": self.live_capable,
                "entry_enabled": self.entry_enabled, "position_known": self.position_known,
                "position": deepcopy(self.position), "flat_confirmed": self.flat_confirmed,
                "protection_active": self.protection_active, "active_profile": deepcopy(self.active_profile),
                "reason": self.reason}

    def records(self):
        return deepcopy(self.journal)

    def record(self, kind, data):
        self.journal.append({"kind": kind, "data": deepcopy(data)})

    def block_entries(self):
        self.block_calls += 1
        self.entry_enabled = False

    def enable_entries(self, profile):
        self.enable_calls.append(deepcopy(profile))
        if self.enable_error:
            raise self.enable_error
        if self.enable_ack:
            self.entry_enabled = True
            self.active_profile = deepcopy(profile)

    def request_close(self, position_id):
        self.close_calls.append(position_id)

    def tick(self, close_requested=False):
        self.tick_calls.append(close_requested)
        if self.position is not None and self.protection_active:
            self.protection_ticks += 1

    def open_owned_position(self, intent_id="intent-1"):
        self.position = {"intent_id": intent_id, "symbol": "BTCUSDT", "quantity": "0.01", "side": "LONG"}
        self.flat_confirmed = False
        self.protection_active = True


class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.port = FakePort(self.clock)
        self.controller = control.Controller(self.port, scope_id=SCOPE, now_ms=self.clock)
        self.request_number = 0

    def payload(self, action, snapshot=None, **overrides):
        snapshot = snapshot or self.controller.status()
        self.request_number += 1
        payload = {"action": action, "request_id": f"request-{self.request_number:024d}",
                   "expected_scope_id": SCOPE, "expected_runtime_id": snapshot["runtime_id"],
                   "expected_version": snapshot["version"]}
        if action in ("start", "resume"):
            payload["profile"] = deepcopy(PROFILE)
        if action == "close_stop":
            payload["expected_position_id"] = (snapshot.get("position") or {}).get("intent_id")
        payload.update(overrides)
        return payload

    def execute(self, action, **overrides):
        return self.controller.execute(**self.payload(action, **overrides))

    def test_boot_blocks_entries_and_never_enables_or_closes(self):
        snapshot = self.controller.status()
        self.assertEqual("OFF", snapshot["state"])
        self.assertFalse(snapshot["entry_enabled"])
        self.assertGreaterEqual(self.port.block_calls, 1)
        self.assertFalse(self.port.enable_calls)
        self.assertFalse(self.port.close_calls)

    def test_live_pause_resume_and_close_wait_for_confirmed_terminal_flat(self):
        self.assertEqual("LIVE", self.execute("start")["state"])
        self.port.open_owned_position()
        self.assertEqual("PAUSED", self.execute("pause")["state"])
        self.assertFalse(self.port.entry_enabled)
        self.controller.tick()
        self.assertEqual(1, self.port.protection_ticks)
        self.assertFalse(self.port.close_calls)
        self.assertTrue(self.port.protection_active)
        self.assertEqual("LIVE", self.execute("resume")["state"])
        close_request = self.payload("close_stop")
        self.assertEqual("CLOSING", self.controller.execute(**close_request)["state"])
        self.assertEqual("CLOSING", self.controller.execute(**close_request)["state"])
        self.assertTrue(self.controller.needs_tick)
        self.assertFalse(self.port.entry_enabled)
        self.assertEqual(["intent-1"], self.port.close_calls)
        self.assertEqual("CLOSING", self.controller.tick()["state"])
        self.port.position = None
        self.port.protection_active = False
        # Position quantity zero alone is insufficient while owned orders remain.
        self.port.flat_confirmed = False
        self.assertEqual("CLOSING", self.controller.tick()["state"])
        self.port.flat_confirmed = True
        self.assertEqual("OFF", self.controller.tick()["state"])
        self.assertFalse(self.controller.needs_tick)
        self.assertFalse(self.port.entry_enabled)
        self.assertTrue(any(self.port.tick_calls))

    def test_pending_owned_entry_requires_close_and_terminal_confirmation(self):
        self.execute("start")
        self.port.open_owned_position("pending-intent")
        self.port.position["quantity"] = "0"
        self.port.protection_active = False
        self.assertEqual("CLOSING", self.execute("close_stop")["state"])
        self.assertEqual(["pending-intent"], self.port.close_calls)
        self.assertFalse(self.port.entry_enabled)
        self.assertEqual("CLOSING", self.controller.tick()["state"])
        self.port.position = None
        self.port.flat_confirmed = True
        self.assertEqual("OFF", self.controller.tick()["state"])

    def test_duplicate_request_is_applied_once_even_after_version_changes(self):
        request = self.payload("start")
        first = self.controller.execute(**request)
        second = self.controller.execute(**request)
        self.assertEqual("LIVE", first["state"])
        self.assertEqual("LIVE", second["state"])
        self.assertEqual(1, len(self.port.enable_calls))
        kinds = [item["kind"] for item in self.port.journal]
        self.assertEqual(1, kinds.count("CONTROL_REQUESTED"))
        self.assertEqual(1, kinds.count("CONTROL_APPLIED"))

    def test_conflicting_idempotency_payload_is_rejected(self):
        request = self.payload("start")
        self.controller.execute(**request)
        conflicting = deepcopy(request)
        conflicting["profile"]["margin_usdt"] = "100"
        with self.assertRaises(control.ControlUnavailable):
            self.controller.execute(**conflicting)
        self.assertEqual(1, len(self.port.enable_calls))

    def test_wrong_scope_runtime_or_version_cannot_enable_entries(self):
        for field, value in (("expected_scope_id", "NORMAL_TEST"),
                             ("expected_runtime_id", "old-runtime"), ("expected_version", -1)):
            with self.subTest(field=field), self.assertRaises(control.ControlUnavailable):
                self.controller.execute(**self.payload("start", **{field: value}))
        self.assertFalse(self.port.enable_calls)

    def test_stale_or_unknown_position_state_blocks_start(self):
        request = self.payload("start")
        self.port.observed_at_override = self.clock() - 60000
        with self.assertRaises(control.ControlUnavailable):
            self.controller.execute(**request)
        self.assertFalse(self.port.enable_calls)
        self.port.observed_at_override = None
        request = self.payload("start")
        self.port.position_known = False
        self.port.flat_confirmed = False
        with self.assertRaises(control.ControlUnavailable):
            self.controller.execute(**request)
        self.assertFalse(self.port.enable_calls)

    def test_runtime_guards_and_existing_position_block_start(self):
        for flag in ("ready", "live_capable", "flat_confirmed"):
            request = self.payload("start")
            setattr(self.port, flag, False)
            with self.subTest(flag=flag), self.assertRaises(control.ControlUnavailable):
                self.controller.execute(**request)
            setattr(self.port, flag, True)
        self.port.open_owned_position()
        with self.assertRaises(control.ControlUnavailable):
            self.execute("start")
        self.assertFalse(self.port.enable_calls)

    def test_close_is_bound_to_current_owned_intent(self):
        self.execute("start")
        self.port.open_owned_position("old-intent")
        request = self.payload("close_stop")
        self.port.open_owned_position("replacement-intent")
        with self.assertRaises(control.ControlUnavailable):
            self.controller.execute(**request)
        self.assertFalse(self.port.close_calls)

    def test_enable_requires_authoritative_acknowledgement(self):
        self.port.enable_ack = False
        with self.assertRaises(control.ControlUnavailable):
            self.execute("start")
        self.assertFalse(self.port.entry_enabled)
        self.assertNotEqual("LIVE", self.controller.status()["state"])

    def test_failed_enable_is_recorded_and_replay_does_not_repeat_effect(self):
        self.port.enable_error = RuntimeError("simulated adapter failure")
        request = self.payload("start")
        with self.assertRaises(control.ControlUnavailable):
            self.controller.execute(**request)
        try:
            self.controller.execute(**request)
        except control.ControlUnavailable:
            pass
        self.assertEqual(1, len(self.port.enable_calls))
        self.assertFalse(self.port.entry_enabled)
        self.assertIn("CONTROL_FAILED", [item["kind"] for item in self.port.journal])

    def test_restart_blocks_entries_and_does_not_replay_recorded_start(self):
        request = self.payload("start")
        self.controller.execute(**request)
        old_runtime = self.controller.status()["runtime_id"]
        restarted = control.Controller(self.port, scope_id=SCOPE, now_ms=self.clock)
        self.assertFalse(self.port.entry_enabled)
        self.assertNotEqual(old_runtime, restarted.status()["runtime_id"])
        try:
            restarted.execute(**request)
        except control.ControlUnavailable:
            pass
        self.assertEqual(1, len(self.port.enable_calls))
        self.assertFalse(self.port.entry_enabled)

    def test_requested_record_write_failure_prevents_effect_and_same_request_replay(self):
        request = self.payload("start")
        with patch.object(self.port, "record", side_effect=OSError("simulated journal write failure")) as record:
            with self.assertRaises(control.ControlUnavailable) as first:
                self.controller.execute(**request)
            self.assertEqual("CONTROL_JOURNAL_UNAVAILABLE", first.exception.code)
            with self.assertRaises(control.ControlUnavailable) as replay:
                self.controller.execute(**request)
            self.assertEqual("CONTROL_EFFECT_UNCERTAIN", replay.exception.code)
            self.assertEqual(1, record.call_count)
        self.assertFalse(self.port.enable_calls)
        self.assertFalse(self.port.entry_enabled)

    def test_dangling_durable_request_recovery_never_reapplies_it(self):
        request = self.payload("start")
        original_record = self.port.record

        def stored_but_acknowledgement_lost(kind, data):
            original_record(kind, data)
            raise OSError("simulated lost journal acknowledgement")

        with patch.object(self.port, "record", side_effect=stored_but_acknowledgement_lost):
            with self.assertRaises(control.ControlUnavailable):
                self.controller.execute(**request)
        restarted = control.Controller(self.port, scope_id=SCOPE, now_ms=self.clock)
        snapshot = restarted.status()
        self.assertEqual("ERROR", snapshot["state"])
        self.assertEqual("CONTROL_EFFECT_UNCERTAIN", snapshot["reason"])
        with self.assertRaises(control.ControlUnavailable):
            restarted.execute(**request)
        fresh_request = {**request, "request_id": "new-request-after-crash-00000001",
                         "expected_runtime_id": snapshot["runtime_id"], "expected_version": snapshot["version"]}
        with self.assertRaises(control.ControlUnavailable):
            restarted.execute(**fresh_request)
        self.assertFalse(self.port.enable_calls)
        self.assertFalse(self.port.entry_enabled)

    def test_losing_readiness_blocks_admission_and_preserves_protection_ticks(self):
        self.execute("start")
        self.port.open_owned_position()
        self.port.ready = False
        snapshot = self.controller.status()
        self.assertFalse(self.port.entry_enabled)
        self.assertNotEqual("LIVE", snapshot["state"])
        self.controller.tick()
        self.assertEqual(1, self.port.protection_ticks)
        self.assertTrue(self.port.protection_active)
        self.assertFalse(self.port.close_calls)

    def test_failed_close_recovery_preserves_error_without_retrying_close(self):
        self.execute("start")
        self.port.open_owned_position()

        def failed_close(position_id):
            self.port.close_calls.append(position_id)
            raise RuntimeError("simulated close adapter failure")

        with patch.object(self.port, "request_close", side_effect=failed_close):
            with self.assertRaises(control.ControlUnavailable):
                self.execute("close_stop")
            restarted = control.Controller(self.port, scope_id=SCOPE, now_ms=self.clock)
            snapshot = restarted.status()
            self.assertEqual("ERROR", snapshot["state"])
            self.assertEqual("CONTROL_PREVIOUSLY_FAILED", snapshot["reason"])
            self.assertFalse(snapshot["allowed_actions"])
            restarted.tick()
        self.assertFalse(self.port.tick_calls[-1])
        self.assertEqual(["intent-1"], self.port.close_calls)
        self.assertEqual(1, self.port.protection_ticks)
        self.assertFalse(self.port.entry_enabled)


class SnapshotAndProfileTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.controller = control.Controller(FakePort(self.clock), scope_id=SCOPE, now_ms=self.clock)

    def test_valid_snapshot_and_maximum_profile(self):
        snapshot = self.controller.status()
        control.validate_snapshot(snapshot, now_ms=self.clock())
        control.validate_profile(PROFILE)

    def test_stale_future_or_wrong_typed_snapshot_is_rejected(self):
        valid = self.controller.status()
        for field, value in (("observed_at_ms", self.clock() - 60000),
                             ("observed_at_ms", self.clock() + 60000),
                             ("version", True), ("entry_enabled", "false"),
                             ("position_known", "true"), ("protocol", "NORMAL_CONTROL_V1")):
            candidate = deepcopy(valid)
            candidate[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(control.ControlUnavailable):
                control.validate_snapshot(candidate, now_ms=self.clock())

    def test_profile_limits_types_and_nonfinite_values_are_rejected(self):
        for field, value in (("margin_usdt", "200.01"), ("margin_usdt", "NaN"),
                             ("margin_usdt", "0"), ("margin_usdt", "-1"),
                             ("leverage", 11), ("leverage", 1.5), ("leverage", True), ("version", True)):
            candidate = deepcopy(PROFILE)
            candidate[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(control.ControlUnavailable):
                control.validate_profile(candidate)


class SignedProtocolTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.port = FakePort(self.clock)
        self.controller = control.Controller(self.port, scope_id=SCOPE, now_ms=self.clock)
        self.key = b"step-control-test-key-material-32"
        self.authority = control.ControlAuthority(self.controller, self.key, scope_id=SCOPE, now_ms=self.clock)
        self.nonce = 0

    def request(self, method="status", payload=None, **overrides):
        self.nonce += 1
        args = {"now_ms": self.clock(), "nonce": f"{self.nonce:032x}"}
        args.update(overrides)
        return control.make_request(self.key, SCOPE, method, payload or {}, **args)

    def test_signed_round_trip_and_wrong_request_binding(self):
        request = self.request()
        reply = self.authority.handle(request)
        result = control.verify_reply(self.key, request, reply, now_ms=self.clock())
        self.assertEqual("OFF", result["state"])
        with self.assertRaises(control.ControlUnavailable):
            control.verify_reply(self.key, self.request(), reply, now_ms=self.clock())

    def test_replayed_request_expired_request_and_bad_signature_fail(self):
        request = self.request()
        self.authority.handle(request)
        with self.assertRaises(control.ControlUnavailable):
            self.authority.handle(request)
        expired = self.request(now_ms=self.clock() - 120000)
        with self.assertRaises(control.ControlUnavailable):
            self.authority.handle(expired)
        tampered = self.request()
        tampered["signature"] = "0" * 64
        with self.assertRaises(control.ControlUnavailable):
            self.authority.handle(tampered)
        self.assertFalse(self.port.enable_calls)

    def test_tampered_or_stale_reply_is_rejected(self):
        request = self.request()
        reply = self.authority.handle(request)
        tampered = deepcopy(reply)
        tampered["signature"] = "0" * 64
        with self.assertRaises(control.ControlUnavailable):
            control.verify_reply(self.key, request, tampered, now_ms=self.clock())
        with self.assertRaises(control.ControlUnavailable):
            control.verify_reply(self.key, request, reply, now_ms=self.clock() + 60000)

    def test_new_nonce_retry_of_same_command_does_not_duplicate_effect(self):
        snapshot = self.controller.status()
        payload = {"action": "start", "request_id": "idempotent-request-0000000000001",
                   "expected_scope_id": SCOPE, "expected_runtime_id": snapshot["runtime_id"],
                   "expected_version": snapshot["version"], "profile": deepcopy(PROFILE)}
        for _ in range(2):
            request = self.request("execute", payload)
            reply = self.authority.handle(request)
            self.assertEqual("LIVE", control.verify_reply(self.key, request, reply, now_ms=self.clock())["state"])
        self.assertEqual(1, len(self.port.enable_calls))

    def test_authority_rejects_wrong_account_scope_and_unknown_method(self):
        wrong_scope = control.make_request(self.key, "NORMAL_TEST", "status", {},
                                           now_ms=self.clock(), nonce="f" * 32)
        with self.assertRaises(control.ControlUnavailable):
            self.authority.handle(wrong_scope)
        with self.assertRaises(control.ControlUnavailable):
            self.authority.handle(self.request("close_all"))
        self.assertFalse(self.port.enable_calls)
        self.assertFalse(self.port.close_calls)


class HTTPSBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.key = b"step-control-test-key-material-32"
        self.url = "https://step-control.example.invalid/step-control/v1"
        self.client = control.HTTPSControlClient(self.url, self.key, SCOPE)
        self.controller = control.Controller(FakePort(self.clock), scope_id=SCOPE, now_ms=self.clock)
        self.authority = control.ControlAuthority(self.controller, self.key, scope_id=SCOPE, now_ms=self.clock)

    def test_redirect_is_rejected_without_following_it(self):
        opened = []

        def build_opener(*handlers):
            def open_request(request, timeout):
                opened.append(request.full_url)
                for handler in handlers:
                    if isinstance(handler, control._NoRedirect):
                        return handler.redirect_request(request, None, 302, "Found", {},
                                                        "https://untrusted.example.invalid/collect")
                self.fail("HTTPS transport did not install its redirect rejection handler")
            return Mock(open=open_request)

        with patch.object(control.urllib.request, "build_opener", side_effect=build_opener), \
                patch.object(control, "_now", self.clock):
            with self.assertRaises(control.ControlUnavailable) as error:
                self.client._call("status", {})
        self.assertEqual("REDIRECT_REJECTED", error.exception.code)
        self.assertEqual([self.url], opened)

    def _response_for(self, request, change):
        envelope = control.loads_strict(request.data)
        reply = self.authority.handle(envelope)
        body = change(reply)

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *unused):
                return False

            def read(self, size):
                return body[:size]

        return Response()

    def test_forged_https_reply_is_not_accepted_or_retried(self):
        def forged(reply):
            reply["signature"] = "0" * 64
            return control.canonical_bytes(reply)

        opener = Mock()
        opener.open.side_effect = lambda request, timeout: self._response_for(request, forged)
        with patch.object(control.urllib.request, "build_opener", return_value=opener), \
                patch.object(control, "_now", self.clock):
            with self.assertRaises(control.ControlUnavailable) as error:
                self.client._call("status", {})
        self.assertEqual("INVALID_SIGNATURE", error.exception.code)
        self.assertEqual(1, opener.open.call_count)

    def test_raw_duplicate_json_keys_are_rejected_before_reply_acceptance(self):
        def duplicated_signature(reply):
            # A normal last-key-wins decoder would accept the valid second signature.
            return (b'{"reply":' + control.canonical_bytes(reply["reply"]) +
                    b',"signature":"' + b"0" * 64 + b'","signature":"' +
                    reply["signature"].encode("ascii") + b'"}')

        opener = Mock()
        opener.open.side_effect = lambda request, timeout: self._response_for(request, duplicated_signature)
        with patch.object(control.urllib.request, "build_opener", return_value=opener), \
                patch.object(control, "_now", self.clock):
            with self.assertRaises(control.ControlUnavailable) as error:
                self.client._call("status", {})
        self.assertEqual("DUPLICATE_JSON_KEY", error.exception.code)
        self.assertEqual(1, opener.open.call_count)

    def test_missing_or_partial_configuration_never_constructs_network_client(self):
        values = {"STEP_CONTROL_URL": self.url, "STEP_OPERATOR_CONTROL_KEY": self.key.hex(),
                  "STEP_CONTROL_SCOPE": SCOPE}
        for count in range(3):
            for present in combinations(values, count):
                configured = {name: values[name] if name in present else "" for name in values}
                with self.subTest(present=present), patch.dict(os.environ, configured), \
                        patch.object(control, "HTTPSControlClient") as client_factory, \
                        patch.object(control.urllib.request, "build_opener") as connection:
                    if count == 0:
                        self.assertIsNone(control.client_for({}))
                    else:
                        with self.assertRaises(control.ControlUnavailable) as error:
                            control.client_for({})
                        self.assertEqual("INCOMPLETE_CONTROL_CONFIGURATION", error.exception.code)
                    client_factory.assert_not_called()
                    connection.assert_not_called()


if __name__ == "__main__":
    unittest.main()
