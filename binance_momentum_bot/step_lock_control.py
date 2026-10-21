"""STEP-only operator control protocol. Importing this module performs no I/O.

The controller grants no execution authority: the runtime port must retain its
own account, numeric, exchange and final-arm guards. The bundled accepted STEP
runtime has final arm OFF and therefore cannot enable entries through this API.
"""
from __future__ import annotations

import asyncio
from copy import deepcopy
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


PROTOCOL = "STEP_CONTROL_V1"
MAX_BODY_BYTES = 65536
MAX_AGE_MS = 15000
REQUEST_TTL_MS = 10000
CLOCK_ALLOWANCE_MS = 1000
ACTIONS = frozenset(("start", "pause", "resume", "close_stop"))
STATES = frozenset(("OFF", "LIVE", "PAUSED", "CLOSING", "ERROR", "UNAVAILABLE"))
_IDENTIFIER = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
_REQUEST_ID = re.compile(r"[A-Za-z0-9_-]{16,128}\Z")
_HEX_NONCE = re.compile(r"[a-f0-9]{32,64}\Z")


class ControlUnavailable(RuntimeError):
    """A bounded, non-secret error code safe for the operator boundary."""
    def __init__(self, code="CONTROL_UNAVAILABLE"):
        self.code = code if re.fullmatch(r"[A-Z][A-Z0-9_]{0,95}", str(code)) else "CONTROL_UNAVAILABLE"
        super().__init__(self.code)


def _fail(code):
    raise ControlUnavailable(code)


def _now():
    return time.time_ns() // 1000000


def _timestamp(value, now, age=MAX_AGE_MS):
    if type(value) is not int or value < now - age or value > now + CLOCK_ALLOWANCE_MS:
        _fail("STALE_STATUS")


def _identifier(value, code="INVALID_IDENTIFIER"):
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        _fail(code)
    return value


def _exact_keys(value, required, optional=()):
    if type(value) is not dict or not set(required).issubset(value) or set(value) - set(required) - set(optional):
        _fail("INVALID_SCHEMA")


def _decimal(value, *, zero=False):
    if not isinstance(value, str) or len(value) > 40 or not re.fullmatch(r"\d+(?:\.\d+)?", value):
        _fail("INVALID_AMOUNT")
    try:
        amount = Decimal(value)
    except InvalidOperation:
        _fail("INVALID_AMOUNT")
    if not amount.is_finite() or amount < 0 or (amount == 0 and not zero):
        _fail("INVALID_AMOUNT")
    return amount


def validate_profile(profile):
    _exact_keys(profile, ("version", "basis", "margin_usdt", "leverage"))
    if type(profile["version"]) is not int or profile["version"] != 1:
        _fail("INVALID_PROFILE")
    if profile["basis"] == "legacy_notional":
        if profile["margin_usdt"] is not None or profile["leverage"] is not None:
            _fail("INVALID_PROFILE")
    elif profile["basis"] == "margin":
        amount = _decimal(profile["margin_usdt"])
        leverage = profile["leverage"]
        if amount > 200 or amount.as_tuple().exponent < -2 or type(leverage) is not int or not 1 <= leverage <= 10:
            _fail("PROFILE_LIMIT_EXCEEDED")
        if amount * leverage > 2000:
            _fail("PROFILE_LIMIT_EXCEEDED")
    else:
        _fail("INVALID_PROFILE")
    return deepcopy(profile)


_OBSERVATION_FIELDS = ("observed_at_ms", "ready", "live_capable", "position_known", "position",
                       "flat_confirmed", "protection_active", "active_profile", "reason", "entry_enabled")


def _validate_observation(value, now):
    if type(value) is not dict or not set(_OBSERVATION_FIELDS).issubset(value):
        _fail("INVALID_STATUS")
    _timestamp(value["observed_at_ms"], now)
    for field in ("ready", "live_capable", "position_known", "flat_confirmed", "entry_enabled"):
        if type(value[field]) is not bool:
            _fail("INVALID_STATUS")
    if value["protection_active"] is not None and type(value["protection_active"]) is not bool:
        _fail("INVALID_STATUS")
    if not isinstance(value["reason"], str) or len(value["reason"]) > 512:
        _fail("INVALID_STATUS")
    position = value["position"]
    if position is not None:
        _exact_keys(position, ("intent_id", "symbol", "quantity", "side"))
        _identifier(position["intent_id"])
        if not isinstance(position["symbol"], str) or not re.fullmatch(r"[A-Z0-9]{2,30}", position["symbol"]):
            _fail("INVALID_POSITION")
        _decimal(position["quantity"], zero=True)  # zero-fill owned pending entry still needs draining
        if position["side"] != "LONG" or not value["position_known"] or value["flat_confirmed"]:
            _fail("INVALID_POSITION")
    if value["flat_confirmed"] and (not value["position_known"] or position is not None):
        _fail("UNVERIFIED_FLAT")
    if not value["position_known"] and value["protection_active"] is not None:
        _fail("UNVERIFIED_PROTECTION")
    if value["active_profile"] is not None:
        validate_profile(value["active_profile"])
    return deepcopy(value)


def validate_snapshot(snapshot, now_ms=None):
    _exact_keys(snapshot, ("protocol", "scope_id", "runtime_id", "version", "state", "allowed_actions") + _OBSERVATION_FIELDS)
    _validate_observation(snapshot, _now() if now_ms is None else now_ms)
    if snapshot["protocol"] != PROTOCOL or snapshot["state"] not in STATES:
        _fail("INVALID_STATUS")
    _identifier(snapshot["scope_id"])
    _identifier(snapshot["runtime_id"])
    if type(snapshot["version"]) is not int or snapshot["version"] < 0:
        _fail("INVALID_STATUS")
    choices = snapshot["allowed_actions"]
    if type(choices) is not list or any(type(x) is not str or x not in ACTIONS for x in choices) or len(set(choices)) != len(choices):
        _fail("INVALID_ACTIONS")
    state = snapshot["state"]
    if state == "OFF" and (snapshot["entry_enabled"] or not snapshot["flat_confirmed"]):
        _fail("UNVERIFIED_OFF")
    if state == "LIVE" and (not snapshot["entry_enabled"] or not snapshot["live_capable"] or not snapshot["ready"]):
        _fail("UNVERIFIED_LIVE")
    if state in ("PAUSED", "CLOSING") and snapshot["entry_enabled"]:
        _fail("UNVERIFIED_PAUSE")
    if state in ("ERROR", "UNAVAILABLE", "CLOSING") and choices:
        _fail("INVALID_ACTIONS")
    permissible = {"OFF": {"start"}, "LIVE": {"pause", "close_stop"}, "PAUSED": {"resume", "close_stop"}}.get(state, set())
    if set(choices) - permissible:
        _fail("INVALID_ACTIONS")
    if any(x in choices for x in ("start", "resume")):
        if not snapshot["ready"] or not snapshot["live_capable"] or not snapshot["position_known"]:
            _fail("INVALID_ACTIONS")
    if "start" in choices and not snapshot["flat_confirmed"]:
        _fail("UNVERIFIED_FLAT")
    return deepcopy(snapshot)


def canonical_bytes(value):
    try:
        result = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, OverflowError):
        _fail("INVALID_JSON")
    if len(result) > MAX_BODY_BYTES:
        _fail("BODY_TOO_LARGE")
    return result


def loads_strict(body):
    if not isinstance(body, (bytes, str)) or len(body) > MAX_BODY_BYTES:
        _fail("BODY_TOO_LARGE")
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                _fail("DUPLICATE_JSON_KEY")
            result[key] = value
        return result
    try:
        return json.loads(body, object_pairs_hook=pairs, parse_constant=lambda _: _fail("INVALID_JSON"))
    except (UnicodeError, ValueError, TypeError, RecursionError):
        _fail("INVALID_JSON")


class Controller:
    """Serialized STEP admission/close intent, with durable idempotency receipts.

    The port owns execution, position reconciliation and protection. Pause only
    blocks admission. Close is complete only after a fresh terminal-flat proof.
    """
    def __init__(self, port, scope_id, now_ms=None):
        self.port = port
        self.scope_id = _identifier(scope_id)
        self.runtime_id = secrets.token_hex(16)
        self.now_ms = now_ms or _now
        self._lock = threading.RLock()
        self._mode = "OFF"
        self._error = None
        self._closing = False
        self._target = None
        self._version = 0
        self._fingerprint = None
        self._receipts = {}
        try:
            self.port.block_entries()
            self._recover(self.port.records())
        except Exception:
            self._error = "CONTROL_RECOVERY_FAILED"

    @property
    def needs_tick(self):
        return self._closing

    def _recover(self, records):
        if type(records) is not list:
            _fail("INVALID_CONTROL_JOURNAL")
        for record in records:
            if type(record) is not dict or not isinstance(record.get("kind"), str):
                _fail("INVALID_CONTROL_JOURNAL")
            kind, data = record["kind"], record.get("data", {})
            if not kind.startswith("CONTROL_"):
                continue
            if type(data) is not dict or data.get("scope_id") != self.scope_id:
                _fail("CONTROL_JOURNAL_SCOPE_MISMATCH")
            if kind == "CONTROL_REQUESTED":
                request = data.get("request")
                if type(request) is not dict or not _REQUEST_ID.fullmatch(str(request.get("request_id", ""))):
                    _fail("INVALID_CONTROL_JOURNAL")
                digest = hashlib.sha256(canonical_bytes(request)).hexdigest()
                if digest != data.get("digest") or request["request_id"] in self._receipts:
                    _fail("INVALID_CONTROL_JOURNAL")
                self._receipts[request["request_id"]] = {"digest": digest, "state": "requested", "request": request}
            elif kind in ("CONTROL_APPLIED", "CONTROL_FAILED"):
                receipt = self._receipts.get(data.get("request_id"))
                if receipt is None or receipt["digest"] != data.get("digest") or receipt["state"] != "requested":
                    _fail("INVALID_CONTROL_JOURNAL")
                receipt["state"] = "applied" if kind == "CONTROL_APPLIED" else "failed"
                if receipt["request"]["action"] == "close_stop":
                    # A crash during close must never silently restore admission.
                    self._closing = True
                    self._target = receipt["request"].get("expected_position_id")
                    if kind == "CONTROL_FAILED":
                        self._error = "CONTROL_PREVIOUSLY_FAILED"
            elif kind == "CONTROL_CLOSE_COMPLETE":
                self._closing = False
                self._target = None
                if self._error == "CONTROL_PREVIOUSLY_FAILED":
                    self._error = None
        if any(item["state"] == "requested" for item in self._receipts.values()):
            self._error = "CONTROL_EFFECT_UNCERTAIN"
        if self._closing:
            self._mode = "CLOSING"

    def _read(self):
        return _validate_observation(self.port.inspect(), self.now_ms())

    def _unknown(self, reason):
        return {"observed_at_ms": self.now_ms(), "ready": False, "live_capable": False,
                "position_known": False, "position": None, "flat_confirmed": False,
                "protection_active": None, "active_profile": None, "reason": reason,
                "entry_enabled": False}

    def _snapshot(self, observation=None):
        try:
            observed = observation if observation is not None else self._read()
            if observed["entry_enabled"] and (self._error or self._closing or not observed["ready"]
                                              or not observed["live_capable"] or not observed["position_known"]):
                # Loss of authority/source health must close admission without
                # interrupting the port's ongoing protection/reconciliation loop.
                self.port.block_entries()
                observed = self._read()
                if observed["entry_enabled"]:
                    self._error = "ENTRY_BLOCK_NOT_CONFIRMED"
            unavailable = False
        except Exception:
            try:
                self.port.block_entries()
            except Exception:
                pass
            observed = self._unknown("STATUS_UNAVAILABLE")
            unavailable = True
        if unavailable:
            state = "UNAVAILABLE"
        elif self._error:
            state = "ERROR"
            observed["reason"] = self._error
        elif self._closing:
            state = "CLOSING" if not observed["entry_enabled"] else "ERROR"
        elif observed["entry_enabled"]:
            state = "LIVE" if observed["ready"] and observed["live_capable"] else "ERROR"
        elif self._mode in ("LIVE", "PAUSED") or not observed["flat_confirmed"]:
            state = "PAUSED" if observed["position_known"] else "UNAVAILABLE"
        else:
            state = "OFF"
        choices = []
        capable = observed["ready"] and observed["live_capable"] and observed["position_known"]
        protected = observed["position"] is None or _decimal(observed["position"]["quantity"], zero=True) == 0 or observed["protection_active"] is True
        if state == "OFF" and capable and observed["flat_confirmed"]:
            choices = ["start"]
        elif state == "LIVE":
            choices = ["pause"]
            if observed["position_known"] and (observed["live_capable"] or observed["flat_confirmed"]):
                choices.append("close_stop")
        elif state == "PAUSED":
            if capable and protected and (observed["position"] is not None or observed["flat_confirmed"]):
                choices.append("resume")
            if observed["position_known"] and (observed["live_capable"] or observed["flat_confirmed"]):
                choices.append("close_stop")
        snapshot = {key: deepcopy(observed[key]) for key in _OBSERVATION_FIELDS}
        snapshot.update(protocol=PROTOCOL, scope_id=self.scope_id, runtime_id=self.runtime_id,
                        state=state, allowed_actions=choices, version=self._version)
        relevant = {key: value for key, value in snapshot.items() if key not in ("version", "observed_at_ms")}
        digest = hashlib.sha256(canonical_bytes(relevant)).hexdigest()
        if digest != self._fingerprint:
            self._version += 1
            self._fingerprint = digest
        snapshot["version"] = self._version
        return validate_snapshot(snapshot, now_ms=self.now_ms())

    def status(self):
        with self._lock:
            return self._snapshot()

    def _record(self, kind, data):
        self.port.record(kind, {"scope_id": self.scope_id, "runtime_id": self.runtime_id, **data})

    def execute(self, *, action, request_id, expected_scope_id, expected_runtime_id,
                expected_version, profile=None, expected_position_id=None):
        with self._lock:
            if action not in ACTIONS or not isinstance(request_id, str) or not _REQUEST_ID.fullmatch(request_id):
                _fail("INVALID_REQUEST")
            if expected_scope_id != self.scope_id:
                _fail("SCOPE_MISMATCH")
            if type(expected_version) is not int or expected_version < 0:
                _fail("VERSION_MISMATCH")
            _identifier(expected_runtime_id)
            if expected_position_id is not None:
                _identifier(expected_position_id)
            if action in ("start", "resume"):
                profile = validate_profile(profile)
            elif profile is not None:
                _fail("UNEXPECTED_PROFILE")
            request = dict(action=action, request_id=request_id, expected_scope_id=expected_scope_id,
                           expected_runtime_id=expected_runtime_id, expected_version=expected_version,
                           profile=profile, expected_position_id=expected_position_id)
            digest = hashlib.sha256(canonical_bytes(request)).hexdigest()
            prior = self._receipts.get(request_id)
            if prior:
                if prior["digest"] != digest:
                    _fail("IDEMPOTENCY_CONFLICT")
                if prior["state"] != "applied":
                    _fail("CONTROL_EFFECT_UNCERTAIN" if prior["state"] == "requested" else "CONTROL_PREVIOUSLY_FAILED")
                return self._snapshot()
            snapshot = self._snapshot()
            if expected_runtime_id != self.runtime_id:
                _fail("RUNTIME_CHANGED")
            if expected_version != snapshot["version"]:
                _fail("STATUS_CHANGED")
            if action not in snapshot["allowed_actions"]:
                _fail("ACTION_NOT_ALLOWED")
            position = snapshot["position"]
            if action == "close_stop" and expected_position_id != (position or {}).get("intent_id"):
                _fail("POSITION_CHANGED")
            if action == "resume" and position is not None and profile != snapshot["active_profile"]:
                _fail("PROFILE_LOCKED_WHILE_POSITION_ACTIVE")
            receipt = {"digest": digest, "state": "requested", "request": deepcopy(request)}
            # Keep the in-memory key even if a mirrored write has uncertain acknowledgement.
            self._receipts[request_id] = receipt
            try:
                self._record("CONTROL_REQUESTED", {"request": request, "digest": digest})
            except Exception:
                self._error = "CONTROL_JOURNAL_UNAVAILABLE"
                try:
                    self.port.block_entries()
                except Exception:
                    pass
                _fail(self._error)
            try:
                if action in ("start", "resume"):
                    self.port.enable_entries(deepcopy(profile))
                    checked = self._read()
                    if not checked["entry_enabled"] or not checked["ready"] or not checked["live_capable"] or checked["active_profile"] != profile:
                        _fail("ENABLE_NOT_CONFIRMED")
                    self._mode = "LIVE"
                elif action == "pause":
                    self.port.block_entries()
                    if self._read()["entry_enabled"]:
                        _fail("PAUSE_NOT_CONFIRMED")
                    self._mode = "PAUSED"
                else:
                    self.port.block_entries()
                    if self._read()["entry_enabled"]:
                        _fail("PAUSE_NOT_CONFIRMED")
                    self._closing = True
                    self._target = expected_position_id
                    self._mode = "CLOSING"
                    # A close request is an intent, not evidence that an order filled.
                    self.port.request_close(expected_position_id)
                self._record("CONTROL_APPLIED", {"request_id": request_id, "digest": digest, "action": action})
                receipt["state"] = "applied"
            except Exception as exc:
                try:
                    self.port.block_entries()
                except Exception:
                    pass
                self._error = exc.code if isinstance(exc, ControlUnavailable) else "CONTROL_APPLY_FAILED"
                try:
                    self._record("CONTROL_FAILED", {"request_id": request_id, "digest": digest, "reason": self._error})
                    receipt["state"] = "failed"
                except Exception:
                    self._error = "CONTROL_EFFECT_UNCERTAIN"
                _fail(self._error)
            return self._snapshot()

    def tick(self):
        with self._lock:
            try:
                # The runtime retains responsibility for managing an open position
                # even when entry admission is paused or an operator call failed.
                self.port.tick(close_requested=self._closing and self._error is None)
                observed = self._read()
                if self._closing and observed["flat_confirmed"] and not observed["entry_enabled"]:
                    self._record("CONTROL_CLOSE_COMPLETE", {"position_id": self._target})
                    self._closing = False
                    self._target = None
                    self._mode = "OFF"
                    self._error = None
                return self._snapshot(observed)
            except Exception:
                self._error = "CONTROL_TICK_FAILED"
                try:
                    self.port.block_entries()
                except Exception:
                    pass
                return self._snapshot()


def _key_bytes(key):
    if not isinstance(key, bytes) or len(key) < 32:
        _fail("INVALID_CONTROL_KEY")
    return key


def _signature(key, value):
    return hmac.new(_key_bytes(key), canonical_bytes(value), hashlib.sha256).hexdigest()


def _check_signature(key, value, signature):
    if not isinstance(signature, str) or not re.fullmatch(r"[a-f0-9]{64}", signature) or not hmac.compare_digest(_signature(key, value), signature):
        _fail("INVALID_SIGNATURE")


def make_request(key, scope_id, method, payload, now_ms=None, nonce=None):
    issued = _now() if now_ms is None else now_ms
    request = {"protocol": PROTOCOL, "scope_id": _identifier(scope_id), "method": method,
               "payload": deepcopy(payload), "nonce": nonce or secrets.token_hex(16),
               "issued_ms": issued, "expires_ms": issued + REQUEST_TTL_MS}
    return {"request": request, "signature": _signature(key, request)}


class ControlAuthority:
    """End-to-end operator authentication at the gateway, not at the relay."""
    def __init__(self, controller, key, scope_id, now_ms=None):
        self.controller = controller
        self.key = _key_bytes(key)
        self.scope_id = _identifier(scope_id)
        if controller.scope_id != scope_id:
            _fail("SCOPE_MISMATCH")
        self.now_ms = now_ms or _now
        self._nonces = {}
        self._lock = threading.Lock()

    def handle(self, envelope):
        canonical_bytes(envelope)
        _exact_keys(envelope, ("request", "signature"))
        request = envelope["request"]
        _exact_keys(request, ("protocol", "scope_id", "method", "payload", "nonce", "issued_ms", "expires_ms"))
        _check_signature(self.key, request, envelope["signature"])
        if request["protocol"] != PROTOCOL or request["scope_id"] != self.scope_id:
            _fail("SCOPE_MISMATCH")
        now = self.now_ms()
        _timestamp(request["issued_ms"], now, REQUEST_TTL_MS)
        if type(request["expires_ms"]) is not int or request["expires_ms"] != request["issued_ms"] + REQUEST_TTL_MS or request["expires_ms"] < now:
            _fail("REQUEST_EXPIRED")
        nonce = request["nonce"]
        if not isinstance(nonce, str) or not _HEX_NONCE.fullmatch(nonce):
            _fail("INVALID_NONCE")
        method, payload = request["method"], request["payload"]
        if method == "status":
            _exact_keys(payload, ())
        elif method == "execute":
            _exact_keys(payload, ("action", "request_id", "expected_scope_id", "expected_runtime_id", "expected_version"), ("profile", "expected_position_id"))
        else:
            _fail("INVALID_METHOD")
        with self._lock:
            self._nonces = {item: expiry for item, expiry in self._nonces.items() if expiry >= now}
            if nonce in self._nonces:
                _fail("NONCE_REPLAY")
            if len(self._nonces) >= 4096:
                _fail("CONTROL_BUSY")
            self._nonces[nonce] = request["expires_ms"]
        result = self.controller.status() if method == "status" else self.controller.execute(**payload)
        reply = {"nonce": nonce, "scope_id": self.scope_id, "observed_at_ms": self.now_ms(), "result": result}
        return {"reply": reply, "signature": _signature(self.key, reply)}


def verify_reply(key, request_envelope, reply_envelope, now_ms=None):
    canonical_bytes(reply_envelope)
    _exact_keys(reply_envelope, ("reply", "signature"))
    reply = reply_envelope["reply"]
    _exact_keys(reply, ("nonce", "scope_id", "observed_at_ms", "result"))
    _check_signature(key, reply, reply_envelope["signature"])
    request = request_envelope["request"]
    if reply["nonce"] != request["nonce"] or reply["scope_id"] != request["scope_id"]:
        _fail("REPLY_BINDING_MISMATCH")
    now = _now() if now_ms is None else now_ms
    _timestamp(reply["observed_at_ms"], now)
    snapshot = validate_snapshot(reply["result"], now_ms=now)
    if snapshot["scope_id"] != request["scope_id"]:
        _fail("SCOPE_MISMATCH")
    return snapshot


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ControlUnavailable("REDIRECT_REJECTED")


class HTTPSControlClient:
    def __init__(self, url, key, scope_id, *, timeout=5):
        parsed = urllib.parse.urlsplit(url)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.path != "/step-control/v1" or parsed.query or parsed.fragment):
            _fail("INVALID_CONTROL_URL")
        self.url = url
        self.key = _key_bytes(key)
        self.scope_id = _identifier(scope_id)
        self.timeout = timeout

    def _call(self, method, payload):
        envelope = make_request(self.key, self.scope_id, method, payload)
        request = urllib.request.Request(self.url, data=canonical_bytes(envelope), method="POST",
                                         headers={"Content-Type": "application/json", "Accept": "application/json"})
        try:
            opener = urllib.request.build_opener(_NoRedirect())
            with opener.open(request, timeout=self.timeout) as response:
                if response.status != 200:
                    _fail("CONTROL_HTTP_ERROR")
                body = response.read(MAX_BODY_BYTES + 1)
            return verify_reply(self.key, envelope, loads_strict(body))
        except ControlUnavailable:
            raise
        except Exception:
            # No automatic retries: the operator must refresh after an uncertain reply.
            _fail("CONTROL_TRANSPORT_UNAVAILABLE")

    async def status(self):
        return await asyncio.to_thread(self._call, "status", {})

    async def execute(self, *, action, request_id, expected_scope_id, expected_runtime_id,
                      expected_version, profile=None, expected_position_id=None):
        payload = dict(action=action, request_id=request_id, expected_scope_id=expected_scope_id,
                       expected_runtime_id=expected_runtime_id, expected_version=expected_version,
                       profile=profile, expected_position_id=expected_position_id)
        return await asyncio.to_thread(self._call, "execute", payload)


def client_for(bot):
    """Explicit injection for tests, otherwise complete opt-in operator settings."""
    if bot.get("STEP_CONTROL_CLIENT") is not None:
        return bot["STEP_CONTROL_CLIENT"]
    url = os.environ.get("STEP_CONTROL_URL", "")
    key_hex = os.environ.get("STEP_OPERATOR_CONTROL_KEY", "")
    scope = os.environ.get("STEP_CONTROL_SCOPE", "")
    if not url and not key_hex and not scope:
        return None
    if not url or not scope or not re.fullmatch(r"[a-fA-F0-9]{64,128}", key_hex):
        _fail("INCOMPLETE_CONTROL_CONFIGURATION")
    return HTTPSControlClient(url, bytes.fromhex(key_hex), scope)
