"""Offline integration of exact production call sites with no Binance/Telegram IO."""
import ast
import asyncio
import contextvars
import hashlib
import hmac
import json
import logging
import sys
import time
import types
import unittest
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "binance_momentum_bot"))
import binance_rest_guard as guard

SOURCE = ROOT / "binance_momentum_bot" / "bot.py"
TREE = ast.parse(SOURCE.read_text(encoding="utf-8"))
FUNCTIONS = {node.name: node for node in TREE.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


class FakeResponse:
    def __init__(self, status, body, headers=None):
        self.status = status
        self.body = body
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def text(self):
        return json.dumps(self.body)

    async def json(self):
        return self.body

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(str(self.status))


class FakeSession:
    def __init__(self, *responses):
        self.responses = iter(responses)
        self.calls = []

    def _call(self, method, url, **kwargs):
        self.calls.append(method)
        return next(self.responses)

    def get(self, url, **kwargs):
        return self._call("GET", url, **kwargs)

    def request(self, method, url, **kwargs):
        return self._call(method, url, **kwargs)

    def delete(self, url, **kwargs):
        return self._call("DELETE", url, **kwargs)


def exact_functions(names, ns):
    for name in names:
        node = FUNCTIONS[name]
        # Remove telemetry decorator only, so we exercise the handler itself
        # without creating a real database or recording production telemetry.
        node.decorator_list = []
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE), "exec"), ns)


class Integration(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.ns = dict(
            asyncio=asyncio, binance_rest_guard=guard, time=time,
            json=json, hmac=hmac, hashlib=hashlib, urlencode=urlencode,
            Optional=Optional, aiohttp=types.SimpleNamespace(ClientSession=object, ClientTimeout=lambda **kwargs: None),
            log=logging.getLogger("offline-binance-rest"),
            REST="https://example.invalid", BINANCE_API_KEY="fake", BINANCE_API_SECRET="fake",
            BINANCE_RATE_LIMIT_SOFT_WEIGHT=1800, BINANCE_RATE_LIMIT_SOFT_COOLDOWN_SECONDS=10,
            BINANCE_RATE_LIMIT_429_FALLBACK_SECONDS=60,
            binance_rate_state=dict(used_weight_1m=0, observed_mono=0.0, blocked_until_mono=0.0),
            _binance_new_entry=contextvars.ContextVar("test_new_entry", default=False),
            now_ms=lambda: 1795693039000,
            AUTO_TRADE_LIVE_ALLOWED=True,
            autotrade_cfg=dict(mode="LIVE", exit_profile="CURRENT_TP2", max_entry_slippage_pct=0.2,
                trade_margin_usdt=25, leverage=5, margin_type="ISOLATED"),
            premium_lock=types.SimpleNamespace(profile=lambda x:x, PROFILE="STEP_LOCK", UNKNOWN="UNKNOWN"),
            autotrade_active_by_symbol={}, states={"TESTUSDT":types.SimpleNamespace(ask_price=1.0)},
            pct_change=lambda a,b:(a/b-1)*100,
            _at_log_event=Mock(),
            _at_qro_reentry_safe=AsyncMock(return_value=True),
            _at_account_snapshot=AsyncMock(),
            _at_place_market_entry=AsyncMock(),
        )
        exact_functions(["_binance_observe_rate_headers", "_binance_rate_gate", "fetch_json",
                         "binance_signed_request", "_at_query_order_by_client", "_at_place_market_entry",
                         "autotrade_handle_premium", "_at_try_live_enable"], self.ns)
        self.scope = self.ns["_binance_new_entry"]
        self.state = self.ns["binance_rate_state"]

    def ban(self):
        guard.observe(self.state, {"Retry-After":"120"}, 418)

    async def test_public_418_blocks_signed_new_entry_without_network(self):
        s=FakeSession(FakeResponse(418, {"code":-1003,"msg":"banned"}, {"Retry-After":"120"}))
        with self.assertRaises(RuntimeError):
            await self.ns["fetch_json"](s, "/public")
        self.assertGreater(self.state["blocked_until_mono"], time.monotonic()+115)
        token=self.scope.set(True)
        try:
            with self.assertRaises(guard.RateLimitBlocked):
                await self.ns["binance_signed_request"](s,"POST","/order",{})
        finally:
            self.scope.reset(token)
        self.assertEqual(s.calls,["GET"])

    async def test_signed_418_blocks_public_same_guard(self):
        s=FakeSession(FakeResponse(418, {"code":-1003,"msg":"banned"}, {"Retry-After":"120"}))
        with self.assertRaisesRegex(RuntimeError,"418"):
            await self.ns["binance_signed_request"](s,"GET","/balance")
        token=self.scope.set(True)
        try:
            with self.assertRaises(guard.RateLimitBlocked):
                await self.ns["fetch_json"](s,"/public")
        finally:
            self.scope.reset(token)
        self.assertEqual(s.calls,["GET"])

    async def test_new_entry_during_ban_does_not_reach_account_or_order(self):
        self.ban()
        await self.ns["autotrade_handle_premium"](FakeSession(),1,"TESTUSDT",{"price":1.0},{})
        self.ns["_at_qro_reentry_safe"].assert_not_awaited()
        self.ns["_at_account_snapshot"].assert_not_awaited()
        self.assertEqual(self.ns["_at_log_event"].call_args.args[0],"ENTRY_BLOCKED")
        self.assertFalse(self.scope.get())

    async def test_local_market_order_rejection_never_queries_order(self):
        original=self.ns["binance_signed_request"]
        self.ns["binance_signed_request"]=AsyncMock(side_effect=guard.RateLimitBlocked("blocked"))
        lookup=AsyncMock()
        self.ns["_at_query_order_by_client"]=lookup
        try:
            with self.assertRaises(guard.RateLimitBlocked):
                await self.ns["_at_place_market_entry"](FakeSession(),"TESTUSDT",1,"BOTH","id")
            lookup.assert_not_awaited()
        finally:
            self.ns["binance_signed_request"]=original

    async def test_uncertain_dispatch_lookup_uses_management_lane(self):
        self.ns["binance_signed_request"]=AsyncMock(side_effect=TimeoutError())
        async def lookup(*args):
            self.assertFalse(self.scope.get())
            return {"status":"FILLED","orderId":9}
        self.ns["_at_query_order_by_client"]=lookup
        token=self.scope.set(True)
        try:
            result=await self.ns["_at_place_market_entry"](FakeSession(),"TESTUSDT",1,"BOTH","id")
            self.assertEqual(result["orderId"],9)
            self.assertTrue(self.scope.get())
        finally:
            self.scope.reset(token)

    async def test_actual_activation_refuses_ban_without_changing_mode(self):
        self.ban()
        self.ns.update(_at_admin_allowed=lambda *args,**kwargs:True,
                       autotrade_live_confirm={"owner":("123456",time.time()+120)},
                       _at_risk_allowed=lambda *args:(True,"OK"),
                       _at_save_setting=Mock())
        self.ns["autotrade_cfg"]["mode"]="OFF"
        async def snapshot(session):
            await self.ns["_binance_rate_gate"](priority=True)
            self.fail("account request should not pass gate")
        self.ns["_at_account_snapshot"]=snapshot
        result=await self.ns["_at_try_live_enable"](FakeSession(),"admin","owner","123456")
        self.assertIn("BINANCE_REST_COOLDOWN",result)
        self.assertEqual(self.ns["autotrade_cfg"]["mode"],"OFF")
        self.ns["_at_save_setting"].assert_not_called()
        self.assertFalse(self.scope.get())

    async def test_429_weight_updates_and_deadline(self):
        s=FakeSession(FakeResponse(429, {"code":-1003}, {"Retry-After":"25","X-MBX-USED-WEIGHT-1M":"2400"}))
        with self.assertRaisesRegex(RuntimeError,"429"):
            await self.ns["binance_signed_request"](s,"GET","/balance")
        self.assertEqual(self.state["used_weight_1m"],2400)
        self.assertGreater(self.state["blocked_until_mono"],time.monotonic()+23)
