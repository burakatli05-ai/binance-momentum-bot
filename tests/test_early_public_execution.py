import asyncio
import types
import unittest
from unittest.mock import AsyncMock, Mock

from early_public_execution import PublicH1PremiumIntegration


class FakePilot:
    def __init__(self, qualified=False):
        self.cfg = types.SimpleNamespace(min_score=90.0)
        self.active = {}
        self.mode = "OFF"
        self.events = []
        self.qualified = bool(qualified)

    def clock(self):
        return 1_700_000_000_000

    def record_selector(self, *args, **kwargs):
        return self.qualified

    def event(self, event, payload):
        self.events.append((event, payload))

    def selector_report(self, recent_limit=0):
        return {
            "total": 1,
            "qualified": int(self.qualified),
            "rejected": int(not self.qualified),
            "qualified_stats": {"mature60": 0},
            "recent": [],
        }


class PublicH1ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.handler = AsyncMock(return_value=None)
        self.cfg = {
            "mode": "LIVE",
            "trade_margin_usdt": 25.0,
            "leverage": 5,
            "max_open_positions": 2,
            "daily_max_loss_pct": 2.0,
            "max_consecutive_stops": 2,
        }
        self.adapter = object.__new__(PublicH1PremiumIntegration)
        self.adapter.pilot = FakePilot(qualified=False)
        self.adapter.step_lock = None
        self.adapter._shadow_io = None
        self.adapter._session = object()
        self.adapter._dispatching = set()
        self.adapter._legacy_active = False
        self.adapter.source_enabled = True
        self.adapter._queue_notify = Mock()
        self.adapter.b = {
            "states": {},
            "ignition_shadow_score": lambda metrics, score: (
                72.0,
                "EARLY_SHADOW_ONLY",
                ["BELOW_V2_THRESHOLD"],
            ),
            "estimate_trade_plan": lambda symbol, metrics: {
                "entry_high": 1.01,
                "invalidation": 0.99,
                "target1": 1.02,
            },
            "autotrade_handle_premium": self.handler,
            "autotrade_cfg": self.cfg,
            "AUTO_TRADE_LIVE_ALLOWED": True,
            "_at_risk_allowed": lambda mode: (True, "OK"),
            "autotrade_active_by_symbol": {},
        }

    @staticmethod
    def _metrics():
        return {
            "price": 1.0,
            "chg10": 0.1,
            "chg30": 0.2,
            "chg60": 0.3,
            "flow10": 2.0,
            "flow30": 3.0,
            "flow60": 4.0,
            "buy30": 0.7,
            "rel30": 0.2,
            "flow_eff30": 1.0,
            "dist15high_pct": 0.1,
            "spread": 0.01,
            "qv24": 1_000_000.0,
            "oi5": 0.1,
            "oi_accel5": 0.0,
            "compression_ratio": 1.0,
            "extended": False,
        }

    def _event_payload(self, name):
        for event, payload in self.adapter.pilot.events:
            if event == name:
                return payload
        self.fail(f"missing event {name}")

    def test_public_h1_dispatches_even_when_v2_selector_rejects(self):
        asyncio.run(
            self.adapter.arm_async(201, "STXUSDT", self._metrics(), 72)
        )

        self.handler.assert_awaited_once()
        args = self.handler.await_args.args
        self.assertEqual(args[1:3], (201, "STXUSDT"))
        self.assertEqual(args[3]["entry_source"], "EARLY_SHARED")
        self.assertFalse(args[3]["early_selector_qualified"])
        self.assertFalse(args[4]["early_selector_qualified"])

        selector = self._event_payload("SELECTOR_DECISION")
        self.assertFalse(selector["qualified"])
        self.assertEqual(selector["selector_role"], "SHADOW_ONLY")
        self.assertEqual(selector["execution_gate"], "PUBLIC_H1")

        dispatch = self._event_payload("EARLY_SHARED_DISPATCH")
        self.assertEqual(dispatch["admission"], "PUBLIC_H1")
        self.assertFalse(dispatch["selector_qualified"])
        self.adapter._queue_notify.assert_not_called()

    def test_qualified_selector_still_does_not_emit_v2_card(self):
        self.adapter.pilot.qualified = True
        asyncio.run(
            self.adapter.arm_async(202, "BULLAUSDT", self._metrics(), 80)
        )

        self.handler.assert_awaited_once()
        self.assertTrue(
            self._event_payload("EARLY_SHARED_DISPATCH")["selector_qualified"]
        )
        self.adapter._queue_notify.assert_not_called()

    def test_source_off_remains_fail_closed(self):
        self.adapter.source_enabled = False
        asyncio.run(
            self.adapter.arm_async(203, "STXUSDT", self._metrics(), 72)
        )

        self.handler.assert_not_awaited()
        blocked = self._event_payload("EARLY_SHARED_NOT_DISPATCHED")
        self.assertEqual(blocked["reason"], "SOURCE_OFF")

    def test_premium_off_remains_fail_closed(self):
        self.cfg["mode"] = "OFF"
        asyncio.run(
            self.adapter.arm_async(204, "STXUSDT", self._metrics(), 72)
        )

        self.handler.assert_not_awaited()
        blocked = self._event_payload("EARLY_SHARED_NOT_DISPATCHED")
        self.assertEqual(blocked["reason"], "PREMIUM_AUTOTRADE_OFF")

    def test_panel_declares_public_h1_admission_and_shadow_selector(self):
        text = self.adapter.status()
        self.assertIn("Tüm Public H1 Early", text)
        self.assertIn("V2 selector yalnız shadow", text)
        self.assertIn("V2 shadow eşik", text)

        labels = [
            button["text"]
            for row in self.adapter.markup()["inline_keyboard"]
            for button in row
        ]
        self.assertIn("V2 Shadow Score", labels)
        self.assertNotIn("Min V2 Score", labels)


if __name__ == "__main__":
    unittest.main()
