import asyncio
import os
import types
import unittest
from unittest.mock import AsyncMock, patch

from early_autotrader_v2 import Blocked
from early_shared_execution import SharedPremiumIntegration


class FakePilot:
    def __init__(self):
        self.cfg = types.SimpleNamespace(min_score=90.0)
        self.active = {}
        self.mode = 'OFF'
        self.events = []

    def clock(self):
        return 1_700_000_000_000

    def record_selector(self, *args, **kwargs):
        return True

    def event(self, event, payload):
        self.events.append((event, payload))

    def selector_report(self, recent_limit=0):
        return {
            'total': 1,
            'qualified': 1,
            'rejected': 0,
            'qualified_stats': {'mature60': 1},
            'recent': [],
        }

    def set_mode(self, mode):
        self.mode = mode

    def configure(self, key, value):
        setattr(self.cfg, key, value)


class SharedEarlyExecutionTests(unittest.TestCase):
    def setUp(self):
        self.handler = AsyncMock(return_value=None)
        self.saved = []
        self.cfg = {
            'mode': 'DRY',
            'trade_margin_usdt': 25.0,
            'leverage': 5,
            'max_open_positions': 1,
            'daily_max_loss_pct': 2.0,
            'max_consecutive_stops': 2,
        }

        async def enable_live(session, chat, user, code):
            self.cfg['mode'] = 'LIVE'
            return '✅ Premium AutoTrade LIVE açıldı.'

        self.adapter = object.__new__(SharedPremiumIntegration)
        self.adapter.pilot = FakePilot()
        self.adapter.step_lock = None
        self.adapter._shadow_io = None
        self.adapter._session = object()
        self.adapter._dispatching = set()
        self.adapter._legacy_active = False
        self.adapter.source_enabled = True
        self.adapter.show = AsyncMock(return_value=None)
        self.adapter.b = {
            'states': {},
            'ignition_shadow_score': lambda m, score: (95.0, 'FAST_EARLY_V2', ['TEST']),
            'estimate_trade_plan': lambda symbol, m: {
                'entry_high': 1.01,
                'invalidation': 0.99,
                'target1': 1.02,
            },
            'autotrade_handle_premium': self.handler,
            'autotrade_cfg': self.cfg,
            '_at_admin_allowed': lambda chat, user, require_user_id=False: (
                chat == 'admin' and user == 'owner'
            ),
            '_at_save_setting': lambda key, value: self.saved.append((key, value)),
            '_at_try_live_enable': enable_live,
            'autotrade_live_confirm': {},
            'BINANCE_API_KEY': 'configured',
            'BINANCE_API_SECRET': 'configured',
            'AUTO_TRADE_LIVE_ALLOWED': True,
            '_at_risk_allowed': lambda mode: (True, 'OK'),
            'autotrade_active_by_symbol': {},
        }

    def _metrics(self):
        return {
            'price': 1.0,
            'chg10': 0.1,
            'chg30': 0.2,
            'chg60': 0.3,
            'flow10': 2.0,
            'flow30': 3.0,
            'flow60': 4.0,
            'buy30': 0.7,
            'rel30': 0.2,
            'flow_eff30': 1.0,
            'dist15high_pct': 0.1,
            'spread': 0.01,
            'qv24': 1_000_000.0,
            'oi5': 0.1,
            'oi_accel5': 0.0,
            'compression_ratio': 1.0,
            'extended': False,
        }

    def test_qualified_early_dispatches_to_existing_premium_handler(self):
        with patch('early_shared_execution.flag', return_value=False):
            asyncio.run(
                self.adapter.arm_async(101, 'TESTUSDT', self._metrics(), 80)
            )
        self.handler.assert_awaited_once()
        args = self.handler.await_args.args
        self.assertEqual(args[1:3], (101, 'TESTUSDT'))
        self.assertEqual(args[3]['entry_source'], 'EARLY_SHARED')
        self.assertEqual(args[3]['early_v2_score'], 95.0)
        self.assertEqual(args[4]['entry_source'], 'EARLY_SHARED')
        events = [name for name, _ in self.adapter.pilot.events]
        self.assertIn('EARLY_SHARED_DISPATCH', events)

    def test_source_off_records_shadow_but_never_dispatches(self):
        self.adapter.source_enabled = False
        with patch('early_shared_execution.flag', return_value=False):
            asyncio.run(
                self.adapter.arm_async(102, 'TESTUSDT', self._metrics(), 80)
            )
        self.handler.assert_not_awaited()
        reasons = [
            payload.get('reason')
            for name, payload in self.adapter.pilot.events
            if name == 'EARLY_SHARED_NOT_DISPATCHED'
        ]
        self.assertIn('SOURCE_OFF', reasons)

    def test_live_confirmation_reuses_premium_live_gate_and_enables_source(self):
        self.cfg['mode'] = 'DRY'
        self.adapter.source_enabled = False
        handled = asyncio.run(
            self.adapter.command(
                None, '/earlyv2 confirm 123456', 'admin', 'owner'
            )
        )
        self.assertTrue(handled)
        self.assertEqual(self.cfg['mode'], 'LIVE')
        self.assertTrue(self.adapter.source_enabled)
        self.adapter.show.assert_awaited_once()

    def test_dry_uses_shared_premium_mode_and_persists_it(self):
        self.cfg['mode'] = 'OFF'
        self.adapter.source_enabled = False
        asyncio.run(
            self.adapter.command(None, '/earlyv2 dry', 'admin', 'owner')
        )
        self.assertEqual(self.cfg['mode'], 'DRY')
        self.assertTrue(self.adapter.source_enabled)
        self.assertIn(('mode', 'DRY'), self.saved)

    def test_early_off_does_not_turn_off_premium_engine(self):
        self.cfg['mode'] = 'LIVE'
        self.adapter.source_enabled = True
        asyncio.run(
            self.adapter.command(None, '/earlyv2 off', 'admin', 'owner')
        )
        self.assertFalse(self.adapter.source_enabled)
        self.assertEqual(self.cfg['mode'], 'LIVE')

    def test_status_declares_single_shared_execution_owner(self):
        text = self.adapter.status()
        self.assertIn('PAYLAŞIMLI MOTOR', text)
        self.assertIn('Premium AutoTrade DRY', text)
        self.assertIn('Emir/risk/pozisyon/SL-TP/reconciliation: Premium AutoTrade ile aynı', text)
        self.assertNotIn('Bütçe Premium’dan ayrı', text)

    def test_legacy_active_position_blocks_shared_source(self):
        self.adapter._legacy_active = True
        with self.assertRaises(Blocked):
            self.adapter._set_source(True, reason='TEST')


if __name__ == '__main__':
    unittest.main()
