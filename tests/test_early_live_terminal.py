import asyncio
import importlib
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from early_autotrader_v2 import Config, Pilot
from early_v2_adapter import Integration
from execution_v2 import Blocked
import notification_policy
import telegram_ux


class EarlyLiveTerminalTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / 'early-live.db'
        self.state_path = Path(self.tempdir.name) / 'early-notification.json'
        self.old_state = os.environ.get('EARLY_NOTIFICATION_STATE_PATH')
        self.old_default = os.environ.get('EARLY_SIGNAL_TELEGRAM_ENABLED')
        os.environ['EARLY_NOTIFICATION_STATE_PATH'] = str(self.state_path)
        os.environ['EARLY_SIGNAL_TELEGRAM_ENABLED'] = '0'
        importlib.reload(notification_policy)

        def connect():
            return sqlite3.connect(self.db_path)

        self.pilot = Pilot(
            connect,
            live_allowed=True,
            score_validated=True,
        )
        self.adapter = object.__new__(Integration)
        self.adapter.pilot = self.pilot
        self.adapter.step_lock = None
        self.adapter.b = {
            '_at_admin_allowed': (
                lambda chat, user, require_user_id=False:
                (chat, user) == ('admin', 'owner')
            ),
            'BINANCE_API_KEY': 'configured',
            'BINANCE_API_SECRET': 'configured',
        }
        self.adapter.show = AsyncMock(return_value=None)

    def tearDown(self):
        if self.old_state is None:
            os.environ.pop('EARLY_NOTIFICATION_STATE_PATH', None)
        else:
            os.environ['EARLY_NOTIFICATION_STATE_PATH'] = self.old_state
        if self.old_default is None:
            os.environ.pop('EARLY_SIGNAL_TELEGRAM_ENABLED', None)
        else:
            os.environ['EARLY_SIGNAL_TELEGRAM_ENABLED'] = self.old_default
        self.tempdir.cleanup()

    def test_200_usdt_and_10x_are_valid_fail_closed_ceilings(self):
        Config(margin=200, leverage=10).validate()
        with self.assertRaises(Blocked):
            Config(margin=200.01, leverage=10).validate()
        with self.assertRaises(Blocked):
            Config(margin=200, leverage=11).validate()

    def test_one_panel_shows_combined_controls(self):
        self.pilot.configure('margin', 200)
        self.pilot.configure('leverage', 10)
        notification_policy.set_early_signal_notifications_enabled(True)
        text = self.adapter.status()
        self.assertIn('\u26a1 EARLY TERM\u0130NAL\u0130', text)
        self.assertIn('Bildirimler: \U0001f7e2 A\u00c7IK', text)
        self.assertIn('\u0130\u015flem modu: OFF', text)
        self.assertIn('Anapara (teminat): 200 USDT', text)
        self.assertIn('Kald\u0131ra\u00e7: 10x', text)
        self.assertIn(
            'Nominal pozisyon: yakla\u015f\u0131k 2000 USDT',
            text,
        )
        labels = [
            button['text']
            for row in self.adapter.markup()['inline_keyboard']
            for button in row
        ]
        for label in (
            'Bildirimler',
            'Mod',
            'Anapara',
            'Kald\u0131ra\u00e7',
        ):
            self.assertIn(label, labels)

    def test_panel_choices_include_200_usdt_and_10x(self):
        asyncio.run(
            self.adapter.command(
                None,
                '/earlyv2 margin',
                'admin',
                'owner',
            )
        )
        margin_markup = self.adapter.show.await_args.args[3]
        margin_values = [
            button['text']
            for row in margin_markup['inline_keyboard']
            for button in row
        ]
        self.assertIn('200', margin_values)

        self.adapter.show.reset_mock()
        asyncio.run(
            self.adapter.command(
                None,
                '/earlyv2 leverage',
                'admin',
                'owner',
            )
        )
        leverage_markup = self.adapter.show.await_args.args[3]
        leverage_values = [
            button['text']
            for row in leverage_markup['inline_keyboard']
            for button in row
        ]
        self.assertIn('10', leverage_values)

    def test_notification_toggle_is_inside_same_early_terminal(self):
        self.assertFalse(
            notification_policy.early_signal_notifications_enabled()
        )
        handled = asyncio.run(
            self.adapter.command(
                None,
                '/earlyv2 notify',
                'admin',
                'owner',
            )
        )
        self.assertTrue(handled)
        self.assertTrue(
            notification_policy.early_signal_notifications_enabled()
        )
        self.adapter.show.assert_awaited_once()

    def test_live_requires_second_confirmation_and_settings_freeze(self):
        self.pilot.configure('margin', 200)
        self.pilot.configure('leverage', 10)
        self.pilot.set_mode('DRY')
        with self.assertRaises(Blocked):
            self.pilot.set_mode('LIVE')
        token = self.pilot.challenge('admin:owner', 'LIVE')
        self.pilot.confirm(
            'admin:owner',
            token,
            ready=True,
            kind='LIVE',
        )
        self.assertEqual(self.pilot.mode, 'LIVE')
        with self.assertRaises(Blocked):
            self.pilot.configure('margin', 100)

    def test_single_menu_entry_alias_and_public_early_binding(self):
        labels = [label for label, _ in telegram_ux.MENU]
        self.assertEqual(labels.count('\u26a1 Early Terminali'), 1)
        self.assertNotIn('\U0001f514 Early Bildirimleri', labels)
        self.assertEqual(
            telegram_ux.normalize('/earlyterminal'),
            '/earlyv2',
        )
        source = Path('binance_momentum_bot/bot.py').read_text(
            encoding='utf-8'
        )
        self.assertIn(
            'await early_v2.arm_async('
            'st.active_radar_id, symbol, m, score)',
            source,
        )
        self.assertIn('notification_kind="EARLY"', source)


if __name__ == '__main__':
    unittest.main()
