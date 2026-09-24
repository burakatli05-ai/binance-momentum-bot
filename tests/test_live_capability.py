"""LIVE opt-in safety: no real network, orders, or production database."""
import ast
from early_v2_compat import StripEarlyHooks
import asyncio
from contextlib import ExitStack
import io
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'binance_momentum_bot'))
os.environ['PYTHON_DOTENV_DISABLED'] = '1'
import bot
import startup


class CapabilityTests(unittest.TestCase):
    def test_execution_risk_scanner_and_signal_functions_match_deployed_base(self):
        tree = ast.parse((ROOT / 'binance_momentum_bot/bot.py').read_text(encoding='utf-8'))
        tree = StripEarlyHooks().visit(tree)
        changed = {'load_autotrade_settings', 'handle_autotrade_callback', '_at_try_live_enable', '_at_command'}
        # Strip additive shadow hooks; this digest includes the intentional Telegram transport hotfix.
        class StripQualityHooks(ast.NodeTransformer):
            def visit_Expr(self, node):
                if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name) and node.value.func.id == 'quality_arm':
                    return None
                return self.generic_visit(node)
            def visit_If(self, node):
                if isinstance(node.test, ast.Name) and node.test.id == 'quality_recorder':
                    return None
                return self.generic_visit(node)
            def visit_Tuple(self, node):
                node.elts = [x for x in node.elts if not (isinstance(x, ast.Call) and isinstance(x.func, ast.Name) and x.func.id == 'quality_shadow_loop')]
                return self.generic_visit(node)
        changed |= {'quality_features', 'quality_arm', 'quality_shadow_loop'}
        nodes = [StripQualityHooks().visit(n) for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name not in changed]
        digest = hashlib.sha256(ast.dump(ast.Module(body=nodes, type_ignores=[]), include_attributes=False).encode()).hexdigest()
        self.assertEqual('200e9f721c8b5279ad172a4db1d01081c51366032f938318ea42ddf695d83e34', digest)

    def test_bot_environment_is_fail_closed_and_always_boots_off(self):
        tree = ast.parse((ROOT / 'binance_momentum_bot/bot.py').read_text(encoding='utf-8'))
        names = {'AUTO_TRADE_LIVE_ALLOWED', 'AUTO_TRADE_BOOT_MODE'}
        nodes = [n for n in tree.body if isinstance(n, ast.Assign) and
                 any(isinstance(t, ast.Name) and t.id in names for t in n.targets)]
        code = compile(ast.Module(body=nodes, type_ignores=[]), '<config>', 'exec')
        for value in (None, '', '0', 'false', 'no', 'off', 'invalid', '2', '1', 'true', ' YES ', 'On'):
            for boot in ('OFF', 'DRY', 'LIVE', 'invalid'):
                with self.subTest(value=value, boot=boot):
                    env = {'AUTO_TRADE_BOOT_MODE': boot}
                    if value is not None: env['AUTO_TRADE_LIVE_ALLOWED'] = value
                    with patch.dict(os.environ, env, clear=True):
                        scope = {'os': os}
                        exec(code, scope)
                    self.assertEqual(scope['AUTO_TRADE_LIVE_ALLOWED'], value in ('1', 'true', ' YES ', 'On'))
                    self.assertEqual(scope['AUTO_TRADE_BOOT_MODE'], 'OFF')

    def test_startup_passes_normalized_capability_and_logs_effective_values(self):
        for value, expected in ((None, '0'), ('0', '0'), ('bad', '0'), ('', '0'), ('1', '1'), (' True ', '1')):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                root = Path(directory).resolve()
                env = {'RAILWAY_VOLUME_MOUNT_PATH': str(root), 'DB_PATH': str(root/'signals.db'),
                       'AUTO_TRADE_BOOT_MODE': 'LIVE', 'RESEARCH_EXPORT_ENABLED': '0'}
                if value is not None: env['AUTO_TRADE_LIVE_ALLOWED'] = value
                stack.enter_context(patch.dict(os.environ, env, clear=True))
                stack.enter_context(patch.object(Path, 'is_mount', return_value=True))
                stack.enter_context(patch.object(startup, 'prepare_db', return_value={}))
                stack.enter_context(patch.object(startup.os, 'chdir'))
                output = stack.enter_context(patch('sys.stdout', new_callable=io.StringIO))
                def check_exec(*args):
                    self.assertEqual(os.environ['AUTO_TRADE_LIVE_ALLOWED'], expected)
                    self.assertEqual(os.environ['AUTO_TRADE_BOOT_MODE'], 'OFF')
                execute = stack.enter_context(patch.object(startup.os, 'execv', side_effect=check_exec))
                startup.main()
                execute.assert_called_once()
                record = json.loads(output.getvalue().splitlines()[0])
                self.assertEqual(record['live_allowed'], int(expected))
                self.assertEqual(record['boot_mode'], 'OFF')


class LiveSafetyTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for name, value in [('AUTO_TRADE_LIVE_ALLOWED', True), ('TELEGRAM_ADMIN_CHAT_ID', 'admin'),
                            ('TELEGRAM_ADMIN_USER_ID', 'user'), ('BINANCE_API_KEY', 'test'), ('BINANCE_API_SECRET', 'test')]:
            self.stack.enter_context(patch.object(bot, name, value))
        self.stack.enter_context(patch.dict(bot.autotrade_cfg, {'mode': 'OFF'}))
        self.stack.enter_context(patch.dict(bot.autotrade_live_confirm, {}, clear=True))
        self.stack.enter_context(patch.dict(bot.autotrade_active_by_symbol, {}, clear=True))
        for name in ('telegram_send', 'telegram_api_call', 'binance_signed_request', '_at_place_market_entry'):
            setattr(self, name, self.stack.enter_context(patch.object(bot, name, new_callable=AsyncMock)))
        self.snapshot = self.stack.enter_context(patch.object(bot, '_at_account_snapshot', new_callable=AsyncMock,
            return_value=({'canTrade': True}, {'balance': '1000'}, [])))
        self.risk = self.stack.enter_context(patch.object(bot, '_at_risk_allowed', return_value=(True, '')))
        self.save = self.stack.enter_context(patch.object(bot, '_at_save_setting'))
        self.stack.enter_context(patch.object(bot, '_at_daily_row', return_value={}))
        self.stack.enter_context(patch.object(bot, '_at_cache_account_balance'))
        self.stack.enter_context(patch.object(bot.telemetry_p0, 'decision'))

    def request(self, user='user', chat='admin'):
        cb = {'id': 'test', 'data': 'at:mode:LIVE', 'from': {'id': user}, 'message': {'chat': {'id': chat}}}
        asyncio.run(bot.handle_autotrade_callback(None, cb))

    def confirm(self, code, user='user', chat='admin'):
        return asyncio.run(bot._at_try_live_enable(None, chat, user, code))

    def test_panel_selection_requires_second_confirmation_and_never_sends_orders(self):
        self.request()
        self.assertEqual(bot.autotrade_cfg['mode'], 'OFF')
        self.save.assert_not_called()
        self.snapshot.assert_not_awaited()
        code, expiry = bot.autotrade_live_confirm['user']
        self.assertGreater(expiry, time.time())
        asyncio.run(bot._at_command(None, '/autotrade confirm '+code, 'admin', 'user'))
        self.assertEqual(bot.autotrade_cfg['mode'], 'LIVE')
        self.save.assert_called_once_with('mode', 'LIVE')
        self.confirm(code)
        self.assertEqual(self.save.call_count, 1)
        self.binance_signed_request.assert_not_awaited()
        self._at_place_market_entry.assert_not_awaited()

    def test_command_selection_also_requires_confirmation(self):
        asyncio.run(bot._at_command(None, '/autotrade live', 'admin', 'user'))
        self.assertEqual(bot.autotrade_cfg['mode'], 'OFF')
        self.assertIn('user', bot.autotrade_live_confirm)
        self.save.assert_not_called()

    def test_unauthorized_chat_user_and_missing_admin_cannot_request_or_confirm(self):
        for chat, user in [('other', 'user'), ('admin', 'other')]:
            self.request(user, chat)
            self.assertFalse(bot.autotrade_live_confirm)
            bot.autotrade_live_confirm['user'] = ('123', time.time()+120)
            self.confirm('123', user, chat)
            bot.autotrade_live_confirm.clear()
        with patch.object(bot, 'TELEGRAM_ADMIN_USER_ID', ''):
            self.request()
            self.assertFalse(bot.autotrade_live_confirm)
            self.confirm('123')
        self.save.assert_not_called()
        self.snapshot.assert_not_awaited()

    def test_missing_wrong_expired_and_replayed_code_fail_closed(self):
        self.confirm('123')
        for code, expiry, supplied in [('123', time.time()-1, '123'), ('123', time.time()+120, 'wrong')]:
            bot.autotrade_live_confirm['user'] = (code, expiry)
            self.confirm(supplied)
            self.confirm(code)
        self.assertEqual(bot.autotrade_cfg['mode'], 'OFF')
        self.save.assert_not_called()
        self.snapshot.assert_not_awaited()

    def test_capability_credentials_risk_and_cantrade_guards(self):
        for name, value in [('AUTO_TRADE_LIVE_ALLOWED', False), ('BINANCE_API_KEY', ''), ('BINANCE_API_SECRET', '')]:
            with patch.object(bot, name, value):
                bot.autotrade_live_confirm['user'] = ('123', time.time()+120)
                self.confirm('123')
                self.assertEqual(bot.autotrade_cfg['mode'], 'OFF')
        for snapshot, risk in [(({'canTrade': False}, {}, []), (True, '')), (({'canTrade': True}, {}, []), (False, 'DAILY_LIMIT'))]:
            self.snapshot.return_value = snapshot
            self.risk.return_value = risk
            bot.autotrade_live_confirm['user'] = ('123', time.time()+120)
            self.confirm('123')
            self.assertEqual(bot.autotrade_cfg['mode'], 'OFF')
        self.save.assert_not_called()
        self.binance_signed_request.assert_not_awaited()

    def test_off_mode_with_capability_never_reaches_account_or_order_api(self):
        asyncio.run(bot.autotrade_handle_premium(None, 1, 'BTCUSDT', {'price': 100}, {}))
        self.snapshot.assert_not_awaited()
        self.binance_signed_request.assert_not_awaited()
        self._at_place_market_entry.assert_not_awaited()

    def test_persisted_live_and_dry_restart_off_without_changing_risk_settings(self):
        with tempfile.TemporaryDirectory() as directory:
            db = str(Path(directory)/'test.db')
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE autotrade_settings(key TEXT PRIMARY KEY,value TEXT,updated_ts INTEGER)')
                conn.execute("INSERT INTO autotrade_settings VALUES('mode','LIVE',0)")
            conn.close()
            with patch.object(bot, 'db_connect', side_effect=lambda: sqlite3.connect(db)), \
                 patch.object(bot, 'recover_autotrade_active'), patch.object(bot, '_at_repair_daily_from_trade_history'):
                for mode in ('LIVE', 'DRY'):
                    with sqlite3.connect(db) as conn:
                        conn.execute("UPDATE autotrade_settings SET value=? WHERE key='mode'", (mode,))
                    conn.close()
                    bot.autotrade_live_confirm['user'] = ('old', time.time()+120)
                    before = {k:v for k,v in bot.autotrade_cfg.items() if k != 'mode'}
                    bot.load_autotrade_settings()
                    self.assertEqual(bot.autotrade_cfg['mode'], 'OFF')
                    self.assertFalse(bot.autotrade_live_confirm)
                    self.assertEqual(before, {k:v for k,v in bot.autotrade_cfg.items() if k != 'mode'})
                    self.save.assert_called_with('mode', 'OFF')


if __name__ == '__main__': unittest.main()
