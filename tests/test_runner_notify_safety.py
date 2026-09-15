"""Notification safety regressions; all transport and bot execution are mocked."""
from contextlib import ExitStack
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'binance_momentum_bot'))
import startup


class RunnerNotifySafetyTests(unittest.TestCase):
    def load_runner(self, value=None):
        env = {'TELEGRAM_BOT_TOKEN': 'test-token', 'TELEGRAM_CHAT_ID': 'test-chat'}
        if value is not None:
            env['RUNNER_SCORE_V1_NOTIFY'] = value
        spec = importlib.util.spec_from_file_location('runner_notify_test', ROOT / 'binance_momentum_bot/runner_shadow_v1.py')
        module = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, env, clear=True), patch.dict(sys.modules, {spec.name: module}):
            spec.loader.exec_module(module)
            notifier = module.TelegramShadowNotifier()
        return module, notifier

    def test_unset_off_empty_and_invalid_values_never_queue_or_start_transport(self):
        for value in (None, '0', 'false', 'no', 'off', '', ' ', 'typo', '2'):
            with self.subTest(value=value):
                runner, notifier = self.load_runner(value)
                with patch.object(runner.threading, 'Thread') as thread, patch.object(runner.urllib.request, 'urlopen') as http:
                    self.assertFalse(notifier.enabled)
                    for title in ('FAST RUNNER PREMIUM — SHADOW', 'REACQUIRE PREMIUM — SHADOW'):
                        self.assertFalse(notifier.send(title))
                    self.assertTrue(notifier._queue.empty())
                    self.assertFalse(notifier._started)
                    thread.assert_not_called()
                    http.assert_not_called()

    def test_explicit_opt_in_queues_with_mocked_worker(self):
        for value in ('1', 'true', 'yes', 'on', ' TRUE ', 'On'):
            with self.subTest(value=value):
                runner, notifier = self.load_runner(value)
                with patch.object(runner.threading, 'Thread') as thread:
                    self.assertTrue(notifier.enabled)
                    self.assertTrue(notifier.send('test shadow'))
                    self.assertEqual(notifier._queue.get_nowait(), 'test shadow')
                    thread.return_value.start.assert_called_once()

    def test_default_off_preserves_fast_and_reacquire_decisions_and_telemetry(self):
        runner, notifier = self.load_runner()
        with sqlite3.connect(':memory:') as conn, patch.object(runner.threading, 'Thread') as thread, patch.object(runner.urllib.request, 'urlopen') as http:
            self.addCleanup(conn.close)
            runner.migrate(conn)
            engine = runner.RunnerShadowV1(lambda: conn, notifier=notifier)
            features = dict(price=100, score=80, momentum_score=80, chg30=.80, chg60=1.57,
                            chg5=1.04, flow30=7.2, buy30=.694, rel30=.78, gainer_rank=3,
                            book_imbalance=.62, phase='LOW', oi5=.12, breakout=False)
            t = int(time.time() * 1000)
            engine.on_stage('fast', 'FASTUSDT', 'EARLY', event_ts_ms=t, price=100, features=features)
            engine.on_tick('FASTUSDT', 100.5, t + 15001, t + 15001)
            engine.on_stage('shake', 'CAPUSDT', 'EARLY', event_ts_ms=t, price=100, features=features)
            engine.on_tick('CAPUSDT', 99.4, t + 15001, t + 15001)
            reacquire = dict(features, price=99.8, score=88, momentum_score=88, rel30=.82, chg30=.92, gainer_rank=4)
            engine.on_stage('reacquire', 'CAPUSDT', 'CANDIDATE', event_ts_ms=t + 60000, price=99.8, features=reacquire)
            engine.on_tick('CAPUSDT', 100.3, t + 75001, t + 75001)
            rows = conn.execute("SELECT kind,notify_state FROM runner_watch_v1_shadow WHERE decision='ALLOW' ORDER BY kind").fetchall()
            self.assertEqual([tuple(row) for row in rows], [('FAST', 'NOT_CONFIGURED'), ('REACQUIRE', 'NOT_CONFIGURED')])
            self.assertEqual(conn.execute('SELECT count(*) FROM runner_score_v1_shadow').fetchone()[0], 3)
            self.assertTrue(notifier._queue.empty())
            thread.assert_not_called()
            http.assert_not_called()

    def test_startup_normalizes_notify_before_exec_and_logs_effective_value(self):
        for value, expected in ((None, '0'), ('0', '0'), ('', '0'), ('invalid', '0'), (' OFF ', '0'), ('1', '1'), (' True ', '1')):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                root = Path(directory).resolve()
                env = {'RAILWAY_VOLUME_MOUNT_PATH': str(root), 'DB_PATH': str(root / 'signals.db')}
                if value is not None:
                    env['RUNNER_SCORE_V1_NOTIFY'] = value
                stack.enter_context(patch.dict(os.environ, env, clear=True))
                stack.enter_context(patch.object(Path, 'is_mount', return_value=True))
                stack.enter_context(patch.object(startup, 'prepare_db', return_value={}))
                stack.enter_context(patch.object(startup.os, 'chdir'))
                output = stack.enter_context(patch('sys.stdout', new_callable=io.StringIO))
                def intercept_exec(*args):
                    self.assertEqual(os.environ['RUNNER_SCORE_V1_NOTIFY'], expected)
                    self.assertEqual(os.environ['AUTO_TRADE_LIVE_ALLOWED'], '0')
                    self.assertEqual(os.environ['AUTO_TRADE_BOOT_MODE'], 'OFF')
                    self.assertEqual(os.environ['PYTHON_DOTENV_DISABLED'], '1')
                execute = stack.enter_context(patch.object(startup.os, 'execv', side_effect=intercept_exec))
                startup.main()
                execute.assert_called_once_with(sys.executable, [sys.executable, '-u', 'bot.py'])
                self.assertEqual(json.loads(output.getvalue())['runner_score_v1_notify'], int(expected))


if __name__ == '__main__':
    unittest.main()
