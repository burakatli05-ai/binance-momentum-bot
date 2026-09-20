"""Railpack entrypoint regressions; never launch the bot or use a production DB."""
from contextlib import ExitStack
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'binance_momentum_bot'))
import startup


class StopStartup(Exception):
    """Stop an intentional maintenance wait or intercepted exec."""


class RailpackStartupTests(unittest.TestCase):
    def test_railpack_uses_protective_entrypoint_in_application_root(self):
        app = ROOT / 'binance_momentum_bot'
        config = json.loads((app / 'railpack.json').read_text(encoding='utf-8'))
        self.assertEqual(config['$schema'], 'https://schema.railpack.com')
        self.assertEqual(config['deploy']['startCommand'], 'python startup.py')
        self.assertTrue((app / 'startup.py').is_file())

    def test_maintenance_never_opens_database_or_launches_bot(self):
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {
                'DB_MAINTENANCE': '1', 'DB_PATH': '/data/signals.db',
            }, clear=True))
            sleep = stack.enter_context(patch.object(startup.time, 'sleep', side_effect=StopStartup))
            prepare = stack.enter_context(patch.object(startup, 'prepare_db'))
            connect = stack.enter_context(patch.object(startup.sqlite3, 'connect'))
            opened = stack.enter_context(patch.object(Path, 'open'))
            execute = stack.enter_context(patch.object(startup.os, 'execv'))
            with self.assertRaises(StopStartup):
                startup.main()
            sleep.assert_called_once_with(30)
            prepare.assert_not_called()
            connect.assert_not_called()
            opened.assert_not_called()
            execute.assert_not_called()

    def test_unmounted_data_fails_without_creating_directory(self):
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {}, clear=True))
            stack.enter_context(patch.object(Path, 'is_mount', return_value=False))
            mkdir = stack.enter_context(patch.object(Path, 'mkdir'))
            prepare = stack.enter_context(patch.object(startup, 'prepare_db'))
            connect = stack.enter_context(patch.object(startup.sqlite3, 'connect'))
            execute = stack.enter_context(patch.object(startup.os, 'execv'))
            with self.assertRaisesRegex(RuntimeError, 'Persistent volume is not mounted'):
                startup.main()
            mkdir.assert_not_called()
            prepare.assert_not_called()
            connect.assert_not_called()
            execute.assert_not_called()

    def test_database_outside_mount_fails_before_database_access(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            root = Path(directory).resolve()
            stack.enter_context(patch.dict(os.environ, {
                'RAILWAY_VOLUME_MOUNT_PATH': str(root),
                'DB_PATH': str(root / 'outside' / 'signals.db'),
            }, clear=True))
            stack.enter_context(patch.object(Path, 'is_mount', return_value=True))
            prepare = stack.enter_context(patch.object(startup, 'prepare_db'))
            execute = stack.enter_context(patch.object(startup.os, 'execv'))
            with self.assertRaisesRegex(RuntimeError, 'DB_PATH must be directly'):
                startup.main()
            prepare.assert_not_called()
            execute.assert_not_called()
            self.assertEqual(list(root.iterdir()), [])

    def test_missing_database_without_restore_source_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            root = Path(directory).resolve()
            stack.enter_context(patch.dict(os.environ, {
                'RAILWAY_VOLUME_MOUNT_PATH': str(root),
                'DB_PATH': str(root / 'signals.db'),
            }, clear=True))
            stack.enter_context(patch.object(Path, 'is_mount', return_value=True))
            connect = stack.enter_context(patch.object(startup.sqlite3, 'connect'))
            execute = stack.enter_context(patch.object(startup.os, 'execv'))
            with self.assertRaisesRegex(RuntimeError, 'Verified restore source unavailable'):
                startup.main()
            connect.assert_not_called()
            execute.assert_not_called()
            self.assertEqual(list(root.iterdir()), [])

    def test_verified_start_passes_capability_and_keeps_boot_off(self):
        for overrides in ({}, {'AUTO_TRADE_LIVE_ALLOWED': '1', 'AUTO_TRADE_BOOT_MODE': 'LIVE'}):
            with self.subTest(overrides=overrides), tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                root = Path(directory).resolve()
                stack.enter_context(patch.dict(os.environ, {
                    'RAILWAY_VOLUME_MOUNT_PATH': str(root),
                    'DB_PATH': str(root / 'signals.db'), **overrides,
                }, clear=True))
                stack.enter_context(patch.object(Path, 'is_mount', return_value=True))
                stack.enter_context(patch.object(startup, 'prepare_db', return_value={}))
                stack.enter_context(patch.object(startup.os, 'chdir'))

                def intercept_exec(*args):
                    self.assertEqual(os.environ['AUTO_TRADE_LIVE_ALLOWED'], overrides.get('AUTO_TRADE_LIVE_ALLOWED', '0'))
                    self.assertEqual(os.environ['AUTO_TRADE_BOOT_MODE'], 'OFF')
                    self.assertEqual(os.environ['PYTHON_DOTENV_DISABLED'], '1')
                    raise StopStartup

                execute = stack.enter_context(patch.object(startup.os, 'execv', side_effect=intercept_exec))
                with self.assertRaises(StopStartup):
                    startup.main()
                execute.assert_called_once_with(sys.executable, [sys.executable, '-u', 'bot.py'])


if __name__ == '__main__':
    unittest.main()
