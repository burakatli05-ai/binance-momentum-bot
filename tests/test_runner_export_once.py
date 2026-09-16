import hashlib
import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import unittest
from unittest.mock import Mock, patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'binance_momentum_bot'))
import runner_export_once as once
import runner_download_once as download
import test_runner_snapshot_export as fixtures


class StartupTests(unittest.TestCase):
    def test_disabled_exact_passthrough(self):
        for flag in ('', '0', '1', 'true', 'other'):
            env = {'RUNNER_EXPORT_ONCE': flag, 'BINANCE_API_SECRET': 'private'}
            with patch.dict(os.environ, env, clear=True), patch.object(once.subprocess, 'Popen') as spawn, patch.object(once.os, 'execv') as execute:
                once.startup()
                spawn.assert_not_called()
                execute.assert_called_once_with(sys.executable, [sys.executable, 'startup.py'])
                self.assertEqual(dict(os.environ), env)

    def test_spawn_failure_still_execs_and_clean_environment(self):
        env = {'RUNNER_EXPORT_ONCE': once.SNAPSHOT_ID, 'RUNNER_EXPORT_SOURCE_SHA256': 'a' * 64,
               'BINANCE_API_SECRET': 'private', 'TELEGRAM_BOT_TOKEN': 'private', 'PYTHONPATH': 'untrusted'}
        for failure in (None, OSError('disk full'), MemoryError(), subprocess.TimeoutExpired('helper', 1)):
            with patch.dict(os.environ, env, clear=True), patch.object(once.subprocess, 'Popen', side_effect=failure) as spawn, patch.object(once.os, 'execv') as execute:
                once.startup()
                execute.assert_called_once()
                self.assertEqual(spawn.call_args.kwargs['env'], once.CLEAN_ENV)
                self.assertFalse(spawn.call_args.kwargs['shell'])
                self.assertTrue(spawn.call_args.kwargs['start_new_session'])

    def test_missing_hash_does_not_spawn(self):
        with patch.dict(os.environ, {'RUNNER_EXPORT_ONCE': once.SNAPSHOT_ID}, clear=True), patch.object(once.subprocess, 'Popen') as spawn, patch.object(once.os, 'execv'):
            once.startup()
            spawn.assert_not_called()

    def test_rollback_restores_original_config(self):
        root = Path(once.__file__).parent
        original = tomllib.loads((root / 'railway.toml').read_text())
        temporary = tomllib.loads((root / 'railway.runner-export.toml').read_text())
        self.assertEqual(original['deploy']['startCommand'], 'python startup.py')
        self.assertEqual(temporary['deploy']['startCommand'], 'python runner_export_once.py')
        temporary['deploy']['startCommand'] = original['deploy']['startCommand']
        self.assertEqual(temporary, original)


class HelperTests(unittest.TestCase):
    def test_failures_never_block_parent_exec(self):
        for failure in (FileNotFoundError(), MemoryError(), OSError('disk full')):
            with patch.object(once, 'limits'), patch.object(once, 'claim'), patch.object(once.time, 'sleep'), patch.object(once, 'preflight', side_effect=failure), patch.object(once.subprocess, 'Popen') as spawn:
                self.assertEqual(once.supervise('a' * 64), 1)
                spawn.assert_not_called()
            with patch.object(once.os, 'execv') as execute, patch.dict(os.environ, {}, clear=True):
                once.startup()
                execute.assert_called_once()

    @unittest.skipUnless(sys.platform == 'linux', 'Linux process groups')
    def test_timeout_kills_process_group(self):
        child = Mock(pid=1234)
        child.wait.side_effect = [subprocess.TimeoutExpired('worker', 1), -9]
        with patch.object(once, 'limits'), patch.object(once, 'claim'), patch.object(once.time, 'sleep'), patch.object(once, 'preflight'), patch.object(once.subprocess, 'Popen', return_value=child) as spawn, patch.object(once.os, 'killpg', create=True) as kill:
            self.assertEqual(once.supervise('a' * 64), 1)
            kill.assert_called_once_with(1234, once.signal.SIGKILL)
            self.assertEqual(spawn.call_args.kwargs['env'], once.CLEAN_ENV)

    def test_nonzero_or_oom_exit_not_success(self):
        for code in (1, -9, 137):
            child = Mock()
            child.wait.return_value = code
            with patch.object(once, 'limits'), patch.object(once, 'claim'), patch.object(once.time, 'sleep'), patch.object(once, 'preflight'), patch.object(once.subprocess, 'Popen', return_value=child), patch.object(once, 'event') as event:
                self.assertEqual(once.supervise('a' * 64), 1)
                event.assert_called_once_with('CHILD_FAILED')

    @unittest.skipUnless(sys.platform == 'linux', 'Linux filesystem durability')
    def test_exclusive_marker_blocks_restart_even_after_failure(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(once, 'STATE', Path(directory) / 'state'), patch.object(once, 'limits'), patch.object(once.time, 'sleep'), patch.object(once, 'preflight', side_effect=MemoryError()) as check:
            self.assertEqual(once.supervise('a' * 64), 1)
            marker = next(once.STATE.iterdir())
            before = marker.read_bytes()
            self.assertEqual(once.supervise('a' * 64), 1)
            check.assert_called_once()
            self.assertEqual(marker.read_bytes(), before)
            self.assertEqual(marker.stat().st_mode & 0o777, 0o600)

    def test_preflight_disk_and_memory(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'snapshot.db'
            source.touch()
            with patch.object(once, 'SOURCE', source), patch.object(once.shutil, 'disk_usage', return_value=Mock(free=0)), self.assertRaises(OSError):
                once.preflight()
            with patch.object(once, 'SOURCE', source), patch.object(once.shutil, 'disk_usage', return_value=Mock(free=10**12)), patch.object(once, 'memory_available', return_value=0), self.assertRaises(MemoryError):
                once.preflight()

    @unittest.skipUnless(sys.platform == 'linux', 'Real Linux resource limits')
    def test_real_memory_limit(self):
        code = 'import runner_export_once as r; r.limits(80*1024**2, 5); bytearray(160*1024**2)'
        result = subprocess.run([sys.executable, '-c', code], cwd=Path(once.__file__).parent, capture_output=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'MemoryError', result.stderr)

    @unittest.skipUnless(sys.platform == 'linux', 'Real Linux CPU limits')
    def test_real_cpu_limit(self):
        code = 'import runner_export_once as r; r.limits(80*1024**2, 1)\nwhile True: pass'
        result = subprocess.run([sys.executable, '-c', code], cwd=Path(once.__file__).parent, capture_output=True, timeout=8)
        self.assertLess(result.returncode, 0)

    @unittest.skipUnless(sys.platform == 'linux', 'Real Linux watchdog')
    def test_real_watchdog_reaps_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory) / 'sleep.py'
            script.write_text('import time; time.sleep(30)')
            with patch.object(once, 'SCRIPT', script), patch.object(once, 'STATE', Path(directory) / 'state'), patch.object(once, 'limits'), patch.object(once, 'preflight'), patch.object(once, 'DELAY_SECONDS', 0), patch.object(once, 'TIMEOUT_SECONDS', 0.2), patch.object(once, 'event') as event:
                started = time.monotonic()
                self.assertEqual(once.supervise('a' * 64), 1)
                self.assertLess(time.monotonic() - started, 5)
                event.assert_called_once_with('TIMEOUT')


class ExportAndDownloadTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.RunnerSnapshotExportCase()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.state = self.fixture.base / 'state'
        self.state.mkdir()
        for name, value in {'ROOT': self.fixture.root, 'SOURCE': self.fixture.source,
                            'OUTPUT': self.fixture.output, 'STATE': self.state}.items():
            p = patch.object(once, name, value)
            p.start()
            self.addCleanup(p.stop)

    def test_worker_hash_source_and_zip_verified_no_live_open(self):
        original = fixtures.sqlite3.connect
        def connect(path, **kw):
            self.assertEqual(path, self.fixture.source.as_uri() + '?mode=ro&immutable=1')
            return original(path, **kw)
        before = self.fixture.source.read_bytes()
        with patch.object(once, 'limits'), patch.object(fixtures.sqlite3, 'connect', side_effect=connect), patch.object(once, 'event') as event:
            self.assertEqual(once.worker(self.fixture.manifest['sha256']), 0)
            self.assertEqual(event.call_args.args, ('SUCCESS',))
        self.assertEqual(before, self.fixture.source.read_bytes())
        receipt = json.loads(next(self.state.iterdir()).read_text())
        self.assertEqual(receipt['zip_sha256'], hashlib.sha256(once.OUTPUT.read_bytes()).hexdigest())
        self.assertEqual(receipt['row_counts'], {k: 1 for k in fixtures.exports.FIELDS})
        with zipfile.ZipFile(once.OUTPUT, 'a') as archive:
            archive.writestr('unapproved.txt', 'bad')
        with self.assertRaises(ValueError):
            once.verify_zip(self.fixture.manifest['sha256'])

    def test_wrong_pin_and_missing_snapshot_fail_without_sqlite(self):
        with patch.object(once, 'limits'), patch.object(fixtures.sqlite3, 'connect', side_effect=AssertionError('no open')):
            self.assertEqual(once.worker('0' * 64), 1)
            self.fixture.source.unlink()
            self.assertEqual(once.worker(self.fixture.manifest['sha256']), 1)
        self.assertFalse(once.OUTPUT.exists())
        self.assertEqual(list(self.state.iterdir()), [])

    def test_download_auth_fixed_path_hash_expiry_and_no_logs(self):
        with patch.object(once, 'limits'):
            self.assertEqual(once.worker(self.fixture.manifest['sha256']), 0)
        server = download.HTTPServer(('127.0.0.1', 0), download.Handler)
        server.token_hash = hashlib.sha256(b'test-token').hexdigest()
        server.expires = time.time() + 60
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        def get(path, token='test-token'):
            connection = http.client.HTTPConnection(*server.server_address, timeout=3)
            try:
                connection.request('GET', path, headers={'Authorization': 'Bearer ' + token})
                response = connection.getresponse()
                return response.status, response.read()
            finally:
                connection.close()
        try:
            self.assertEqual(get('/health', 'wrong')[0], 404)
            self.assertEqual(get('/health'), (200, b'{"ready":true}'))
            self.assertEqual(get('/../signals.db')[0], 404)
            self.assertEqual(get('/snapshot')[0], 200)
            self.assertEqual(get('/runner.zip'), (200, once.OUTPUT.read_bytes()))
            once.OUTPUT.write_bytes(b'corrupt')
            self.assertEqual(get('/runner.zip')[0], 503)
            server.expires = time.time() - 1
            self.assertEqual(get('/health')[0], 404)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == '__main__':
    unittest.main()
