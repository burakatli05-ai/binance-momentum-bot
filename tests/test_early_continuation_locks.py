from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import test_v5135  # Existing isolated import/bootstrap; never starts bot.main().
import early_continuation as ec


class InjectedConnection:
    """Fail before a selected SQLite operation, retaining the real transaction."""
    def __init__(self, connection, operation, failures):
        self.connection = connection
        self.operation = operation
        self.remaining = failures
        self.injected = 0
        self.event_written = False

    def execute(self, sql, *args):
        if self.operation == 'insert' and sql.startswith('INSERT INTO early_continuation_flags '):
            self.fail()
        if self.operation == 'pending' and sql.startswith('INSERT INTO early_continuation_pending '):
            self.fail()
        if self.operation == 'outcome' and sql.startswith('INSERT OR IGNORE INTO early_continuation_outcomes '):
            self.fail()
        result = self.connection.execute(sql, *args)
        if sql.startswith('INSERT INTO early_continuation_flags '):
            self.event_written = True
        return result

    def fail(self):
        if self.remaining:
            self.remaining -= 1
            self.injected += 1
            error = sqlite3.OperationalError('database is busy')
            error.sqlite_errorcode = sqlite3.SQLITE_BUSY
            raise error

    def commit(self):
        if self.operation == 'commit' and self.event_written:
            self.fail()
        return self.connection.commit()

    def executescript(self, script):
        if self.operation == 'schema': self.fail()
        return self.connection.executescript(script)

    def close(self):
        self.connection.close()


class LockRecoveryTests(unittest.TestCase):
    def test_first_writes_and_commits_recover_without_replaying_events(self):
        connect = sqlite3.connect
        for operation in ('insert', 'pending', 'outcome', 'commit', 'schema'):
            for failures in (1, 2):
                with self.subTest(operation=operation, failures=failures), tempfile.TemporaryDirectory() as directory:
                    path = Path(directory)/'fixture.db'
                    connect(path).close()
                    injected = []

                    def factory(*args, **kwargs):
                        proxy = InjectedConnection(connect(*args, **kwargs), operation, failures)
                        injected.append(proxy)
                        return proxy

                    worker = ec.Collector(str(path), {}, 'test', 'code')
                    now = int(time.time()*1000)
                    worker.event('X', 'candidate_start', {}, price=100, decision_ts=now)
                    worker.event('X', 'candidate_start', {}, price=100, decision_ts=now+1)
                    worker.submit('tick', ('X', 105, now+300001, now+300001))
                    worker.stop.set()  # Drain all messages, then exit normally.
                    with patch.object(ec.sqlite3, 'connect', side_effect=factory), patch.object(ec.time, 'sleep') as sleep:
                        worker.run()
                    self.assertEqual(failures, injected[0].injected)
                    self.assertEqual(list(ec.LOCK_RETRY_DELAYS[:failures]), [c.args[0] for c in sleep.call_args_list])
                    with closing(connect(path)) as c:
                        events = c.execute('SELECT operands_json FROM early_continuation_flags ORDER BY decision_ts').fetchall()
                        self.assertEqual([0, 1], [json.loads(r[0])['prior_candidate_count_observed'] for r in events])
                        self.assertEqual(2, c.execute('SELECT COUNT(*) FROM early_continuation_pending').fetchone()[0])
                        outcomes = c.execute('SELECT horizon_s,return_pct FROM early_continuation_outcomes').fetchall()
                        self.assertEqual(2, len(outcomes))
                        for horizon, ret in outcomes:
                            self.assertEqual(300, horizon)
                            self.assertAlmostEqual(5, ret)
                        gap = c.execute("SELECT duration_ms,detail_json FROM research_runtime_gaps WHERE kind='SQLITE_LOCK_RETRY'").fetchall()
                        self.assertEqual(1, len(gap))
                        self.assertGreaterEqual(gap[0][0], 0)
                        self.assertEqual(failures, json.loads(gap[0][1])['retries'])

    def test_real_sqlite_lock_releases_and_producer_remains_nonblocking(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'fixture.db'
            with closing(sqlite3.connect(path)) as c:
                c.execute('PRAGMA journal_mode=WAL')
                ec.migrate(c)
            blocker = sqlite3.connect(path)
            worker = ec.Collector(str(path), {}, 'test', 'code', capacity=4)
            blocked = threading.Event()
            original_execute = ec.WorkerConnection.execute

            def observe(connection, sql, *args):
                if sql.startswith('INSERT INTO early_continuation_flags '): blocked.set()
                return original_execute(connection, sql, *args)

            worker.thread.start()
            try:
                # Wait for completed initialization using a read-only test query.
                deadline = time.monotonic()+3
                while time.monotonic() < deadline:
                    with closing(sqlite3.connect(path)) as c:
                        if c.execute('SELECT COUNT(*) FROM early_continuation_runs').fetchone()[0]: break
                    time.sleep(.01)
                else: self.fail('worker did not initialize')
                blocker.execute('BEGIN IMMEDIATE')
                with patch.object(ec.WorkerConnection, 'execute', observe):
                    now = int(time.time()*1000)
                    worker.event('X', 'candidate_start', {}, price=100, decision_ts=now)
                    self.assertTrue(blocked.wait(2))
                    # Separate producer can finish while the worker is stuck in SQLite.
                    produced = threading.Event()

                    def produce():
                        for _ in range(20): worker.submit('tick', ('X', 101, now+1, now+1))
                        produced.set()

                    producer = threading.Thread(target=produce)
                    producer.start()
                    self.assertTrue(produced.wait(.5))
                    producer.join(1)
                    self.assertGreater(worker.lost, 0)
                    time.sleep(.25)  # Exceeds the worker's 100ms SQLite timeout.
                    blocker.rollback()
                    deadline = time.monotonic()+3
                    while time.monotonic() < deadline:
                        with closing(sqlite3.connect(path)) as c:
                            if c.execute("SELECT COUNT(*) FROM research_runtime_gaps WHERE kind='SQLITE_LOCK_RETRY'").fetchone()[0]: break
                        time.sleep(.01)
                    else: self.fail('retry diagnostic not persisted')
                    self.assertTrue(worker.thread.is_alive())
                    self.assertTrue(worker.enabled)
            finally:
                blocker.rollback(); blocker.close()
                worker.stop.set(); worker.thread.join(3)
            self.assertFalse(worker.thread.is_alive())
            with closing(sqlite3.connect(path)) as c:
                self.assertEqual(1, c.execute('SELECT COUNT(*) FROM early_continuation_flags').fetchone()[0])

    def test_retry_budget_is_finite_and_unexpected_errors_are_not_retried(self):
        for message, code, retryable in [
            ('database is locked', sqlite3.SQLITE_BUSY, True),
            ('database table is locked', sqlite3.SQLITE_LOCKED, True),
            ('database is locked', sqlite3.SQLITE_BUSY | (2 << 8), True),
            ('no such table: broken', sqlite3.SQLITE_ERROR, False),
            ('database disk image is malformed', sqlite3.SQLITE_CORRUPT, False),
        ]:
            with self.subTest(message=message, code=code):
                error = sqlite3.OperationalError(message)
                error.sqlite_errorcode = code

                class Broken:
                    calls = 0
                    def execute(self, *args):
                        self.calls += 1
                        raise error

                raw = Broken()
                with patch.object(ec.time, 'sleep') as sleep, self.assertRaises(sqlite3.OperationalError):
                    ec.WorkerConnection(raw).execute('SELECT 1')
                expected = len(ec.LOCK_RETRY_DELAYS) if retryable else 0
                self.assertEqual(expected+1, raw.calls)
                self.assertEqual(expected, sleep.call_count)

    def test_no_error_code_fallback_is_narrow(self):
        self.assertTrue(ec.transient_lock(sqlite3.OperationalError('database is locked')))
        self.assertTrue(ec.transient_lock(sqlite3.OperationalError('database is busy')))
        self.assertFalse(ec.transient_lock(sqlite3.OperationalError('unable to open database file')))
        self.assertFalse(ec.transient_lock(sqlite3.DatabaseError('database is locked')))


if __name__ == '__main__':
    unittest.main()
