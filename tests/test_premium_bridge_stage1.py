"""New Stage 1 tests only: synthetic fixtures, no bot import or live DB access."""
import ast
import base64
import copy
from contextlib import closing, redirect_stdout
import hashlib
from io import StringIO
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
import zlib
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'binance_momentum_bot'))
import assistant_bridge as bridge
import research_export as exporter
from export_safety import ExportSkipped, GIB

REAL_CONNECT = sqlite3.connect
NOW = 1792922400000  # Synthetic fixed clock; no production inference.
MANIFEST = {'snapshot_id': 'stage1-test-cut', 'created_time_ms': NOW,
            'deployment_id': 'SYNTHETIC', 'code_identity': 'SYNTHETIC'}


def source_schemas():
    """Read exact CREATE TABLE definitions without executing/importing bot.py."""
    tree = ast.parse((ROOT / 'binance_momentum_bot' / 'bot.py').read_text(encoding='utf-8'))
    result = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        sql = node.value.strip()
        for table in bridge.INCLUDED_TABLES:
            if sql.startswith('CREATE TABLE IF NOT EXISTS ' + table + ' ('):
                result[table] = sql
    if set(result) != set(bridge.INCLUDED_TABLES):
        raise AssertionError('Pinned source schema fixture incomplete')
    return result


class Stage1Case(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='mb-stage1-test-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.path = self.base / 'synthetic.db'
        self.lines = []

    def printer(self, *args, **kwargs):
        self.lines.append(args[0])

    def make_db(self, count=3, omit=None, outcomes=2, wal=False):
        with closing(REAL_CONNECT(self.path)) as conn, conn:
            if wal:
                conn.execute('PRAGMA journal_mode=WAL')
            for table, schema in source_schemas().items():
                if table != omit:
                    conn.execute(schema)
            for sid in range(1, count + 1):
                conn.execute('INSERT INTO signals_v2(id,ts,symbol,level,score,price,flow10,buy10) '
                             'VALUES(?,?,?,?,?,?,?,?)',
                             (sid, NOW // 1000 - 3600, 'SYNTH' + str(sid), 'CONFIRMED',
                              91, 1.234567890123, 2.75, None if sid == 1 else .61))
                for table in bridge.INCLUDED_TABLES:
                    if table in ('signals_v2', omit):
                        continue
                    if table == 'signal_outcomes':
                        for horizon in range(outcomes):
                            conn.execute('INSERT INTO signal_outcomes(signal_id,horizon_s,return_pct,ts) '
                                         'VALUES(?,?,?,?)', (sid, horizon, -0.25, NOW // 1000))
                    else:
                        info = conn.execute('PRAGMA table_info("' + table + '")').fetchall()
                        values = {'signal_id': sid}
                        for _, name, kind, notnull, default, pk in info:
                            if name == 'signal_id':
                                continue
                            if pk or (notnull and default is None):
                                values[name] = 'SYNTHETIC' if kind == 'TEXT' else 1
                        if table == 'signal_meta':
                            values.update(premium=1, entry_quality=85, rise_score=87,
                                          candidate_runup=0.1234567890123, qv24=12345.67)
                        if table == 'premium_context':
                            values.update(signal_generated_ts_ms=NOW - 3600000,
                                          seconds_since_episode_peak=None if sid == 1 else 17.125)
                        names = list(values)
                        conn.execute('INSERT INTO "' + table + '" (' + ','.join(names) + ') '
                                     'VALUES(' + ','.join('?' for _ in names) + ')',
                                     [values[n] for n in names])
            conn.execute('INSERT INTO signals_v2(id,ts,symbol,level,score,price) '
                         'VALUES(?,?,?,?,?,?)', (count + 10, NOW // 1000, 'EARLY_ONLY', 'EARLY', 11, 0.25))
            conn.execute('CREATE TABLE forbidden_account_secrets(token TEXT)')
            conn.execute("INSERT INTO forbidden_account_secrets VALUES ('DO_NOT_EXPORT_SECRET')")
        return self.path

    def payload(self):
        return bridge.build_live_payload(self.path, dict(MANIFEST))

    @staticmethod
    def rows(payload, table):
        item = payload['tables'][table]
        return [dict(zip(item['columns'], row)) for row in item['rows']]

    def worker(self):
        return exporter.Exporter(self.path, self.base / 'exports', deployment='SYNTHETIC')

    def test_lossless_premium_allowlist_and_log_roundtrip(self):
        self.make_db()
        before = self.path.read_bytes()
        meta = bridge.emit_live(self.path, dict(MANIFEST), printer=self.printer)
        chunks = [line.split(' ', 4)[4] for line in self.lines if ' CHUNK ' in line]
        payload = bridge.decode_chunks(chunks, meta['sha256'])
        self.assertEqual(set(bridge.INCLUDED_TABLES), set(payload['tables']))
        self.assertEqual([1, 2, 3], [row['id'] for row in self.rows(payload, 'signals_v2')])
        self.assertNotIn('DO_NOT_EXPORT_SECRET', json.dumps(payload))
        self.assertNotIn('EARLY_ONLY', json.dumps(payload))
        self.assertEqual(1.234567890123, self.rows(payload, 'signals_v2')[0]['price'])
        self.assertEqual(0.1234567890123, self.rows(payload, 'signal_meta')[0]['candidate_runup'])
        self.assertIsNone(self.rows(payload, 'premium_context')[0]['seconds_since_episode_peak'])
        self.assertIsNone(payload.get('source_sha256'))
        self.assertTrue(self.lines[0].startswith(bridge.LOG_PREFIX + ' META '))
        self.assertTrue(self.lines[-1].startswith(bridge.LOG_PREFIX + ' END '))
        self.assertEqual(before, self.path.read_bytes())

    def test_missing_required_table_even_empty_fails_without_success_output(self):
        self.make_db(count=0, omit='premium_context')
        with self.assertRaisesRegex(ValueError, 'premium_context'):
            bridge.emit_live(self.path, dict(MANIFEST), printer=self.printer)
        self.assertEqual([], self.lines)

    def test_readonly_query_only_transaction_and_source_unchanged(self):
        self.make_db()
        before = self.path.read_bytes()
        opened, traces, denied, query_only = [], [], [], []
        class AuditedConnection(sqlite3.Connection):
            def execute(conn, sql, parameters=()):
                result = super(AuditedConnection, conn).execute(sql, parameters)
                if sql.strip().upper() == 'BEGIN':
                    query_only.append(conn.execute('PRAGMA query_only').fetchone()[0])
                    try:
                        super(AuditedConnection, conn).execute('UPDATE signals_v2 SET score=0')
                    except sqlite3.OperationalError:
                        denied.append(True)
                    else:
                        denied.append(False)
                return result
        def connect(*args, **kwargs):
            opened.append((args, kwargs.copy()))
            kwargs['factory'] = AuditedConnection
            conn = REAL_CONNECT(*args, **kwargs)
            conn.set_trace_callback(traces.append)
            return conn
        with patch.object(bridge.sqlite3, 'connect', side_effect=connect):
            self.payload()
        self.assertEqual(1, len(opened))
        self.assertIn('mode=ro', str(opened[0][0][0]))
        self.assertTrue(opened[0][1]['uri'])
        self.assertEqual([1], query_only)
        self.assertEqual([True], denied)
        self.assertEqual(1, sum(sql.strip().upper() == 'BEGIN' for sql in traces))
        self.assertEqual(before, self.path.read_bytes())

    def test_concurrent_wal_commit_cannot_mix_root_and_child_cut(self):
        self.make_db(count=3, wal=True)
        updates = []
        class MutatingConnection(sqlite3.Connection):
            def execute(conn, sql, parameters=()):
                result = super(MutatingConnection, conn).execute(sql, parameters)
                if 'SELECT' in sql.upper() and 'signals_v2' in sql and 'LIMIT' in sql.upper() and not updates:
                    with closing(REAL_CONNECT(self.path)) as writer, writer:
                        writer.execute('UPDATE signals_v2 SET score=2 WHERE id=1')
                        writer.execute('UPDATE premium_context SET seconds_since_episode_peak=999 WHERE signal_id=1')
                        writer.execute('INSERT INTO signals_v2(id,ts,symbol,level,score,price) VALUES(1000,1,?,?,1,1)',
                                       ('NEW_AFTER_CUT', 'CONFIRMED'))
                        writer.execute('INSERT INTO premium_context(signal_id,seconds_since_episode_peak,updated_ts) VALUES(1000,777,1)')
                    updates.append(True)
                return result
        def connect(*args, **kwargs):
            kwargs['factory'] = MutatingConnection
            return REAL_CONNECT(*args, **kwargs)
        with patch.object(bridge.sqlite3, 'connect', side_effect=connect):
            payload = self.payload()
        self.assertEqual([True], updates, 'Fixture must commit while export transaction is open')
        self.assertEqual([1, 2, 3], [row['id'] for row in self.rows(payload, 'signals_v2')])
        self.assertEqual(91, self.rows(payload, 'signals_v2')[0]['score'])
        self.assertIsNone(self.rows(payload, 'premium_context')[0]['seconds_since_episode_peak'])
        with closing(REAL_CONNECT(self.path)) as check:
            self.assertEqual(999, check.execute('SELECT seconds_since_episode_peak FROM premium_context WHERE signal_id=1').fetchone()[0])
            self.assertEqual(1, check.execute('SELECT COUNT(*) FROM signals_v2 WHERE id=1000').fetchone()[0])

    def test_sql_reads_are_true_500_row_keyset_pages_for_parent_and_children(self):
        self.make_db(count=1001, outcomes=2)
        reads = []
        class PagedConnection(sqlite3.Connection):
            def execute(conn, sql, parameters=()):
                if 'SELECT' in sql.upper() and 'LIMIT' in sql.upper():
                    reads.append((sql, parameters))
                return super(PagedConnection, conn).execute(sql, parameters)
        def connect(*args, **kwargs):
            kwargs['factory'] = PagedConnection
            return REAL_CONNECT(*args, **kwargs)
        with patch.object(bridge.sqlite3, 'connect', side_effect=connect):
            payload = self.payload()
        self.assertEqual(1001, len(self.rows(payload, 'signals_v2')))
        self.assertEqual(2002, len(self.rows(payload, 'signal_outcomes')))
        self.assertGreaterEqual(len([q for q, p in reads if 'FROM "signals_v2"' in q]), 3)
        self.assertGreaterEqual(len([q for q, p in reads if 'FROM "signal_outcomes"' in q]), 5)
        self.assertTrue(reads)
        for sql, parameters in reads:
            self.assertNotIn('OFFSET', sql.upper())
            self.assertTrue('LIMIT 500' in sql or
                            ('LIMIT 1' in sql and 'IS NULL' in sql) or
                            (sql.rstrip().endswith('LIMIT ?') and parameters[-1] == 500), sql)

    def test_guard_checkpoint_fails_without_success_output(self):
        self.make_db()
        def stop():
            raise ExportSkipped('TIME_LIMIT')
        with self.assertRaisesRegex(ExportSkipped, 'TIME_LIMIT'):
            bridge.emit_live(self.path, dict(MANIFEST), printer=self.printer, checkpoint=stop)
        self.assertEqual([], self.lines)

    def test_scheduler_bridge_success_does_not_relabel_failed_full_snapshot(self):
        self.make_db()
        worker = self.worker()
        pinned = {'snapshot_id': 'unchanged-old-full-snapshot'}
        (worker.root / 'latest.json').write_text(json.dumps(pinned), encoding='utf-8')
        (worker.root / 'latest-summary.json').write_text(json.dumps(pinned), encoding='utf-8')
        full_calls = []
        def snapshot(now):
            full_calls.append(now)
            return worker.root / 'absent', {'valid': False, 'snapshot_id': 'failed-full',
                                          'skipped_export_reason': 'MIN_FREE'}
        with patch.object(worker, 'snapshot', side_effect=snapshot), \
             patch('export_safety.shutil.disk_usage', return_value=SimpleNamespace(free=90*GIB, used=0)), \
             redirect_stdout(StringIO()):
            result = worker.run_due(NOW)
        self.assertEqual([NOW], full_calls)
        self.assertEqual('SKIPPED', result['status'])
        self.assertEqual('MIN_FREE', result['skipped_export_reason'])
        self.assertEqual('VALID', result['assistant_bridge']['status'])
        self.assertEqual(pinned, json.loads((worker.root / 'latest.json').read_text()))
        self.assertEqual(pinned, json.loads((worker.root / 'latest-summary.json').read_text()))

    def test_small_export_disk_and_budget_guards_remain_enforced(self):
        self.make_db()
        worker = self.worker()
        with patch('export_safety.shutil.disk_usage', return_value=SimpleNamespace(free=8*GIB, used=0)):
            result = worker.premium_bridge(NOW, printer=self.printer)
        self.assertEqual('SKIPPED', result['status'])
        self.assertEqual('MIN_FREE', result['skipped_export_reason'])
        with patch('export_safety.shutil.disk_usage', return_value=SimpleNamespace(free=90*GIB, used=0)), \
             patch.object(worker.safety, 'usage', return_value=20*GIB):
            result = worker.premium_bridge(NOW, printer=self.printer)
        self.assertEqual('SKIPPED', result['status'])
        self.assertEqual('EXPORT_BUDGET', result['skipped_export_reason'])
        self.assertFalse(any(' META ' in line or ' END ' in line for line in self.lines))

    def test_row_and_byte_caps_fail_closed_instead_of_truncating(self):
        self.make_db()
        for constant, value, reason in [('MAX_ROWS', 2, 'PREMIUM_ROW_LIMIT'),
                                         ('MAX_RAW_BYTES', 4096, 'PREMIUM_BYTE_LIMIT')]:
            with self.subTest(constant=constant), patch.object(bridge, constant, value):
                with self.assertRaisesRegex(Exception, reason):
                    bridge.emit_live(self.path, dict(MANIFEST), printer=self.printer)
                self.assertEqual([], self.lines)

    def test_corrupt_reordered_or_missing_chunks_fail_integrity(self):
        self.make_db(count=200)
        # A small transport chunk is a fixture for exercising chunk ordering;
        # export page size and resource limits remain their production values.
        with patch.object(bridge, 'CHUNK_CHARS', 100):
            meta = bridge.emit_live(self.path, dict(MANIFEST), printer=self.printer)
        chunks = [line.split(' ', 4)[4] for line in self.lines if ' CHUNK ' in line]
        self.assertGreater(len(chunks), 2)
        trailing_stream = base64.b64encode(base64.b64decode(''.join(chunks)) + zlib.compress(b'extra')).decode('ascii')
        for bad in [chunks[:-1], [chunks[1], chunks[0], *chunks[2:]],
                    chunks + [chunks[-1]], [trailing_stream],
                    [chunks[0][:-1] + ('A' if chunks[0][-1] != 'A' else 'B'), *chunks[1:]]]:
            with self.subTest(kind='broken_chunk_sequence'):
                with self.assertRaises(Exception):
                    bridge.decode_chunks(bad, meta['sha256'])
        with self.assertRaisesRegex(ValueError, 'sha256'):
            bridge.decode_chunks(chunks, '0' * 64)

    def test_worker_failure_preserves_prior_success_and_records_attempt(self):
        self.make_db()
        worker = self.worker()
        prior_success = {'created_time_ms': NOW - 3600000, 'sha256': '1' * 64}
        exporter.atomic_json(worker.root / 'scheduler.json', {'premium_bridge_last_success': prior_success})
        with patch.object(worker, 'snapshot', return_value=(worker.root / 'absent',
             {'valid': False, 'snapshot_id': 'failed-full', 'skipped_export_reason': 'MIN_FREE'})), \
             patch.object(worker, 'premium_bridge', return_value={'status': 'SKIPPED',
                        'skipped_export_reason': 'PREMIUM_ROW_LIMIT'}):
            result = worker.run_due(NOW)
        state = worker.state()
        self.assertEqual('SKIPPED', result['assistant_bridge']['status'])
        self.assertEqual(NOW, state['premium_bridge_last_attempt_ms'])
        self.assertEqual(result['assistant_bridge'], state['premium_bridge_last_result'])
        self.assertEqual(prior_success, state['premium_bridge_last_success'])

    def test_existing_hourly_claim_prevents_new_bridge_scheduler(self):
        self.make_db()
        worker = self.worker()
        exporter.atomic_json(worker.root / 'scheduler.json', {'last_hourly_attempt': NOW})
        with patch.object(worker, 'snapshot') as snapshot, patch.object(worker, 'premium_bridge') as small:
            result = worker.run_due(NOW + 1000)
        self.assertEqual('NOT_DUE', result['status'])
        snapshot.assert_not_called()
        small.assert_not_called()

    def test_missing_row_missing_column_null_and_orphan_are_distinct(self):
        self.make_db()
        with closing(REAL_CONNECT(self.path)) as conn, conn:
            conn.execute('DELETE FROM premium_context WHERE signal_id=2')
            conn.execute('ALTER TABLE signal_meta DROP COLUMN qv24')
            # Physical absent parent inside pinned max-id range, separately from
            # an EARLY parent that is intentionally outside the Premium subset.
            conn.execute('INSERT INTO signal_outcomes(signal_id,horizon_s,ts) VALUES(0,60,1)')
            conn.execute('INSERT INTO signal_outcomes(signal_id,horizon_s,ts) VALUES(13,60,1)')
        payload = self.payload()
        context = payload['reconciliation']['premium_context']
        self.assertEqual([2], context['missing_parent_ids'])
        self.assertEqual([1], context['required_null_ids']['seconds_since_episode_peak'])
        self.assertEqual(['qv24'], payload['reconciliation']['signal_meta']['missing_required_columns'])
        outcomes = payload['reconciliation']['signal_outcomes']
        self.assertEqual([[0, 60]], outcomes['orphan_keys'])
        self.assertEqual(1, outcomes['orphan_count'])
        self.assertNotIn(0, [r['signal_id'] for r in self.rows(payload, 'signal_outcomes')])
        self.assertNotIn(13, [r['signal_id'] for r in self.rows(payload, 'signal_outcomes')])
        self.assertIn('UNKNOWN', payload['logical_export']['data_completeness'])
        self.assertTrue(bridge.validate_live_payload(payload))

    def test_page_field_count_schema_and_relation_tampering_are_rejected(self):
        self.make_db()
        good = self.payload()
        tamperers = {
            'page_hash': lambda p: p['reconciliation']['signals_v2']['pages'][0].update(sha256='0' * 64),
            'page_rows': lambda p: p['reconciliation']['signals_v2']['pages'][0].update(rows=99),
            'source_count': lambda p: p['reconciliation']['signals_v2'].update(source_row_count=99),
            'null_count': lambda p: p['reconciliation']['premium_context']['null_counts'].update(seconds_since_episode_peak=0),
            'schema_hash': lambda p: p['logical_export'].update(schema_sha256='0' * 64),
            'root_cut': lambda p: p['logical_export'].update(max_signal_id=2),
            'missing_row': lambda p: p['tables']['signals_v2']['rows'].pop(),
            'duplicate_row': lambda p: p['tables']['signal_outcomes']['rows'].append(p['tables']['signal_outcomes']['rows'][0]),
            'child_foreign_key': lambda p: p['tables']['signal_outcomes']['rows'][0].__setitem__(0, 999),
            'orphan_count': lambda p: p['reconciliation']['signal_meta'].update(orphan_count=999),
        }
        for label, tamper in tamperers.items():
            with self.subTest(tamper=label):
                bad = copy.deepcopy(good)
                tamper(bad)
                with self.assertRaises((ValueError, KeyError)):
                    bridge.validate_live_payload(bad)

    def test_empty_premium_cut_is_explicit_zero_not_missing_schema_success(self):
        self.make_db(count=0)
        payload = self.payload()
        self.assertEqual(0, payload['logical_export']['premium_count'])
        self.assertEqual(0, payload['logical_export']['max_signal_id'])
        self.assertEqual(0, payload['logical_export']['total_rows'])
        self.assertTrue(payload['logical_export']['logical_not_restorable'])
        self.assertIn('UNKNOWN', payload['logical_export']['data_completeness'])
        for table in bridge.INCLUDED_TABLES:
            self.assertEqual([], self.rows(payload, table))
            self.assertEqual([], payload['reconciliation'][table]['pages'])

    def test_real_min_free_guard_rejects_full_snapshot_but_accepts_small_cut(self):
        self.make_db()
        worker = self.worker()
        before = self.path.read_bytes()
        with patch('export_safety.shutil.disk_usage',
                   return_value=SimpleNamespace(free=worker.safety.policy.min_free + 128*1024**2, used=0)), \
             redirect_stdout(StringIO()):
            result = worker.run_due(NOW)
        self.assertEqual('SKIPPED', result['status'])
        self.assertEqual('MIN_FREE', result['skipped_export_reason'])
        self.assertEqual('VALID', result['assistant_bridge']['status'])
        self.assertEqual(3, result['assistant_bridge']['premium_count'])
        self.assertFalse((worker.root / 'latest.json').exists())
        self.assertEqual([], list(worker.root.glob('*/signals.db')))
        self.assertEqual(before, self.path.read_bytes())

    def test_real_budget_guard_rejects_full_snapshot_but_accepts_small_cut(self):
        self.make_db()
        worker = self.worker()
        with patch('export_safety.shutil.disk_usage', return_value=SimpleNamespace(free=90*GIB, used=0)), \
             patch.object(worker.safety, 'usage', return_value=worker.safety.policy.budget - 128*1024**2), \
             redirect_stdout(StringIO()):
            result = worker.run_due(NOW)
        self.assertEqual('SKIPPED', result['status'])
        self.assertEqual('EXPORT_BUDGET', result['skipped_export_reason'])
        self.assertEqual('VALID', result['assistant_bridge']['status'])
        self.assertEqual([], list(worker.root.glob('*/signals.db')))
        self.assertFalse((worker.root / 'latest.json').exists())

    def test_sqlite_progress_interrupt_preserves_resource_guard_reason(self):
        self.make_db()
        callbacks = []
        def stop():
            callbacks.append(True)
            raise ExportSkipped('TIME_LIMIT')
        with self.assertRaisesRegex(ExportSkipped, 'TIME_LIMIT'):
            with bridge._live_connection(self.path.resolve(), stop) as conn:
                conn.execute('WITH RECURSIVE numbers(n) AS (VALUES(1) UNION ALL '
                             'SELECT n+1 FROM numbers WHERE n<1000000) SELECT SUM(n) FROM numbers').fetchone()
        self.assertTrue(callbacks, 'The stop must occur inside SQLite progress callback')

    def test_null_parent_key_fails_closed_before_relational_filter_can_hide_it(self):
        self.make_db()
        with closing(REAL_CONNECT(self.path)) as conn, conn:
            conn.execute('DROP TABLE premium_wave_events')
            conn.execute('CREATE TABLE premium_wave_events(signal_id INTEGER, wave_no INTEGER, '
                         'PRIMARY KEY(signal_id,wave_no))')
            conn.execute('INSERT INTO premium_wave_events VALUES(NULL,1)')
        with self.assertRaisesRegex(ValueError, 'PREMIUM_NULL_PARENT_KEY: premium_wave_events'):
            bridge.emit_live(self.path, dict(MANIFEST), printer=self.printer)
        self.assertEqual([], self.lines)

    def test_source_code_identity_is_retained_without_live_db_hash_claim(self):
        self.make_db()
        manifest = dict(MANIFEST, source_code_sha256={'assistant_bridge.py': 'f' * 64},
                        config_identity='1' * 64, git_commit='synthetic-test-commit')
        payload = bridge.build_live_payload(self.path, manifest)
        self.assertEqual(manifest['source_code_sha256'], payload['source_identity']['source_code_sha256'])
        self.assertEqual(manifest['config_identity'], payload['source_identity']['config_identity'])
        self.assertIsNone(payload['source_sha256'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
