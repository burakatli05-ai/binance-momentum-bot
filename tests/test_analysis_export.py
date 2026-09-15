import asyncio
from contextlib import closing
from datetime import datetime
import json
import gc
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import AsyncMock, patch
import zipfile

import test_v5135 as fixtures
import analysis_export as exports
from research_export import Exporter, IST, atomic_json, readonly

bot = fixtures.bot


class AnalysisExportCase(unittest.TestCase):
    def setUp(self):
        fixtures.DatabaseCase.setUp(self)
        # Existing bot initialization can leave cyclic SQLite objects until GC;
        # Windows requires their handles released before TemporaryDirectory cleanup.
        self.addCleanup(gc.collect)
    sql = fixtures.DatabaseCase.sql

    def seed(self):
        self.now = int(datetime(2026, 9, 16, 12, tzinfo=IST).timestamp() * 1000)
        self.start = int(datetime(2026, 9, 9, tzinfo=IST).timestamp() * 1000)
        self.worker = Exporter(bot.DB_PATH, Path(self.tmp.name) / 'exports')
        # Old, exact lower bound, today's matured, exact upper bound, future.
        for ident, ts in enumerate((self.start - 1000, self.start, self.now - 3600000, self.now, self.now + 1000), 1):
            self.sql("INSERT INTO signals_v2(id,ts,symbol,level,score,price) VALUES(?,?,'CAPUSDT','CONFIRMED',80,100)", (ident, ts // 1000))
            self.sql("INSERT INTO candidate_events(id,ts,symbol,event,note) VALUES(?,?,'CAPUSDT','premium_reject','flow;reset;reacquire')", (ident, ts // 1000))
            self.sql('INSERT INTO signal_meta(signal_id,gainer_rank) VALUES(?,2)', (ident,))
            self.sql('INSERT INTO signal_outcomes VALUES(?,3600,5,8,-1,?)', (ident, ts // 1000))
            self.sql('INSERT INTO premium_wave_events(signal_id,wave_no,peak_mfe_pct) VALUES(?,1,8)', (ident,))
            self.sql("INSERT INTO runner_score_v1_shadow(id,source_key,symbol,stage,event_ts_ms,score,model_version,raw_features_json,contributions_json,reason_codes_json,mfe_60_pct,mae_60_pct,outcome_label) VALUES(?,?,'CAPUSDT','CANDIDATE',?,80,'v1','{\"rel60\":2}','{}','[]',8,-1,'RUNNER')", (ident, str(ident), ts))
            self.sql("INSERT INTO runner_watch_v1_shadow(watch_id,score_id,symbol,kind,anchor_ts_ms,anchor_price,anchor_score,expires_ts_ms,outcome_due_ts_ms,state,reason_codes_json,created_ts_ms,updated_ts_ms) VALUES(?,?,'CAPUSDT','FAST_RUNNER',?,100,80,?,?,'WATCH','[]',?,?)", (str(ident), ident, ts, ts+30000, ts+3600000, ts, ts))
            self.sql("INSERT INTO non_runner_veto_v1_shadow VALUES(?,15,?,100,1,2,-1,'PASS','[]','{}','v1')", (str(ident), ts))
            self.sql("INSERT INTO runner_watch_outcome_v1_shadow VALUES(?,?,8,-1,'RUNNER',0,'v1')", (str(ident), ts))
        self.sql('CREATE TABLE raw_ticks(payload BLOB)')
        self.sql('INSERT INTO raw_ticks VALUES(zeroblob(4000000))')
        # A future child must not leak just because its parent is in-window.
        self.sql('INSERT INTO premium_micro_snapshots(signal_id,horizon_ms,observed_ts_ms,age_ms) VALUES(3,15000,?,15000)', (self.now + 1,))
        self.sql('INSERT INTO premium_micro_snapshots(signal_id,horizon_ms,observed_ts_ms,age_ms,rel30) VALUES(3,30000,?,30000,2)', (self.now - 30000,))
        self.sql("INSERT INTO momentum_episodes(id,symbol,start_ts,end_ts) VALUES(1,'CAPUSDT',?,?)", (self.start // 1000 - 100, self.start // 1000 + 100))

    def test_fallback_bounded_complete_compact_manifest_repeat_and_cleanup(self):
        self.seed()
        gc.collect()  # Finish fixture writers before measuring immutable source bytes.
        original = Path(bot.DB_PATH).read_bytes()
        hashes = []
        for _ in range(2):
            with exports.package(self.worker, self.now) as (bundle, manifest):
                with zipfile.ZipFile(bundle) as archive:
                    self.assertEqual(set(archive.namelist()), {'analysis.db', 'manifest.json', 'README.txt'})
                    self.assertEqual(manifest, json.loads(archive.read('manifest.json')))
                target = bundle.parent / 'analysis.db'
                hashes.append(exports.sha256(target))
                self.assertEqual(manifest['analysis_sha256'], hashes[-1])
                self.assertEqual(manifest['analysis_size_bytes'], target.stat().st_size)
                self.assertEqual(manifest['source_sha256'], exports.sha256(manifest['source_snapshot_path']))
                self.assertEqual(manifest['requested_range'], {'start_ms': self.start, 'end_ms': self.now})
                self.assertEqual(manifest['quick_check'], ['ok'])
                self.assertEqual(manifest['integrity_check'], ['ok'])
                self.assertEqual(manifest['source_kind'], 'READONLY_BACKUP')
                self.assertLess(target.stat().st_size, len(original) // 2)
                with closing(readonly(target)) as conn, closing(readonly(bot.DB_PATH)) as src:
                    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
                    self.assertEqual(tables, set(exports.INCLUDED_TABLES))
                    for table, stats in manifest['tables'].items():
                        self.assertEqual(stats['rows'], conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
                        self.assertEqual(list(conn.execute(f'PRAGMA table_info("{table}")')), list(src.execute(f'PRAGMA table_info("{table}")')))
                        for field, expected in stats['timestamp_ranges'].items():
                            self.assertEqual(expected, list(conn.execute(f'SELECT MIN("{field}"),MAX("{field}") FROM "{table}"').fetchone()))
                    for table, key in (('signals_v2','id'), ('candidate_events','id'), ('signal_meta','signal_id'), ('signal_outcomes','signal_id'), ('premium_wave_events','signal_id'), ('runner_score_v1_shadow','id')):
                        self.assertEqual([2, 3, 4], [r[0] for r in conn.execute(f'SELECT {key} FROM {table} ORDER BY {key}')])
                    for table in ('runner_watch_v1_shadow','non_runner_veto_v1_shadow','runner_watch_outcome_v1_shadow'):
                        self.assertEqual(['2','3','4'], [r[0] for r in conn.execute(f'SELECT watch_id FROM {table} ORDER BY watch_id')])
                    self.assertEqual([(30000,)], conn.execute('SELECT horizon_ms FROM premium_micro_snapshots').fetchall())
                    self.assertEqual([(8.0, -1.0)], conn.execute('SELECT mfe_pct,mae_pct FROM signal_outcomes WHERE signal_id=3 AND horizon_s=3600').fetchall())
                    self.assertEqual(1, conn.execute('SELECT COUNT(*) FROM momentum_episodes').fetchone()[0])
            self.assertFalse(bundle.exists())
            self.assertEqual(original, Path(bot.DB_PATH).read_bytes())
            self.assertFalse((self.worker.root / 'latest.json').exists())
        self.assertEqual(hashes[0], hashes[1])
        self.assertEqual([], list(self.worker.root.iterdir()))

    def test_latest_valid_uses_snapshot_not_live_even_with_invalid_newer_pointer(self):
        self.seed()
        directory, source_manifest = self.worker.snapshot(self.now - 1000)
        self.assertTrue(source_manifest['valid'])
        before = (directory / 'signals.db').read_bytes()
        bad = self.worker.root / 'invalid-newer'
        bad.mkdir()
        (bad / 'signals.db').write_bytes(b'not sqlite')
        atomic_json(bad / 'manifest.json', {'valid': True, 'snapshot_id': bad.name, 'created_time_ms': self.now, 'sha256': 'bad'})
        atomic_json(self.worker.root / 'latest.json', {'snapshot_id': bad.name})
        with patch.object(exports, '_backup', side_effect=AssertionError('must not read live')):
            with exports.package(self.worker, self.now) as (bundle, manifest):
                self.assertEqual(source_manifest['snapshot_id'], manifest['source_snapshot_id'])
                self.assertEqual(self.now - 1000, manifest['effective_range']['end_ms'])
                self.assertEqual(2, manifest['tables']['signals_v2']['rows'])
        self.assertEqual(before, (directory / 'signals.db').read_bytes())
        self.assertEqual(bad.name, json.loads((self.worker.root / 'latest.json').read_text())['snapshot_id'])

    def test_missing_table_fails_explicitly_without_partial_bundle_or_source_change(self):
        self.seed()
        self.sql('DROP TABLE runner_watch_outcome_v1_shadow')
        before = Path(bot.DB_PATH).read_bytes()
        with self.assertRaisesRegex(ValueError, 'runner_watch_outcome_v1_shadow'):
            with exports.package(self.worker, self.now):
                self.fail('must not yield incomplete research package')
        self.assertEqual(before, Path(bot.DB_PATH).read_bytes())
        self.assertEqual([], list(self.worker.root.iterdir()))

    def test_busy_export_rejected_and_lock_released_after_send_exception(self):
        self.seed()
        with self.assertRaisesRegex(RuntimeError, 'send failed'):
            with exports.package(self.worker, self.now):
                with self.assertRaisesRegex(RuntimeError, 'already running'):
                    with exports.package(self.worker, self.now):
                        self.fail('parallel export')
                raise RuntimeError('send failed')
        with exports.package(self.worker, self.now):
            pass

    def test_readonly_backup_includes_committed_wal_without_checkpointing_source(self):
        self.seed()
        with closing(sqlite3.connect(bot.DB_PATH)) as writer:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute("INSERT INTO candidate_events(ts,symbol,event) VALUES(?,'CYSUSDT','candidate_reset')", (self.now // 1000,))
            writer.commit()
            before = {p: p.read_bytes() for p in (Path(bot.DB_PATH), Path(bot.DB_PATH + '-wal'))}
            with exports.package(self.worker, self.now) as (_, manifest):
                self.assertEqual(4, manifest['tables']['candidate_events']['rows'])
            for path, contents in before.items():
                self.assertEqual(contents, path.read_bytes())

    def test_real_admin_auth_private_only_disabled_and_success(self):
        self.seed()
        with patch.object(bot, 'TELEGRAM_ADMIN_CHAT_ID', '123'), patch.object(bot, 'TELEGRAM_ADMIN_USER_ID', '123'), patch.object(bot, 'export_worker', self.worker), patch.object(bot, 'telegram_send', new_callable=AsyncMock) as notify, patch.object(bot, 'telegram_send_document', new_callable=AsyncMock, return_value=True) as send:
            with patch.object(exports, 'package', wraps=exports.package) as create:
                for chat, user in (('456','456'), ('123','456'), ('-123','123')):
                    self.assertTrue(asyncio.run(bot._at_command(None, '/analysisexport', chat, user)))
                create.assert_not_called()
                send.assert_not_awaited()
                asyncio.run(bot._at_command(None, '/analysisexport', '123', '123'))
                create.assert_called_once()
                self.assertEqual('123', send.await_args.kwargs['chat_id'])
                self.assertFalse(Path(send.await_args.args[1]).exists())
            with patch.object(bot, 'TELEGRAM_ADMIN_CHAT_ID', '-123'):
                asyncio.run(bot._at_command(None, '/analysisexport', '-123', '123'))
                self.assertIn('özel admin', notify.await_args.args[1])
            with patch.object(bot, 'export_worker', None):
                asyncio.run(bot._at_command(None, '/analysisexport', '123', '123'))
                self.assertIn('Export kapalı', notify.await_args.args[1])

    def test_command_send_failure_and_build_failure_do_not_escape(self):
        self.seed()
        with patch.object(bot, 'TELEGRAM_ADMIN_CHAT_ID', '123'), patch.object(bot, 'TELEGRAM_ADMIN_USER_ID', '123'), patch.object(bot, 'export_worker', self.worker), patch.object(bot, 'telegram_send', new_callable=AsyncMock) as notify, patch.object(bot, 'telegram_send_document', new_callable=AsyncMock, side_effect=TimeoutError('secret')):
            self.assertTrue(asyncio.run(bot._at_command(None, '/analysisexport', '123', '123')))
            self.assertIn('TimeoutError', notify.await_args.args[1])
            self.assertNotIn('secret', notify.await_args.args[1])
            self.assertEqual([], list(self.worker.root.iterdir()))
            with patch.object(exports, '_backup', side_effect=OSError('private path')):
                self.assertTrue(asyncio.run(bot._at_command(None, '/analysisexport', '123', '123')))
                self.assertIn('OSError', notify.await_args.args[1])

    def test_every_allowlisted_table_retains_real_schema_and_in_window_rows(self):
        self.seed()
        with closing(sqlite3.connect(bot.DB_PATH)) as conn:
            for table in exports.INCLUDED_TABLES:
                if conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]:
                    continue
                info = list(conn.execute(f'PRAGMA table_info("{table}")'))
                row = {name: ('3' if 'TEXT' in kind else 3) for _, name, kind, *_ in info}
                if table in exports.ROOTS:
                    clock, unit = exports.ROOTS[table]
                    row[clock] = (self.now - 3600000) // unit
                if table in exports.CHILDREN:
                    key, _, _, clock, unit = exports.CHILDREN[table]
                    row[key] = '3' if key in ('cohort_id','watch_id') else 3
                    if clock:
                        row[clock] = (self.now - 30000) // unit
                if table in exports.EPISODES:
                    row['start_ts'] = (self.now - 3600000) // 1000
                    row[exports.EPISODES[table]] = self.now // 1000
                columns = ','.join(exports.quote(name) for name in row)
                placeholders = ','.join('?' for _ in row)
                conn.execute(f'INSERT INTO "{table}"({columns}) VALUES({placeholders})', tuple(row.values()))
            conn.commit()
        with exports.package(self.worker, self.now) as (_, manifest):
            for table in exports.INCLUDED_TABLES:
                self.assertGreater(manifest['tables'][table]['rows'], 0, table)

    def test_telegram_oversize_and_http_failure_explicit_and_cleanup(self):
        from test_latestexport_hotfix import LatestExportCase
        self.seed()
        with patch.object(bot, 'TELEGRAM_ADMIN_CHAT_ID', '123'), patch.object(bot, 'TELEGRAM_ADMIN_USER_ID', '123'), patch.object(bot, 'TELEGRAM_BOT_TOKEN', 'fake-token'), patch.object(bot, 'export_worker', self.worker), patch.object(bot, 'telegram_send', new_callable=AsyncMock) as notify:
            session = LatestExportCase().response(413, {'ok': False})
            with patch.object(bot, 'TELEGRAM_DOCUMENT_MAX_BYTES', 1):
                asyncio.run(bot._at_command(session, '/analysisexport', '123', '123'))
                session.post.assert_not_called()
                self.assertIn('Telegram dosya limiti', notify.await_args.args[1])
                self.assertIn('bayt', notify.await_args.args[1])
            asyncio.run(bot._at_command(session, '/analysisexport', '123', '123'))
            self.assertIn('HTTP 413', notify.await_args.args[1])
            self.assertEqual([], list(self.worker.root.iterdir()))

    def test_stale_snapshot_reports_empty_window_without_reading_live(self):
        self.seed()
        directory, source_manifest = self.worker.snapshot(self.start - 1)
        with patch.object(exports, '_backup', side_effect=AssertionError('live read')):
            with exports.package(self.worker, self.now) as (_, manifest):
                self.assertTrue(manifest['effective_range']['empty'])
                self.assertTrue(all(stats['rows'] == 0 for stats in manifest['tables'].values()))


if __name__ == '__main__':
    unittest.main()
