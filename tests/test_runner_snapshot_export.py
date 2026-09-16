"""Standalone exporter safety tests; never import/start the production bot."""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'binance_momentum_bot'))
import runner_snapshot_export as exports


class RunnerSnapshotExportCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.root = self.base / 'research_exports'
        self.directory = self.root / exports.SNAPSHOT_ID
        self.directory.mkdir(parents=True)
        self.source = self.directory / 'signals.db'
        self.output = self.base / 'runner.zip'
        ddl = (Path(exports.__file__).parent / 'runner_shadow_v1.py').read_text(encoding='utf-8')
        with closing(sqlite3.connect(self.source)) as conn:
            # Real repository runner schema, with deliberately sensitive extra data.
            for sql in re.findall(r'CREATE TABLE IF NOT EXISTS (?:runner_\w+|non_runner_\w+)\s*\(.*?\);', ddl, re.S):
                conn.execute(sql)
            conn.executescript('''
                CREATE TABLE signals_v2(id INTEGER, ts INTEGER, symbol TEXT, level TEXT, score INTEGER, price REAL, episode_id INTEGER);
                CREATE TABLE signal_meta(signal_id INTEGER, premium INTEGER);
                CREATE TABLE signal_outcomes(signal_id INTEGER, horizon_s INTEGER, return_pct REAL, mfe_pct REAL, mae_pct REAL, ts INTEGER);
                CREATE TABLE autotrade_settings(secret TEXT);
                INSERT INTO autotrade_settings VALUES('SECRET_MUST_NOT_LEAK');
                INSERT INTO signals_v2 VALUES(1,1789580016,'XUSDT','CONFIRMED',80,100,42);
                INSERT INTO signal_meta VALUES(1,1);
                INSERT INTO signal_outcomes VALUES(1,3600,-1,2,-3,1789583617);
                INSERT INTO runner_score_v1_shadow(id,source_key,symbol,stage,event_ts_ms,episode_id,signal_id,price,score,model_version,raw_features_json,contributions_json,reason_codes_json)
                VALUES(1,'test','XUSDT','CANDIDATE',1789580016000,42,1,100,80,'v1','SECRET_MUST_NOT_LEAK','{}','[]');
                INSERT INTO runner_watch_v1_shadow(watch_id,score_id,symbol,kind,anchor_ts_ms,anchor_price,anchor_score,expires_ts_ms,outcome_due_ts_ms,state,reason_codes_json,created_ts_ms,updated_ts_ms)
                VALUES('w1',1,'XUSDT','FAST_RUNNER',1789580016000,100,80,1789581816000,1789583616000,'WATCH','[]',1789580016000,1789580016000);
                INSERT INTO runner_watch_outcome_v1_shadow VALUES('w1',1789583617000,2,-3,'NON_RUNNER',0,'v1');
                INSERT INTO non_runner_veto_v1_shadow VALUES('w1',15,1789580031000,100,0,1,-1,'WAIT','[]','SECRET_MUST_NOT_LEAK','v1');
                ALTER TABLE runner_score_v1_shadow ADD COLUMN api_secret TEXT DEFAULT 'SECRET_MUST_NOT_LEAK';
            ''')
        self.manifest = {'valid': True, 'snapshot_id': exports.SNAPSHOT_ID,
                         'created_time_ms': 1789585648525,
                         'sha256': exports.sha256(self.source),
                         'quick_check': ['ok'], 'integrity_check': ['ok'], 'restore_smoke_check': ['ok'],
                         'tables': {table: {'rows': 1} for table in exports.FIELDS},
                         'private_metadata': 'SECRET_MUST_NOT_LEAK'}
        self.save_manifest()

    def save_manifest(self):
        (self.directory / 'manifest.json').write_text(json.dumps(self.manifest), encoding='utf-8')

    def run_export(self):
        return exports.export_snapshot(exports.SNAPSHOT_ID, self.output, root=self.root)

    def test_allowlist_hash_ranges_and_read_only_connection(self):
        before = {p.name: p.read_bytes() for p in self.directory.iterdir()}
        original_connect = sqlite3.connect
        def connect(database, **kwargs):
            self.assertEqual(database, self.source.as_uri() + '?mode=ro&immutable=1')
            self.assertTrue(kwargs['uri'])
            conn = original_connect(database, **kwargs)
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute('CREATE TABLE forbidden(x)')
            return conn
        with patch.object(exports.sqlite3, 'connect', side_effect=connect):
            manifest = self.run_export()
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.directory.iterdir()})
        with zipfile.ZipFile(self.output) as archive:
            self.assertEqual(set(archive.namelist()), {'payload.json', 'manifest.json', 'README.txt'})
            for name in archive.namelist():
                self.assertNotIn(b'SECRET_MUST_NOT_LEAK', archive.read(name))
            payload = archive.read('payload.json')
            self.assertEqual(hashlib.sha256(payload).hexdigest(), manifest['payload_sha256'])
            data = json.loads(payload)['tables']
            self.assertEqual(set(data), set(exports.FIELDS))
            for table, fields in exports.FIELDS.items():
                self.assertEqual(data[table]['columns'], fields)
                self.assertEqual(len(data[table]['rows']), manifest['tables'][table]['rows'])
        self.assertEqual(manifest['tables']['signals_v2']['timestamp_ranges']['ts'],
                         {'min': 1789580016, 'max': 1789580016, 'unit': 'seconds'})

    def test_live_paths_and_traversal_rejected_before_open(self):
        with patch.object(exports.sqlite3, 'connect', side_effect=AssertionError('must not connect')):
            for identifier in ('/data/signals.db', '../signals.db', '../' + exports.SNAPSHOT_ID, 'latest', exports.SNAPSHOT_ID + '/..'):
                with self.subTest(identifier=identifier), self.assertRaises(ValueError):
                    exports.export_snapshot(identifier, self.output, root=self.root)

    def test_invalid_missing_or_changed_manifest_fails_closed(self):
        for key, value in [('valid', False), ('sha256', '0' * 64), ('snapshot_id', 'other'),
                           ('quick_check', []), ('integrity_check', []), ('restore_smoke_check', [])]:
            original = self.manifest[key]
            self.manifest[key] = value
            self.save_manifest()
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.run_export()
            self.assertFalse(self.output.exists())
            self.manifest[key] = original
        (self.directory / 'manifest.json').unlink()
        with self.assertRaises(FileNotFoundError):
            self.run_export()

    def test_sidecars_and_hardlinks_refused(self):
        sidecar = Path(str(self.source) + '-wal')
        sidecar.write_bytes(b'')
        with self.assertRaisesRegex(ValueError, 'sidecar'):
            self.run_export()
        sidecar.unlink()
        link = self.base / 'linked.db'
        os.link(self.source, link)
        with self.assertRaisesRegex(ValueError, 'Hard-linked'):
            self.run_export()

    def test_symlink_source_manifest_directory_and_output_refused(self):
        probe = self.base / 'probe'
        try:
            probe.symlink_to(self.source)
        except OSError as exc:
            self.skipTest('OS does not permit symlinks: ' + str(exc))
        probe.unlink()
        for path in (self.source, self.directory / 'manifest.json', self.directory):
            with self.subTest(path=path):
                moved = path.with_name(path.name + '-real')
                path.rename(moved)
                path.symlink_to(moved, target_is_directory=moved.is_dir())
                try:
                    with self.assertRaisesRegex(ValueError, 'Symlink'):
                        self.run_export()
                finally:
                    path.unlink()
                    moved.rename(path)
        self.output.symlink_to(self.source)
        with self.assertRaisesRegex(ValueError, 'exists'):
            self.run_export()

    def test_missing_table_columns_and_manifest_count_mismatch(self):
        self.manifest['tables']['signals_v2']['rows'] = 2
        self.save_manifest()
        with self.assertRaisesRegex(ValueError, 'row count'):
            self.run_export()
        self.manifest['tables']['signals_v2']['rows'] = 1
        with closing(sqlite3.connect(self.source)) as conn:
            conn.execute('ALTER TABLE signals_v2 DROP COLUMN episode_id')
            conn.commit()
        self.manifest['sha256'] = exports.sha256(self.source)
        self.save_manifest()
        with self.assertRaisesRegex(ValueError, 'columns missing'):
            self.run_export()
        with closing(sqlite3.connect(self.source)) as conn:
            conn.execute('DROP TABLE signals_v2')
            conn.commit()
        self.manifest['sha256'] = exports.sha256(self.source)
        self.save_manifest()
        with self.assertRaisesRegex(ValueError, 'table missing'):
            self.run_export()

    def test_no_overwrite_or_output_inside_snapshot_tree(self):
        self.output.write_bytes(b'keep')
        with self.assertRaisesRegex(ValueError, 'exists'):
            self.run_export()
        self.assertEqual(self.output.read_bytes(), b'keep')
        with self.assertRaisesRegex(ValueError, 'outside'):
            exports.export_snapshot(exports.SNAPSHOT_ID, self.directory / 'out.zip', root=self.root)

    def test_source_change_during_export_prevents_publication(self):
        original = exports.source_files
        calls = 0
        def validate(*args):
            nonlocal calls
            calls += 1
            if calls == 2:
                self.manifest['created_time_ms'] += 1
                self.save_manifest()
            return original(*args)
        with patch.object(exports, 'source_files', side_effect=validate):
            with self.assertRaisesRegex(ValueError, 'changed'):
                self.run_export()
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
