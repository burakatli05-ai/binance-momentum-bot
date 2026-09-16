import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'binance_momentum_bot'))
import assistant_bridge as bridge


class BridgeCase(unittest.TestCase):
    def make_db(self, omit=None):
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        path = Path(td.name) / 'snapshot.db'
        with sqlite3.connect(path) as c:
            for table in bridge.INCLUDED_TABLES:
                if table == omit:
                    continue
                c.execute(f'CREATE TABLE "{table}" (id INTEGER, txt TEXT, val REAL)')
                c.execute(f'INSERT INTO "{table}" VALUES (1, ?, 2.5)', (table,))
            c.execute('CREATE TABLE api_secrets(token TEXT)')
            c.execute("INSERT INTO api_secrets VALUES ('DO_NOT_EXPORT')")
        return path

    def test_roundtrip_allowlist_and_hash(self):
        path = self.make_db()
        lines = []
        meta = bridge.emit(
            path,
            {'snapshot_id': 'snap-1', 'created_time_ms': 123, 'deployment_id': 'dep', 'sha256': 'src'},
            printer=lambda *args, **kwargs: lines.append(args[0]),
        )
        chunks = [line.split(' ', 4)[4] for line in lines if ' CHUNK ' in line]
        payload = bridge.decode_chunks(chunks, meta['sha256'])
        self.assertEqual('snap-1', payload['snapshot_id'])
        self.assertEqual(set(bridge.INCLUDED_TABLES), set(payload['tables']))
        self.assertNotIn('api_secrets', payload['tables'])
        self.assertNotIn('DO_NOT_EXPORT', json.dumps(payload))
        self.assertTrue(lines[0].startswith(bridge.LOG_PREFIX + ' META '))
        self.assertTrue(lines[-1].startswith(bridge.LOG_PREFIX + ' END '))

    def test_missing_required_table_fails_closed(self):
        path = self.make_db(omit='signal_paths')
        with self.assertRaisesRegex(ValueError, 'signal_paths'):
            bridge.build_payload(path, {'snapshot_id': 'x'})


if __name__ == '__main__':
    unittest.main()
