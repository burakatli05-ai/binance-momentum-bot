from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'binance_momentum_bot'))
import startup


class StartupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.backup = self.root/'backup.db'
        self.target = self.root/'signals.db'
        with closing(sqlite3.connect(self.backup)) as c, c:
            c.execute('CREATE TABLE signals_v2(id INTEGER PRIMARY KEY,ts INTEGER,symbol TEXT,price REAL)')
            c.execute("INSERT INTO signals_v2 VALUES(1,123,'BTCUSDT',1.0)")
        self.manifest = dict(bytes=self.backup.stat().st_size,
            sha256=hashlib.sha256(self.backup.read_bytes()).hexdigest(),
            table_counts={'signals_v2':1}, anchors=[[1,123,'BTCUSDT',1.0]])

    def test_missing_db_restores_verified_source(self):
        startup.prepare_db(self.target,self.backup,self.manifest)
        self.assertEqual(self.target.read_bytes(),self.backup.read_bytes())

    def test_valid_db_kept_without_backup(self):
        startup.prepare_db(self.target,self.backup,self.manifest)
        with closing(sqlite3.connect(self.target)) as c, c:
            c.execute("INSERT INTO signals_v2 VALUES(2,124,'ETHUSDT',2.0)")
        before=self.target.read_bytes()
        self.backup.unlink()
        startup.prepare_db(self.target,self.backup,self.manifest)
        self.assertEqual(before,self.target.read_bytes())

    def test_wrong_history_quarantined_and_restored(self):
        startup.prepare_db(self.target,self.backup,self.manifest)
        with closing(sqlite3.connect(self.target)) as c, c:
            c.execute("UPDATE signals_v2 SET symbol='WRONG'")
        previous=self.target.read_bytes()
        startup.prepare_db(self.target,self.backup,self.manifest)
        self.assertEqual(next(self.root.glob('quarantine-*/signals.db')).read_bytes(),previous)
        self.assertEqual(self.target.read_bytes(),self.backup.read_bytes())

    def test_empty_db_without_source_fails_without_modification(self):
        self.target.write_bytes(b'')
        self.backup.unlink()
        with self.assertRaises(RuntimeError):
            startup.prepare_db(self.target,self.backup,self.manifest)
        self.assertEqual(self.target.read_bytes(),b'')

    def test_bad_hash_never_replaces_existing_database(self):
        self.target.write_bytes(b'old evidence')
        self.manifest['sha256']='0'*64
        with self.assertRaises(RuntimeError):
            startup.prepare_db(self.target,self.backup,self.manifest)
        self.assertEqual(self.target.read_bytes(),b'old evidence')

    def test_no_mount_never_executes_bot(self):
        with patch.dict('os.environ',{'DB_MAINTENANCE':'0'}), patch.object(Path,'is_mount',return_value=False), patch('os.execv') as execute:
            with self.assertRaises(RuntimeError): startup.main()
            execute.assert_not_called()

    def test_restore_validation_failure_preserves_original(self):
        self.target.write_bytes(b'old evidence')
        self.manifest['table_counts']['signals_v2']=2
        with self.assertRaises(RuntimeError):
            startup.prepare_db(self.target,self.backup,self.manifest)
        self.assertEqual(self.target.read_bytes(),b'old evidence')


if __name__=='__main__': unittest.main()
