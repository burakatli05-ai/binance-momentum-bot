import ast
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'binance_momentum_bot'))
import quality_shadow as q

class RecorderTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'db.sqlite'
        self.connect=lambda:sqlite3.connect(self.path)
        self.now=1800000000000
        self.r=q.Recorder(self.connect,self.now)

    def rows(self,table):
        c=self.connect()
        try:return c.execute('SELECT * FROM '+table).fetchall()
        finally:c.close()

    def test_no_sql_on_tick_and_snapshot_idempotency(self):
        self.r.arm('PREMIUM',1,'ABC',2,100,self.now,{'score':80})
        self.r.connect=lambda:(_ for _ in ()).throw(AssertionError('hot-path SQL'))
        for i in range(1,201):self.r.tick('ABC',100+i/100,self.now+i,self.now+i)
        self.r.sample(self.now+60000,{})
        self.r.sample(self.now+60000,{})
        self.r.connect=self.connect
        batch=self.r.take_batch();self.r.flush(batch);self.r.flush(batch)
        self.assertEqual(4,len(self.rows('quality_shadow_snapshots')))
        self.assertEqual(2,len(self.rows('quality_shadow_scores')))
        for r in self.rows('quality_shadow_scores'):
            self.assertIsNone(r[4]);self.assertIn('ABSTAIN',r[5])

    def test_restart_recovers_snapshots_and_touches_with_gap(self):
        self.r.arm('EARLY',1,'ABC',2,100,self.now)
        self.r.tick('ABC',110,self.now+1000,self.now+1000)
        self.r.flush(self.r.take_batch())
        r=q.Recorder(self.connect,self.now+5000)
        r.tick('ABC',110,self.now+6000,self.now+6000)
        r.sample(self.now+180000,{})
        r.flush(r.take_batch())
        self.assertEqual(6,len(self.rows('quality_shadow_snapshots')))
        touches=self.rows('quality_shadow_touches')
        self.assertEqual(len(touches),len({x[1] for x in touches}))
        self.assertTrue(any(x[3]=='RESTART_UNOBSERVED_INTERVAL' for x in self.rows('quality_shadow_gaps')))

    def test_failed_flush_retains_replayable_transaction(self):
        self.r.arm('EARLY',1,'ABC',2,100,self.now)
        batch=self.r.take_batch()
        self.r.connect=lambda:(_ for _ in ()).throw(sqlite3.OperationalError('locked'))
        with self.assertRaises(sqlite3.OperationalError):self.r.flush(batch)
        self.r.connect=self.connect;self.r.flush(batch)
        self.assertEqual(1,len(self.rows('quality_shadow_cohorts')))

    def test_old_schema_unchanged_and_missing_telemetry_explicit(self):
        c=self.connect();c.execute('CREATE TABLE production(id INTEGER PRIMARY KEY,value TEXT)')
        c.execute("INSERT INTO production VALUES (1,'keep')");c.commit();q.migrate(c);c.close()
        self.r.arm('PREMIUM',2,'ABC',2,100,self.now)
        self.r.sample(self.now+181000,{})
        self.r.flush(self.r.take_batch())
        self.assertEqual([(1,'keep')],self.rows('production'))
        p=json.loads(self.rows('quality_shadow_snapshots')[-1][4])
        self.assertIn('chg30',p['missing'])

    def test_buffer_is_bounded_and_loss_is_persisted(self):
        self.r.limit=3
        for i in range(20):self.r.arm('PREMIUM',i,'ABC',i,100,self.now)
        self.assertLessEqual(len(self.r.pending),3)
        self.r.flush(self.r.take_batch())
        self.assertTrue(any('BUFFER_OVERFLOW' in x[3] for x in self.rows('quality_shadow_gaps')))

    def test_future_and_out_of_order_ticks_do_not_create_false_touch(self):
        self.r.arm('EARLY',1,'ABC',2,100,self.now)
        self.r.tick('ABC',120,self.now+1000,self.now)
        self.r.tick('ABC',90,self.now-1,self.now)
        self.r.flush(self.r.take_batch())
        self.assertEqual([],self.rows('quality_shadow_touches'))

    def test_recovery_discovers_saved_but_unflushed_signals(self):
        c=self.connect();c.execute('CREATE TABLE signals_v2(id,symbol,episode_id,price,ts)')
        c.execute('INSERT INTO signals_v2 VALUES (1,?,?,?,?)',('ABC',2,100,self.now//1000));c.commit();c.close()
        self.assertEqual(1,len(self.r.discover(self.now+2000)))

    def test_module_has_no_execution_or_network_dependencies(self):
        tree=ast.parse((ROOT/'binance_momentum_bot/quality_shadow.py').read_text())
        imports={n.name for node in ast.walk(tree) if isinstance(node,ast.Import) for n in node.names}
        self.assertFalse(imports&{'bot','aiohttp','requests','urllib','subprocess'})

if __name__=='__main__':unittest.main()
