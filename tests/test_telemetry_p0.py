import asyncio
import ast
import gc
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
import telemetry_p0 as p0
import telemetry_report


class ForwardTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(gc.collect)
        self.path=Path(self.tmp.name)/'test.db'
        with p0.connection(self.connect) as c:
            p0.migrate(c); p0.migrate(c)
        self.engine=p0.Telemetry(self.connect)

    def connect(self):
        return sqlite3.connect(self.path)

    def rows(self, sql, args=()):
        with p0.connection(self.connect) as c:
            c.row_factory=sqlite3.Row
            return [dict(r) for r in c.execute(sql,args)]

    def test_allow_reference_excludes_anchor_and_predecision_ticks(self):
        self.engine.arm('allow:x','X','FAST',100000,110,1,'parent')
        self.engine.tick('X',90,99999,100001)
        self.assertIsNone(self.engine.active['allow:x']['fill'])
        self.engine.tick('X',111,100001,100001,112,100001,100001)
        for stamp in range(101000,160001,1000):
            self.engine.tick('X',113,stamp,stamp,113,stamp,stamp)
        row=self.rows('SELECT * FROM p0_forward_outcomes')[0]
        self.assertAlmostEqual((113/112-1)*100,row['return_pct'])
        self.assertAlmostEqual((113/110-1)*100,row['reference_return_pct'])
        self.assertAlmostEqual((111/112-1)*100,row['mae_pct'])
        self.assertLess(row['net_return_pct'],row['return_pct'])
        self.assertEqual(160000,row['due_ts_ms'])

    def test_first_stop_ordering_continues_full_horizon(self):
        self.engine.arm('x','X','REACQUIRE',100000,100)
        self.engine.tick('X',100,100001,100001,100,100001,100001)
        self.engine.tick('X',98,101000,101000)
        self.engine.tick('X',104,102000,102000)
        for stamp in range(103000,160001,1000):
            self.engine.tick('X',103,stamp,stamp)
        row=self.rows('SELECT * FROM p0_forward_outcomes')[0]
        self.assertEqual('STOP',row['first_passage'])
        self.assertAlmostEqual(4,row['mfe_pct'])
        self.assertAlmostEqual(-2,row['mae_pct'])

    def test_late_horizon_never_uses_future_extreme(self):
        self.engine.arm('x','X','FAST',100000,100)
        self.engine.tick('X',100,100001,100001,100,100001,100001)
        self.engine.tick('X',120,180000,180000)
        row=self.rows('SELECT * FROM p0_forward_outcomes')[0]
        self.assertIsNone(row['return_pct'])
        self.assertIsNone(row['mfe_pct'])
        self.assertEqual('LATE_HORIZON',row['missing_reason'])

    def test_null_missingness_frozen_and_ignition_isolated(self):
        f=dict(price=100,chg5=1,chg60=.7,chg30=.4,buy30=.65,flow30=3,
               candidate_runup=.3,rel30=.3,qv24=10000000,spread=.02,
               trade_event_ts_ms=99999,trade_received_ts_ms=99999,
               book_event_ts_ms=99999,book_received_ts_ms=99999)
        with patch('urllib.request.urlopen',side_effect=AssertionError('network forbidden')):
            self.engine.stage('stage:1','X','CANDIDATE',100000,f,1)
            self.engine.stage('stage:2','X','EARLY',100000,f,1)
        f['oi5']=9
        self.engine.stage('stage:1','X','CANDIDATE',100001,f,1)
        snap=json.loads(self.rows('SELECT snapshot_json FROM p0_feature_snapshots WHERE source_key=?',('stage:1',))[0]['snapshot_json'])
        self.assertIsNone(snap['values']['oi5'])
        self.assertEqual('NOT_OBSERVED_AT_DECISION',snap['missingness']['oi5'])
        self.assertEqual(1,len(self.rows("SELECT * FROM p0_forward WHERE layer='IGNITION'")))
        tree=ast.parse(Path(p0.__file__).read_text())
        imports={n.names[0].name for n in ast.walk(tree) if isinstance(n,ast.Import)}
        self.assertFalse(imports & {'bot','aiohttp','runner_shadow_v1','telegram_ux','x_watcher'})

    def test_restart_stale_late_and_missing_are_separate(self):
        self.engine.arm('x','X','FAST',100000,100)
        self.engine.tick('X',100,100001,100001,101,90000,90000)
        self.engine.tick('X',100,100000,104000)
        self.engine.tick('X',100,100002,104000)
        recovered=p0.Telemetry(self.connect)
        recovered.expire(3800000)
        kinds={r['kind'] for r in self.rows('SELECT kind FROM p0_gap_events')}
        self.assertTrue({'STALE_ASK','LATE_EVENT','RESTART_GAP','MISSING_HORIZON'}<=kinds)
        self.assertEqual(5,len(self.rows('SELECT * FROM p0_forward_outcomes')))
        self.assertEqual('NEXT_TRADE_PROXY',self.rows('SELECT fill_source FROM p0_forward')[0]['fill_source'])

    def test_migration_preserves_existing_rows(self):
        self.engine.arm('x','X','FAST',100000,100)
        before=self.rows('SELECT * FROM p0_forward')
        with p0.connection(self.connect) as c:
            p0.migrate(c);p0.migrate(c)
        self.assertEqual(before,self.rows('SELECT * FROM p0_forward'))

    def test_report_dedup_without_outcome_selection_and_missing_denominator(self):
        self.engine.arm('a','X','FAST',100000,100,1,'parent')
        self.engine.arm('b','X','FAST',100001,100,2,'parent')
        self.engine.expire(4000000)
        with p0.connection(self.connect) as c:
            result=telemetry_report.report(c,1,5000000,'parent')
        self.assertEqual(1,result['layers']['FAST']['decisions'])
        self.assertEqual(1,result['layers']['FAST']['gap_rate'])
        self.assertIsNone(result['layers']['FAST']['good_recall_proxy'])

    def test_all_five_horizons_and_no_postdeadline_peak(self):
        self.engine.arm('x','X','FAST',100000,100)
        self.engine.tick('X',100,100001,100001,100,100001,100001)
        # Every second is observed, with a large jump just AFTER the first deadline.
        for offset in range(1000,3600001,1000):
            self.engine.tick('X',105 if offset>60000 else 100,100000+offset,100000+offset)
        outcomes=self.rows('SELECT * FROM p0_forward_outcomes ORDER BY horizon_s')
        self.assertEqual(list(p0.HORIZONS),[r['horizon_s'] for r in outcomes])
        self.assertEqual(0,outcomes[0]['mfe_pct'])
        self.assertAlmostEqual(5,outcomes[-1]['mfe_pct'])
        self.assertFalse(any(r['gap'] for r in outcomes))


class SafetyTests(unittest.TestCase):
    def test_production_logic_and_live_locks_unchanged_from_deployed_base(self):
        root=Path(__file__).resolve().parents[1]
        baseline=json.loads((root/'tests/p0_production_baseline.json').read_text())
        tree=ast.parse((root/'binance_momentum_bot/bot.py').read_text(encoding='utf-8'))
        functions={n.name:n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))}
        # Only the audit wrapper is permitted; handler body remains exactly the same.
        handler=functions['autotrade_handle_premium']
        self.assertEqual(['telemetry_p0.audit_premium'],[ast.unparse(n) for n in handler.decorator_list])
        handler.decorator_list=[]
        constants={n.targets[0].id:n for n in tree.body if isinstance(n,ast.Assign) and len(n.targets)==1 and isinstance(n.targets[0],ast.Name)}
        for category,current in (('functions',functions),('constants',constants)):
            for name,expected in baseline[category].items():
                self.assertEqual(expected,hashlib.sha256(ast.dump(current[name],include_attributes=False).encode()).hexdigest(),name)
        for name,expected in baseline['files'].items():
            raw=(root/name).read_bytes().replace(b'\r\n',b'\n')
            self.assertEqual(expected,hashlib.sha256(raw).hexdigest(),name)


class BotIntegrationTests(unittest.TestCase):
    def setUp(self):
        from test_v5135 import DatabaseCase
        DatabaseCase.setUp(self)
        self.addCleanup(gc.collect)

    def rows(self,sql,args=()):
        from test_v5135 import DatabaseCase
        return DatabaseCase.rows(self,sql,args)

    def test_off_audit_no_orders_or_notifications(self):
        from test_v5135 import bot
        bot.autotrade_cfg['mode']='OFF'
        with patch.object(bot,'telegram_send',new=AsyncMock()) as notify,patch.object(bot,'_at_place_market_entry',new=AsyncMock()) as order:
            asyncio.run(bot.autotrade_handle_premium(None,302,'BTWUSDT',{},{}))
            notify.assert_not_called();order.assert_not_called()
        events=self.rows('SELECT * FROM p0_autotrade_decisions')
        self.assertEqual(['DECISION_START','SKIP_MODE_OFF','DECISION_END'],[r['event'] for r in events])
        self.assertEqual(1,len({r['chain_id'] for r in events}))
        self.assertTrue(all(r['signal_id']==302 and r['decision_ts_ms']>0 for r in events))

    def test_rejects_keep_original_reasons(self):
        from test_v5135 import bot
        for reason in ('MAX_OPEN_POSITIONS','DAILY_MAX_LOSS','COOLDOWN','MAX_CONSECUTIVE_STOPS'):
            with patch.object(bot,'_at_risk_allowed',return_value=(False,reason)):
                asyncio.run(bot.autotrade_handle_premium(None,1,'X',{},{}))
        reasons=[json.loads(r['detail_json'])['reason'] for r in self.rows("SELECT detail_json FROM p0_autotrade_decisions WHERE event='ENTRY_BLOCKED'")]
        self.assertEqual(['MAX_OPEN_POSITIONS','DAILY_MAX_LOSS','COOLDOWN','MAX_CONSECUTIVE_STOPS'],reasons)

    def test_duplicate_and_slippage(self):
        from test_v5135 import bot
        with patch.object(bot,'_at_risk_allowed',return_value=(True,'')):
            bot.autotrade_active_by_symbol['X']={1}
            asyncio.run(bot.autotrade_handle_premium(None,1,'X',{},{}))
            bot.autotrade_active_by_symbol.clear()
            bot.states['X'].ask_price=110
            asyncio.run(bot.autotrade_handle_premium(None,2,'X',{'price':100},{}))
        reasons=[json.loads(r['detail_json'])['reason'] for r in self.rows("SELECT detail_json FROM p0_autotrade_decisions WHERE event='ENTRY_BLOCKED'")]
        self.assertEqual('BOT_POSITION_ALREADY_ACTIVE',reasons[0])
        self.assertTrue(reasons[1].startswith('SLIPPAGE'))

    def test_audit_failure_does_not_change_off_behavior(self):
        from test_v5135 import bot
        bot.autotrade_cfg['mode']='OFF'
        with patch.object(p0,'decision',side_effect=RuntimeError('audit unavailable')):
            asyncio.run(bot.autotrade_handle_premium(None,1,'X',{},{}))

    def test_runner_allow_hook_and_next_tick_only(self):
        from test_v5135 import bot
        from runner_shadow_v1 import Watch
        import time
        now=int(time.time()*1000)
        w=Watch('w',1,'X','FAST',now-15000,90,90,now+100000,now+3600000,'WATCH')
        with patch.object(self.measure.runner.notifier,'send',return_value=False):
            self.measure.runner._allow(w,now,100,[],{'age_s':15,'return_pct':1,'mfe_pct':1,'mae_pct':0})
        self.assertIsNone(self.rows('SELECT * FROM p0_forward')[0]['fill_price'])
        self.measure.tick('X',101,now+1,now+1,telemetry_ask=102,book_event_ms=now+1,book_received_ms=now+1)
        self.assertEqual(102,self.rows('SELECT * FROM p0_forward')[0]['fill_price'])

    def test_candidate_feature_copy_no_production_mutation(self):
        from test_v5135 import bot
        m={'price':100}
        bot._arm_stage_entry('X','CANDIDATE',m,1,levels={'stop':99,'tp1':101,'tp2':102})
        self.assertEqual({'price':100},m)
        snap=json.loads(self.rows('SELECT snapshot_json FROM p0_feature_snapshots')[0]['snapshot_json'])
        self.assertIsNone(snap['values']['rank_velocity'])
        self.assertIsNone(snap['values']['oi5'])


if __name__=='__main__':
    unittest.main()
