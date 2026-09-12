import ast
import asyncio
import hashlib
import json
from pathlib import Path
import time
import unittest
from unittest.mock import AsyncMock,patch
import test_v5135 as fixtures
from test_v5135 import bot,audit,ROOT,xmod
from position_observer import PositionObserver


class IntegrationTests(unittest.TestCase):
    setUp=fixtures.DatabaseCase.setUp
    rows=fixtures.DatabaseCase.rows
    sql=fixtures.DatabaseCase.sql
    trade=fixtures.DatabaseCase.trade

    def test_production_ast_baseline(self):
        baseline=json.loads((ROOT/'tests/production_baseline.json').read_text())
        tree=ast.parse((ROOT/'binance_momentum_bot/bot.py').read_text(encoding='utf-8'))
        functions={n.name:n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))}
        constants={n.targets[0].id:n for n in tree.body if isinstance(n,ast.Assign) and len(n.targets)==1 and isinstance(n.targets[0],ast.Name)}
        for group,current in (('functions',functions),('constants',constants)):
            for name,digest in baseline[group].items():
                self.assertEqual(digest,hashlib.sha256(ast.dump(current[name],include_attributes=False).encode()).hexdigest(),name)

    def test_stage_integration_has_causal_link_without_backdated_fill(self):
        created=time.time()-30
        bot._arm_stage_entry('BTCUSDT','GATE30',{'price':100},1,created_ts=created,signal_id=1,levels=dict(stop=99,tp1=101,tp2=102))
        stage=self.rows('SELECT * FROM entry_stage_forward_shadow')[0]
        causal=self.rows('SELECT * FROM causal_cohorts')[0]
        self.assertEqual(stage['causal_cohort_id'],causal['id'])
        self.assertGreater(stage['decision_time_ms'],stage['nominal_time_ms'])
        self.assertIsNone(causal['fill_time_ms'])

    def test_depth_capture_persists_two_references_and_v3(self):
        p=bot.PendingOutcome(1,'BTCUSDT',100,time.time()-30,target1=101,target2=102,invalidation=99)
        book={'bids':[['101.9','10'],['101.8','5']],'asks':[['102.1','10'],['102.2','5']]}
        with patch.object(bot,'fetch_json',new=AsyncMock(return_value=book)),patch.object(bot,'compute_metrics',return_value={}),patch.object(bot,'save_post_premium_risk'),patch.object(bot,'update_gate_recovery_30'),patch.object(bot,'maybe_finalize_execution_composite'):
            asyncio.run(bot.capture_liquidity_snapshot(None,p,30000))
        row=self.rows('SELECT * FROM premium_liquidity_snapshots')[0]
        self.assertEqual('INITIAL_ANCHOR',row['reference_kind'])
        self.assertEqual(0,row['ask_025'])
        self.assertGreater(json.loads(row['current_mid_metrics_json'])['ask_025'],0)
        v3=self.rows('SELECT * FROM premium_liquidity_transition_v3')[0]
        self.assertIsNotNone(v3['causal_cohort_id'])
        self.assertEqual('LIQ_V3',self.rows('SELECT stage FROM causal_cohorts')[0]['stage'])

    def test_research_failure_does_not_escape_to_production(self):
        with patch.object(bot.measurements,'fatigue',side_effect=RuntimeError('test')):
            self.assertIsNone(bot.measurement_call('fatigue',1,'BTCUSDT',1))

    def test_risk_nonfinite_rejected(self):
        self.assertFalse(bot._at_risk_allowed('DRY',float('nan'))[0])
        self.assertFalse(bot._at_risk_allowed('DRY',float('inf'))[0])
        self.assertEqual(float('inf'),bot._at_trade_worst_case_risk_usdt(dict(entry_price=float('nan'),stop_price=99,qty=1)))

    def test_cohort_timeout_without_ticks_has_no_fake_pnl(self):
        cid=self.measure.arm('dead','BTCUSDT','GATE15')
        self.measure.expire(self.measure.active[cid]['deadline_ms']+1)
        row=self.rows('SELECT * FROM causal_cohorts')[0]
        self.assertEqual('NO_FILL_TIMEOUT',row['first_event'])
        self.assertIsNone(row['net_pnl_pct'])
        self.assertNotIn(cid,self.measure.active)

    def test_reclaim_persistence_resets_across_restart(self):
        cid=self.measure.arm('p','BTCUSDT','PREMIUM',stop_pct=1,tp_pct=2)
        t=self.measure.active[cid]['decision_time_ms']+100
        self.measure.tick('BTCUSDT',100,t,t);self.measure.tick('BTCUSDT',99,t+1,t+1)
        child=next(iter(self.measure.active.values()))
        t=max(t+100,child['decision_time_ms']+100)
        self.measure.tick('BTCUSDT',100,t,t)
        self.assertIsNotNone(child['reclaim_since_ms'])
        restored=audit.Measurements(bot.db_connect)
        self.assertIsNone(restored.active[child['id']]['reclaim_since_ms'])

    def test_tick_extrema_batch_preserves_first_touch(self):
        cid=self.measure.arm('p','BTCUSDT','PREMIUM',stop_pct=1,tp_pct=2)
        t=self.measure.active[cid]['decision_time_ms']+100
        self.measure.tick('BTCUSDT',100,t,t)
        for i in range(1,50):self.measure.tick('BTCUSDT',100.5,t+i,t+i)
        self.measure.tick('BTCUSDT',99,t+50,t+50)
        row=self.rows('SELECT * FROM causal_cohorts WHERE id=?',(cid,))[0]
        self.assertEqual('STOP',row['first_event'])
        self.assertAlmostEqual(.5,row['mfe_pct'])
        self.assertEqual(t+50,row['first_event_time_ms'])

    def test_observer_basis_reset_creates_new_identity(self):
        pos=[dict(symbol='BTCUSDT',positionSide='LONG',positionAmt=1,entryPrice=100,markPrice=101,unRealizedProfit=1,positionInitialMargin=10)]
        async def request(session,method,path):
            return pos if 'positionRisk' in path else [dict(symbol='BTCUSDT',leverage=20)]
        po=PositionObserver(bot.db_connect,request,AsyncMock(return_value=True),lambda *_:'MANUAL',bot._po_zone,[5,10,20],1)
        asyncio.run(po.poll(None));first=self.rows('SELECT position_instance_id FROM position_observer_state')[0]['position_instance_id']
        pos[0]['entryPrice']=99
        asyncio.run(po.poll(None));second=self.rows('SELECT position_instance_id FROM position_observer_state')[0]['position_instance_id']
        self.assertNotEqual(first,second)
        self.assertEqual(1,len(self.rows("SELECT * FROM position_observer_events WHERE event='RESET'")))
        self.assertEqual('symbolConfig',self.rows('SELECT leverage_source FROM position_observer_state')[0]['leverage_source'])

    def test_x_photos_limit_and_delivery_retry(self):
        send=AsyncMock(side_effect=[False,True]);photo=AsyncMock(return_value=True)
        watcher=xmod.XWatcher(bot.db_connect,lambda s: {'price':100} if s else {'BTCUSDT'},send,photo)
        self.sql("INSERT INTO x_watcher_accounts VALUES ('chartexpt','42','1',1)")
        watcher.request=AsyncMock(return_value={'data':[{'id':'2','text':'$BTC market update','attachments':{'media_keys':['a','b','c']}}], 'includes':{'media':[{'media_key':k,'type':'photo','url':'https://pbs.twimg.com/'+k} for k in 'abc']}})
        asyncio.run(watcher.poll_account(None,'chartexpt'))
        asyncio.run(watcher.process(None));asyncio.run(watcher.process(None))
        self.assertEqual(2,photo.await_count)
        self.assertEqual(2,send.await_count)
        self.assertEqual('DELIVERED',self.rows('SELECT delivery FROM x_watcher_tweets')[0]['delivery'])

    def test_x_partial_api_failure_does_not_advance_cursor(self):
        watcher=xmod.XWatcher(bot.db_connect,lambda _:set(),AsyncMock(),AsyncMock())
        self.sql("INSERT INTO x_watcher_accounts VALUES ('chartexpt','42','1',1)")
        watcher.request=AsyncMock(side_effect=[{'data':[{'id':'2','text':'hello'}],'meta':{'next_token':'n'}},RuntimeError('rate limit')])
        with self.assertRaises(RuntimeError):asyncio.run(watcher.poll_account(None,'chartexpt'))
        self.assertEqual('1',self.rows('SELECT since_id FROM x_watcher_accounts')[0]['since_id'])
        self.assertEqual([],self.rows('SELECT * FROM x_watcher_tweets'))

    def test_x_multiple_numeric_conditions_stays_manual_watch(self):
        result=xmod.classify('$BTC wait above 100 or below 90',{'BTCUSDT'})
        self.assertEqual('WATCH_SETUP',result['category']);self.assertEqual('$BTC wait above 100 or below 90',result['condition'])

    def test_real_legacy_schema_migrates_twice_without_repricing(self):
        old_db=bot.DB_PATH
        bot.DB_PATH=str(Path(self.tmp.name)/'legacy_fixture.db')
        try:
            c=bot.db_connect()
            c.executescript((ROOT/'tests/legacy_schema_v5134.sql').read_text(encoding='utf-8'))
            c.execute("INSERT INTO autotrade_trades(signal_id,symbol,mode,status,margin_usdt,leverage,notional_usdt,net_pnl,commission,updated_ts) VALUES (1,'BTCUSDT','DRY','CLOSED',200,10,2000,7.3,0,1)")
            c.commit();c.close()
            bot.init_db();bot.init_db()
            row=self.rows('SELECT * FROM autotrade_trades')[0]
            self.assertEqual(7.3,row['net_pnl'])
            self.assertEqual(0,row['commission'])
            self.assertIsNone(row['cost_model_version'])
            self.assertIsNone(row['gross_pnl'])
        finally:bot.DB_PATH=old_db
