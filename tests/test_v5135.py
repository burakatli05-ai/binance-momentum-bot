import ast
import asyncio
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch
import zipfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'binance_momentum_bot'))
# Tests never read operator dotenv/secrets or start main/network loops.
os.environ['PYTHON_DOTENV_DISABLED']='1'
os.environ['AUTO_TRADE_LIVE_ALLOWED']='1'  # even hostile inherited env must stay locked
os.environ['AUTO_TRADE_BOOT_MODE']='LIVE'
import bot
import research_v5135 as audit
from position_observer import PositionObserver,LeverageCache,roe_values
import x_watcher as xmod


class DatabaseCase(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.old_db=bot.DB_PATH
        bot.DB_PATH=str(Path(self.tmp.name)/'test.db')
        self.addCleanup(setattr,bot,'DB_PATH',self.old_db)
        bot.init_db()
        bot.autotrade_active.clear(); bot.autotrade_active_by_symbol.clear()
        self.cfg=bot.autotrade_cfg.copy()
        self.addCleanup(bot.autotrade_cfg.update,self.cfg)
        bot.autotrade_cfg.update(mode='DRY',daily_max_loss_pct=3,max_open_positions=3,trade_margin_usdt=200,leverage=10,exit_profile='CURRENT_TP2')
        bot.states.clear(); bot.pending_stage_entries.clear(); bot.pending_research.clear()
        self.measure=audit.Measurements(bot.db_connect)
        self.old_measure=bot.measurements
        bot.measurements=self.measure
        self.addCleanup(setattr,bot,'measurements',self.old_measure)

    def rows(self,sql,params=()):
        c=bot.db_connect()
        try:
            c.row_factory=sqlite3.Row
            return [dict(x) for x in c.execute(sql,params)]
        finally: c.close()

    def sql(self,sql,params=()):
        c=bot.db_connect()
        try: c.execute(sql,params); c.commit()
        finally: c.close()

    def trade(self,signal=1):
        return bot._at_insert_trade(signal,'BTCUSDT','DRY',100,100,20,dict(stop=99,tp1=101,tp2=102,runner=105),{})

    def test_migration_idempotent_preserves_history(self):
        tid=self.trade()
        self.sql("UPDATE autotrade_trades SET cost_model_version=NULL,fee_pct_per_side=NULL,net_pnl=7.3 WHERE id=?",(tid,))
        before=self.rows('SELECT * FROM autotrade_trades')
        bot.init_db(); bot.init_db()
        self.assertEqual(before,self.rows('SELECT * FROM autotrade_trades'))
        self.assertEqual(1,len(self.rows('SELECT * FROM measurement_migrations')))

    def test_live_locked_regardless_env(self):
        self.assertFalse(bot.AUTO_TRADE_LIVE_ALLOWED)
        self.assertEqual('OFF',bot.AUTO_TRADE_BOOT_MODE)

    def test_dry_costs_and_daily_net(self):
        tid=self.trade()
        bot.autotrade_on_tick('BTCUSDT',99,time.time())
        tr=self.rows('SELECT * FROM autotrade_trades WHERE id=?',(tid,))[0]
        self.assertAlmostEqual(-20,tr['gross_pnl'])
        self.assertAlmostEqual(1.99,tr['commission'])
        self.assertAlmostEqual(.796,tr['slippage_cost'])
        self.assertAlmostEqual(-22.786,tr['net_pnl'])
        self.assertAlmostEqual(tr['net_pnl'],bot._at_daily_row(scope='DRY')['realized_net_pnl'])
        bot._at_close_trade(tid,'STOP',99,-20)
        self.assertAlmostEqual(tr['net_pnl'],bot._at_daily_row(scope='DRY')['realized_net_pnl'])

    def test_partial_exit_costs_all_legs(self):
        bot.autotrade_cfg['exit_profile']='PARTIAL_RUNNER'
        tid=self.trade()
        bot.autotrade_on_tick('BTCUSDT',101,time.time())
        bot.autotrade_on_tick('BTCUSDT',99,time.time())
        tr=self.rows('SELECT * FROM autotrade_trades WHERE id=?',(tid,))[0]
        self.assertAlmostEqual(0,tr['gross_pnl'])
        self.assertAlmostEqual(-2.8,tr['net_pnl'])

    def test_cost_settings_frozen_across_restart(self):
        tid=self.trade()
        bot.autotrade_active.clear(); bot.recover_autotrade_active()
        with patch.object(audit,'DRY_FEE_PCT',9),patch.object(audit,'DRY_SLIPPAGE_PCT',9):
            bot._at_close_trade(tid,'STOP',99,-20)
        self.assertAlmostEqual(-22.786,self.rows('SELECT net_pnl FROM autotrade_trades')[0]['net_pnl'])

    def test_risk_sum_boundary_and_open_reconstruction(self):
        bot._at_daily_row(2000,'DRY')
        self.sql("UPDATE autotrade_daily SET realized_net_pnl=-10 WHERE scope='DRY'")
        tid=self.trade()
        risk=bot._at_trade_worst_case_risk_usdt(bot.autotrade_active[tid])
        self.assertAlmostEqual(22.786,risk)
        self.assertTrue(bot._at_risk_allowed('DRY',50-risk)[0])
        self.assertFalse(bot._at_risk_allowed('DRY',50-risk+.01)[0])
        bot.autotrade_active.clear();bot.recover_autotrade_active()
        self.assertFalse(bot._at_risk_allowed('DRY',50-risk+.01)[0])

    def test_restart_repairs_crash_between_trade_and_ledger(self):
        tid=self.trade();bot._at_close_trade(tid,'STOP',99,-20)
        self.sql("UPDATE autotrade_daily SET realized_net_pnl=0 WHERE scope='DRY'")
        bot.load_autotrade_settings()
        self.assertAlmostEqual(-22.786,bot._at_daily_row(scope='DRY')['realized_net_pnl'])
        bot._at_repair_daily_from_trade_history('DRY')
        self.assertAlmostEqual(-22.786,bot._at_daily_row(scope='DRY')['realized_net_pnl'])

    def test_day_boundary_open_risk_survives(self):
        self.trade()
        with patch.object(bot,'_at_local_date',return_value='2099-01-02'):
            r=bot._at_daily_row(scope='DRY')
            self.assertEqual(0,r['realized_net_pnl'])
            self.assertGreater(bot._at_open_worst_case_risk('DRY'),22)

    def test_unknown_risk_fails_closed(self):
        self.assertEqual(float('inf'),bot._at_trade_worst_case_risk_usdt({}))

    def test_fatigue_per_episode_idempotent(self):
        for sid in (1,2,3):self.measure.fatigue(sid,'BTCUSDT',100)
        self.measure.fatigue(3,'BTCUSDT',100)
        self.measure.fatigue(4,'BTCUSDT',101)
        self.assertEqual([1,2,3,1],[r['premium_ordinal'] for r in self.rows('SELECT * FROM premium_fatigue_shadow ORDER BY signal_id')])

    def test_actual_decision_then_next_tick_fill(self):
        now=bot.now_ms()
        cid=self.measure.arm('gate','BTCUSDT','GATE30',nominal_ms=now-30000)
        x=self.measure.active[cid];decision=x['decision_time_ms']
        self.measure.tick('BTCUSDT',100,decision-1,decision+1,100.1)
        self.assertEqual('ARMED',x['status'])
        self.measure.tick('BTCUSDT',101,decision+2,decision+3,101.1)
        row=self.rows('SELECT * FROM causal_cohorts')[0]
        self.assertEqual(101.1,row['first_executable_price'])
        self.assertGreater(row['fill_time_ms'],row['decision_time_ms'])
        self.assertGreater(row['decision_time_ms'],row['nominal_time_ms'])

    def test_stop_watch_reclaim_new_trade_and_combined_costs(self):
        cid=self.measure.arm('premium','BTCUSDT','PREMIUM',stop_pct=1,tp_pct=2)
        t=self.measure.active[cid]['decision_time_ms']+10
        self.measure.tick('BTCUSDT',100,t,t)
        self.measure.tick('BTCUSDT',99,t+1,t+1)
        child=next(x for x in self.measure.active.values() if x['stage']=='REENTRY')
        self.assertEqual('WATCH',child['status'])
        t=max(t,child['decision_time_ms']+10)
        self.measure.tick('BTCUSDT',100,t+10,t+10)
        self.measure.tick('BTCUSDT',100,t+6010,t+6010)
        self.assertEqual('ARMED',child['status'])
        self.measure.tick('BTCUSDT',100,t+6011,t+6011)
        self.measure.tick('BTCUSDT',102,t+6012,t+6012)
        rows=self.rows('SELECT * FROM causal_cohorts ORDER BY parent_id')
        self.assertNotEqual(rows[0]['id'],rows[1]['id'])
        self.assertAlmostEqual(sum(r['net_pnl_pct'] for r in rows),rows[1]['combined_net_pnl_pct'])
        self.assertTrue(all(r['fees_pct']>0 and r['slippage_pct']>0 for r in rows))
        self.assertEqual(0,len(self.rows('SELECT * FROM autotrade_trades')))

    def test_restart_cohort_marks_gap(self):
        cid=self.measure.arm('gate','BTCUSDT','GATE15')
        restored=audit.Measurements(bot.db_connect)
        self.assertEqual(1,restored.active[cid]['observation_gap'])

    def test_liquidity_moving_book_separates_reference(self):
        book={'bids':[['101.9','10'],['101.8','5']],'asks':[['102.1','10'],['102.2','5']]}
        anchor=bot.analyze_depth_snapshot('BTCUSDT',book,100,101,{})
        mid=bot.analyze_depth_snapshot('BTCUSDT',book,102,103.02,{})
        self.assertEqual(0,anchor['ask_025'])
        self.assertGreater(mid['ask_025'],1000)
        self.assertAlmostEqual(102,mid['mid_price'])

    def test_backup_restores_without_source_mutation(self):
        self.trade()
        before=self.rows('SELECT * FROM autotrade_trades')
        z,health=bot.create_consistent_db_backup()
        with zipfile.ZipFile(z) as f:
            manifest=json.loads(f.read('manifest.json'))
            self.assertEqual(hashlib.sha256(f.read('signals.db')).hexdigest(),manifest['sha256'])
            self.assertEqual(['ok'],manifest['restore_smoke_check'])
            self.assertTrue(manifest['restore_row_counts_match'])
            self.assertEqual(1,manifest['tables']['autotrade_trades']['rows'])
        self.assertEqual(before,self.rows('SELECT * FROM autotrade_trades'))

    def test_trend_logging_not_throttled_by_notify(self):
        m={'symbol':'BTCUSDT','price':100}
        st=bot.states['BTCUSDT'];st.trend_build_last_notify=time.time()
        with patch.object(bot,'_trend_build_score',return_value=(100,{},[])),patch.object(bot,'get_oi_context',new=AsyncMock(return_value=(None,None,None))),patch.object(bot,'telegram_send',new=AsyncMock()) as send:
            for n in range(6):
                asyncio.run(bot.maybe_trend_build_up(None,'BTCUSDT',m,90,time.time()+n*10))
            self.assertEqual(2,len(self.rows("SELECT * FROM research_events WHERE event_type='TREND_BUILDUP'")))
            send.assert_not_awaited()

    def test_observer_unknown_and_event_lifecycle(self):
        positions=[{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'1','entryPrice':'100','markPrice':'101','unRealizedProfit':'1'}]
        async def request(session,method,path):
            return positions if 'positionRisk' in path else []
        send=AsyncMock(return_value=False)
        po=PositionObserver(bot.db_connect,request,send,lambda *_:'MANUAL',bot._po_zone,[5,10,20],1)
        asyncio.run(po.poll(None))
        state=self.rows('SELECT * FROM position_observer_state')[0]
        self.assertIsNone(state['leverage']);self.assertIsNone(state['last_roe'])
        positions[0]['positionInitialMargin']='10'
        asyncio.run(po.poll(None))
        events=self.rows("SELECT * FROM position_observer_events WHERE event='ROE_PROFIT'")
        self.assertEqual('FAILED',events[0]['notification_delivery'])
        self.assertEqual('positionInitialMargin',events[0]['margin_source'])
        positions.clear();asyncio.run(po.poll(None))
        self.assertEqual('CLOSE_OBSERVED',self.rows('SELECT * FROM position_observer_events ORDER BY id DESC')[0]['event'])


class PureTests(unittest.TestCase):
    def test_roe_margin_priority_and_unknown(self):
        self.assertEqual((10,1,'positionInitialMargin'),roe_values({'unRealizedProfit':1,'positionInitialMargin':10},'LONG',100,101,None))
        self.assertIsNone(roe_values({},'LONG',100,101,None)[0])
        self.assertAlmostEqual(10,roe_values({},'SHORT',100,99,10)[0])

    def test_leverage_cache_only_correct_endpoint(self):
        req=AsyncMock(return_value=[{'symbol':'BTCUSDT','leverage':20}])
        cache=LeverageCache()
        asyncio.run(cache.refresh(None,req));asyncio.run(cache.refresh(None,req))
        self.assertEqual((20,'symbolConfig'),cache.get('BTCUSDT'))
        req.assert_awaited_once_with(None,'GET','/fapi/v1/symbolConfig')
        cache.expires=0;req.side_effect=RuntimeError()
        asyncio.run(cache.refresh(None,req))
        self.assertEqual((None,'UNKNOWN'),cache.get('BTCUSDT'))

    def test_x_results_are_not_setups(self):
        for text in ('$BTC target hit +51%','$BTC +%51 yaptı','$BTC TP2 hit accuracy 90%'):
            a=xmod.classify(text,{'BTCUSDT'})
            self.assertEqual('RESULT_UPDATE',a['category']);self.assertIsNone(a['condition'])

    def test_x_conditional_and_ambiguous(self):
        a=xmod.classify('$BTC wait for breakout above 100',{'BTCUSDT'})
        self.assertEqual('WATCH_SETUP',a['category'])
        self.assertEqual({'operator':'CROSS_ABOVE','price':100},a['condition'])
        a=xmod.classify('$BTC wait reclaim, do not buy market',{'BTCUSDT'})
        self.assertIsNone(a['condition'])
        self.assertIsNone(xmod.classify('$BTC $ETH above 100',{'BTCUSDT','ETHUSDT'})['symbol'])


class XTests(unittest.TestCase):
    setUp=DatabaseCase.setUp
    rows=DatabaseCase.rows
    sql=DatabaseCase.sql
    def test_bootstrap_new_tweet_delivery_and_trigger(self):
        price={'price':99,'chg5':1,'chg24':2}
        send=AsyncMock(return_value=True);media=AsyncMock(return_value=True)
        watcher=xmod.XWatcher(bot.db_connect,lambda symbol:price if symbol else {'BTCUSDT'},send,media)
        watcher.request=AsyncMock(side_effect=[{'data':{'id':'42'}},{'data':[{'id':'1','text':'$BTC old setup'}]}, {'data':[{'id':'2','text':'$BTC wait breakout above 100'}]}])
        asyncio.run(watcher.poll_account(None,'chartexpt'));asyncio.run(watcher.process(None))
        send.assert_not_awaited()
        asyncio.run(watcher.poll_account(None,'chartexpt'));asyncio.run(watcher.process(None))
        self.assertEqual(1,send.await_count)
        price['price']=101
        asyncio.run(watcher.process(None));asyncio.run(watcher.process(None));asyncio.run(watcher.process(None))
        self.assertEqual(2,send.await_count)
        self.assertIn('X SHADOW TETİK',send.call_args.args[1])
        self.assertEqual('DELIVERED',self.rows("SELECT trigger_delivery FROM x_watcher_tweets WHERE tweet_id='2'")[0]['trigger_delivery'])


if __name__=='__main__':unittest.main()
