import ast
import asyncio
from contextlib import closing
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import unittest
from unittest.mock import AsyncMock,patch

import test_v5135 as fixtures
import alt_shadow as alt
import research_export as export
import research_reports as reports
import daytrades
from position_observer import PositionObserver,render_card

bot=fixtures.bot


class AltCase(unittest.TestCase):
    setUp=fixtures.DatabaseCase.setUp
    rows=fixtures.DatabaseCase.rows
    sql=fixtures.DatabaseCase.sql

    def engine(self,balance=2000):return alt.ShadowEngine(bot.db_connect,balance)

    def premium(self,e,sid=1,ts=None,age=20,symbol='BTCUSDT'):
        ts=ts or int(time.time()*1000)
        e.on_premium(dict(id=sid,symbol=symbol,ts=ts/1000,price=100,episode_id=1,candidate_to_premium_age=age),now=ts)
        return ts

    def tick(self,e,price,ts,symbol='BTCUSDT'):
        e.step(now=ts,events=[(symbol,price,ts,ts,None)])

    def test_models_idempotent_restart_frozen_first_touch(self):
        e=self.engine();t=self.premium(e);self.premium(e,ts=t)
        self.assertEqual(2,len(self.rows('SELECT * FROM alt_research_trades')))
        self.tick(e,100,t+1);e=self.engine()
        with patch.object(alt.cost_model,'DRY_FEE_PCT',9):self.tick(e,106,t+2)
        self.tick(e,90,t+3)
        data=self.rows('SELECT * FROM alt_research_trades ORDER BY model_name')
        for r in data:
            self.assertEqual('TP',r['terminal_reason']);self.assertEqual(1,r['observation_gap'])
            self.assertAlmostEqual(r['notional']*.06,r['gross_pnl'])
            self.assertAlmostEqual(r['notional']*2.06*.0005,r['fees'])
            self.assertAlmostEqual(r['gross_pnl']-r['fees']-r['slippage_cost'],r['net_pnl'])
        self.assertEqual(2,len(self.rows("SELECT * FROM alt_lifecycle_events WHERE event='TP'")))

    def test_stop_geometry_and_time_exit(self):
        e=self.engine();t=self.premium(e);self.tick(e,100,t+1);self.tick(e,96.9,t+2)
        data={r['model_name']:r for r in self.rows('SELECT * FROM alt_research_trades')}
        self.assertEqual('SL',data['ALT_CONTROL_60']['terminal_reason'])
        self.assertEqual('OPEN',data['ALT_WIDE_60']['lifecycle_status'])
        self.tick(e,102,t+3600001)
        wide=self.rows("SELECT * FROM alt_research_trades WHERE model_name='ALT_WIDE_60'")[0]
        self.assertEqual('TIME_EXIT',wide['terminal_reason']);self.assertAlmostEqual(8,wide['gross_pnl'])

    def test_missing_time_exit_never_fabricates(self):
        e=self.engine();t=self.premium(e);self.tick(e,100,t+1)
        e.step(now=t+3606000)
        for r in self.rows('SELECT * FROM alt_research_trades'):
            self.assertEqual('UNPRICED_TIMEOUT',r['lifecycle_status']);self.assertIsNone(r['net_pnl'])
        self.premium(e,2,t+3607000)
        self.assertEqual({'UNPRICED_RISK'},{r['skip_reason'] for r in self.rows('SELECT * FROM alt_research_trades WHERE signal_id=2')})

    def test_capacity_independent_of_production(self):
        e=self.engine();t=int(time.time()*1000);before=dict(bot.autotrade_cfg)
        for sid in range(1,5):self.premium(e,sid,t+sid,symbol=f'S{sid}USDT')
        self.assertEqual(6,len(self.rows("SELECT * FROM alt_research_trades WHERE lifecycle_status='ARMED'")))
        self.assertEqual({'CAPACITY'},{r['skip_reason'] for r in self.rows('SELECT * FROM alt_research_trades WHERE signal_id=4')})
        self.assertEqual(before,bot.autotrade_cfg);self.assertFalse(bot.autotrade_active)
        self.assertFalse(self.rows('SELECT * FROM autotrade_daily'))

    def test_daily_risk_costs_and_reconstruction(self):
        e=self.engine(480);t=self.premium(e)
        data={r['model_name']:r for r in self.rows('SELECT * FROM alt_research_trades')}
        self.assertEqual('DAILY_RISK',data['ALT_WIDE_60']['skip_reason'])
        self.assertIsNone(data['ALT_CONTROL_60']['skip_reason'])
        self.tick(e,100,t+1);self.tick(e,96,t+2)
        e=self.engine(480);self.premium(e,2,t+3)
        self.assertEqual('DAILY_RISK',self.rows("SELECT skip_reason FROM alt_research_trades WHERE signal_id=2 AND model_name='ALT_CONTROL_60'")[0]['skip_reason'])

    def test_cooldown_cross_midnight_and_starting_balance(self):
        e=self.engine(10000);t=int(datetime(2026,9,12,23,59,tzinfo=alt.IST).timestamp()*1000)
        for sid in range(1,5):
            self.premium(e,sid,t+sid*10);self.tick(e,100,t+sid*10+1);self.tick(e,96,t+sid*10+2)
        e=self.engine(10000);self.premium(e,5,t+120000)
        self.assertEqual({'COOLDOWN'},{r['skip_reason'] for r in self.rows('SELECT * FROM alt_research_trades WHERE signal_id=5')})
        self.assertTrue(all(r['starting_balance']<10000 for r in self.rows("SELECT * FROM alt_daily WHERE local_date='2026-09-13'")))

    def test_age_diagnostic_and_sequence_as_known(self):
        e=self.engine();t=self.premium(e,age=46);self.premium(e,2,t+1000)
        self.assertEqual({'AGE_GT_45'},{r['skip_reason'] for r in self.rows('SELECT * FROM alt_research_trades WHERE signal_id=1')})
        r=self.rows('SELECT * FROM alt_premium_context WHERE signal_id=2')[0]
        self.assertEqual(2,r['premium_sequence_number']);self.assertEqual(1,r['time_since_previous_premium'])
        self.assertEqual('OPEN_OR_UNKNOWN',r['previous_outcome_at_decision'])

    def test_closed_candles_only_and_no_invented_history(self):
        candles=[(i*60000,100,101,99,100+i) for i in range(11)]
        r=alt.past_context(candles,600000)
        self.assertAlmostEqual(9,r['pre_candidate_10m_return'])
        self.assertEqual(540,r['trend_age_proxy_s'])
        self.assertIsNone(alt.past_context([],600000)['pre_candidate_3m_return'])

    def test_post_peak_returns_and_future_horizons(self):
        e=self.engine();t=self.premium(e);self.tick(e,100,t+1);self.tick(e,103,t+60000)
        self.tick(e,100,t+120000)
        ctx=self.rows('SELECT * FROM alt_premium_context')[0]
        self.assertAlmostEqual(3,json.loads(ctx['post_returns_json'])['60']['return_pct'])
        self.assertIn('EXHAUSTION_LIKE',json.loads(ctx['tags_json']))
        self.tick(e,101,t+1800001)
        self.assertEqual(2,len(self.rows("SELECT * FROM alt_forward_outcomes WHERE horizon_s=1800 AND status='OBSERVED'")))
        self.tick(e,102,t+7200001)
        self.assertEqual(2,len(self.rows("SELECT * FROM alt_forward_outcomes WHERE horizon_s=7200 AND status='OBSERVED'")))

    def test_ingest_forward_only_migration_and_context_view(self):
        self.sql("INSERT INTO signals_v2(id,ts,level,symbol,price,score) VALUES(1,1,'CONFIRMED','BTCUSDT',100,80)")
        e=self.engine();e.step(now=int(time.time()*1000))
        self.assertFalse(self.rows('SELECT * FROM alt_research_trades'))
        t=int(time.time())
        self.sql("INSERT INTO signals_v2(id,ts,level,symbol,price,score) VALUES(2,?,'CONFIRMED','BTCUSDT',100,80)",(t,))
        e.step(now=t*1000+1);bot.init_db();bot.init_db()
        self.assertEqual(2,len(self.rows('SELECT * FROM alt_research_trades')))
        self.assertEqual(1,len(self.rows('SELECT * FROM alt_linked_context')))

    def test_causal_features_frozen(self):
        f={'momentum_score':78,'chg_10s':1.2}
        cid=self.measure.arm('early-feature','BTCUSDT','EARLY',features=f)
        f['momentum_score']=100
        r=self.rows('SELECT config_json FROM causal_cohorts WHERE id=?',(cid,))[0]
        self.assertEqual(78,json.loads(r['config_json'])['decision_features']['momentum_score'])

    def test_worker_ingests_eligible_premium_and_fills_only_later(self):
        e=self.engine();t=int(time.time())*1000
        self.sql("INSERT INTO candidate_events(ts,symbol,event,price,score,episode_id,candidate_age_s) VALUES(?, 'BTCUSDT','candidate_start',100,80,5,0)",((t-20000)//1000,))
        self.sql("INSERT INTO candidate_events(ts,symbol,event,price,score,episode_id,candidate_age_s) VALUES(?, 'BTCUSDT','premium_signal',100,80,5,20)",(t//1000,))
        self.sql("INSERT INTO signals_v2(ts,symbol,level,price,score,episode_id) VALUES(?,'BTCUSDT','CONFIRMED',100,80,5)",(t//1000,))
        e.step(now=t+10)
        self.assertEqual({'ARMED'},{r['lifecycle_status'] for r in self.rows('SELECT * FROM alt_research_trades')})
        e.feed('BTCUSDT',100,t+11,t+11,100.01);e.step(now=t+12)
        for r in self.rows('SELECT * FROM alt_research_trades'):
            self.assertEqual('FRESH_ASK',r['fill_source']);self.assertEqual(20,r['candidate_to_premium_age'])
            self.assertLess(r['decision_ts'],r['fill_ts'])

    def test_no_late_fill_and_no_backfill_after_reenable(self):
        e=self.engine();t=self.premium(e);self.tick(e,100,t+31000)
        self.assertEqual({'NO_FRESH_FILL'},{r['skip_reason'] for r in self.rows('SELECT * FROM alt_research_trades')})
        self.assertTrue(all(r['fill_ts'] is None for r in self.rows('SELECT * FROM alt_research_trades')))

    def test_initial_balance_frozen_and_hourly_process_lease(self):
        e=self.engine(2000);self.assertEqual(2000,self.engine(9999).starting_balance)
        worker=export.Exporter(bot.DB_PATH,Path(self.tmp.name)/'exports');t=int(time.time()*1000)
        worker.run_due(t)
        with closing(sqlite3.connect(worker.root/'lease.sqlite')) as c,c:c.execute('UPDATE lease SET until_ms=?',(t+10000,))
        self.assertEqual('BUSY',worker.run_due(t+1)['status'])

    def test_timeout_buckets_do_not_discard_gap_outliers(self):
        e=self.engine();t=self.premium(e);self.tick(e,100,t+1);self.tick(e,110,t+3600001)
        with closing(bot.db_connect()) as c:report=reports.summary(c)
        for model in alt.MODELS:
            self.assertEqual(1,report[model]['timeout_return_buckets_pct']['outside'])
            self.assertEqual(1,report[model]['terminals']['TIME_EXIT'])

    def test_non_usdt_fees_not_fake_net(self):
        fill=dict(symbol='BTCUSDT',positionSide='LONG',orderId=7,time=1,side='SELL',realizedPnl='3',commission='.1',commissionAsset='BNB')
        r=daytrades.group_fills([fill],{},[])[0]
        self.assertIsNone(r['net_pnl']);self.assertEqual({'BNB':.1},r['other_fees'])

    def test_export_integrity_unchanged_bad_and_hourly_daily(self):
        worker=export.Exporter(bot.DB_PATH,Path(self.tmp.name)/'exports')
        t=int(datetime(2026,9,13,21,59,tzinfo=alt.IST).timestamp()*1000)
        r=worker.run_due(t);self.assertEqual('VALID',r['status']);self.assertEqual('NO_NEW_MATURE_DATA',r['summary'])
        self.assertEqual('NOT_DUE',worker.run_due(t+1)['status'])
        r=worker.run_due(t+60000);self.assertEqual('2026-09-13',r['daily'])
        self.assertNotIn('daily',worker.run_due(t+61000))
        r=worker.run_due(t+3600000);self.assertEqual('UNCHANGED',r['summary'])
        latest=worker.latest()[1]['snapshot_id']
        with patch.object(export,'inspect_snapshot',return_value={'valid':False,'quick_check':['bad'],'integrity_check':['bad']}):
            self.assertFalse(worker.snapshot(t+7200000)[1]['valid'])
        self.assertEqual(latest,worker.latest()[1]['snapshot_id'])
        self.assertTrue(worker.download_bundle().exists())

    def test_export_new_mature_summary_and_source_unchanged(self):
        worker=export.Exporter(bot.DB_PATH,Path(self.tmp.name)/'exports');e=self.engine()
        t=self.premium(e);self.tick(e,100,t+1);self.tick(e,106,t+2)
        before=Path(bot.DB_PATH).read_bytes();r=worker.run_due(t+3)
        self.assertEqual('NEW_MATURE_DATA',r['summary']);self.assertEqual(before,Path(bot.DB_PATH).read_bytes())
        info=worker.latest()[1]
        self.assertTrue(info['valid']);self.assertEqual(['ok'],info['integrity_check'])
        self.assertIn('alt_research_trades',info['tables'])

    def test_admin_commands_reject_without_reads(self):
        with patch.object(bot,'_at_admin_allowed',return_value=False),patch.object(bot,'db_connect',side_effect=AssertionError('no read')):
            for command in ('/altstats','/daytrades','/latestexport'):
                self.assertTrue(asyncio.run(bot._at_command(None,command,'x','x')))

    def test_daytrades_partial_fills_unknown_and_dry_separation(self):
        fills=[dict(symbol='BTCUSDT',positionSide='LONG',orderId=7,id=i,time=i,side='SELL',realizedPnl='3',commission='.1',commissionAsset='USDT') for i in (1,2)]
        grouped=daytrades.group_fills(fills,{},[])
        self.assertEqual(1,len(grouped));self.assertEqual('UNKNOWN OWNERSHIP',grouped[0]['ownership']);self.assertAlmostEqual(5.8,grouped[0]['net_pnl'])
        async def request(path,params):return []
        with closing(bot.db_connect()) as c:message=asyncio.run(daytrades.report(c,request,int(time.time()*1000)))
        self.assertIn('BOT / DRY',message);self.assertIn('MANUAL BINANCE / REAL',message);self.assertNotIn('GENEL TOPLAM',message)
        self.assertEqual('BOT LIVE',daytrades.classify('BTCUSDT','7','owned',[dict(symbol='BTCUSDT',mode='LIVE',stop_client_id='owned')]))

    def test_observer_forced_refresh_and_unknown_clean_card(self):
        position=dict(symbol='BTCUSDT',positionSide='BOTH',positionAmt='1',entryPrice='100',markPrice='101',unRealizedProfit='1',positionInitialMargin='10')
        async def request(session,method,path):return [position] if path.endswith('positionRisk') else [dict(symbol='BTCUSDT',leverage='10')]
        send=AsyncMock(return_value=True)
        observer=PositionObserver(bot.db_connect,request,send,lambda *a:'MANUAL',lambda *a:('PROFIT',1),(5,10,20),0)
        observer.cache.values={'BTCUSDT':5};observer.cache.expires=time.monotonic()+300
        asyncio.run(observer.poll(None))
        self.assertEqual(10,self.rows('SELECT leverage FROM position_observer_state')[0]['leverage'])
        position['entryPrice']='102'
        async def failed(session,method,path):
            if path.endswith('positionRisk'):return [position]
            raise RuntimeError('offline')
        observer.request=failed;asyncio.run(observer.poll(None))
        self.assertIsNone(self.rows('SELECT leverage FROM position_observer_state')[0]['leverage'])
        message=render_card(self.rows("SELECT * FROM position_observer_events ORDER BY id DESC LIMIT 1")[0])
        self.assertIn('UNKNOWN',message)
        for forbidden in ('{','detail_json','symbolConfig','positionInitialMargin'):self.assertNotIn(forbidden,message)


class ExtendedInvariants(unittest.TestCase):
    def test_current_baseline_and_no_shadow_production_consumers(self):
        baseline=json.loads((fixtures.ROOT/'tests/alt_production_baseline.json').read_text())
        tree=ast.parse((fixtures.ROOT/'binance_momentum_bot/bot.py').read_text(encoding='utf-8'))
        functions={n.name:n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))}
        for name,digest in baseline['functions'].items():
            self.assertEqual(digest,hashlib.sha256(ast.dump(functions[name],include_attributes=False).encode()).hexdigest(),name)
        for path,digest in baseline['files'].items():self.assertEqual(digest,hashlib.sha256((fixtures.ROOT/path).read_bytes().replace(b'\r\n',b'\n')).hexdigest(),path)
        for name,node in functions.items():
            if any(isinstance(n,ast.Name) and n.id=='alt_engine' for n in ast.walk(node)):
                self.assertIn(name,('main','aggtrade_chunk_ws','alt_shadow_loop'))
        shadow=(fixtures.ROOT/'binance_momentum_bot/alt_shadow.py').read_text()
        for forbidden in ('import bot','binance_signed_request','autotrade_cfg','autotrade_active','telegram_public_alert'):self.assertNotIn(forbidden,shadow)
