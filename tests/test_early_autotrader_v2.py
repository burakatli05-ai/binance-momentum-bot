"""Network-free pilot safety tests. No production database or credentials."""
import asyncio
from contextlib import closing
from dataclasses import asdict
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'binance_momentum_bot'))
from execution_v2 import Blocked, Uncertain, capped_plan, marketable_price, reconcile_fills, submit_ioc
from early_autotrader_v2 import Pilot, Config
from early_v2_adapter import Integration, Binance
from step_lock_shadow import StepLockShadow

FILTERS = dict(tick=.01, step=.001, min_qty=.001, max_qty=1000, min_notional=1)
SIGNAL = dict(id=7, symbol='TESTUSDT', price=100., stop=99., target=102., entry_high=100.1,
              v2_score=95., v2_label='FAST_EARLY_V2', ts_ms=1000000)

class Exchange:
    def __init__(self, clock):
        self.now=clock; self.calls=[]; self.executed=.05; self.ask_price=100.; self.timeout=False
        self.incomplete=False; self.kill_hook=None; self.stop_fails=False; self.fill_price=100.
    async def filters(self, symbol): return FILTERS
    async def ask(self, symbol):
        if self.kill_hook: self.kill_hook()
        return self.ask_price
    async def preflight(self,tr): self.calls.append(('preflight',tr['symbol']))
    async def request(self,method,path,params):
        self.calls.append((method,path,dict(params)))
        if self.timeout: raise TimeoutError('lost response')
        if path=='/fapi/v1/order':
            return dict(orderId=13,symbol='TESTUSDT',clientOrderId=params.get('newClientOrderId',params.get('origClientOrderId')),
                side='BUY',type='LIMIT',timeInForce='IOC',status='FILLED' if self.executed else 'EXPIRED',executedQty=self.executed)
        raise AssertionError((method,path))
    async def protect(self,tr,stop,save):
        self.calls.append(('protect',tr['qty'],stop))
        if self.stop_fails: raise RuntimeError('no stop')
        tr['stops']=[dict(id=4,active=True,price=stop)]; save(tr)
    async def ensure_fallback_tp(self,tr,save):
        self.calls.append(('tp',tr['qty'],tr['vwap']*(1+tr['config']['fallback_tp_pct']/100)))
        tr['take_profits']=[dict(id=5,active=True)];save(tr)
    async def cancel_fallback_tp(self,tr,save):
        self.calls.append(('cancel_tp',))
        for tp in tr.get('take_profits',[]):tp['active']=False
        save(tr)
    async def emergency(self,tr,save):
        self.calls.append(('emergency',tr['qty']));tr['emergency_client']='exit';save(tr)
    async def fills(self,symbol,order_id):
        if self.incomplete:return []
        return [dict(id=2,symbol=symbol,orderId=order_id,side='BUY',price=self.fill_price,qty=self.executed,
                     time=self.now(),commission='0.001',commissionAsset='USDT')]
    async def reconcile_position(self,tr,save): pass
    def posts(self): return [c for c in self.calls if c[0]=='POST']

class MathTests(unittest.TestCase):
    def test_three_caps_and_rr(self):
        cfg=asdict(Config());p=capped_plan(SIGNAL,cfg,FILTERS)
        self.assertLessEqual(p['cap'],min(p['caps'].values()))
        self.assertGreaterEqual((SIGNAL['target']-p['cap'])/(p['cap']-p['stop']),cfg['min_rr'])
    def test_each_cap_can_bind(self):
        for key in ('slippage','band','rr'):
            sig=dict(SIGNAL, stop=99.5);cfg=asdict(Config())
            if key=='band':sig['entry_high']=100.01
            if key=='rr':sig['target']=100.52
            p=capped_plan(sig,cfg,FILTERS)
            self.assertEqual(min(p['caps'],key=p['caps'].get),key)
    def test_round_up_ask_must_fit_rounded_down_cap(self):
        p=capped_plan(SIGNAL,asdict(Config()),FILTERS)
        with self.assertRaises(Blocked):marketable_price(p['cap']+.0001,p,.01)
    def test_quantity_never_rounded_up_to_minimum(self):
        with self.assertRaises(Blocked):capped_plan(SIGNAL,asdict(Config()),dict(FILTERS,min_qty=1))
    def test_nonfinite_rejected(self):
        for x in (float('nan'),float('inf'),0,-1,True):
            with self.subTest(x=x),self.assertRaises(Blocked):capped_plan(dict(SIGNAL,price=x),asdict(Config()),FILTERS)
    def test_vwap_uses_all_unique_fills(self):
        order=dict(orderId=8,executedQty=3)
        a=dict(id=1,orderId=8,symbol='S',side='BUY',qty=1,price=100)
        b=dict(a,id=2,qty=2,price=101)
        self.assertAlmostEqual(reconcile_fills(order,[a,b,a],'S')['vwap'],302/3)
    def test_incomplete_conflicting_and_foreign_fills_fail(self):
        order=dict(orderId=8,executedQty=2);a=dict(id=1,orderId=8,symbol='S',side='BUY',qty=1,price=100)
        for fills in ([a],[a,dict(a,price=101)],[a,dict(a,id=2,orderId=9)]):
            with self.assertRaises(Uncertain):reconcile_fills(order,fills,'S')
    def test_missing_or_nonfinite_executed_quantity_is_not_zero_fill(self):
        from execution_v2 import validate_ioc
        base=dict(orderId=1,symbol='S',clientOrderId='C',side='BUY',type='LIMIT',timeInForce='IOC',status='EXPIRED')
        for order in (base,dict(base,executedQty='NaN'),dict(base,executedQty=-1)):
            with self.assertRaises(Uncertain):validate_ioc(order,'S','C')

    def test_config_validation(self):
        for name,val in [('leverage',2.5),('retry',2),('min_score',84),('daily_loss',float('nan')),('execution','true')]:
            with self.assertRaises(Blocked):Config(**{name:val}).validate()

class PilotTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'pilot.db';self.now=1000000
        self.connect=lambda:sqlite3.connect(self.path)
        self.p=Pilot(self.connect,live_allowed=True,score_validated=True,profit_live_allowed=True,clock=lambda:self.now)
        self.p.configure('margin',10.)
        self.ex=Exchange(lambda:self.now)
    async def asyncTearDown(self):self.tmp.cleanup()
    def live(self):
        self.p.set_mode('DRY');token=self.p.challenge('u');self.p.confirm('u',token,ready=True)
    async def enter(self,**signal):await self.p.enter(dict(SIGNAL,**signal),self.ex)
    def trade(self):return next(iter(self.p.active.values()))
    async def test_boot_off_and_no_io(self):
        await self.enter();self.assertEqual(self.p.mode,'OFF');self.assertEqual(self.ex.calls,[])
    async def test_selector_shadow_records_while_execution_off(self):
        self.assertEqual(self.p.mode,'OFF')
        qualified=self.p.record_selector(
            '7','TESTUSDT',self.now,100.0,80,95,'FAST_EARLY_V2',
            ['reason'],{'chg60':0.5,'buy30':0.6}
        )
        self.assertTrue(qualified)
        report=self.p.selector_report(recent_limit=1)
        self.assertEqual((report['total'],report['qualified'],report['rejected']),(1,1,0))
        self.assertEqual(report['recent'][0]['signal_id'],'7')

    async def test_adapter_arm_scores_and_records_even_when_off(self):
        adapter=Integration.__new__(Integration)
        adapter.pilot=self.p
        adapter.step_lock=None
        adapter.queue=asyncio.Queue(maxsize=20)
        adapter.b={
            'ignition_shadow_score':lambda m,base:(95,'FAST_EARLY_V2',['ok']),
            'estimate_trade_plan':lambda symbol,m:dict(entry_high=100.1,invalidation=99,target1=102),
            'states':{},
        }
        adapter.arm(77,'TESTUSDT',dict(
            price=100.,chg10=.2,chg30=.4,chg60=.5,flow10=1.2,flow30=1.1,flow60=1.0,
            buy30=.6,rel30=.2,flow_eff30=.3,dist15high_pct=.5,spread=.05,qv24=1e7,
            oi5=.1,oi_accel5=.1,compression_ratio=1.0,extended=False
        ),80)
        self.assertTrue(adapter.queue.empty())
        report=self.p.selector_report(recent_limit=1)
        self.assertEqual(report['qualified'],1)
        self.assertEqual(report['recent'][0]['signal_id'],'77')
    async def test_restart_off_and_profit_shadow(self):
        self.live();await self.enter()
        token=self.p.challenge('u','PROFIT_LIVE');self.p.confirm('u',token,ready=True,kind='PROFIT_LIVE')
        p=Pilot(self.connect,clock=lambda:self.now)
        self.assertEqual((p.mode,p.profit_mode),('OFF','SHADOW'));self.assertEqual(len(p.active),1)
    async def test_dry_no_private_calls(self):
        self.p.set_mode('DRY');await self.enter();self.assertEqual(self.ex.calls,[])
        self.assertEqual(self.trade()['reference'],'DRY_ASK_PROXY')
    async def test_live_requires_second_confirmation(self):
        with self.assertRaises(Blocked):self.p.set_mode('LIVE')
        self.p.set_mode('DRY');self.p.challenge('u');self.assertEqual(self.p.mode,'DRY')
    async def test_confirmation_bound_to_user_and_single_use(self):
        self.p.set_mode('DRY');t=self.p.challenge('u')
        with self.assertRaises(Blocked):self.p.confirm('other',t,ready=True)
        with self.assertRaises(Blocked):self.p.confirm('u',t,ready=True)
    async def test_expired_confirmation(self):
        self.p.set_mode('DRY');t=self.p.challenge('u');self.now+=120001
        with self.assertRaises(Blocked):self.p.confirm('u',t,ready=True)
    async def test_unvalidated_score_blocks_live(self):
        self.p.score_validated=False;self.p.set_mode('DRY');t=self.p.challenge('u')
        with self.assertRaises(Blocked):self.p.confirm('u',t,ready=True)
    async def test_gate_disabled_does_not_bypass_qualification(self):
        self.p.configure('score_gate',False);self.p.set_mode('DRY');await self.enter();self.assertFalse(self.p.active)
    async def test_unqualified_and_nonfinite_scores(self):
        self.p.set_mode('DRY')
        for score,label in ((84,'FAST_EARLY_V2'),(99,'IGNITION_V2'),(float('nan'),'FAST_EARLY_V2')):
            await self.enter(v2_score=score,v2_label=label)
        self.assertFalse(self.p.active)
    async def test_stale_and_future_signals(self):
        self.p.set_mode('DRY');await self.enter(ts_ms=self.now-4000);await self.enter(ts_ms=self.now+1)
        self.assertFalse(self.p.active)
    async def test_stale_after_reservation_does_not_burn_quota_or_cooldown(self):
        self.live()
        self.ex.kill_hook=lambda:setattr(self,'now',self.now+4000)
        await self.enter()
        self.assertFalse(self.p.halted)
        self.assertEqual(self.p.report('LIVE')['attempts'],0)
        self.assertEqual(self.ex.posts(),[])
        self.ex.kill_hook=None
        await self.enter(id=8,ts_ms=self.now)
        self.assertEqual(len(self.ex.posts()),1)
        self.assertEqual(self.p.report('LIVE')['attempts'],1)

    async def test_pre_reservation_timeout_does_not_halt_or_count_attempt(self):
        self.live()
        self.ex.filters=AsyncMock(side_effect=TimeoutError('filters timeout'))
        await self.enter()
        self.assertFalse(self.p.halted)
        self.assertEqual(self.p.report('LIVE')['attempts'],0)
        self.assertFalse(self.p.active)

    async def test_post_reservation_ask_timeout_fails_closed_without_counting_attempt(self):
        self.live()
        self.ex.ask=AsyncMock(side_effect=TimeoutError('ask timeout'))
        await self.enter()
        self.assertTrue(self.p.halted)
        self.assertEqual(self.p.report('LIVE')['attempts'],0)
        self.assertTrue(self.p.unresolved())
        self.assertIn('ENTRY_ASK:TimeoutError', self.p.last_error)

    async def test_live_ioc_partial_kept_protected_and_actual_vwap(self):
        self.live();self.ex.fill_price=99.99;await self.enter();t=self.trade()
        self.assertEqual(t['vwap'],99.99);self.assertTrue(t['partial']);self.assertEqual(len(self.ex.posts()),1)
        self.assertEqual(self.ex.posts()[0][2]['type'],'LIMIT');self.assertEqual(self.ex.posts()[0][2]['timeInForce'],'IOC')
        self.assertIn(('protect',.05,99.),self.ex.calls)
    async def test_no_fill_only_one_controlled_retry(self):
        self.p.configure('retry',1);self.ex.executed=0;self.live();await self.enter()
        self.assertEqual(len(self.ex.posts()),2);self.assertFalse(self.p.active)
    async def test_no_retry_for_partial_fill(self):
        self.p.configure('retry',1);self.live();await self.enter();self.assertEqual(len(self.ex.posts()),1)
    async def test_timeout_never_resubmits_and_restart_reserves_symbol(self):
        self.p.configure('retry',1);self.ex.timeout=True;self.live();await self.enter()
        self.assertEqual(len(self.ex.posts()),1);self.assertTrue(self.p.halted)
        self.assertEqual(self.p.report('LIVE')['attempts'],1)
        other=Pilot(self.connect,clock=lambda:self.now);self.assertEqual(len(other.active),1)
        self.assertEqual(other.report('LIVE')['attempts'],1)
        with self.assertRaises(Blocked):other.set_mode('DRY')
    async def test_missing_fills_preserve_native_stop_and_halt(self):
        self.live();self.ex.incomplete=True;await self.enter()
        self.assertTrue(self.p.halted);self.assertTrue(self.trade()['stops']);self.assertNotIn('vwap',self.trade())
    async def test_protection_failure_flattens_only_filled_quantity(self):
        self.live();self.ex.stop_fails=True;await self.enter()
        self.assertIn(('emergency',.05),self.ex.calls);self.assertTrue(self.p.halted)
    async def test_kill_mid_await_blocks_post(self):
        self.live();self.ex.kill_hook=lambda:self.p.kill();await self.enter()
        self.assertEqual(self.ex.posts(),[]);self.assertEqual(self.p.mode,'OFF')
    async def test_off_dry_live_cycle_cannot_resume_old_intent(self):
        self.live()
        def cycle():
            self.p.set_mode('OFF');self.live()
        self.ex.kill_hook=cycle;await self.enter();self.assertEqual(self.ex.posts(),[])
    async def test_price_chase_blocked(self):
        self.live();self.ex.ask_price=100.2;await self.enter();self.assertEqual(self.ex.posts(),[])
    async def test_duplicate_symbol_and_signal(self):
        self.p.set_mode('DRY');await self.enter();await self.enter(id=8)
        self.assertEqual(len(self.p.active),1)
    async def test_premium_confirmation_without_reentry(self):
        self.live();await self.enter()
        self.assertTrue(self.p.premium_confirm('TESTUSDT',55,'LIVE'))
        self.assertEqual(self.trade()['classification'],'EARLY→PREMIUM_CONFIRMED');self.assertEqual(len(self.ex.posts()),1)
    async def test_dry_early_does_not_suppress_live_premium(self):
        self.p.set_mode('DRY');await self.enter();self.assertFalse(self.p.premium_confirm('TESTUSDT',55,'LIVE'))
    async def test_premium_busy_blocks_early(self):
        self.live();await self.p.enter(SIGNAL,self.ex,premium_busy=lambda:True);self.assertFalse(self.p.active)
    async def test_concurrent_early_max_one(self):
        self.live();await asyncio.gather(self.enter(),self.enter(id=8));self.assertEqual(len(self.ex.posts()),1)
    async def test_daily_loss_reserves_open_risk(self):
        self.p.configure('daily_loss',.1);self.p.set_mode('DRY');await self.enter();self.assertFalse(self.p.active)
    async def test_daily_attempt_limit_and_cooldown_durable(self):
        self.p.configure('daily_trades',1);self.live();self.ex.executed=0;await self.enter()
        self.now+=1000000;await self.enter(id=8,ts_ms=self.now);self.assertEqual(len(self.ex.posts()),1)
        other=Pilot(self.connect,clock=lambda:self.now);self.assertEqual(other.report('LIVE')['attempts'],1)
    async def test_cooldown_rejects_next_attempt(self):
        self.live();self.ex.executed=0;await self.enter();await self.enter(id=8);self.assertEqual(len(self.ex.posts()),1)
    async def test_profit_live_separate_capability(self):
        self.live();self.assertEqual(self.p.profit_mode,'SHADOW');self.p.profit_live_allowed=False
        t=self.p.challenge('u','PROFIT_LIVE')
        with self.assertRaises(Blocked):self.p.confirm('u',t,ready=True,kind='PROFIT_LIVE')
    async def test_dry_profit_ratcheting_distinct_trades(self):
        self.p.set_mode('DRY');await self.enter();t=self.trade()
        for i,price in enumerate((100.51,100.51,100.51,100.81,100.81,100.81,101.3,101.3,101.3)):
            self.now+=110;self.p.tick('TESTUSDT',price,self.now,self.now,i)
        self.assertGreater(t['shadow_stop'],100.5)
        previous=t['shadow_stop'];self.now+=110;self.p.tick('TESTUSDT',100.7,self.now,self.now,9)
        self.assertGreaterEqual(t['shadow_stop'],previous)
    async def test_dry_realized_loss_and_report(self):
        self.p.set_mode('DRY');await self.enter();self.now+=10;self.p.tick('TESTUSDT',98.,self.now,self.now,1)
        self.assertFalse(self.p.active);r=self.p.report('DRY');self.assertEqual(r['closed'],1);self.assertLess(r['net'],0)
    async def test_selector_shadow_records_and_matches_public_early(self):
        self.p.record_selector(
            'sel1','TESTUSDT',self.now,100.0,80,95.0,'FAST_EARLY_V2',
            ['fast'],{'chg30':0.4,'buy30':0.62}
        )
        with closing(self.connect()) as db:
            db.execute(
                '''CREATE TABLE IF NOT EXISTS entry_stage_forward_shadow(
                    id INTEGER PRIMARY KEY,symbol TEXT,stage TEXT,created_ts_ms INTEGER,
                    mfe_pct REAL,mae_pct REAL,completed_60m INTEGER
                )'''
            )
            db.execute(
                '''INSERT INTO entry_stage_forward_shadow
                   (id,symbol,stage,created_ts_ms,mfe_pct,mae_pct,completed_60m)
                   VALUES (1,'TESTUSDT','EARLY',?,2.5,-0.4,1)''',
                (self.now+1000,)
            )
            db.commit()
        report=self.p.selector_report(recent_limit=5)
        self.assertEqual((report['total'],report['qualified']), (1,1))
        self.assertEqual(report['qualified_stats']['mature60'],1)
        self.assertEqual(report['qualified_stats']['reached']['2.0'],1)
        self.assertEqual(report['threshold_stats']['95']['mature60'],1)
        self.assertEqual(report['threshold_stats']['98']['mature60'],0)

    async def test_adapter_arm_records_selector_even_when_pilot_off(self):
        adapter=Integration.__new__(Integration)
        adapter.pilot=self.p
        adapter.step_lock=None
        adapter.queue=asyncio.Queue(maxsize=20)
        adapter.b={
            'ignition_shadow_score':lambda m,base:(95.0,'FAST_EARLY_V2',['ok']),
            'estimate_trade_plan':lambda symbol,m:dict(
                entry_high=100.1,invalidation=99.0,target1=102.0
            ),
            'states':{},
        }
        adapter.arm('sel-off','TESTUSDT',{
            'price':100.0,'chg10':0.2,'chg30':0.4,'chg60':0.5,
            'flow10':1.2,'flow30':1.1,'flow60':1.0,'buy30':0.6,
            'rel30':0.2,'flow_eff30':0.3,'dist15high_pct':0.8,
            'spread':0.05,'qv24':10000000,'extended':False,
        },80)
        report=self.p.selector_report(recent_limit=5)
        self.assertEqual(report['total'],1)
        self.assertEqual(report['qualified'],1)
        self.assertTrue(adapter.queue.empty())

    async def test_menu_contains_all_requested_controls(self):
        adapter=Integration.__new__(Integration);adapter.pilot=self.p
        labels=[b['text'] for row in adapter.markup()['inline_keyboard'] for b in row]
        self.assertEqual(len(labels),12);self.assertIn('Kill Switch',labels);self.assertIn('Min V2 Score',labels)
    async def test_unauthorized_telegram_cannot_change_state(self):
        adapter=Integration.__new__(Integration);adapter.pilot=self.p
        adapter.b={'_at_admin_allowed':lambda *a,**k:False,'telegram_send':AsyncMock()}
        await adapter.command(None,'/earlyv2 dry','c','u');self.assertEqual(self.p.mode,'OFF')


    async def test_steplock_command_is_admin_only_read_only_and_reports_shadow(self):
        adapter=Integration.__new__(Integration);adapter.pilot=self.p
        adapter.step_lock=StepLockShadow(self.connect)
        adapter.step_lock.arm('sl1','TESTUSDT',100.0,self.now)
        adapter.step_lock.tick('TESTUSDT',100.21,self.now+100,self.now+100,1)
        adapter.step_lock.tick('TESTUSDT',100.19,self.now+200,self.now+200,2)
        sender=AsyncMock()
        adapter.b={'_at_admin_allowed':lambda *a,**k:True,'telegram_send':sender}
        mode=self.p.mode; halted=self.p.halted
        handled=await adapter.command(None,'/steplock recent','c','u')
        self.assertTrue(handled)
        self.assertEqual((self.p.mode,self.p.halted),(mode,halted))
        sender.assert_awaited_once()
        message=sender.await_args.args[1]
        self.assertIn('STEP LOCK V1',message)
        self.assertIn('200×10',message)
        self.assertIn('TESTUSDT',message)

    async def test_steplock_unauthorized_returns_without_query_mutation(self):
        adapter=Integration.__new__(Integration);adapter.pilot=self.p
        adapter.step_lock=StepLockShadow(self.connect)
        sender=AsyncMock()
        adapter.b={'_at_admin_allowed':lambda *a,**k:False,'telegram_send':sender}
        handled=await adapter.command(None,'/steplock','c','u')
        self.assertTrue(handled)
        sender.assert_awaited_once()
        self.assertIn('Yetkili',sender.await_args.args[1])


    async def test_step_lock_report_log_is_periodic_and_read_only(self):
        adapter=Integration.__new__(Integration);adapter.pilot=self.p
        adapter.step_lock=StepLockShadow(self.connect);adapter._last_step_report_ms=0
        adapter.step_lock.arm('log1','TESTUSDT',100.0,self.now)
        adapter.step_lock.tick('TESTUSDT',105.1,self.now+100,self.now+100,1)
        mode=self.p.mode;halted=self.p.halted
        with self.assertLogs('early_v2_adapter',level='INFO') as logs:
            adapter._log_step_lock_report()
        self.assertTrue(any('STEP_LOCK_REPORT ' in line for line in logs.output))
        self.assertTrue(any('closed_net_usdt_200x10' in line for line in logs.output))
        self.assertEqual((self.p.mode,self.p.halted),(mode,halted))
        first=adapter._last_step_report_ms
        adapter._log_step_lock_report()
        self.assertEqual(adapter._last_step_report_ms,first)


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();path=Path(self.tmp.name)/'db.sqlite'
        self.p=Pilot(lambda:sqlite3.connect(path),clock=lambda:1000000)
        self.snapshot=AsyncMock(return_value=({'canTrade':True,'dualSidePosition':False},{'availableBalance':100},[]))
        self.requests=AsyncMock()
        self.adapter=Binance({'_at_account_snapshot':self.snapshot,'binance_signed_request':self.requests},None,self.p)
        self.tr=dict(id='a'*24,symbol='TESTUSDT',qty=.05,stop=99.,stops=[],config=asdict(Config()),created_ms=999900)
        self.saved=[]
    async def asyncTearDown(self):self.tmp.cleanup()
    def save(self,tr):self.saved.append(json.loads(json.dumps(tr)))
    async def test_initial_protection_is_quantity_reduce_only(self):
        async def req(session,method,path,params):
            return dict(algoId=1,clientAlgoId=params['clientAlgoId'],algoStatus='NEW')
        self.requests.side_effect=req
        await self.adapter.protect(self.tr,99.,self.save)
        params=self.requests.call_args.args[3]
        self.assertEqual(params['reduceOnly'],'true');self.assertEqual(params['quantity'],'0.05')
        self.assertNotIn('closePosition',params);self.assertTrue(self.saved[0]['pending_stop'])
    async def test_replacement_ack_before_old_cancel(self):
        self.tr['stops']=[dict(id=1,client='old',price=99.,active=True)]
        async def req(session,method,path,params):
            return dict(algoId=2,clientAlgoId=params.get('clientAlgoId'),algoStatus='NEW')
        self.requests.side_effect=req
        await self.adapter.protect(self.tr,100.25,self.save)
        self.assertEqual([c.args[1] for c in self.requests.call_args_list],['POST','DELETE'])
        self.assertTrue(self.tr['stops'][-1]['active']);self.assertFalse(self.tr['stops'][0]['active'])
    async def test_uncertain_replacement_keeps_old_stop(self):
        self.tr['stops']=[dict(id=1,client='old',price=99.,active=True)];self.requests.side_effect=TimeoutError()
        with self.assertRaises(TimeoutError):await self.adapter.protect(self.tr,100.25,self.save)
        self.assertTrue(self.tr['stops'][0]['active']);self.assertTrue(self.tr['pending_stop'])
        self.assertFalse(any(c.args[1]=='DELETE' for c in self.requests.call_args_list))
    async def test_pending_stop_recovery_does_not_post_again(self):
        self.tr['pending_stop']=dict(client='pending',price=99.)
        self.requests.return_value=dict(algoId=2,clientAlgoId='pending',algoStatus='NEW')
        await self.adapter.protect(self.tr,99.,self.save)
        self.assertEqual(self.requests.call_args.args[1],'GET');self.assertIsNone(self.tr['pending_stop'])
    async def test_filter_is_limit_lot_not_market_lot(self):
        self.adapter.b['fetch_json']=AsyncMock(return_value={'symbols':[dict(symbol='TESTUSDT',status='TRADING',marginAsset='USDT',filters=[
            dict(filterType='PRICE_FILTER',tickSize='.01'),dict(filterType='LOT_SIZE',stepSize='.001',minQty='.001',maxQty='100'),
            dict(filterType='MARKET_LOT_SIZE',stepSize='1',minQty='1',maxQty='100'),dict(filterType='MIN_NOTIONAL',notional='5')])]})
        self.assertEqual((await self.adapter.filters('TESTUSDT'))['step'],.001)
    async def test_hedge_mode_blocks_before_account_mutation(self):
        self.snapshot.return_value=({'canTrade':True,'dualSidePosition':True},{'availableBalance':100},[])
        with self.assertRaises(Blocked):await self.adapter.preflight(self.tr)
        self.requests.assert_not_awaited()
    async def test_existing_manual_position_blocks_before_mutation(self):
        self.snapshot.return_value=({'canTrade':True,'dualSidePosition':False},{'availableBalance':100},[dict(symbol='TESTUSDT',positionAmt='.1')])
        with self.assertRaises(Blocked):await self.adapter.preflight(self.tr)
        self.requests.assert_not_awaited()
    async def test_preexisting_orders_block(self):
        self.requests.return_value=[{'orderId':3}]
        with self.assertRaises(Blocked):await self.adapter.preflight(self.tr)
        self.assertTrue(all(c.args[1]=='GET' for c in self.requests.call_args_list))
    async def test_emergency_close_is_sell_reduce_only_once(self):
        self.snapshot.return_value=({'canTrade':True,'dualSidePosition':False},{},[dict(symbol='TESTUSDT',positionAmt='.05')])
        await self.adapter.emergency(self.tr,self.save);await self.adapter.emergency(self.tr,self.save)
        self.requests.assert_awaited_once();params=self.requests.call_args.args[3]
        self.assertEqual((params['side'],params['reduceOnly'],params['quantity']),('SELL','true','0.05'))
    async def test_changed_position_quantity_halts_without_mutation(self):
        self.snapshot.return_value=({'dualSidePosition':False},{},[dict(symbol='TESTUSDT',positionAmt='.07')])
        with self.assertRaises(Uncertain):await self.adapter.reconcile_position(self.tr,self.save)
        self.requests.assert_not_awaited()
    async def test_close_reconciles_exact_exit_fills_fees_funding(self):
        self.tr.update(stops=[dict(id=1,price=99.,active=True)],fills=[dict(commissionAsset='USDT',commission='.01')])
        async def req(session,method,path,params):
            if path=='/fapi/v1/algoOrder':return dict(algoStatus='FINISHED',actualOrderId=44)
            if path=='/fapi/v1/userTrades':return [dict(id=7,orderId=44,symbol='TESTUSDT',side='SELL',qty='.05',price='101',realizedPnl='.05',commission='.01',commissionAsset='USDT',time=1000000)]
            if path=='/fapi/v1/income':return [dict(income='-.005',asset='USDT')]
            raise AssertionError(path)
        self.requests.side_effect=req
        await self.adapter.reconcile_position(self.tr,self.save)
        self.assertEqual(self.tr['status'],'CLOSED');self.assertAlmostEqual(self.tr['net'],.025)
    async def test_manual_close_not_misallocated(self):
        self.tr['stops']=[dict(id=1,price=99.,active=True)]
        self.requests.return_value=dict(algoStatus='CANCELED',actualOrderId='')
        with self.assertRaises(Uncertain):await self.adapter.reconcile_position(self.tr,self.save)
        self.assertNotEqual(self.tr.get('status'),'CLOSED')
    async def test_truncated_fills_never_silently_accepted(self):
        self.requests.return_value=[{}]*1000
        with self.assertRaises(Uncertain):await self.adapter.fills('TESTUSDT',12)

class IsolationTests(unittest.TestCase):
    def test_exact_hook_removal_matches_whole_base_module(self):
        import ast, hashlib
        from early_v2_compat import StripEarlyHooks
        root=Path(__file__).resolve().parents[1]
        current=ast.parse((root/'binance_momentum_bot/bot.py').read_text(encoding='utf-8'))
        normalized=StripEarlyHooks().visit(current)
        digest=hashlib.sha256(ast.dump(normalized,include_attributes=False).encode()).hexdigest()
        expected=json.loads((root/'tests/early_v2_base_digest.json').read_text())
        self.assertEqual(digest,expected['bot_ast_sha256'])

class RecoveryTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = PilotTests.asyncSetUp
    asyncTearDown = PilotTests.asyncTearDown
    live = PilotTests.live
    enter = PilotTests.enter
    trade = PilotTests.trade
    async def test_recovery_fills_unknown_entry_without_second_post(self):
        self.ex.timeout=True;self.live();await self.enter();self.ex.timeout=False
        await self.p.reconcile(self.ex)
        self.assertEqual(self.trade()['status'],'OPEN');self.assertEqual(len(self.ex.posts()),1)
        self.assertTrue(self.p.halted)  # reconciliation never re-enables LIVE
    async def test_partial_retry_cannot_increase_limit(self):
        self.p.configure('retry',1);self.ex.executed=0;self.live()
        count=0
        async def ask(symbol):
            nonlocal count
            count+=1
            return 100 if count==1 else 100.01
        self.ex.ask=ask;await self.enter();self.assertEqual(len(self.ex.posts()),1)
    async def test_stop_reconciliation_fault_cannot_be_cleared_by_dry(self):
        self.live();await self.enter()
        self.ex.reconcile_position=AsyncMock(side_effect=Uncertain('ownership'))
        await self.p.reconcile(self.ex)
        with self.assertRaises(Blocked):self.p.set_mode('DRY')
        self.assertEqual(self.p.mode,'OFF')
    async def test_recovery_wrong_order_identity_stays_reserved(self):
        self.live();self.ex.timeout=True;await self.enter();self.ex.timeout=False
        self.ex.request=AsyncMock(return_value=dict(orderId=13,symbol='OTHER',clientOrderId='wrong',side='BUY',type='LIMIT',timeInForce='IOC',status='FILLED',executedQty=.05))
        await self.p.reconcile(self.ex)
        self.assertTrue(self.p.unresolved());self.assertNotIn('vwap',self.trade())
    async def test_cooldown_restarts_at_close(self):
        self.p.set_mode('DRY');await self.enter();self.now+=1000000
        self.p.tick('TESTUSDT',98,self.now,self.now,1)
        await self.enter(id=8,ts_ms=self.now)
        self.assertFalse(self.p.active)
    async def test_profit_shadow_never_sends_stop_ratchet(self):
        self.live();await self.enter();self.trade()['shadow_stop']=100.25
        previous=len([c for c in self.ex.calls if c[0]=='protect'])
        await self.p.reconcile(self.ex)
        self.assertEqual(len([c for c in self.ex.calls if c[0]=='protect']),previous)
    async def test_live_profit_raises_stop_after_explicit_confirmation(self):
        self.live();await self.enter();self.trade()['shadow_stop']=100.25
        token=self.p.challenge('u','PROFIT_LIVE');self.p.confirm('u',token,ready=True,kind='PROFIT_LIVE')
        await self.p.reconcile(self.ex)
        self.assertIn(('protect',.05,100.25),self.ex.calls)

if __name__=='__main__':unittest.main()
