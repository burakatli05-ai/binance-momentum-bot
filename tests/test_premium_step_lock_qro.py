"""Fake HTTP tests exercise the production adapter, not a substitute state machine."""
import asyncio
import ast
import hashlib
from copy import deepcopy
from decimal import Decimal
import json
import pathlib
import socket
import sqlite3
import tempfile
import unittest
from unittest.mock import patch, AsyncMock
import premium_step_lock as sl
import premium_step_lock_qro as qro


def trade():
    tr = dict(id=7, symbol='BTCUSDT', side='LONG', position_side='BOTH', mode='LIVE',
              status='OPEN', exit_profile=sl.PROFILE, expected_qty='2', qty='2',
              entry_order_id='100', entry_client_id='ENTRY-7', manual_intervention=0,
              **sl.entry_fields('100', '98.7', 1000))
    tr.update(qro.new_trade_fields(tr))
    return tr


class MemoryStore:
    def __init__(self): self.tr=trade(); self.history=[]; self.fail=False
    def load(self, tid): return deepcopy(self.tr)
    def save(self, tid, expected, state, **fields):
        if self.fail: raise RuntimeError('SIMULATED_COMMIT_FAILURE')
        if expected != self.tr['qro_state']: raise qro.Block('QRO_CONCURRENT_OR_STALE_STATE')
        state=deepcopy(state); state['serial']+=1
        raw=qro.encode(state); self.tr.update(fields, qro_state=raw)
        self.history.append(deepcopy(self.tr))
        return state,raw


def fill(oid, qty, side='SELL'):
    return dict(id=int(oid), orderId=str(oid), symbol='BTCUSDT', side=side,
                positionSide='BOTH', qty=str(qty), price='100')


class HTTP:
    def __init__(self, store):
        self.store=store; self.calls=[]; self.algos={}; self.normal={}; self.fills={}
        self.qty='2'; self.position_side='BOTH'; self.hedge=False; self.price='102'; self.quote_time=100000
        self.post_loss=False; self.cancel_loss=False; self.cancel_noop=False; self.query_errors=0
        self.hook=None; self.next_id=201; self.extra=[]; self.step='0.001'; self.budget=80
        self.entry=dict(orderId='100',clientOrderId='ENTRY-7',symbol='BTCUSDT',side='BUY',
                        positionSide='BOTH',status='FILLED',origQty='2',executedQty='2')
    async def __call__(self, method, route, params):
        self.calls.append((method,route,deepcopy(params)))
        if self.hook: self.hook(method,route,params)
        if route.endswith('/positionSide/dual'): return dict(dualSidePosition=self.hedge)
        if route.endswith('/positionRisk'): return [dict(symbol='BTCUSDT',positionSide=self.position_side,positionAmt=self.qty)]
        if route.endswith('/exchangeInfo'):
            return dict(symbols=[dict(symbol='BTCUSDT',status='TRADING',contractType='PERPETUAL',filters=[
                dict(filterType='PRICE_FILTER',tickSize='0.01'),
                dict(filterType='LOT_SIZE',stepSize=self.step,minQty='0.001',maxQty='1000'),
                dict(filterType='MARKET_LOT_SIZE',stepSize=self.step,minQty='0.001',maxQty='1000'),
                dict(filterType='MIN_NOTIONAL',notional='5')])])
        if route.endswith('/ticker/price'): return dict(symbol='BTCUSDT',price=self.price,time=self.quote_time)
        if route.endswith('/userTrades'):
            return [fill(100,2,'BUY')] if str(params['orderId'])=='100' else deepcopy(self.fills[str(params['orderId'])])
        if route.endswith('/order'):
            oid=str(params['orderId'])
            if method=='DELETE': self.normal[oid]['status']='CANCELED'
            return deepcopy(self.entry if oid=='100' else self.normal[oid])
        if route.endswith('/openOrders'):
            return deepcopy([x for x in self.normal.values() if x['status'] in ('NEW','PARTIALLY_FILLED')])
        if route.endswith('/openAlgoOrders'):
            return deepcopy([x for x in self.algos.values() if x['algoStatus']=='NEW']+self.extra)
        if route.endswith('/algoOrder'):
            if method=='POST':
                s=json.loads(self.store.tr['qro_state']); cid=params['clientAlgoId']
                assert s['pending']==cid and s['intents'][cid]['post_attempted'], 'intent must be durable'
                assert len([r for r in self.algos.values() if r['algoStatus']=='NEW'])<2, 'third stop'
                assert params['reduceOnly']=='true' and params['closePosition']=='false'
                self.algos[cid]=dict(params,algoId=str(self.next_id),orderType=params['type'],
                    reduceOnly=True,closePosition=False,priceProtect=False,algoStatus='NEW',actualOrderId='')
                self.next_id+=1
                if self.post_loss: raise RuntimeError('secret=FAKE_SECRET signedquery=FAKE_SIGNATURE')
                return deepcopy(self.algos[cid])
            if method=='DELETE':
                row=next(r for r in self.algos.values() if r['algoId']==params['algoId'])
                if not self.cancel_noop: row['algoStatus']='CANCELED'
                if self.cancel_loss: raise RuntimeError('FAKE_SECRET')
                return deepcopy(row)
            if self.query_errors:
                self.query_errors-=1; raise RuntimeError('HTTP error FAKE_SECRET')
            return deepcopy(self.algos[params['clientAlgoId']])
        raise AssertionError((method,route,params))
    def seed(self, revision=0, target='98.7'):
        cid=qro.client_id(7,revision); p=qro.request(self.store.tr,cid,Decimal(2),Decimal(target))
        row=dict(p,algoId=str(200+revision),orderType='STOP_MARKET',reduceOnly=True,
                 closePosition=False,priceProtect=False,algoStatus='NEW',actualOrderId='')
        self.algos[cid]=row
        s=json.loads(self.store.tr['qro_state'])
        s.update(active=cid,revision=revision,phase='STABLE',intents={cid:dict(params=p,post_attempted=True,algoId=row['algoId'])})
        self.store.tr.update(qro_state=qro.encode(s),stop_client_id=cid,stop_algo_id=row['algoId'],stop_revision=revision)
        return cid
    def trigger(self,cid,qty='1',status='PARTIALLY_FILLED'):
        self.algos[cid].update(algoStatus='FINISHED',actualOrderId='900')
        self.normal['900']=dict(orderId='900',symbol='BTCUSDT',side='SELL',positionSide='BOTH',
            reduceOnly=True,type='MARKET',origQty='2',executedQty=qty,status=status)
        self.fills['900']=[fill(900,qty)] if Decimal(qty)>0 else []
        self.qty=str(Decimal(2)-Decimal(qty))


class QROTests(unittest.TestCase):
    def setUp(self):
        self.store=MemoryStore(); self.http=HTTP(self.store); self.old=self.http.seed()
        self.desired('101')
    def desired(self,price):
        self.store.tr.update(sl.tick_fields(self.store.tr,price,2000,'0.01'))
    def exchange(self,**kwargs):
        return qro.BinancePremiumStepLockQROExchange(self.http,enabled=True,clock=lambda:1,wall=lambda:100,**kwargs)
    def runq(self,**kwargs): return asyncio.run(self.exchange(**kwargs).reconcile_trade(7,self.store))
    def state(self): return json.loads(self.store.tr['qro_state'])
    def writes(self,method='POST'): return [c for c in self.http.calls if c[0]==method]
    def pending(self, posted=True, attempted=True):
        cid=qro.client_id(7,1);p=qro.request(self.store.tr,cid,Decimal(2),Decimal('100.5'))
        s=self.state();s.update(pending=cid,revision=1,phase='INTENT',unresolved=[cid])
        s['intents'][cid]=dict(params=p,post_attempted=attempted)
        self.store.tr.update(qro_state=qro.encode(s),pending_stop_client_id=cid,pending_stop_price='100.5',stop_revision=1)
        if posted:self.http.algos[cid]=dict(p,algoId='201',orderType='STOP_MARKET',reduceOnly=True,
            closePosition=False,priceProtect=False,algoStatus='NEW',actualOrderId='')
        return cid
    def assert_blocked(self,reason):
        self.assertEqual(self.runq(),reason);self.assertTrue(self.state()['quarantine'])
        count=len(self.writes()); self.runq(); self.assertEqual(len(self.writes()),count)

    def test_13_one_way(self): self.assertEqual(self.runq(),'REPLACED')
    def test_14_hedge(self): self.http.hedge=True;self.assert_blocked('QRO_ACCOUNT_MODE_NOT_CERTIFIED');self.assertFalse(self.writes())
    def test_15_quantity_exchange(self):
        self.assertEqual(self.runq(),'REPLACED');self.assertEqual(self.writes()[0][2]['quantity'],'2')
    def test_16_mismatch(self): self.http.qty='3';self.assert_blocked(qro.MUTATION);self.assertFalse(self.writes())
    def test_17_dust_no_partial_rounding(self): self.http.step='0.3';self.assert_blocked('QRO_INVALID_OR_DUST_QUANTITY')
    def test_18_zero_no_post(self): self.http.qty='0';self.assertEqual(self.runq(),'FLAT_CLEAN');self.assertFalse(self.writes())
    def test_18b_invalid_qty(self): self.http.qty='NaN';self.assert_blocked('INVALID_DECIMAL')
    def test_19_reduce_only(self): self.runq();self.assertEqual(self.writes()[0][2]['reduceOnly'],'true')
    def test_20_close_position_false(self): self.runq();self.assertEqual(self.writes()[0][2]['closePosition'],'false')
    def test_21_durable(self):
        self.runq();self.assertTrue(any(json.loads(t['qro_state'])['phase']=='INTENT' for t in self.store.history))
    def test_21b_commit_failure_no_post(self):
        self.store.fail=True
        with self.assertRaises(RuntimeError):self.runq()
        self.assertFalse(self.writes())
    def test_22_deterministic(self): self.assertEqual(qro.client_id(7,1),'QRO1-7-1');self.assertNotEqual(qro.client_id(8,1),qro.client_id(7,1))
    def test_23_post_then_query(self):
        self.runq();p=next(i for i,c in enumerate(self.http.calls) if c[0]=='POST')
        self.assertTrue(any(c[0]=='GET' and c[2].get('clientAlgoId')=='QRO1-7-1' for c in self.http.calls[p+1:]))
    def test_24_post_loss(self): self.http.post_loss=True;self.assertEqual(self.runq(),'REPLACED');self.assertEqual(len(self.writes()),1)
    def test_25_query_error_bounded(self): self.http.query_errors=2;self.assertEqual(self.runq(),'REPLACED')
    def test_26_identity_mismatch(self): self.http.algos[self.old]['symbol']='BAD';self.assert_blocked('QRO_ORDER_IDENTITY_MISMATCH');self.assertFalse(self.writes('DELETE'))
    def test_27_ack_before_cancel(self):
        self.runq();i=next(i for i,c in enumerate(self.http.calls) if c[0]=='DELETE')
        self.assertTrue(any(c[2].get('clientAlgoId')=='QRO1-7-1' for c in self.http.calls[:i]))
        self.assertTrue(any(json.loads(t['qro_state'])['phase']=='NEW_STOP_WORKING' for t in self.store.history))
    def test_28_cancel_loss_dual_query(self):
        self.http.cancel_loss=True;self.assertEqual(self.runq(),'REPLACED')
        i=next(i for i,c in enumerate(self.http.calls) if c[0]=='DELETE')
        self.assertEqual({c[2].get('clientAlgoId') for c in self.http.calls[i+1:] if 'clientAlgoId' in c[2]}, {self.old,'QRO1-7-1'})
    def test_29_no_post_retry(self): self.pending(posted=False);self.assert_blocked(qro.BUDGET);self.assertFalse(self.writes())
    def test_30_no_cancel_retry(self): self.http.cancel_noop=True;self.assert_blocked(qro.BUDGET);self.assertEqual(len(self.writes('DELETE')),1)
    def test_31_no_third(self): self.pending();self.http.cancel_noop=True;self.desired('103');self.runq();self.assertFalse(self.writes());self.assertEqual(len(self.http.algos),2)
    def test_32_at_most_two(self): self.runq();self.assertEqual(len(self.http.algos),2)
    def test_33_new_partial(self):
        cid=self.pending();self.http.trigger(cid);self.assert_blocked('PARTIAL_FILL_REQUIRES_MANUAL_REVIEW')
        self.assertIn('NEW_STOP_PARTIALLY_FILLED',self.state()['events']);self.assertFalse(self.writes())
    def test_34_old_partial(self):
        self.http.trigger(self.old);self.assert_blocked('PARTIAL_FILL_REQUIRES_MANUAL_REVIEW')
        self.assertIn('OLD_STOP_PARTIALLY_FILLED',self.state()['events'])
    def test_35_partial_position_without_fill(self): self.http.qty='1';self.assert_blocked(qro.MUTATION)
    def test_36_flat_during_replacement(self): self.pending();self.http.qty='0';self.assertEqual(self.runq(),'FLAT_CLEAN')
    def test_37_old_residual_cleanup(self): self.http.qty='0';self.runq();self.assertEqual(self.http.algos[self.old]['algoStatus'],'CANCELED')
    def test_38_new_residual_cleanup(self): cid=self.pending();self.http.qty='0';self.runq();self.assertEqual(self.http.algos[cid]['algoStatus'],'CANCELED')
    def test_39_final_zeros(self):
        self.pending();self.http.qty='0';self.runq();s=self.state()
        self.assertFalse(s['pending'] or s['unresolved'] or s['quarantine'] or s['residual'])
        self.assertTrue(all(r['algoStatus']=='CANCELED' for r in self.http.algos.values()))
    def test_40_unclean_quarantine(self): self.http.qty='0';self.http.cancel_noop=True;self.assert_blocked(qro.BUDGET);self.assertTrue(self.state()['residual'])
    def test_41_reentry_block(self): self.assertTrue(qro.local_residual(self.store.tr))
    def test_42_reentry_clean(self):
        self.http.qty='0';self.runq();self.assertFalse(qro.local_residual(self.store.tr))
        self.assertTrue(asyncio.run(self.exchange().check_reentry('BTCUSDT',[self.store.tr],self.store)))
    def test_43_manual_qty(self): self.http.qty='2.1';self.assert_blocked(qro.MUTATION)
    def test_44_side_change(self): self.http.position_side='SHORT';self.assert_blocked(qro.MUTATION)
    def test_45_unowned(self): self.http.extra=[dict(self.http.algos[self.old],clientAlgoId='foreign')];self.assert_blocked(qro.EXTRA)
    def test_46_unknown_terminal(self): self.http.algos[self.old]['algoStatus']='TRIGGERING';self.assert_blocked('TERMINAL_STATE_UNKNOWN')
    def test_47_actual_trigger_false(self): self.assertIs(qro.ACTUAL_TRIGGER_CERTIFIED,False);self.assertIs(qro.LIVE_ACTIVATION_READY,False)
    def test_48_restart_before_post(self):self.pending(posted=False,attempted=False);self.runq();self.assertFalse(self.writes());self.assertTrue(self.state()['quarantine'])
    def test_49_restart_lost_post(self):self.pending();self.assertEqual(self.runq(),'REPLACED');self.assertFalse(self.writes())
    def test_50_restart_ack(self):self.pending();self.store.tr['replacement_status']='NEW_STOP_WORKING';self.assertEqual(self.runq(),'REPLACED');self.assertFalse(self.writes())
    def test_51_restart_cancel_loss(self):
        self.pending();s=self.state();s['cancels']=[self.old];s['cancel_writes']=1;self.store.tr['qro_state']=qro.encode(s)
        self.http.algos[self.old]['algoStatus']='CANCELED';self.assertEqual(self.runq(),'REPLACED');self.assertFalse(self.writes('DELETE'))
    def test_52_restart_two_orders(self):self.pending();self.assertEqual(self.runq(),'REPLACED');self.assertFalse(self.writes())
    def test_53_restart_partial(self):self.pending();self.http.trigger(self.old);self.runq();self.assertEqual(self.runq(),'QUARANTINED_MANUAL_REVIEW');self.assertFalse(self.writes())
    def test_54_restart_flat(self):self.pending();self.http.qty='0';self.assertEqual(self.runq(),'FLAT_CLEAN');self.assertFalse(self.writes())
    def test_55_restart_unresolved(self):self.pending(posted=False);self.assert_blocked(qro.BUDGET);self.assertTrue(self.state()['unresolved'])
    def test_56_no_new_id(self):cid=self.pending(posted=False);self.runq();self.runq();self.assertEqual(self.state()['pending'],cid)
    def test_57_profile_preserved(self):before={k:self.store.tr[k] for k in sl.FROZEN_FIELDS};self.runq();self.assertEqual(before,{k:self.store.tr[k] for k in before})
    def test_58_duplicate_tick(self):self.assertEqual(sl.tick_fields(self.store.tr,'101',2000,'0.01'),{})
    def test_59_out_of_order(self):self.assertEqual(sl.tick_fields(self.store.tr,'105',1999,'0.01'),{})
    def test_60_jump(self):self.store.tr.update(sl.tick_fields(self.store.tr,'105',3000,'0.01'));self.http.price='106';self.runq();self.assertEqual(len(self.writes()),1);self.assertEqual(Decimal(self.writes()[0][2]['triggerPrice']),Decimal('104.5'))
    def test_61_pending_desired(self):
        self.pending();self.store.tr.update(sl.tick_fields(self.store.tr,'103',3000,'0.01'));self.http.price='104'
        self.runq();self.assertEqual(self.store.tr['desired_stop_price'],'102.50');self.assertEqual(self.store.tr['sl_active_stop_price'],'100.5');self.assertFalse(self.writes())
    def test_62_cap(self):
        self.store.tr.update(sl.tick_fields(self.store.tr,'111',3000,'0.01'));self.http.price='112';self.runq();r=self.store.tr['stop_revision']
        self.assertEqual(sl.tick_fields(self.store.tr,'130',4000,'0.01'),{});self.assertEqual(self.runq(),'STABLE');self.assertEqual(self.store.tr['stop_revision'],r)
    def test_63_round_same(self):
        self.store.tr['desired_stop_price']=self.store.tr['sl_active_stop_price'];self.assertEqual(self.runq(),'STABLE');self.assertFalse(self.writes())
    def test_64_retrace(self):self.http.price='100';self.assertEqual(self.runq(),'INVALID_TARGET_KEEP_OLD_STOP');self.assertFalse(self.writes('DELETE'))
    def test_65_bad_market(self):self.http.quote_time=1;self.assert_blocked('INVALID_MARKET_REFERENCE_KEEP_OLD_STOP');self.assertFalse(self.writes())
    def test_66_tick_no_network(self):sl.tick_fields(self.store.tr,'102',3000,'0.01');self.assertEqual(self.http.calls,[])
    def test_67_query_budget(self):self.assertEqual(self.runq(query_budget=1),qro.BUDGET);self.assertEqual(len(self.http.calls),1)
    def test_68_cancel_budget(self):self.assertEqual(self.runq(cancel_budget=0),qro.BUDGET);self.assertFalse(self.writes('DELETE'))
    def test_69_retry_bounded(self):self.http.query_errors=100;self.assert_blocked(qro.BUDGET);self.assertEqual(sum(c[1].endswith('/algoOrder') for c in self.http.calls),3)
    def test_70_external_network_guard(self):
        with self.assertRaisesRegex(RuntimeError,'NETWORK_DISABLED_IN_TESTS'):socket.create_connection(('fapi.binance.com',443))
    def test_71_disabled_no_requests(self):
        x=qro.BinancePremiumStepLockQROExchange(self.http);self.assertEqual(asyncio.run(x.reconcile_trade(7,self.store)),'QRO_ADAPTER_DISABLED');self.assertFalse(self.http.calls)
    def test_72_no_demo_or_account_setters(self):self.runq();self.assertTrue(all(c[1].startswith('/fapi/') for c in self.http.calls));self.assertTrue(all(c[1]=='/fapi/v1/algoOrder' for c in self.http.calls if c[0]!='GET'))
    def test_73_secret_sanitized(self):self.http.post_loss=True;self.runq();self.assertNotIn('FAKE_SECRET',json.dumps(self.store.history))
    def test_74_signature_not_archived(self):self.http.post_loss=True;self.runq();self.assertNotIn('FAKE_SIGNATURE',json.dumps(self.store.history))
    def test_75_prefix_not_ownership(self):self.http.extra=[dict(self.http.algos[self.old],clientAlgoId='QRO1-7-999')];self.assert_blocked(qro.EXTRA)
    def test_76_unowned_normal(self):self.http.normal['999']=dict(orderId='999',status='NEW');self.assert_blocked(qro.EXTRA)
    def test_80_no_conversion(self):
        with self.assertRaisesRegex(qro.Block,'EXISTING_POSITION_NOT_CONVERTIBLE'):qro.new_trade_fields(self.store.tr)
    def test_81_open_rollback(self):self.assertFalse(sl.rollback_readiness([self.store.tr],mode='OFF',global_profile='CURRENT_TP2',exchange_orders=[])['rollback_safe'])
    def test_82_residual_rollback(self):self.assertTrue(qro.drain_blockers([], [dict(clientAlgoId='QRO1-7-1')]))
    def test_83_pending_rollback(self):self.pending();self.assertTrue(qro.drain_blockers([self.store.tr],[]))
    def test_84_drained_rollback(self):self.http.qty='0';self.runq();self.store.tr['status']='CLOSED';self.assertTrue(sl.rollback_readiness([self.store.tr],mode='OFF',global_profile='CURRENT_TP2',exchange_orders=[])['rollback_safe'])
    def test_old_filled_cleanup(self):self.pending();self.http.trigger(self.old,'2','FILLED');self.assertEqual(self.runq(),'FLAT_CLEAN');self.assertIn('OLD_STOP_FILLED',self.state()['events'])
    def test_child_residual_blocks_drain(self):
        s=self.state();s.update(phase='FLAT_CLEAN',children={'900':self.old});self.store.tr.update(qro_state=qro.encode(s),status='CLOSED')
        self.assertIn('QRO_RESIDUAL_ORDER_RECONCILIATION_REQUIRED',qro.drain_blockers([self.store.tr],[dict(orderId='900',clientOrderId='exchange-generated')]))
    def test_reentry_requires_fresh_zeros(self):
        self.http.qty='0';self.runq();self.http.qty='2'
        self.assertFalse(asyncio.run(self.exchange().check_reentry('BTCUSDT',[self.store.tr],self.store)))
    def test_new_filled_cleanup(self):cid=self.pending();self.http.trigger(cid,'2','FILLED');self.assertEqual(self.runq(),'FLAT_CLEAN');self.assertIn('NEW_STOP_FILLED',self.state()['events'])
    def test_finished_child_missing(self):self.http.algos[self.old]['algoStatus']='FINISHED';self.assert_blocked('TERMINAL_STATE_UNKNOWN')
    def test_fill_ledger_mismatch(self):self.http.trigger(self.old);self.http.fills['900']=[];self.assert_blocked('TERMINAL_STATE_UNKNOWN')
    def test_partial_then_flat_cleanup_on_restart(self):
        self.pending();self.http.trigger(self.old);self.runq()
        self.http.qty='0';self.assertEqual(self.runq(),'FLAT_CLEAN')
        self.assertFalse(self.writes());self.assertFalse(self.state()['residual'])
        self.assertTrue(qro.local_residual(self.store.tr))  # manual review latch remains
    def test_new_initial_stop(self):
        self.store=MemoryStore();self.http=HTTP(self.store)
        self.assertEqual(self.runq(),'REPLACED');self.assertEqual(self.writes()[0][2]['triggerPrice'],'98.7')
    def test_concurrent_workers_one_post(self):
        async def both():return await asyncio.gather(self.exchange().reconcile_trade(7,self.store),self.exchange().reconcile_trade(7,self.store))
        self.assertEqual(asyncio.run(both()),['REPLACED','STABLE']);self.assertEqual(len(self.writes()),1)
    def test_other_profile_no_binding(self):self.store.tr['exit_profile']='CURRENT_TP2';self.assertEqual(self.runq(),'QRO_PROFILE_NOT_BOUND');self.assertFalse(self.http.calls)
    def test_unknown_profile_no_binding(self):self.store.tr['exit_profile']='UNKNOWN';self.assertEqual(self.runq(),'QRO_PROFILE_NOT_BOUND');self.assertFalse(self.http.calls)


class SchemaTests(unittest.TestCase):
    def test_77_78_idempotent_nullable(self):
        with sqlite3.connect(':memory:') as db:
            db.execute('CREATE TABLE autotrade_trades(id INTEGER, exit_profile TEXT)')
            db.execute("INSERT INTO autotrade_trades VALUES(1,'CURRENT_TP2')")
            sl.migrate(db); first=list(db.execute('PRAGMA table_info(autotrade_trades)'))
            sl.migrate(db);self.assertEqual(first,list(db.execute('PRAGMA table_info(autotrade_trades)')))
            self.assertEqual(len(first),20);self.assertTrue(all(r[3]==0 for r in first))
            row=db.execute('SELECT exit_profile,qro_state,qro_contract_version FROM autotrade_trades').fetchone()
            self.assertEqual(row,('CURRENT_TP2',None,None))
    def test_79_old_schema_read(self):self.assertFalse(qro.local_residual(dict(status='CLOSED',exit_profile='CURRENT_TP2')))
    def test_sqlite_commit_cas_and_restart(self):
        with tempfile.TemporaryDirectory() as td:
            p=str(pathlib.Path(td)/'test.db')
            with sqlite3.connect(p) as db:
                db.execute('CREATE TABLE autotrade_trades(id INTEGER, qro_contract_version TEXT, qro_state TEXT)')
                tr=trade();db.execute('INSERT INTO autotrade_trades VALUES(?,?,?)',(7,qro.CONTRACT,tr['qro_state']))
            db.close()
            store=qro.SQLiteStore(lambda:sqlite3.connect(p));s=json.loads(tr['qro_state']);s['phase']='INTENT'
            state,raw=store.save(7,tr['qro_state'],s)
            self.assertEqual(json.loads(qro.SQLiteStore(lambda:sqlite3.connect(p)).load(7)['qro_state'])['phase'],'INTENT')
            with self.assertRaisesRegex(qro.Block,'CONCURRENT'):store.save(7,tr['qro_state'],s)


class BindingTests(unittest.TestCase):
    def setUp(self):
        import test_v5135 as legacy
        legacy.DatabaseCase.setUp(self)
        self.sql=legacy.DatabaseCase.sql.__get__(self)
        self.rows=legacy.DatabaseCase.rows.__get__(self)
    def insert(self):
        import bot
        tr=trade();tr.update(margin_usdt=20,leverage=10,notional_usdt=200,updated_ts=1);keys=list(tr)
        self.sql('INSERT INTO autotrade_trades ('+','.join(keys)+') VALUES ('+','.join('?' for _ in keys)+')',list(tr.values()))
        return tr
    def test_binding_is_default_disabled(self):
        import bot
        tr=self.insert()
        with patch.object(bot,'binance_signed_request',new_callable=AsyncMock) as request:
            asyncio.run(bot._at_step_recovery_check(None,tr,[]))
            request.assert_not_awaited()
        self.assertEqual(self.rows('SELECT last_error FROM autotrade_trades')[0]['last_error'],'QRO_ADAPTER_DISABLED')
    def test_binding_uses_real_adapter_and_sqlite(self):
        import bot
        tr=self.insert();store=bot._at_qro_store();http=HTTP(store)
        # Real store readback verifies durable state at the instant of POST.
        class Readback:
            @property
            def tr(self):return store.load(7)
        http.store=Readback()
        async def request(session,method,route,params,**kwargs):return await http(method,route,params)
        with patch.object(qro,'MANAGEMENT_ENABLED',True),patch.object(bot,'binance_signed_request',side_effect=request),patch.object(qro.time,'time',return_value=100):
            x=bot._at_qro_exchange(None);x.clock=lambda:1;x.wall=lambda:100
            with patch.object(bot,'_at_qro_exchange',return_value=x):asyncio.run(bot._at_step_recovery_check(None,tr,[]))
        self.assertEqual(json.loads(store.load(7)['qro_state'])['phase'],'STABLE')
        self.assertEqual(sum(c[0]=='POST' for c in http.calls),1)
    def test_reentry_guard_existing_qro(self):
        import bot
        self.insert();self.assertFalse(asyncio.run(bot._at_qro_reentry_safe(None,'BTCUSDT')))
    def test_existing_profiles_no_qro_calls(self):
        import bot
        with patch.object(bot,'_at_qro_exchange') as factory:
            self.assertTrue(asyncio.run(bot._at_qro_reentry_safe(None,'BTCUSDT')))
            factory.assert_not_called()
    def test_frozen_qro_version_rejects_mutation(self):
        import bot
        self.insert()
        with self.assertRaisesRegex(ValueError,'FROZEN'):bot._at_update_trade(7,qro_contract_version=None)
    def test_all_other_bot_functions_byte_semantic_equal(self):
        root=pathlib.Path(__file__).resolve().parents[1]
        m=json.loads((root/'tests/premium_step_lock_noninterference.json').read_text())['qro_consolidation']
        fs={n.name:n for n in ast.parse((root/'binance_momentum_bot/bot.py').read_text(encoding='utf-8')).body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))}
        for n,h in m['premium_function_hashes'].items():
            if n not in m['changed']:self.assertEqual(hashlib.sha256(ast.dump(fs[n],include_attributes=False).encode()).hexdigest(),h,n)
        self.assertEqual(m['unchanged_premium_functions'],227)
    def test_residual_veto_before_live_snapshot(self):
        import inspect,bot
        src=inspect.getsource(bot.autotrade_handle_premium.__wrapped__ if hasattr(bot.autotrade_handle_premium,'__wrapped__') else bot.autotrade_handle_premium)
        self.assertLess(src.index('await _at_qro_reentry_safe'),src.index('await _at_account_snapshot'))


# Exact response-field corruption cases use the same HTTP adapter and frozen contract.
def corrupt_test(key,value):
    def test(self):
        self.http.algos[self.old][key]=value
        result=self.runq();self.assertIn(result,{'QRO_ORDER_IDENTITY_MISMATCH','QRO_ORDER_SEMANTICS_MISMATCH','INVALID_DECIMAL'})
        self.assertFalse(self.writes());self.assertFalse(self.writes('DELETE'))
    return test
for _key,_value in [('side','BUY'),('positionSide','LONG'),('reduceOnly',False),('closePosition',True),
                   ('quantity','1'),('triggerPrice','98'),('workingType','MARK_PRICE'),('orderType','TAKE_PROFIT_MARKET'),
                   ('algoType','BAD'),('priceProtect',True),('algoId','foreign'),('clientAlgoId','QRO1-8-0')]:
    setattr(QROTests,'test_exact_field_'+_key,corrupt_test(_key,_value))
