"""No-network Premium fixtures. Fake exchange tests prove protocol, not Binance capability."""
import asyncio
from copy import deepcopy
from decimal import Decimal
import sqlite3
import ast
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch
import premium_step_lock as sl


def trade():
    return dict(id=7, symbol="BTCUSDT", side="LONG", position_side="BOTH", mode="LIVE",
                status="OPEN", exit_profile=sl.PROFILE, expected_qty="2", stop_algo_id="old",
                stop_client_id=sl.client_id(7, 0), **sl.entry_fields("100", "98.7", 1000))


def order(tr, cid, target, oid="old", status="NEW"):
    return dict(algoId=oid, clientAlgoId=cid, symbol=tr["symbol"], side="SELL",
                positionSide=tr["position_side"], orderType="STOP_MARKET", workingType="CONTRACT_PRICE",
                closePosition=True, quantity="0", triggerPrice=str(target), algoStatus=status)


class CalculatorTests(unittest.TestCase):
    def test_exact_noninterference_function_manifest(self):
        root=Path(__file__).resolve().parents[1]
        manifest=json.loads((root/'tests/premium_step_lock_noninterference.json').read_text())
        tree=ast.parse((root/'binance_momentum_bot/bot.py').read_text(encoding='utf-8'))
        functions={n.name:n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))}
        self.assertEqual(set(functions),set(manifest['functions'])|set(manifest['added_functions']))
        for name,hashes in manifest['functions'].items():
            with self.subTest(name=name):
                actual=hashlib.sha256(ast.dump(functions[name],include_attributes=False).encode()).hexdigest()
                self.assertEqual(hashes['candidate'],actual)
                if name not in manifest['changed_functions']: self.assertEqual(hashes['base'],actual)
    def test_profile_spec_file_hash(self):
        root=Path(__file__).resolve().parents[1]
        raw=(root/'docs/premium_step_lock/Premium_Step_Lock_V1_Profile_Spec.json').read_bytes().replace(b'\r\n',b'\n')
        self.assertEqual(raw,sl.SPEC_BYTES)
        self.assertEqual(hashlib.sha256(raw).hexdigest(),sl.CONTRACT_HASH)
    def check(self, price, trigger, lock):
        t, l, stop = sl.levels("100", price, "0.000001")
        self.assertEqual((t,l), (trigger,lock))
        self.assertEqual(stop, None if not trigger else Decimal(100)*(1+Decimal(lock)/10000))
    def test_01_below_first(self): self.check("100.499999",0,0)
    def test_02_exact_first(self): self.check("100.50",50,25)
    def test_03_above_first(self): self.check("100.500001",50,25)
    def test_04_below_second(self): self.check("100.999999",50,25)
    def test_05_exact_second(self): self.check("101",100,50)
    def test_05b_above_second(self): self.check("101.000001",100,50)
    def test_06_third(self): self.check("101.5",150,100)
    def test_07_jump(self): self.check("102.60",250,200)
    def test_08_maximum(self): self.check("110",1000,950)
    def test_08b_below_maximum(self): self.check("109.999999",950,900)
    def test_08c_above_maximum(self): self.check("110.000001",1000,950)
    def test_09_above_cap(self): self.check("115",1000,950)
    def test_13_round_down(self):
        _,_,stop=sl.levels("1.003","1.009","0.001")
        self.assertEqual(stop,Decimal("1.005"))
        self.assertLessEqual(stop,Decimal("1.003")*Decimal("1.0025"))
    def test_invalid_numbers(self):
        for v in (0,-1,"NaN","Infinity",True,None):
            with self.subTest(v=v), self.assertRaises(ValueError): sl.levels("100",v,"0.1")
    def test_all_frozen_steps(self):
        for t in range(50,1001,50):
            self.check(str(Decimal(100)+Decimal(t)/100),t,25 if t==50 else t-50)


class StateTests(unittest.TestCase):
    def setUp(self): self.tr=trade()
    def tick(self,p,ts=2000,tick="0.01",last=0):
        fields=sl.tick_fields(self.tr,p,ts,tick,last_seen_ms=last)
        self.tr.update(fields)
        return fields
    def test_10_retrace_never_lowers(self):
        self.tick("102.6"); before=deepcopy(self.tr)
        self.assertEqual(self.tick("101",3000),{})
        self.assertEqual(self.tr,before)
    def test_11_duplicate(self):
        self.tick("102.6"); self.assertEqual(self.tick("102.6"),{})
    def test_12_out_of_order(self):
        self.tick("101",3000); self.assertEqual(self.tick("103",2000),{})
        self.assertEqual(self.tick("103",4000,last=5000),{})
    def test_pre_entry(self): self.assertEqual(self.tick("110",999),{})
    def test_14_rounded_equal_no_target(self):
        self.tr.update(sl_initial_stop="99",sl_active_stop_price="99",desired_stop_price="99")
        fields=self.tick("100.5",tick="3")
        self.assertNotIn("desired_stop_price",fields)
        self.assertEqual(fields["highest_trigger_bp"],50)
    def test_15_initial_stop_preserved(self):
        self.assertEqual(self.tick("100.499999"),{})
        self.assertEqual(self.tr["sl_active_stop_price"],"98.7")
    def test_32_unknown_profile(self):
        for v in ("OTHER","",None):
            with self.assertRaisesRegex(ValueError,sl.UNKNOWN): sl.profile(v)
        self.assertEqual(sl.profile(None,legacy=True),"CURRENT_TP2")
    def test_33_34_35_additive_idempotent_migration(self):
        with sqlite3.connect(":memory:") as db:
            db.execute("CREATE TABLE autotrade_trades(id INTEGER,exit_profile TEXT,stop_price REAL)")
            db.execute("INSERT INTO autotrade_trades VALUES(1,NULL,98.7)")
            sl.migrate(db); before=list(db.execute("PRAGMA table_info(autotrade_trades)"))
            sl.migrate(db)
            self.assertEqual(before,list(db.execute("PRAGMA table_info(autotrade_trades)")))
            row=db.execute("SELECT exit_profile,stop_price,highest_trigger_bp FROM autotrade_trades").fetchone()
            self.assertEqual(row,(None,98.7,None))
    def test_36_rollback_open_unsafe(self):
        out=sl.rollback_readiness([self.tr],mode="OFF",global_profile="CURRENT_TP2",exchange_orders=[])
        self.assertIn("ROLLBACK_UNSAFE_OPEN_STEP_LOCK_POSITION",out["blockers"])
    def test_37_rollback_drained_safe(self):
        self.tr["status"]="CLOSED"
        self.assertTrue(sl.rollback_readiness([self.tr],mode="OFF",global_profile="CURRENT_TP2",exchange_orders=[])["rollback_safe"])
    def test_rollback_requires_exchange_evidence(self):
        self.assertFalse(sl.rollback_readiness([],mode="OFF",global_profile="CURRENT_TP2")["rollback_safe"])
        self.assertFalse(sl.rollback_readiness([],mode="OFF",global_profile="CURRENT_TP2",exchange_orders=[{"clientAlgoId":"PSL1-7-1"}])["rollback_safe"])
    def test_rollback_empty_profile_is_unknown(self):
        self.tr['exit_profile']=''
        self.assertIn(sl.UNKNOWN,sl.rollback_readiness([self.tr],mode='OFF',global_profile='CURRENT_TP2',exchange_orders=[])['blockers'])
    def test_38_no_revision_after_cap(self):
        self.tick("110"); before=deepcopy(self.tr)
        self.assertEqual(self.tick("115",3000),{}); self.assertEqual(self.tr,before)
    def test_contract_tamper_fails_closed(self):
        self.tr["profile_contract_hash"]="tampered"
        with self.assertRaisesRegex(ValueError,"PROFILE_CONTRACT_MISMATCH"): self.tick("101")


class FakeExchange:
    """Explicit model of ACK/cancel uncertainty; no claim of actual overlap support."""
    def __init__(self,tr,events,load):
        self.events,self.load=events,load
        self.orders={tr["stop_client_id"]:order(tr,tr["stop_client_id"],tr["sl_active_stop_price"])}
        self.position=[dict(symbol=tr["symbol"],positionSide=tr["position_side"],positionAmt="2")]
        self.post_timeout=False; self.lose_post=False; self.cancel_uncertain=False; self.cancel_timeout=False
        self.market="103"; self.before_post=None
    async def positions(self): return deepcopy(self.position)
    async def open_orders(self,symbol): return [deepcopy(o) for o in self.orders.values() if o['algoStatus']=='NEW']
    async def price_filter_and_market(self,symbol): return "0.01",self.market
    async def query(self,cid):
        self.events.append(("QUERY",cid))
        return deepcopy(self.orders.get(cid))
    async def post(self,tr,cid,target):
        self.events.append(("POST",cid))
        saved=self.load(tr["id"])
        assert saved["pending_stop_client_id"]==cid and saved["replacement_status"]=="INTENT"
        if self.before_post: self.before_post()
        if not self.lose_post: self.orders[cid]=order(tr,cid,target,"new")
        if self.post_timeout: raise TimeoutError("POST response lost")
    async def cancel(self,oid):
        self.events.append(("CANCEL",oid))
        if not self.cancel_uncertain:
            for o in self.orders.values():
                if o["algoId"]==oid: o["algoStatus"]="CANCELED"
        if self.cancel_timeout: raise TimeoutError("cancel response lost")


class ReplacementTests(unittest.TestCase):
    def setUp(self):
        self.tr=trade(); self.tr.update(sl.tick_fields(self.tr,"102.6",2000,"0.01"))
        self.events=[]
        self.exchange=FakeExchange(self.tr,self.events,self.load)
    def load(self,tid): return deepcopy(self.tr)
    def persist(self,tid,**fields):
        self.events.append(("COMMIT",deepcopy(fields))); self.tr.update(fields)
    def run_reconcile(self,proven=True):
        return asyncio.run(sl.reconcile(7,self.load,self.persist,self.exchange,overlap_proven=proven))
    def test_22_25_intent_post_ack_cancel_order(self):
        self.assertEqual(self.run_reconcile(),"REPLACED")
        kinds=[x[0] for x in self.events]
        self.assertLess(kinds.index("COMMIT"),kinds.index("POST"))
        ack=next(i for i,x in enumerate(self.events) if x[0]=="COMMIT" and x[1].get("replacement_status")=="NEW_CONFIRMED")
        self.assertLess(ack,kinds.index("CANCEL"))
        self.assertEqual(self.tr["sl_active_stop_price"],"102.00")
    def test_23_timeout_query_same_id(self):
        self.exchange.post_timeout=True
        self.assertEqual(self.run_reconcile(),"REPLACED")
        self.assertEqual(sum(x[0]=="POST" for x in self.events),1)
    def test_24_no_ack_keeps_old(self):
        self.exchange.post_timeout=True; self.exchange.lose_post=True
        self.assertEqual(self.run_reconcile(),"QUERY_UNCERTAIN")
        self.assertNotIn("CANCEL",[x[0] for x in self.events])
        self.assertEqual(self.tr["stop_algo_id"],"old")
    def test_26_27_cancel_uncertain_no_third(self):
        self.exchange.cancel_uncertain=True
        self.assertEqual(self.run_reconcile(),"CANCEL_UNCERTAIN")
        self.assertEqual(self.run_reconcile(),"CANCEL_UNCERTAIN")
        self.assertEqual(sum(x[0]=="POST" for x in self.events),1)
        self.assertEqual(len(self.exchange.orders),2)
    def test_cancel_response_lost_but_canceled(self):
        self.exchange.cancel_timeout=True
        self.assertEqual(self.run_reconcile(),"REPLACED")
    def test_28_restart_pending_query_only(self):
        self.exchange.lose_post=True; self.exchange.post_timeout=True
        self.run_reconcile(); self.events.clear()
        self.assertEqual(self.run_reconcile(),"QUERY_UNCERTAIN")
        self.assertNotIn("POST",[x[0] for x in self.events])
    def test_29_restart_after_cancel_before_commit(self):
        self.exchange.cancel_uncertain=True; self.run_reconcile()
        self.exchange.orders[sl.client_id(7,0)]["algoStatus"]="CANCELED"
        self.exchange.cancel_uncertain=False; self.events.clear()
        self.assertEqual(self.run_reconcile(),"REPLACED")
        self.assertNotIn("POST",[x[0] for x in self.events])
    def test_30_stop_mismatch(self):
        self.exchange.orders[sl.client_id(7,0)]["triggerPrice"]="98"
        with self.assertRaisesRegex(ValueError,"OWNERSHIP"): self.run_reconcile()
        self.assertNotIn("POST",[x[0] for x in self.events])
    def test_31_manual_qty_mismatch(self):
        self.exchange.position[0]["positionAmt"]="3"
        with self.assertRaisesRegex(ValueError,"MANUAL"): self.run_reconcile()
        self.assertEqual(self.events,[])
    def test_manual_side_mismatch(self):
        self.exchange.position[0]["positionSide"]="SHORT"
        with self.assertRaisesRegex(ValueError,"MANUAL"): self.run_reconcile()
    def test_unknown_extra_order_blocks(self):
        self.exchange.orders['manual']=order(self.tr,'manual','99','manual')
        with self.assertRaisesRegex(ValueError,"UNOWNED"): self.run_reconcile()
    def test_capability_unproven_never_posts_or_cancels(self):
        self.assertEqual(self.run_reconcile(False),sl.LIVE_BLOCK_REASON)
        self.assertFalse(any(x[0] in ('POST','CANCEL') for x in self.events))
    def test_market_retrace_keeps_old(self):
        self.exchange.market="101.9"
        self.assertEqual(self.run_reconcile(),"INVALID_TARGET_KEEP_OLD_STOP")
        self.assertFalse(any(x[0] in ('POST','CANCEL') for x in self.events))
    def test_concurrent_new_desired_does_not_overwrite_pending(self):
        self.exchange.before_post=lambda:self.tr.update(sl.tick_fields(self.tr,"104",3000,"0.01"))
        self.assertEqual(self.run_reconcile(),"REPLACED")
        self.assertEqual(self.tr["desired_stop_price"],"103.50")
        self.assertEqual(self.tr["sl_active_stop_price"],"102.00")
        self.assertEqual(self.tr["replacement_status"],"DESIRED")
    def test_hedge_close_position_semantics(self):
        self.tr['position_side']='LONG'
        self.exchange=FakeExchange(self.tr,self.events,self.load)
        self.assertEqual(self.run_reconcile(),"REPLACED")
    def test_ack_identity_side_price_and_close_semantics_required(self):
        for key,value in [('symbol','OTHER'),('side','BUY'),('positionSide','SHORT'),('triggerPrice','103'),('closePosition',False),('quantity','2'),('algoStatus','REJECTED'),('clientAlgoId','unowned')]:
            with self.subTest(key=key):
                self.setUp()
                original=self.exchange.post
                async def invalid(tr,cid,target):
                    await original(tr,cid,target)
                    self.exchange.orders[cid][key]=value
                self.exchange.post=invalid
                self.assertEqual(self.run_reconcile(),'QUERY_UNCERTAIN')
                self.assertFalse(any(x[0]=='CANCEL' for x in self.events))
    def test_restart_after_intent_before_post_does_not_repost(self):
        self.persist(7,pending_stop_client_id=sl.client_id(7,1),pending_stop_price='102',stop_revision=1,replacement_status='INTENT')
        self.assertEqual(self.run_reconcile(),'QUERY_UNCERTAIN')
        self.assertFalse(any(x[0] in ('POST','CANCEL') for x in self.events))
    def test_new_stop_filled_before_cancel_keeps_old(self):
        original=self.exchange.query
        counts={}
        async def changed(cid):
            counts[cid]=counts.get(cid,0)+1
            if cid==sl.client_id(7,1) and counts[cid]>1: self.exchange.orders[cid]['algoStatus']='FINISHED'
            return await original(cid)
        self.exchange.query=changed
        with self.assertRaisesRegex(ValueError,'STOP_NOT_WORKING'): self.run_reconcile()
        self.assertFalse(any(x[0]=='CANCEL' for x in self.events))


if __name__ == "__main__": unittest.main()
