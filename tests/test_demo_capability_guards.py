"""Offline prerequisite guards, distinct from exchange capability evidence."""
import importlib.util
import pathlib
import tempfile
import unittest
from copy import deepcopy
from decimal import Decimal
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('demo_capability',pathlib.Path(__file__).resolve().parents[1]/'research'/'premium_step_lock_demo_capability.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
CID='DPSL-0123456789ab-old'
def stop():
    return dict(algoId=1,clientAlgoId=CID,symbol='BTCUSDT',side='SELL',positionSide='BOTH',
                orderType='STOP_MARKET',algoType='CONDITIONAL',closePosition=True,quantity='0',
                workingType='CONTRACT_PRICE',triggerPrice='98.7',algoStatus='NEW')
class Guards(unittest.TestCase):
    def deny(self,fun,*args):
        with self.assertRaises(m.Blocked):fun(*args)
    def wrong(self,field,value):
        s=stop();s[field]=value;self.deny(m.verify_stop,s,CID,'98.7')
    def test_01_production_before_network(self):
        with patch.object(m.socket,'getaddrinfo') as network:
            for h in m.PRODUCTION_DENY:self.deny(m.validate_target,'https://'+h,'POST','/fapi/v1/algoOrder')
            network.assert_not_called()
    def test_02_redirect_production_never_followed(self):
        for c in (301,302,303,307,308):self.deny(m.check_redirect,c)
    def test_03_http_denied(self):self.deny(m.validate_target,'http://'+m.HOST,'GET','/fapi/v1/time')
    def test_04_custom_host_denied(self):self.deny(m.validate_target,'https://example.com','GET','/fapi/v1/time')
    def test_05_userinfo_denied(self):self.deny(m.validate_target,'https://a@'+m.HOST,'GET','/fapi/v1/time')
    def test_06_port_denied(self):self.deny(m.validate_target,m.BASE+':444','GET','/fapi/v1/time')
    def test_07_route_denied(self):self.deny(m.validate_target,m.BASE,'POST','/fapi/v1/positionSide/dual')
    def test_08_modify_denied(self):self.deny(m.validate_target,m.BASE,'PUT','/fapi/v1/algoOrder')
    def test_09_local_dns_denied(self):self.deny(m.check_addresses,[(2,1,6,'',('127.0.0.1',443))])
    def test_10_mixed_dns_denied(self):self.deny(m.check_addresses,[(2,1,6,'',('8.8.8.8',443)),(2,1,6,'',('10.0.0.1',443))])
    def test_11_missing_credentials(self):self.deny(m.credentials,{})
    def test_12_no_generic_fallback(self):self.deny(m.credentials,{'BINANCE_API_KEY':'fixture','BINANCE_API_SECRET':'fixture'})
    def test_13_half_credentials_denied(self):self.deny(m.credentials,{m.KEY_NAMES[0]:'fixture'})
    def test_14_header_injection_denied(self):self.deny(m.credentials,{m.KEY_NAMES[0]:'fixture\n',m.KEY_NAMES[1]:'fixture'})
    def test_15_one_way_only(self):
        m.require_mode({'dualSidePosition':False});self.deny(m.require_mode,{'dualSidePosition':True})
    def test_16_dirty_position(self):self.deny(m.require_clean,m.clean_counts([{'positionAmt':'0.001'}],[],[]))
    def test_17_dirty_normal(self):self.deny(m.require_clean,m.clean_counts([],[{}],[]))
    def test_18_dirty_algo(self):self.deny(m.require_clean,m.clean_counts([],[],[{}]))
    def test_19_final_pending_not_clean(self):self.deny(m.require_clean,{'position_count':0,'pending_local_intent':1})
    def test_20_final_unresolved_not_clean(self):self.deny(m.require_clean,{'position_count':0,'unresolved_client_id':1})
    def test_21_nan_position_denied(self):self.deny(m.clean_counts,[{'positionAmt':'NaN'}],[],[])
    def test_22_first_stop_exact(self):self.assertEqual(m.verify_stop(stop(),CID,'98.7'),'1')
    def test_23_second_stop_exact(self):
        s=stop();s.update(algoId=2,clientAlgoId='DPSL-0123456789ab-new',triggerPrice='99')
        self.assertEqual(m.verify_stop(s,s['clientAlgoId'],'99'),'2')
    def test_24_unowned_denied(self):self.wrong('clientAlgoId','some-other-order')
    def test_25_symbol_denied(self):self.wrong('symbol','ETHUSDT')
    def test_26_buy_denied(self):self.wrong('side','BUY')
    def test_27_hedge_not_transferred(self):self.wrong('positionSide','LONG')
    def test_28_quantity_stop_denied(self):self.wrong('quantity','0.01')
    def test_29_close_all_required(self):self.wrong('closePosition',False)
    def test_30_wrong_working_type(self):self.wrong('workingType','MARK_PRICE')
    def test_31_wrong_trigger(self):self.wrong('triggerPrice','98.8')
    def test_32_terminal_not_working(self):self.wrong('algoStatus','CANCELED')
    def test_33_unknown_not_working(self):self.wrong('algoStatus','UNRECOGNIZED')
    def test_34_both_same_snapshot(self):
        a=stop();b=stop();b.update(algoId=2,clientAlgoId='DPSL-0123456789ab-new',triggerPrice='99')
        known={CID:{'target':'98.7','algo_id':'1'},b['clientAlgoId']:{'target':'99','algo_id':'2'}}
        m.verify_both([a,b],known)
        self.deny(m.verify_both,[a],known);self.deny(m.verify_both,[a,a],known)
    def test_35_extra_unowned_blocks(self):
        a=stop();self.deny(m.verify_both,[a,a,dict(clientAlgoId='unowned')],{CID:{'target':'98.7','algo_id':'1'}})
    def test_36_no_duplicate_post(self):
        with tempfile.TemporaryDirectory() as d:
            calls=[];r=m.Run(d,'0123456789ab',lambda *args:calls.append(args))
            r.posted.add(CID);self.deny(r.create_stop,CID,Decimal('98.7'));self.assertEqual(calls,[])
    def test_37_no_third_stop(self):
        with tempfile.TemporaryDirectory() as d:
            calls=[];r=m.Run(d,'0123456789ab',lambda *args:calls.append(args))
            r.state['stops']={'a':{},'b':{}};self.deny(r.create_stop,CID,Decimal('98.7'));self.assertEqual(calls,[])
    def test_38_evidence_never_overwritten(self):
        with tempfile.TemporaryDirectory() as d:
            m.Run(d,'0123456789ab',lambda *a:None)
            with self.assertRaises(FileExistsError):m.Run(d,'0123456789ab',lambda *a:None)
    def test_39_no_secret_in_durable_state(self):
        with tempfile.TemporaryDirectory() as d:
            r=m.Run(d,'0123456789ab',lambda *a:None);r.key='dummy-key-sensitive';r.secret='dummy-secret-sensitive';r.save()
            output=''.join(p.read_text() for p in pathlib.Path(d).iterdir())
            self.assertNotIn(r.key,output);self.assertNotIn(r.secret,output)
    def test_40_runtime_not_imported(self):
        import ast
        tree=ast.parse(pathlib.Path(m.__file__).read_text())
        names=[n.module or '' for n in ast.walk(tree) if isinstance(n,ast.ImportFrom)]
        names.extend(a.name for n in ast.walk(tree) if isinstance(n,ast.Import) for a in n.names)
        self.assertFalse(set(names)&{'bot','dotenv','sqlite3','telegram'})

class FakeDemo:
    def __init__(self,reject_second=False):
        self.orders={};self.algos={};self.qty='0';self.calls=[];self.reject_second=reject_second
    def __call__(self,method,route,p):
        self.calls.append((method,route,deepcopy(p)))
        if route.endswith('/time'):return {'serverTime':1000000}
        if route.endswith('/positionSide/dual'):return {'dualSidePosition':False}
        if route.endswith('/positionRisk'):return [{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':self.qty}]
        if route.endswith('/openOrders'):return []
        if route.endswith('/openAlgoOrders'):return [deepcopy(s) for s in self.algos.values() if s['algoStatus']=='NEW']
        if route.endswith('/symbolConfig'):return [{'symbol':'BTCUSDT','marginType':'ISOLATED','leverage':1}]
        if route.endswith('/ticker/price'):return {'price':'100000'}
        if route.endswith('/exchangeInfo'):return {'symbols':[{'symbol':'BTCUSDT','status':'TRADING','contractType':'PERPETUAL','filters':[
            {'filterType':'PRICE_FILTER','tickSize':'0.1'},
            {'filterType':'MIN_NOTIONAL','notional':'100'},
            {'filterType':'LOT_SIZE','stepSize':'0.001','minQty':'0.001','maxQty':'100'},
            {'filterType':'MARKET_LOT_SIZE','stepSize':'0.001','minQty':'0.001','maxQty':'100'}]}]}
        if route.endswith('/order'):
            if method=='POST':
                cid=p['newClientOrderId'];self.qty=p['quantity'] if p['side']=='BUY' else '0'
                self.orders[cid]=dict(p,clientOrderId=cid,status='FILLED',executedQty=p['quantity'],avgPrice='100000',orderId=len(self.orders)+1)
                return deepcopy(self.orders[cid])
            return deepcopy(self.orders[p['origClientOrderId']])
        if route.endswith('/algoOrder'):
            if method=='POST':
                if self.reject_second and len(self.algos)==1:raise m.ExchangeError(400,-4130)
                cid=p['clientAlgoId'];self.algos[cid]=dict(p,algoId=len(self.algos)+1,orderType=p['type'],quantity='0',algoStatus='NEW')
                return deepcopy(self.algos[cid])
            if method=='DELETE':
                s=next(v for v in self.algos.values() if str(v['algoId'])==str(p['algoId']))
                s['algoStatus']='CANCELED';return {'code':'200'}
            if p['clientAlgoId'] not in self.algos:raise m.ExchangeError(400,-2013)
            return deepcopy(self.algos[p['clientAlgoId']])
        raise AssertionError('unexpected fixture route')

class Flow(unittest.TestCase):
    def test_standard_ordering_and_cleanup(self):
        with tempfile.TemporaryDirectory() as d:
            ex=FakeDemo();r=m.Run(d,'0123456789ab',ex);result=r.execute()
            self.assertEqual(result['cycle_result'],'STANDARD_OVERLAP_PASS')
            self.assertEqual(result['final_cleanup']['status'],'PASS')
            self.assertTrue(result['new_confirmed_before_old_cancel'])
            self.assertEqual(result['algo_posts'],2)
            cancel=next(i for i,c in enumerate(ex.calls) if c[0]=='DELETE')
            creates=[i for i,c in enumerate(ex.calls) if c[0]=='POST' and c[1].endswith('/algoOrder')]
            self.assertTrue(all(i<cancel for i in creates))
            self.assertTrue(any(c[1].endswith('/openAlgoOrders') for c in ex.calls[creates[-1]+1:cancel]))
    def test_second_rejection_stops_and_cleans(self):
        with tempfile.TemporaryDirectory() as d:
            ex=FakeDemo(True);result=m.Run(d,'0123456789ab',ex).execute()
            self.assertEqual(result['cycle_result'],'SECOND_CLOSEPOSITION_STOP_REJECTED')
            self.assertEqual(result['final_cleanup']['status'],'PASS')
            self.assertEqual(result['entry_posts'],1)
            self.assertFalse(result['overlap_observed'])
            self.assertEqual(result['pending'],[]);self.assertEqual(result['unresolved'],[])
            self.assertEqual(result['stops']['DPSL-0123456789ab-new']['status'],'REJECTION_AND_EXACT_ABSENCE_VERIFIED')

if __name__=='__main__':unittest.main()
