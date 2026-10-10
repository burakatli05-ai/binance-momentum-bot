"""R3 falsification cases. All transport is synthetic; no exchange proof."""
import json,pathlib,tempfile,unittest
from copy import deepcopy
from decimal import Decimal
from test_demo_capability_guards import FakeDemo,m

class EntryDemo(FakeDemo):
    def __init__(self,post='reject',query='absent',dirty=None,balance='1000'):
        super().__init__();self.post=post;self.query=query;self.dirty=dirty;self.balance=balance;self.attempted=False
    def __call__(self,method,route,p):
        if route.endswith('/balance'):
            self.calls.append((method,route,deepcopy(p)))
            return [{'asset':'USDT','availableBalance':self.balance}]
        if route.endswith('/order') and method=='POST' and p['side']=='BUY':
            self.attempted=True
            if self.post=='lost_filled':
                super().__call__(method,route,p);raise TimeoutError('fixture')
            if self.post=='contradictory':super().__call__(method,route,p)
            else:self.calls.append((method,route,deepcopy(p)))
            if self.post=='timeout':raise TimeoutError('fixture')
            if self.post=='unknown':raise m.ExchangeError(503,-1007)
            raise m.ExchangeError(400,-2019)
        if route.endswith('/order') and method=='GET' and self.post not in ('lost_filled','contradictory'):
            self.calls.append((method,route,deepcopy(p)))
            if self.query=='unavailable':raise m.ExchangeError(503,-1007)
            raise m.ExchangeError(400,-2013)
        if self.attempted and self.dirty:
            if route.endswith('/positionRisk') and self.dirty=='position':
                self.calls.append((method,route,deepcopy(p)));return [{'symbol':'BTCUSDT','positionSide':'BOTH','positionAmt':'0.001'}]
            if route.endswith('/openOrders') and self.dirty=='normal':
                self.calls.append((method,route,deepcopy(p)));return [{'clientOrderId':'unowned'}]
            if route.endswith('/openAlgoOrders') and self.dirty=='algo':
                self.calls.append((method,route,deepcopy(p)));return [{'clientAlgoId':'unowned'}]
        return super().__call__(method,route,p)

class EntryRejection(unittest.TestCase):
    def run_case(self,ex):
        with tempfile.TemporaryDirectory() as d:
            r=m.Run(d,'0123456789ab',ex);v=r.execute()
            saved=json.loads((pathlib.Path(d)/'local_state.json').read_text())
            self.assertEqual(saved,v)
            return v
    def writes(self,ex):return [c for c in ex.calls if c[0]!='GET']
    def test_explicit_rejection_absence_clean_resolves_without_retry(self):
        ex=EntryDemo();v=self.run_case(ex)
        self.assertEqual(v['cycle_result'],'ENTRY_REJECTED_CLEANLY_RESOLVED')
        self.assertEqual(v['entry']['post_exchange_code'],-2019)
        self.assertEqual(v['entry']['status'],'REJECTED_NO_ORDER_CREATED')
        self.assertEqual(v['pending'],[]);self.assertEqual(v['unresolved'],[])
        self.assertEqual(v['final_cleanup']['status'],'PASS')
        self.assertEqual(len(self.writes(ex)),1);self.assertEqual(v['algo_posts'],0)
    def test_zero_balance_prevents_every_write(self):
        ex=EntryDemo(balance='0');v=self.run_case(ex)
        self.assertEqual(v['reason'],'INSUFFICIENT_DEMO_BALANCE')
        self.assertEqual(self.writes(ex),[]);self.assertIsNone(v['entry'])
        self.assertEqual(v['final_cleanup']['status'],'PASS')
    def test_insufficient_balance_reserve_prevents_writes(self):
        ex=EntryDemo(balance='100');v=self.run_case(ex)
        self.assertEqual(self.writes(ex),[]);self.assertEqual(v['entry_posts'],0)
    def test_required_funding_boundary(self):
        self.assertEqual(m.funding_for([{'asset':'USDT','availableBalance':'101'}],Decimal('100')),(Decimal('101'),Decimal('101')))
    def test_invalid_balance_identity_or_nonfinite_denied(self):
        for balances in ([],[{'asset':'USDT','availableBalance':'NaN'}],[{'asset':'USDT','availableBalance':'1000'}]*2):
            with self.subTest(balances=balances),self.assertRaises(m.Blocked):m.funding_for(balances,Decimal('100'))
    def assert_unresolved(self,ex):
        v=self.run_case(ex)
        self.assertIsNone(v['final_cleanup']);self.assertEqual(len(v['pending']),1)
        self.assertEqual(len(v['unresolved']),1);self.assertEqual(len(self.writes(ex)),1)
        self.assertFalse(v['overlap_observed']);self.assertEqual(v['algo_posts'],0)
        return v
    def test_timeout_plus_absence_is_not_rejection_proof(self):self.assert_unresolved(EntryDemo(post='timeout'))
    def test_unknown_exchange_error_plus_absence_stays_unresolved(self):self.assert_unresolved(EntryDemo(post='unknown'))
    def test_query_uncertainty_is_not_absence(self):self.assert_unresolved(EntryDemo(query='unavailable'))
    def test_position_after_reject_blocks_and_is_not_closed(self):self.assert_unresolved(EntryDemo(dirty='position'))
    def test_normal_order_after_reject_blocks(self):self.assert_unresolved(EntryDemo(dirty='normal'))
    def test_algo_order_after_reject_blocks(self):self.assert_unresolved(EntryDemo(dirty='algo'))
    def test_contradictory_filled_query_after_reject_not_accepted(self):
        v=self.assert_unresolved(EntryDemo(post='contradictory'))
        self.assertEqual(v['reason'],'REJECTED_ENTRY_CONTRADICTORY_ORDER')
    def test_lost_entry_response_recovers_fill_without_repost(self):
        ex=EntryDemo(post='lost_filled');v=self.run_case(ex)
        self.assertEqual(v['cycle_result'],'STANDARD_OVERLAP_PASS')
        self.assertEqual(v['entry_posts'],1);self.assertEqual(v['final_cleanup']['status'],'PASS')
    def test_durable_rejection_receipt_can_be_reconciled_read_only(self):
        with tempfile.TemporaryDirectory() as d:
            ex=EntryDemo();r=m.Run(d,'0123456789ab',ex);cid='DPSL-0123456789ab-entry'
            r.state.update(entry={'client_id':cid,'quantity':'0.001','status':'INTENT','post_http_status':400,'post_exchange_code':-2019},entry_posts=1,pending=[cid],unresolved=[cid]);r.save()
            persisted=json.loads((pathlib.Path(d)/'local_state.json').read_text())
            with tempfile.TemporaryDirectory() as second:
                restored=m.Run(second,'0123456789ab',ex);restored.state=persisted
                self.assertFalse(restored.resolve_entry());self.assertEqual(restored.state['pending'],[])
                self.assertEqual(self.writes(ex),[])
    def test_wrong_durable_intent_identity_blocks_before_query(self):
        with tempfile.TemporaryDirectory() as d:
            ex=EntryDemo();r=m.Run(d,'0123456789ab',ex)
            r.state['entry']={'client_id':'someone-else','quantity':'0.001','post_http_status':400,'post_exchange_code':-2019}
            with self.assertRaisesRegex(m.Blocked,'ENTRY_INTENT_IDENTITY'):r.resolve_entry()
            self.assertEqual(ex.calls,[])

if __name__=='__main__':unittest.main()
