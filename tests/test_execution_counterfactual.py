import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'research'))
from execution_counterfactual import replay

class ExecutionTests(unittest.TestCase):
    def run_case(self,quotes,**kwargs):
        return replay(quotes,decision_ms=1000,deadline_ms=10000,entry_low=99.9,entry_high=100.2,target=101,stop=99,**kwargs)
    def test_future_stale_and_predecision_quotes_cannot_fill(self):
        quotes=[dict(event_ms=999,received_ms=1001,bid=100,ask=100.1),
                dict(event_ms=9000,received_ms=1100,bid=100,ask=100.1)]
        self.assertIsNone(self.run_case(quotes)['fill_price'])
    def test_missing_path_or_funding_never_claims_net(self):
        q=[dict(event_ms=2000,received_ms=2001,bid=100,ask=100.1),dict(event_ms=3000,received_ms=3001,bid=101,ask=101.1)]
        self.assertIsNone(self.run_case(q)['net_ev_proxy'])
        self.assertIsNotNone(self.run_case(q,path_complete=True,funding_pct=0)['net_ev_proxy'])
    def test_widening_cannot_retroactively_capture_target(self):
        q=[dict(event_ms=2000,received_ms=2001,bid=101,ask=101.1)]
        self.assertEqual('TARGET_BEFORE_ENTRY',self.run_case(q,scenario='wider_band',widen_pct=2)['event'])
