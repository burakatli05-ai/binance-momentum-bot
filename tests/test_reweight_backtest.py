import sys
from pathlib import Path
import unittest
try:
    import numpy as np
    import pandas as pd
    import sklearn
except ImportError:
    raise unittest.SkipTest('Install research/requirements.txt; dedicated research CI requires these tests')
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'research'))
import reweight_backtest as r

class ResearchTests(unittest.TestCase):
    def test_imputation_is_fit_only_on_train(self):
        train=pd.DataFrame({'x':[1.,2.,3.,np.nan],'z':[0.,1.,0.,1.]})
        p=r.pipeline().fit(train,[0,0,1,1]);before=p['imputer'].statistics_.copy()
        p.predict_proba(pd.DataFrame({'x':[1e9,np.nan],'z':[0,1]}))
        np.testing.assert_array_equal(before,p['imputer'].statistics_)
        self.assertEqual(2,before[0])

    def test_source_time_and_availability_reject_future_and_unknown(self):
        d=pd.DataFrame([dict(kind='premium',id=1,decision_ts=100)])
        records=[dict(kind='premium',id=1,feature=f,value=1,source_ms=s,available_ms=a,max_age_ms=1000,trusted=True)
                 for f,s,a in [('chg30',99000,100000),('flow30',100001,100001),('book_imbalance',98000,99000)]]
        x,audit=r.strict_features(d,records)
        self.assertEqual(1,x.loc[0,'chg30']);self.assertTrue(np.isnan(x.loc[0,'flow30']))
        self.assertTrue(np.isnan(x.loc[0,'book_imbalance']));self.assertTrue(np.isnan(x.loc[0,'oi_accel5']))

    def test_purge_maturity_and_symbol_out(self):
        rows=[]
        for i in range(6):
            t=pd.Timestamp('2026-09-10',tz='Europe/Istanbul').timestamp()+i*86400
            rows.append(dict(day=f'2026-09-{10+i}',decision_ts=t+100,label_ready=t+200,
                             episode_key=str(i),wave43200='same' if i in (1,3) else str(i),symbol='A' if i%2 else 'B'))
        d=pd.DataFrame(rows)
        for _,tr,te in r.forward_splits(d,True):
            self.assertFalse(set(d.loc[tr,'symbol'])&set(d.loc[te,'symbol']))
            self.assertFalse(set(d.loc[tr,'wave43200'])&set(d.loc[te,'wave43200']))
            self.assertTrue((d.loc[tr,'label_ready']<d.loc[te,'decision_ts'].min()).all())

    def test_tied_score_has_exact_coverage(self):
        self.assertAlmostEqual(2.5,r.exact_weights([80]*10).sum())

if __name__=='__main__':unittest.main()
