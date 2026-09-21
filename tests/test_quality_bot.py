import copy
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'binance_momentum_bot'))
os.environ['PYTHON_DOTENV_DISABLED']='1'
import bot

class QualityIntegrationTests(unittest.TestCase):
    def test_snapshot_does_not_mutate_production_state(self):
        now=bot.now_ms();st=bot.SymbolState(last_price=100,last_trade_event_ms=now-10,last_trade_receive_ms=now-5)
        st.trades.extend([bot.TradeSample(now-2000,99,100,True),bot.TradeSample(now-10,100,200,False)])
        before=copy.deepcopy(st)
        with patch.object(bot,'states',{'ABC':st}):
            data=bot.quality_features('ABC')
        self.assertEqual(before,st)
        self.assertGreaterEqual(data['feature_ready_ts_ms'],data['_sources']['chg30']['source_ms'])
        self.assertIsNone(data['dist_episode_peak_pct'])

    def test_shadow_failure_cannot_escape_signal_callback(self):
        class Broken:
            def arm(self,*args):raise RuntimeError('isolated test')
        with patch.object(bot,'quality_recorder',Broken()),patch.object(bot,'quality_features',return_value={}):
            bot.quality_arm('EARLY',1,'ABC',2,{'price':100})

    def test_no_recorder_is_noop_and_no_model_consumer(self):
        with patch.object(bot,'quality_recorder',None),patch.object(bot,'quality_features',side_effect=AssertionError):
            self.assertIsNone(bot.quality_arm('PREMIUM',1,'ABC',2,{'price':100}))

