import asyncio
import os
import sys
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'binance_momentum_bot'))
os.environ['PYTHON_DOTENV_DISABLED']='1'
os.environ['AUTO_TRADE_LIVE_ALLOWED']='0'

import bot


class TpLimitExecutionTests(unittest.TestCase):
    def test_take_profit_limit_payload_is_reduce_only_in_one_way(self):
        request=AsyncMock(return_value={'algoId':123})
        with patch.object(bot,'binance_signed_request',request):
            asyncio.run(bot._at_place_algo(
                None,symbol='COTIUSDT',order_type='TAKE_PROFIT',trigger_price=0.01877,
                position_side='BOTH',client_id='tp-test',quantity=107822,
                limit_price=0.01877,time_in_force='GTC'))
        args=request.await_args.args
        self.assertEqual(('POST','/fapi/v1/algoOrder'),args[1:3])
        params=args[3]
        self.assertEqual('TAKE_PROFIT',params['type'])
        self.assertEqual(0.01877,params['triggerPrice'])
        self.assertEqual(0.01877,params['price'])
        self.assertEqual('GTC',params['timeInForce'])
        self.assertEqual(107822,params['quantity'])
        self.assertEqual('true',params['reduceOnly'])
        self.assertNotIn('closePosition',params)

    def test_take_profit_limit_payload_hedge_mode_has_position_side_not_reduce_only(self):
        request=AsyncMock(return_value={'algoId':123})
        with patch.object(bot,'binance_signed_request',request):
            asyncio.run(bot._at_place_algo(
                None,symbol='BTCUSDT',order_type='TAKE_PROFIT',trigger_price=101,
                position_side='LONG',client_id='tp-test',quantity=1,
                limit_price=101,time_in_force='GTC'))
        params=request.await_args.args[3]
        self.assertEqual('LONG',params['positionSide'])
        self.assertNotIn('reduceOnly',params)

    def test_stop_market_semantics_unchanged(self):
        request=AsyncMock(return_value={'algoId':456})
        with patch.object(bot,'binance_signed_request',request):
            asyncio.run(bot._at_place_algo(
                None,symbol='BTCUSDT',order_type='STOP_MARKET',trigger_price=99,
                position_side='BOTH',client_id='stop-test',close_position=True))
        params=request.await_args.args[3]
        self.assertEqual('STOP_MARKET',params['type'])
        self.assertEqual('true',params['closePosition'])
        self.assertNotIn('price',params)
        self.assertNotIn('timeInForce',params)

    def test_fallback_only_after_trigger_and_grace(self):
        old=bot.AUTO_TRADE_TP_LIMIT_FALLBACK_SECONDS
        try:
            bot.AUTO_TRADE_TP_LIMIT_FALLBACK_SECONDS=2.0
            self.assertFalse(bot._at_tp_limit_fallback_due(None,10000))
            self.assertFalse(bot._at_tp_limit_fallback_due({'triggerTime':0},10000))
            self.assertFalse(bot._at_tp_limit_fallback_due({'triggerTime':9000},10000))
            self.assertTrue(bot._at_tp_limit_fallback_due({'triggerTime':8000},10000))
        finally:
            bot.AUTO_TRADE_TP_LIMIT_FALLBACK_SECONDS=old


if __name__=='__main__':
    unittest.main()
