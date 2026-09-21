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

    def test_cancel_failure_is_fail_closed_until_order_state_is_definitive(self):
        uncertain=AsyncMock(side_effect=[RuntimeError('timeout'),{'status':'NEW'}])
        with patch.object(bot,'binance_signed_request',uncertain):
            self.assertFalse(asyncio.run(bot._at_cancel_normal_order(None,'BTCUSDT','77')))
        closed=AsyncMock(side_effect=[RuntimeError('timeout'),{'status':'FILLED'}])
        with patch.object(bot,'binance_signed_request',closed):
            self.assertTrue(asyncio.run(bot._at_cancel_normal_order(None,'BTCUSDT','77')))

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


class TpLimitFallbackReconcileTests(unittest.TestCase):
    def setUp(self):
        from test_v5135 import DatabaseCase
        DatabaseCase.setUp(self)
        self.rows=DatabaseCase.rows.__get__(self, type(self))
        bot.autotrade_cfg.update(mode='LIVE',exit_profile='CURRENT_TP2')

    def test_triggered_partial_limit_cancels_then_closes_verified_remainder(self):
        tid=bot._at_insert_trade(
            9001,'XUSDT','LIVE',100,100,10,
            dict(stop=99,tp1=101,tp2=102,runner=105),{},position_side='BOTH',entry_order_id='entry')
        bot._at_update_trade(tid,tp2_algo_id='tp-algo',tp2_client_id='tp-client')
        trigger_ms=bot.now_ms()-3000
        snapshots=[
            ({'canTrade':True},{'balance':'1000'},[{'symbol':'XUSDT','positionSide':'BOTH','positionAmt':'10'}]),
            ({'canTrade':True},{'balance':'1000'},[{'symbol':'XUSDT','positionSide':'BOTH','positionAmt':'4'}]),
        ]
        async def signed(session,method,path,params=None,**kwargs):
            if method=='GET' and path=='/fapi/v1/order':
                return {'status':'PARTIALLY_FILLED','orderId':77}
            return {}
        with patch.object(bot,'AUTO_TRADE_TP_LIMIT_FALLBACK_SECONDS',2.0), \
             patch.object(bot,'BINANCE_API_KEY','test'), \
             patch.object(bot,'BINANCE_API_SECRET','test'), \
             patch.object(bot,'_at_account_snapshot',new=AsyncMock(side_effect=snapshots)), \
             patch.object(bot,'_at_algo_state',new=AsyncMock(return_value={'triggerTime':trigger_ms,'actualOrderId':'77'})), \
             patch.object(bot,'binance_signed_request',new=AsyncMock(side_effect=signed)), \
             patch.object(bot,'_at_cancel_normal_order',new=AsyncMock(return_value=True)) as cancel_normal, \
             patch.object(bot,'_at_emergency_close',new=AsyncMock(return_value={'status':'FILLED'})) as market_close, \
             patch.object(bot,'_at_window_net_pnl',new=AsyncMock(return_value=(12.0,0.5,101.9))), \
             patch.object(bot,'_at_cancel_trade_algos',new=AsyncMock()), \
             patch.object(bot,'telegram_send',new=AsyncMock()), \
             patch.object(bot.asyncio,'sleep',new=AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(bot.autotrade_reconcile_loop(None))
        cancel_normal.assert_awaited_once_with(None,'XUSDT','77')
        market_close.assert_awaited_once()
        self.assertAlmostEqual(4.0,float(market_close.await_args.args[2]))
        row=self.rows('SELECT status,close_reason,exit_price,net_pnl FROM autotrade_trades WHERE id=?',(tid,))[0]
        self.assertEqual('CLOSED',row['status'])
        self.assertEqual('TP2_LIMIT_FALLBACK',row['close_reason'])
        self.assertAlmostEqual(101.9,row['exit_price'])
        self.assertAlmostEqual(11.5,row['net_pnl'])


if __name__=='__main__':
    unittest.main()
