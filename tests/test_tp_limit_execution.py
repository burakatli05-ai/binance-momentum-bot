import asyncio
import os
import sys
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch, Mock

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'binance_momentum_bot'))
os.environ['PYTHON_DOTENV_DISABLED']='1'
os.environ['AUTO_TRADE_LIVE_ALLOWED']='0'

import bot


class TpLimitExecutionTests(unittest.TestCase):
    def test_resting_limit_payload_is_reduce_only_in_one_way(self):
        request=AsyncMock(return_value={'orderId':123,'status':'NEW'})
        with patch.object(bot,'binance_signed_request',request):
            out=asyncio.run(bot._at_place_limit_exit(
                None,'COTIUSDT',107822,0.01877,'BOTH','tp-test'))
        self.assertEqual(123,out['orderId'])
        args=request.await_args.args
        self.assertEqual(('POST','/fapi/v1/order'),args[1:3])
        params=args[3]
        self.assertEqual('LIMIT',params['type'])
        self.assertEqual(0.01877,params['price'])
        self.assertEqual('GTC',params['timeInForce'])
        self.assertEqual(107822,params['quantity'])
        self.assertEqual('true',params['reduceOnly'])
        self.assertNotIn('triggerPrice',params)
        self.assertNotIn('closePosition',params)

    def test_resting_limit_payload_hedge_mode_has_position_side_not_reduce_only(self):
        request=AsyncMock(return_value={'orderId':123,'status':'NEW'})
        with patch.object(bot,'binance_signed_request',request):
            asyncio.run(bot._at_place_limit_exit(None,'BTCUSDT',1,101,'LONG','tp-test'))
        params=request.await_args.args[3]
        self.assertEqual('LONG',params['positionSide'])
        self.assertNotIn('reduceOnly',params)
        self.assertNotIn('triggerPrice',params)

    def test_resting_limit_timeout_queries_client_id_instead_of_blind_retry(self):
        request=AsyncMock(side_effect=RuntimeError('timeout'))
        query=AsyncMock(return_value={'orderId':77,'status':'NEW'})
        with patch.object(bot,'binance_signed_request',request), patch.object(bot,'_at_query_order_by_client',query):
            out=asyncio.run(bot._at_place_limit_exit(None,'BTCUSDT',1,101,'BOTH','unique'))
        self.assertEqual(77,out['orderId'])
        self.assertEqual(request.await_count,1)
        query.assert_awaited_once_with(None,'BTCUSDT','unique')

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

    def test_retrace_fallback_requires_real_target_touch(self):
        old_pct=bot.AUTO_TRADE_TP_RETRACE_FALLBACK_PCT
        old_min=bot.AUTO_TRADE_TP_RETRACE_MIN_SECONDS
        try:
            bot.AUTO_TRADE_TP_RETRACE_FALLBACK_PCT=0.15
            bot.AUTO_TRADE_TP_RETRACE_MIN_SECONDS=1.0
            tr={'tp2_price':102.0,'tp2_target_seen_ts_ms':0}
            self.assertFalse(bot._at_tp_retrace_fallback_due(tr,101.0,10000))
            tr['tp2_target_seen_ts_ms']=9000
            self.assertFalse(bot._at_tp_retrace_fallback_due(tr,101.90,10000))
            self.assertTrue(bot._at_tp_retrace_fallback_due(tr,101.80,10000))
        finally:
            bot.AUTO_TRADE_TP_RETRACE_FALLBACK_PCT=old_pct
            bot.AUTO_TRADE_TP_RETRACE_MIN_SECONDS=old_min

    def test_legacy_trigger_timer_retained_only_for_old_positions(self):
        old=bot.AUTO_TRADE_TP_LIMIT_FALLBACK_SECONDS
        try:
            bot.AUTO_TRADE_TP_LIMIT_FALLBACK_SECONDS=2.0
            self.assertFalse(bot._at_tp_limit_fallback_due({'triggerTime':9000},10000))
            self.assertTrue(bot._at_tp_limit_fallback_due({'triggerTime':8000},10000))
        finally:
            bot.AUTO_TRADE_TP_LIMIT_FALLBACK_SECONDS=old

    def test_live_tick_records_first_tp2_touch_once(self):
        tr={'id':1,'signal_id':99,'symbol':'XUSDT','mode':'LIVE','status':'OPEN',
            'exit_profile':'CURRENT_TP2','tp2_price':102.0,'tp2_order_id':'77',
            'tp2_target_seen_ts_ms':0}
        active={1:tr}
        by_symbol={'XUSDT':{1}}
        events=[]
        def update(tid,**fields):
            tr.update(fields)
        def log(event,**kwargs):
            events.append((event,kwargs))
        with patch.object(bot,'autotrade_active',active), \
             patch.object(bot,'autotrade_active_by_symbol',by_symbol), \
             patch.object(bot,'_at_update_trade',side_effect=update), \
             patch.object(bot,'_at_log_event',side_effect=log):
            bot.autotrade_on_tick('XUSDT',102.01,123.456)
            bot.autotrade_on_tick('XUSDT',102.02,123.999)
        self.assertEqual(123456,tr['tp2_target_seen_ts_ms'])
        self.assertEqual([x[0] for x in events],['TP2_TARGET_SEEN'])

    def test_cancel_trade_protection_includes_resting_tp(self):
        cancel_normal=AsyncMock(return_value=True)
        cancel_algo=AsyncMock()
        tr={'symbol':'BTCUSDT','tp2_order_id':'88','tp1_algo_id':'11','tp2_algo_id':'22','stop_algo_id':'33'}
        with patch.object(bot,'_at_cancel_normal_order',cancel_normal), patch.object(bot,'_at_cancel_algo',cancel_algo):
            asyncio.run(bot._at_cancel_trade_algos(None,tr))
        cancel_normal.assert_awaited_once_with(None,'BTCUSDT','88')
        self.assertEqual([x.args[1] for x in cancel_algo.await_args_list],['11','22','33'])


class TpLimitFallbackReconcileTests(unittest.TestCase):
    def setUp(self):
        from test_v5135 import DatabaseCase
        DatabaseCase.setUp(self)
        self.rows=DatabaseCase.rows.__get__(self,type(self))
        bot.autotrade_cfg.update(mode='LIVE',exit_profile='CURRENT_TP2')
        bot.states['XUSDT']=bot.SymbolState()

    def tearDown(self):
        bot.states.pop('XUSDT',None)

    def test_schema_has_resting_limit_and_target_seen_columns(self):
        with bot.db_connect() as conn:
            cols={r[1] for r in conn.execute('PRAGMA table_info(autotrade_trades)').fetchall()}
        self.assertTrue({'tp2_order_id','tp2_order_client_id','tp2_target_seen_ts_ms'} <= cols)

    def test_partial_resting_limit_retrace_closes_only_verified_remainder(self):
        tid=bot._at_insert_trade(
            9001,'XUSDT','LIVE',100,100,10,
            dict(stop=99,tp1=101,tp2=102,runner=105),{},position_side='BOTH',entry_order_id='entry')
        bot._at_update_trade(tid,tp2_order_id='77',tp2_order_client_id='tp-client',
                             tp2_target_seen_ts_ms=bot.now_ms()-5000)
        bot.states['XUSDT'].last_price=101.80
        snapshots=[
            ({'canTrade':True},{'balance':'1000'},[{'symbol':'XUSDT','positionSide':'BOTH','positionAmt':'4'}]),
            ({'canTrade':True},{'balance':'1000'},[{'symbol':'XUSDT','positionSide':'BOTH','positionAmt':'4'}]),
        ]
        order={'status':'PARTIALLY_FILLED','orderId':'77','origQty':'10','executedQty':'6'}
        with patch.object(bot,'AUTO_TRADE_TP_RETRACE_FALLBACK_PCT',0.15), \
             patch.object(bot,'AUTO_TRADE_TP_RETRACE_MIN_SECONDS',1.0), \
             patch.object(bot,'BINANCE_API_KEY','test'), \
             patch.object(bot,'BINANCE_API_SECRET','test'), \
             patch.object(bot,'_at_account_snapshot',new=AsyncMock(side_effect=snapshots)), \
             patch.object(bot,'_at_query_normal_order',new=AsyncMock(return_value=order)), \
             patch.object(bot,'_at_algo_state',new=AsyncMock(return_value=None)), \
             patch.object(bot,'_at_cancel_normal_order',new=AsyncMock(return_value=True)) as cancel_normal, \
             patch.object(bot,'_at_emergency_close',new=AsyncMock(return_value={'status':'FILLED','orderId':'fb'})) as market_close, \
             patch.object(bot,'_at_window_net_pnl',new=AsyncMock(return_value=(18.0,1.5,101.79))), \
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
        self.assertEqual('TP2_RETRACE_FALLBACK',row['close_reason'])
        self.assertAlmostEqual(101.79,row['exit_price'])
        self.assertAlmostEqual(16.5,row['net_pnl'])

    def test_no_retrace_fallback_before_target_seen_even_when_below_target(self):
        tid=bot._at_insert_trade(
            9002,'XUSDT','LIVE',100,100,10,
            dict(stop=99,tp1=101,tp2=102,runner=105),{},position_side='BOTH',entry_order_id='entry')
        bot._at_update_trade(tid,tp2_order_id='78',tp2_order_client_id='tp-client')
        bot.states['XUSDT'].last_price=100.50
        snapshot=({'canTrade':True},{'balance':'1000'},[{'symbol':'XUSDT','positionSide':'BOTH','positionAmt':'10'}])
        with patch.object(bot,'BINANCE_API_KEY','test'), patch.object(bot,'BINANCE_API_SECRET','test'), \
             patch.object(bot,'_at_account_snapshot',new=AsyncMock(return_value=snapshot)), \
             patch.object(bot,'_at_query_normal_order',new=AsyncMock(return_value={'status':'NEW','orderId':'78','origQty':'10','executedQty':'0'})), \
             patch.object(bot,'_at_algo_state',new=AsyncMock(return_value=None)), \
             patch.object(bot,'_at_cancel_normal_order',new=AsyncMock(return_value=True)) as cancel_normal, \
             patch.object(bot,'_at_emergency_close',new=AsyncMock()) as market_close, \
             patch.object(bot.asyncio,'sleep',new=AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(bot.autotrade_reconcile_loop(None))
        cancel_normal.assert_not_awaited()
        market_close.assert_not_awaited()
        row=self.rows('SELECT status FROM autotrade_trades WHERE id=?',(tid,))[0]
        self.assertEqual('OPEN',row['status'])


if __name__=='__main__':
    unittest.main()
