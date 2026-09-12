import asyncio
import ast
import json
import unittest
from unittest.mock import AsyncMock, patch, MagicMock
import test_v5135 as fixtures
from test_v5135 import bot, xmod, ROOT
from position_observer import PositionObserver


class InformationTests(unittest.TestCase):
    setUp = fixtures.DatabaseCase.setUp
    rows = fixtures.DatabaseCase.rows
    sql = fixtures.DatabaseCase.sql

    def watcher(self, text='$BTC long setup'):
        watcher = xmod.XWatcher(bot.db_connect, lambda s: {'price':999,'chg5':2,'chg24':3} if s else {'BTCUSDT'}, AsyncMock(return_value=True), AsyncMock(return_value=True))
        watcher.request = AsyncMock(side_effect=[{'data':{'id':'42'}}, {'data':[{'id':'1','text':text,'created_at':'2026-01-01T00:00:30Z'}]}])
        asyncio.run(watcher.poll_account(None,'chartexpt'))
        return watcher

    def test_all_horizons_use_tweet_time_and_survive_restart(self):
        watcher = self.watcher()
        row = self.rows('SELECT * FROM x_watcher_tweets')[0]
        start = row['tweet_time_ms']
        async def price(session, symbol, target):
            return (100 if target == start else 110, target//60000*60000)
        watcher.historical_price = AsyncMock(side_effect=price)
        with patch.object(xmod.time,'time',return_value=(start+86400000)/1000):
            asyncio.run(watcher.process(None))
        outcomes = self.rows('SELECT * FROM x_forward_outcomes ORDER BY horizon_s')
        self.assertEqual(list(xmod.HORIZONS),[r['horizon_s'] for r in outcomes])
        for r in outcomes:
            self.assertEqual(start+r['horizon_s']*1000,r['target_time_ms'])
            self.assertAlmostEqual(10,r['return_pct'])
            self.assertAlmostEqual(10,r['directional_return_pct'])
            self.assertEqual(30000,r['target_time_ms']-r['price_time_ms'])
        row = self.rows('SELECT * FROM x_watcher_tweets')[0]
        self.assertEqual(100,row['tweet_price'])
        self.assertEqual(999,json.loads(row['market_context_json'])['market']['price'])
        restored = xmod.XWatcher(bot.db_connect, lambda _: {}, AsyncMock(), AsyncMock())
        restored.historical_price = AsyncMock()
        asyncio.run(restored.process(None))
        restored.historical_price.assert_not_awaited()
        self.assertEqual([],self.rows('SELECT * FROM autotrade_trades'))
        self.assertEqual([],self.rows('SELECT * FROM causal_cohorts'))

    def test_no_early_or_fabricated_outcomes_and_missing_data_recovery(self):
        watcher = self.watcher()
        start = self.rows('SELECT tweet_time_ms FROM x_watcher_tweets')[0]['tweet_time_ms']
        watcher.historical_price = AsyncMock(return_value=None)
        with patch.object(xmod.time,'time',return_value=(start+300000)/1000):
            asyncio.run(watcher.process(None))
        self.assertEqual([],self.rows('SELECT * FROM x_forward_outcomes'))
        self.assertIsNone(self.rows('SELECT tweet_price FROM x_watcher_tweets')[0]['tweet_price'])
        watcher.historical_price = AsyncMock(side_effect=lambda session,symbol,target: (100,target//60000*60000))
        with patch.object(xmod.time,'time',return_value=(start+360000)/1000):
            asyncio.run(watcher.process(None))
        self.assertEqual([300],[r['horizon_s'] for r in self.rows('SELECT * FROM x_forward_outcomes')])

    def test_conditional_post_is_not_scored_as_triggered_prediction(self):
        watcher = self.watcher('$BTC long if above 100')
        start = self.rows('SELECT tweet_time_ms FROM x_watcher_tweets')[0]['tweet_time_ms']
        watcher.historical_price = AsyncMock(side_effect=lambda session,symbol,target: (110,target//60000*60000))
        with patch.object(xmod.time,'time',return_value=(start+86400000)/1000):
            asyncio.run(watcher.process(None))
        self.assertTrue(all(r['directional_return_pct'] is None for r in self.rows('SELECT * FROM x_forward_outcomes')))
        watcher.send.assert_not_awaited()

    def test_legacy_trigger_is_cancelled_and_migration_idempotent(self):
        self.watcher()
        self.sql("UPDATE x_watcher_tweets SET watch_status='WATCH',trigger_delivery='PENDING',measurement_status=NULL")
        bot.init_db();bot.init_db()
        row = self.rows('SELECT * FROM x_watcher_tweets')[0]
        self.assertEqual('RETIRED_INFORMATION_ONLY',row['watch_status'])
        self.assertEqual('CANCELLED_INFORMATION_ONLY',row['trigger_delivery'])
        self.assertEqual('PENDING',row['measurement_status'])
        self.assertEqual('BTCUSDT',row['symbol'])

    def test_historical_endpoint_checks_timestamp_and_price(self):
        watcher = self.watcher()
        response = AsyncMock()
        response.status = 200
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=response)
        context.__aexit__ = AsyncMock(return_value=False)
        session = MagicMock()
        session.get.return_value = context
        for data,expected in (([[60000,'100']],(100,60000)), ([[0,'100']],None), ([[60000,'nan']],None), ([],None)):
            response.json.return_value = data
            self.assertEqual(expected,asyncio.run(watcher.historical_price(session,'BTCUSDT',90000)))
        self.assertEqual(60000,session.get.call_args.kwargs['params']['startTime'])

    def test_market_snapshot_does_not_call_mutating_metrics(self):
        st = bot.states['BTCUSDT']
        st.last_price = 100
        st.last_trade_receive_ms = bot.now_ms()
        with patch.object(bot,'compute_metrics',side_effect=AssertionError('must not call')):
            self.assertEqual(100,bot._x_market('BTCUSDT')['price'])
        self.assertEqual({},bot._x_market('UNKNOWN'))

    def test_x_cannot_feed_production_decisions(self):
        source = (ROOT/'binance_momentum_bot/bot.py').read_text(encoding='utf-8')
        tree = ast.parse(source)
        consumers = {node.name for node in tree.body if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)) and any(isinstance(n,ast.Name) and n.id=='x_watcher' for n in ast.walk(node))}
        self.assertEqual({'main','telegram_command_loop'},consumers)
        xsource = (ROOT/'binance_momentum_bot/x_watcher.py').read_text(encoding='utf-8')
        self.assertNotIn('X SHADOW TETİK',xsource)
        self.assertNotIn('CROSS_ABOVE',xsource)
        self.assertNotIn('CROSS_BELOW',xsource)


class RetryTests(unittest.TestCase):
    setUp = fixtures.DatabaseCase.setUp
    rows = fixtures.DatabaseCase.rows
    sql = fixtures.DatabaseCase.sql

    def observer(self, send):
        return PositionObserver(bot.db_connect,AsyncMock(return_value=[]),send,lambda *_:'MANUAL',bot._po_zone,[5],1)

    def seed(self):
        self.sql("""INSERT INTO position_observer_events(position_instance_id,symbol,position_side,event,event_time_ms,entry_price,current_price,detail_json,notification_delivery)
            VALUES ('instance','BTCUSDT','BOTH','ROE_PROFIT',1,100,101,'{}','PENDING')""")

    def test_three_retries_backoff_restart_and_no_reset_by_migration(self):
        self.seed()
        send = AsyncMock(return_value=False)
        for sec,count in ((1000,1),(1001,1),(1029,1),(1030,2),(1089,2),(1090,3),(1209,3),(1210,4),(9999,4)):
            with patch('position_observer.time.time',return_value=sec):
                asyncio.run(self.observer(send).deliver_pending(None))
            bot.init_db()
            self.assertEqual(count,send.await_count)
            self.assertEqual(count,self.rows('SELECT attempt_count FROM position_observer_events')[0]['attempt_count'])
        self.assertEqual(1210000,self.rows('SELECT last_attempt_time FROM position_observer_events')[0]['last_attempt_time'])

    def test_retry_success_stops_and_exception_counts(self):
        self.seed()
        send = AsyncMock(side_effect=[RuntimeError('offline'),True])
        for sec in (1000,1030,9999):
            with patch('position_observer.time.time',return_value=sec):
                asyncio.run(self.observer(send).deliver_pending(None))
        self.assertEqual(2,send.await_count)
        self.assertEqual('DELIVERED',self.rows('SELECT notification_delivery FROM position_observer_events')[0]['notification_delivery'])

    def test_attempt_reserved_before_send_and_cancelled_send_is_bounded(self):
        self.seed()
        async def cancel(*_):
            self.assertEqual(1,self.rows('SELECT attempt_count FROM position_observer_events')[0]['attempt_count'])
            raise asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(self.observer(cancel).deliver_pending(None))
        send = AsyncMock(return_value=True)
        asyncio.run(self.observer(send).deliver_pending(None))
        send.assert_not_awaited()

    def test_legacy_failed_delivery_migration_preserves_initial_attempt(self):
        self.seed()
        self.sql("UPDATE position_observer_events SET notification_delivery='FAILED',delivery_time_ms=1234")
        self.sql('ALTER TABLE position_observer_events DROP COLUMN attempt_count')
        self.sql('ALTER TABLE position_observer_events DROP COLUMN last_attempt_time')
        bot.init_db();bot.init_db()
        row = self.rows('SELECT * FROM position_observer_events')[0]
        self.assertEqual(1,row['attempt_count'])
        self.assertEqual(1234,row['last_attempt_time'])
