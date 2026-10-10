import asyncio
import time
from unittest.mock import patch, AsyncMock
import unittest
import test_v5135 as legacy
import bot
import premium_step_lock as sl


class PremiumIntegrationTests(unittest.TestCase):
    def setUp(self):
        legacy.DatabaseCase.setUp(self)
        self.rows=legacy.DatabaseCase.rows.__get__(self)
        self.sql=legacy.DatabaseCase.sql.__get__(self)
        bot.autotrade_cfg.update(exit_profile=sl.PROFILE)
        self.addCleanup(bot._premium_step_tick_watermarks.clear)
        self.old_filter=bot.exchange_filters.get('BTCUSDT')
        bot.exchange_filters['BTCUSDT']={'tick_size':0.01,'step_size':0.001}
        self.addCleanup(self.restore_filter)
    def restore_filter(self):
        if self.old_filter is None: bot.exchange_filters.pop('BTCUSDT',None)
        else: bot.exchange_filters['BTCUSDT']=self.old_filter
    def trade(self,signal=1):
        return bot._at_insert_trade(signal,'BTCUSDT','DRY',99.1,100,2,
                dict(stop=98.7,tp1=101,tp2=102,runner=105),{})
    def row(self,tid): return self.rows('SELECT * FROM autotrade_trades WHERE id=?',(tid,))[0]
    def stamp(self,tid,offset=1000): return (self.row(tid)['sl_fill_ts_ms']+offset)/1000
    def test_16_no_tp_runner_or_close_at_cap(self):
        tid=self.trade()
        bot.autotrade_on_tick('BTCUSDT',110,self.stamp(tid))
        row=self.row(tid)
        self.assertEqual(row['status'],'OPEN'); self.assertEqual(row['stop_price'],109.5)
        self.assertEqual(row['expected_qty'],2)
        self.assertIsNone(row['tp1_algo_id']); self.assertIsNone(row['tp2_order_id']); self.assertIsNone(row['tp2_algo_id'])
        bot.autotrade_on_tick('BTCUSDT',115,self.stamp(tid,2000))
        self.assertEqual(self.row(tid)['stop_revision'],1)
        bot.autotrade_on_tick('BTCUSDT',109.4,self.stamp(tid,3000))
        self.assertEqual(self.row(tid)['close_reason'],'STOP')
    def test_19_existing_rows_not_converted(self):
        bot.autotrade_cfg['exit_profile']='PARTIAL_RUNNER'
        tid=self.trade(); before=self.row(tid)
        bot.init_db(); bot.init_db()
        self.assertEqual(self.row(tid),before)
        self.assertIsNone(self.row(tid)['sl_entry_vwap'])
    def test_20_frozen_profile_and_fill_reference(self):
        tid=self.trade()
        self.assertEqual(self.row(tid)['sl_entry_vwap'],'100')
        self.assertEqual(self.row(tid)['entry_signal_price'],99.1)
        with self.assertRaisesRegex(ValueError,'IMMUTABLE'): bot._at_update_trade(tid,exit_profile='CURRENT_TP2')
        bot.autotrade_on_tick('BTCUSDT',100.5,self.stamp(tid))
        self.assertEqual(self.row(tid)['stop_price'],100.25)
    def test_21_soft_rollback_new_trades_only(self):
        tid=self.trade(); bot.autotrade_cfg['exit_profile']='CURRENT_TP2'
        second=self.trade(2)
        bot.autotrade_on_tick('BTCUSDT',102.6,self.stamp(tid))
        self.assertEqual(self.row(tid)['exit_profile'],sl.PROFILE)
        self.assertEqual(self.row(tid)['status'],'OPEN')
        self.assertEqual(self.row(second)['exit_profile'],'CURRENT_TP2')
        self.assertEqual(self.row(second)['close_reason'],'TP2')
    def test_restart_restores_steps_and_mode_off(self):
        tid=self.trade(); bot.autotrade_on_tick('BTCUSDT',102.6,self.stamp(tid))
        bot._at_save_setting('exit_profile','CURRENT_TP2'); bot._at_save_setting('mode','LIVE')
        bot.autotrade_active.clear(); bot.load_autotrade_settings()
        self.assertEqual(bot.autotrade_cfg['mode'],'OFF')
        self.assertEqual(bot.autotrade_active[tid]['highest_trigger_bp'],250)
        bot.autotrade_on_tick('BTCUSDT',103,self.stamp(tid,2000))
        self.assertEqual(self.row(tid)['stop_price'],102.5)
    def test_duplicate_same_step_and_out_of_order_no_writes(self):
        tid=self.trade(); stamp=self.stamp(tid)
        bot.autotrade_on_tick('BTCUSDT',101,stamp)
        with patch.object(bot,'_at_update_trade') as update:
            bot.autotrade_on_tick('BTCUSDT',101,stamp)
            bot.autotrade_on_tick('BTCUSDT',101.4,stamp+2)
            bot.autotrade_on_tick('BTCUSDT',105,stamp+1)
            bot.autotrade_on_tick('BTCUSDT',float('nan'),stamp+3)
            bot.autotrade_on_tick('BTCUSDT',-1,stamp+3)
            update.assert_not_called()
    def test_initial_stop_unchanged_before_trigger(self):
        tid=self.trade(); bot.autotrade_on_tick('BTCUSDT',100.499999,self.stamp(tid))
        self.assertEqual(self.row(tid)['stop_price'],98.7)
    def test_live_step_entry_blocked_before_network(self):
        bot.autotrade_cfg['mode']='LIVE'
        with patch.object(bot,'binance_signed_request',new=AsyncMock()) as request:
            asyncio.run(bot.autotrade_handle_premium(None,10,'BTCUSDT',{'price':100},{}))
        request.assert_not_awaited()
        self.assertEqual(self.rows('SELECT * FROM autotrade_trades'),[])
        self.assertIn(sl.LIVE_BLOCK_REASON,self.rows('SELECT * FROM autotrade_events')[-1]['detail'])
    def test_direct_live_insert_cannot_bypass_block(self):
        with self.assertRaisesRegex(ValueError,'LIVE_BLOCKED'):
            bot._at_insert_trade(10,'BTCUSDT','LIVE',100,100,2,dict(stop=99),{})
    def test_unknown_setting_preserved_and_entry_blocked(self):
        bot._at_save_setting('exit_profile','INVALID'); bot.load_autotrade_settings()
        self.assertEqual(bot.autotrade_cfg['exit_profile'],'INVALID')
        bot.autotrade_cfg['mode']='DRY'
        asyncio.run(bot.autotrade_handle_premium(None,10,'BTCUSDT',{'price':100},{}))
        self.assertEqual(self.rows('SELECT * FROM autotrade_trades'),[])
        self.assertEqual(self.rows('SELECT * FROM autotrade_events')[-1]['event'],sl.UNKNOWN)
    def test_unknown_open_trade_never_tp_fallback(self):
        bot.autotrade_active[7]=dict(id=7,symbol='BTCUSDT',status='OPEN',mode='DRY',exit_profile='INVALID',tp2_price=102)
        bot.autotrade_active_by_symbol['BTCUSDT'].add(7)
        with patch.object(bot,'_at_profile_block') as block,patch.object(bot,'_at_close_trade') as close:
            bot.autotrade_on_tick('BTCUSDT',110,time.time())
        block.assert_called_once(); close.assert_not_called()
    def test_admin_profile_config_auth_audit_frozen(self):
        tid=self.trade()
        with patch.object(bot,'TELEGRAM_ADMIN_CHAT_ID','admin'),patch.object(bot,'TELEGRAM_ADMIN_USER_ID','user'),patch.object(bot,'telegram_send',new=AsyncMock()):
            asyncio.run(bot._at_command(None,'/exitprofile CURRENT_TP2','other','user'))
            self.assertEqual(bot.autotrade_cfg['exit_profile'],sl.PROFILE)
            asyncio.run(bot._at_command(None,'/exitprofile CURRENT_TP2','admin','user'))
        self.assertEqual(bot.autotrade_cfg['exit_profile'],'CURRENT_TP2')
        self.assertEqual(self.row(tid)['exit_profile'],sl.PROFILE)
        self.assertEqual(self.rows('SELECT * FROM autotrade_events')[-1]['event'],'EXIT_PROFILE_CHANGED')
    def test_recovery_pending_identity_read_only_and_alert(self):
        from test_premium_step_lock import trade,order
        tr=trade(); tr.update(pending_stop_client_id=sl.client_id(7,1),pending_stop_price='102',
            desired_stop_price='102',pending_stop_algo_id='new',stop_revision=1,highest_trigger_bp=250,current_lock_bp=200)
        active=order(tr,tr['stop_client_id'],'98.7')
        pending=order(tr,tr['pending_stop_client_id'],'102','new')
        positions=[dict(symbol='BTCUSDT',positionSide='BOTH',positionAmt='2')]
        with patch.object(bot,'binance_signed_request',new=AsyncMock(side_effect=[active,pending,[active,pending]])) as request,patch.object(bot,'_at_profile_block') as block,patch.object(bot,'telegram_send',new=AsyncMock()) as alert:
            asyncio.run(bot._at_step_recovery_check(None,tr,positions))
        self.assertEqual([c.args[1] for c in request.await_args_list],['GET']*3)
        self.assertEqual(request.await_args_list[1].args[3],{'clientAlgoId':sl.client_id(7,1)})
        block.assert_called_once_with(tr,sl.LIVE_BLOCK_REASON); alert.assert_awaited_once()
    def test_recovery_manual_mismatch_never_changes_exchange(self):
        from test_premium_step_lock import trade
        tr=trade()
        with patch.object(bot,'binance_signed_request',new=AsyncMock()) as request,patch.object(bot,'_at_profile_block') as block,patch.object(bot,'telegram_send',new=AsyncMock()):
            asyncio.run(bot._at_step_recovery_check(None,tr,[dict(symbol='BTCUSDT',positionSide='BOTH',positionAmt='3')]))
        request.assert_not_awaited(); self.assertIn('MANUAL_INTERVENTION',block.call_args.args[1])

    def live_entry(self, profile, *, stop_fails=False):
        bot.autotrade_cfg.update(mode='LIVE',exit_profile=profile)
        bot.states['BTCUSDT']=bot.SymbolState(); bot.states['BTCUSDT'].ask_price=100
        snapshot=({'canTrade':True,'dualSidePosition':False},{'balance':'100000','availableBalance':'100000'},[])
        algo=AsyncMock(side_effect=RuntimeError('stop reject')) if stop_fails else AsyncMock(return_value={'algoId':'stop'})
        with patch.object(bot,'AUTO_TRADE_LIVE_ALLOWED',True),patch.object(bot,'BINANCE_API_KEY','fake'),patch.object(bot,'BINANCE_API_SECRET','fake'),patch.object(bot,'_at_account_snapshot',new=AsyncMock(return_value=snapshot)),patch.object(bot,'binance_signed_request',new=AsyncMock(return_value={})),patch.object(bot,'_at_place_market_entry',new=AsyncMock(return_value={'orderId':'entry','executedQty':'20','avgPrice':'100.5'})),patch.object(bot,'_at_place_algo',new=algo),patch.object(bot,'_at_place_limit_exit',new=AsyncMock(return_value={'orderId':'tp2'})) as limit,patch.object(bot,'_at_emergency_close',new=AsyncMock()) as emergency,patch.object(bot,'telegram_send',new=AsyncMock()):
            asyncio.run(bot.autotrade_handle_premium(None,101,'BTCUSDT',{'price':100},dict(entry_mid=100,invalidation=98.7,target1=101,target2=102)))
        return self.rows('SELECT * FROM autotrade_trades')[0],algo,limit,emergency
    def test_current_tp2_live_entry_and_initial_stop_regression(self):
        row,algo,limit,emergency=self.live_entry('CURRENT_TP2')
        self.assertEqual(row['entry_price'],100.5)
        self.assertEqual(row['stop_price'],99.19)
        self.assertEqual(row['status'],'OPEN')
        self.assertEqual(algo.await_count,1)
        self.assertEqual(algo.await_args.kwargs['order_type'],'STOP_MARKET')
        self.assertTrue(algo.await_args.kwargs['close_position'])
        self.assertEqual(limit.await_args.args[3],102.51)
        emergency.assert_not_awaited()
    def test_partial_runner_live_entry_regression(self):
        row,algo,limit,emergency=self.live_entry('PARTIAL_RUNNER')
        self.assertEqual([c.kwargs['order_type'] for c in algo.await_args_list],['STOP_MARKET','TAKE_PROFIT_MARKET','TAKE_PROFIT_MARKET'])
        self.assertEqual(sum(c.kwargs.get('quantity',0) for c in algo.await_args_list),20)
        self.assertEqual(row['status'],'OPEN'); limit.assert_not_awaited(); emergency.assert_not_awaited()
    def test_emergency_close_existing_profiles_unchanged(self):
        row,algo,limit,emergency=self.live_entry('CURRENT_TP2',stop_fails=True)
        emergency.assert_awaited_once(); limit.assert_not_awaited()
        self.assertEqual(row['close_reason'],'EMERGENCY_CLOSE')
    def test_partial_runner_tp1_and_runner_dry_regression(self):
        bot.autotrade_cfg['exit_profile']='PARTIAL_RUNNER'; tid=self.trade()
        bot.autotrade_on_tick('BTCUSDT',101,time.time())
        row=self.row(tid); self.assertEqual(row['status'],'PARTIAL'); self.assertEqual(row['tp1_hit'],1)
        bot.autotrade_on_tick('BTCUSDT',105,time.time())
        self.assertEqual(self.row(tid)['close_reason'],'RUNNER')
    def test_duplicate_symbol_and_risk_limits_regression(self):
        self.trade(); bot.states['BTCUSDT']=bot.SymbolState()
        asyncio.run(bot.autotrade_handle_premium(None,11,'BTCUSDT',{'price':100},{}))
        self.assertIn('BOT_POSITION_ALREADY_ACTIVE',self.rows('SELECT * FROM autotrade_events')[-1]['detail'])
        bot.autotrade_cfg['max_open_positions']=1
        self.assertFalse(bot._at_risk_allowed('DRY')[0])
    def test_legacy_null_profile_reads_as_current(self):
        tid=self.trade()
        # Simulated old-schema row, bypassing the application update interface.
        self.sql('UPDATE autotrade_trades SET exit_profile=NULL WHERE id=?',(tid,))
        bot.recover_autotrade_active()
        bot.autotrade_on_tick('BTCUSDT',102,time.time())
        self.assertEqual(self.row(tid)['close_reason'],'TP2')

    def test_offline_readiness_requires_fresh_complete_snapshot(self):
        import premium_step_lock_readiness as ready
        bot._at_save_setting('mode','OFF'); bot._at_save_setting('exit_profile','CURRENT_TP2')
        self.assertFalse(ready.check(bot.DB_PATH,now_ms=100000)['rollback_safe'])
        snapshot={'complete':True,'scope':'ALL_BOT_STEP_LOCK_ORDERS','observed_ts_ms':99000,'orders':[]}
        self.assertTrue(ready.check(bot.DB_PATH,snapshot,now_ms=100000)['rollback_safe'])
        snapshot['observed_ts_ms']=1
        self.assertFalse(ready.check(bot.DB_PATH,snapshot,now_ms=100000)['rollback_safe'])


if __name__=='__main__': unittest.main()
