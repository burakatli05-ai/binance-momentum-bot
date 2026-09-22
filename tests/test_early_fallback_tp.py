"""Network-free end-to-end Early pilot + Binance adapter safety regressions."""
import copy
from dataclasses import asdict
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
import test_early_autotrader_v2 as fixtures
from early_autotrader_v2 import Pilot, Config
from early_v2_adapter import Binance, Integration
from execution_v2 import Blocked

class FallbackTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'pilot.db'
        self.now = 1000000
        self.connect = lambda: sqlite3.connect(self.path)
        self.p = Pilot(self.connect, live_allowed=True, score_validated=True,
                       profit_live_allowed=True, clock=lambda: self.now)
        self.p.configure('margin', 10.)
        self.orders = {}; self.calls = []; self.qty = .05; self.vwap = 99.99
        self.fail_tp = False; self.timeout_ack = False; self.cancel_ignored = False
        self.cancel_hook = None; self.tp_override = {}; self.exit_order = None
        self.ex = Binance({'_at_account_snapshot': self.snapshot, 'binance_signed_request': self.request}, None, self.p)
        self.ex.preflight = AsyncMock()
        self.ex.filters = AsyncMock(return_value=fixtures.FILTERS)
        self.ex.ask = AsyncMock(return_value=100.)

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def snapshot(self, session):
        return {'canTrade': True, 'dualSidePosition': False}, {'availableBalance': 100}, [dict(symbol='TESTUSDT', positionAmt=self.qty)]

    async def request(self, session, method, path, params):
        self.calls.append((method, path, dict(params)))
        if path == '/fapi/v1/order':
            if params.get('side') == 'SELL':
                self.exit_order = 90
                return dict(orderId=90, status='FILLED', side='SELL')
            if params.get('origClientOrderId', '').startswith('ev2x-'):
                return dict(orderId=90, status='FILLED', side='SELL')
            return dict(orderId=13, symbol='TESTUSDT', clientOrderId=params.get('newClientOrderId'),
                        side='BUY', type='LIMIT', timeInForce='IOC', status='EXPIRED', executedQty=.05)
        if path == '/fapi/v1/userTrades':
            sell = params['orderId'] != 13
            return [dict(id=20 if sell else 2, orderId=params['orderId'], symbol='TESTUSDT',
                         side='SELL' if sell else 'BUY', price='101' if sell else str(self.vwap), qty='.05',
                         time=self.now, realizedPnl='.05' if sell else '0', commission='.001', commissionAsset='USDT')]
        if path == '/fapi/v1/income':
            return []
        if path != '/fapi/v1/algoOrder':
            raise AssertionError(path)
        if method == 'POST':
            if params['type'] == 'TAKE_PROFIT_MARKET' and self.fail_tp:
                raise TimeoutError('tp unavailable')
            oid = len(self.orders) + 1
            response = dict(algoId=oid, clientAlgoId=params['clientAlgoId'], symbol=params['symbol'],
                            side=params['side'], orderType=params['type'], quantity=params['quantity'],
                            triggerPrice=params['triggerPrice'], reduceOnly=True, positionSide='BOTH',
                            workingType='CONTRACT_PRICE', algoStatus='NEW', actualOrderId='0')
            self.orders[oid] = response
            if params['type'] == 'TAKE_PROFIT_MARKET':
                response.update(self.tp_override)
                if self.timeout_ack:
                    raise TimeoutError('ack lost')
            return copy.deepcopy(response)
        state = (self.orders.get(params['algoId']) if 'algoId' in params else
                 next((o for o in self.orders.values() if o['clientAlgoId'] == params['clientAlgoId']), None))
        if state is None:
            raise RuntimeError('-2013 unknown order')
        if method == 'DELETE':
            if state['orderType'] == 'TAKE_PROFIT_MARKET' and self.cancel_hook:
                self.cancel_hook()
            if not self.cancel_ignored or state['orderType'] != 'TAKE_PROFIT_MARKET':
                if state['algoStatus'] != 'FINISHED': state['algoStatus'] = 'CANCELED'
            return {'code': '200'}
        return copy.deepcopy(state)

    def live(self):
        self.p.set_mode('DRY')
        self.p.confirm('u', self.p.challenge('u'), ready=True)

    async def enter(self):
        self.live()
        await self.p.enter(dict(fixtures.SIGNAL, stop=99.5), self.ex)
        return next(iter(self.p.active.values()))

    def profit_confirm(self):
        self.p.confirm('u', self.p.challenge('u', 'PROFIT_LIVE'), ready=True, kind='PROFIT_LIVE')

    def tp_posts(self):
        return [c for c in self.calls if c[0] == 'POST' and c[2].get('type') == 'TAKE_PROFIT_MARKET']

    async def test_partial_fill_tp_uses_real_vwap_and_reduce_only_quantity(self):
        tr = await self.enter()
        params = self.tp_posts()[0][2]
        self.assertEqual((params['side'], params['quantity'], params['reduceOnly']), ('SELL','0.05','true'))
        self.assertEqual(float(params['triggerPrice']), 100.99)
        self.assertTrue(tr['partial']); self.assertFalse(self.p.halted)
        self.assertEqual(tr['profit_policy'], 'FALLBACK_TP')

    async def test_profit_off_also_has_tp(self):
        self.p.profit_mode = 'OFF'
        await self.enter()
        self.assertEqual(len(self.tp_posts()), 1)

    async def test_tp_setting_is_validated_and_persisted(self):
        for value in (0, -.1, .29, 3.01, True, float('nan'), '1'):
            with self.subTest(value=value), self.assertRaises(Blocked): self.p.configure('fallback_tp_pct', value)
        self.p.configure('fallback_tp_pct', .8)
        tr = await self.enter()
        self.assertEqual(float(self.tp_posts()[0][2]['triggerPrice']), 100.79)
        self.assertEqual(Pilot(self.connect).cfg.fallback_tp_pct, .8)
        self.assertEqual(tr['config']['fallback_tp_pct'], .8)

    async def test_live_transition_waits_for_tp_cancel_and_ratchets_first(self):
        tr = await self.enter(); tr['shadow_stop'] = 100.25
        self.profit_confirm()
        self.assertEqual(self.p.profit_mode, 'SHADOW')
        await self.p.reconcile(self.ex)
        self.assertEqual(self.p.profit_mode, 'LIVE')
        self.assertFalse(any(t['active'] for t in tr['take_profits']))
        self.assertEqual(tr['profit_policy'], 'DYNAMIC')
        self.assertEqual(tr['stop'], 100.25)
        stop_post = next(i for i,c in enumerate(self.calls) if c[0]=='POST' and c[2].get('triggerPrice')=='100.25')
        tp_cancel = next(i for i,c in enumerate(self.calls) if c[0]=='DELETE' and c[2].get('algoId')==2)
        self.assertLess(stop_post,tp_cancel)

    async def test_no_tp_when_profit_live_already_acknowledged(self):
        self.live(); self.profit_confirm(); await self.p.reconcile(self.ex)
        await self.p.enter(dict(fixtures.SIGNAL, stop=99.5), self.ex)
        self.assertEqual(self.tp_posts(), [])
        self.assertEqual(self.p.profit_mode, 'LIVE')

    async def test_restart_resets_modes_restores_fallback_and_keeps_raised_stop(self):
        tr = await self.enter(); tr['shadow_stop'] = 100.25
        self.profit_confirm(); await self.p.reconcile(self.ex)
        self.p = Pilot(self.connect, clock=lambda:self.now); self.ex.pilot = self.p
        await self.p.reconcile(self.ex)
        recovered = next(iter(self.p.active.values()))
        self.assertEqual((self.p.mode, self.p.profit_mode), ('OFF','SHADOW'))
        self.assertEqual(recovered['stop'], 100.25)
        self.assertEqual(len(self.tp_posts()),2)
        self.assertEqual(recovered['profit_policy'], 'FALLBACK_TP')

    async def test_kill_during_tp_cancel_restores_fallback(self):
        tr = await self.enter(); self.profit_confirm(); self.cancel_hook = self.p.kill
        await self.p.reconcile(self.ex)
        self.assertEqual((self.p.mode,self.p.profit_mode),('OFF','SHADOW'))
        self.assertEqual(len(self.tp_posts()),2)
        self.assertTrue(tr['take_profits'][-1]['active'])

    async def test_ack_timeout_recovers_same_id_without_duplicate(self):
        self.timeout_ack=True; tr=await self.enter()
        self.assertEqual(len(self.tp_posts()),1); self.assertIsNone(tr['pending_tp'])
        await self.p.reconcile(self.ex)
        self.assertEqual(len(self.tp_posts()),1); self.assertFalse(self.p.halted)

    async def test_unknown_tp_fails_closed_with_single_emergency_exit(self):
        self.fail_tp=True; tr=await self.enter()
        self.assertTrue(self.p.halted); self.assertTrue(tr['pending_tp']); self.assertTrue(tr['emergency_client'])
        await self.p.reconcile(self.ex)
        exits=[c for c in self.calls if c[0]=='POST' and c[2].get('type')=='MARKET']
        self.assertEqual(len(exits),1); self.assertEqual(exits[0][2]['reduceOnly'],'true')
        self.assertEqual(len(self.tp_posts()),1)
        self.assertTrue(tr['stops'][0]['active'])

    async def test_bad_ack_identity_does_not_claim_protection(self):
        self.tp_override={'quantity':'5'}; tr=await self.enter()
        self.assertTrue(self.p.halted); self.assertTrue(tr['emergency_client'])
        self.assertNotEqual(tr.get('profit_policy'),'FALLBACK_TP')

    async def test_cancel_ack_with_still_working_tp_cannot_activate_profit(self):
        tr=await self.enter(); self.cancel_ignored=True; self.profit_confirm()
        await self.p.reconcile(self.ex)
        self.assertEqual(self.p.profit_mode,'SHADOW'); self.assertTrue(self.p.halted)
        self.assertTrue(tr['take_profits'][0]['active'])

    async def test_canceled_tp_replaced_once_at_original_target(self):
        tr=await self.enter(); self.orders[2]['algoStatus']='CANCELED'
        self.p.set_mode('OFF'); self.p.configure('fallback_tp_pct',2.)
        await self.p.reconcile(self.ex); await self.p.reconcile(self.ex)
        self.assertEqual(len(self.tp_posts()),2)
        self.assertEqual(self.tp_posts()[0][2]['triggerPrice'],self.tp_posts()[1][2]['triggerPrice'])

    async def test_tp_fill_closes_ledger_and_cancels_remaining_sl(self):
        tr=await self.enter(); self.qty=0
        self.orders[2].update(algoStatus='FINISHED',actualOrderId=44)
        await self.p.reconcile(self.ex)
        self.assertEqual(tr['status'],'CLOSED'); self.assertFalse(self.p.active)
        self.assertEqual(tr['exit_fills'][0]['orderId'],44)
        self.assertAlmostEqual(tr['net'],.048)
        self.assertEqual(self.orders[1]['algoStatus'],'CANCELED')

    async def test_sl_fill_cancels_remaining_tp(self):
        tr=await self.enter(); self.qty=0
        self.orders[1].update(algoStatus='FINISHED',actualOrderId=45)
        await self.p.reconcile(self.ex)
        self.assertEqual(tr['status'],'CLOSED'); self.assertEqual(self.orders[2]['algoStatus'],'CANCELED')

    async def test_pending_tp_after_restart_queries_without_second_post(self):
        tr=await self.enter()
        tp=tr['take_profits'].pop();tr['pending_tp']={'client':tp['client'],'price':tp['price']};self.p.save(tr)
        self.p=Pilot(self.connect,clock=lambda:self.now);self.ex.pilot=self.p
        await self.p.reconcile(self.ex)
        self.assertEqual(len(self.tp_posts()),1);self.assertFalse(self.p.unresolved())

    async def test_dry_off_uses_configured_tp_without_private_requests(self):
        self.p.configure('fallback_tp_pct',.5);self.p.profit_mode='OFF';self.p.set_mode('DRY')
        await self.p.enter(dict(fixtures.SIGNAL, stop=99.5),self.ex)
        tr=next(iter(self.p.active.values()));self.now+=100
        self.p.tick('TESTUSDT',100.49,self.now,self.now,1);self.assertEqual(tr['status'],'OPEN')
        self.now+=100;self.p.tick('TESTUSDT',100.6,self.now,self.now,2)
        self.assertEqual(tr['status'],'CLOSED');self.assertEqual(tr['exit_price'],100.5);self.assertEqual(self.calls,[])

    async def test_profit_flag_required_even_after_user_confirmation(self):
        await self.enter();self.p.profit_live_allowed=False
        with self.assertRaises(Blocked):self.profit_confirm()
        await self.p.reconcile(self.ex)
        self.assertEqual(self.p.profit_mode,'SHADOW');self.assertEqual(len(self.tp_posts()),1)

    async def test_telegram_can_change_margin_positions_and_fallback(self):
        adapter=Integration.__new__(Integration);adapter.pilot=self.p
        adapter.b={'_at_admin_allowed':lambda *a,**k:True,'telegram_send':AsyncMock()}
        for key,value in [('margin',25),('max_positions',2),('fallback_tp_pct',.8)]:
            await adapter.command(None,f'/earlyv2 set {key} {value}','c','u')
            self.assertEqual(getattr(self.p.cfg,key),value)
        await adapter.command(None,'/earlyv2 fallback_tp_pct','c','u')
        self.assertIn('/earlyv2 set fallback_tp_pct',adapter.b['telegram_send'].call_args.args[1])

    async def test_env_flags_do_not_activate_modes_and_startup_logs_both_systems(self):
        with patch.dict('os.environ',{'EARLY_V2_LIVE_ALLOWED':'1','EARLY_V2_SCORE_VALIDATED':'1',
                'EARLY_V2_PROFIT_LIVE_ALLOWED':'1','EARLY_V2_FALLBACK_TP_PCT':'0.8'}):
            with self.assertLogs('early_v2_adapter',level='INFO') as logs:
                adapter=Integration({'db_connect':self.connect,'autotrade_cfg':{'mode':'OFF'}})
        self.assertEqual((adapter.pilot.mode,adapter.pilot.profit_mode),('OFF','SHADOW'))
        self.assertEqual(adapter.pilot.cfg.fallback_tp_pct,.8)
        self.assertIn('Premium=OFF',logs.output[0])

    async def test_profit_off_restores_tp_without_lowering_stop(self):
        tr=await self.enter();tr['shadow_stop']=100.25
        self.profit_confirm();await self.p.reconcile(self.ex)
        self.p.profit_mode='OFF';await self.p.reconcile(self.ex)
        self.assertEqual(tr['stop'],100.25);self.assertEqual(len(self.tp_posts()),2)

    async def test_fallback_behind_ratchet_triggers_owned_emergency_exit(self):
        tr=await self.enter();tr['shadow_stop']=101.2
        self.profit_confirm();await self.p.reconcile(self.ex)
        self.p.kill();await self.p.reconcile(self.ex)
        self.assertTrue(tr['emergency_client']);self.assertEqual(tr['stop'],101.2)
        self.assertEqual(len(self.tp_posts()),1)

    async def test_fallback_reward_is_included_in_rr_price_cap(self):
        from execution_v2 import capped_plan
        cfg=asdict(Config(fallback_tp_pct=.3))
        plan=capped_plan(fixtures.SIGNAL,cfg,fixtures.FILTERS)
        self.assertLessEqual(plan['cap']-plan['stop'],plan['cap']*.003/cfg['min_rr'])
        self.assertEqual(plan['cap'],99.29)

    async def test_invalid_fill_geometry_closes_instead_of_leaving_only_sl(self):
        self.vwap=101.;tr=await self.enter()
        self.assertTrue(self.p.halted);self.assertTrue(tr['emergency_client'])
        self.assertEqual(len(self.tp_posts()),0)

    async def test_numeric_menu_buttons_change_settings_through_authorized_callback(self):
        adapter=Integration.__new__(Integration);adapter.pilot=self.p
        adapter.b={'_at_admin_allowed':lambda *a,**k:True,'telegram_send':AsyncMock(),'telegram_api_call':AsyncMock()}
        for key,value in [('margin',25),('max_positions',2),('fallback_tp_pct',.8)]:
            await adapter.command(None,f'/earlyv2 {key}','c','u')
            buttons=adapter.b['telegram_send'].call_args.kwargs['reply_markup']['inline_keyboard'][0]
            data=next(b['callback_data'] for b in buttons if b['text']==str(value))
            await adapter.callback(None,{'id':'test','data':data,'from':{'id':'u'},'message':{'chat':{'id':'c'}}})
            self.assertEqual(getattr(self.p.cfg,key),value)
        self.live()
        await adapter.command(None,'/earlyv2 set margin 50','c','u')
        self.assertEqual(self.p.cfg.margin,25)

if __name__=='__main__':unittest.main()
