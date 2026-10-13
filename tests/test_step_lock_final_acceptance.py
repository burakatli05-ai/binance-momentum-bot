"""Final scoped acceptance. Real candidate functions, SQLite and injected HTTP only."""
import asyncio
import ast
import hashlib
from copy import deepcopy
from decimal import Decimal as D
import json
import pathlib
import sqlite3
import unittest
from unittest.mock import patch
import premium_step_lock as sl
import premium_step_lock_qro as qro
import test_premium_step_lock_qro as previous


class ProtocolAcceptance(unittest.TestCase):
    def setUp(self):
        self.store = previous.MemoryStore()
        self.http = previous.HTTP(self.store)
        self.old = self.http.seed()

    def runq(self):
        adapter = qro.BinancePremiumStepLockQROExchange(self.http, enabled=True,
                                                      clock=lambda: 1, wall=lambda: 100)
        return asyncio.run(adapter.reconcile_trade(7, self.store))

    def state(self):
        return json.loads(self.store.tr['qro_state'])

    def writes(self):
        return [c for c in self.http.calls if c[0] in ('POST', 'DELETE')]

    def tick(self, price, ts):
        update = sl.tick_fields(self.store.tr, price, ts, '0.01')
        self.store.tr.update(update)
        return update

    def test_T1_all_frozen_steps_monotonic_and_cap(self):
        for i, bp in enumerate(range(50, 1001, 50), 1):
            price = D(100) * (1 + D(bp)/10000)
            self.http.price = str(price + 1)
            self.tick(str(price), 2000+i)
            self.assertEqual(self.runq(), 'REPLACED')
            expected = D('100.25') if bp == 50 else D(100)*(1+D(bp-50)/10000)
            self.assertEqual(D(self.store.tr['sl_active_stop_price']), expected)
            self.assertEqual(self.store.tr['stop_revision'], i)
            self.assertEqual(self.tick(str(price), 2000+i), {})
            self.assertEqual(self.tick(str(price-D('0.1')), 3000+i), {})
            self.assertEqual(self.runq(), 'STABLE')
        self.assertEqual(self.tick('150', 5000), {})
        self.assertEqual(self.store.tr['stop_revision'], 20)
        self.assertEqual(sum(c[0]=='POST' for c in self.writes()), 20)

    def test_T1_economics_and_unrelated_source_unchanged(self):
        self.assertEqual(sl.CONTRACT_HASH, '29e695ec40d2c12b6037db8ad6cbede27da5cac0f58a01054e491b2967746cd3')
        self.assertEqual(sl.SPEC['scope'], 'new_premium_autotrade_long_positions_only')
        root = pathlib.Path(__file__).resolve().parents[1]
        manifest = json.loads((root/'tests/final_acceptance_noninterference.json').read_text())
        functions = {n.name: ast.dump(n, include_attributes=False)
                     for n in ast.parse((root/'binance_momentum_bot/bot.py').read_text(encoding='utf-8')).body
                     if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
        self.assertEqual(set(functions), set(manifest['baseline_function_hashes']))
        for name, previous_hash in manifest['baseline_function_hashes'].items():
            expected = manifest['candidate_changed_function_hashes'].get(name, previous_hash)
            self.assertEqual(hashlib.sha256(functions[name].encode()).hexdigest(), expected, name)

    def test_T2_duplicate_and_response_loss_single_revision(self):
        self.tick('101', 2000)
        self.http.post_loss = self.http.cancel_loss = True
        self.assertEqual(self.runq(), 'REPLACED')
        before = self.writes()
        self.assertEqual(self.tick('101', 2000), {})
        for _ in range(3):
            self.assertEqual(self.runq(), 'STABLE')
        self.assertEqual(self.writes(), before)
        self.assertEqual([c[0] for c in before], ['POST', 'DELETE'])

    def test_T2_delayed_ack_remains_fail_closed_across_restart(self):
        self.tick('101', 2000)
        def lose_new_ack(method, route, params):
            if method == 'POST':
                self.http.query_errors = 100
        self.http.hook = lose_new_ack
        self.runq()
        self.assertTrue(self.state()['quarantine'])
        count = len(self.writes())
        for _ in range(3):
            self.runq()
        self.assertEqual(len(self.writes()), count)
        self.assertEqual(sum(c[0]=='POST' for c in self.writes()), 1)
        self.assertEqual(sum(c[0]=='DELETE' for c in self.writes()), 0)

    def test_T3_long_and_short_hedge_leg_routing(self):
        # Independent two-leg fill oracle. This does not certify Binance execution.
        for leg, action in [('LONG', 'SELL'), ('SHORT', 'BUY')]:
            with self.subTest(leg=leg):
                tr = dict(self.store.tr, side=leg, position_side=leg)
                p = qro.request(tr, qro.client_id(7, 1), D('2'), D('100'))
                self.assertEqual((p['side'], p['positionSide']), (action, leg))
                self.assertNotIn('reduceOnly', p)
                positions = {'LONG': D('2'), 'SHORT': D('3')}
                other = 'SHORT' if leg == 'LONG' else 'LONG'
                untouched = positions[other]
                # A closing leg order is capped to remaining owned size in this oracle.
                for requested_fill in [D('.75'), D('1.25'), D('2')]:
                    fill = min(requested_fill, positions[leg], D(p['quantity']))
                    positions[leg] -= fill
                    self.assertGreaterEqual(positions[leg], 0)
                    self.assertEqual(positions[other], untouched)
                self.assertEqual(positions[leg], 0)
                ack = dict(p, algoId='301', orderType='STOP_MARKET', algoStatus='NEW',
                           closePosition=False, priceProtect=False)
                qro.verify_order(ack, {'params': p}, tr)
                ack['positionSide'] = other
                with self.assertRaises(qro.Block):
                    qro.verify_order(ack, {'params': p}, tr)

    def test_T3_existing_real_adapter_still_rejects_hedge(self):
        self.http.hedge = True
        self.assertEqual(self.runq(), 'QRO_ACCOUNT_MODE_NOT_CERTIFIED')
        self.assertEqual(self.writes(), [])
        self.assertFalse(qro.MANAGEMENT_ENABLED)
        self.assertFalse(qro.LIVE_ACTIVATION_READY)

    def test_T4_oversize_and_inconsistent_leg_rejected(self):
        for tr, qty in [(self.store.tr, D('2.001')),
                        (dict(self.store.tr, side='SHORT', position_side='LONG'), D('2'))]:
            with self.subTest(qty=qty, side=tr['side']):
                with self.assertRaises(qro.Block):
                    qro.request(tr, qro.client_id(7, 1), qty, D('100'))

    def test_T4_partial_remaining_exact_no_revision_or_reverse(self):
        self.http.trigger(self.old, qty='.75')
        self.assertEqual(self.runq(), 'PARTIAL_FILL_REQUIRES_MANUAL_REVIEW')
        state = self.state()
        self.assertEqual(D(state['observed_owned_qty']), D('1.25'))
        self.assertEqual(D(state['filled_exit_qty']), D('.75'))
        self.assertEqual(D(state['observed_owned_qty']) + D(state['filled_exit_qty']), D('2'))
        self.tick('105', 4000)
        self.runq()
        self.assertEqual(self.writes(), [])
        self.assertEqual(self.state()['revision'], 0)

    def test_T4_overlap_unresolved_no_third_and_flat_cleanup(self):
        self.tick('101', 2000)
        self.http.cancel_noop = True
        self.runq()
        self.assertTrue(self.state()['quarantine'])
        before = len(self.writes())
        self.tick('105', 3000)
        self.runq()
        self.assertEqual(len(self.writes()), before)
        self.assertEqual(len(self.http.algos), 2)

    def test_T5_confirmed_generation_survives_fresh_store(self):
        self.tick('101', 2000)
        self.assertEqual(self.runq(), 'REPLACED')
        confirmed = deepcopy(self.store.tr)
        replacement = previous.MemoryStore()
        replacement.tr = json.loads(json.dumps(confirmed))
        self.store = replacement
        self.http.store = replacement
        before = len(self.writes())
        self.assertEqual(self.runq(), 'STABLE')
        self.assertEqual(self.store.tr['stop_revision'], 1)
        self.assertEqual(D(self.store.tr['sl_active_stop_price']), D('100.5'))
        self.assertEqual(len(self.writes()), before)
        self.assertEqual(self.tick('100.5', 1900), {})


class TerminalAcceptance(unittest.TestCase):
    def setUp(self):
        import test_v5135 as legacy
        legacy.DatabaseCase.setUp(self)
        self.sql = legacy.DatabaseCase.sql.__get__(self)
        self.rows = legacy.DatabaseCase.rows.__get__(self)
        import bot
        self.bot = bot
        tr = previous.trade()
        tr.update(margin_usdt=20, leverage=10, notional_usdt=200, updated_ts=1)
        keys = list(tr)
        self.sql('INSERT INTO autotrade_trades ('+','.join(keys)+') VALUES ('+','.join('?' for _ in keys)+')', list(tr.values()))
        self.store = bot._at_qro_store()
        self.http = previous.HTTP(self.store)
        class Readback:
            @property
            def tr(inner):
                return self.store.load(7)
        self.http.store = Readback()
        self.economics = True
        self.crash_after_flat = False

    async def request(self, method, route, params):
        result = await self.http(method, route, params)
        if route.endswith('/userTrades') and self.economics:
            for row in result:
                row.update(realizedPnl='0' if row['side']=='BUY' else '2',
                           commission='.10', commissionAsset='USDT', time=120000)
                if row['side'] == 'SELL':
                    row['price'] = '101'
        return result

    def cycle(self):
        x = qro.BinancePremiumStepLockQROExchange(self.request, enabled=True, clock=lambda:1, wall=lambda:100)
        with patch.object(self.bot, '_at_qro_exchange', return_value=x):
            asyncio.run(self.bot._at_step_recovery_check(None, self.store.load(7), []))

    def prepare_full_fill(self):
        self.cycle()
        self.http.trigger(qro.client_id(7, 0), qty='2', status='FILLED')

    def test_T6_flat_clean_closes_and_ledger_once(self):
        self.prepare_full_fill()
        self.cycle()
        tr = self.store.load(7)
        self.assertEqual(tr['status'], 'CLOSED')
        self.assertEqual(json.loads(tr['qro_state'])['phase'], 'CLOSED')
        self.assertAlmostEqual(tr['net_pnl'], 1.8)
        self.assertEqual(len(self.rows("SELECT * FROM autotrade_events WHERE event='CLOSE' AND trade_id=7")), 1)
        daily = self.rows('SELECT * FROM autotrade_daily')
        self.assertAlmostEqual(daily[0]['realized_net_pnl'], 1.8)
        before = deepcopy(self.http.calls)
        for _ in range(3):
            self.cycle()
        self.assertEqual(self.http.calls, before)
        self.assertEqual(self.rows('SELECT * FROM autotrade_daily'), daily)
        self.assertEqual(sl.tick_fields(tr, '120', 200000, '.01'), {})
        self.assertFalse(self.http.normal['900']['status'] not in qro.CHILD_TERMINAL)
        self.assertFalse(any(o['algoStatus']=='NEW' for o in self.http.algos.values()))

    def test_T5_crash_after_flat_recovers_terminal_without_order_replay(self):
        self.prepare_full_fill()
        # Actual adapter persists FLAT_CLEAN; crash before application bridge.
        x = qro.BinancePremiumStepLockQROExchange(self.request, enabled=True, clock=lambda:1, wall=lambda:100)
        self.assertEqual(asyncio.run(x.reconcile_trade(7,self.store)), 'FLAT_CLEAN')
        before = deepcopy(self.http.calls)
        self.store = qro.SQLiteStore(self.bot.db_connect)
        self.cycle()
        self.assertEqual(self.store.load(7)['status'], 'CLOSED')
        self.assertEqual(self.http.calls, before)
        self.assertEqual(len(self.rows("SELECT * FROM autotrade_events WHERE event='CLOSE'")), 1)

    def test_T6_atomic_close_rollback_and_retry(self):
        self.prepare_full_fill()
        self.sql("CREATE TRIGGER fail_terminal BEFORE INSERT ON autotrade_events WHEN NEW.event='CLOSE' BEGIN SELECT RAISE(ABORT,'test crash'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.cycle()
        self.assertEqual(self.store.load(7)['status'], 'OPEN')
        self.assertEqual(self.rows('SELECT * FROM autotrade_daily'), [])
        self.sql('DROP TRIGGER fail_terminal')
        before = deepcopy(self.http.calls)
        self.cycle()
        self.assertEqual(self.store.load(7)['status'], 'CLOSED')
        self.assertEqual(self.http.calls, before)
        self.assertAlmostEqual(self.rows('SELECT * FROM autotrade_daily')[0]['realized_net_pnl'], 1.8)

    def test_T6_missing_fill_economics_never_invented(self):
        self.economics = False
        self.prepare_full_fill()
        self.cycle()
        tr = self.store.load(7)
        self.assertNotEqual(tr['status'], 'CLOSED')
        self.assertEqual(self.rows('SELECT * FROM autotrade_daily'), [])
        self.assertEqual(len(self.rows("SELECT * FROM autotrade_events WHERE event='CLOSE'")), 0)

    def test_T6_unclean_residual_never_closes(self):
        self.cycle()
        self.http.qty = '0'
        self.http.cancel_noop = True
        self.cycle()
        self.assertNotEqual(self.store.load(7)['status'], 'CLOSED')
        self.assertEqual(self.rows('SELECT * FROM autotrade_daily'), [])

    def test_T4_T6_partial_then_complete_closes_without_rearming_manual_latch(self):
        self.cycle()
        self.http.trigger(qro.client_id(7, 0), qty='.75')
        self.cycle()
        self.assertEqual(D(json.loads(self.store.load(7)['qro_state'])['observed_owned_qty']), D('1.25'))
        before = [c for c in self.http.calls if c[0]=='POST']
        self.http.trigger(qro.client_id(7, 0), qty='2', status='FILLED')
        self.cycle()
        tr = self.store.load(7)
        self.assertEqual(tr['status'], 'CLOSED')
        self.assertEqual(tr['manual_intervention'], 1)
        self.assertTrue(qro.local_residual(tr))
        self.assertEqual([c for c in self.http.calls if c[0]=='POST'], before)
        self.assertAlmostEqual(tr['net_pnl'], 1.8)

