import ast
import copy
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'binance_momentum_bot'))
sys.path.insert(0, str(ROOT/'research'))
from position_management_v2 import Policy, ShadowPosition, entry_reference, summarize
from profit_lock_shadow import early_snapshot, main, run

T = 1800000000000


def cohort():
    return dict(kind='EARLY', early_id='radar:1', symbol='ABCUSDT', episode_id=1,
                decision_ms=T-1000, signal_price=99, initial_stop_price=97,
                fills_complete=True, fills_available_ms=T, executed_qty=4,
                fills=[dict(source='BINANCE', side='BUY', early_id='radar:1',
                            symbol='ABCUSDT', account_ref='test-account', order_id=7,
                            trade_id=10, price=99, qty=3, event_ms=T-500),
                       dict(source='BINANCE', side='BUY', early_id='radar:1',
                            symbol='ABCUSDT', account_ref='test-account', order_id=7,
                            trade_id=11, price=103, qty=1, event_ms=T-200)],
                costs=dict(entry_fee_pct=.05, exit_fee_pct=.05,
                           exit_slippage_pct=.02, funding=[]))


def tick(i, price, stamp=None):
    return dict(symbol='ABCUSDT', trade_id=i, price=price,
                event_ms=stamp or T+i*100, received_ms=stamp or T+i*100)


def replay(prices, c=None, policy=None):
    p = ShadowPosition(c or cohort(), len(prices)*100, policy)
    for i, price in enumerate(prices, 1):
        p.tick(tick(i, price))
    return p, p.finish(T+len(prices)*100, True)


class ProfitLockTests(unittest.TestCase):
    def test_actual_weighted_fills_not_signal_or_ledger(self):
        c=cohort(); c['entry_price']=123
        ref=entry_reference(c)
        self.assertEqual(100, ref['price'])
        self.assertEqual(T, ref['start_ms'])
        c['fills'].append(copy.deepcopy(c['fills'][0]))
        self.assertEqual(100, entry_reference(c)['price'])

    def test_reject_ambiguous_or_unreconciled_fill(self):
        for field, value in [('fills_complete', False), ('executed_qty', 9), ('fills_available_ms',T-300)]:
            with self.subTest(field=field):
                c=cohort(); c[field]=value
                with self.assertRaises(ValueError): entry_reference(c)
        for field, value in [('early_id','premium:1'),('symbol','OTHER'),('side','SELL'),('source','LEDGER')]:
            c=cohort(); c['fills'][0][field]=value
            with self.assertRaises(ValueError): entry_reference(c)
        c=cohort(); c['fills'].append(dict(c['fills'][0], price=102))
        with self.assertRaises(ValueError): entry_reference(c)

    def test_missing_fill_is_explicit_and_proxy_opt_in(self):
        c=cohort(); c['fills']=[]
        p=ShadowPosition(c,1000)
        r=p.finish(T+1000,True)
        self.assertEqual('NO_BINANCE_FILL',r['status'])
        self.assertFalse(r['eligible'])
        p=ShadowPosition(c,1000,allow_signal_proxy=True)
        self.assertEqual('SIGNAL_PRICE_PROXY',p.entry['kind'])

    def test_first_step_then_confirmed_return_exit(self):
        p,r=replay([100.5]*3+[100.2]*3)
        self.assertAlmostEqual(100.25,p.stop)
        self.assertEqual('CONFIRMED_PROFIT_LOCK',r['outcomes']['dynamic']['reason'])
        # Exit uses confirming trade, not optimistic stop level.
        self.assertAlmostEqual(.2,r['outcomes']['dynamic']['gross_pct'])
        self.assertTrue(r['eligible'])

    def test_first_lock_020_variant(self):
        p,_=replay([100.5]*3,policy=Policy(first_lock_pct=.20))
        self.assertAlmostEqual(100.20,p.stop)

    def test_second_step_and_monotonic_stop(self):
        p=ShadowPosition(cohort(),1200)
        stops=[]
        for i,price in enumerate([100.5]*3+[100.8]*3+[100.6]*3+[100.4]*3,1):
            p.tick(tick(i,price)); stops.append(p.stop)
        self.assertEqual(sorted(stops),stops)
        self.assertAlmostEqual(100.5,p.stop)
        self.assertAlmostEqual(.4,p.finish(T+1200,True)['outcomes']['dynamic']['gross_pct'])

    def test_adaptive_runner_keeps_open_above_fixed_target(self):
        p,r=replay([100.5]*3+[100.8]*3+[101.2]*3+[103.0]*3+[102.0]*3)
        self.assertAlmostEqual(102,p.stop)
        self.assertAlmostEqual(2,r['outcomes']['dynamic']['gross_pct'])
        self.assertAlmostEqual(1,r['outcomes']['fixed']['gross_pct'])
        self.assertAlmostEqual(2/3,r['outcomes']['dynamic']['runner_capture'])

    def test_one_wick_cannot_arm_or_ratchet(self):
        p,_=replay([100,103,100,100,100.8,100.8,100.8,110,100.7,100.7])
        self.assertAlmostEqual(100.5,p.stop)
        self.assertAlmostEqual(.8,p.confirmed_peak)

    def test_single_down_tick_does_not_profit_exit(self):
        p,r=replay([100.8]*3+[100.4,100.7,100.4,100.7])
        self.assertIsNone(p.exits['dynamic'])
        self.assertEqual('HORIZON_MARK_PROXY',r['outcomes']['dynamic']['reason'])

    def test_initial_sl_immediate_even_during_profit_confirmation(self):
        _,r=replay([100.8]*3+[100.4,96])
        self.assertEqual('INITIAL_SL',r['outcomes']['dynamic']['reason'])
        self.assertAlmostEqual(-4,r['outcomes']['dynamic']['gross_pct'])

    def test_minimum_duration_and_duplicate_trade_do_not_confirm(self):
        p=ShadowPosition(cohort(),1000)
        for i in range(1,4): p.tick(tick(i,100.8,T+i))
        for _ in range(10): p.tick(tick(3,100.8,T+3))
        self.assertEqual(97,p.stop)

    def test_distinct_same_timestamp_trades_not_discarded(self):
        p=ShadowPosition(cohort(),1000)
        for i in range(1,4): p.tick(tick(i,101,T+100))
        self.assertEqual(3,p.last['trade_id'])
        self.assertEqual(97,p.stop)
        self.assertFalse(p.flags)

    def test_high_frequency_trades_can_confirm_after_minimum_time(self):
        p=ShadowPosition(cohort(),1000)
        for i in range(1,203):p.tick(tick(i,100.8,T+i))
        self.assertAlmostEqual(100.5,p.stop)
        self.assertLess(p.window.count,3)

    def test_touch_order_ties_and_post_exit_observation(self):
        _,r=replay([101.2,97,105])
        self.assertEqual([.5,1.],r['first_touch_order'][0]['levels'])
        self.assertEqual([-3.],r['first_touch_order'][1]['levels'])
        self.assertAlmostEqual(5,r['mfe_pct'])
        self.assertAlmostEqual(-3,r['mae_pct'])
        self.assertAlmostEqual(4.2,r['peak_to_trough_drawdown_proxy_pp'])

    def test_gap_breaks_confirmation_and_excludes_economics(self):
        p=ShadowPosition(cohort(),5000)
        p.tick(tick(1,100.8));p.tick(tick(2,100.8));p.tick(tick(3,100.8,T+4000))
        self.assertEqual(97,p.stop)
        r=p.finish(T+5000,True)
        self.assertFalse(r['eligible']);self.assertIn('TRADE_OBSERVATION_GAP',r['flags'])

    def test_bad_trade_types_order_nan_future_and_missing_ids(self):
        variants=[dict(tick(2,101),price=float('nan')),tick(0,101),
                  dict(tick(2,101),received_ms=T+100),tick(4,101),
                  dict(tick(2,101),symbol='WRONG')]
        for bad in variants:
            p=ShadowPosition(cohort(),1000);p.tick(tick(1,100));p.tick(bad)
            self.assertFalse(p.finish(T+1000,True)['eligible'])
            self.assertTrue(p.flags)

    def test_pre_entry_and_post_horizon_prices_do_not_leak(self):
        p=ShadowPosition(cohort(),1000)
        p.tick(tick(0,120,T-1));p.tick(tick(1,100,T+1000));p.tick(tick(2,130,T+1001))
        r=p.finish(T+1000,True)
        self.assertEqual(0,r['mfe_pct'])

    def test_no_stale_timeout_fills_or_complete_claim_without_assertion(self):
        p=ShadowPosition(cohort(),10000);p.tick(tick(1,100))
        r=p.finish(T+10000,True)
        self.assertEqual('OPEN_CENSORED',r['outcomes']['fixed']['reason'])
        self.assertFalse(r['eligible'])
        p,_=replay([100]*3)
        self.assertFalse(p.finish(T+300,False)['eligible'])

    def test_missing_costs_not_zero_and_funding_per_exit(self):
        c=cohort();c['costs'].pop('funding')
        _,r=replay([101.2]*3+[100.7]*3,c)
        self.assertIsNone(r['outcomes']['fixed']['net_pct'])
        self.assertFalse(r['eligible'])
        c=cohort();c['costs']['funding']=[dict(event_ms=T+400,pct=.1)]
        _,r=replay([101.2]*3+[100.7]*3,c)
        self.assertEqual(0,r['outcomes']['fixed']['funding_pct'])
        self.assertEqual(.1,r['outcomes']['dynamic']['funding_pct'])
        ratio=1.007*(1-.02/100)
        self.assertAlmostEqual(100*(ratio-1)-.05-.05*ratio-.1,r['outcomes']['dynamic']['net_pct'])

    def test_exit_before_runner_still_measured(self):
        _,r=replay([100.5]*3+[100.2]*3+[104]*3)
        self.assertTrue(r['outcomes']['dynamic']['early_exit'])
        self.assertAlmostEqual(.05,r['outcomes']['dynamic']['runner_capture'])

    def test_summary_complete_pairs_and_reference_separation(self):
        _,good=replay([100.5]*3+[100.2]*3)
        bad=copy.deepcopy(good);bad['eligible']=False
        proxy=copy.deepcopy(good);proxy['reference']['kind']='SIGNAL_PRICE_PROXY'
        report=summarize([good,bad,proxy])
        self.assertEqual(2,len(report['groups']))
        group=next(v for k,v in report['groups'].items() if k.startswith('BINANCE'))
        self.assertEqual(2,group['total']);self.assertEqual(1,group['eligible'])
        self.assertEqual(.5,group['coverage'])

    def test_finish_is_idempotent_and_does_not_freeze_open_session(self):
        p=ShadowPosition(cohort(),1000);p.tick(tick(1,100))
        first=p.finish(T+100,False)
        self.assertEqual(first,p.finish(T+100,False))
        p.tick(tick(2,101))
        self.assertEqual('FIXED_TP_1',p.exits['fixed']['reason'])

    def test_existing_sl_is_preserved_and_not_reset_to_minus_three(self):
        c=cohort();c['initial_stop_price']=99
        _,r=replay([98.9,98.8,98.7],c)
        self.assertEqual(99,r['initial_stop_price'])
        self.assertAlmostEqual(-1.1,r['outcomes']['dynamic']['gross_pct'])
        self.assertNotIn('-3.0',r['first_touches'])

    def test_tiny_price_scale_has_identical_percentage_behavior(self):
        c=cohort();scale=1e-8
        c['initial_stop_price']*=scale
        c['signal_price']*=scale
        for f in c['fills']: f['price']*=scale
        _,tiny=replay([p*scale for p in [100.5]*3+[100.2]*3],c)
        _,normal=replay([100.5]*3+[100.2]*3)
        self.assertAlmostEqual(normal['outcomes']['dynamic']['net_pct'],tiny['outcomes']['dynamic']['net_pct'])
        self.assertEqual('CONFIRMED_PROFIT_LOCK',tiny['outcomes']['dynamic']['reason'])

    def test_observation_watermark_cannot_use_unreceived_tick(self):
        p=ShadowPosition(cohort(),300)
        t=tick(1,101,T+300);t['received_ms']=T+400;p.tick(t)
        self.assertFalse(p.finish(T+300,True)['eligible'])

    def test_invalid_policy_and_non_early_rejected(self):
        for kw in [dict(confirm_trades=1),dict(first_lock_pct=.3),dict(trail_peak_fraction=1),dict(confirm_ms=float('nan'))]:
            with self.assertRaises(ValueError): Policy(**kw)
        with self.assertRaises(ValueError): ShadowPosition(dict(cohort(),kind='PREMIUM'),1000)

    def test_module_cannot_access_production_or_exchange(self):
        tree=ast.parse((ROOT/'binance_momentum_bot/position_management_v2.py').read_text())
        imports=set()
        for node in ast.walk(tree):
            if isinstance(node,ast.Import):imports.update(n.name for n in node.names)
            elif isinstance(node,ast.ImportFrom):imports.add(node.module)
        self.assertLessEqual(imports,{'collections','dataclasses','hashlib','json','math'})
        bot=(ROOT/'binance_momentum_bot/bot.py').read_text(encoding='utf-8')
        self.assertNotIn('position_management_v2',bot)


class ReplayTests(unittest.TestCase):
    def test_snapshot_read_only_and_all_early_counted(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'source.sqlite'
            c=sqlite3.connect(path)
            c.execute('CREATE TABLE radar_signals(id,symbol,price,ts,notified,notify_ts)')
            c.executemany('INSERT INTO radar_signals VALUES (?,?,?,?,?,?)',
                          [(1,'ABCUSDT',100,T//1000,1,None),(2,'ABCUSDT',100,T//1000,0,None)])
            c.commit();c.close();before=path.read_bytes()
            cohorts=list(early_snapshot(path))
            self.assertEqual(1,len(cohorts));self.assertEqual([],cohorts[0]['fills'])
            self.assertEqual(before,path.read_bytes())

    def test_replay_duplicate_idempotency_and_bounds(self):
        trades=[tick(i,100.5) for i in range(1,4)]
        rows,_=run([cohort(),cohort()],trades,observation_end_ms=T+300,coverage_complete=True,horizons=(300,))
        self.assertEqual(1,len(rows))
        with self.assertRaises(ValueError):
            run([cohort(),dict(cohort(),signal_price=101)],trades,observation_end_ms=T+300)
        with self.assertRaises(ValueError):
            run([cohort()],trades,observation_end_ms=T+300,max_cohorts=0)

    def test_cli_reproducible_and_never_overwrites(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);events=root/'cohorts.jsonl';trades=root/'trades.jsonl'
            events.write_text(json.dumps(cohort())+'\n')
            trades.write_text(''.join(json.dumps(tick(i,100.5))+'\n' for i in range(1,4)))
            args=['--cohorts',str(events),'--trades',str(trades),'--observation-end-ms',str(T+300),
                  '--horizons-ms','300','--coverage-complete']
            main(args+['--out',str(root/'a')]);main(args+['--out',str(root/'b')])
            for name in ('records.jsonl','summary.json','manifest.json'):
                self.assertEqual((root/'a'/name).read_bytes(),(root/'b'/name).read_bytes())
            with self.assertRaises(SystemExit):main(args+['--out',str(root/'a')])


if __name__ == '__main__':
    unittest.main()
