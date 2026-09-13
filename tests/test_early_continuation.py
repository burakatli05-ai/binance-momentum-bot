import ast
import hashlib
import json
from pathlib import Path
import random
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

import test_v5135 as fixtures
from ec_test_support import production_tree
import early_continuation as ec
import early_continuation_bridge as bridge

bot = fixtures.bot
ROOT = fixtures.ROOT


class FlagTests(unittest.TestCase):
    def test_c1_boundaries(self):
        m = dict(chg30=.40, rel30=.30, flow30=2.999999)
        self.assertTrue(ec.flags(m)['C1'])
        for key, value in [('chg30', .399999), ('rel30', .299999), ('flow30', 3.0)]:
            self.assertFalse(ec.flags(dict(m, **{key: value}))['C1'])

    def test_t1_formula_and_boundaries(self):
        self.assertTrue(ec.flags(dict(chg30=.4, flow30=2))['T1'])
        self.assertFalse(ec.flags(dict(chg30=.399999, flow30=2))['T1'])
        self.assertFalse(ec.flags(dict(chg30=1, flow30=3))['T1'])
        self.assertAlmostEqual(4, ec.flags(dict(chg30=.4, flow30=0))['flow_eff30'])
        self.assertTrue(ec.flags(dict(chg30=.02, flow30=0))['T1'])
        self.assertFalse(ec.flags(dict(chg30=.01999999, flow30=0))['T1'])

    def test_t2_inclusive_range(self):
        for value, expected in [(-1.50, True), (-.40, True), (-1.50001, False), (-.39999, False)]:
            self.assertEqual(expected, ec.flags(dict(max_dd10=value))['T2'])

    def test_t3_rank(self):
        for value, expected in [(0, False), (1, True), (30, True), (31, False)]:
            self.assertEqual(expected, ec.flags(dict(gainers_rank=value))['T3'])

    def test_unknown_not_false_or_zero(self):
        self.assertTrue(all(v is None for v in ec.flags({}).values()))
        self.assertIsNone(ec.flags(dict(chg30=float('nan'), flow30=1))['T1'])


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.c = sqlite3.connect(':memory:')
        self.addCleanup(self.c.close)
        self.now = 2_000_000
        self.e = ec.Engine(self.c, {}, 'code', 'test', self.now)
        self.serial = 0

    def event(self, kind='candidate_start', ts=None, symbol='XUSDT', **meta):
        self.serial += 1
        ts = self.now if ts is None else ts
        e = dict(event_id=str(self.serial), symbol=symbol, event_type=kind,
                 operands=dict(chg30=.4, rel30=.3, flow30=2, max_dd10=-.5, gainers_rank=2),
                 decision_ts=ts, observed_ts=ts, price=100, **meta)
        self.e.event(e)
        return e

    def rows(self, sql, params=()):
        self.c.row_factory = sqlite3.Row
        return [dict(r) for r in self.c.execute(sql, params)]

    def test_c2_prior_symbol_type_window_and_equal_timestamp(self):
        t = self.now+1_800_000
        self.event(ts=t-1_800_001)
        self.event(ts=t-1_800_000)
        self.event('early_alert', ts=t-500)
        self.event(ts=t-100, symbol='OTHER')
        self.event(ts=t-1)
        first = self.event(ts=t)
        second = self.event(ts=t)
        for e in (first, second):
            row = self.rows('SELECT * FROM early_continuation_flags WHERE event_id=?', (e['event_id'],))[0]
            self.assertEqual(2, row['prior_candidate_count'])
            self.assertEqual(1, row['C2'])
        row = self.event(ts=t+1_800_001)
        self.assertEqual(0, self.rows('SELECT C2 FROM early_continuation_flags WHERE event_id=?', (row['event_id'],))[0]['C2'])

    def test_partial_history_and_queue_loss_are_unknown(self):
        self.event()
        self.assertIsNone(self.rows('SELECT C2 FROM early_continuation_flags')[0]['C2'])
        self.e.gap(None, 'QUEUE_LOSS', None, self.now+1)
        self.event(ts=self.now+2)
        self.assertIsNone(self.rows('SELECT C2 FROM early_continuation_flags ORDER BY decision_ts DESC')[0]['C2'])

    def test_additive_idempotent_no_legacy_changes_or_backfill(self):
        self.c.execute('CREATE TABLE signals_v2(id INTEGER PRIMARY KEY, price REAL)')
        self.c.execute('INSERT INTO signals_v2 VALUES(1,123)')
        before = list(self.c.execute('SELECT * FROM signals_v2'))
        ec.migrate(self.c)
        ec.migrate(self.c)
        self.assertEqual(before, list(self.c.execute('SELECT * FROM signals_v2')))
        self.assertEqual(0, self.c.execute('SELECT COUNT(*) FROM early_continuation_flags').fetchone()[0])

    def test_market_observation_continues_after_trade_tp_and_stop(self):
        event = self.event()
        # An unrelated legacy trade may finish at TP or STOP; the market observer never reads it.
        self.c.execute('CREATE TABLE legacy_trade(label TEXT)')
        self.c.execute("INSERT INTO legacy_trade VALUES ('TP')")
        for seconds in range(1, 3601):
            price = 98 if seconds == 2 else (112 if seconds >= 1800 else 102)
            self.e.tick('XUSDT', price, self.now+seconds*1000, self.now+seconds*1000)
            if seconds == 2: self.c.execute("UPDATE legacy_trade SET label='STOP'")
        self.e.flush(self.now+3_600_001)
        outcomes = self.rows('SELECT * FROM early_continuation_outcomes ORDER BY horizon_s')
        self.assertEqual([300, 900, 1800, 3600], [r['horizon_s'] for r in outcomes])
        self.assertAlmostEqual(12, outcomes[-1]['mfe_pct'])
        self.assertAlmostEqual(-2, outcomes[-1]['mae_pct'])
        self.assertAlmostEqual(12, outcomes[-1]['return_pct'])
        self.assertEqual(self.now+1_800_000, json.loads(outcomes[-1]['touches_json'])['10'])
        self.assertNotIn(event['event_id'], self.e.active)

    def test_horizon_extrema_do_not_leak_later_tick(self):
        self.event()
        for second in range(1, 300): self.e.tick('XUSDT', 101, self.now+second*1000, self.now+second*1000)
        self.e.tick('XUSDT', 120, self.now+301000, self.now+301000)
        row = self.rows('SELECT * FROM early_continuation_outcomes')[0]
        self.assertAlmostEqual(1, row['mfe_pct'])
        self.assertAlmostEqual(20, row['return_pct'])
        self.assertNotIn('10', json.loads(row['touches_json']))
        self.assertEqual(self.now+301000, row['source_event_ts'])

    def test_missing_and_stale_prices_do_not_fabricate_outcomes(self):
        self.event()
        self.e.tick('XUSDT', 130, self.now+1, self.now+100_000)
        self.e.flush(self.now+3_606_000)
        outcomes = self.rows('SELECT * FROM early_continuation_outcomes')
        self.assertEqual(4, len(outcomes))
        for r in outcomes:
            self.assertIsNone(r['close_price'])
            self.assertIsNone(r['mfe_pct'])
            self.assertEqual(1, r['observation_gap'])

    def test_delayed_first_tick_is_marked_gapped(self):
        self.event()
        self.e.tick('XUSDT', 106, self.now+300000, self.now+300000)
        row = self.rows('SELECT * FROM early_continuation_outcomes')[0]
        self.assertEqual(1, row['observation_gap'])
        self.assertEqual('GAPPED', row['status'])

    def test_stale_stream_diagnostics_are_rate_limited(self):
        self.event()
        for offset in range(100):
            self.e.tick('XUSDT', 100, None, self.now+offset)
        self.assertEqual(1, self.c.execute("SELECT COUNT(*) FROM research_runtime_gaps WHERE kind='STALE_OR_MISSING_SOURCE'").fetchone()[0])

    def test_restart_restores_pending_but_never_links_old_episode(self):
        first = self.event(source_episode_id=7)
        self.e.tick('XUSDT', 101, self.now+1, self.now+1)
        self.e.flush(self.now+2)
        self.e = ec.Engine(self.c, {}, 'code', 'test', self.now+3)
        self.assertIn(first['event_id'], self.e.active)
        second = self.event('early_alert', ts=self.now+4, source_episode_id=7)
        rows = self.rows('SELECT * FROM early_continuation_flags ORDER BY decision_ts')
        self.assertIsNone(rows[-1]['parent_event_id'])
        self.assertNotEqual(rows[0]['episode_id'], rows[-1]['episode_id'])
        self.assertIsNone(rows[-1]['C2'])
        self.assertTrue(self.e.active[first['event_id']]['gap'])

    def test_gap_reset_and_new_candidate_break_linkage(self):
        self.event(source_episode_id=1)
        self.e.tick('XUSDT', 100, self.now+1, self.now+1)
        self.event('early_alert', ts=self.now+2, source_episode_id=1)
        linked = self.rows('SELECT * FROM early_continuation_flags ORDER BY decision_ts')[-1]
        self.assertEqual('1', linked['parent_event_id'])
        self.e.gap('XUSDT', 'SOURCE_HEARTBEAT_GAP', self.now+2, self.now+20_000)
        self.event('early_alert', ts=self.now+20_001, source_episode_id=1)
        gap = self.rows('SELECT * FROM early_continuation_flags ORDER BY decision_ts')[-1]
        self.assertIsNone(gap['parent_event_id'])
        self.e.tick('XUSDT', 100, self.now+20_002, self.now+20_002)
        self.event('episode_reset', ts=self.now+20_003, source_episode_id=1)
        self.event(ts=self.now+20_004, source_episode_id=1)
        self.assertIsNone(self.rows('SELECT * FROM early_continuation_flags ORDER BY decision_ts')[-1]['parent_event_id'])

    def test_trend_to_candidate_link_requires_continuity(self):
        self.event('trend_evaluation')
        self.e.tick('XUSDT', 100, self.now+1, self.now+1)
        self.event(ts=self.now+2, source_episode_id=3)
        rows = self.rows('SELECT * FROM early_continuation_flags ORDER BY decision_ts')
        self.assertEqual(rows[0]['trend_id'], rows[1]['trend_id'])
        self.assertEqual('CONTIGUOUS_TREND_TO_CANDIDATE', rows[1]['linkage_reason'])

    def test_null_timing_and_event_idempotency(self):
        e = self.event()
        self.e.event(e)
        rows = self.rows('SELECT * FROM early_continuation_flags')
        self.assertEqual(1, len(rows))
        for key in ('nominal_ts', 'ready_ts', 'source_event_ts', 'receive_ts', 'fill_ts', 'source_id'):
            self.assertIsNone(rows[0][key])

    def test_first_touch_is_frozen_and_no_predecision_tick(self):
        self.event()
        self.e.tick('XUSDT', 200, self.now-1, self.now)
        self.e.tick('XUSDT', 111, self.now+1, self.now+1)
        self.e.tick('XUSDT', 120, self.now+2, self.now+2)
        p = next(iter(self.e.active.values()))
        self.assertEqual(self.now+1, p['touches']['10'])


class IntegrationTests(unittest.TestCase):
    def test_capture_whitelists_predictors_and_preserves_missing_timing(self):
        w = ec.Collector('unused', {}, 'test', 'code')
        st = bot.SymbolState()
        m = dict(price=100, chg30=.4, rel30=.3, flow30=2,
                 future_peak=500, progress_label='WIN', gate_results_later='PASS')
        scope = dict(vars(bot), states={'X': st}, gainers_prev_rank={})
        with patch.object(bridge, 'worker', w):
            bridge.capture(scope, 'X', 'candidate_start', m, 70, source_id=17, terminal='candidate_start')
        _, e = w.queue.get_nowait()
        self.assertEqual(17, e['source_id'])
        self.assertIsNone(e['terminal'])
        self.assertIsNone(e['source_event_ts'])
        self.assertIsNone(e['ready_ts'])
        self.assertNotIn('future_peak', e['operands'])
        self.assertNotIn('progress_label', e['operands'])
        self.assertNotIn('gate_results_later', e['operands'])

    def test_entire_production_ast_and_other_application_files_unchanged(self):
        baseline = json.loads((ROOT/'tests/early_continuation_baseline.json').read_text())
        tree = production_tree(ast.parse((ROOT/'binance_momentum_bot/bot.py').read_text(encoding='utf-8')))
        self.assertEqual(baseline['bot_ast_sha256'], hashlib.sha256(ast.dump(tree, include_attributes=False).encode()).hexdigest())
        for path, digest in baseline['files'].items():
            self.assertEqual(digest, hashlib.sha256((ROOT/path).read_bytes()).hexdigest(), path)

    def test_gate_snapshots_match_existing_candidate_and_early_predicates(self):
        rng = random.Random(19)
        st = bot.SymbolState()
        st.candidate_passes = 2
        st.active_radar_id = 1
        config = vars(bot)
        for _ in range(300):
            m = dict(qv24=1e9, chg10=rng.uniform(0, 2), chg30=rng.uniform(0, 2),
                     chg60=rng.uniform(0, 3), flow10=rng.uniform(0, 8), flow30=rng.uniform(0, 12),
                     buy30=rng.uniform(.4, .95), spread=rng.uniform(0, .4), trades10=4, trades30=8,
                     book_imbalance=rng.uniform(.4, .95), extended=False, breakout=True,
                     rel30=.4, chg5=rng.uniform(0, 5))
            score = rng.randint(50, 100)
            gates = bridge.gate_snapshot('candidate_evaluation', m, score, st, config, 1e9)
            self.assertEqual(bot.qualifies(m, score), all(g['passed'] for g in gates))
            gates = bridge.gate_snapshot('early_notify_evaluation', m, score, st, config, 1e9)
            self.assertEqual(bot.early_notify_pass(m, score, st), all(g['passed'] for g in gates))
            gates = bridge.gate_snapshot('early_watch_evaluation', m, score, st, config, 1e9)
            self.assertEqual(bot.early_watch_pass(m, score), all(g['passed'] for g in gates))
            gates = bridge.gate_snapshot('continuity_evaluation', m, score, st, config, 1e9)
            self.assertEqual(bot.continuity_pass(m, score), all(g['passed'] for g in gates))

    def test_flags_do_not_change_premium_or_create_orders(self):
        st = bot.SymbolState()
        m = dict(price=100, breakout=True, buy30=.7, book_imbalance=.6, chg30=.7,
                 chg60=1, flow30=7, extended=False)
        before = bot.premium_trade_guard(m, 90, 90, 90, st)
        active = dict(bot.autotrade_active)
        cfg = dict(bot.autotrade_cfg)
        for values in ({}, dict(chg30=.4, rel30=.3, flow30=2, max_dd10=-.5, gainers_rank=1)):
            m.update(ec.flags(values, 2))
            self.assertEqual(before, bot.premium_trade_guard(m, 90, 90, 90, st))
        self.assertEqual(active, bot.autotrade_active)
        self.assertEqual(cfg, bot.autotrade_cfg)
        self.assertFalse(bot.AUTO_TRADE_LIVE_ALLOWED)
        for name in ('early_continuation.py', 'early_continuation_bridge.py'):
            source = (ROOT/'binance_momentum_bot'/name).read_text()
            for forbidden in ('import aiohttp', 'import requests', 'binance_signed_request', 'autotrade_handle_premium', 'telegram_send'):
                self.assertNotIn(forbidden, source)

    def test_queue_is_bounded_nonblocking_sampled_and_no_inline_db(self):
        w = ec.Collector('never-open.db', {}, 'test', 'hash', capacity=1)
        with patch.object(ec.sqlite3, 'connect', side_effect=AssertionError('inline DB')):
            w.event('X', 'candidate_evaluation', {}, decision_ts=100_000)
            w.event('X', 'candidate_evaluation', {}, decision_ts=100_001)
            self.assertEqual(0, w.lost)
            w.event('X', 'candidate_start', {}, decision_ts=100_002)
            self.assertEqual(1, w.lost)
        self.assertEqual(1, w.queue.qsize())

    def test_worker_uses_existing_db_only_and_can_persist(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory)/'test.db')
            c = sqlite3.connect(path)
            c.execute('CREATE TABLE legacy(value TEXT)')
            c.execute("INSERT INTO legacy VALUES ('keep')")
            c.commit(); c.close()
            w = ec.Collector(path, {}, 'test', 'code')
            w.event('X', 'candidate_start', {}, price=100)
            w.stop.set()
            w.run()
            c = sqlite3.connect(path)
            self.assertEqual(('keep',), c.execute('SELECT * FROM legacy').fetchone())
            self.assertEqual(1, c.execute('SELECT COUNT(*) FROM early_continuation_flags').fetchone()[0])
            c.close()
            missing = str(Path(directory)/'missing'/'signals.db')
            with self.assertLogs('early_continuation', level='ERROR'):
                ec.Collector(missing, {}, 'test', 'code').run()
            self.assertFalse(Path(missing).parent.exists())


if __name__ == '__main__':
    unittest.main()
