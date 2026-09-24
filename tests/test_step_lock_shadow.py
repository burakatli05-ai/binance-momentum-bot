import sqlite3
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "binance_momentum_bot"))
from step_lock_shadow import StepLockShadow, STEP_LEVELS, VERSION


class StepLockShadowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "shadow.db"
        self.connect = lambda: sqlite3.connect(self.path)
        self.s = StepLockShadow(self.connect)
        self.s.arm("1", "TESTUSDT", 100.0, 1000, 7)

    def tearDown(self):
        self.tmp.cleanup()

    def tick(self, price, n, event_ms=None, received_ms=None):
        event_ms = event_ms if event_ms is not None else 1000 + n * 100
        received_ms = received_ms if received_ms is not None else event_ms
        self.s.tick("TESTUSDT", price, event_ms, received_ms, n)

    def test_policy_has_requested_levels_and_no_partial_exit(self):
        self.assertEqual(STEP_LEVELS[:3], (0.20, 0.50, 0.75))
        self.assertEqual(STEP_LEVELS[-1], 4.75)
        self.tick(100.21, 1)
        row = self.s.get("1")
        self.assertEqual(row["status"], "OPEN")
        self.assertAlmostEqual(row["current_lock_pct"], 0.20)
        self.tick(100.19, 2)
        row = self.s.get("1")
        self.assertEqual(row["close_reason"], "STEP_LOCK")
        self.assertAlmostEqual(row["exit_level_pct"], 0.20)
        with self.connect() as db:
            closes = db.execute(
                "SELECT COUNT(*) FROM early_step_lock_events_v1 WHERE signal_id='1' AND event='CLOSE'"
            ).fetchone()[0]
        self.assertEqual(closes, 1)

    def test_lock_only_moves_up_and_full_position_closes_on_last_lock(self):
        for n, price in enumerate((100.21, 100.51, 100.76, 101.01), 1):
            self.tick(price, n)
        self.assertAlmostEqual(self.s.get("1")["current_lock_pct"], 1.00)
        self.tick(100.99, 5)
        row = self.s.get("1")
        self.assertEqual(row["status"], "CLOSED")
        self.assertEqual(row["close_reason"], "STEP_LOCK")
        self.assertAlmostEqual(row["exit_level_pct"], 1.00)
        self.assertLess(row["observed_exit_pct"], 1.00)

    def test_final_target_is_five_percent_and_cost_is_applied_once(self):
        self.tick(105.10, 1)
        row = self.s.get("1")
        self.assertEqual(row["close_reason"], "FINAL_TP")
        self.assertAlmostEqual(row["exit_level_pct"], 5.0)
        self.assertAlmostEqual(row["gross_pct"], 5.0)
        self.assertAlmostEqual(row["net_pct"], 4.86)

    def test_initial_stop_is_minus_two_percent(self):
        self.tick(97.95, 1)
        row = self.s.get("1")
        self.assertEqual(row["close_reason"], "INITIAL_SL")
        self.assertAlmostEqual(row["exit_level_pct"], -2.0)
        self.assertLess(row["net_pct"], -2.0)

    def test_restart_recovers_open_position(self):
        self.tick(100.51, 1)
        other = StepLockShadow(self.connect)
        self.assertIn("1", other.active)
        self.assertAlmostEqual(other.get("1")["current_lock_pct"], 0.50)
        other.tick("TESTUSDT", 100.49, 1200, 1200, 2)
        self.assertEqual(other.get("1")["close_reason"], "STEP_LOCK")

    def test_trade_gap_is_flagged_for_analysis(self):
        self.tick(100.21, 1)
        self.s.tick("TESTUSDT", 100.51, 4000, 4000, 5)
        row = self.s.get("1")
        self.assertIn("MISSING_AGG_TRADE_IDS", row["data_flags"])
        self.assertIn("TRADE_OBSERVATION_GAP", row["data_flags"])
        self.assertEqual(row["version"], VERSION)

    def test_stale_tick_does_not_move_lock(self):
        self.s.tick("TESTUSDT", 101.0, 1100, 4000, 1)
        row = self.s.get("1")
        self.assertIsNone(row["current_lock_pct"])
        self.assertIn("STALE_OR_FUTURE_TRADE", row["data_flags"])


    def test_summary_is_read_only_and_reports_levels_and_usdt(self):
        # signal 1 closes at +0.20 lock
        self.tick(100.21, 1)
        self.tick(100.19, 2)
        # signal 2 reaches +5 final target
        self.s.arm("2", "MOONUSDT", 100.0, 2000, 8)
        self.s.tick("MOONUSDT", 105.10, 2100, 2100, 10)
        # signal 3 stays open with +0.50 lock
        self.s.arm("3", "OPENUSDT", 100.0, 3000, 9)
        self.s.tick("OPENUSDT", 100.51, 3100, 3100, 20)
        before = self.s.get("3")
        report = self.s.summary(notional_usdt=2000.0, recent_limit=10)
        after = self.s.get("3")
        self.assertEqual(before, after)
        self.assertEqual(report["total"], 3)
        self.assertEqual(report["closed"], 2)
        self.assertEqual(report["open"], 1)
        self.assertEqual(report["close_reason_counts"]["STEP_LOCK"], 1)
        self.assertEqual(report["close_reason_counts"]["FINAL_TP"], 1)
        self.assertEqual(report["exit_level_counts"][0.2], 1)
        self.assertEqual(report["exit_level_counts"][5.0], 1)
        self.assertEqual(report["open_lock_counts"][0.5], 1)
        self.assertEqual(report["reached_level_counts"][0.2], 3)
        self.assertEqual(report["reached_level_counts"][0.5], 2)
        self.assertEqual(report["reached_level_counts"][5.0], 1)
        self.assertAlmostEqual(report["closed_net_usdt"], (0.05 + 4.86) * 20.0, places=6)
        self.assertEqual(len(report["recent"]), 3)

    def test_summary_validates_notional_and_limit(self):
        with self.assertRaises(ValueError):
            self.s.summary(notional_usdt=0)
        with self.assertRaises(ValueError):
            self.s.summary(recent_limit=51)


    def test_runner_review_falls_back_to_nearest_stage_when_episode_missing(self):
        with self.connect() as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS entry_stage_forward_shadow(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL,
                    episode_id INTEGER, stage TEXT NOT NULL, created_ts_ms INTEGER NOT NULL,
                    entry_price REAL NOT NULL, mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0,
                    close60_price REAL, completed_60m INTEGER DEFAULT 0
                )"""
            )
            db.execute(
                """INSERT INTO entry_stage_forward_shadow(
                    symbol,episode_id,stage,created_ts_ms,entry_price,mfe_pct,mae_pct,close60_price,completed_60m
                ) VALUES ('TESTUSDT',99,'EARLY',1002,100.0,0.80,-0.30,100.4,1)"""
            )
            db.commit()
        # Re-arm fixture row without episode id to mirror historical Step Lock rows.
        with self.connect() as db:
            db.execute("UPDATE early_step_lock_shadow_v1 SET episode_id=NULL WHERE signal_id='1'")
            db.commit()
        self.tick(100.21,1)
        self.tick(100.19,2)
        review=self.s.runner_review(exit_level_pct=.2)
        self.assertEqual(review["matched_stage"],1)
        self.assertEqual(review["mature_60m"],1)
        self.assertEqual(review["reached_after_exit_proxy"][.5],1)
        self.assertEqual(review["reached_after_exit_proxy"][.75],1)

    def test_runner_review_matches_early_stage_and_counts_later_thresholds(self):
        # Build the existing production-stage table shape minimally for this test.
        with self.connect() as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS entry_stage_forward_shadow(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL,
                    episode_id INTEGER, stage TEXT NOT NULL, created_ts_ms INTEGER NOT NULL,
                    entry_price REAL NOT NULL, mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0,
                    close60_price REAL, completed_60m INTEGER DEFAULT 0
                )"""
            )
            db.execute(
                """INSERT INTO entry_stage_forward_shadow(
                    symbol,episode_id,stage,created_ts_ms,entry_price,mfe_pct,mae_pct,close60_price,completed_60m
                ) VALUES ('TESTUSDT',7,'EARLY',1000,100.0,1.10,-0.40,100.8,1)"""
            )
            db.commit()
        self.tick(100.21,1)
        self.tick(100.19,2)
        review=self.s.runner_review(exit_level_pct=.2)
        self.assertEqual(review["total"],1)
        self.assertEqual(review["matched_stage"],1)
        self.assertEqual(review["mature_60m"],1)
        self.assertEqual(review["reached_after_exit_proxy"][.5],1)
        self.assertEqual(review["reached_after_exit_proxy"][1.0],1)
        self.assertEqual(review["reached_after_exit_proxy"][1.25],0)
        self.assertEqual(review["items"][0]["symbol"],"TESTUSDT")

    def test_initial_sl_recovery_uses_public_early_stage_and_exact_poststop_order(self):
        with self.connect() as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS entry_stage_forward_shadow(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL,
                    episode_id INTEGER, stage TEXT NOT NULL, created_ts_ms INTEGER NOT NULL,
                    entry_price REAL NOT NULL, mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0,
                    close60_price REAL, completed_60m INTEGER DEFAULT 0
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS radar_signals(
                    id INTEGER PRIMARY KEY, ts INTEGER, notify_ts INTEGER, price REAL
                )"""
            )
            db.execute(
                """INSERT INTO entry_stage_forward_shadow(
                    symbol,episode_id,stage,created_ts_ms,entry_price,mfe_pct,mae_pct,close60_price,completed_60m
                ) VALUES ('TESTUSDT',7,'EARLY',1000,100.0,0.40,-2.50,100.2,1)"""
            )
            db.execute("INSERT INTO radar_signals(id,ts,notify_ts,price) VALUES (1,0,1,99.0)")
            db.commit()
        self.tick(97.90,1)
        self.assertEqual(self.s.get('1')['close_reason'],'INITIAL_SL')
        # After the -2 stop, price first worsens to -2.5, then recovers through +0.20.
        self.s.tick('TESTUSDT',97.50,1200,1200,2)
        self.s.tick('TESTUSDT',100.25,1300,1300,3)
        review=self.s.initial_sl_recovery_review()
        self.assertEqual(review['total'],1)
        item=review['items'][0]
        self.assertEqual(item['historical_minus3_assessment'],'WOULD_SURVIVE_MINUS3_AND_REACH_020')
        self.assertAlmostEqual(item['public_early_mfe_60m'],0.40)
        self.assertAlmostEqual(item['public_early_mae_60m'],-2.50)
        self.assertEqual(item['exact_poststop']['minus3_would_save_to_020'],'YES')
        self.assertAlmostEqual(item['exact_poststop']['trough_before_020_pct'],-2.50)

    def test_poststop_minus3_before_recovery_is_no(self):
        with self.connect() as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS entry_stage_forward_shadow(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL,
                    episode_id INTEGER, stage TEXT NOT NULL, created_ts_ms INTEGER NOT NULL,
                    entry_price REAL NOT NULL, mfe_pct REAL DEFAULT 0, mae_pct REAL DEFAULT 0,
                    close60_price REAL, completed_60m INTEGER DEFAULT 0
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS radar_signals(
                    id INTEGER PRIMARY KEY, ts INTEGER, notify_ts INTEGER, price REAL
                )"""
            )
            db.execute("INSERT INTO entry_stage_forward_shadow(symbol,episode_id,stage,created_ts_ms,entry_price,mfe_pct,mae_pct,completed_60m) VALUES ('TESTUSDT',7,'EARLY',1000,100.0,0.5,-3.5,1)")
            db.execute("INSERT INTO radar_signals(id,ts,notify_ts,price) VALUES (1,0,1,99.0)")
            db.commit()
        self.tick(97.90,1)
        self.s.tick('TESTUSDT',96.90,1200,1200,2)
        self.s.tick('TESTUSDT',100.25,1300,1300,3)
        item=self.s.initial_sl_recovery_review()['items'][0]
        self.assertEqual(item['historical_minus3_assessment'],'ORDER_UNKNOWN_BOTH_MINUS3_AND_020_OCCUR')
        self.assertEqual(item['exact_poststop']['minus3_would_save_to_020'],'NO')


    def test_historical_profit_review_microcut_bounds_and_counterfactual(self):
        with self.connect() as db:
            db.execute(
                """CREATE TABLE entry_stage_forward_shadow(
                    id INTEGER PRIMARY KEY, symbol TEXT, stage TEXT, created_ts_ms INTEGER,
                    entry_price REAL, mfe_pct REAL, mae_pct REAL, close60_price REAL,
                    current_outcome TEXT, fee_adjusted_current_pct REAL, completed_60m INTEGER
                )"""
            )
            db.executemany(
                """INSERT INTO entry_stage_forward_shadow
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    (1,'TESTUSDT','EARLY',1000,100.0,0.80,-0.40,100.30,'TP',0.5,1),
                    (2,'BADUSDT','EARLY',2000,100.0,0.10,-2.50,98.00,'STOP',-2.1,1),
                    (3,'STRONGUSDT','EARLY',3000,100.0,1.20,-0.10,101.00,'TP',1.0,1),
                ],
            )
            db.execute(
                """CREATE TABLE quality_shadow_cohorts(
                    key TEXT PRIMARY KEY,symbol TEXT,kind TEXT,episode_id INTEGER,
                    signal_id INTEGER,radar_id INTEGER,anchor_price REAL,decision_ms INTEGER,
                    wave_key TEXT,recovery_gap INTEGER,payload TEXT
                )"""
            )
            db.execute(
                """CREATE TABLE quality_shadow_prices(
                    key TEXT,bucket_ms INTEGER,width_ms INTEGER,payload TEXT,
                    PRIMARY KEY(key,bucket_ms,width_ms)
                )"""
            )
            db.execute(
                """CREATE TABLE quality_shadow_gaps(
                    key TEXT,start_ms INTEGER,end_ms INTEGER,reason TEXT,
                    PRIMARY KEY(key,start_ms,end_ms,reason)
                )"""
            )
            db.execute(
                """INSERT INTO quality_shadow_cohorts
                   VALUES ('EARLY:1','TESTUSDT','EARLY',7,NULL,1,100.0,1000,'w',0,'{}')"""
            )
            db.executemany(
                """INSERT INTO quality_shadow_prices VALUES (?,?,?,?)""",
                [
                    ('EARLY:1',1000,1000,'{"high":100.05,"low":99.75}'),
                    ('EARLY:1',2000,1000,'{"high":100.60,"low":99.90}'),
                ],
            )
            db.commit()
        # Actual Step Lock baseline closes profitably after +0.20.
        self.tick(100.21,1)
        self.tick(100.19,2)
        review=self.s.historical_profit_review(notional_usdt=2000.0)
        self.assertEqual(review['full_history']['completed_60m'],3)
        h=review['full_history']['microcut_bounds']['0.2']
        self.assertEqual(h['down_no_up'],1)
        self.assertEqual(h['down_no_up_hits_minus2'],1)
        self.assertEqual(h['up_no_down'],1)
        self.assertEqual(h['both'],1)
        q=review['quality_exactish']['microcut_first_touch']['0.2']
        print("DEBUG_HYBRID_Q", q)
        self.assertEqual(q['down_first'],1)
        self.assertEqual(q['down_first_later']['0.2'],1)
        self.assertEqual(q['down_first_later']['0.5'],1)
        cf=review['current_step_lock_counterfactual']['0.2']
        self.assertEqual(cf['matched_unambiguous'],1)
        self.assertEqual(cf['cut_count'],1)
        self.assertEqual(cf['baseline_positive_that_would_be_cut'],1)
        self.assertLess(cf['delta_usdt'],0)
        hybrid=review['quality_exactish']['hybrid_step_ladder_60m']['hybrid_minus020']
        self.assertEqual(hybrid['low_high']['cohort'],1)
        self.assertEqual(hybrid['directional']['cohort'],1)
        self.assertEqual(hybrid['high_low']['cohort'],1)
        # The first bucket hits -0.20 before +0.20 under low_high/directional,
        # while high_low reaches +0.20 first and then applies the ladder.
        self.assertEqual(hybrid['low_high']['close_reasons']['MICRO_CUT'],1)
        self.assertIn('STEP_LOCK', hybrid['high_low']['close_reasons'])



if __name__ == "__main__":
    unittest.main()
