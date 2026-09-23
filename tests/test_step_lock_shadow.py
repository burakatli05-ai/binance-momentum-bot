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


if __name__ == "__main__":
    unittest.main()
