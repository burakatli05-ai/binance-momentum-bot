import sqlite3
import tempfile
import unittest
from pathlib import Path

import early_checkpoint_shadow as h1


class H1CheckpointShadowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / "h1.db")
        h1.reset_runtime_state()
        self.addCleanup(h1.reset_runtime_state)
        c = self.connect()
        try:
            h1.migrate(c)
            c.commit()
        finally:
            c.close()

    def connect(self):
        return sqlite3.connect(self.path)

    def row(self, episode_id):
        c = self.connect()
        try:
            c.row_factory = sqlite3.Row
            row = c.execute(
                "SELECT * FROM early_checkpoint_latency_shadow_v1 WHERE episode_id=?",
                (episode_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            c.close()

    def test_migration_idempotent(self):
        c = self.connect()
        try:
            h1.migrate(c)
            h1.migrate(c)
            c.commit()
            tables = {
                r[0]
                for r in c.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        finally:
            c.close()
        self.assertIn("early_checkpoint_latency_shadow_v1", tables)

    def test_ready_then_public_early_computes_checkpoint_tax(self):
        h1.observe(
            self.connect,
            episode_id=10,
            symbol="AAAUSDT",
            candidate_start_ts_ms=1_000,
            observed_ts_ms=16_000,
            price=100.0,
            confirm_passes=2,
            ready=False,
            failed_gates=["FLOW30"],
        )
        h1.observe(
            self.connect,
            episode_id=10,
            symbol="AAAUSDT",
            candidate_start_ts_ms=1_000,
            observed_ts_ms=20_000,
            price=101.0,
            confirm_passes=2,
            ready=True,
            failed_gates=[],
        )
        h1.public_early(
            self.connect,
            episode_id=10,
            symbol="AAAUSDT",
            candidate_start_ts_ms=1_000,
            actual_ts_ms=31_000,
            actual_price=102.0,
            confirm_passes=3,
        )
        row = self.row(10)
        self.assertEqual("PUBLIC_EARLY", row["status"])
        self.assertEqual(20_000, row["first_ready_ts_ms"])
        self.assertEqual(["FLOW30"], __import__("json").loads(row["prior_failed_gates_json"]))
        self.assertAlmostEqual(11.0, row["seconds_saved"])
        self.assertAlmostEqual((102.0 / 101.0 - 1.0) * 100.0, row["price_saved_pct"])

    def test_ready_without_public_is_preserved(self):
        h1.observe(
            self.connect,
            episode_id=11,
            symbol="BBBUSDT",
            candidate_start_ts_ms=1_000,
            observed_ts_ms=16_000,
            price=100.0,
            confirm_passes=2,
            ready=True,
            failed_gates=[],
            would_create_radar=True,
        )
        h1.end(self.connect, episode_id=11, end_ts_ms=25_000, reason="BREAKDOWN_REJECT")
        row = self.row(11)
        self.assertEqual("READY_NO_PUBLIC", row["status"])
        self.assertEqual("BREAKDOWN_REJECT", row["end_reason"])

    def test_public_on_same_checkpoint_is_zero_latency(self):
        h1.public_early(
            self.connect,
            episode_id=12,
            symbol="CCCUSDT",
            candidate_start_ts_ms=1_000,
            actual_ts_ms=16_000,
            actual_price=100.0,
            confirm_passes=2,
        )
        row = self.row(12)
        self.assertEqual("PUBLIC_EARLY", row["status"])
        self.assertEqual(0.0, row["seconds_saved"])
        self.assertEqual(0.0, row["price_saved_pct"])


if __name__ == "__main__":
    unittest.main()
