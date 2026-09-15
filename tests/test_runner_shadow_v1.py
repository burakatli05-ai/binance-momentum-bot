import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

import runner_shadow_v1 as runner
import research_v5135 as research


class FakeNotifier:
    def __init__(self):
        self.messages = []

    def send(self, text):
        self.messages.append(text)
        return True


class RunnerShadowCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "signals.db"
        with self.connect() as c:
            c.execute("CREATE TABLE candidate_events(id INTEGER PRIMARY KEY,ts INTEGER,symbol TEXT,event TEXT,price REAL,score INTEGER,chg30 REAL,chg60 REAL,chg5 REAL,flow30 REAL,buy30 REAL,book_imbalance REAL,rel30 REAL,breakout INTEGER,candidate_age_s REAL,confirm_passes INTEGER,gainer_rank INTEGER,qv24 REAL,note TEXT)")
            runner.migrate(c)
        self.notifier = FakeNotifier()
        self.engine = runner.RunnerShadowV1(self.connect, notifier=self.notifier)

    def tearDown(self):
        self.tmp.cleanup()

    def connect(self):
        return sqlite3.connect(self.path)

    @staticmethod
    def cap_features():
        return dict(price=.063890, score=80, momentum_score=80, chg30=.80, chg60=1.57,
                    chg5=1.04, flow30=7.2, buy30=.694, rel30=.78, gainer_rank=3,
                    book_imbalance=.62, phase="LOW", oi5=.12, breakout=False)

    @staticmethod
    def cys_features():
        return dict(price=100, score=79, momentum_score=79, chg30=.76, chg60=1.18,
                    flow30=4.8, buy30=.67, rel30=.64, gainer_rank=8,
                    book_imbalance=.58, phase="MEDIUM", oi5=.18, breakout=False)

    def rows(self, sql, args=()):
        with self.connect() as c:
            c.row_factory = sqlite3.Row
            return c.execute(sql, args).fetchall()

    def test_cap_scores_high_without_over_penalizing_7x_flow(self):
        score, contrib, reasons = runner.score_features(self.cap_features())
        self.assertGreaterEqual(score, 80)
        self.assertIn("GAINERS_TOP10", reasons)
        self.assertIn("BTC_RELATIVE_LEADER", reasons)
        self.assertGreaterEqual(contrib["flow30"], 0)

    def test_mtl_style_exhaustion_does_not_become_fast_runner(self):
        features = dict(price=.39, score=85, momentum_score=85, chg30=.18, chg60=.32,
                        flow30=12.0, buy30=.86, rel30=.08, gainer_rank=35,
                        book_imbalance=.94, candidate_runup=1.48, phase="MEDIUM", oi5=-.45)
        score, _, reasons = runner.score_features(features)
        self.assertLess(score, runner.RUNNER_WATCH_MIN_SCORE)
        self.assertIn("BUY_EXHAUSTION_RISK", reasons)
        t = int(time.time() * 1000)
        self.engine.on_stage("mtl-early", "MTLUSDT", "EARLY", event_ts_ms=t, price=.39, features=features)
        self.assertFalse(self.rows("SELECT * FROM runner_watch_v1_shadow WHERE symbol='MTLUSDT'"))

    def test_cys_style_30s_rescue_allows_after_inconclusive_15s(self):
        t = int(time.time() * 1000)
        self.engine.on_stage("cys-early", "CYSUSDT", "EARLY", event_ts_ms=t, price=100, features=self.cys_features())
        self.engine.on_tick("CYSUSDT", 100.05, t + 15001, t + 15001)
        r = self.rows("SELECT state,t15_json FROM runner_watch_v1_shadow WHERE symbol='CYSUSDT'")[0]
        self.assertEqual("WAIT_30", r["state"])
        self.assertEqual("WAIT_30", json.loads(r["t15_json"])["decision"])
        self.engine.on_tick("CYSUSDT", 100.50, t + 30001, t + 30001)
        r = self.rows("SELECT state,decision,t30_json,notify_state FROM runner_watch_v1_shadow WHERE symbol='CYSUSDT'")[0]
        self.assertEqual("ALLOW", r["state"])
        self.assertEqual("ALLOW", json.loads(r["t30_json"])["decision"])
        self.assertEqual("QUEUED", r["notify_state"])
        self.assertEqual(1, len(self.notifier.messages))
        self.assertIn("FAST RUNNER PREMIUM", self.notifier.messages[0])

    def test_rejection_keeps_wave_memory_and_reacquire_creates_child_watch(self):
        t = int(time.time() * 1000)
        self.engine.on_stage("cap-shake", "CAPUSDT", "EARLY", event_ts_ms=t, price=100, features=self.cap_features())
        self.engine.on_tick("CAPUSDT", 99.4, t + 15001, t + 15001)
        parent = self.rows("SELECT * FROM runner_watch_v1_shadow WHERE symbol='CAPUSDT' AND kind='FAST'")[0]
        self.assertEqual("WATCH_REACQUIRE", parent["state"])
        reacquire = dict(self.cap_features(), price=99.8, score=88, momentum_score=88, rel30=.82, chg30=.92, gainer_rank=4)
        self.engine.on_stage("cap-reacquire", "CAPUSDT", "CANDIDATE", event_ts_ms=t + 60000, price=99.8, features=reacquire)
        child = self.rows("SELECT * FROM runner_watch_v1_shadow WHERE symbol='CAPUSDT' AND kind='REACQUIRE'")[0]
        self.assertEqual(parent["watch_id"], child["parent_watch_id"])
        self.assertEqual("WAIT_15", child["state"])

    def test_duplicate_source_is_idempotent_and_snapshot_is_frozen(self):
        t = int(time.time() * 1000)
        f = self.cap_features()
        first = self.engine.on_stage("same-key", "CAPUSDT", "EARLY", event_ts_ms=t, price=f["price"], features=f)
        f["momentum_score"] = 1
        second = self.engine.on_stage("same-key", "CAPUSDT", "EARLY", event_ts_ms=t, price=.1, features=f)
        self.assertEqual(first, second)
        self.assertEqual(1, len(self.rows("SELECT * FROM runner_score_v1_shadow WHERE source_key='same-key'")))
        self.assertEqual(1, len(self.rows("SELECT * FROM runner_watch_v1_shadow")))
        saved = json.loads(self.rows("SELECT raw_features_json FROM runner_score_v1_shadow WHERE id=?", (first,))[0][0])
        self.assertEqual(80, saved["momentum_score"])

    def test_restart_marks_incomplete_watch_gapped_and_never_retro_allows(self):
        t = int(time.time() * 1000)
        self.engine.on_stage("restart-early", "CAPUSDT", "EARLY", event_ts_ms=t, price=100, features=self.cap_features())
        restarted = runner.RunnerShadowV1(self.connect, notifier=FakeNotifier())
        restarted.on_tick("CAPUSDT", 102, t + 20000, t + 20000)
        row = self.rows("SELECT state,observation_gap,decision FROM runner_watch_v1_shadow WHERE symbol='CAPUSDT'")[0]
        self.assertEqual(1, row["observation_gap"])
        self.assertEqual("WATCH_REACQUIRE", row["state"])
        self.assertIsNone(row["decision"])

    def test_forward_outcome_runner_non_runner_gray_labels(self):
        t = int(time.time() * 1000)
        cases = (("R", 100, 106.2, "RUNNER"), ("N", 100, 101.0, "NON_RUNNER"), ("G", 100, 103.0, "GRAY"))
        for name, entry, last, expected in cases:
            self.engine.on_stage(name, name + "USDT", "CANDIDATE", event_ts_ms=t, price=entry,
                                 features=dict(price=entry, score=70, chg30=.5, chg60=.8, rel30=.3, gainer_rank=20, flow30=2, buy30=.62))
            self.engine.on_tick(name + "USDT", last, t + 3600001, t + 3600001)
            row = self.rows("SELECT outcome_label FROM runner_score_v1_shadow WHERE source_key=?", (name,))[0]
            self.assertEqual(expected, row["outcome_label"])

    def test_migration_is_additive_and_idempotent(self):
        with self.connect() as c:
            c.execute("CREATE TABLE legacy_keep(id INTEGER PRIMARY KEY,value TEXT)")
            c.execute("INSERT INTO legacy_keep(value) VALUES('keep')")
            runner.migrate(c)
            runner.migrate(c)
            self.assertEqual("keep", c.execute("SELECT value FROM legacy_keep").fetchone()[0])
            for table in ("runner_score_v1_shadow", "runner_watch_v1_shadow", "non_runner_veto_v1_shadow", "runner_watch_outcome_v1_shadow"):
                self.assertTrue(c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


class ResearchIntegrationCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "signals.db"
        with self.connect() as c:
            for table in ("autotrade_trades", "premium_liquidity_snapshots", "entry_stage_forward_shadow", "research_events", "premium_liquidity_transition_v3", "position_observer_state"):
                c.execute(f"CREATE TABLE {table}(id INTEGER PRIMARY KEY)")
            c.execute("CREATE TABLE candidate_events(id INTEGER PRIMARY KEY,ts INTEGER,symbol TEXT,event TEXT,price REAL,score INTEGER,chg30 REAL,chg60 REAL,chg5 REAL,flow30 REAL,buy30 REAL,book_imbalance REAL,rel30 REAL,breakout INTEGER,candidate_age_s REAL,confirm_passes INTEGER,gainer_rank INTEGER,qv24 REAL,note TEXT)")
            research.migrate(c)

    def tearDown(self):
        self.tmp.cleanup()

    def connect(self):
        return sqlite3.connect(self.path)

    def test_measurements_arm_and_tick_feed_runner_without_changing_causal_api(self):
        m = research.Measurements(self.connect)
        t = int(time.time() * 1000)
        f = RunnerShadowCase.cap_features()
        cid = m.arm("stage:test", "CAPUSDT", "EARLY", ready_ms=t, nominal_ms=t, episode_id=7, features=f)
        self.assertTrue(cid)
        with self.connect() as c:
            c.row_factory = sqlite3.Row
            rs = c.execute("SELECT * FROM runner_score_v1_shadow WHERE source_key='stage:test'").fetchone()
            self.assertIsNotNone(rs)
            self.assertGreaterEqual(rs["score"], 80)
        event_t = rs["event_ts_ms"]
        m.tick("CAPUSDT", f["price"] * 1.003, event_t + 15001, event_t + 15001)
        with self.connect() as c:
            state = c.execute("SELECT state FROM runner_watch_v1_shadow WHERE score_id=?", (rs["id"],)).fetchone()[0]
            self.assertEqual("ALLOW", state)


if __name__ == "__main__":
    unittest.main()
