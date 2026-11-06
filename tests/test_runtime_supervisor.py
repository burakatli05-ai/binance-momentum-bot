"""Runtime supervisor regressions; never launch the bot or touch production data."""

import asyncio
import logging
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "binance_momentum_bot"))
from runtime_supervisor import RuntimeSupervisor


class DummyLog(logging.Logger):
    def __init__(self):
        super().__init__("runtime-supervisor-test")


def make_runtime(path):
    stop = asyncio.Event()

    async def main():
        return None

    def save(*args):
        raise AssertionError("original writer should be replaced")

    return {
        "log": DummyLog(),
        "DB_PATH": str(path),
        "stop_event": stop,
        "main": main,
        "save_radar_outcome": save,
        "request_stop": lambda *_: stop.set(),
    }


class RadarWriterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "signals.db"
        conn = sqlite3.connect(self.db)
        conn.execute(
            "CREATE TABLE radar_outcomes("
            "radar_id INTEGER,horizon_s INTEGER,return_pct REAL,"
            "mfe_pct REAL,mae_pct REAL,ts INTEGER,"
            "PRIMARY KEY(radar_id,horizon_s))"
        )
        conn.commit()
        conn.close()
        self.runtime = make_runtime(self.db)
        self.supervisor = RuntimeSupervisor(self.runtime, hard_exit_seconds=60)

    async def asyncTearDown(self):
        self.supervisor.disarm_hard_exit()
        self.tmp.cleanup()

    async def test_real_sqlite_lock_is_deferred_then_flushed(self):
        blocker = sqlite3.connect(self.db, timeout=0)
        blocker.execute("BEGIN EXCLUSIVE")
        blocker.execute("UPDATE radar_outcomes SET ts=ts")
        try:
            started = asyncio.get_running_loop().time()
            self.assertFalse(
                self.supervisor.save_radar_outcome(7, 300, 1.2, 2.4, -0.7)
            )
            elapsed = asyncio.get_running_loop().time() - started
            self.assertLess(elapsed, 0.25)
            self.assertEqual(self.supervisor.pending_count(), 1)
        finally:
            blocker.rollback()
            blocker.close()

        task = asyncio.create_task(self.supervisor._radar_retry_loop())
        for _ in range(40):
            if self.supervisor.pending_count() == 0:
                break
            await asyncio.sleep(0.05)
        self.runtime["stop_event"].set()
        await task
        conn = sqlite3.connect(self.db)
        row = conn.execute(
            "SELECT radar_id,horizon_s,return_pct,mfe_pct,mae_pct "
            "FROM radar_outcomes WHERE radar_id=7"
        ).fetchone()
        conn.close()
        self.assertEqual(row, (7, 300, 1.2, 2.4, -0.7))

    async def test_lock_is_deferred_and_flushed(self):
        original = self.supervisor._write_radar_rows
        calls = {"count": 0}

        def flaky(rows, *, timeout_ms):
            calls["count"] += 1
            if calls["count"] == 1:
                raise sqlite3.OperationalError("database is locked")
            return original(rows, timeout_ms=timeout_ms)

        self.supervisor._write_radar_rows = flaky
        self.assertFalse(
            self.supervisor.save_radar_outcome(1, 60, 1.0, 2.0, -0.5)
        )
        self.assertEqual(self.supervisor.pending_count(), 1)
        task = asyncio.create_task(self.supervisor._radar_retry_loop())
        for _ in range(20):
            if self.supervisor.pending_count() == 0:
                break
            await asyncio.sleep(0.05)
        self.runtime["stop_event"].set()
        await task
        self.assertEqual(self.supervisor.pending_count(), 0)
        conn = sqlite3.connect(self.db)
        row = conn.execute(
            "SELECT radar_id,horizon_s,return_pct,mfe_pct,mae_pct "
            "FROM radar_outcomes"
        ).fetchone()
        conn.close()
        self.assertEqual(row, (1, 60, 1.0, 2.0, -0.5))

    async def test_non_lock_error_is_not_hidden(self):
        def broken(rows, *, timeout_ms):
            raise sqlite3.OperationalError("no such table")

        self.supervisor._write_radar_rows = broken
        with self.assertRaisesRegex(sqlite3.OperationalError, "no such table"):
            self.supervisor.save_radar_outcome(1, 60, 1.0, 2.0, -0.5)

    async def test_following_rows_do_not_repeat_sync_lock_wait(self):
        calls = {"count": 0}

        def locked(rows, *, timeout_ms):
            calls["count"] += 1
            raise sqlite3.OperationalError("database is locked")

        self.supervisor._write_radar_rows = locked
        self.assertFalse(
            self.supervisor.save_radar_outcome(1, 60, 1.0, 2.0, -0.5)
        )
        self.assertFalse(
            self.supervisor.save_radar_outcome(2, 60, 1.1, 2.1, -0.4)
        )
        self.assertEqual(calls["count"], 1)
        self.assertEqual(self.supervisor.pending_count(), 2)

    async def test_requeue_keeps_newer_same_key_value(self):
        old = (1, 60, 1.0, 2.0, -0.5, 100)
        newer = (1, 60, 1.5, 2.5, -0.2, 200)
        self.supervisor._defer_row(newer)
        self.supervisor._requeue([old])
        rows = self.supervisor._take_pending()
        self.assertEqual(rows, [newer])

    async def test_retry_non_lock_error_is_not_hidden(self):
        self.supervisor._defer_row((1, 60, 1.0, 2.0, -0.5, 123))

        def broken(rows, *, timeout_ms):
            raise sqlite3.OperationalError("no such table")

        self.supervisor._write_radar_rows = broken
        with self.assertRaisesRegex(sqlite3.OperationalError, "no such table"):
            await self.supervisor._radar_retry_loop()
        self.assertEqual(self.supervisor.pending_count(), 1)


class WrapperWiringTests(unittest.TestCase):
    def test_wrapper_loads_without_triggering_bot_main_guard(self):
        source = (
            ROOT / "binance_momentum_bot" / "bot_shared.py"
        ).read_text(encoding="utf-8")
        self.assertIn('run_name="momentum_runtime"', source)
        self.assertNotIn('run_name="__main__"', source)
        self.assertIn("run_runtime(runtime)", source)
        self.assertIn(
            "early_v2_adapter.Integration = PublicH1PremiumIntegration",
            source,
        )


class SupervisionTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_task_cancels_peer_and_reraises(self):
        cancelled = asyncio.Event()
        runtime = make_runtime(":memory:")
        supervisor = RuntimeSupervisor(runtime, hard_exit_seconds=60)

        async def failing():
            await asyncio.sleep(0)
            raise RuntimeError("boom")

        async def peer():
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        with self.assertRaisesRegex(RuntimeError, "boom"):
            await supervisor._supervise((failing(), peer()))
        self.assertTrue(cancelled.is_set())
        self.assertTrue(runtime["stop_event"].is_set())
        supervisor.disarm_hard_exit()

    async def test_allowed_one_shot_completion_does_not_fail(self):
        runtime = make_runtime(":memory:")
        supervisor = RuntimeSupervisor(runtime, hard_exit_seconds=60)

        async def early_exit_export_once():
            return "done"

        async def stopper():
            await asyncio.sleep(0.02)
            runtime["stop_event"].set()
            return "stopped"

        result = await supervisor._supervise(
            (early_exit_export_once(), stopper())
        )
        self.assertEqual(result, ["done", "stopped"])
        supervisor.disarm_hard_exit()

    async def test_graceful_cancel_after_stop_does_not_raise(self):
        runtime = make_runtime(":memory:")
        supervisor = RuntimeSupervisor(runtime, hard_exit_seconds=60)

        async def stopped_task():
            runtime["stop_event"].set()
            raise asyncio.CancelledError

        result = await supervisor._supervise((stopped_task(),))
        self.assertEqual(result, [None])
        supervisor.disarm_hard_exit()

    async def test_patch_only_intercepts_runtime_main_gather(self):
        runtime = make_runtime(":memory:")

        async def main():
            return await asyncio.gather(asyncio.sleep(0, result="runtime"))

        runtime["main"] = main
        supervisor = RuntimeSupervisor(runtime, hard_exit_seconds=60)
        calls = {"supervise": 0}

        async def counted(aws):
            calls["supervise"] += 1
            return await supervisor._original_gather(*aws)

        supervisor._supervise = counted
        supervisor.install()
        try:
            outside = await asyncio.gather(
                asyncio.sleep(0, result="outside")
            )
            self.assertEqual(outside, ["outside"])
            self.assertEqual(calls["supervise"], 0)
            inside = await runtime["main"]()
            self.assertEqual(inside, ["runtime"])
            self.assertEqual(calls["supervise"], 1)
        finally:
            supervisor.restore()
            supervisor.disarm_hard_exit()


if __name__ == "__main__":
    unittest.main()
