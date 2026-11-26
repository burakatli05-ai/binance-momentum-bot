import unittest
from unittest.mock import patch
import binance_rest_guard as guard


class GuardTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.mono = 100.0
        self.wall = 1795693039.0
        self.state = {"used_weight_1m": 0, "observed_mono": 0.0, "blocked_until_mono": 0.0}
        self.sleeps = []
        self.addCleanup(patch.stopall)
        patch.object(guard.time, "monotonic", side_effect=lambda: self.mono).start()
        patch.object(guard.time, "time", side_effect=lambda: self.wall).start()

    async def run_gate(self, **kwargs):
        return await guard.gate(self.state, soft_weight=1800, soft_cooldown=10, **kwargs)

    def test_actual_418_timestamp(self):
        wait = guard.observe(self.state, {}, 418, "IP banned until 1795693439889.")
        self.assertAlmostEqual(wait, 400.889, places=3)

    def test_418_header_and_body_take_longest(self):
        self.assertEqual(guard.observe(self.state, {"Retry-After": "600"}, 418, "IP banned until 1795693439889"), 600)

    def test_429_header(self):
        self.assertEqual(guard.observe(self.state, {"Retry-After": "25", "X-MBX-USED-WEIGHT-1M": "2401"}, 429), 25)
        self.assertEqual(self.state["used_weight_1m"], 2401)

    def test_418_missing_or_invalid_header(self):
        self.assertEqual(guard.observe(self.state, {"Retry-After": "nan"}, 418), 120)

    def test_429_fallback(self):
        self.assertEqual(guard.observe(self.state, {}, 429), 60)

    def test_never_shorten_existing_deadline(self):
        guard.observe(self.state, {"Retry-After": "600"}, 418)
        self.mono += 10
        self.assertEqual(guard.observe(self.state, {"Retry-After": "15"}, 429), 590)

    async def test_new_entry_rejected_immediately_even_priority(self):
        guard.observe(self.state, {}, 418)
        with patch.object(guard.asyncio, "sleep") as sleep:
            with self.assertRaises(guard.RateLimitBlocked):
                await self.run_gate(priority=True, reject_if_blocked=True)
            sleep.assert_not_called()

    async def test_hard_gate_rechecks_extension(self):
        guard.observe(self.state, {"Retry-After": "10"}, 418)
        async def sleep(seconds):
            self.sleeps.append(seconds)
            self.mono += seconds
            if len(self.sleeps) == 1:
                guard.observe(self.state, {"Retry-After": "20"}, 418)
        with patch.object(guard.asyncio, "sleep", side_effect=sleep):
            await self.run_gate(priority=True)
        self.assertEqual(self.sleeps, [10, 20])

    async def test_background_soft_limit_rechecked(self):
        guard.observe(self.state, {"X-MBX-USED-WEIGHT-1M": "1900"})
        async def sleep(seconds):
            self.sleeps.append(seconds)
            self.mono += seconds
            if len(self.sleeps) == 2:
                guard.observe(self.state, {"X-MBX-USED-WEIGHT-1M": "100"})
        with patch.object(guard.asyncio, "sleep", side_effect=sleep):
            await self.run_gate()
        self.assertEqual(self.sleeps, [10, 10])

    async def test_soft_budget_reserved_for_management_not_new_entry(self):
        guard.observe(self.state, {"X-MBX-USED-WEIGHT-1M": "1900"})
        await self.run_gate(priority=True)
        with self.assertRaises(guard.RateLimitBlocked):
            await self.run_gate(priority=True, reject_if_blocked=True)

    async def test_stale_soft_observation_cleared(self):
        guard.observe(self.state, {"X-MBX-USED-WEIGHT-1M": "1900"})
        self.mono += 71
        await self.run_gate(reject_if_blocked=True)
        self.assertEqual(self.state["used_weight_1m"], 0)


if __name__ == "__main__":
    unittest.main()
