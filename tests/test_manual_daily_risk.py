"""Manual extra daily LIVE risk: isolated tests, no exchange calls or deployment."""
import asyncio
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "binance_momentum_bot"))
os.environ["PYTHON_DOTENV_DISABLED"] = "1"

import manual_daily_risk as policy
import bot


class ManualRiskPolicyTests(unittest.TestCase):
    def setUp(self):
        self.row = {"local_date": "2026-10-14", "start_balance": 989.0,
                    "locked": False, "lock_reason": ""}
        self.grant = policy.encode_resume(local_date="2026-10-14",
            start_balance=989.0, daily_pct=5.0, extra_usdt=20.0,
            actor="admin", created_ts=123456)

    def test_hard_ceiling_and_one_day_scoping(self):
        self.assertEqual(policy.max_extra_usdt(989, 5), 49.45)
        self.assertEqual(policy.max_extra_usdt(989, 8), 19.78)
        self.assertEqual(policy.max_extra_usdt(989, 10), 0)
        self.assertEqual(policy.max_extra_usdt(100000, 5), 50)
        self.assertEqual(policy.active_extra(self.grant, self.row, 5, "LIVE"), 20)
        self.assertEqual(policy.active_extra(self.grant, self.row, 5, "DRY"), 0)
        self.assertEqual(policy.active_extra(self.grant,
            {**self.row, "local_date": "2026-10-15"}, 5, "LIVE"), 0)
        self.assertEqual(policy.active_extra(self.grant, self.row, 6, "LIVE"), 0)
        self.assertEqual(policy.active_extra(self.grant,
            {**self.row, "start_balance": 990}, 5, "LIVE"), 0)

    def test_malformed_state_fails_closed(self):
        for raw in ("", "invalid json", "null", "[]", '{"version":1}'):
            self.assertEqual(policy.active_extra(raw, self.row, 5, "LIVE"), 0)
        for key, value in (("extra_usdt", 90), ("extra_usdt", float("inf")),
                           ("scope", "DRY"), ("active", "true"), ("version", 2)):
            d = json.loads(self.grant)
            d[key] = value
            self.assertEqual(policy.active_extra(json.dumps(d), self.row, 5, "LIVE"), 0)

    def test_revocation_blocks_regrant_today(self):
        revised = policy.revoke(self.grant, "2026-10-14")
        self.assertTrue(policy.recorded_today(revised, "2026-10-14"))
        self.assertEqual(policy.active_extra(revised, self.row, 5, "LIVE"), 0)
        self.assertIsNone(policy.revoke(revised, "2026-10-14"))

    def test_only_daily_lock_is_eligible(self):
        row = {**self.row, "locked": True}
        self.assertTrue(policy.permits_daily_lock({**row, "lock_reason": "DAILY_LOSS_5.00PCT"}, 20))
        self.assertFalse(policy.permits_daily_lock({**row, "lock_reason": "OTHER_RISK"}, 20))
        self.assertFalse(policy.permits_daily_lock({**row, "lock_reason": "DAILY_LOSS_5.00PCT"}, 0))

    def test_invalid_amount_fails_closed(self):
        for value in (0, -1, 80, 20.001, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                policy.encode_resume(local_date="2026-10-14", start_balance=989,
                    daily_pct=5, extra_usdt=value, actor="admin", created_ts=123456)


class ManualRiskBotIntegrationTests(unittest.TestCase):
    """Minimal isolated SQLite fixture exercises the real runtime risk gate."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = str(Path(self.temp.name) / "isolated_signals.db")
        self.patchers = [
            patch.object(bot, "DB_PATH", self.db),
            patch.object(bot, "TELEGRAM_ADMIN_CHAT_ID", "admin_chat"),
            patch.object(bot, "TELEGRAM_ADMIN_USER_ID", "owner"),
            patch.dict(bot.autotrade_cfg, {"mode": "LIVE", "daily_max_loss_pct": 5.0,
                       "max_open_positions": 3, "max_consecutive_stops": 4,
                       "stop_cooldown_minutes": 60}),
            patch.object(bot, "_at_local_date", return_value="2026-10-14"),
            patch.object(bot, "_at_repair_daily_from_trade_history"),
            patch.object(bot, "_at_open_count", return_value=0),
            patch.object(bot, "_at_open_worst_case_risk", return_value=0),
        ]
        for p in self.patchers:
            p.start()
            self.addCleanup(p.stop)
        bot._autotrade_pending_risk_resume.clear()
        with sqlite3.connect(self.db) as conn:
            conn.execute("CREATE TABLE autotrade_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_ts INTEGER NOT NULL)")
            conn.execute("CREATE TABLE autotrade_events (id INTEGER PRIMARY KEY, ts_ms INTEGER NOT NULL, trade_id INTEGER, signal_id INTEGER, symbol TEXT, event TEXT, detail TEXT)")
            conn.execute("CREATE TABLE autotrade_daily (local_date TEXT, scope TEXT, start_balance REAL, realized_net_pnl REAL, consecutive_stops INTEGER, locked INTEGER, lock_reason TEXT, cooldown_until_ts INTEGER, updated_ts INTEGER, PRIMARY KEY (local_date, scope))")
            conn.execute("INSERT INTO autotrade_daily VALUES ('2026-10-14', 'LIVE', 989, -38.78, 2, 0, NULL, 0, 1)")

    def current(self):
        return bot._at_daily_row(scope="LIVE")

    def grant(self, extra=20):
        return bot._at_manual_risk_grant(extra, "owner",
            expected_date="2026-10-14", expected_balance=989,
            expected_daily_pct=5)

    def test_before_and_after_manual_approval_projected_risk(self):
        self.assertFalse(bot._at_risk_allowed("LIVE", 11.0)[0])
        self.assertTrue(self.grant()[0])
        self.assertEqual(bot._at_manual_risk_extra("LIVE", self.current()), 20)
        self.assertTrue(bot._at_risk_allowed("LIVE", 11.0)[0])
        self.assertFalse(bot._at_risk_allowed("LIVE", 35.0)[0])
        self.assertEqual(bot._at_manual_risk_extra("DRY", self.current()), 0)
        with sqlite3.connect(self.db) as conn:
            rows = conn.execute("SELECT event FROM autotrade_events").fetchall()
        self.assertEqual(rows, [("MANUAL_DAILY_RISK_RESUME",)])

    def test_sticky_daily_lock_bypassed_only_with_valid_extra(self):
        self.assertTrue(self.grant()[0])
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE autotrade_daily SET locked=1,lock_reason='DAILY_LOSS_5.00PCT',realized_net_pnl=-52 WHERE scope='LIVE'")
        self.assertTrue(bot._at_risk_allowed("LIVE", 10.0)[0])
        self.assertFalse(bot._at_risk_allowed("LIVE", 20.0)[0])
        self.assertTrue(bot._at_manual_risk_revoke("owner")[0])
        self.assertFalse(bot._at_risk_allowed("LIVE", 1.0)[0])
        self.assertFalse(self.grant()[0])
        with sqlite3.connect(self.db) as conn:
            events = conn.execute("SELECT event FROM autotrade_events ORDER BY id").fetchall()
        self.assertEqual(events, [("MANUAL_DAILY_RISK_RESUME",), ("MANUAL_DAILY_RISK_REVOKED",)])

    def test_position_and_cooldown_gates_remain(self):
        self.assertTrue(self.grant()[0])
        with patch.object(bot, "_at_open_count", return_value=3):
            self.assertEqual(bot._at_risk_allowed("LIVE", 1.0)[1], "MAX_OPEN_POSITIONS")
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE autotrade_daily SET cooldown_until_ts=?",
                         (int(time.time()) + 200,))
        self.assertTrue(bot._at_risk_allowed("LIVE", 1.0)[1].startswith("STOP_COOLDOWN_"))

    def test_midnight_and_pct_change_revoke_effectively(self):
        self.assertTrue(self.grant()[0])
        with patch.object(bot, "_at_local_date", return_value="2026-10-15"):
            self.assertEqual(bot._at_manual_risk_extra("LIVE", bot._at_daily_row(scope="LIVE")), 0)
        with patch.dict(bot.autotrade_cfg, {"daily_max_loss_pct": 6}):
            self.assertEqual(bot._at_manual_risk_extra("LIVE", self.current()), 0)

    def test_duplicate_and_stale_confirmations_do_not_grant(self):
        self.assertTrue(self.grant()[0])
        self.assertFalse(self.grant()[0])
        with patch.object(bot, "_at_local_date", return_value="2026-10-15"):
            self.assertFalse(self.grant()[0])
        with sqlite3.connect(self.db) as conn:
            num = conn.execute("SELECT COUNT(*) FROM autotrade_events").fetchone()[0]
        self.assertEqual(num, 1)

    def test_atomic_failure_rolls_back_grant(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute("DROP TABLE autotrade_events")
        with self.assertRaises(sqlite3.OperationalError):
            self.grant()
        self.assertIsNone(bot._at_manual_risk_record())

    def test_request_requires_admin_and_second_confirmation(self):
        message = AsyncMock()
        with patch.object(bot, "telegram_send", message):
            asyncio.run(bot._at_command(None, "/riskresume 20", "admin_chat", "other"))
            self.assertFalse(bot._autotrade_pending_risk_resume)
            asyncio.run(bot._at_command(None, "/riskresume 20", "admin_chat", "owner"))
            self.assertEqual(len(bot._autotrade_pending_risk_resume), 1)
            self.assertIsNone(bot._at_manual_risk_record())
        cb_token = next(iter(bot._autotrade_pending_risk_resume))
        calls = AsyncMock()
        with patch.object(bot, "telegram_api_call", calls):
            cb = lambda actor: {"id":"id1","data":"at:riskgrant:"+cb_token,
                "from":{"id":actor},"message":{"chat":{"id":"admin_chat"},"message_id":23}}
            asyncio.run(bot.handle_autotrade_callback(None, cb("other")))
            self.assertIsNone(bot._at_manual_risk_record())
            asyncio.run(bot.handle_autotrade_callback(None, cb("owner")))
        self.assertEqual(bot._at_manual_risk_extra("LIVE", self.current()), 20)


if __name__ == "__main__":
    unittest.main()
