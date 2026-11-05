import asyncio
import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock


class EarlyNotificationTerminalTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.state_path = Path(self.tempdir.name) / "early-notification.json"
        self.old_path = os.environ.get("EARLY_NOTIFICATION_STATE_PATH")
        self.old_default = os.environ.get("EARLY_SIGNAL_TELEGRAM_ENABLED")
        os.environ["EARLY_NOTIFICATION_STATE_PATH"] = str(self.state_path)
        os.environ["EARLY_SIGNAL_TELEGRAM_ENABLED"] = "0"
        import notification_policy
        self.policy = importlib.reload(notification_policy)

    def tearDown(self):
        if self.old_path is None:
            os.environ.pop("EARLY_NOTIFICATION_STATE_PATH", None)
        else:
            os.environ["EARLY_NOTIFICATION_STATE_PATH"] = self.old_path
        if self.old_default is None:
            os.environ.pop("EARLY_SIGNAL_TELEGRAM_ENABLED", None)
        else:
            os.environ["EARLY_SIGNAL_TELEGRAM_ENABLED"] = self.old_default
        self.tempdir.cleanup()

    def test_environment_default_is_used_before_terminal_override(self):
        self.assertFalse(self.policy.early_signal_notifications_enabled())
        os.environ["EARLY_SIGNAL_TELEGRAM_ENABLED"] = "1"
        self.assertTrue(self.policy.early_signal_notifications_enabled())
        self.assertEqual(
            self.policy.early_signal_notification_status()["source"],
            "environment",
        )

    def test_terminal_toggle_persists_across_reload(self):
        self.assertTrue(self.policy.set_early_signal_notifications_enabled(True))
        self.assertTrue(self.policy.early_signal_notifications_enabled())
        self.assertEqual(json.loads(self.state_path.read_text())["enabled"], True)

        self.policy = importlib.reload(self.policy)
        self.assertTrue(self.policy.early_signal_notifications_enabled())
        self.assertEqual(
            self.policy.early_signal_notification_status()["source"],
            "persistent",
        )

        self.assertFalse(self.policy.set_early_signal_notifications_enabled(False))
        self.assertFalse(self.policy.early_signal_notifications_enabled())
        self.assertEqual(json.loads(self.state_path.read_text())["enabled"], False)

    def test_malformed_or_non_boolean_override_falls_back_to_environment(self):
        os.environ["EARLY_SIGNAL_TELEGRAM_ENABLED"] = "1"
        for payload in (
            "not-json",
            json.dumps([]),
            json.dumps({}),
            json.dumps({"enabled": "false"}),
            json.dumps({"enabled": 1}),
        ):
            with self.subTest(payload=payload):
                self.state_path.write_text(payload, encoding="utf-8")
                self.policy = importlib.reload(self.policy)
                self.assertTrue(self.policy.early_signal_notifications_enabled())
                self.assertEqual(
                    self.policy.early_signal_notification_status()["source"],
                    "environment",
                )
        with self.assertRaises(TypeError):
            self.policy.set_early_signal_notifications_enabled("false")

    def test_admin_callback_toggles_and_unauthorized_user_cannot_change_state(self):
        import telegram_ux as ux

        api = AsyncMock(return_value={"ok": True})
        send = AsyncMock(return_value=True)
        bot = {
            "_at_admin_allowed": lambda chat, user: (chat, user) == ("admin", "owner"),
            "telegram_api_call": api,
            "telegram_send": send,
        }

        def callback(action, chat="admin", user="owner"):
            return {
                "id": "callback-id",
                "data": "earlynotify:" + action,
                "from": {"id": user},
                "message": {"chat": {"id": chat}},
            }

        self.assertTrue(asyncio.run(ux.early_notification_callback(bot, None, callback("on"))))
        self.assertTrue(self.policy.early_signal_notifications_enabled())
        self.assertEqual(json.loads(self.state_path.read_text())["enabled"], True)
        send.assert_awaited_once()
        self.assertIn("Premium bildirimleri: etkilenmez", send.await_args.args[1])
        self.assertIn("Otomatik emir: YOK", send.await_args.args[1])

        send.reset_mock()
        api.reset_mock()
        self.assertTrue(
            asyncio.run(
                ux.early_notification_callback(
                    bot,
                    None,
                    callback("off", chat="admin", user="other"),
                )
            )
        )
        self.assertTrue(self.policy.early_signal_notifications_enabled())
        send.assert_not_awaited()
        self.assertTrue(api.await_args.args[2]["show_alert"])

        api.reset_mock()
        self.assertTrue(asyncio.run(ux.early_notification_callback(bot, None, callback("off"))))
        self.assertFalse(self.policy.early_signal_notifications_enabled())
        self.assertEqual(json.loads(self.state_path.read_text())["enabled"], False)

    def test_unrelated_callback_is_not_consumed(self):
        import telegram_ux as ux

        bot = {
            "_at_admin_allowed": lambda *_: True,
            "telegram_api_call": AsyncMock(),
            "telegram_send": AsyncMock(),
        }
        handled = asyncio.run(
            ux.early_notification_callback(
                bot,
                None,
                {"id": "x", "data": "uxmode:anything"},
            )
        )
        self.assertFalse(handled)
        bot["telegram_api_call"].assert_not_awaited()
        bot["telegram_send"].assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
