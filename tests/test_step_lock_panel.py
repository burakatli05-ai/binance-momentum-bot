"""STEP panel preparation: isolated drafts, explicit confirmation, no trading I/O."""
import asyncio
from copy import deepcopy
from contextlib import ExitStack
from decimal import Decimal
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "binance_momentum_bot"))
import step_lock_panel as panel


class DraftValidationTests(unittest.TestCase):
    def test_legacy_default_keeps_notional_distinct_from_margin_and_actual_leverage(self):
        draft = panel.Draft()
        self.assertEqual("legacy_notional", draft.basis)
        self.assertIsNone(draft.margin_usdt)
        self.assertIsNone(draft.leverage)
        self.assertEqual(Decimal("25"), draft.notional_usdt)
        draft.validate_ready()

    def test_switching_basis_requires_both_user_choices(self):
        for partial in (panel.Draft().with_margin("200"), panel.Draft().with_leverage("10")):
            with self.subTest(partial=partial):
                self.assertEqual("margin", partial.basis)
                self.assertIsNone(partial.notional_usdt)
                with self.assertRaises(ValueError):
                    partial.validate_ready()
        full = panel.Draft().with_margin("200").with_leverage("10")
        full.validate_ready()
        self.assertEqual(Decimal("2000"), full.notional_usdt)
        self.assertEqual(Decimal("1000"), full.with_leverage("5").notional_usdt)
        self.assertEqual(Decimal("100"), full.with_margin("10").notional_usdt)

    def test_margin_bounds_precision_and_nonfinite_input(self):
        for value, expected in (("0.01", "0.01"), ("12,50", "12.50"), ("200", "200")):
            with self.subTest(value=value):
                self.assertEqual(Decimal(expected), panel.parse_margin(value))
        for value in ("0", "-1", "200.01", "201", "1.001", "NaN", "Infinity", "-Infinity", "", "x", None, True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                panel.parse_margin(value)

    def test_leverage_is_an_integer_inside_user_limit(self):
        for value in range(1, 11):
            self.assertEqual(value, panel.parse_leverage(str(value)))
        for value in ("0", "11", "-1", "1.5", "10.1", "NaN", "Infinity", "", "x", None, True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                panel.parse_leverage(value)


class DraftStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "drafts.sqlite3"
        self.store = panel.DraftStore(self.path)

    def test_round_trip_is_scoped_to_both_chat_and_actor(self):
        self.assertIsNone(self.store.load("admin", "user"))
        draft = panel.Draft().with_margin("200").with_leverage("10")
        self.store.save("admin", "user", draft)
        reopened = panel.DraftStore(self.path)
        self.assertEqual(draft, reopened.load("admin", "user"))
        self.assertIsNone(reopened.load("other", "user"))
        self.assertIsNone(reopened.load("admin", "other"))

    def test_incomplete_draft_cannot_be_persisted(self):
        with self.assertRaises(ValueError):
            self.store.save("admin", "user", panel.Draft().with_margin("200"))
        self.assertIsNone(self.store.load("admin", "user"))

    def test_corrupt_store_is_not_silently_reset(self):
        corrupt = b"not a sqlite database; preserve for diagnosis"
        self.path.write_bytes(corrupt)
        with self.assertRaises(panel.DraftUnavailable):
            self.store.load("admin", "user")
        self.assertEqual(corrupt, self.path.read_bytes())


class PanelInteractionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.events = []
        panel.sessions.clear()
        panel.pending_confirmations.clear()
        self.addCleanup(panel.sessions.clear)
        self.addCleanup(panel.pending_confirmations.clear)

        async def send(session, text, **kwargs):
            self.events.append(("send", {"text": text, **deepcopy(kwargs)}))
            return {"ok": True, "result": {"message_id": 10}}

        async def api(session, method, data):
            self.events.append((method, deepcopy(data)))
            return {"ok": True, "result": {"message_id": 10}}

        self.authorized = {("admin", "user")}
        self.auth = Mock(side_effect=lambda chat, user, *, require_user_id=False:
                         require_user_id and (str(chat), str(user)) in self.authorized)
        self.normal_config = {"mode": "DRY", "trade_margin_usdt": 200, "leverage": 10}
        self.normal_before = deepcopy(self.normal_config)
        self.bot = {
            "DB_PATH": str(Path(self.tmp.name) / "signals.db"),
            "_at_admin_allowed": self.auth,
            "telegram_send": AsyncMock(side_effect=send),
            "telegram_api_call": AsyncMock(side_effect=api),
            "autotrade_cfg": self.normal_config,
            "autotrade_account_cache": {"wallet_balance": 12345, "available_balance": 12345},
            "binance_signed_request": AsyncMock(),
            "_at_try_live_enable": AsyncMock(),
            "_at_place_market_entry": AsyncMock(),
            "_at_save_setting": Mock(),
        }

    def command(self, text, chat="admin", user="user"):
        self.assertTrue(asyncio.run(panel.handle(self.bot, None, text, chat, user)))

    def tap(self, data, chat="admin", user="user"):
        callback = {"id": "query", "data": data, "from": {"id": user},
                    "message": {"message_id": 10, "chat": {"id": chat}}}
        self.assertTrue(asyncio.run(panel.callback(self.bot, None, callback)))

    def markup(self):
        for _, payload in reversed(self.events):
            if "inline_keyboard" in payload.get("reply_markup", {}):
                return payload["reply_markup"]["inline_keyboard"]
        self.fail("No inline keyboard was produced")

    def action(self, prefix):
        for row in self.markup():
            for button in row:
                if button.get("callback_data", "").startswith(prefix):
                    return button["callback_data"]
        self.fail(f"No action with prefix {prefix!r}")

    def texts(self):
        return "\n".join(str(payload.get("text", "")) for _, payload in self.events)

    def select_max(self):
        self.command("/step margin 200")
        self.command("/step leverage 10")

    def save_max(self):
        self.select_max()
        self.tap(self.action("slp:save:"))
        token = self.action("slp:confirm:")
        self.tap(token)
        return token

    def assert_no_trading(self):
        self.assertEqual(self.normal_before, self.normal_config)
        self.bot["_at_save_setting"].assert_not_called()
        for name in ("binance_signed_request", "_at_try_live_enable", "_at_place_market_entry"):
            self.bot[name].assert_not_awaited()

    def test_default_display_does_not_borrow_normal_balance_or_claim_live_data(self):
        self.command("/step")
        text = self.texts()
        self.assertIn("25", text)
        self.assertIn("5", text)
        self.assertIn("TASLAK", text.upper())
        self.assertIn("kilitli", text.casefold().replace("i\u0307", "i"))
        self.assertNotIn("12345", text)
        self.assert_no_trading()

    def test_max_choices_need_confirmation_and_survive_restart_as_draft_only(self):
        self.select_max()
        with patch.object(panel.DraftStore, "save", autospec=True) as save:
            self.tap(self.action("slp:save:"))
            save.assert_not_called()
            confirmation = self.action("slp:confirm:")
            self.assertIn("200", self.texts())
            self.assertIn("10", self.texts())
        self.tap(confirmation)
        panel.sessions.clear()
        panel.pending_confirmations.clear()
        self.events.clear()
        self.command("/step")
        text = self.texts()
        self.assertIn("200", text)
        self.assertIn("10", text)
        self.assertIn("2000", text.replace(",", "").replace(".", "").replace(" ", ""))
        self.assertIn("TASLAK", text.upper())
        self.tap("slp:live")
        self.assert_no_trading()

    def test_incomplete_selection_cannot_be_saved(self):
        self.command("/step margin 200")
        with patch.object(panel.DraftStore, "save", autospec=True) as save:
            save_actions = [button["callback_data"] for row in self.markup() for button in row
                            if button.get("callback_data", "").startswith("slp:save:")]
            for action in save_actions:
                self.tap(action)
            self.assertFalse(panel.pending_confirmations)
            save.assert_not_called()
        self.assert_no_trading()

    def test_unauthorized_actor_cannot_read_or_stage_or_save(self):
        self.command("/step margin 200", user="other")
        self.tap("slp:panel", user="other")
        self.assertFalse(panel.sessions)
        self.assertFalse(panel.pending_confirmations)
        self.assertFalse(list(Path(self.tmp.name).iterdir()))
        self.assertTrue(all(call.kwargs.get("require_user_id") is True for call in self.auth.call_args_list))
        self.assert_no_trading()

    def test_confirmation_is_bound_to_actor_chat_and_single_use(self):
        self.select_max()
        self.tap(self.action("slp:save:"))
        confirmation = self.action("slp:confirm:")
        self.authorized.update({("admin", "other"), ("other", "user")})
        with patch.object(panel.DraftStore, "save", autospec=True) as save:
            self.tap(confirmation, user="other")
            self.tap(confirmation, chat="other")
            save.assert_not_called()
            self.tap(confirmation)
            self.assertEqual(1, save.call_count)
            self.tap(confirmation)
            self.assertEqual(1, save.call_count)
        self.assert_no_trading()

    def test_expired_confirmation_cannot_save(self):
        self.select_max()
        self.tap(self.action("slp:save:"))
        confirmation = self.action("slp:confirm:")
        with patch.object(panel.time, "monotonic", return_value=time.monotonic() + panel.CONFIRM_TTL_SECONDS + 10), \
                patch.object(panel.DraftStore, "save", autospec=True) as save:
            self.tap(confirmation)
            save.assert_not_called()

    def test_changing_selection_invalidates_old_confirmation(self):
        self.select_max()
        self.tap(self.action("slp:save:"))
        confirmation = self.action("slp:confirm:")
        self.command("/step margin 100")
        with patch.object(panel.DraftStore, "save", autospec=True) as save:
            self.tap(confirmation)
            save.assert_not_called()
        self.assert_no_trading()

    def test_cancelled_confirmation_cannot_be_replayed(self):
        self.select_max()
        self.tap(self.action("slp:save:"))
        confirmation = self.action("slp:confirm:")
        cancellation = self.action("slp:cancel:")
        self.tap(cancellation)
        with patch.object(panel.DraftStore, "save", autospec=True) as save:
            self.tap(confirmation)
            save.assert_not_called()

    def test_stale_picker_cannot_overwrite_newer_selection(self):
        self.command("/step")
        self.tap("slp:margin")
        stale_choice = self.action("slp:set:m:")
        self.command("/step margin 200")
        self.command("/step leverage 10")
        self.tap(stale_choice)
        self.tap(self.action("slp:save:"))
        with patch.object(panel.DraftStore, "save", autospec=True) as save:
            self.tap(self.action("slp:confirm:"))
            self.assertEqual(1, save.call_count)
            draft = save.call_args.args[-1]
            self.assertEqual(Decimal("2000"), draft.notional_usdt)

    def test_picker_from_previous_session_cannot_edit_reloaded_saved_draft(self):
        self.command("/step")
        self.tap("slp:margin")
        stale_choice = self.action("slp:set:m:")
        self.save_max()
        panel.sessions.clear()
        self.command("/step")
        self.tap(stale_choice)
        self.tap(self.action("slp:save:"))
        with patch.object(panel.DraftStore, "save", autospec=True) as save:
            self.tap(self.action("slp:confirm:"))
            self.assertEqual(1, save.call_count)
            self.assertEqual(Decimal("2000"), save.call_args.args[-1].notional_usdt)

    def test_expired_session_rejects_old_picker_when_saved_draft_reloads(self):
        self.command("/step")
        self.tap("slp:margin")
        stale_choice = self.action("slp:set:m:")
        self.save_max()
        expired_time = time.monotonic() + panel.SESSION_TTL_SECONDS + 1
        with patch.object(panel.time, "monotonic", return_value=expired_time):
            self.command("/step")
            self.tap(stale_choice)
            self.tap(self.action("slp:save:"))
            with patch.object(panel.DraftStore, "save", autospec=True) as save:
                self.tap(self.action("slp:confirm:"))
                self.assertEqual(Decimal("2000"), save.call_args.args[-1].notional_usdt)

    def test_restore_legacy_profile_and_callback_payload_lengths(self):
        self.select_max()
        self.tap(self.action("slp:legacy:"))
        self.tap(self.action("slp:save:"))
        with patch.object(panel.DraftStore, "save", autospec=True) as save:
            self.tap(self.action("slp:confirm:"))
            saved = save.call_args.args[-1]
            self.assertEqual(panel.Draft(), saved)
        for picker in ("slp:margin", "slp:leverage"):
            self.tap(picker)
        for _, payload in self.events:
            for row in payload.get("reply_markup", {}).get("inline_keyboard", []):
                for button in row:
                    if "callback_data" in button:
                        self.assertLessEqual(len(button["callback_data"].encode("utf-8")), 64)
        self.assert_no_trading()

    def test_invalid_command_values_do_not_create_saveable_draft(self):
        for command in ("/step margin 201", "/step margin NaN", "/step margin -1", "/step leverage 11", "/step leverage 1.5"):
            self.command(command)
        self.assertFalse(panel.pending_confirmations)
        self.assert_no_trading()

    def test_corrupt_store_blocks_ui_save_and_preserves_file(self):
        path = panel._store_path(self.bot)
        corrupt = b"unreadable draft database"
        path.write_bytes(corrupt)
        self.command("/step")
        self.assertIn("okunamadı", self.texts())
        self.select_max()
        revision = self.action("slp:legacy:").split(":")[-1]
        self.tap("slp:save:" + revision)
        self.assertIn("Taslak deposu okunamadığı için kaydetme kilitli", self.texts())
        self.assertFalse(panel.pending_confirmations)
        self.assertEqual(corrupt, path.read_bytes())
        self.assert_no_trading()

    def test_failed_save_is_not_reported_as_success_or_replayable(self):
        self.select_max()
        self.tap(self.action("slp:save:"))
        confirmation = self.action("slp:confirm:")
        self.events.clear()
        with patch.object(panel.DraftStore, "save", autospec=True,
                          side_effect=panel.DraftUnavailable("Taslak kaydedilemedi.")) as save:
            self.tap(confirmation)
            self.tap(confirmation)
            self.assertEqual(1, save.call_count)
        self.assertIn("kaydedilemedi", self.texts())
        self.assertNotIn("Taslak kaydedildi", self.texts())
        self.assert_no_trading()

    def test_failed_telegram_edit_falls_back_after_save_without_replaying_write(self):
        self.select_max()
        self.tap(self.action("slp:save:"))
        confirmation = self.action("slp:confirm:")
        self.events.clear()
        self.bot["telegram_send"].reset_mock()

        async def unavailable_edit(session, method, data):
            self.events.append((method, deepcopy(data)))
            return {"ok": False, "description": "message to edit not found"} if method == "editMessageText" else {"ok": True}

        self.bot["telegram_api_call"].side_effect = unavailable_edit
        original_save = panel.DraftStore.save
        with patch.object(panel.DraftStore, "save", autospec=True, side_effect=original_save) as save:
            self.tap(confirmation)
            self.bot["telegram_send"].assert_awaited_once()
            self.assertIn("Taslak kaydedildi", self.bot["telegram_send"].await_args.args[1])
            self.tap(confirmation)
            self.assertEqual(1, save.call_count)
        saved = panel.DraftStore(panel._store_path(self.bot)).load("admin", "user")
        self.assertEqual(Decimal("2000"), saved.notional_usdt)
        self.assert_no_trading()

    def test_both_telegram_deliveries_failing_keep_committed_draft_and_alert_user(self):
        self.select_max()
        self.tap(self.action("slp:save:"))
        confirmation = self.action("slp:confirm:")
        self.events.clear()

        async def unavailable_edit(session, method, data):
            self.events.append((method, deepcopy(data)))
            return {"ok": False, "description": "message to edit not found"} if method == "editMessageText" else {"ok": True}

        self.bot["telegram_api_call"].side_effect = unavailable_edit
        self.bot["telegram_send"].side_effect = None
        self.bot["telegram_send"].return_value = False
        original_save = panel.DraftStore.save
        with patch.object(panel.DraftStore, "save", autospec=True, side_effect=original_save) as save:
            self.tap(confirmation)
            alerts = [payload for method, payload in self.events
                      if method == "answerCallbackQuery" and payload.get("show_alert")]
            self.assertTrue(alerts)
            self.assertIn("/step", alerts[-1]["text"])
            self.tap(confirmation)
            self.assertEqual(1, save.call_count)
        saved = panel.DraftStore(panel._store_path(self.bot)).load("admin", "user")
        self.assertEqual(Decimal("2000"), saved.notional_usdt)
        self.assert_no_trading()

    def test_telegram_unchanged_message_response_does_not_send_duplicate_panel(self):
        self.command("/step")
        self.bot["telegram_send"].reset_mock()

        async def unchanged_edit(session, method, data):
            self.events.append((method, deepcopy(data)))
            return {"ok": False, "description": "Bad Request: message is not modified"} if method == "editMessageText" else {"ok": True}

        self.bot["telegram_api_call"].side_effect = unchanged_edit
        self.tap("slp:panel")
        self.bot["telegram_send"].assert_not_awaited()
        self.assert_no_trading()

    def test_other_namespaces_are_not_intercepted(self):
        self.assertFalse(asyncio.run(panel.handle(self.bot, None, "/autotrade", "admin", "user")))
        self.assertFalse(asyncio.run(panel.callback(self.bot, None, {"data": "at:mode:LIVE"})))
        self.assert_no_trading()

    def test_real_bot_routes_step_menu_and_callbacks_without_touching_normal_state(self):
        import test_v5135 as fixtures
        bot = fixtures.bot
        normal_database = Path(self.bot["DB_PATH"])
        normal_contents = b"normal bot database must not be opened by STEP settings"
        normal_database.write_bytes(normal_contents)
        with ExitStack() as stack:
            for name in ("DB_PATH", "telegram_send", "telegram_api_call", "binance_signed_request",
                         "_at_try_live_enable", "_at_place_market_entry", "_at_save_setting"):
                stack.enter_context(patch.object(bot, name, self.bot[name]))
            stack.enter_context(patch.object(bot, "TELEGRAM_ADMIN_CHAT_ID", "admin"))
            stack.enter_context(patch.object(bot, "TELEGRAM_ADMIN_USER_ID", "user"))
            stack.enter_context(patch.object(bot, "AUTO_TRADE_LIVE_ALLOWED", True))
            stack.enter_context(patch.dict(bot.autotrade_cfg, self.normal_config))
            before = deepcopy(bot.autotrade_cfg)
            for raw in ("🪜 STEP Lock", "/step margin 200", "/step leverage 10"):
                self.assertTrue(asyncio.run(bot._at_command(None, raw, "admin", "user")))

            def route(data):
                callback = {"id": "query", "data": data, "from": {"id": "user"},
                            "message": {"message_id": 10, "chat": {"id": "admin"}}}
                self.assertTrue(asyncio.run(bot.handle_autotrade_callback(None, callback)))

            route(self.action("slp:save:"))
            route(self.action("slp:confirm:"))
            route("slp:live")
            self.assertEqual(before, bot.autotrade_cfg)
            self.assertEqual(normal_contents, normal_database.read_bytes())
            self.assert_no_trading()
            count = len(self.events)
            with patch.object(bot, "TELEGRAM_ADMIN_USER_ID", ""):
                self.assertTrue(asyncio.run(bot._at_command(None, "/step", "admin", "user")))
            self.assertEqual(count, len(self.events))


if __name__ == "__main__":
    unittest.main()
