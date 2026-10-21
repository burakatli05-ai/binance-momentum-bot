"""Focused installer tests; all writes stay in temporary fake application trees."""
from __future__ import annotations

from contextlib import redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock

import step_panel_install as installer


REVIEW_SOURCE = Path(__file__).resolve().parents[2] / "binance_momentum_bot"
if not REVIEW_SOURCE.is_dir():
    REVIEW_SOURCE = (
        Path(__file__).resolve().parent.parent
        / "step-lock-panel-review"
        / "binance_momentum_bot"
    )


def digest(data):
    return hashlib.sha256(data).hexdigest()


class InstallerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="step-panel-install-test-")
        self.addCleanup(temporary.cleanup)
        self.app = Path(temporary.name)
        self.original = {
            "bot.py": b"# Preserved normal bot fixture\nBOT_CORE = 'unchanged'\n",
            "step_signal_bridge.py": b"# Preserved normal relay fixture\nRELAY = 'unchanged'\n",
            "startup.py": b"# Preserved verified startup fixture\nBOOT_MODE = 'OFF'\n",
            "telegram_ux.py": b"# Old Telegram interface fixture\nOLD_UX = True\n",
            "signals.db": b"not a database: must never be opened or rewritten\x00",
            "support.py": b"SUPPORT = 'unchanged'\n",
        }
        for name, data in self.original.items():
            (self.app / name).write_bytes(data)
        (self.app / "startup.py").chmod(0o750)
        (self.app / "telegram_ux.py").chmod(0o640)
        (self.app / "signals.db").chmod(0o600)
        self.core_patch = mock.patch.object(installer, "CORE_SHA256", {
            name: digest(self.original[name])
            for name in ("bot.py", "step_signal_bridge.py")
        })
        self.base_patch = mock.patch.object(installer, "BASE_SHA256", {
            name: digest(self.original[name])
            for name in ("startup.py", "telegram_ux.py")
        })
        self.core_patch.start()
        self.base_patch.start()
        self.addCleanup(self.core_patch.stop)
        self.addCleanup(self.base_patch.stop)
        # The author separately exercises the real isolated importer against the
        # complete frozen repository. Fixtures deliberately contain no real bot.
        imports = mock.patch.object(installer, "validate_imports", return_value=None)
        self.import_check = imports.start()
        self.addCleanup(imports.stop)
        startup = mock.patch.object(installer.os, "execv", return_value=None)
        self.execv = startup.start()
        self.addCleanup(startup.stop)
        self.payloads = installer.decode_payloads()

    def tree(self):
        result = {}
        for path in self.app.rglob("*"):
            relative = str(path.relative_to(self.app))
            result[relative] = (
                path.read_bytes() if path.is_file() else None,
                stat.S_IMODE(path.stat().st_mode),
            )
        return result

    def fail_ux_replace(self, *, after_rename=False, fail_rollback=False):
        original_replace = installer.os.replace

        def replace(source, destination):
            source, destination = Path(source), Path(destination)
            if destination == self.app / "telegram_ux.py":
                if source.name == "new-telegram_ux.py":
                    if after_rename:
                        original_replace(source, destination)
                    raise OSError("injected replacement failure")
                if fail_rollback and source.name == "old-telegram_ux.py":
                    raise OSError("injected rollback failure")
            return original_replace(source, destination)

        return mock.patch.object(installer.os, "replace", side_effect=replace)

    def run_main(self):
        output = io.StringIO()
        with redirect_stdout(output):
            result = installer.main(["--app-dir", str(self.app)])
        return result, [json.loads(line) for line in output.getvalue().splitlines()]

    def assert_original_startup_exec(self):
        self.execv.assert_called_once_with(
            installer.sys.executable,
            [installer.sys.executable, "-u", str(self.app / "startup.py")],
        )

    def test_embedded_payloads_are_exact_frozen_reviewed_sources(self):
        self.assertEqual(set(self.payloads), {
            "telegram_ux.py", "step_lock_panel.py", "step_lock_control.py",
        })
        for name, content in self.payloads.items():
            with self.subTest(name=name):
                self.assertEqual(content, (REVIEW_SOURCE / name).read_bytes())
                self.assertEqual(digest(content), installer.PAYLOAD_SHA256[name])
                self.assertEqual(len(content), installer.PAYLOAD_SIZES[name])

    def test_install_preserves_core_startup_and_data_and_never_execs(self):
        before = self.tree()
        result = installer.install(self.app)
        self.assertEqual(result.status, "installed")
        self.assertEqual(result.changed, installer.INSTALL_ORDER)
        for name, content in self.payloads.items():
            self.assertEqual((self.app / name).read_bytes(), content)
        after = self.tree()
        for name in self.original.keys() - {"telegram_ux.py"}:
            self.assertEqual(after[name], before[name], name)
        self.assertEqual(after["telegram_ux.py"][1], before["telegram_ux.py"][1])
        self.assertEqual(set(after), set(before) | set(self.payloads))
        for name in ("bot.py", "step_signal_bridge.py", "startup.py"):
            self.assertEqual(result.source_hashes[name], digest(self.original[name]))
        self.import_check.assert_called_once()
        self.execv.assert_not_called()

    def test_second_install_is_idempotent_without_staging_or_replacement(self):
        installer.install(self.app)
        before = self.tree()
        with mock.patch.object(installer.tempfile, "mkdtemp") as staging, \
                mock.patch.object(installer.os, "replace") as replace:
            result = installer.install(self.app)
        self.assertEqual(result.status, "already_installed")
        self.assertEqual(result.changed, ())
        staging.assert_not_called()
        replace.assert_not_called()
        self.assertEqual(self.tree(), before)
        self.execv.assert_not_called()

    def test_mismatched_original_sources_refuse_before_any_staging(self):
        for name in ("bot.py", "step_signal_bridge.py", "startup.py", "telegram_ux.py"):
            with self.subTest(name=name):
                source = self.app / name
                source.write_bytes(self.original[name] + b"# unreviewed change\n")
                before = self.tree()
                self.import_check.reset_mock()
                with mock.patch.object(installer.tempfile, "mkdtemp") as staging, \
                        mock.patch.object(installer, "_write_staged") as write, \
                        mock.patch.object(installer.os, "replace") as replace:
                    with self.assertRaises(installer.PreflightError) as caught:
                        installer.install(self.app)
                self.assertEqual(caught.exception.code, "SOURCE_HASH_MISMATCH:" + name)
                staging.assert_not_called()
                write.assert_not_called()
                replace.assert_not_called()
                self.import_check.assert_not_called()
                self.assertEqual(self.tree(), before)
                source.write_bytes(self.original[name])
        self.execv.assert_not_called()

    def test_unexpected_existing_step_module_is_never_overwritten(self):
        for name in ("step_lock_panel.py", "step_lock_control.py"):
            with self.subTest(name=name):
                path = self.app / name
                path.write_bytes(b"# Unrelated local module\nUNREVIEWED = True\n")
                before = self.tree()
                with mock.patch.object(installer.tempfile, "mkdtemp") as staging:
                    with self.assertRaises(installer.PreflightError) as caught:
                        installer.install(self.app)
                self.assertEqual(caught.exception.code, "SOURCE_HASH_MISMATCH:" + name)
                staging.assert_not_called()
                self.assertEqual(self.tree(), before)
                path.unlink()

    def test_new_ux_with_missing_module_refuses_startup_but_old_ux_can_complete(self):
        for missing in ("step_lock_panel.py", "step_lock_control.py"):
            with self.subTest(missing=missing):
                for name, content in self.payloads.items():
                    (self.app / name).write_bytes(content)
                (self.app / missing).unlink()
                before = self.tree()
                self.import_check.reset_mock()
                with mock.patch.object(installer.tempfile, "mkdtemp") as staging, \
                        mock.patch.object(installer, "_write_staged") as write:
                    result, events = self.run_main()
                self.assertEqual(result, 1)
                self.assertEqual(events[0]["event"], "PANEL_INSTALL_FAILED")
                self.assertEqual(events[0]["reason"], "INCOMPLETE_PRIOR_INSTALL")
                self.assertFalse(events[0]["normal_startup"])
                staging.assert_not_called()
                write.assert_not_called()
                self.import_check.assert_not_called()
                self.execv.assert_not_called()
                self.assertEqual(self.tree(), before)
                # A modules-first partial install with the old UX is recoverable.
                (self.app / "telegram_ux.py").write_bytes(self.original["telegram_ux.py"])
                installed = installer.install(self.app)
                self.assertEqual(installed.status, "installed")
                for name, content in self.payloads.items():
                    self.assertEqual((self.app / name).read_bytes(), content)
                self.execv.assert_not_called()

    def test_only_newline_equivalence_is_accepted_and_core_bytes_are_preserved(self):
        protected = ("bot.py", "step_signal_bridge.py", "startup.py")
        for name in protected:
            (self.app / name).write_bytes(self.original[name].replace(b"\n", b"\r\n"))
        for name, content in self.payloads.items():
            (self.app / name).write_bytes(content.replace(b"\n", b"\r\n"))
        before = self.tree()
        installer.install(self.app)
        for name in protected:
            self.assertEqual(self.tree()[name], before[name])
        for name, content in self.payloads.items():
            self.assertEqual((self.app / name).read_bytes(), content)
        (self.app / "bot.py").write_bytes(self.original["bot.py"].replace(b" = ", b"  = "))
        with self.assertRaises(installer.PreflightError) as caught:
            installer.install(self.app)
        self.assertEqual(caught.exception.code, "SOURCE_HASH_MISMATCH:bot.py")

    def test_invalid_payload_or_import_failure_prevents_staging(self):
        cases = (
            mock.patch.dict(installer.PAYLOAD_B64, {"step_lock_panel.py": "!!!"}),
            mock.patch.object(installer, "validate_imports", side_effect=
                              installer.PreflightError("IMPORT_VALIDATION_FAILED")),
        )
        for failure in cases:
            with self.subTest(failure=failure.attribute if hasattr(failure, "attribute") else "payload"):
                before = self.tree()
                with failure, mock.patch.object(installer.tempfile, "mkdtemp") as staging:
                    with self.assertRaises(installer.PreflightError):
                        installer.install(self.app)
                staging.assert_not_called()
                self.assertEqual(self.tree(), before)

    def test_replacement_failure_rolls_back_before_and_after_rename(self):
        for after_rename in (False, True):
            with self.subTest(after_rename=after_rename):
                before = self.tree()
                with self.fail_ux_replace(after_rename=after_rename):
                    with self.assertRaises(installer.InstallError) as caught:
                        installer.install(self.app)
                self.assertEqual(caught.exception.code, "INSTALL_ROLLED_BACK")
                self.assertTrue(caught.exception.safe_to_start)
                self.assertEqual(self.tree(), before)
        self.execv.assert_not_called()

    def test_main_success_executes_only_original_startup(self):
        before_startup = (self.app / "startup.py").read_bytes()
        result, events = self.run_main()
        self.assertEqual(result, 0)
        self.assertEqual(events[0]["event"], "PANEL_INSTALL_OK")
        self.assertEqual(events[0]["status"], "installed")
        self.assertEqual((self.app / "startup.py").read_bytes(), before_startup)
        self.assert_original_startup_exec()

    def test_main_safe_rollback_falls_back_to_preserved_original_startup(self):
        before = self.tree()
        with self.fail_ux_replace(after_rename=True):
            result, events = self.run_main()
        self.assertEqual(result, 0)
        self.assertEqual(events[0]["event"], "PANEL_INSTALL_FAILED")
        self.assertEqual(events[0]["reason"], "INSTALL_ROLLED_BACK")
        self.assertTrue(events[0]["normal_startup"])
        self.assertEqual(self.tree(), before)
        self.assert_original_startup_exec()

    def test_main_refuses_to_exec_after_failed_rollback(self):
        with self.fail_ux_replace(after_rename=True, fail_rollback=True):
            result, events = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(events[0]["event"], "PANEL_INSTALL_FAILED")
        self.assertEqual(events[0]["reason"], "ROLLBACK_FAILED")
        self.assertFalse(events[0]["normal_startup"])
        self.execv.assert_not_called()
        for name in ("bot.py", "step_signal_bridge.py", "startup.py", "signals.db"):
            self.assertEqual((self.app / name).read_bytes(), self.original[name])

    def test_main_unknown_error_does_not_assume_startup_is_safe(self):
        with mock.patch.object(installer, "install", side_effect=RuntimeError("unclassified")):
            result, events = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(events[0]["reason"], "UNEXPECTED_FAILURE")
        self.assertFalse(events[0]["normal_startup"])
        self.execv.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
