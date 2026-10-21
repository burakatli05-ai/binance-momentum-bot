#!/usr/bin/env python3
"""Install only the reviewed STEP Telegram surface into the pinned existing build.

No download, database, exchange, Telegram or environment mutation is performed.
All source verification and isolated imports finish before filesystem staging.
The original startup.py remains byte-for-byte intact and owns normal startup.
"""
from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import zlib


INSTALLER_VERSION = "STEP_PANEL_INSTALL_V1"
BASE_COMMIT = "17864116d51483d8bbec23d34376fe837cf1ff4b"
CORE_SHA256 = {
    "bot.py": "6f9d6339365ac42d3fa9a25e2e6115f63a9a7c5c63830efe18d26601e4239d12",
    "step_signal_bridge.py": "3429936c83caeb48b04ed475b4d8b18b2748d7616d760043ed37d6c4dde1df5b",
}
BASE_SHA256 = {
    "startup.py": "2da57131c324ad5edb012b5152abeda2f1f0ac02df1f219af1ed6657ae3457ba",
    "telegram_ux.py": "33288ae641d2d89151c66aefb666a88108084e8d787a2d2392158a5330ec161a",
}
PAYLOAD_SHA256 = {
    "telegram_ux.py": "fb1cdcebafb2db26471af2b7766490730d34e5f3fc78cf06c015c623ccc05e6a",
    "step_lock_panel.py": "8647913e7906701bf600641ac8192ab6213ea459acdd2f61d44fafa71c92d7b0",
    "step_lock_control.py": "b6bf4a8617b01436588ba4af47828e318adb2ff446fdc3fe895d155422be1da4",
}
PAYLOAD_SIZES = {"telegram_ux.py": 10599, "step_lock_panel.py": 36454,
                 "step_lock_control.py": 27325}
INSTALL_ORDER = ("step_lock_control.py", "step_lock_panel.py", "telegram_ux.py")
MAX_SOURCE_BYTES = 5_000_000

# BEGIN EMBEDDED PAYLOADS
PAYLOAD_B64 = {
    'telegram_ux.py': (
        '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'
    ),
    'step_lock_panel.py': (
        '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'
        'hPer2X2r+ew+hOO33h/ofmOW6c/7/zb/bJI='
    ),
    'step_lock_control.py': (
        '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'
    ),
}
# END EMBEDDED PAYLOADS


class InstallError(RuntimeError):
    def __init__(self, code, *, safe_to_start=True):
        # Only internally assigned short codes are ever printed, never exceptions.
        self.code = code
        self.safe_to_start = safe_to_start
        super().__init__(code)


class PreflightError(InstallError):
    pass


@dataclass(frozen=True)
class FileSnapshot:
    data: bytes | None
    mode: int = 0o644


@dataclass(frozen=True)
class InstallResult:
    status: str
    changed: tuple[str, ...]
    source_hashes: dict[str, str]


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _hash_matches(data, expected):
    """Accept only the same bytes or their exact LF/CRLF representations."""
    lf = data.replace(b"\r\n", b"\n")
    return expected in {_sha(data), _sha(lf), _sha(lf.replace(b"\n", b"\r\n"))}


def decode_payloads():
    if set(PAYLOAD_B64) != set(INSTALL_ORDER):
        raise PreflightError("EMBEDDED_PAYLOAD_SET_INVALID")
    payloads = {}
    for name in INSTALL_ORDER:
        try:
            compressed = base64.b64decode(PAYLOAD_B64[name], validate=True)
            decoder = zlib.decompressobj()
            content = decoder.decompress(compressed, PAYLOAD_SIZES[name] + 1)
            if (len(content) != PAYLOAD_SIZES[name] or not decoder.eof
                    or decoder.unused_data or decoder.unconsumed_tail
                    or _sha(content) != PAYLOAD_SHA256[name]):
                raise ValueError("payload identity")
            compile(content, name, "exec", dont_inherit=True)
        except Exception:
            raise PreflightError("EMBEDDED_PAYLOAD_INVALID:" + name) from None
        payloads[name] = content
    return payloads


def _read_source(path, *, required=True):
    try:
        info = path.lstat()
    except FileNotFoundError:
        if required:
            raise PreflightError("SOURCE_MISSING:" + path.name) from None
        return FileSnapshot(None)
    except OSError:
        raise PreflightError("SOURCE_UNREADABLE:" + path.name) from None
    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_SOURCE_BYTES:
        raise PreflightError("SOURCE_NOT_REGULAR:" + path.name)
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(fd, "rb") as stream:
            current = os.fstat(stream.fileno())
            if (not stat.S_ISREG(current.st_mode) or current.st_size > MAX_SOURCE_BYTES
                    or current.st_ino != info.st_ino or current.st_dev != info.st_dev):
                raise PreflightError("SOURCE_CHANGED:" + path.name)
            data = stream.read(MAX_SOURCE_BYTES + 1)
    except OSError:
        raise PreflightError("SOURCE_UNREADABLE:" + path.name) from None
    if len(data) > MAX_SOURCE_BYTES:
        raise PreflightError("SOURCE_TOO_LARGE:" + path.name)
    return FileSnapshot(data, stat.S_IMODE(current.st_mode))


def _assert_unchanged(app_dir, before):
    for name, original in before.items():
        current = _read_source(app_dir / name, required=original.data is not None)
        if current != original:
            raise PreflightError("SOURCE_CHANGED:" + name)


# The child reads sources only. It imports the three exact embedded modules from
# memory, checks their existing local dependencies, and forbids bot/startup import,
# filesystem mutation, database opens and network operations. -B prevents pyc writes.
_IMPORT_CHECK = r'''
import base64, importlib, importlib.abc, importlib.util, json, os, sys
payloads = {name[:-3]: base64.b64decode(value) for name, value in json.load(sys.stdin).items()}

def audit(event, args):
    if event.startswith("socket.") or event in {
        "sqlite3.connect", "subprocess.Popen", "os.system", "os.exec", "os.fork",
        "os.remove", "os.rename", "os.mkdir", "os.rmdir", "os.link", "os.symlink",
        "os.chmod", "os.chown", "os.truncate", "os.utime", "os.putenv", "os.unsetenv"
    }:
        raise RuntimeError("IMPORT_SIDE_EFFECT_BLOCKED")
    if event == "open":
        mode, flags = args[1], args[2]
        if ((isinstance(mode, str) and any(letter in mode for letter in "wax+"))
                or (isinstance(flags, int) and flags & (
                    os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND))):
            raise RuntimeError("IMPORT_WRITE_BLOCKED")

sys.addaudithook(audit)

class FrozenModules(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {"bot", "startup", "step_signal_bridge"}:
            raise ImportError("CORE_IMPORT_BLOCKED")
        if fullname in payloads:
            return importlib.util.spec_from_loader(fullname, self)
        return None
    def create_module(self, spec):
        return None
    def exec_module(self, module):
        filename = os.path.join(os.getcwd(), module.__name__ + ".py")
        module.__file__ = filename
        exec(compile(payloads[module.__name__], filename, "exec", dont_inherit=True), module.__dict__)

sys.meta_path.insert(0, FrozenModules())
control = importlib.import_module("step_lock_control")
panel = importlib.import_module("step_lock_panel")
ux = importlib.import_module("telegram_ux")
assert control.PROTOCOL == "STEP_CONTROL_V1"
assert panel.MAX_LEVERAGE == 10 and str(panel.MAX_MARGIN_USDT) == "200"
assert callable(panel.handle) and callable(panel.callback)
assert ux.normalize("\U0001fa9c STEP Lock") == "/step"
assert callable(ux.handle) and callable(ux.mode_callback)
assert not ({"bot", "startup", "step_signal_bridge"} & set(sys.modules))
print("PANEL_IMPORT_VALIDATED")
'''


def validate_imports(app_dir, payloads):
    encoded = {name: base64.b64encode(data).decode("ascii") for name, data in payloads.items()}
    try:
        completed = subprocess.run(
            [sys.executable, "-B", "-c", _IMPORT_CHECK], cwd=app_dir,
            input=json.dumps(encoded), text=True, capture_output=True, timeout=20, check=False,
        )
    except Exception:
        raise PreflightError("IMPORT_VALIDATION_FAILED") from None
    if completed.returncode != 0 or completed.stdout.strip() != "PANEL_IMPORT_VALIDATED":
        raise PreflightError("IMPORT_VALIDATION_FAILED")


def preflight(app_dir):
    try:
        app_dir = Path(app_dir).resolve(strict=True)
        if not app_dir.is_dir():
            raise OSError("not directory")
    except OSError:
        raise PreflightError("APP_DIRECTORY_INVALID") from None
    payloads = decode_payloads()
    before = {}
    for name, digest in {**CORE_SHA256, "startup.py": BASE_SHA256["startup.py"]}.items():
        before[name] = _read_source(app_dir / name)
        if not _hash_matches(before[name].data, digest):
            raise PreflightError("SOURCE_HASH_MISMATCH:" + name)
        try:
            compile(before[name].data, name, "exec", dont_inherit=True)
        except Exception:
            raise PreflightError("SOURCE_COMPILE_FAILED:" + name) from None
    for name in INSTALL_ORDER:
        before[name] = _read_source(app_dir / name, required=name == "telegram_ux.py")
        content = before[name].data
        accepted = [PAYLOAD_SHA256[name]]
        if name == "telegram_ux.py":
            accepted.append(BASE_SHA256[name])
        if content is not None and not any(_hash_matches(content, digest) for digest in accepted):
            raise PreflightError("SOURCE_HASH_MISMATCH:" + name)
    if (_hash_matches(before["telegram_ux.py"].data, PAYLOAD_SHA256["telegram_ux.py"])
            and any(before[name].data is None for name in INSTALL_ORDER if name != "telegram_ux.py")):
        # Our modules-first commit cannot create this state. The prior UX already
        # imports missing modules, so it is not a safe normal-startup fallback.
        raise PreflightError("INCOMPLETE_PRIOR_INSTALL", safe_to_start=False)
    validate_imports(app_dir, payloads)
    _assert_unchanged(app_dir, before)
    return app_dir, before, payloads


def _write_staged(path, content, mode):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(fd, "wb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
        os.fchmod(stream.fileno(), mode)


def _sync_directory(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _verify_installed(app_dir, before, payloads):
    for name, original in before.items():
        expected = FileSnapshot(payloads[name], original.mode) if name in payloads else original
        if _read_source(app_dir / name) != expected:
            raise InstallError("POST_INSTALL_VERIFICATION_FAILED:" + name)


def _rollback(app_dir, staging, before, touched):
    errors = []
    for name in reversed(touched):
        try:
            if before[name].data is None:
                (app_dir / name).unlink(missing_ok=True)
            else:
                os.replace(staging / ("old-" + name), app_dir / name)
        except Exception:
            errors.append(name)
    try:
        _assert_unchanged(app_dir, before)
        _sync_directory(app_dir)
    except Exception:
        errors.append("verification")
    return not errors


def install(app_dir):
    """Verify and install the three frozen sources. Never execute normal startup."""
    app_dir, before, payloads = preflight(app_dir)
    changed = tuple(name for name in INSTALL_ORDER if before[name].data != payloads[name])
    sources = {name: _sha(snapshot.data) for name, snapshot in before.items()
               if name not in payloads and snapshot.data is not None}
    sources.update(PAYLOAD_SHA256)
    if not changed:
        return InstallResult("already_installed", (), sources)
    staging = None
    touched = []
    keep_backups = False
    try:
        staging = Path(tempfile.mkdtemp(prefix=".step-panel-install-", dir=app_dir))
        for name in changed:
            original = before[name]
            if original.data is not None:
                _write_staged(staging / ("old-" + name), original.data, original.mode)
            _write_staged(staging / ("new-" + name), payloads[name], original.mode)
        _sync_directory(staging)
        _assert_unchanged(app_dir, before)
        for name in changed:
            # Include even a replace which raises after completing its rename.
            touched.append(name)
            os.replace(staging / ("new-" + name), app_dir / name)
        _verify_installed(app_dir, before, payloads)
        _sync_directory(app_dir)
    except BaseException as error:
        safe = not touched or _rollback(app_dir, staging, before, touched)
        keep_backups = not safe
        if not isinstance(error, Exception) and safe:
            raise
        raise InstallError("INSTALL_ROLLED_BACK" if safe else "ROLLBACK_FAILED",
                           safe_to_start=safe) from None
    finally:
        if staging is not None and not keep_backups:
            shutil.rmtree(staging, ignore_errors=True)
    return InstallResult("installed", changed, sources)


def _log(event, **fields):
    print(json.dumps({"event": event, "installer": INSTALLER_VERSION, **fields},
                     sort_keys=True, separators=(",", ":")), flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-dir", default=os.getcwd())
    args = parser.parse_args(argv)
    app_dir = Path(args.app_dir).resolve()
    try:
        result = install(app_dir)
    except InstallError as error:
        _log("PANEL_INSTALL_FAILED", reason=error.code, normal_startup=error.safe_to_start)
        if not error.safe_to_start:
            return 1
    except Exception:
        # Unknown failures are not assumed to have left a safe original tree.
        _log("PANEL_INSTALL_FAILED", reason="UNEXPECTED_FAILURE", normal_startup=False)
        return 1
    else:
        admin_present = bool(os.environ.get("TELEGRAM_ADMIN_USER_ID", "").strip()
                             and (os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "").strip()
                                  or os.environ.get("TELEGRAM_CHAT_ID", "").strip()))
        _log("PANEL_INSTALL_OK", status=result.status, changed=list(result.changed),
             base_commit=BASE_COMMIT, source_hashes=result.source_hashes,
             admin_config_present=admin_present)
    try:
        os.execv(sys.executable, [sys.executable, "-u", str(app_dir / "startup.py")])
    except OSError:
        _log("PANEL_STARTUP_EXEC_FAILED", reason="EXEC_FAILED")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
