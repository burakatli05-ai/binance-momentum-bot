"""Create a reviewed, shell-quoted Railway command for the STEP UI overlay.

The immutable installer is verified before execution and cached on /data.
An unavailable download starts the existing image's unchanged startup.py.
This script does not deploy, contact Telegram, or enable STEP controls.
"""
from __future__ import annotations

import argparse
import re
import shlex


BOOTSTRAP = '''import hashlib, os, pathlib, sys, tempfile, urllib.request
installer_url = {url!r}
installer_sha = {sha!r}
cache = pathlib.Path("/data/step_panel_installer_" + installer_sha + ".py")
payload = None
try:
    if cache.is_file() and not cache.is_symlink() and cache.stat().st_size <= 1048576:
        candidate = cache.read_bytes()
        if hashlib.sha256(candidate).hexdigest() == installer_sha:
            payload = candidate
    if payload is None:
        request = urllib.request.Request(installer_url, headers={{"User-Agent": "STEP-panel-bootstrap/1"}})
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = response.read(1048577)
        if len(payload) > 1048576 or hashlib.sha256(payload).hexdigest() != installer_sha:
            raise ValueError("installer_integrity_failed")
        pending = None
        try:
            with tempfile.NamedTemporaryFile(dir=cache.parent, prefix="step_panel_cache_", delete=False) as handle:
                pending = handle.name
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(pending, cache)
            pending = None
        except Exception:
            print("STEP_PANEL_BOOTSTRAP_CACHE_UNAVAILABLE", flush=True)
        finally:
            if pending:
                try:
                    os.unlink(pending)
                except OSError:
                    pass
except Exception as error:
    print("STEP_PANEL_BOOTSTRAP_FAILED reason=" + type(error).__name__ + " action=original_startup", flush=True)
    os.execv(sys.executable, [sys.executable, "startup.py"])
exec(compile(payload, str(cache), "exec"), {{"__name__": "__main__", "__file__": str(cache)}})
'''


def bootstrap_code(url: str, sha: str) -> str:
    if not re.fullmatch(r"https://raw\.githubusercontent\.com/burakatli05-ai/binance-momentum-bot/[0-9a-f]{40}/deploy/step_panel/step_panel_install\.py", url):
        raise ValueError("Expected the immutable STEP installer URL")
    if not re.fullmatch(r"[0-9a-f]{64}", sha):
        raise ValueError("Expected an installer SHA256")
    return BOOTSTRAP.format(url=url, sha=sha)


def start_command(url: str, sha: str) -> str:
    return "python -c " + shlex.quote(bootstrap_code(url, sha))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--installer-url", required=True)
    parser.add_argument("--installer-sha256", required=True)
    args = parser.parse_args()
    print(start_command(args.installer_url, args.installer_sha256))
