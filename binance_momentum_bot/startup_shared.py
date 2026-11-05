"""Run the verified startup path, redirecting only the final bot entrypoint."""

import faulthandler
import os

import startup


_REAL_EXECV = os.execv


def _redirect_bot_exec(executable, argv):
    args = list(argv)
    if args and args[-1] == "bot.py":
        args[-1] = "bot_shared.py"
    return _REAL_EXECV(executable, args)


if __name__ == "__main__":
    # Preserve every DB verification/export/startup invariant from startup.py.
    # Only the final Python entrypoint changes, so the existing verified startup
    # remains the single owner of restore, integrity, and boot-OFF behavior.
    startup.os.execv = _redirect_bot_exec
    faulthandler.dump_traceback_later(60, repeat=True)
    try:
        startup.main()
    finally:
        faulthandler.cancel_dump_traceback_later()
