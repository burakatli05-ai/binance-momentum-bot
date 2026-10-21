# STEP panel overlay for the existing Telegram bot

This release installs the reviewed STEP panel into the existing bot image. It
provides `/step` and a `🪜 STEP Lock` button in the existing `/menu` keyboard.
The first rollout is a panel and draft-settings test. It does not configure the
STEP control endpoint, arm the private execution services, or submit orders.

## Why this uses the current image

The running image includes a Premium signal relay added after the Git base
`17864116d51483d8bbec23d34376fe837cf1ff4b`. Those exact source files are not in
this checkout. Rebuilding this branch as the normal service would omit that
relay. Redeploy the known existing image with the verified start command; do not
connect this release branch as the normal service's source.

`step_panel_install.py` contains the three exact reviewed files from
`b3367dbc4422c19e3c7806e4baaf01dd8d7182be`:

| File | SHA256 |
| --- | --- |
| `step_lock_control.py` | `b6bf4a8617b01436588ba4af47828e318adb2ff446fdc3fe895d155422be1da4` |
| `step_lock_panel.py` | `8647913e7906701bf600641ac8192ab6213ea459acdd2f61d44fafa71c92d7b0` |
| `telegram_ux.py` | `fb1cdcebafb2db26471af2b7766490730d34e5f3fc78cf06c015c623ccc05e6a` |

Before writing, the installer checks the recorded current `bot.py` and
`step_signal_bridge.py` hashes, original `startup.py`, and the expected old or
already-installed panel sources. LF/CRLF equivalents are accepted. Unknown
source versions are not overwritten. It verifies the payloads and imports them
in an isolated child with network, database and mutation audit checks before
staging any application files. Existing `bot.py`, `step_signal_bridge.py` and
`startup.py` remain byte-for-byte unchanged.

It installs dependencies before the Telegram route and verifies the result.
Ordinary installation failure restores the original files before starting the
original bot. A failed rollback prevents startup. Success emits
`PANEL_INSTALL_OK` with file hashes and whether the required admin configuration
is present. No credential values are logged.

## Start command

`make_start_command.py` accepts only an immutable installer URL in this
repository and a SHA256. It prints the properly shell-quoted `python -c` command.
The launcher verifies the installer before execution and caches its verified
bytes on `/data`. Each cache read is checked again. An unavailable or invalid
download starts the existing image's unchanged `startup.py` without installing
the panel. A cache-write failure can use the verified payload in memory.

Use a service start-command change followed by a redeploy of the known existing
build. Do not request a new build from the older Git source. Railway terminal
`SUCCESS` alone is insufficient: check `PANEL_INSTALL_OK`, expected source
hashes, original startup `mode=OFF`, and `Bootstrap complete` in runtime logs.
The user then verifies a real `/step` reply in the existing Telegram chat.

## Expected behavior

- The panel requires the configured administrator chat and Telegram user.
- `/menu` refreshes the keyboard for a chat that still shows the old buttons.
- Draft margin is selectable up to 200 USDT and leverage from 1 to 10.
- Draft saves use a separate SQLite file alongside the normal `DB_PATH`.
- Without the complete STEP control configuration, live actions remain locked.
- The existing normal bot sets AutoTrade to OFF on every restart. It reloads
  existing stored live trades for reconciliation. Exchange protective orders
  remain in place, while process-managed actions pause during restart/bootstrap.

## Validation

Run the overlay-specific tests without connecting to production:

```sh
python -m unittest -v test_step_panel_install test_step_panel_bootstrap
```

The prior reviewed panel/control suite passed 64 STEP tests. Its combined
Telegram suite had one separately reproduced, pre-existing base AST expectation
mismatch; that historical test and its expected value were not changed.

This directory and its tests contain no exchange credentials, Telegram token,
private STEP execution source, or financial activation approval.
