# STEP panel rollout status — 21 October 2026

**Result: blocked by Railway's deployment pause. The Telegram panel is not yet
installed.** The prepared release remains available; no deployment is queued.

## Prepared release

- Immutable release commit: `b2d9f08df7665d256b0719e149df9aae43199cf6`.
- Installer SHA256: `c49649b5276c41eb2c8ab71e53a4acae20c88993f274e70ba783b011b6aa90a5`.
- All three embedded application files exactly match the reviewed sources.
- Installer tests: 13 passed. Bootstrap tests: 6 passed.
- The immutable raw GitHub installer was fetched after publication and matched
  the frozen local bytes exactly.

## Actual platform actions

The normal service's start command was changed to the SHA-pinned installer
launcher and read back. Source configuration and variable names remained
unchanged. A redeploy of the existing known build was then requested.

Railway rejected the request with `Deploys have been paused temporarily`,
`INVALID_ARGUMENT`, and no new deployment ID. Its response also appended a
build-copy explanation. That appended explanation has not been independently
established; the existing deployment still reports `canRedeploy: true`.

The original `python startup.py` command was restored immediately and verified
through a fresh service read. Fresh deployment history and environment health
showed the same original successful deployment, one running replica, no service
issues, no new deployment, and no staged or applying work. The attempted panel
rollout did not restart the normal bot.

No STEP control variables, exchange credentials, private execution services,
financial activation settings, or Telegram messages were changed or submitted.

## Provider evidence

At 17:08 UTC (20:08 Türkiye time), the [official Railway status page](https://status.railway.com/)
showed an active major outage. Its latest update at 16:29 UTC stated that builds
and deployments remained paused. This independently supports the pause error.

## Remaining work

After the provider resumes deployments, recheck current service identity,
configuration, pending work and build availability. Reapply the verified
launcher and request the same existing build. Do not rebuild the older Git
source, which does not include the running Premium relay.

Require the installer success marker and original bot bootstrap before calling
the panel installed. Then verify a real `/step` response in the existing
Telegram bot. `/menu` refreshes its keyboard to show the STEP Lock button.
Opening or saving this panel's draft settings does not enable live STEP orders.

The user's panel-deployment instruction remains the task authorization. It does
not authorize live financial activation. No background retry was scheduled.
