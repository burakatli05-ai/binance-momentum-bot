# Manual daily risk resume — isolated draft v1
Date: 2026-10-14 (Europe/Istanbul) | Status: NONPRODUCTION_DRAFT / NOT_DEPLOYED / NOT_LIVE_CERTIFIED

## User goal and scope
Allow the named Telegram admin to explicitly re-admit new LIVE entries after the daily loss budget becomes too small. Do NOT erase realized P/L, reset stop streaks, alter frozen exits, or change production without separate approval.

This is a candidate implementation on branch `feature/manual-daily-risk-resume-20261014` from production-pinned commit `17864116d51483d8bbec23d34376fe837cf1ff4b`; draft PR https://github.com/burakatli05-ai/binance-momentum-bot/pull/69 targets the identical pinned research branch, not main.

## Operator flow — available ONLY after a separately approved release
1. Existing `/riskstatus` or `/autotrade` displays the risk panel and `🧩 Manuel Günlük Risk`.
2. Authorized Telegram admin sends `/riskresume 20` to request **20 USDT additional** day risk allowance.
3. Telegram shows old and proposed new limits. Admin must click the one-use confirmation within 120 s. No confirmation means no state change.
4. `/riskresume off` irrevocably revokes today's grant. A second grant that day is prohibited. The budget expires on the next Istanbul calendar day.
5. The existing realized loss, daily lock marker, history, cooldown, max positions, entry slippage and risk-at-stop calculations are NOT reset.

### Candidate limits (design choice; not yet user-approved for production)
Extra budget may be granted at most once per local date, at most 50 USDT, at most 5% of frozen daily starting balance, and total configured daily limit plus extra must not exceed 10% of starting balance. Unapproved configuration switches or stale record/date/balance fail closed; DRY has zero exception.

At a 989 USDT daily starting balance and 5% base limit, old threshold=49.45 USDT; the maximum additional allowance is 49.45 USDT. A 20 USDT grant changes prospective total to 69.45 USDT. Realized P/L remains intact (example: -38.78 USDT).

## Source implementation and auditing
- New pure policy module: `binance_momentum_bot/manual_daily_risk.py`.
- Existing `bot.py`: admin and two-step Telegram control; persistence in existing `autotrade_settings`; atomic event in `autotrade_events`; effective day risk guard; panel visibility.
- New `tests/test_manual_daily_risk.py` isolated acceptance tests. No new table or schema migration.
- Authoritative `autotrade_daily.realized_net_pnl` and `locked` are **unchanged** by the grant/revocation. A sticky historical `DAILY_LOSS_*` lock may be conditionally bypassed for the exact valid same-day grant; projected exposure still must fit effective total. Other locks cannot be overridden.

## Evidence and status
- Local pure policy tests: 6/6 PASS.
- GitHub Actions new `test_manual_daily_risk.py` targeted tests: 12/12 PASS (policy + isolated SQLite + admin callback + atomic rollback).
- GitHub Actions initial whole-suite: 11 FAIL, 7 ERROR, 1 SKIP; NOT_ALL_GREEN. Specific immutable AST/base-equivalence protections intentionally detect the new code and fail. Non-scope historical/environmental failures also appear. **Do not weaken frozen reference manifests or call this release accepted.**
- Other workflow initial results: quality-shadow and runner isolation SUCCESS; LIVE capability, P0 telemetry and Early V2 pilot tests FAILED on exact-base digest assertions. They are not a release PASS.
- `bot.py` branch change, new policy module, new isolated test file only at first comparison; GitHub production branch, production deploy/runtime/config, Binance writes, account/mode, live activation: **not changed by this work**.
- Prior unrelated Parallel Step Lock/W1 remains separate; neither activation nor Stage 9/A05 was started.

## Hypotheses / falsification and unresolved gates
- H1: Atomic dual write prevents a grant from being active without an audit event. Falsification: drop the events table and verify settings rollback. **TESTED PASS** in isolated SQLite.
- H2: Denial logic still enforces projected exposure, stop cooldown, and max positions under an active exception. Falsification: inject over-budget, cooldown and maxpos states. **TESTED PASS** isolated.
- H3: Authorization and one-use date binding reject a second grant / wrong admin. Falsification: duplicate same-day grant, unauthorized callback and wrong-day grant. **TESTED PASS** isolated.
- H4: Production trade entry and risk-grant revocation are race-free. Falsification: hold entry pre-order, revoke grant concurrently, confirm no new Binance write. **NOT_RUN; live execution risk remains unproven**.
- H5: Separate worker/restarts and network failures cannot duplicate a manual grant or lose audit. Falsification: crash/process restart and SQLite fault injection across BEGIN/commit. **PARTLY_TESTED (isolated audit-table failure), broader interruption tests NOT_RUN**.
- H6: Modified bot remains fully noninterfering with archived production baseline under the newly authorized risk exception. Falsification: full additive baseline comparison with explicit new digest/negative controls. **NOT_COMPLETE — original unchanged-source assertions fail by design; no accepted amended manifest**.

## Single next action
`COMPLETE_VERSIONED_NONINTERFERENCE_ACCEPTANCE_AND_REVIEW_CANDIDATE_LIMITS_BEFORE_SEPARATE_PRODUCTION_APPROVAL`.

Do not merge, deploy, restart, change Binance settings, or enable a bypass automatically. Existing production Telegram command `/riskresume` is NOT installed until separately approved and deployed.
