# Telegram UX: menu, daily views and observer recovery

Implements Issue #8. This is a separate UI/notification PR based on main
`9573df0ea22d7463e464b2b07decd2ca1ba475ac`. It does not reintroduce the reverted
Early Continuation Research implementation.

## Navigation

`/menu`, `menu`, `menü` and `/start` open the persistent reply keyboard. Its eight
buttons route to `/status`, `/positions`, `/daytrades`, `/daytradeslist`, `/top`,
`/altstats`, `/settings`, `/help`. `/daytrade` aliases `/daytrades`. Explicit slash
commands, including bot suffixes, remain usable. `/latestexport` and
`/latestexportfull` appear only under help. Account/menu commands retain the existing
admin authorization check; aliases and keyboard labels do not bypass it.

The settings panel retains its existing confirmation flow for numeric settings.
OFF/DRY mode changes now also ask for a 120-second confirmation bound to the same
admin user and chat, including direct `/autotrade off|dry` commands. Confirmation
is single-use; cancellation does not change settings. LIVE remains unavailable.
No order execution, sizing, exit or strategy implementation changes.

## Daily and open-position views

Use existing `autotrade_trades`, authenticated read-only account history and the
existing positionRisk/symbolConfig observer architecture. No new trade ledger or
historical backfill is introduced. `/positions` includes DRY bot positions and all
currently returned nonzero LIVE positions. Stale/missing DRY mark prices are not
presented as current. Unknown leverage/ROE is a dash, never guessed as 1x.

`/daytrades` shows Istanbul-day closed records, all currently open positions
(including carryovers), their combined record count per source, closed-record
realized P/L and arithmetic mean known ROE with coverage counts. DRY and LIVE money
is never added into a combined total. A failed LIVE request is explicitly unavailable,
not zero. Partial/unpriced history remains marked; a missing net P/L is not zero.

`/daytradeslist` pages today's closed records below Telegram's UTF-16 text limit.
Each row shows symbol, ownership category, direction, leverage, open/close time,
entry/exit, P/L, ROE and close reason. Available DRY ledger values are exact ledger
fields. Live partial fills remain grouped by close order as before: these are
**closing-order records, not a reconstruction of complete position cycles**.
Counts can include partial closes. Fees in currencies without conversion leave
net P/L unknown; LIVE net excludes historical opening fees and funding.

For matched bot LIVE orders, existing ledger metadata supplies opening time,
entry, leverage and the close-order P/L contribution relative to ledger margin.
For unmatched historical LIVE records, unavailable opening time, entry, leverage
and ROE display `—`. Current symbolConfig leverage is never applied retrospectively.
The live exit is the close order's fill-weighted price. An order type alone does
not establish a manual/strategy close reason; STOP/TAKE PROFIT types are labeled as
order-type evidence. These limitations are also stated on the cards.

User-visible source categories are only:

- 🤖 BOT / DRY
- 👤 MANUEL / LIVE
- ⚡ BOT / LIVE

`MANUEL / LIVE` is the product's category for account activity not matched to this
bot; it is not proof that a human placed an unmatched order. Other external API
clients can fall into this display category. The internal `UNKNOWN OWNERSHIP`
classification is retained unchanged and never exposed as a fourth UI category.
The UI explains this convention. No ownership classification used for trading is changed.

## Observer notifications

Existing initial downward ROE milestones and positive ROE alerts are retained.
Recovery above -20%, -10% and -5% adds `ROE_RECOVERY` events delivered via the
existing durable pending/retry outbox. A recovery arms only after an observation at
least one ROE percentage point below its threshold. A new downward alert re-arms
only after an observation at least one point above. Crossing itself is downward
`<= threshold` or upward `> threshold`; a shared 60-second per-level cooldown
prevents rapid opposite notifications. Eligibility can wait for cooldown while the
condition continues to hold. Missing ROE changes no crossing state.

Entry crossings retain the original direction-adjusted zone, price hysteresis and
confirmation interval, with an added 60-second cooldown. A confirmed transition
uses 🔴 GİRİŞ ALTINA İNDİ or 🟢 GİRİŞ ÜZERİNE ÇIKTI. As in the existing observer,
the zone is relative to profitable/adverse movement: for SHORT, profit is below
the entry price. A newly observed closed position sends 🔵 POZİSYON KAPANDI once
through the same outbox. Its last observed ROE/P/L is labeled as such; it is not
represented as exact realized closing P/L or an exact close price.

One additive/idempotent table, `position_observer_ux_state`, stores the JSON re-arm
and cooldown state under the existing durable `position_instance_id` primary key.
Restarts retain it; a new/reset position instance cannot inherit old alert state.
Existing loss-hit arrays are not cleared. Observer event/delivery tables and their
retry semantics are reused. No legacy rows are deleted or migrations rewritten.

## Research package status and scope boundary

`/reportstatus` reads existing scheduler/pointer/manifest/daily files. It shows the
daily date, latest snapshot, the daily package's own snapshot, saved DB-check result,
snapshot-change flag and readiness. It does not run an export, recheck a live DB,
modify a watermark or claim the saved integrity result is a new check. Read errors
produce an error notice.

Automatic daily-package completion notifications are deferred: adding scheduler
hooks would cross the requested research/startup boundary. The export scheduler,
research calculations, snapshot creation and startup ordering remain unchanged.

## Safety validation

The existing production AST and file baselines remain intact. A new baseline
compares the complete bot AST outside four explicitly named Telegram UI handlers
and the single new import, and hashes the untouched application files. This covers
main/startup ordering, scanner thresholds, Premium confirmation, TP/SL/exit/sizing,
AutoTrade execution, ALT and research/export behavior. Existing LIVE hard-lock and
startup OFF regressions run alongside menu, alias, paging, ownership, day views,
bidirectional recovery, cooldown and restart tests. No production DB, live account
or Telegram recipient is accessed by tests. No merge or deployment is performed.
