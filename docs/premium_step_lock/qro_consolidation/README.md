# Premium Step Lock QRO — disabled production review candidate

Exact Premium base: `9d115152408bf971e587aab2a04d10545f748d00`.
The economic profile is unchanged (`29e695ec40d2c12b6037db8ad6cbede27da5cac0f58a01054e491b2967746cd3`).

`BinancePremiumStepLockQROExchange` binds the existing single-shot signed request
helper through `_at_qro_exchange`. `_at_step_recovery_check` dispatches only rows
whose frozen profile is `PREMIUM_STEP_LOCK_V1` and whose frozen mechanism is
`PREMIUM_QRO_1.0`. `MANAGEMENT_ENABLED=False`; LIVE entry remains independently
blocked. No environment flag, startup command or profile default is changed.
Existing Step Lock rows stay on their original read-only recovery path.

The adapter supports initial protection for a newly recorded verified entry and
later replacements. `new_trade_fields` is a new-entry transaction helper, not a
migration. The application does not create LIVE QRO entries in this candidate.
Activation/entry enablement requires a separate approved change and evidence.

## Durable protocol

Only one-way USD-M LONG/BOTH is accepted. Query the exact entry order and its fill
ledger, match frozen VWAP and quantity, then match current exchange position.
Quantity is never silently rounded down: a non-integral exchange position is
quarantined rather than leaving dust unprotected. LOT_SIZE, MARKET_LOT_SIZE,
MIN_NOTIONAL and PRICE_FILTER are checked.

Persist the intent and deterministic `QRO1-{trade_id}-{revision}` identity in
SQLite, commit an attempted marker, POST once, then query the exact client ID.
Verify symbol, side, positionSide, type, algoType, flags, quantity, target,
workingType, ownership and provider ID. Observe position and both orders before
canceling the old exact ID. Persist each cancel attempt before its single DELETE.
Re-query both orders, normal children and position before promoting the new stop.
A newer desired target remains durable while the pending revision is resolved.

The JSON journal uses compare-and-swap; another worker cannot overwrite an
intervening durable state or repeat an effect. An in-process per-trade async lock
serializes the normal worker. A crash before POST conservatively consumes that
intent: restart queries it and never guesses that POST was absent.

At most two intent identities exist. Extra or unowned algo/normal orders block
all new effects. Query errors have at most three exact-ID attempts across restart;
the entire unresolved replacement has at most 80 reads and four cancel writes.
Budget exhaustion quarantines without resetting the budget. Stable successful
episodes reset their counters for subsequent new steps.

## Fill and residual handling

An algo's `actualOrderId` binds its normal child. Verify its identity, quantity,
side, reduceOnly and MARKET type, then reconcile executedQty with unique exact
userTrades. Partial fills or unexplained position mutations never resize or
create another stop. Explicit old/new partial and filled events are retained.
Two position reads surround each order observation; total observation duration
must be at most two seconds. These sequential REST observations are not atomic.

Flat positions trigger exact owned algo and normal-child cleanup, followed by
exact terminal queries and repeated zero checks. A quarantined trade still gets
bounded flat cleanup. A manual-review latch set after partial/unknown state is
preserved even after cleanup; reentry needs explicit review as well as fresh zeros.
Unknown identities, missing child/fill evidence and exhausted budgets remain blocked.

The same-symbol LIVE Premium entry veto checks historical QRO rows, including
closed rows. Disabled management blocks unresolved reentry. A fresh enabled check
requires zero position, zero algo/normal orders, zero pending/unresolved state,
zero quarantine and zero residual cleanup. Other profiles with no QRO history
do not call the QRO exchange adapter.

## Schema and rollback

The original 16 columns are preserved. Two nullable additive columns are added
idempotently: `qro_contract_version` is immutable after new-entry assignment;
`qro_state` contains exact intents, child links, counters, attempted effects and
quarantine. There is no UPDATE/backfill, DROP or RENAME. No production migration
has been run by this Work.

Soft rollback selects CURRENT_TP2 for new entries; open frozen trades retain
their mechanism. Code downgrade requires zero open Step Lock trades, pending
intents, owned algo/normal-child orders, unresolved IDs and quarantine. Offline
readiness evidence remains a caller-supplied snapshot, not an exchange attestation.

## Evidence and limits

The adjacent provenance file records raw hashes from the immutable QRO packages.
The local-only f43165/abcf37 candidates were semantically ported; no remote Git
identity or cherry-pick is claimed. Demo entry sizing, 10x settings and .987/.990
stop geometry were not ported. The Premium initial plan and frozen levels remain.

All validation uses fake HTTP through the production adapter and isolated SQLite.
The test harness blocks external sockets and blanks credentials. Production/Demo
requests, orders, deployments, profile activation and production DB writes are zero.
ACTUAL_TRIGGER_CERTIFIED=false; actual natural trigger remains NOT_OBSERVED;
continuity/atomic protection NOT_PROVEN; capability UNKNOWN; certificate NOT_ISSUED;
LIVE_ACTIVATION_READY=false. This candidate is only for disabled deployment review.

Official endpoint contract reference (read-only documentation, no exchange calls):
[Binance USD-M trade REST API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade).
