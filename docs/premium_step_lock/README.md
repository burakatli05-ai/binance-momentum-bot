# Premium Step Lock V1 — isolated, LIVE blocked

Source base: `29cc15ea261c0a148cd57cee46b986946b4c492f`.
Branch: `feature/premium-step-lock-v1-20261010`.

This candidate implements the frozen Decimal calculator, additive persistence,
Premium DRY entry/tick integration, profile immutability in the application API,
unknown-profile fail-closed behavior, and a transport-independent replacement
protocol. The default remains CURRENT_TP2. No Early shadow state is reused.

**This is outcome B, not a deployment-ready LIVE candidate.**
`PREMIUM_STEP_LOCK_V1_LIVE_BLOCKED_SAFE_STOP_REPLACEMENT_NOT_PROVEN`
is an unconditional code gate before any Step Lock LIVE entry network call and
also before direct LIVE trade insertion. There is no environment switch that
removes this gate. The production bot does not bind the replacement write
protocol to Binance. Do not deploy or activate on the strength of fake tests.

## Contract

First trigger 50 bp locks 25 bp; later 50 bp increments lock trigger minus 50 bp.
Maximum trigger 1000 bp, maximum lock 950 bp. No close at the cap, no partial
sale, TP1, TP2 limit, or runner. Stop prices floor to PRICE_FILTER tick size.
DRY uses an explicitly simulated entry reference; real-fill LIVE entry remains
blocked. Existing Premium plan/initial-stop calculation is unchanged.

New rows store the exact contract hash, version and immutable entry/initial-stop
references. Old rows are not backfilled. NULL legacy exit profiles read as
CURRENT_TP2; empty and unknown values fail closed. `_at_update_trade` rejects
frozen-field changes. Direct operator SQL is outside this application invariant.

Tick processing validates finite positive input, fill time, accepted step time,
and the in-process event watermark. Only a newly reached step writes state.
LIVE desired-state does no network I/O. DRY installs the simulated stop locally.
LIVE recovery rejects pre-process-start events, reads active/pending client IDs,
checks quantity/positionSide and unowned orders, alerts, and preserves protection.
Unexpected LIVE rows remain quarantined and require operator reconciliation;
automatic LIVE entry, replacement, and terminal recovery are not certified.

## Replacement protocol and capability gap

`premium_step_lock.reconcile` requires a durable `persist` callback. The model
sequence is intent commit -> POST -> client-ID query -> verified new WORKING stop
-> position recheck -> old cancel -> query both -> promote new identity. POST
uncertainty never resends blindly. Restarted pending intents only query. Newer
ticks cannot overwrite an in-flight price/client identity. Unowned orders,
mismatched fields, positions, triggered stops and uncertain cancels block progress.

The model selects closePosition STOP_MARKET semantics with zero explicit quantity
for BOTH (one-way) and LONG (hedge), matching existing Premium protection. It
does not silently substitute a quantity-based reduce-only order: that would need
separate proof of full-position overlap handling and hedge behavior.

The current official [Algo Order API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade)
documents closePosition, positionSide, client identity and order queries, but the
reviewed surface does not establish safe simultaneous full-position stop coverage
under all trigger/cancel races. The [error documentation](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/error-code)
also describes conflicts involving reduce-only orders. This is an evidence gap,
not a claim that the exchange always forbids overlap. No exchange experiment was
authorized or run. A permissive fake proves our ordering, not Binance capability.

## Tests

Use the existing runtime dependencies plus `aiohttp`, `python-dotenv` and `tzdata`.
The gate runner blocks external sockets, disables dotenv and blanks credentials.
Local loopback is allowed only for existing HTTP fixture/Windows asyncio tests.
It collects unreachable SQLite cycles before Windows temporary-directory cleanup.

```text
python tests/premium_step_lock_gate_runner.py . evidence/focused.json test_premium_step_lock test_premium_step_lock_integration test_v5135 test_tp_limit_execution test_live_capability test_telemetry_p0 test_integration
python tests/premium_step_lock_gate_runner.py . evidence/full.json
```

Set `PREMIUM_TEST_DEPS` only when dependencies live in a separate local directory.
No test was deleted. Existing AST snapshots are refreshed only for the authorized
eight changed functions and resulting module hashes. Prior hashes are retained
in the JSON snapshots, with an additional complete function identity manifest.
Unchanged base failures must remain visible; full-suite success is not assumed.

## Soft rollback and downgrade gate

The admin-only `/exitprofile CURRENT_TP2` path persists a setting and audit event,
and says it affects only new trades. Existing Step Lock DRY rows retain their
profile and finish with their own stop. The Step Lock selector is refused in
LIVE mode; LIVE entry remains blocked even if selected earlier in OFF/DRY.
Commands in this document are procedures only and were not sent to production.

Code downgrade is a hard blocker while any Step Lock row is non-CLOSED, a pending
replacement exists, exchange evidence is missing/stale, or bot-owned PSL1 stops
remain. Require mode OFF and global CURRENT_TP2 as well. Check an offline DB copy:

```text
python binance_momentum_bot/premium_step_lock_readiness.py --db offline-copy.db --exchange-snapshot exchange-orders.json
```

The caller-supplied snapshot requires `complete: true`,
`scope: "ALL_BOT_STEP_LOCK_ORDERS"`, `observed_ts_ms` no older than 60 seconds,
and an `orders` list. This tool is read-only and does not attest that supplied
exchange evidence is authentic. No evidence means unsafe; an empty list may
only be supplied after an actual complete reconciliation. A PASS is a necessary
checklist condition, never permission to deploy or activate.

The only next work for this candidate is
`RESOLVE_PREMIUM_STEP_LOCK_V1_SAFE_STOP_REPLACEMENT_CAPABILITY`.
