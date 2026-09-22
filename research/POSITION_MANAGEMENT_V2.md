# Position Management V2 — Dynamic Profit Lock

Status: SHADOW research implementation. Long only. Policy `pm-v2-shadow-20260922-v1`.

This module compares fixed +1% TP against a dynamic profit lock on identical Early
V1 entries and price paths. It makes no production decision. Early V1, Premium,
LIVE execution, actual TP/SL, settings, startup and deployment files are unchanged.
The implementation is a separate, explicitly invoked replay/sidecar evaluator;
it is **not installed into the production event loop or collecting live data**.
It has no exchange, notification or order capability. No auto-promotion exists.

## Ownership

Early V2 decides signal quality; Execution V2 addresses acceptable entry price;
Position Management V2 evaluates exit policies after entry. None is imported by
this module. `binance_momentum_bot/position_management_v2.py` is the pure streaming
state machine; `research/profit_lock_shadow.py` reads immutable local exports and
writes a new research output directory. Do not point the adapter at a running DB.
New forward exports can be replayed by the same evaluator after they close.

## Entry and stop contract

Use `sum(fill.price * fill.qty) / sum(fill.qty)`, with account reference, symbol,
order ID, trade ID and explicit Early allocation. Identical fill IDs deduplicate;
conflicting duplicates, wrong symbol/side/Early, multiple accounts, incomplete
allocations and executed-quantity mismatches fail validation. The caller must
export exchange-origin fills and reconcile allocation; a `source=BINANCE` label
is an input assertion, not an independent exchange verification by this module.
Do not infer ownership from nearby timestamps or reuse a later Premium fill.

The replay clock starts at `fills_available_ms`, after the complete allocated
entry is known. Pre-entry price movement cannot arm the policy. Signal time,
fill time and availability time remain distinct. Protection during the entry
fill/reconciliation interval is outside this counterfactual's scope. Existing
production protection is not modified. Scale-ins, shorts and mixed/manual
allocations are unsupported; normalize a verified single long entry or exclude.

`initial_stop_price` is the existing absolute SL supplied for that position, not
silently reset to -3% from VWAP. Both policies inherit that SL. The -3% touch
statistic is an independent research barrier, not a replacement SL. If an Early
never entered, default status is `NO_BINANCE_FILL`. Only an explicit
`--allow-signal-proxy` permits signal-price references, reported separately. The
radar snapshot adapter has no actual SL, so its -3% signal SL is explicitly named
`ASSUMED_MINUS_3_PERCENT_SIGNAL_PROXY`. It never claims that this was production SL.

## Candidate policy and confirmation

All percentages are unlevered price returns from the entry reference, before fees.

| Confirmed return | Candidate profit stop |
|---|---|
| Below +0.50% | Existing initial SL |
| At least +0.50% | +0.25%; +0.20% is a separately hashed test variant |
| At least +0.80% | +0.50% |
| At least +1.20% | Confirmed peak minus max(0.40 percentage points, confirmed peak / 3), with +0.50% floor |

At +1.20% confirmed peak, this gives +0.80%; at +3.00%, +2.00%.
These are initial research parameters, not fitted or promoted rules. Every update
is `max(previous_stop, candidate_stop)`. There is no fixed upper TP for dynamic.

An evidence block requires **both** at least three distinct consecutive aggregate
trades and 200 ms between first and last trade. The minimum return in the block
supports the confirmed peak; an isolated high wick cannot ratchet a stop.
After each completed block a new block begins. Below +0.50%, the peak evidence
resets. Consequently a step near a block boundary may take an additional block
to confirm. Evidence uses constant memory even at high tick rates.

Breaching the already active profit stop starts a separate three-trade/200 ms
confirmation. A recovery above the stop resets it. The initial SL always remains
an immediate floor, including during profit-stop confirmation. Duplicated trades
do not count; distinct IDs sharing a timestamp do count but cannot alone satisfy
200 ms. Gaps and invalid/stale/out-of-order data reset pending confirmations.
No quote/tick from after the decision is used to backdate a stop change.

Stop crossing and confirmation are not guaranteed fills: dynamic exits use the
confirming observed trade price, potentially below the stop, plus explicit exit
slippage. Initial SL uses the observed crossed price, including adverse gaps.
Fixed TP is capped at exactly +1% before exit costs, without favorable gap credit.
Raw trades are not executable bid quotes; all exits remain execution proxies.
Depth, queue position, latency impact and partial exit fills are not simulated.

## Records and comparisons

Default horizons: 60 minutes and 12 hours **from entry availability**. Both exits
are independent; prices continue to be observed after each hypothetical exit.
An unexited policy at a fully observed horizon uses the fresh final trade as an
explicit `HORIZON_MARK_PROXY`, not a claimed order fill. An incomplete horizon
stays censored; it receives no fabricated timeout price.

Each output carries Early/episode/symbol IDs, version, policy hash, reference and
fill IDs, horizon, initial/final stop, ratchet count, flags, gross/net outcomes,
MFE/MAE, raw first observed +0.5/+1/-3 touches and their grouped sequence.
Levels crossed on one trade tie. These raw touches do not require confirmation;
the profit lock does. Paths with missing data cannot prove true first-touch order.

The report groups by reference kind, horizon and policy hash. It includes every
input Early in the denominator, including missing-fill cases, and reports eligible
count, exclusions and coverage. A paired net comparison requires both policies
on the same complete path with known costs; no winner-only or closed-only subset.
Snapshot mode enumerates `radar_signals.notified=1`, not only profitable events.
Default weighting is per notification. Episode ID is retained for a later
wave/episode sensitivity analysis; this release does not claim independent waves.

Metric definitions:

- Net expectancy: arithmetic mean of paired unlevered net percentage returns.
  Exit price ratio includes declared exit slippage; entry fee is on entry notional,
  exit fee on exit notional. Funding cashflow percentages are relative to entry
  notional and included only before each policy's own exit. A positive funding
  value is a cost, a negative value is income. `funding=[]` explicitly assumes zero;
  missing funding or fees makes net economics unknown, never zero. Actual VWAP
  already includes entry slippage; do not subtract it twice. Proxy entry results
  assume entry at signal price and must not be pooled with real-fill references.
- Drawdown proxies: maximum full-horizon peak-to-trough drop in return percentage
  points; and per-policy cumulative equal-notional return drawdown ordered by
  decision time/ID. Neither is a capital-constrained portfolio drawdown. Overlaps,
  leverage, margin, capacity and liquidation are not modeled.
- Runner: full-horizon MFE at least +3% for this policy version. Runner capture is
  gross exit return / MFE, with runner N. Negative captures are retained.
- Early-exit rate: among complete runners, exit before the horizon with a subsequent
  observed return at least one percentage point above exit return. Dynamic uses
  only post-exit prices. A horizon mark is not an early exit.
- MFE/MAE: max/min return including entry zero through the common horizon, not
  truncated at the first policy exit. Empty paths produce null, not zero.

The exporter must assert complete capture. The engine additionally excludes paths
with >2s observation gaps, stale/future data, out-of-order trades, missing aggregate
trade IDs, explicit restart flags or stale horizon marks. The >2s rule is deliberately
conservative: a naturally quiet market can fail it too. Such cases require a
separate coverage analysis; they are not automatically proven packet loss.
Coarse OHLC/horizon summaries cannot establish consecutive-trade confirmation or
barrier order. Do not expand bars into invented tick paths.

## Input and use

The cohort JSONL has one object per notified Early. Example of a normalized
verified allocation (IDs and numbers below are synthetic):

```json
{"kind":"EARLY","early_id":"radar:1","symbol":"ABCUSDT","episode_id":1,"decision_ms":1800000000000,"signal_price":100,"initial_stop_price":97,"fills_complete":true,"fills_available_ms":1800000000200,"executed_qty":2,"fills":[{"source":"BINANCE","side":"BUY","early_id":"radar:1","symbol":"ABCUSDT","account_ref":"account-alias","order_id":7,"trade_id":10,"price":100.1,"qty":2,"event_ms":1800000000100}],"costs":{"entry_fee_pct":0.05,"exit_fee_pct":0.05,"exit_slippage_pct":0.02,"funding":[]}}
```

Raw aggregate trades are normalized without changing arrival order. `trade_id`
is Binance aggregate trade ID, **not** a user-fill ID. Preserve exported receive
timestamps rather than replacing them with exchange timestamps. Example:

```json
{"symbol":"ABCUSDT","trade_id":200,"price":100.6,"event_ms":1800000000300,"received_ms":1800000000310}
```

Run from the repository root against closed exports:

```text
python research/profit_lock_shadow.py --cohorts early_fills.jsonl --trades aggtrades.jsonl --observation-end-ms 1800043200200 --coverage-complete --out shadow_run_001
python research/profit_lock_shadow.py --early-snapshot analysis.db --trades aggtrades.jsonl --observation-end-ms 1800043200200 --allow-signal-proxy --out shadow_signal_proxy_001
python -m unittest discover -s tests -p test_position_management_v2.py -v
```

The SQLite adapter deliberately does not populate fees or funding. Enrich the
normalized cohort file with a documented cost model before comparing net results.
Outputs: `records.jsonl`, `summary.json`, `manifest.json` with input SHA256 values.
An existing output directory is rejected. Invalid allocations fail the run before
writing output. Identical Early duplicates deduplicate; conflicting duplicates
fail. Runs are deterministic and resumable by replaying the same immutable inputs
into a new directory. Up to 5,000 cohorts per run; partition larger files explicitly.
Open-process state is not checkpointed. Live collection and restart-aware capture
are separate integration work and are not enabled by this release.

## Validation and rollout

Tests cover VWAP, allocation and duplicate reconciliation, both first-lock variants,
second step, adaptive ratchet, high-rate confirmation, isolated wicks, stop-floor
priority, real confirming exit prices, identical-time trade IDs, first-touch ties,
post-exit runner observation, missing/stale/gapped input, cost/funding timing,
paired metrics, read-only SQLite and reproducible CLI output.

Use synthetic paths only to validate mechanics. Before inferring performance,
collect complete raw trades and explicit fill allocations, publish excluded counts,
test .20/.25 variants and conservative slippage assumptions on a held-out forward
period, and account for episode/wave overlap. This release provides no production
success threshold or automatic promotion. PR review, any forward collection hookup,
merge and deploy are separate actions; merge/deploy require user approval.
