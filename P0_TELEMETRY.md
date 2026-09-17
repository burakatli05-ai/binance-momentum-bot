# P0 measurement instrumentation

Base: `00338485fb7ec4b7f0e4272562108c1a4ccc2771`, production deployment
`46f699b0-02a8-42c5-8d12-efe40112d6ed`. Source: 2026-09-17 Premium analysis,
snapshot `20260917T171035Z-5c1abea6`. Revalidated both logical-transfer SHA-256
hashes and all 40 table counts against its manifest, plus OFF setting and absent
AutoTrade events/trades for BTW signal 302. The original 868880384-byte source
snapshot hash is preserved in the source evidence; this implementation check
rehashed the logical transfers, not the original production database.

## Safety contract

No Premium threshold, TP/SL, risk, order, LIVE/boot setting or notification change.
The Premium handler body and evaluation function are unchanged. An async wrapper
records a chain UUID, signal ID, mode and decision timestamp. Existing entry block,
open and error events are mirrored; OFF produces `SKIP_MODE_OFF`. Remaining gates
are explicitly unevaluated on OFF, rather than hypothetical rejects. Execution
status is recorded as context: this does not add an execution gate to AutoTrade.
The existing first-failing-gate order remains authoritative. Telemetry failures
are logged and do not change a production return value or exception.

`p0_*` tables are new and created idempotently. There is no historical backfill,
destructive migration or update to legacy outcomes/features. Rollback to the base
code leaves these tables intact. Never delete the persistent `/data` volume.

## Frozen observations and ignition

`p0_feature_snapshots` joins legacy stage rows by `source_key=stage:<id>`.
Score/rank/velocity, OI change/acceleration, source/receive times, ages and stale
flags are copied at observation time. Missing or nonfinite numeric values are
JSON null, with per-field reasons. The original metrics dict is never enriched.
OI is taken only from already completed existing OI requests; no extra network
request or wait is added. A missing prior OI observation remains null. Cached OI
may be stale and must be filtered using its recorded age. Rank velocity remains
null until sufficient rank history is available.

`Experiment` in `telemetry_p0.py` is an immutable, versioned research config,
persisted in each snapshot and forward cohort. It combines report slow ignition
(chg5<1.5%, chg60>0.5%, chg30>0.2%, buy>=60%) with explicitly experimental limited
runup, relative strength and liquidity/freshness bounds. These additional numeric
bounds are priors, not validated edge. OI/rank are observational support features,
not mandatory ignition gates. Both selected and rejected CANDIDATE/EARLY rows are
retained. One ignition selection per symbol/episode is observed. The module has
no order, notifier, DRY, LIVE, Classic gate or X watcher dependency.

## New forward reference

`p0_forward` contains CLASSIC, FAST, REACQUIRE, IGNITION and denominator
CANDIDATE/EARLY observations. Runner arm occurs after the existing ALLOW decision
is persisted. `allow_decision_ts` is the observed decision time in milliseconds;
`allow_reference_price` is its trade reference. Existing anchor tables are intact.
Classic uses the new observation time, not a backdated signal timestamp.

Fill is the first eligible subsequent trade observation with a fresh post-decision
ask when available, otherwise an explicitly flagged `NEXT_TRADE_PROXY`. It is
not an actual exchange fill or a depth/size guarantee. Book event and receive
times must both follow the decision and be at most 3 seconds old. The ask is the
first applicable quote observed by the trade-tick consumer, not a reconstructed
complete book stream. Fill state is persisted immediately.

Horizons 1/5/15/30/60 minutes start at the decision, not the anchor or fill. Each
stores reference return, fill-based return/MFE/MAE and turnover-adjusted fees and
slippage. Experimental first STOP/TARGET ordering uses frozen -1%/+2% research
levels, independent of production TP/SL; observation continues after a crossing.
Ordering is observed tick order, not a guarantee of intratick execution.

At a horizon, the first terminal trade has an explicit <=5-second tolerance.
Extrema and first-passage fields do not incorporate ticks after the deadline.
Later observations produce NULL outcome fields and a missing reason, not fake
returns. Restart, missing/stale ask, event gaps (>10 seconds), late/out-of-order
events and missing horizons are separately counted in `p0_gap_events`.
Gap flags are conservative; gap=0 does not prove exchange tick completeness.
Active extrema are checkpointed every 15 seconds and immediately on fill,
first passage and horizon; recovery always flags the cohort as gapped.

## Common report

Run on a completed snapshot using a read-only SQLite connection:

```
python telemetry_report.py --db /path/to/snapshot.db --start-ms START --end-ms END --dedup episode
```

`--dedup parent` additionally collapses repeated Runner parents; `none` retains
all decisions. Selection is chronological before outcome filtering. The report
shows quality (GOOD fraction), candidate-to-decision and decision-to-peak timing,
GOOD recall proxy, non-GOOD per wall-clock hour, net forward return, MAE and gap
rate. Clean and gap-inclusive returns are separated. Empty denominators are null.
Recall is only against observed, mature, clean GOOD candidate episodes in the
same window; a capture must precede that candidate's observed peak. It is not
whole-market recall or a trading PnL claim. Missing episode links cannot establish
capture. A 60-minute maturity wait is needed before quality can be assessed.

## Verification and runtime

`test_telemetry_p0.py` covers OFF/reject audit, duplicate/slippage, failure
isolation, immutable missingness, shadow-only selection, post-ALLOW references,
five horizons, first-stop ordering, future-tick exclusion, restart/gaps,
migration idempotence, deduplication and deployed-base safety fingerprints.
New focused CI does not modify the unrelated legacy tests or their baselines.

Runtime logs: `P0_TELEMETRY_READY`, `P0_AUTOTRADE`, `P0_FORWARD_ARM`,
`P0_TELEMETRY_COUNTS` (once per minute), and `P0_TELEMETRY_ERROR` on failures.
Counts are since process start. A deployment must be followed by natural event
observations; never synthesize a Premium or enable trading just to verify audit.
