# Early Continuation Research V1

These five hypotheses came from prior offline research. They are frozen forward
measurements, **not production trading rules**. Discovery strength does not imply
executable entry quality.

## Activation and safety

Collection starts silently when the existing protective launcher starts the bot.
The background worker opens only the already-existing DB with SQLite `mode=rw`.
It cannot create a missing database or `/data` directory. Initialization adds six
new tables and their indexes; it does not read historical Candidate/Trend/EARLY
records, alter legacy schemas, recompute existing flags, or run a replay/backfill.
Already-pending V1 observations can resume after a restart with an explicit gap.

There are no new environment settings, dependencies, notifications, order APIs,
or consumers of these flags in trading code. Premium confirmation, Momentum,
Entry and Rise thresholds, CURRENT_TP2/exit logic, sizing, AutoTrade, ALT shadow,
exports, LIVE hard lock, boot OFF policy, startup and Railpack behavior are retained.
No merge or deployment is part of this PR.

## Frozen predictors

All returns are percentage points; flow is a ratio. Each flag is stored separately.
Missing/nonfinite operands produce SQL NULL, not false or a made-up zero.

| Flag | Rule |
| --- | --- |
| C1 | `chg30 >= .40 AND rel30 >= .30 AND flow30 < 3.0` |
| T1 | `flow_eff30 = chg30 / max(flow30, .10)`; `flow_eff30 >= .20 AND flow30 < 3.0` |
| T2 | `-1.50 <= max_dd10 <= -.40` |
| T3 | `1 <= gainers_rank <= 30` |
| C2 | At least two **prior** same-symbol `candidate_start` events in `[decision_ts - 30m, decision_ts)` |

T1 uses decimal input strings to preserve the exact `.02 / .10 == .20` boundary.
C2 uses rolling timestamp buckets and a counter, with amortized constant-time
expiry; there is no per-event historical SQL scan. During the first 30 minutes
after start or a known queue/clock loss, fewer than two observed priors mean NULL.
Two known priors already prove C2 true even with partial history. The observed
count and history-completeness flag are stored independently. C2 is never inferred
from later repeated signals, and same-timestamp starts do not count each other.

Operands are copied through an explicit allowlist. It includes the contemporaneous
short momentum/flow, relative strength, OI/funding where present, closed-candle trend
context, rank and candidate age. Later progress, peak, gate and exhaustion labels
are excluded. Missing candle context leaves T2 NULL. Existing `max_dd10` is the
scanner's past-close drawdown measure, not future MFE or intrabar adverse excursion.

## Events, gates and linkage

Every successfully persisted new `candidate_events` transition is mirrored with
its actual source ID, including `candidate_start` and public 2/3 `early_alert`.
Actual `TREND_BUILDUP` records are mirrored separately with their research source ID.
They are the existing REAL_EARLY_WATCH stage; they are never reclassified as public
EARLY. Candidate, public EARLY, and TREND_BUILDUP all arm independent market outcomes.

Candidate preselection and Trend evaluations below notification/storage thresholds
are sampled at most once per symbol per 60 seconds. Trend capture occurs immediately
after the existing score calculation, before its score threshold return. It reuses
the already-calculated context and is independent of notification cooldown. Active
candidate, continuity, early-watch and early-notify operand snapshots are captured
at their existing evaluation points. They have no return value in production gates.

`research_gate_trace` stores names, raw values, thresholds, pass/fail, capture time,
the first failing gate, and code/config hashes. These are explicitly labeled
`SHADOW_OPERAND_SNAPSHOT`: all operands are inspected read-only, even when Python's
production boolean expression would short-circuit. They are not claims that every
subexpression executed in the original predicate. The bridge's formulas are tested
against the unchanged production predicates. Recorded transitions use
`RECORDED_TRANSITION`, preserve the original reason/note, and expose real terminal
reject/wait reasons separately. Accepted transitions are not labeled blockers.

V1 event/trend/episode IDs are UUIDs. Source episode integers are retained only as
references. Parent links require observed same-process stream continuity and are
cleared at explicit resets, source gaps, known loss and restarts. A new candidate
always starts a new research episode; it can retain a prior trend only when no
previous candidate was attached and continuity is proven. A symbol match or reused
legacy episode number is never sufficient to reconnect after restart. Unsupported
links remain NULL with `NEW_OR_UNPROVEN_BOUNDARY`.

## Timing and outcomes

Times are Unix milliseconds. `decision_ts` is the instrumentation point's clock;
`observed_ts` is the snapshot enqueue clock. `source_event_ts` and `receive_ts` refer
to the latest actual aggTrade received for that symbol at capture, not a claimed
per-operand exchange timestamp. Missing exchange `T` remains NULL even though the
legacy scanner has a separate fallback. Mixed candle/OI/market snapshots do not
provide a reliable single feature-ready or nominal scheduler timestamp, so
`ready_ts` and `nominal_ts` remain NULL. No fill is invented: this layer has no
simulated fill, and `fill_ts` remains NULL. The existing execution cohorts retain
their own timing/entry semantics.

Each armed event tracks 5/15/30/60-minute market horizons independently of any
TP/STOP, open position or existing trade table. The reference price is the detection
snapshot price, **not an executable fill**. MFE/MAE and first observed touches at
`+1/+3/+5/+10/-1/-2/-3%` use subsequently received valid aggTrades. Pre-decision,
duplicate/out-of-order and stale source ticks cannot update outcomes.

Extrema/touches for a horizon never include a tick after its deadline. A tick exactly
at the deadline is included. Close is the first fresh tick at/after the deadline,
with at most 5 seconds of lateness; the actual source and observed timestamps are
stored so this approximation is visible. Later closes are NULL. An expiry without
a tick writes a missing close, never a ticker fallback. Extrema with no observed
ticks are NULL. First-touch times are first **observed** touches, not reconstructed
crossing times. Data gaps make extrema lower-bound observations, not complete MFE.

Restarts, over-15-second source/observation gaps, missing/stale sources, queue loss
and worker backlog mark affected observations as gapped. Run heartbeats and pending
state are persisted once per second; a restored observation is always marked gapped
because the uncommitted final interval is unknowable. Event and initial pending
state persist in the same transaction. Flags/outcomes are immutable; only V1
pending state and run heartbeat advance. Completed pending rows are retired.

For comparison cohorts, report sample counts and missing/gapped rates by event
type, run/code/config and horizon. Do not pool public EARLY with TREND_BUILDUP or
treat NULL flags as false. Report observed MFE separately from executable-entry
quality and use valid closes with their measured offsets.

## Additive schema

| Table | Purpose |
| --- | --- |
| `early_continuation_flags` | Immutable events, five flags, operands, causal IDs, timing, provenance |
| `early_continuation_outcomes` | Immutable horizon market observations and touch maps |
| `early_continuation_pending` | Only V1 active observation checkpoints for restart recovery |
| `research_gate_trace` | Gate snapshots and recorded transition reasons |
| `research_runtime_gaps` | Process/source/queue/backlog/restore diagnostics |
| `early_continuation_runs` | Run identity, code/config hashes and worker heartbeat |

Primary keys and explicit indexes cover event, symbol, source and time lookups.
Initialization is idempotent. No legacy table or export semantics change.

## Performance and deliberate limits

Scanner calls copy bounded operands and enqueue into an 8,192-item nonblocking
queue. One daemon worker owns its own SQLite connection, aggregates tick extrema
in memory, and checkpoints once per second. No transaction is held while waiting
for another queue item. Only watched symbols enqueue ticks; no per-tick DB history
queries or network requests are added. SQLite lock waits are bounded to 100 ms in
the worker. An unrecoverable worker/DB error disables collection and logs an error;
stopped run heartbeats expose that interruption. The scanner continues unchanged.
Queue drops are counted and persisted as gaps rather than causing scanner waits.

A local synthetic WAL-DB exercise with 600 symbols, 100 active market observations
and 30,000 ticks over about five seconds completed with zero queue drops; enqueue
p99 was 8.3 microseconds. This is not a Railway load guarantee or a measurement of
production DB contention. No production database was used.

Deferred: exhaustive every-evaluation Trend/preselection storage (sampled instead),
precise per-operand ready/nominal and simulated-fill linkage (NULL instead), a
full-universe tick archive/replay, and transport-wide websocket disconnect causes.
V1 observes source continuity only for watched symbols; a heartbeat gap may mean no
trades, transport loss or process scheduling delay and does not identify the cause.
Gate traces cover Candidate/EARLY flow and recorded downstream blockers; they do
not instrument every operand inside OI/network/Telegram/AutoTrade internals.

The existing production AST baseline hashes are retained. The test adapter removes
only an explicit whitelist of reviewed observer statements and the named INSERT
cursor, then checks the unchanged production AST. A second baseline verifies the
entire bot module this way and hashes every other existing application file.
