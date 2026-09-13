# ALT research operations and review

Base: `8d9754860fb85d308fd70181951c9080979abf78` (production V5.13.5).
This change prepares one PR. No merge, production deployment, LIVE unlock or production DB operation is included.

## Models and isolation

| Model | Eligibility | Stop / target | Margin × leverage | Hold |
|---|---|---|---|---|
| ALT_WIDE_60 | Premium, candidate-to-Premium ≤45s | -3.5% / +6% |40×10 =400 USDT|60m|
| ALT_CONTROL_60 | Same | -3% / +6% |45×10 =450 USDT|60m|

Each model has independent simulated starting equity, capacity3, projected daily budget3%, and4 consecutive stops →60-minute cooldown. Projected loss = max(0,-realized daily net) + other ARMED/OPEN stop exposures + proposed stop exposure, all including fee/slippage. Winners offset daily realized losses; gross losses are not counted again. Initial balance defaults2000, frozen once. Subsequent daily starting equity adds prior model net results. Existing day rows stay frozen. All admission attempts record reason, projected risk, concurrent positions and remaining budget before the proposed position. No bot risk ledger is reused.

The worker reads new CONFIRMED DB rows after Premium processing and gets a nonblocking trade-event feed. `evaluate`, Premium public delivery, all production thresholds, 3×15s confirmation, public Early, CURRENT_TP2, stop, selector/Gate and AutoTrade handlers are unchanged. Models have no order or Telegram API. New DB operations run in a background thread; production decisions do not wait on a model outcome. Shared SQLite/storage contention still needs runtime monitoring.

On first enable, the cursor skips existing history. Restart continues its durable cursor; backlog Premiums older than30s are diagnostic skips. Stable UUID5(model,signal) and unique constraints prevent duplicates. A new signal must have measured candidate age; unknown/>45s never fills. No Candidate auto-trade. No historical result synthesis.

Ready/decision timestamps reflect worker observation, separate from Premium nominal time. Fill requires a later fresh event (≤3s receive lag), within30s of decision. Fresh ask is preferred; NEXT_TRADE_PROXY is explicit. Stop/TP geometry is based on simulated fill. Fee/slippage reuse V5.13.5 defaults0.05% and0.02% per side and are frozen at reservation, stored per trade. Costs are cash deductions, not a second shift in entry/levels. Exit is first observed trade touch (gap/slippage may exceed stop). At60m, first quote within5s gives TIME_EXIT before post-timeout TP/SL classification. No quote →UNPRICED_TIMEOUT, null P/L, model fail-closed risk until separately reviewed. Do not silently reprice/recover it.

Future30/45/60/90/120-minute observations remain available after terminal. Missing windows are null/MISSING_WINDOW. Restart/overflow gaps are flagged; first touch and MFE/MAE exclude unseen intervals. These are simulated observations, never exchange fills.

Closed minute candles provide pre-candidate3/5/10m return/run-up. Missing history stays null. Trend-age proxy is consecutive higher closed-minute closes, independent of Candidate resets; it is not actual trend onset. Post-Premium1/3/5m returns, peak over60m, givebacks1/3/5m after peak and drawdown are collected until65m. Exhaustion tags are descriptive. No early partial TP was added.

## Additive schema

- `alt_runtime`: first-enable Premium cursor, frozen initial research balance.
- `alt_daily`: independent per-model/date reconstruction, stop streak/cooldown, unknown-risk count.
- `alt_research_trades`: model/signal identity, causal timing/fill, frozen cost/geometry, risk skips, first-touch terminal, gross/net P/L, fees/slippage, excursions, gap flags.
- `alt_lifecycle_events`: durable ARM/FILL/SKIP/TP/SL/TIME_EXIT/UNPRICED_TIMEOUT events.
- `alt_forward_outcomes`: future horizon observations and missing-window status.
- `alt_premium_context`: candidate context, episode/session ordinal, previous outcome as known at decision, peaks/returns/giveback/tags.
- `alt_linked_context` view: joins signal IDs to existing Progress/Composite/OI and both liquidity references without combining frames.

Migration uses only CREATE IF NOT EXISTS for these objects, through existing init_db. No legacy row update/backfill, delete, reset, rename or drop. EARLY decision features and re-entry watch observations add keys/events to existing causal JSON/event records only. Observer schema/retry counters remain unchanged. Indexes are additive. The local test suite uses temporary DBs only; `/data/signals.db` was not accessed.

## Admin commands and accounting limits

`/altstats`: model counts, mature/current open inventory, TP/SL/time exit/unpriced, gross/net/fees/slippage, expectancy and closed-equity max drawdown, skips, age diagnostics and requested timeout buckets. Out-of-range timeout returns get an explicit outside bucket. Admission counts use decision time; P/L/mature use terminal time. Daily reports use the same definition, so a prior-day entry closing today is included.

`/daytrades`: Istanbul day, BOT/DRY internal history separate from REAL account history. GET income history discovers active symbols; GET userTrades retrieves fills, saturated windows are split, GET order cross-checks clientOrderId. Same closing order partial fills are grouped. Exact internal LIVE order/client IDs identify BOT LIVE; all other owners are UNKNOWN OWNERSHIP, not guessed manual. Non-USDT commission is not converted at an invented rate; corresponding net is UNKNOWN. API failures are unavailable, not zero.

**History limitation:** a day-window cannot prove full position lifecycle or allocate previous-day opening fees. REAL section therefore explicitly reports closing-order groups, realized P/L minus those fills' USDT fees, not complete-position net/win rate. Funding/opening fees are excluded and called out. Zero-realized one-way closes without reduceOnly/order evidence may not be classifiable. Full exchange statement reconciliation remains a separate data requirement. No fake unified DRY/REAL total is provided.

Observer forces authoritative refresh on new/changed position basis. Same-entry close/reopen entirely between polls cannot be detected from snapshots alone; exact exchange event identity would require an account stream. Existing durable notification initial+3 retries at30/60/120s is unchanged.

## Environment and safe defaults

| Variable | Default | Action |
|---|---|---|
| ALT_SHADOW_ENABLED |1|Forward-only counterfactual collection; set0 to stop worker|
| ALT_SHADOW_STARTING_BALANCE |2000|USDT per model at first activation; stored value takes precedence after restart|
| RESEARCH_EXPORT_ENABLED |0|Set1 to run repository hourly/daily worker after deployment approval|
| RESEARCH_EXPORT_DIR |DB directory/research_exports|Railway recommendation `/data/research_exports`, dedicated persistent directory|
| DRY_FEE_PCT_PER_SIDE |0.05|Existing setting, reused/frozen; percent per side|
| DRY_SLIPPAGE_PCT_PER_SIDE |0.02|Existing setting, reused/frozen; percent per side|

Retain `DB_PATH=/data/signals.db`, `AUTO_TRADE_LIVE_ALLOWED=0`, `AUTO_TRADE_BOOT_MODE=OFF`, `RESEARCH_NOTIFY=0`, `TREND_BUILDUP_NOTIFY=0`, `LIQ_V3_NOTIFY=0`. Hardcoded LIVE lock remains False regardless of env. Preserve deployment's existing X information-only and notification policy. No new API secrets needed; `/daytrades` uses existing read-authorized Binance credentials. No credentials included in reports/manifests: config identity hashes a public settings whitelist, not the environment.

## Safe exports, hourly and22:00

Integrated worker checks due state every30s. A separate lease SQLite file and atomic scheduler JSON live in export directory; source DB is never used for scheduling. Durable hourly pre-claim means at most one attempt per rolling hour, even restart; cross-process lease prevents duplicate concurrent runs. Invalid attempts still count toward hourly rate. Backups have a120s bound.

SQLite Backup API reads source with mode=ro into a new immutable snapshot directory. Run quick_check, integrity_check and isolated temporary-disk restore smoke check. Manifest includes snapshot ID/time/source, SHA256/size, version/deployment/config identity, table count/row counts/time ranges and valid boolean. Failed snapshots retain diagnostics but never replace latest valid pointer. No source repair/recover path. Same SHA records unchanged. Only a changed valid snapshot with newly mature CURRENT/DRY/ALT/causal data creates a light summary; mere new Premiums do not trigger heavy analysis. New-count watermarks include Premium count and latest terminal times.

Daily artifact `daily-YYYY-MM-DD.json` is produced once after22:00 Europe/Istanbul, with current valid snapshot cutoff and integrity explicitly included. If scheduler was down at22:00 it catches up that evening; it does not invent missed prior-day reports. To honor hourly ceiling it may use a snapshot up to an hour old. Contents: current outcomes (percent), DRY and both model P/L/costs/win-loss/DD, skip reasons/age split, causal EARLY/REENTRY percent stats, fatigue forward comparison, exhaustion context, timeout buckets and all future horizons. Percent cohorts and monetary ledgers remain separate.

`/latestexport` verifies the stored SHA and sends ZIP only to the authorized requesting admin chat, containing snapshot+manifest+latest available summary plus its own source/cutoff metadata. A summary may be older than the snapshot when no new mature outcome exists. Disk access is an alternative: `latest.json`, `latest-summary.json` and daily files. No public HTTP server. Telegram's file-size limit can reject large ZIPs; retrieve the immutable files through your authenticated volume/export tooling then. Snapshot retention is deliberately non-destructive; provision/monitor volume capacity and define retention separately before unattended use.

Standalone entrypoint from `binance_momentum_bot`:

```sh
python research_export.py --source /data/signals.db --output /data/research_exports
# Or a dedicated process with --loop (do not replace guarded bot startup).
```

One-shot can run from a separately configured scheduler; integrated worker requires no Railway cron secret. **No scheduler was enabled/deployed in Railway here. ChatGPT automatic push is not configured:** repo cannot securely deliver artifacts into this conversation without a separately authorized connector. Downloadable admin export is implemented; do not invent Railway/OpenAI tokens.

## Deploy checklist (later, explicitly approved)

1. Review/merge separately; this PR stops before merge/deploy. Verify head/base and passing CI.
2. Verify persistent volume mount and `/data/signals.db`, historical anchors/row counts, latest verified backup; keep startup/restore guards. Do not run init on a wrong path or reset history to satisfy a guard.
3. **Inspect Railway staged/pending migration-hold Start Command overrides.** Confirm the effective intended command is guarded `python startup.py` in the bot root. This PR preserves railway.toml; do not accidentally deploy a stale hold override or bypass DB preflight.
4. Keep LIVE locked and boot OFF. Keep production Premium/TP2/stop/Gate/selector/public Early and existing notification environment unchanged.
5. Review export disk capacity/retention and configure research variables above. No new secret values belong in Git.
6. Post-deploy: startup guard passes on mounted historical DB; migration twice is harmless; Premium timing/criteria unchanged; ALT ARM/FILL only on new eligible signals; two separate ledgers; no exchange order from ALT; Observer new-position leverage correct or UNKNOWN and retries survive restart.
7. Verify `/altstats`, `/daytrades` ownership/units and `/latestexport` only from admin; check manifest SHA/integrity, hourly skip/no-new path and daily22:00 artifact's snapshot cutoff.

Rollback: set ALT_SHADOW_ENABLED=0 and RESEARCH_EXPORT_ENABLED=0, then redeploy the reviewed baseline commit through existing guards if needed. Keep all new tables/artifacts for audit; older code ignores them. Never drop research tables or replace the live DB for rollback. Re-enable resumes cursors with gaps/stale candidates explicitly marked.

## Validation

Local validation: **75 tests passed,0 failed** (baseline53 +22 new), compileall passed, diff whitespace check passed. Tests cover lifecycle/restart/idempotency, frozen costs, independent risk/day-boundary/capacity/cooldown, first touch/time exit/missing data, actual Premium inbox/fill integration, causal context, leverage refresh/UNKNOWN/clean cards, account separation/ownership/foreign fees, backup validity/unchanged/no-new/daily/lease, admin access and production AST/file invariants. Existing8 strategy function hashes/25 constants remain guarded; the extended baseline pins11 functions including evaluate/public delivery/AutoTrade handler and4 untouched startup/DB baseline/Railway/X files (line-ending normalized). No live Binance/X/Telegram/Railway action is exercised. Compile and unittest commands are in existing GitHub Actions workflow. One deliberate fault-injection test logs a shadow measurement error while verifying that Premium handling continues; this is expected, not a test failure.

Endpoint references: [Binance USD-M trade API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade), [account API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account).
