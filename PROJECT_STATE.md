# PROJECT_STATE — V5.13.5

## Production contract

Baseline: `fd37b56`, V5.13.4. V5.13.5 is a measurement/accounting release.
LIVE is release-locked: `AUTO_TRADE_LIVE_ALLOWED=False`, irrespective of environment. Boot is OFF/DRY only. Existing real positions retain their existing protection/reconciliation behavior. No merge or deployment is authorized by this change.

Premium selection, entry/exit thresholds, stop, TP, Gate, BE and runner production behavior stay at the V5.13.4 baseline. Gate V2.1, BE/runner experiments, Liquidity/OI V3, Trend Build-up/REAL EARLY, reclaim/re-entry and repeat-Premium fatigue are research only. OI is context, never a new hard gate. X never influences a signal or AutoTrade.

## Decisions

- Trend, V3 and research notification defaults are off; DB research collection remains enabled. Trend notification cooldown does not throttle qualifying research samples or tighten scores.
- Premium, main signal and Position Observer delivery remain enabled under their existing configuration.
- X watcher: information and forward-outcome measurement only, default account `@chartexpt` (confirmed by user). Requires X API read access and a Railway secret. Missing credentials are shown explicitly; no claim of runtime activation before deployment.
- X first startup establishes a silent high-water mark. Result/target-hit posts are not new setups. Numeric crossing watches and trigger alerts are removed. Conditions are descriptive only. X has zero influence on Premium, Early, filters, scores, Gate or AutoTrade. Tweet-relative outcomes use explicit minute-open proxies at 5/15/30/60 minutes, 4 hours and 24 hours.
- Position leverage comes from cached read-only symbolConfig. Missing/expired data is UNKNOWN. positionInitialMargin remains the primary ROE denominator. Events record instance identity, source, basis, timestamps and notification outcome. FAILED notifications get at most 3 retries after the initial attempt, spaced 30/60/120 seconds, with durable attempt_count and last_attempt_time (UTC milliseconds).
- Liquidity legacy columns use INITIAL_ANCHOR. Added CURRENT_MID JSON contains independent bands/walls/barrier metrics from the same depth response. Target-dependent current-mid metrics use the original TP1 percentage distance translated to current mid; wall persistence is not inferred across moving frames.
- `causal_cohorts` is the authoritative new decision-after-features research dataset. Legacy stage cohorts are retained for compatibility and remain labeled through their causal cohort link. Never mix legacy nominal fills with causal fills.
- Causal fills use a fresh ask on a later trade event; absent a fresh ask, NEXT_TRADE_PROXY is explicit. These are simulated observations, not exchange fills. Restart gaps are flagged; exact first touch during downtime cannot be reconstructed.
- Repeat Premium ordinal is per symbol/episode. Missing episodes use an explicit six-hour fallback bucket, not the old daily ordinal. Join by signal_id to forward outcomes.
- Re-entry is STOP -> WATCH -> reclaim original entry for a configured persistence interval -> ARMED -> next observation fill -> stop/TP/timeout. It gets a separate cohort ID linked to the first trade; combined net includes both cost-bearing trades. No real order path exists in the module.
- New DRY trades freeze fee/slippage assumptions at opening. Defaults: 0.05% fee + 0.02% slippage per side (0.14% round trip at flat price). Gross P/L stays separate; net and daily ledger include costs. Slippage is a cash cost, not a production level adjustment.
- Historical rows are not silently repriced. Legacy/null cost_model_version remains legacy, including pre-upgrade open trades. No destructive DB migration or production DB access.
- Risk admission includes realized loss, open stop exposure, proposed stop exposure and estimated costs. Existing risk fee reserve remains a minimum floor.
- Backups use SQLite backup from a read-only source connection. Manifest includes SHA-256, table counts and timestamp ranges, deployment/version/config identity, integrity results and independent restore smoke check.

## Backlog / measurement limits

- Mobile Binance deep link: deferred. Existing HTTPS Futures URL retained; device/app routing cannot be reliably verified in this environment.
- X commentary is explicitly deterministic Turkish rule-based analysis, not an LLM-generated claim. Expand parser/AI only with separately reviewed evidence; multi-symbol/ambiguous text remains unscored; no conditions auto-trigger.
- X permissions, delivery to Telegram, and Railway runtime must be validated after the user approves deployment. This PR does not deploy or send test notifications to real recipients.
- Legacy research stages and gaps need filtering in analysis. No historical quote or fill time is invented.

## Change discipline

Keep strategy decisions and production/shadow status updated in this file whenever they change. Any future production Gate/BE/runner, X decision use or LIVE unlock needs an explicit separate decision and review.
