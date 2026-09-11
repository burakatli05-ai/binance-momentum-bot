# V5.13.5 review / Railway notes

V5.13.4 production signal and exit rules are retained. This release fixes measurement, notification routing and DRY accounting; it adds shadow-only X and re-entry tracking. Main must not be merged or deployed without the user's next decision.

## Changes

- `binance_momentum_bot/bot.py`: additive integrations, safe notification defaults, separate Trend logging/cooldown, cost-aware DRY close and risk reserve, version/status text, hard LIVE release lock, consistent backup manifest.
- `research_v5135.py`: idempotent schema additions, actual-decision cohorts and later observation fills, per-episode Premium ordinal, shadow STOP/WATCH/reclaim/new trade/terminal accounting, backup integrity and restore inspection.
- `position_observer.py`: read-only symbolConfig leverage cache, UNKNOWN behavior, initial-margin ROE priority, persistent position identity and notification event history.
- `x_watcher.py`, `X_WATCHER_SETUP.md`: official API v2 account reader for `@chartexpt`, silent bootstrap, photo/text delivery log, conservative classification and explicit crossing watches.
- `.env.example`, `PROJECT_STATE.md`, `.gitignore`: operational defaults, strategy decisions, exclusions for credentials and local databases.
- `tests/`, `.github/workflows/tests.yml`: offline regression/integration suite and PR CI. No exchange orders, real Telegram sends or production DB required.

## Validation

Local Python 3.12, isolated dependencies from the existing requirements.txt. All 35 distinct tests passed, including a V5.13.4 schema fixture migrated twice without repricing legacy data. Syntax/compile check passes for application and tests. AST regression checks cover 8 critical production functions and 25 threshold/config definitions against `fd37b56`.

Test coverage includes fee/slippage + partial exits, frozen cost settings, duplicate close, daily ledger crash repair, day boundary/open risk, budget boundary, nonfinite risk rejection, initial/current-mid depth capture, real causal stage linking, stop/reclaim/second-trade net accounting, no-quote timeout, restart observation gaps, leverage cache failure/UNKNOWN, Observer reset/close/delivery failure, X silent bootstrap, classification, pagination failure, photos capped at two, delivery retry and one-time condition trigger.

Live API credentials and mobile app routing were not tested. No production DB was accessed. Mobile Binance deep link remains in PROJECT_STATE backlog. X commentary is clearly labeled rule-based Turkish analysis; an LLM service is not required or implied.

## Railway environment

Keep existing Telegram/Binance secrets, DB_PATH and production strategy settings. Add/set the following only after deployment is separately approved:

| Variable | Value/default | Effect |
|---|---|---|
| AUTO_TRADE_LIVE_ALLOWED | 0 | Defense in depth; this release also hard-locks LIVE in code |
| AUTO_TRADE_BOOT_MODE | OFF or existing DRY | Never LIVE |
| TREND_BUILDUP_NOTIFY | 0 | Mute Trend/REAL EARLY delivery; preserve DB events |
| LIQ_V3_NOTIFY | 0 | Mute optional V3 summary |
| RESEARCH_NOTIFY | 0 | Default for other research delivery |
| SHADOW_EXIT_NOTIFY | 0 | Explicitly override any old deployed value of 1 |
| X_WATCHER_ENABLED | 1 | Start tracking-only watcher |
| X_WATCHER_NOTIFY | 1 | Deliver new X messages and shadow triggers |
| X_WATCHER_ACCOUNTS | chartexpt | User-confirmed account |
| X_BEARER_TOKEN | secret, no default | X API read access; set only in Railway secret UI |
| X_WATCHER_POLL_SECONDS | 60 | Minimum 30 seconds |
| X_WATCH_TIMEOUT_SECONDS | 86400 | Condition watch expiry |
| DRY_FEE_PCT_PER_SIDE | 0.05 | Fee percentage on each entry/exit leg |
| DRY_SLIPPAGE_PCT_PER_SIDE | 0.02 | Cash slippage assumption per side |
| POSITION_LEVERAGE_CACHE_SECONDS | 300 | Symbol configuration TTL; minimum 30 |
| REENTRY_SHADOW_CONFIRM_SECONDS | 5 | Shadow reclaim persistence |
| REENTRY_SHADOW_TIMEOUT_SECONDS | 3600 | Shadow watch/trade timeout |

`RESEARCH_ENABLED`, `TREND_BUILDUP_ENABLED`, `LIQUIDITY_TRANSITION_V3_ENABLED` and `POSITION_OBSERVER_ENABLED` retain existing collection defaults. Do not disable these to mute notifications. Existing main signal/Premium delivery flags are unchanged.

At flat price, DRY cost is 0.14% round trip. Rates are nonnegative, finite and frozen on new trades. Actual cost uses entry plus exit turnover, including partial exits. Slippage is deducted as cash cost, preserving production price thresholds. Risk reserve is no smaller than the old AUTO_TRADE_RISK_FEE_PCT reserve. Legacy trades are clearly excluded from any claim of cost-corrected historical performance.

## DB impact / analysis

Startup adds nullable columns to six existing tables and creates `causal_cohorts`, `causal_cohort_events`, `causal_cohort_outcomes`, `premium_fatigue_shadow`, `position_observer_events`, `measurement_migrations`, and three `x_watcher_*` tables. Existing columns and history remain intact. Migration is repeatable. No UPDATE backfills past fees, P/L, timestamps or liquidity measurements.

Legacy stage rows retain nominal history; new `causal_cohort_id` points to the decision-after-feature dataset. Use `fill_time_ms > decision_time_ms`, `observation_gap=0` and explicit fill_source when selecting executable cohorts. NEXT_TRADE_PROXY is an observation, never a claimed exchange fill. No quotes at timeout produce an explicit unavailable outcome rather than fabricated P/L.

Causal outcomes use percent return on equal initial notionals; combined re-entry net is the sum of first and second net percentages. The second trade uses the first Premium cohort's stop/TP percentage distances, translated to its own fill. Trend shadow cohorts use 1% stop / 2% TP as a documented measurement policy, not production thresholds. Query parent_id for the two legs and signal_id for fatigue groups.

Legacy liquidity columns remain INITIAL_ANCHOR. `current_mid_metrics_json` is independently recomputed from the same depth response. Its TP1-dependent metrics translate the initial TP1 percentage distance to current mid; legacy wall persistence is not reinterpreted as moving-frame persistence.

Backup ZIP includes signals.db, backup_info.txt and manifest.json. The manifest hashes the completed backup, reports row counts/timestamp ranges, deployment/version/config identity and quick/integrity checks, then validates an independent restore and row counts. Source is opened read-only; no checkpoint or destructive operation is performed.

## Rollback

Before any future deployment, create and verify a consistent backup. To stop only X delivery, set X_WATCHER_NOTIFY=0 (collection continues); to stop the watcher set X_WATCHER_ENABLED=0. Research modules remain separate from real trading.

For application rollback, select the previous deployment/commit `fd37b56` with AUTO_TRADE_LIVE_ALLOWED=0 and AUTO_TRADE_BOOT_MODE=OFF, retaining additive DB tables/columns. Do not drop tables or overwrite the current database. V5.13.4 lacks the new cost model: keep DRY disabled while rolling back if any new cost-version trades remain open, so they are not closed under old accounting. Preserve the database for reconciliation. A backup restoration, if later needed, must target a separate path and be explicitly reviewed; restoring over the live DB is not part of this PR.
