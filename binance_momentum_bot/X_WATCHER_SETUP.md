# X Watcher setup (V5.13.5)

The repo previously had no x_watcher.py or X_WATCHER_SETUP.md. The new module reads the official X API v2 user timeline and is launched alongside existing bot loops.

Railway environment:

```dotenv
X_WATCHER_ENABLED=1
X_WATCHER_NOTIFY=1
X_WATCHER_ACCOUNTS=chartexpt
X_WATCHER_POLL_SECONDS=60
```

Set `X_BEARER_TOKEN` using Railway's secret UI, never in a committed file. API access must permit `GET /2/users/by/username/{username}` and `GET /2/users/{id}/tweets`; access availability and rate limits depend on the account. No token is bundled. No credentials were tested in this PR.

First successful timeline retrieval stores current tweets as SEEN_BOOTSTRAP and advances the account high-water mark. Only subsequent IDs are eligible for notifications. Empty accounts also become bootstrapped. Later polls paginate before advancing the cursor; failed fetches do not advance it.

Tweets retain original text and X URL; at most two photo attachments are delivered. Original text is split for Telegram's size limit. Commentary is short, rule-based Turkish, explicitly labeled. Matching is conservative: exactly one known Futures symbol. Categories: PREDICTION, WATCH_SETUP, RESULT_UPDATE, MARKET_COMMENTARY, NOISE. PREDICTION is a content label, not a trading command.

X is information and performance measurement only. No numeric crossing watcher, live trigger, signal, Premium, Early, filter, score, Gate or AutoTrade input exists. Conditions are descriptive text, never executable predicates. Legacy active watches are retired and pending trigger notifications cancelled idempotently; historical trigger records remain for audit.

Each tweet retains its symbol, rule-based category, UP/DOWN/UNKNOWN direction, descriptive condition, original text/link and creation timestamp. `market_context_json` is explicitly an ingestion-time snapshot (price, 5-minute/24-hour change, quote volume and trade timestamps), never passed off as tweet-time context. Legacy missing snapshots stay null. Ambiguous/missing symbols or timestamps are UNMEASURABLE.

`tweet_price` uses the Binance Futures 1-minute candle open for the minute containing tweet creation. `price_time_ms` and `price_source=BINANCE_FUTURES_1M_OPEN_PROXY` expose the approximation: up to 59.999 seconds before the tweet, not an exact tick or executable fill. Outcomes at 5/15/30/60 minutes, 4 hours and 24 hours use the same minute-open convention at each tweet-relative target. `x_forward_outcomes` stores target, sample and measurement timestamps, raw percentage change and direction-adjusted return only for unconditional directional PREDICTION posts. Conditional/result/commentary posts are never scored as successful trades. No condition activation or simulated position is created.

Missing historical data stays WAITING_DATA and is retried; current prices never replace missing history. Work is limited to 10 tweets per pass, rotated by last attempt, with a 60-second retry floor. Primary keys prevent duplicate horizons after restart. API errors do not advance measurements. Silent bootstrap tweets are measured without sending old notifications. Unknown direction and conditional outcomes require separate analysis; these measurements are not a strategy backtest or P/L.

Tweet text/photo delivery attempts are logged separately. Successfully acknowledged parts are not repeated. A process crash after Telegram accepts a message but before the local acknowledgement can cause a duplicate; Telegram provides no idempotency key for these calls.

`/status` shows watcher readiness/errors and notification flags. Setting X_WATCHER_NOTIFY=0 preserves collection and suppresses delivery. X has no import or callback into trade entry, filtering, or AutoTrade.
