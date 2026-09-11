# X Watcher setup (V5.13.5)

The repo previously had no x_watcher.py or X_WATCHER_SETUP.md. The new module reads the official X API v2 user timeline and is launched alongside existing bot loops.

Railway environment:

```dotenv
X_WATCHER_ENABLED=1
X_WATCHER_NOTIFY=1
X_WATCHER_ACCOUNTS=chartexpt
X_WATCHER_POLL_SECONDS=60
X_WATCH_TIMEOUT_SECONDS=86400
```

Set `X_BEARER_TOKEN` using Railway's secret UI, never in a committed file. API access must permit `GET /2/users/by/username/{username}` and `GET /2/users/{id}/tweets`; access availability and rate limits depend on the account. No token is bundled. No credentials were tested in this PR.

First successful timeline retrieval stores current tweets as SEEN_BOOTSTRAP and advances the account high-water mark. Only subsequent IDs are eligible for notifications. Empty accounts also become bootstrapped. Later polls paginate before advancing the cursor; failed fetches do not advance it.

Tweets retain original text and X URL; at most two photo attachments are delivered. Original text is split for Telegram's size limit. Commentary is short, rule-based Turkish, explicitly labeled. Matching is conservative: exactly one known Futures symbol. Categories: ACTIONABLE_SETUP, WATCH_SETUP, RESULT_UPDATE, MARKET_COMMENTARY, NOISE. ACTIONABLE_SETUP is a content label, not a trading command.

Supported shadow triggers: explicit numeric `breakout above`, `reclaim`, `above`, `pullback to`, `below` conditions. First observed price establishes a baseline; a later crossing generates X SHADOW TETİK. Missing/ambiguous levels are never guessed. Targets already hit and past returns cannot arm a trigger. Sampling can miss crossings between polls; this is not tick-perfect execution simulation.

Tweet text/photos and trigger delivery attempts are logged separately. Successfully acknowledged parts are not repeated. A process crash after Telegram accepts a message but before the local acknowledgement can cause a duplicate; Telegram provides no idempotency key for these calls.

`/status` shows watcher readiness/errors and notification flags. Setting X_WATCHER_NOTIFY=0 preserves collection and suppresses delivery. X has no import or callback into trade entry, filtering, or AutoTrade.
