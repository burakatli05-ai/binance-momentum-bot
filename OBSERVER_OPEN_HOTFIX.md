# LIVE opening observer hotfix (#10)

The first successfully committed account snapshot in each process is a silent
baseline. Positions present then are adopted, including positions opened while
the process was offline. Later absent-to-present, inactive-to-active and direction
reversal observations enqueue one OPEN_OBSERVED notification per instance through
the existing durable outbox. Same-direction entry-basis changes (scale-ins) do not
announce a second opening. Polling cannot detect a close/reopen entirely between
two snapshots; no exact exchange open time is invented.

The compact opening card includes the approved ownership label, side, entry,
current price, available leverage/ROE/P&L and observation time. Shared visual-image
rendering is addressed separately in #11. Existing ROE, recovery, entry and close
notifications remain intact.

Account and Telegram awaits have a 15-second deadline. Cancellation propagates.
Transient/runtime failures log the operation and exception type (no private API
payload) and allow the next poll to retry. Delivery failure does not prevent the
account snapshot from being processed. Entire position snapshots are validated
before state changes so malformed responses cannot manufacture close events.

Exactly-once refers to durable event creation; Telegram delivery retains the
existing bounded retry policy. An ambiguous network timeout after Telegram accepts
a message can still duplicate delivery because sendMessage has no idempotency key.
No production log access was used; timeout/runtime behavior is fixture-tested.

Validation: 112 tests passed (21.996s), including five new opening/restart/error
tests and all existing strategy/LIVE/startup/observer regressions. compileall passed.
bot.py, strategy thresholds, sizing/exits, LIVE=false, boot OFF, startup and research
files are unchanged. No order endpoint, dependency or DB schema change. No production
DB access, merge or deployment.
