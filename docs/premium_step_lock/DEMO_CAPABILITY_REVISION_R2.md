# Authorized cleanup revision R2

New explicit user authorization after the retained failed candidate permits this
separate revision. Parent `dbfff400460eae85e415855211e789187af181c7`; original
feature base `9d115152408bf971e587aab2a04d10545f748d00`. Neither prior evidence nor
old candidate source is rewritten. The previous BLOCKED document is historical.

Scope: query and strictly verify a known stop before deciding whether another
DELETE is permitted. If terminal, return without DELETE; if still working and a
cancel was already attempted, remain blocked. The bot state machine is untouched.
Existing 42 guards retained; three independent guards cover repeated terminal
cleanup, unresolved cancel prohibition and unknown-terminal fail-closed.

First candidate check: 45/45 offline PASS. Final focused/full reports must pass
their gates before any Demo write. A first-cycle pass is not a full certificate.

## Frozen first-cycle experiment

One BTCUSDT Demo position maximum; one-way BOTH only; isolated margin; leverage1;
minimum valid quantity from current exchange filters, notional safety ceiling
250 USDT. No existing position/order may be touched. At most 80 requests and 8
write attempts in this cycle: isolated/leverage setup if needed, one entry, two
closePosition STOP_MARKET creates, one cancel per stop, one reduce-only close.
All IDs begin DPSL-. No retry, no production fallback, no third stop.

First stop is existing Premium fixture plan 100/98.7 translated from actual
fill/VWAP; second stop is a higher below-market technical witness at fill*0.990.
These are mechanics fixtures, not changes to the frozen profit-step profile.
Both stops must be exact-query verified and simultaneously visible before old
cancel. Always attempt owned cleanup; final gate requires five zero counts.

One standard cycle is initially dispatched. A failure/rejection stops that
candidate after cleanup; no alternative semantics or parameter shopping. If it
passes, the remaining authorized standard response-loss cycles and terminal
cycle require separately frozen dispatch records within the total3+1 budget.
Actual trigger observation, if reached, will have a 60-second frozen maximum.

## Documentation limits

Current official Demo host and algo create/query/open/cancel routes verified.
The current rendered docs and official SDK omit the SELL STOP comparator and
do not give an exhaustive algoStatus enum or exact duplicate-algo error. Those
gaps remain explicitly unknown; they are not promoted to verified claims.
Only known NEW/WORKING/CANCELED etc. handling is exercised; unknown status stops.
This limited mechanics probe does not certify triggering semantics, any hedge
mode, production execution, the economic profile, or the complete adapter.

Adapter remains unbound; certificate absent; CURRENT_TP2 and LIVE hard block
unchanged. No production deployment, variables, restart or order action.
