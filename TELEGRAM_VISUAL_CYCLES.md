# Telegram visual cards and LIVE cycle history (#11)

This PR is stacked on the separate #10 observer-opening hotfix. Review/merge that
hotfix first, then retarget this PR to main. Neither PR is merged or deployed here.

## Views

`/daytradeslist` and `/positions` render dark navy PNG panels with two compact cards
per page, green profitable / red losing / blue unknown-or-flat accents, local time,
source counts and separate DRY/LIVE monetary summaries. Closed cards show entry,
exit, open/close times, side, leverage, net P/L, ROE and supported close context.
Open cards show current quantities' notional, unrealized P/L and long/short split.
DRY partial positions use the existing expected_qty remaining quantity; unavailable
remaining quantity is not replaced with original size. Missing/stale prices are not
treated as current. A missing account P/L is no longer rendered as zero.

The #10 opening event uses the same renderer through a narrow Telegram-only adapter;
its durable event identity and delivery retry mechanism remain unchanged. Other
observer event types retain their current text cards and alert rules.

`/daytradesraw` and `/positionsraw` bypass image rendering and expose the full fields
and provenance. Image generation runs off the event loop. If rendering or photo
upload fails, only that page falls back to text; previously sent pages are not
resent. PNG dimensions/bytes, caption UTF-16 length and text page lengths are bounded.
Telegram uploads use sendPhoto with the existing send lock and a network deadline.
As with the existing Telegram outbox, an ambiguous delivery timeout cannot guarantee
exactly-once reception by Telegram.

Pillow 12.3.0 is pinned as the only added dependency and is imported lazily. Missing
Pillow/font support cannot break startup; it triggers text fallback. The unmodified
Open Sans font and SIL OFL license are bundled for deterministic Turkish text across
Railway and Windows. Source icons are drawn locally; captions/raw text retain the
three exact ownership labels. No remote rendering service or account-data upload
other than the already-authorized Telegram destination is used.

## Reconstruction and limitations

This is an on-demand read-only view, not a migration/backfill or a trading ledger.
It reads current positionRisk snapshots, income for today's symbol discovery,
userTrades across local midnight, order metadata, and existing bot ledger records.
Account history looks back seven days before local midnight. API requests are
bounded to 120 per command, order metadata to 30 orders per symbol, individual
requests to 15 seconds and the complete closed-history lookup to 45 seconds.
Saturated fill windows are split and fill IDs deduplicated. Discovery or history
limits/errors are reported; unavailable LIVE totals are unknown rather than zero.

Opening inventory is inferred backward from a current position snapshot and the
complete signed fill stream. Before/after snapshots must have stable quantities;
known updates after the query cutoff cannot anchor a cycle. One-way sign flips are
split into close/open legs with proportionally allocated fees. Hedge LONG/SHORT
inventories remain separate. Partial entries/closes aggregate until inventory is
flat, including cycles spanning midnight. Decimal quantities avoid floating-point
phantom residuals. reduceOnly/closePosition and realized-P/L evidence inconsistent
with the inferred inventory invalidate reconstruction.

Cycle fields are provided only when supported: volume-weighted entry/exit prices,
first observed opening fill time, terminal closing fill time, all cycle realized
P/L less known USDT opening/closing commissions. Funding is excluded because it
cannot be reliably attributed to these position-side cycles. Non-USDT/missing fees
or missing realized P/L leave net unknown. Entry/open time/net for cycles already
open before the available history stay unknown. Incomplete/unanchored closing
records are labeled separately and do not count as complete closed cycles.

Binance fills do not supply historical leverage/margin. No current leverage is
retroactively applied to manual history, and realized P/L does not imply ROE.
Only an exactly matched bot entry order can supply existing ledger leverage/margin.
STOP/TAKE-PROFIT order types are labeled as evidence, not an invented strategy reason.
Raw output retains source provenance and fill IDs for reconstructed cycles.

MANUEL / LIVE is the display category for unmatched account activity, not proof of
human ownership. Internal UNKNOWN OWNERSHIP is retained. BOT/DRY ledger values and
the existing ownership classifier are reused; no stored order/trade is modified.
The existing `/daytrades` close-order summary is deliberately unchanged and may
differ from the new full-cycle `/daytradeslist`; the view explicitly explains this.

REST snapshots are not an atomic fill/account-history transaction. Missing exchange
history or changes entirely between snapshots cannot be proven away. Reconstruction
states its bounded coverage; it does not claim complete account lifetime history.

## Validation and safety

127 tests passed in 20.836 seconds, including 15 new cycle/render/fallback tests and
all five opening-hotfix tests plus existing downward/recovery/entry/close, startup,
restore, LIVE/boot-OFF and production/research regressions. Fixture PNGs for closed,
open and opening-notification cards were rendered and visually inspected locally.
No real Binance account or Telegram recipient was contacted by tests.

The original production baseline hashes are retained. The baseline adapter asserts
and removes exactly the new opening-card UI dispatch statement before verifying
the original bot AST; it removes only the exact Pillow dependency line before
checking the original requirements hash. Trading strategies/thresholds/sizing/exits,
AUTO_TRADE_LIVE_ALLOWED=False, default boot OFF, startup ordering and research are
unchanged. No order actions, DB writes/migrations/resets or /data creation are added.

References: [Binance account trade list](https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Account-Trade-List),
[Telegram sendPhoto](https://core.telegram.org/bots/api#sendphoto),
[Pillow fonts](https://pillow.readthedocs.io/en/stable/reference/ImageFont.html),
[Open Sans source/license](https://github.com/google/fonts/tree/main/ofl/opensans).
