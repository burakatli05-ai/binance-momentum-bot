"""Public H1 Early admission for the shared Premium AutoTrade engine.

Every call to ``arm_async`` already represents a Public H1 Early event: the
caller records the 2/3 continuity checkpoint, invokes this integration, and
then publishes the normal Early card.  The historical Early V2 selector is
therefore retained only as shadow/research telemetry; it no longer decides
whether the Public H1 event reaches Premium AutoTrade.

V2 selection Telegram cards are intentionally not emitted by this class.  The
normal Public Early card remains owned by ``bot.py`` and the existing Early
notification terminal.
"""

from __future__ import annotations

import asyncio
import logging

from early_autotrader_v2 import Blocked
from early_shared_execution import SharedPremiumIntegration


_LOG = logging.getLogger(__name__)


class PublicH1PremiumIntegration(SharedPremiumIntegration):
    """Dispatch every Public H1 Early while keeping V2 selector shadow-only."""

    async def arm_async(self, radar_id, symbol, m, base_score):
        """Record V2 shadow telemetry, then admit the Public H1 event directly."""
        decision_ms = self.pilot.clock()
        frozen = dict(m)
        state = self.b.get("states", {}).get(symbol)
        episode_id = getattr(state, "episode_id", None) if state is not None else None
        if self.step_lock:
            try:
                await self._shadow_io.call(
                    self.step_lock.arm,
                    radar_id,
                    symbol,
                    float(frozen["price"]),
                    decision_ms,
                    episode_id,
                )
            except Exception as exc:
                self.pilot.event(
                    "STEP_LOCK_ARM_FAILED",
                    {
                        "signal_id": str(radar_id),
                        "symbol": symbol,
                        "reason": type(exc).__name__,
                    },
                )

        try:
            score, label, reasons = self.b["ignition_shadow_score"](
                frozen, base_score
            )
            selector_features = {
                key: frozen.get(key)
                for key in (
                    "chg10",
                    "chg30",
                    "chg60",
                    "flow10",
                    "flow30",
                    "flow60",
                    "buy30",
                    "rel30",
                    "flow_eff30",
                    "dist15high_pct",
                    "spread",
                    "qv24",
                    "oi5",
                    "oi_accel5",
                    "compression_ratio",
                    "extended",
                )
            }
            qualified = bool(
                self.pilot.record_selector(
                    radar_id,
                    symbol,
                    decision_ms,
                    float(frozen["price"]),
                    base_score,
                    score,
                    label,
                    reasons,
                    selector_features,
                )
            )
            self.pilot.event(
                "SELECTOR_DECISION",
                {
                    "signal_id": str(radar_id),
                    "symbol": symbol,
                    "score": score,
                    "label": label,
                    "qualified": qualified,
                    "min_score": self.pilot.cfg.min_score,
                    "selector_role": "SHADOW_ONLY",
                    "execution_gate": "PUBLIC_H1",
                    "execution_owner": "PREMIUM_AUTOTRADE_SHARED",
                },
            )

            # No V2-selection Telegram message is generated here.  The caller
            # owns the normal Public Early card and Railway also pins
            # EARLY_V2_NOTIFY=0 as a defence-in-depth production setting.
            if not self.source_enabled:
                self.pilot.event(
                    "EARLY_SHARED_NOT_DISPATCHED",
                    {
                        "signal_id": str(radar_id),
                        "symbol": symbol,
                        "reason": "SOURCE_OFF",
                    },
                )
                return
            mode = self._shared_mode()
            if mode == "OFF":
                self.pilot.event(
                    "EARLY_SHARED_NOT_DISPATCHED",
                    {
                        "signal_id": str(radar_id),
                        "symbol": symbol,
                        "reason": "PREMIUM_AUTOTRADE_OFF",
                    },
                )
                return
            if self._session is None:
                self.pilot.event(
                    "EARLY_SHARED_NOT_DISPATCHED",
                    {
                        "signal_id": str(radar_id),
                        "symbol": symbol,
                        "reason": "SESSION_NOT_READY",
                    },
                )
                return

            dispatch_key = str(radar_id)
            if dispatch_key in self._dispatching:
                self.pilot.event(
                    "EARLY_SHARED_NOT_DISPATCHED",
                    {
                        "signal_id": dispatch_key,
                        "symbol": symbol,
                        "reason": "DISPATCH_IN_FLIGHT",
                    },
                )
                return
            handler = self.b.get("autotrade_handle_premium")
            estimator = self.b.get("estimate_trade_plan")
            if not callable(handler) or not callable(estimator):
                raise Blocked("PREMIUM_AUTOTRADE_HANDLER_MISSING")

            metrics = dict(frozen)
            metrics["entry_source"] = "EARLY_SHARED"
            metrics["early_v2_score"] = float(score)
            metrics["early_v2_label"] = str(label)
            metrics["early_selector_qualified"] = qualified
            metrics["early_decision_ms"] = int(decision_ms)
            plan = dict(estimator(symbol, metrics))
            plan["entry_source"] = "EARLY_SHARED"
            plan["early_v2_score"] = float(score)
            plan["early_selector_qualified"] = qualified

            self._dispatching.add(dispatch_key)
            self.pilot.event(
                "EARLY_SHARED_DISPATCH",
                {
                    "signal_id": dispatch_key,
                    "symbol": symbol,
                    "mode": mode,
                    "score": float(score),
                    "selector_qualified": qualified,
                    "admission": "PUBLIC_H1",
                },
            )
            try:
                await handler(self._session, radar_id, symbol, metrics, plan)
            finally:
                self._dispatching.discard(dispatch_key)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.pilot.event(
                "EARLY_SHARED_DISPATCH_FAILED",
                {
                    "signal_id": str(radar_id),
                    "symbol": symbol,
                    "reason": type(exc).__name__ + ":" + str(exc),
                },
            )
            _LOG.exception("EARLY_SHARED_DISPATCH_FAILED symbol=%s", symbol)

    def status(self):
        text = super().status()
        return (
            text.replace(
                "Kaynak: Public Early + mevcut V2 selector",
                "Kaynak: Tüm Public H1 Early (V2 selector yalnız shadow)",
            )
            .replace("Min V2 skor:", "V2 shadow eşik:")
        )

    def markup(self):
        markup = super().markup()
        for row in markup.get("inline_keyboard", []):
            for button in row:
                if button.get("text") == "Min V2 Score":
                    button["text"] = "V2 Shadow Score"
        return markup
