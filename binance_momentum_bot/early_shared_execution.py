"""Shared Early -> Premium AutoTrade execution bridge.

The Early selector remains an independent signal source, but live execution is
owned by the already deployed Premium AutoTrade engine.  This removes the
second Early order/risk/position state machine while preserving the historical
Early V2 shadow tables and Step Lock research data.

Safety properties:
* Boot is fail-closed: the Early source is OFF after every process start.
* LIVE uses the existing Premium AutoTrade second-confirmation/account/risk
  checks; this module cannot bypass them.
* A legacy Early V2 position, if one exists, keeps the original reconciler and
  blocks the shared bridge until it is resolved.
* The bridge never changes Premium strategy gates, exits, leverage limits,
  risk locks, or exchange credentials.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import secrets
import time
from typing import Any

from early_autotrader_v2 import Blocked
from early_v2_adapter import (
    Integration as LegacyIntegration,
    early_signal_notifications_enabled,
    flag,
    set_early_signal_notifications_enabled,
)


_LOG = logging.getLogger(__name__)


class SharedPremiumIntegration(LegacyIntegration):
    """Keep Early selection, delegate all real execution to Premium AutoTrade."""

    def __init__(self, bot: dict[str, Any]):
        super().__init__(bot)
        self._session = None
        self._dispatching: set[str] = set()
        self._legacy_active = bool(self.pilot.active)
        # Source admission intentionally resets on every boot, exactly like the
        # existing Premium execution mode.  A deployment/restart therefore
        # cannot silently resume new Early entries.
        self.source_enabled = False
        if not self._legacy_active:
            try:
                self.pilot.set_mode("OFF")
            except Exception as exc:  # fail closed even if historical state is odd
                self.pilot.kill("SHARED_BRIDGE_BOOT:" + type(exc).__name__)
        else:
            self.pilot.event(
                "EARLY_SHARED_BLOCKED_LEGACY_ACTIVE",
                {"active": len(self.pilot.active)},
            )
        self.pilot.event(
            "EARLY_SHARED_BOOT_OFF",
            {
                "legacy_active": self._legacy_active,
                "premium_mode": self._shared_mode(),
            },
        )
        _LOG.info(
            "EARLY_SHARED_PREMIUM_ENGINE boot source=OFF premium_mode=%s legacy_active=%s",
            self._shared_mode(),
            int(self._legacy_active),
        )

    def _shared_cfg(self) -> dict[str, Any]:
        return self.b.get("autotrade_cfg") or {}

    def _shared_mode(self) -> str:
        return str(self._shared_cfg().get("mode", "OFF")).upper()

    def _set_source(self, enabled: bool, *, reason: str) -> None:
        if enabled and self._legacy_active:
            raise Blocked("LEGACY_EARLY_POSITION_REQUIRES_RECONCILIATION")
        self.source_enabled = bool(enabled)
        self.pilot.event(
            "EARLY_SHARED_SOURCE",
            {
                "enabled": self.source_enabled,
                "reason": str(reason),
                "premium_mode": self._shared_mode(),
            },
        )

    def _set_shared_dry(self) -> None:
        cfg = self._shared_cfg()
        current = self._shared_mode()
        if current == "LIVE":
            raise Blocked("USE_PREMIUM_AUTOTRADE_PANEL_TO_LEAVE_LIVE")
        cfg["mode"] = "DRY"
        saver = self.b.get("_at_save_setting")
        if callable(saver):
            saver("mode", "DRY")
        self._set_source(True, reason="EARLY_TERMINAL_DRY")

    async def _confirm_shared_live(
        self, session: Any, chat: str, user: str, code: str
    ) -> str:
        activate = self.b.get("_at_try_live_enable")
        if not callable(activate):
            raise Blocked("PREMIUM_LIVE_ACTIVATOR_MISSING")
        message = await activate(session, str(chat), str(user), str(code))
        if self._shared_mode() == "LIVE":
            self._set_source(True, reason="PREMIUM_LIVE_CONFIRMED")
        return str(message)

    async def run(self, session: Any) -> None:
        self._session = session
        if self._legacy_active:
            # Never orphan a position created by the old isolated engine.
            await super().run(session)
            return
        try:
            if self.step_lock:
                try:
                    await asyncio.to_thread(self._write_daily_candidate_profit_review)
                except Exception as exc:
                    _LOG.warning(
                        "EARLY_DAILY_CANDIDATE_REVIEW_FAILED %s", type(exc).__name__
                    )
                try:
                    await asyncio.to_thread(self._write_historical_profit_review)
                except Exception as exc:
                    _LOG.warning(
                        "EARLY_HISTORICAL_PROFIT_REVIEW_FAILED %s", type(exc).__name__
                    )
            while not self.b["stop_event"].is_set():
                await asyncio.to_thread(self._log_step_lock_report)
                await self._drain_notify(session)
                await asyncio.sleep(1)
        finally:
            self._session = None

    async def arm_async(self, radar_id, symbol, m, base_score):
        """Record the unchanged Early selector and dispatch to shared AutoTrade."""
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
            qualified = self.pilot.record_selector(
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
            self.pilot.event(
                "SELECTOR_DECISION",
                {
                    "signal_id": str(radar_id),
                    "symbol": symbol,
                    "score": score,
                    "label": label,
                    "qualified": bool(qualified),
                    "min_score": self.pilot.cfg.min_score,
                    "execution_owner": "PREMIUM_AUTOTRADE_SHARED",
                },
            )

            if qualified and flag("EARLY_V2_NOTIFY"):
                feature = lambda name, digits=2: (
                    "—"
                    if selector_features.get(name) is None
                    else f"{float(selector_features[name]):.{digits}f}"
                )
                reason_text = ", ".join(str(x) for x in (reasons or [])[:3]) or "FAST_EARLY_V2"
                buy30_value = selector_features.get("buy30")
                buy30_text = (
                    "—" if buy30_value is None else f"%{100 * float(buy30_value):.1f}"
                )
                self._queue_notify(
                    "selector",
                    (
                        "🧪 EARLY V2 SEÇİLDİ\n"
                        f"{symbol} | skor {float(score):.1f}/{self.pilot.cfg.min_score:g}\n"
                        f"Fiyat: {float(frozen['price']):.8g}\n"
                        f"30sn {feature('chg30')}% | 60sn {feature('chg60')}% | "
                        f"Flow30 {feature('flow30')}x | Buy30 {buy30_text}\n"
                        f"BTC relatif {feature('rel30')}%\n"
                        f"Neden: {reason_text}\n"
                        f"Motor: Premium AutoTrade {self._shared_mode()} | "
                        f"Early kaynak {'AÇIK' if self.source_enabled else 'KAPALI'}"
                    ),
                )

            if not qualified:
                return
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
            metrics["early_decision_ms"] = int(decision_ms)
            plan = dict(estimator(symbol, metrics))
            plan["entry_source"] = "EARLY_SHARED"
            plan["early_v2_score"] = float(score)

            self._dispatching.add(dispatch_key)
            self.pilot.event(
                "EARLY_SHARED_DISPATCH",
                {
                    "signal_id": dispatch_key,
                    "symbol": symbol,
                    "mode": mode,
                    "score": float(score),
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
        p = self.pilot
        selector = p.selector_report(recent_limit=0)
        cfg = self._shared_cfg()
        notifications = early_signal_notifications_enabled()
        mode = self._shared_mode()
        api_ready = bool(self.b.get("BINANCE_API_KEY") and self.b.get("BINANCE_API_SECRET"))
        live_allowed = bool(self.b.get("AUTO_TRADE_LIVE_ALLOWED"))
        risk_ready = False
        risk_reason = "unknown"
        risk_check = self.b.get("_at_risk_allowed")
        if callable(risk_check):
            try:
                risk_ready, risk_reason = risk_check("LIVE")
            except Exception as exc:
                risk_reason = type(exc).__name__
        ready = bool(api_ready and live_allowed and risk_ready and not self._legacy_active)
        nominal = float(cfg.get("trade_margin_usdt", 0) or 0) * int(
            cfg.get("leverage", 0) or 0
        )
        return (
            "⚡ EARLY TERMİNALİ — PAYLAŞIMLI MOTOR\n\n"
            f"Early bildirimleri: {'🟢 AÇIK' if notifications else '⚪ KAPALI'}\n"
            f"Early işlem kaynağı: {'🟢 AÇIK' if self.source_enabled else '⚪ KAPALI'}\n"
            f"İşlem motoru: Premium AutoTrade {mode}\n"
            f"Teminat: {float(cfg.get('trade_margin_usdt', 0) or 0):g} USDT | "
            f"kaldıraç: {int(cfg.get('leverage', 0) or 0)}x | "
            f"nominal: yaklaşık {nominal:g} USDT\n"
            f"Max ortak pozisyon: {int(cfg.get('max_open_positions', 0) or 0)} | "
            f"günlük zarar: %{float(cfg.get('daily_max_loss_pct', 0) or 0):g}\n\n"
            "Kaynak: Public Early + mevcut V2 selector\n"
            "Emir/risk/pozisyon/SL-TP/reconciliation: Premium AutoTrade ile aynı\n"
            f"Min V2 skor: {p.cfg.min_score:g}\n"
            f"Selector shadow: {selector['qualified']}/{selector['total']} seçildi | "
            f"60dk olgun: {selector['qualified_stats']['mature60']}\n"
            f"LIVE hazırlığı: {'HAZIR' if ready else 'KİLİTLİ'} | "
            f"risk: {risk_reason} | legacy Early pozisyon: {len(p.active)}\n\n"
            "LIVE ikinci onayı Premium AutoTrade hesabı ve risk kapılarıyla yapılır. "
            "Restart/deployment sonrası Early kaynak daima OFF başlar."
        )

    def markup(self):
        buttons = [
            ("Bildirimler", "notify"),
            ("Early Kaynak", "source"),
            ("Mod", "mode"),
            ("Anapara", "margin"),
            ("Kaldıraç", "leverage"),
            ("Min V2 Score", "min_score"),
            ("Max Pozisyon", "max_positions"),
            ("Risk", "risk"),
            ("Pozisyonlar", "positions"),
            ("Selector", "selector"),
            ("Early Kill", "kill"),
        ]
        return {
            "inline_keyboard": [
                [
                    {"text": label, "callback_data": "ev2:" + action}
                    for label, action in buttons[i : i + 2]
                ]
                for i in range(0, len(buttons), 2)
            ]
        }

    async def command(self, session, raw, chat, user):
        if not raw.strip():
            return False
        root = raw.split(maxsplit=1)[0].lower()
        if root == "/steplock":
            return await super().command(session, raw, chat, user)
        if root != "/earlyv2":
            return False
        if not self.b["_at_admin_allowed"](chat, user, require_user_id=True):
            await self.show(session, chat, "Yetkili kullanıcı ID gerekli.")
            return True

        parts = raw.split()[1:]
        cmd = parts[0].lower() if parts else "menu"
        try:
            message = None
            cfg = self._shared_cfg()
            if cmd in ("menu", "status"):
                pass
            elif cmd == "notify":
                enabled = not early_signal_notifications_enabled()
                set_early_signal_notifications_enabled(enabled)
                message = (
                    "Early bildirimleri açıldı."
                    if enabled
                    else "Early bildirimleri kapatıldı."
                )
            elif cmd == "source":
                await self.show(
                    session,
                    chat,
                    self.status(),
                    {
                        "inline_keyboard": [
                            [
                                {"text": "AÇ", "callback_data": "ev2:sourceon"},
                                {"text": "KAPAT", "callback_data": "ev2:sourceoff"},
                            ],
                            [{"text": "Geri", "callback_data": "ev2:menu"}],
                        ]
                    },
                )
                return True
            elif cmd == "sourceon":
                if self._shared_mode() == "OFF":
                    raise Blocked("FIRST_SELECT_DRY_OR_CONFIRM_LIVE")
                self._set_source(True, reason="USER_SOURCE_ON")
                message = "Early sinyal kaynağı paylaşımlı motora açıldı."
            elif cmd in ("sourceoff", "off", "kill"):
                self._set_source(False, reason="USER_SOURCE_OFF")
                message = (
                    "Early yeni giriş kaynağı kapatıldı. Premium AutoTrade modu ve "
                    "mevcut pozisyon yönetimi değiştirilmedi."
                )
            elif cmd == "mode":
                await self.show(
                    session,
                    chat,
                    self.status(),
                    {
                        "inline_keyboard": [
                            [
                                {"text": "EARLY OFF", "callback_data": "ev2:off"},
                                {"text": "ORTAK DRY", "callback_data": "ev2:dry"},
                                {"text": "ORTAK LIVE", "callback_data": "ev2:live"},
                            ],
                            [{"text": "Geri", "callback_data": "ev2:menu"}],
                        ]
                    },
                )
                return True
            elif cmd == "dry":
                self._set_shared_dry()
                message = "Premium AutoTrade DRY; Early kaynak açık. Gerçek emir gönderilmez."
            elif cmd == "live":
                if self._legacy_active:
                    raise Blocked("LEGACY_EARLY_POSITION_REQUIRES_RECONCILIATION")
                confirms = self.b.get("autotrade_live_confirm")
                if not isinstance(confirms, dict):
                    raise Blocked("PREMIUM_LIVE_CONFIRM_STORE_MISSING")
                code = f"{secrets.randbelow(1000000):06d}"
                confirms[str(user)] = (code, time.time() + 120)
                message = (
                    f"🔴 Ortak Premium AutoTrade LIVE onay kodu: {code}\n"
                    f"120 sn içinde /earlyv2 confirm {code}\n"
                    "Onay başarılı olursa Early kaynak da açılır."
                )
            elif cmd == "confirm" and len(parts) == 2:
                message = await self._confirm_shared_live(
                    session, str(chat), str(user), parts[1]
                )
            elif cmd == "set" and len(parts) == 3:
                key = parts[1].lower()
                value = json.loads(parts[2])
                if key == "min_score":
                    p = self.pilot
                    if p.mode != "OFF":
                        p.set_mode("OFF")
                    p.configure("min_score", value)
                else:
                    mapping = {
                        "margin": "trade_margin_usdt",
                        "leverage": "leverage",
                        "max_positions": "max_open_positions",
                    }
                    target = mapping.get(key)
                    if not target:
                        raise Blocked("UNKNOWN_SHARED_SETTING")
                    if self._shared_mode() != "OFF":
                        raise Blocked("SET_PREMIUM_AUTOTRADE_OFF_BEFORE_CONFIG_CHANGE")
                    if target in ("leverage", "max_open_positions"):
                        value = int(value)
                    else:
                        value = float(value)
                    if target == "trade_margin_usdt" and not 5 <= value <= 200:
                        raise Blocked("MARGIN_RANGE_5_200")
                    if target == "leverage" and not 1 <= value <= 10:
                        raise Blocked("LEVERAGE_RANGE_1_10")
                    if target == "max_open_positions" and not 1 <= value <= 3:
                        raise Blocked("MAX_POSITIONS_RANGE_1_3")
                    cfg[target] = value
                    self.b["_at_save_setting"](target, value)
                message = "Ayar kaydedildi."
            elif cmd in ("margin", "leverage", "max_positions", "min_score"):
                current = {
                    "margin": cfg.get("trade_margin_usdt"),
                    "leverage": cfg.get("leverage"),
                    "max_positions": cfg.get("max_open_positions"),
                    "min_score": self.pilot.cfg.min_score,
                }[cmd]
                message = (
                    f"{cmd}: {current}\n"
                    f"Değiştir: /earlyv2 set {cmd} SAYI\n"
                    "Ortak motor ayarları için önce Premium AutoTrade OFF olmalı."
                )
            elif cmd == "risk":
                message = (
                    "Risk, Premium AutoTrade ile ortaktır.\n"
                    f"Günlük zarar: %{float(cfg.get('daily_max_loss_pct', 0) or 0):g}\n"
                    f"Ardışık stop: {int(cfg.get('max_consecutive_stops', 0) or 0)}\n"
                    f"Max pozisyon: {int(cfg.get('max_open_positions', 0) or 0)}\n"
                    "Ayrı Early risk bütçesi veya ikinci pozisyon defteri yoktur."
                )
            elif cmd == "positions":
                active = self.b.get("autotrade_active_by_symbol") or {}
                lines = []
                for symbol, value in sorted(active.items()):
                    if isinstance(value, dict):
                        state = value.get("status") or value.get("state") or "ACTIVE"
                        trade_id = value.get("id") or value.get("trade_id") or ""
                        lines.append(f"{symbol} | {state} | {trade_id}")
                    else:
                        lines.append(f"{symbol} | {value}")
                message = "\n".join(lines) if lines else "Ortak AutoTrade açık pozisyonu yok."
            elif cmd == "selector":
                sr = self.pilot.selector_report(recent_limit=10)
                lines = [
                    "🧪 EARLY V2 SELECTOR SHADOW",
                    f"Toplam: {sr['total']} | seçilen: {sr['qualified']} | reddedilen: {sr['rejected']}",
                ]
                for row in sr.get("recent", [])[:10]:
                    mfe = "-" if row["mfe_pct"] is None else f"{row['mfe_pct']:+.2f}%"
                    lines.append(
                        f"{row['symbol']} score {row['v2_score']} "
                        f"{'✅' if row['qualified'] else '❌'} | MFE {mfe}"
                    )
                message = "\n".join(lines)
            elif cmd in ("execution", "profit", "report"):
                message = (
                    "Early için ayrı execution/profit motoru kaldırıldı. Emir, SL/TP, "
                    "risk, günlük kayıt ve reconciliation Premium AutoTrade panelindeki "
                    "aynı kurallarla yönetilir."
                )
            else:
                message = "Komut tanınmadı. /earlyv2 menüsünü kullanın."
            await self.show(session, chat, message)
        except (Blocked, ValueError, TypeError, KeyError) as exc:
            await self.show(session, chat, "İşlem uygulanmadı: " + str(exc))
        return True
