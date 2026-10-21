"""Telegram STEP Lock drafts and explicitly scoped runtime controls.

The approved 25 USDT notional / max 5x reference is deliberately distinct
from a proposed margin x target-leverage draft.  Neither is an active order
instruction.  Controls require a fresh, validated STEP control client snapshot;
an unconfigured client leaves every operational action unavailable.  This module
never reads NORMAL account data or trading settings or sends exchange requests.
"""
import asyncio
from dataclasses import dataclass
from contextlib import closing
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import secrets
import sqlite3
import time

import step_lock_control


MAX_MARGIN_USDT = Decimal("200")
MAX_LEVERAGE = 10
MAX_NOTIONAL_USDT = Decimal("2000")
REFERENCE_NOTIONAL_USDT = Decimal("25")
REFERENCE_LEVERAGE_CAP = 5
CONFIRM_TTL_SECONDS = 120
SESSION_TTL_SECONDS = 1800
CONTROL_VIEW_TTL_SECONDS = 120
CONTROL_TIMEOUT_SECONDS = 5
MARGIN_CHOICES = (5, 10, 25, 50, 100, 150, 200)
LEVERAGE_CHOICES = tuple(range(1, 11))


class DraftUnavailable(ValueError):
    """The separate draft store cannot be safely read or written."""


def parse_margin(value):
    text = str(value).strip()
    if isinstance(value, bool) or not re.fullmatch(r"(?:0|[1-9]\d{0,2})(?:[.,]\d{1,2})?", text):
        raise ValueError("Teminat 0'dan büyük, en fazla 200 USDT ve en çok 2 ondalık basamak olmalı.")
    try:
        amount = Decimal(text.replace(",", "."))
    except InvalidOperation:
        raise ValueError("Geçerli bir USDT tutarı gir.") from None
    if not amount.is_finite() or not Decimal("0") < amount <= MAX_MARGIN_USDT:
        raise ValueError("Teminat 0'dan büyük ve en fazla 200 USDT olmalı.")
    return amount


def parse_leverage(value):
    text = str(value).strip()
    if isinstance(value, bool) or not re.fullmatch(r"(?:[1-9]|10)", text):
        raise ValueError("Kaldıraç 1 ile 10 arasında tam sayı olmalı.")
    return int(text)


def _number(value):
    return format(Decimal(str(value)).normalize(), "f")


@dataclass(frozen=True)
class Draft:
    basis: str = "legacy_notional"
    margin_usdt: str | None = None
    leverage: int | None = None

    def with_margin(self, value):
        return Draft("margin", _number(parse_margin(value)),
                     self.leverage if self.basis == "margin" else None)

    def with_leverage(self, value):
        return Draft("margin", self.margin_usdt if self.basis == "margin" else None,
                     parse_leverage(value))

    @property
    def notional_usdt(self):
        if self.basis == "legacy_notional":
            return REFERENCE_NOTIONAL_USDT
        if self.margin_usdt is None or self.leverage is None:
            return None
        return parse_margin(self.margin_usdt) * parse_leverage(self.leverage)

    def validate_ready(self):
        if self.basis == "legacy_notional":
            if self.margin_usdt is not None or self.leverage is not None:
                raise ValueError("Referans profil teminat veya hedef kaldıraç içermez.")
            return
        if self.basis != "margin":
            raise ValueError("Taslak profili geçersiz.")
        if self.margin_usdt is None or self.leverage is None:
            raise ValueError("Kaydetmeden önce teminatı ve kaldıracı ayrı ayrı seç.")
        if self.notional_usdt > MAX_NOTIONAL_USDT:
            raise ValueError("Toplam hedef pozisyon en fazla 2.000 USDT olabilir.")

    def document(self):
        self.validate_ready()
        return {"version": 1, "basis": self.basis, "margin_usdt": self.margin_usdt,
                "leverage": self.leverage}

    @classmethod
    def from_document(cls, data):
        if (not isinstance(data, dict) or set(data) != {"version", "basis", "margin_usdt", "leverage"}
                or type(data["version"]) is not int or data["version"] != 1
                or data["margin_usdt"] is not None and not isinstance(data["margin_usdt"], str)
                or data["leverage"] is not None and type(data["leverage"]) is not int):
            raise ValueError("Taslak kaydının biçimi geçersiz.")
        draft = cls(data["basis"], data["margin_usdt"], data["leverage"])
        draft.validate_ready()
        return draft


class DraftStore:
    """Only this store's single configuration table can be changed."""

    def __init__(self, path):
        self.path = Path(path)

    def load(self, chat_id, user_id):
        try:
            if not self.path.exists():
                return None
            # URI mode=ro avoids silently creating/replacing a missing database.
            with closing(sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)) as db:
                row = db.execute("SELECT payload FROM step_panel_drafts WHERE chat_id=? AND user_id=?",
                                 (str(chat_id), str(user_id))).fetchone()
            return None if row is None else Draft.from_document(json.loads(row[0]))
        except (OSError, sqlite3.Error, ValueError, TypeError):
            raise DraftUnavailable("Kayıtlı taslak okunamadı. Canlı durum doğrulanmadı; kayıt işlemi kilitli.") from None

    def save(self, chat_id, user_id, draft):
        payload = json.dumps(draft.document(), ensure_ascii=False, separators=(",", ":"))
        try:
            with closing(sqlite3.connect(self.path, timeout=2)) as db, db:
                db.execute("CREATE TABLE IF NOT EXISTS step_panel_drafts (chat_id TEXT NOT NULL, "
                           "user_id TEXT NOT NULL, payload TEXT NOT NULL, updated_ms INTEGER NOT NULL, "
                           "PRIMARY KEY (chat_id, user_id))")
                db.execute("INSERT INTO step_panel_drafts VALUES (?, ?, ?, ?) "
                           "ON CONFLICT(chat_id, user_id) DO UPDATE SET payload=excluded.payload, "
                           "updated_ms=excluded.updated_ms",
                           (str(chat_id), str(user_id), payload, int(time.time() * 1000)))
        except (OSError, sqlite3.Error):
            raise DraftUnavailable("Taslak kaydedilemedi. Seçimler canlı ayarlara uygulanmadı.") from None


@dataclass
class PanelSession:
    draft: Draft
    saved_draft: Draft | None
    revision: int
    updated_at: float
    notice: str = ""
    storage_blocked: bool = False


@dataclass(frozen=True)
class Confirmation:
    owner: tuple
    revision: int
    draft: Draft
    expires_at: float


@dataclass(frozen=True)
class ControlView:
    owner: tuple
    revision: int
    fingerprint: str
    snapshot: dict
    expires_at: float


@dataclass(frozen=True)
class ControlConfirmation:
    owner: tuple
    revision: int
    draft: Draft
    action: str
    fingerprint: str
    snapshot: dict
    request_id: str
    expires_at: float


sessions = {}
pending_confirmations = {}
control_views = {}
control_confirmations = {}


def _store_path(bot):
    source = str(bot.get("DB_PATH") or "")
    if not source or source == ":memory:" or source.startswith("file:"):
        raise DraftUnavailable("Ayrı STEP taslak dosyasının konumu doğrulanamadı.")
    try:
        normal_path = Path(source).expanduser().resolve()
        path = normal_path.with_name("step_lock_panel_drafts.sqlite3")
        if path.is_symlink():
            raise DraftUnavailable("STEP taslak dosyası bir dosya bağlantısı olamaz.")
    except OSError:
        raise DraftUnavailable("Ayrı STEP taslak dosyasının konumu okunamadı.") from None
    if path.resolve() == normal_path:
        raise DraftUnavailable("STEP taslak dosyası normal bot veritabanından ayrı olmalı.")
    return path


def _prune():
    now = time.monotonic()
    for token, pending in list(pending_confirmations.items()):
        if pending.expires_at <= now:
            pending_confirmations.pop(token, None)
    for owner, state in list(sessions.items()):
        if state.updated_at + SESSION_TTL_SECONDS <= now:
            sessions.pop(owner, None)
    for cache in (control_views, control_confirmations):
        for token, item in list(cache.items()):
            if item.expires_at <= now:
                cache.pop(token, None)


def _state(bot, chat_id, user_id):
    _prune()
    path = _store_path(bot)
    owner = (str(path), str(chat_id), str(user_id))
    if owner not in sessions:
        # A fresh generation also rejects buttons left in Telegram after a
        # process restart or session expiry; counters must not restart at zero.
        revision = secrets.randbelow((1 << 48) - 1) + 1
        try:
            saved = DraftStore(path).load(chat_id, user_id)
            sessions[owner] = PanelSession(saved or Draft(), saved, revision, time.monotonic(),
                "" if saved else "Henüz kayıtlı taslak yok; küçük test profili referans olarak gösteriliyor.")
        except DraftUnavailable as error:
            sessions[owner] = PanelSession(Draft(), None, revision, time.monotonic(), str(error), True)
    state = sessions[owner]
    state.updated_at = time.monotonic()
    return owner, state


def _invalidate(owner):
    for token, pending in list(pending_confirmations.items()):
        if pending.owner == owner:
            pending_confirmations.pop(token, None)
    _invalidate_controls(owner)


def _invalidate_controls(owner):
    for cache in (control_views, control_confirmations):
        for token, item in list(cache.items()):
            if item.owner == owner:
                cache.pop(token, None)


def _stage(owner, state, draft):
    _invalidate(owner)
    state.draft = draft
    state.revision += 1
    if not state.storage_blocked:
        state.notice = "Seçim değişti. Kaydetmek için taslak özetini onayla."


def _unavailable_snapshot(reason="Yönetim bağlantısı yapılandırılmadı."):
    return {"protocol": "STEP_CONTROL_V1", "scope_id": "UNBOUND", "runtime_id": "UNBOUND",
            "version": 0, "observed_at_ms": int(time.time() * 1000), "state": "UNAVAILABLE",
            "ready": False, "live_capable": False, "entry_enabled": False,
            "position_known": False, "position": None, "flat_confirmed": False,
            "protection_active": None, "active_profile": None, "reason": reason,
            "allowed_actions": []}


async def _read_control(bot):
    try:
        client = step_lock_control.client_for(bot)
        if client is None:
            return None, _unavailable_snapshot()
        snapshot = await asyncio.wait_for(client.status(), timeout=CONTROL_TIMEOUT_SECONDS)
        step_lock_control.validate_snapshot(snapshot)
        return client, json.loads(json.dumps(snapshot))
    except asyncio.CancelledError:
        raise
    except Exception:
        # Do not expose URLs, keys, or transport exception details in Telegram.
        return None, _unavailable_snapshot("STEP durumu doğrulanamadı. Kontrol komutları kilitli.")


def _fingerprint(snapshot):
    position = snapshot.get("position") or {}
    bound = {key: snapshot.get(key) for key in ("scope_id", "runtime_id", "version", "state", "active_profile")}
    bound["position"] = {key: position.get(key) for key in ("intent_id", "symbol", "quantity", "side")}
    return hashlib.sha256(json.dumps(bound, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _pending_entry(snapshot):
    position = (snapshot or {}).get("position")
    if not position or snapshot.get("flat_confirmed"):
        return False
    try:
        return Decimal(str(position.get("quantity"))) == 0
    except (InvalidOperation, ValueError):
        return False


def _allowed_controls(snapshot):
    if not snapshot:
        return set()
    choices = {"OFF": {"start"}, "LIVE": {"pause", "close_stop"},
               "PAUSED": {"resume", "close_stop"}}.get(snapshot.get("state"), set())
    choices = choices.intersection(snapshot.get("allowed_actions") or [])
    if not snapshot.get("ready") or not snapshot.get("live_capable"):
        choices -= {"start", "resume"}
    if not snapshot.get("live_capable") and not snapshot.get("flat_confirmed"):
        choices.discard("close_stop")
    if not snapshot.get("position_known"):
        choices -= {"start", "resume", "close_stop"}
    if snapshot.get("position") is not None or not snapshot.get("flat_confirmed"):
        choices.discard("start")
    if (snapshot.get("position") is not None and not _pending_entry(snapshot)
            and snapshot.get("protection_active") is not True):
        choices.discard("resume")
    return choices


def _control_lines(snapshot):
    if not snapshot or snapshot.get("state") == "UNAVAILABLE":
        reason = str((snapshot or {}).get("reason") or "Yönetim bağlantısı yapılandırılmadı.")[:180]
        return ("Panel: AYAR TASLAĞI · canlı komutlar KİLİTLİ\n"
                "STEP çalışma durumu: DOĞRULANAMADI\n"
                "Bu panelin canlı bağlantısı / durumu: doğrulanamadı\n"
                "Serbest bakiye: — · Pozisyon: — · Doğrulanmış stop: —\n"
                "Son canlı veri zamanı: —\n" + reason)
    labels = {"OFF": "⚪ KAPALI", "LIVE": "🟢 ÇALIŞIYOR", "PAUSED": "⏸ YENİ GİRİŞLER DURAKLATILDI",
              "CLOSING": "⏳ KAPATILIYOR", "ERROR": "🔴 HATA · DURUMU KONTROL ET"}
    state = snapshot.get("state")
    lines = ["STEP durumu: " + labels.get(state, "DOĞRULANAMADI"),
             "Yeni giriş kabulü: " + ("açık" if snapshot.get("entry_enabled") is True else "kapalı"),
             "Hesap kapsamı: " + str(snapshot.get("scope_id") or "—")[:80]]
    if not snapshot.get("live_capable"):
        lines.append("Canlı işlem yetkisi: KİLİTLİ")
    position = snapshot.get("position")
    if not snapshot.get("position_known"):
        lines.append("Pozisyon: — · doğrulanamadı")
    elif position:
        if _pending_entry(snapshot):
            lines.append("Bekleyen STEP giriş emri: " + str(position.get("symbol"))[:30] + " · henüz dolmadı")
        else:
            lines.append("STEP pozisyonu: " + str(position.get("symbol"))[:30] + " · LONG · "
                         + str(position.get("quantity"))[:30] + " adet")
    elif snapshot.get("flat_confirmed"):
        lines.append("STEP pozisyonu: açık pozisyon yok (doğrulanmış)")
    else:
        lines.append("STEP pozisyonu: kapanış doğrulaması bekleniyor")
    protection = snapshot.get("protection_active")
    lines.append("Koruma: " + ("aktif" if protection is True else "aktif değil" if protection is False else "doğrulanamadı"))
    lines.append("Serbest bakiye: — · Stop fiyatı: —")
    try:
        observed = time.strftime("%d.%m.%Y %H:%M:%S UTC", time.gmtime(snapshot["observed_at_ms"] / 1000))
    except (KeyError, TypeError, ValueError, OverflowError):
        observed = "—"
    lines.append("Son durum verisi: " + observed)
    if state == "PAUSED":
        continuation = ("mevcut işlemlerin koruması sürer" if protection is True
                        else "açık pozisyon bu işlemle kapatılmaz; koruma durumu yukarıda gösterilir")
        lines.append("Yeni sinyal kabulü durur; " + continuation + ". "
                     "Önceden kabul edilen giriş emri gerçekleşebilir.")
    elif state == "CLOSING":
        lines.append("Kapatma süreci devam ediyor. Kapanış henüz doğrulanmadı.")
    reason = str(snapshot.get("reason") or "")[:180]
    if reason:
        lines.append("Durum notu: " + reason)
    profile = snapshot.get("active_profile")
    if profile:
        try:
            lines.append("Çalışma profili:\n" + _sizing_text(Draft.from_document(profile)))
        except ValueError:
            lines.append("Çalışma profili: ayrıntılar doğrulanamadı")
    return "\n".join(lines)


def _sizing_text(draft):
    if draft.basis == "legacy_notional":
        return ("Küçük test profili: 25 USDT toplam pozisyon\n"
                "Kaldıraç üst sınırı: 5×; gerçekleşen kaldıraç okunmadı.\n"
                "Bu profilde 25 USDT teminat değildir.")
    margin = _number(draft.margin_usdt) + " USDT" if draft.margin_usdt is not None else "seçilmedi"
    leverage = str(draft.leverage) + "×" if draft.leverage is not None else "seçilmedi"
    if draft.notional_usdt is None:
        return f"Teminat: {margin}\nHedef kaldıraç: {leverage}\nToplam hedef pozisyon: seçim bekleniyor."
    return (f"Teminat: {margin}\nHedef kaldıraç: {leverage}\n"
            f"Hedef pozisyon: {margin} × {draft.leverage} = {_number(draft.notional_usdt)} USDT")


def panel_text(draft, *, saved=False, notice="", snapshot=None):
    text = ("🪜 STEP LOCK KONTROL PANELİ\n\n"
            + _control_lines(snapshot) + "\n\n"
            + ("💾 Kayıtlı taslak\n" if saved else "✏️ Kaydedilmemiş seçim\n")
            + _sizing_text(draft) + "\n\n"
            "Seçim sınırları: teminat ≤ 200 USDT · kaldıraç ≤ 10×\n"
            "Toplam hedef pozisyon ≤ 2.000 USDT\n\n"
            "Referans test limitleri (19.10.2026):\n"
            "25 USDT toplam pozisyon · en fazla 5×\n"
            "Aynı anda 1 işlem · UTC gününde 1 yeni giriş\n"
            "Gerçekleşmiş günlük net zarar 1 USDT: yeni giriş kilidi\n\n"
            "Taslak kaydı emir göndermez, borsa kaldıracını değiştirmez. "
            "Yeni ayarlar ayrı başlatma / devam onayında değerlendirilir. "
            "NORMAL bot ayarları ayrıdır.")
    if notice:
        text += "\n\nℹ️ " + notice
    return text


def _button(text, data):
    return {"text": text, "callback_data": data}


def panel_markup(draft, revision=0, can_save=True, *, snapshot=None, control_token=None, saved=False):
    try:
        draft.validate_ready()
    except ValueError:
        can_save = False
    margin = _number(draft.margin_usdt) + " USDT" if draft.margin_usdt is not None else "Teminat seç"
    leverage = str(draft.leverage) + "×" if draft.leverage is not None else "Kaldıraç seç"
    rows = [
        [_button("💰 " + margin, "slp:margin"), _button("⚡ " + leverage, "slp:leverage")],
        [_button("💾 Taslağı kaydet" if can_save else "ℹ️ Seçim / kayıt durumu",
                 f"slp:save:{revision}" if can_save else "slp:summary")],
        [_button("🧪 Küçük test profili", f"slp:legacy:{revision}"),
         _button("📋 Başlatma özeti", "slp:summary")],
    ]
    actions = _allowed_controls(snapshot)
    labels = {"start": "▶️ Canlı başlat", "pause": "⏸ Yeni girişleri duraklat",
              "resume": "▶️ Yeni girişlere devam et", "close_stop": "⛔ Pozisyonu kapat ve STEP’i durdur"}
    for action in ("start", "pause", "resume", "close_stop"):
        if action not in actions:
            continue
        if action in ("start", "resume") and (not saved or not can_save):
            rows.append([_button("💾 Önce seçili taslağı kaydet", "slp:summary")])
        else:
            data = f"slp:control:{action}:{control_token}" if control_token else "slp:controlinfo"
            rows.append([_button(labels[action], data)])
    if not actions:
        state = (snapshot or {}).get("state")
        label = "⏳ Kapanış doğrulanıyor" if state == "CLOSING" else "🔒 Canlı kontrol kilitli"
        rows.append([_button(label, "slp:controlinfo")])
    rows.append([_button("🔄 Yenile", "slp:panel"), _button("⬅️ Ana menü", "slp:menu")])
    return {"inline_keyboard": rows}


def _choices(kind, revision):
    values = MARGIN_CHOICES if kind == "m" else LEVERAGE_CHOICES
    buttons = [_button(f"{value} USDT" if kind == "m" else f"{value}×",
                       f"slp:set:{kind}:{value}:{revision}") for value in values]
    rows = [buttons[i:i + 3] for i in range(0, len(buttons), 3)]
    if kind == "m":
        rows.append([_button("✏️ Başka tutar", "slp:custom")])
    rows.append([_button("⬅️ STEP paneli", "slp:panel")])
    return {"inline_keyboard": rows}


async def _send(bot, session, chat_id, text, markup, message=None):
    if message and message.get("message_id"):
        result = await bot["telegram_api_call"](session, "editMessageText", {
            "chat_id": chat_id, "message_id": message["message_id"], "text": text, "reply_markup": markup})
        if isinstance(result, dict) and (result.get("ok") is True
                or "message is not modified" in str(result.get("description") or "").lower()):
            return
    # Telegram can reject an edit without raising. Keep a committed save and
    # deliver its current state in a fresh message rather than leaving a stale
    # confirmation/picker on screen with a silent callback acknowledgement.
    result = await bot["telegram_send"](session, text, chat_id=chat_id,
                                         reply_markup=markup, track_delivery=False)
    if result is not True and not (isinstance(result, dict) and result.get("ok") is True):
        raise ValueError("Telegram ekranı yenilenemedi. Güncel kayıt durumunu görmek için /step yaz.")


async def _show(bot, session, chat_id, state, message=None):
    _, snapshot = await _read_control(bot)
    owner = next((key for key, value in sessions.items() if value is state), None)
    token = None
    if owner is not None:
        _invalidate_controls(owner)
        token = secrets.token_hex(10)
        control_views[token] = ControlView(owner, state.revision, _fingerprint(snapshot), snapshot,
                                          time.monotonic() + CONTROL_VIEW_TTL_SECONDS)
    saved = state.saved_draft == state.draft
    await _send(bot, session, chat_id,
                panel_text(state.draft, saved=saved, notice=state.notice, snapshot=snapshot),
                panel_markup(state.draft, state.revision, not state.storage_blocked,
                             snapshot=snapshot, control_token=token, saved=saved), message)


async def _answer(bot, session, cb, text="", alert=False):
    if cb.get("id"):
        payload = {"callback_query_id": cb["id"], "text": text[:190]}
        if alert:
            payload["show_alert"] = True
        await bot["telegram_api_call"](session, "answerCallbackQuery", payload)


def _check_revision(value, state):
    if not value.isdigit() or int(value) != state.revision:
        raise ValueError("Bu seçim ekranı eskidi. STEP panelini yenileyip tekrar seç.")


def _summary(draft, snapshot=None):
    return ("📋 STEP BAŞLATMA ÖZETİ\n\n" + _sizing_text(draft) + "\n\n"
            + _control_lines(snapshot) + "\n\n"
            "200 USDT ve 10× seçimi, 2.000 USDT hedef pozisyondur.\n"
            "Diğer test limitleri otomatik büyümez.\n\n"
            "Canlı kullanım için STEP hesabı, bakiye, sembolün gerçek kaldıracı, "
            "miktar/fiyat adımları, minimum işlem tutarı ve koruyucu stop yolu "
            "doğrulanmalıdır. Uygun olmayan hedef otomatik büyütülmez.\n"
            "Yönetim bağlantısı ve limit profili doğrulanmadan canlı komut kullanılamaz.\n\n"
            "Duraklat: yeni sinyal kabulünü durdurur; mevcut işlemlerin koruması sürer.\n"
            "Kapat ve durdur: ayrı onayla yalnız STEP pozisyonunun kapanışını başlatır.\n"
            "Kapanış doğrulanana kadar durum KAPATILIYOR olarak gösterilir.")


def _control_profile(state, action, snapshot):
    if action not in ("start", "resume"):
        return None
    if state.storage_blocked or state.saved_draft != state.draft:
        raise ValueError("Başlatmadan veya devam etmeden önce seçili taslağı kaydet.")
    profile = state.draft.document()
    if (action == "resume" and snapshot.get("position") is not None
            and snapshot.get("active_profile") != profile):
        raise ValueError("Açık STEP pozisyonu varken farklı profille devam edilemez. Çalışma profilini kontrol et.")
    return profile


def _control_review(action, state, snapshot):
    titles = {"start": "▶️ STEP CANLI BAŞLATMA ONAYI", "resume": "▶️ STEP DEVAM ONAYI",
              "close_stop": "⛔ STEP POZİSYONUNU KAPAT VE DURDUR"}
    lines = [titles[action], "", "Hesap kapsamı: " + str(snapshot["scope_id"])[:80]]
    if action in ("start", "resume"):
        lines += [_sizing_text(state.draft), "", "Onay, STEP için yeni giriş kabulünü açar.",
                  "Açık pozisyonun miktarı veya kaldıracı bu ayar seçimiyle değiştirilmez."]
    else:
        position = snapshot.get("position")
        if position:
            if _pending_entry(snapshot):
                lines += ["Yalnız STEP’e ait bekleyen giriş:",
                          str(position["symbol"]) + " · henüz dolmadı"]
            else:
                lines += ["Yalnız STEP’e ait pozisyon:",
                          str(position["symbol"]) + " · LONG · " + str(position["quantity"]) + " adet"]
            lines.append("İşlem kimliği: " + str(position["intent_id"])[:80])
        else:
            lines.append("Açık STEP pozisyonu görülmüyor; kapanış ve durdurma koşulları yeniden doğrulanacak.")
        lines += ["", "Yeni giriş kabulü durdurulur ve STEP kapanış süreci başlatılır.",
                  "Kapanış tamamlanana kadar koruma ve durum takibi sürer.",
                  "Bu işlem NORMAL botun pozisyonlarını kapsamaz."]
    lines += ["", "Onay 120 saniye geçerli; yalnız aynı yönetici kullanabilir.",
              "Hesap, çalışma durumu, pozisyon veya seçili profil değişirse yeniden onay gerekir."]
    return "\n".join(lines)


def _require_control_match(item, owner, state, snapshot, action):
    if (item is None or item.owner != owner or item.revision != state.revision
            or item.expires_at <= time.monotonic()):
        raise ValueError("Kontrol ekranı eskidi. /step ile güncel paneli aç.")
    if _fingerprint(snapshot) != item.fingerprint:
        raise ValueError("STEP durumu veya pozisyon değişti. Paneli yenileyip yeniden onayla.")
    if action not in _allowed_controls(snapshot):
        raise ValueError("STEP’in doğrulanmış durumu bu komuta izin vermiyor. Paneli yenile.")


async def _execute_control(client, action, request_id, state, snapshot):
    profile = _control_profile(state, action, snapshot)
    position = snapshot.get("position") or {}
    try:
        result = await asyncio.wait_for(client.execute(
            action=action, request_id=request_id,
            expected_scope_id=snapshot["scope_id"], expected_runtime_id=snapshot["runtime_id"],
            expected_version=snapshot["version"], profile=profile,
            expected_position_id=position.get("intent_id")), timeout=CONTROL_TIMEOUT_SECONDS)
        step_lock_control.validate_snapshot(result)
        if (result["scope_id"] != snapshot["scope_id"] or result["runtime_id"] != snapshot["runtime_id"]
                or result["version"] < snapshot["version"]):
            raise ValueError("Control response binding changed")
    except asyncio.CancelledError:
        raise
    except Exception:
        raise ValueError("Komutun sonucu doğrulanamadı. Komut otomatik tekrarlanmadı; /step ile durumu kontrol et.") from None
    state.notice = "Kontrol isteği yanıtlandı. Aşağıdaki çalışma durumu yeniden okunarak gösteriliyor."


async def _control_intent(bot, session, cb, owner, state, action, token):
    view = control_views.get(token)
    if (view is None or view.owner != owner or view.revision != state.revision
            or view.expires_at <= time.monotonic()):
        raise ValueError("Kontrol ekranı eskidi. /step ile güncel paneli aç.")
    control_views.pop(token, None)
    client, snapshot = await _read_control(bot)
    _require_control_match(view, owner, state, snapshot, action)
    _control_profile(state, action, snapshot)
    _invalidate(owner)
    request_id = secrets.token_hex(16)
    if action == "pause":
        await _execute_control(client, action, request_id, state, snapshot)
        await _show(bot, session, owner[1], state, cb.get("message"))
        return
    if action not in ("start", "resume", "close_stop"):
        raise ValueError("STEP kontrol isteği geçersiz.")
    nonce = secrets.token_hex(12)
    control_confirmations[nonce] = ControlConfirmation(
        owner, state.revision, state.draft, action, _fingerprint(snapshot), snapshot,
        request_id, time.monotonic() + CONFIRM_TTL_SECONDS)
    await _send(bot, session, owner[1], _control_review(action, state, snapshot),
                {"inline_keyboard": [[_button("✅ Onayla", "slp:controlconfirm:" + nonce),
                                       _button("❌ Vazgeç", "slp:controlcancel:" + nonce)]]}, cb.get("message"))


async def _control_confirm(bot, session, cb, owner, state, token, cancel=False):
    pending = control_confirmations.get(token)
    if (pending is None or pending.owner != owner or pending.revision != state.revision
            or pending.draft != state.draft or pending.expires_at <= time.monotonic()):
        raise ValueError("Kontrol onayı geçersiz, kullanılmış veya süresi dolmuş. /step ile yeniden aç.")
    # Consume before any await: duplicate callback deliveries cannot dispatch twice.
    control_confirmations.pop(token, None)
    if cancel:
        state.notice = "Kontrol isteği iptal edildi; komut gönderilmedi."
    else:
        client, snapshot = await _read_control(bot)
        _require_control_match(pending, owner, state, snapshot, pending.action)
        await _execute_control(client, pending.action, pending.request_id, state, snapshot)
    await _show(bot, session, owner[1], state, cb.get("message"))


async def handle(bot, session, text, chat_id, user_id):
    parts = str(text).strip().split()
    if not parts or parts[0] != "/step":
        return False
    if not bot["_at_admin_allowed"](chat_id, user_id, require_user_id=True):
        return True
    try:
        owner, state = _state(bot, chat_id, user_id)
        if len(parts) == 3 and parts[1] == "margin":
            _stage(owner, state, state.draft.with_margin(parts[2]))
        elif len(parts) == 3 and parts[1] in ("leverage", "lev"):
            _stage(owner, state, state.draft.with_leverage(parts[2]))
        elif len(parts) != 1:
            await _send(bot, session, chat_id,
                "STEP taslak komutları:\n/step\n/step margin 12.50\n/step leverage 5\n\n"
                "Teminat: 0 < tutar ≤ 200 USDT (en çok 2 ondalık). Kaldıraç: 1–10 tam sayı.\n"
                "Canlı işlem bu komutlarla başlatılmaz.", panel_markup(state.draft, state.revision,
                                                                         not state.storage_blocked))
            return True
        await _show(bot, session, chat_id, state)
    except (ValueError, DraftUnavailable) as error:
        await bot["telegram_send"](session, "ℹ️ " + str(error), chat_id=chat_id, track_delivery=False)
    return True


async def callback(bot, session, cb):
    data = str(cb.get("data") or "")
    if not data.startswith("slp:"):
        return False
    message = cb.get("message") or {}
    chat_id = str((message.get("chat") or {}).get("id", ""))
    user_id = str((cb.get("from") or {}).get("id", ""))
    if not bot["_at_admin_allowed"](chat_id, user_id, require_user_id=True):
        await _answer(bot, session, cb, "STEP paneli için yönetici sohbeti ve kullanıcı kimliği gerekli.", True)
        return True
    try:
        owner, state = _state(bot, chat_id, user_id)
        parts = data.split(":")
        if data == "slp:panel":
            await _show(bot, session, chat_id, state, message)
        elif len(parts) == 4 and parts[1] == "control":
            await _control_intent(bot, session, cb, owner, state, parts[2], parts[3])
        elif len(parts) == 3 and parts[1] in ("controlconfirm", "controlcancel"):
            await _control_confirm(bot, session, cb, owner, state, parts[2],
                                   cancel=parts[1] == "controlcancel")
        elif data in ("slp:margin", "slp:leverage"):
            kind = "m" if data == "slp:margin" else "l"
            prompt = ("💰 STEP için hedef teminatı seç (en fazla 200 USDT).\n"
                      "Teminat × hedef kaldıraç = toplam hedef pozisyon.") if kind == "m" else (
                      "⚡ STEP için hedef kaldıracı seç (1–10×).\n"
                      "Bu seçim borsanın kaldıraç ayarını değiştirmez.")
            await _send(bot, session, chat_id, prompt, _choices(kind, state.revision), message)
        elif data == "slp:custom":
            await _send(bot, session, chat_id,
                "✏️ Özel teminat için yaz:\n/step margin 12.50\n\n"
                "0 < tutar ≤ 200 USDT; en fazla iki ondalık basamak.\n"
                "Seçimden sonra panelde taslağı kaydet.",
                {"inline_keyboard": [[_button("⬅️ STEP paneli", "slp:panel")]]}, message)
        elif len(parts) == 5 and parts[1] == "set" and parts[2] in ("m", "l"):
            _check_revision(parts[4], state)
            draft = state.draft.with_margin(parts[3]) if parts[2] == "m" else state.draft.with_leverage(parts[3])
            _stage(owner, state, draft)
            await _show(bot, session, chat_id, state, message)
        elif len(parts) == 3 and parts[1] == "legacy":
            _check_revision(parts[2], state)
            _stage(owner, state, Draft())
            await _show(bot, session, chat_id, state, message)
        elif len(parts) == 3 and parts[1] == "save":
            _check_revision(parts[2], state)
            if state.storage_blocked:
                raise DraftUnavailable("Taslak deposu okunamadığı için kaydetme kilitli.")
            state.draft.validate_ready()
            _invalidate(owner)
            token = secrets.token_hex(12)
            pending_confirmations[token] = Confirmation(owner, state.revision, state.draft,
                                                        time.monotonic() + CONFIRM_TTL_SECONDS)
            await _send(bot, session, chat_id,
                "💾 STEP TASLAĞINI KAYDET\n\n" + _sizing_text(state.draft) + "\n\n"
                "Yalnız bu hesap kapsamına ait ayar taslağı kaydedilecek. Canlı işlem başlamaz.\n"
                "Bu onay 120 saniye geçerli; yalnız aynı yönetici kullanabilir.",
                {"inline_keyboard": [[_button("✅ Taslağı kaydet", "slp:confirm:" + token),
                                       _button("❌ Vazgeç", "slp:cancel:" + token)]]}, message)
        elif len(parts) == 3 and parts[1] in ("confirm", "cancel"):
            pending = pending_confirmations.get(parts[2])
            if (not pending or pending.owner != owner or pending.revision != state.revision
                    or pending.draft != state.draft or pending.expires_at <= time.monotonic()):
                raise ValueError("Onay geçersiz, kullanılmış veya süresi dolmuş. Taslak özetini yeniden aç.")
            pending_confirmations.pop(parts[2], None)
            if parts[1] == "confirm":
                if state.storage_blocked:
                    raise DraftUnavailable("Taslak deposu okunamadığı için kaydetme kilitli.")
                DraftStore(owner[0]).save(chat_id, user_id, pending.draft)
                state.saved_draft = pending.draft
                state.notice = "Taslak kaydedildi. Bu kayıt çalışma profiline uygulanmadı; başlatma komutu gönderilmedi."
            else:
                state.notice = "Bu kaydetme isteği iptal edildi. Önceki kayıt korundu."
            await _show(bot, session, chat_id, state, message)
        elif data in ("slp:summary", "slp:live", "slp:controlinfo"):
            _, snapshot = await _read_control(bot)
            await _send(bot, session, chat_id, _summary(state.draft, snapshot),
                        {"inline_keyboard": [[_button("⬅️ STEP paneli", "slp:panel")]]}, message)
        elif data == "slp:menu":
            # Late import prevents an import cycle with the existing menu router.
            import telegram_ux
            await bot["telegram_send"](session, "🏠 ANA MENÜ", chat_id=chat_id,
                                       reply_markup=telegram_ux.menu_markup(), track_delivery=False)
        else:
            raise ValueError("STEP paneli isteği geçersiz. Paneli yenile.")
        await _answer(bot, session, cb)
    except (ValueError, DraftUnavailable) as error:
        await _answer(bot, session, cb, str(error), True)
    return True
