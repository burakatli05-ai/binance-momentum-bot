"""Telegram STEP Lock configuration drafts; no trading or runtime control API.

The approved 25 USDT notional / max 5x reference is deliberately distinct
from a proposed margin x target-leverage draft.  Neither is an active order
instruction.  This module never reads NORMAL account data or trading settings.
"""
from dataclasses import dataclass
from contextlib import closing
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
import secrets
import sqlite3
import time


MAX_MARGIN_USDT = Decimal("200")
MAX_LEVERAGE = 10
MAX_NOTIONAL_USDT = Decimal("2000")
REFERENCE_NOTIONAL_USDT = Decimal("25")
REFERENCE_LEVERAGE_CAP = 5
CONFIRM_TTL_SECONDS = 120
SESSION_TTL_SECONDS = 1800
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


sessions = {}
pending_confirmations = {}


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


def _stage(owner, state, draft):
    _invalidate(owner)
    state.draft = draft
    state.revision += 1
    if not state.storage_blocked:
        state.notice = "Seçim değişti. Kaydetmek için taslak özetini onayla."


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


def panel_text(draft, *, saved=False, notice=""):
    text = ("🪜 STEP LOCK KONTROL PANELİ\n\n"
            "Panel: AYAR TASLAĞI · canlı komutlar KİLİTLİ\n"
            "Hesap kapsamı: ayrı STEP hesabı · ONE_WAY / LONG\n\n"
            + ("💾 Kayıtlı taslak\n" if saved else "✏️ Kaydedilmemiş seçim\n")
            + _sizing_text(draft) + "\n\n"
            "Seçim sınırları: teminat ≤ 200 USDT · kaldıraç ≤ 10×\n"
            "Toplam hedef pozisyon ≤ 2.000 USDT\n\n"
            "Referans test limitleri (19.10.2026):\n"
            "25 USDT toplam pozisyon · en fazla 5×\n"
            "Aynı anda 1 işlem · UTC gününde 1 yeni giriş\n"
            "Gerçekleşmiş günlük net zarar 1 USDT: yeni giriş kilidi\n\n"
            "Bu panelin canlı bağlantısı: bağlanmadı\n"
            "Serbest bakiye: — · Pozisyon: — · Doğrulanmış stop: —\n"
            "Son canlı veri zamanı: —\n\n"
            "Taslak kaydı emir göndermez, borsa kaldıracını değiştirmez. "
            "NORMAL bot ayarları ayrıdır.")
    if notice:
        text += "\n\nℹ️ " + notice
    return text


def _button(text, data):
    return {"text": text, "callback_data": data}


def panel_markup(draft, revision=0, can_save=True):
    try:
        draft.validate_ready()
    except ValueError:
        can_save = False
    margin = _number(draft.margin_usdt) + " USDT" if draft.margin_usdt is not None else "Teminat seç"
    leverage = str(draft.leverage) + "×" if draft.leverage is not None else "Kaldıraç seç"
    return {"inline_keyboard": [
        [_button("💰 " + margin, "slp:margin"), _button("⚡ " + leverage, "slp:leverage")],
        [_button("💾 Taslağı kaydet" if can_save else "ℹ️ Seçim / kayıt durumu",
                 f"slp:save:{revision}" if can_save else "slp:summary")],
        [_button("🧪 Küçük test profili", f"slp:legacy:{revision}"),
         _button("📋 Başlatma özeti", "slp:summary")],
        [_button("🔒 Canlı başlatma kilitli", "slp:live")],
        [_button("🔄 Yenile", "slp:panel"), _button("⬅️ Ana menü", "slp:menu")],
    ]}


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
    await _send(bot, session, chat_id,
                panel_text(state.draft, saved=state.saved_draft == state.draft, notice=state.notice),
                panel_markup(state.draft, state.revision, not state.storage_blocked), message)


async def _answer(bot, session, cb, text="", alert=False):
    if cb.get("id"):
        payload = {"callback_query_id": cb["id"], "text": text[:190]}
        if alert:
            payload["show_alert"] = True
        await bot["telegram_api_call"](session, "answerCallbackQuery", payload)


def _check_revision(value, state):
    if not value.isdigit() or int(value) != state.revision:
        raise ValueError("Bu seçim ekranı eskidi. STEP panelini yenileyip tekrar seç.")


def _summary(draft):
    return ("📋 STEP BAŞLATMA ÖZETİ\n\n" + _sizing_text(draft) + "\n\n"
            "Durum: yalnız ayar taslağı; canlı başlatma kullanıma açılmadı.\n"
            "200 USDT ve 10× seçimi, 2.000 USDT hedef pozisyondur.\n"
            "Diğer test limitleri otomatik büyümez.\n\n"
            "Canlı kullanım için STEP hesabı, bakiye, sembolün gerçek kaldıracı, "
            "miktar/fiyat adımları, minimum işlem tutarı ve koruyucu stop yolu "
            "doğrulanmalıdır. Uygun olmayan hedef otomatik büyütülmez.\n"
            "Yönetim bağlantısı ve yeni limit profili ayrıca uygulanıp doğrulanmalıdır.\n\n"
            "Bu panelden canlı başlatma, duraklatma veya pozisyon kapatma komutu gönderilmez.")


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
                state.notice = "Taslak kaydedildi. Canlı işlem başlamadı; bu panelin canlı bağlantısı kurulmadı."
            else:
                state.notice = "Bu kaydetme isteği iptal edildi. Önceki kayıt korundu."
            await _show(bot, session, chat_id, state, message)
        elif data in ("slp:summary", "slp:live"):
            await _send(bot, session, chat_id, _summary(state.draft),
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
