"""Read-only Telegram navigation and account views. No scanner/startup hooks."""
import asyncio
from contextlib import closing
import json
import time
import secrets

import daytrades
import notification_policy
import trade_views
import today_positions
import visual_cards
from position_observer import LeverageCache, roe_values
from telegram_cards import ownership, number, pages, timestamp

MENU = (
    ('\u26a1 Early Terminali', '/earlyv2'), ('\U0001f916 Premium AutoTrader', '/autotrade'),
    ('\U0001f4ca Durum', '/status'), ('\U0001f4c8 A\u00e7\u0131k Pozisyonlar', '/positions'),
    ('\U0001f4cb Bug\u00fcn\u00fcn \u0130\u015flemleri', '/daytrades'), ('\U0001f9fe \u0130\u015flem Listesi', '/daytradeslist'),
    ('\U0001f4c5 Bug\u00fcn A\u00e7\u0131lanlar', '/todaypositions'), ('\U0001f525 Is\u0131nan Coinler', '/top'),
    ('\U0001f9ea ALT Shadow', '/altstats'), ('\u2699\ufe0f Ayarlar', '/settings'),
    ('\u2753 Yard\u0131m', '/help'),
)
ALIASES = {label.casefold(): command for label, command in MENU}
ALIASES.update({'menu': '/menu', 'menü': '/menu', '/menu': '/menu', '/start': '/menu',
                '/daytrade': '/daytrades', '/todayopened': '/todaypositions',
                '/todayposition': '/todaypositions', '/earlyterminal': '/earlyv2'})
leverage_cache = LeverageCache()
pending_modes = {}


def early_notification_text():
    state = notification_policy.early_signal_notification_status()
    enabled = bool(state['enabled'])
    return (
        '🔔 EARLY BİLDİRİM TERMİNALİ\n\n'
        f'Durum: {"🟢 AÇIK" if enabled else "⚪ KAPALI"}\n'
        'Kaynak: mevcut production Early kuralları\n'
        'Gönderim: aynı Telegram botu ve mevcut yayın kanalı\n'
        'Premium bildirimleri: etkilenmez\n'
        'Otomatik emir: YOK — bu panel yalnız Early kartlarını yönetir\n\n'
        'Açıldığında yalnız bundan sonra oluşan uygun Early bildirimleri gönderilir; '
        'geçmiş sinyaller tekrar gönderilmez.'
    )


def early_notification_markup():
    enabled = notification_policy.early_signal_notifications_enabled()
    toggle = (
        {'text': '🔴 Bildirimleri Kapat', 'callback_data': 'earlynotify:off'}
        if enabled else
        {'text': '🟢 Bildirimleri Aç', 'callback_data': 'earlynotify:on'}
    )
    return {'inline_keyboard': [
        [toggle],
        [{'text': '🔄 Durumu Yenile', 'callback_data': 'earlynotify:status'}],
    ]}


async def early_notification_callback(bot, session, cb):
    data = str(cb.get('data') or '')
    if not data.startswith('earlynotify:'):
        return False
    callback_id = cb.get('id')
    message = cb.get('message') or {}
    chat = str((message.get('chat') or {}).get('id', ''))
    user = str((cb.get('from') or {}).get('id', ''))
    if not bot['_at_admin_allowed'](chat, user):
        if callback_id:
            await bot['telegram_api_call'](session, 'answerCallbackQuery', {
                'callback_query_id': callback_id,
                'text': 'Bu işlem için yetkin yok.',
                'show_alert': True,
            })
        return True
    action = data.split(':', 1)[1]
    try:
        if action == 'on':
            notification_policy.set_early_signal_notifications_enabled(True)
            answer = 'Early bildirimleri açıldı.'
        elif action == 'off':
            notification_policy.set_early_signal_notifications_enabled(False)
            answer = 'Early bildirimleri kapatıldı.'
        elif action == 'status':
            answer = 'Durum yenilendi.'
        else:
            answer = 'Komut tanınmadı.'
    except OSError:
        if callback_id:
            await bot['telegram_api_call'](session, 'answerCallbackQuery', {
                'callback_query_id': callback_id,
                'text': 'Ayar kalıcı olarak kaydedilemedi.',
                'show_alert': True,
            })
        return True
    if callback_id:
        await bot['telegram_api_call'](session, 'answerCallbackQuery', {
            'callback_query_id': callback_id,
            'text': answer,
        })
    await bot['telegram_send'](
        session,
        early_notification_text(),
        chat_id=chat,
        reply_markup=early_notification_markup(),
    )
    return True


async def observer_open(bot, session, message):
    event = message.opening_event
    detail = json.loads(event.get('detail_json') or '{}')
    record = dict(symbol=event['symbol'], ownership=event.get('source'), side=event.get('direction'),
        entry_price=event.get('entry_price'), exit_price=event.get('current_price'),
        leverage=event.get('leverage'), net_pnl=detail.get('pnl'), roe=event.get('roe'),
        opened_ts_ms=event.get('event_time_ms'), current_ts_ms=event.get('event_time_ms'),
        provenance='Gözlem: '+timestamp(event.get('event_time_ms')))
    view = dict(title='🟢 POZİSYON AÇILDI', date=timestamp(event.get('event_time_ms')), summary=[],
        cards=[trade_views.card(record,True)], notices=['Açılış gözlemidir; kesin emir/fill zamanı değildir.'])
    return await visual_cards.deliver(bot,session,view,bot['TELEGRAM_ADMIN_CHAT_ID'])


async def confirm_mode(bot, session, target, chat_id, user_id):
    if target not in ('OFF','DRY') or not bot['_at_admin_allowed'](chat_id,user_id): return
    now=time.time()
    for token, item in list(pending_modes.items()):
        if item[3]<now:pending_modes.pop(token,None)
    token=secrets.token_hex(8)
    pending_modes[token]=(target,chat_id,user_id,now+120)
    await bot['telegram_send'](session,f'⚙️ MOD DEĞİŞİKLİĞİ\n{bot["autotrade_cfg"]["mode"]} → {target}\nOnaylıyor musunuz?',chat_id=chat_id,
        reply_markup={'inline_keyboard':[[{'text':'✅ Onayla','callback_data':'uxmode:'+token},
                                         {'text':'❌ Vazgeç','callback_data':'uxmode:cancel:'+token}]]})


async def mode_callback(bot, session, cb):
    data=str(cb.get('data') or '')
    if data.startswith('earlynotify:'):
        return await early_notification_callback(bot, session, cb)
    if not data.startswith('uxmode:'):return False
    chat=str(((cb.get('message') or {}).get('chat') or {}).get('id',''))
    user=str((cb.get('from') or {}).get('id',''))
    parts=data.split(':');token=parts[-1];pending=pending_modes.get(token)
    message='İstek geçersiz veya süresi dolmuş.'
    if (bot['_at_admin_allowed'](chat,user) and pending and pending[1:3]==(chat,user) and pending[3]>=time.time()):
        pending_modes.pop(token,None)
        if len(parts)==3 and parts[1]=='cancel':message='Değişiklik iptal edildi.'
        elif len(parts)==2 and pending[0] in ('OFF','DRY'):
            bot['autotrade_cfg']['mode']=pending[0]
            bot['_at_save_setting']('mode',pending[0])
            message='AutoTrade modu: '+pending[0]
    await bot['telegram_api_call'](session,'answerCallbackQuery',{'callback_query_id':cb.get('id'),'text':message})
    return True


def normalize(text):
    text = text.strip()
    if text.startswith('/'):
        first, *rest = text.split(maxsplit=1)
        text = first.split('@', 1)[0].lower() + (' '+rest[0] if rest else '')
    return ALIASES.get(text.casefold(), text)


def menu_markup():
    return {'keyboard': [[{'text': label} for label, _ in MENU[i:i+2]] for i in range(0, len(MENU), 2)],
            'resize_keyboard': True, 'is_persistent': True}


def report_status(worker):
    if worker is None: return '🧪 ARAŞTIRMA PAKETİ\nAraştırma export servisi kapalı.'
    state = worker.state()
    pointer = worker.root/'latest.json'
    if not pointer.exists(): return '🧪 ARAŞTIRMA PAKETİ\nHenüz doğrulanmış snapshot yok.'
    sid = json.loads(pointer.read_text(encoding='utf-8'))['snapshot_id']
    directory = (worker.root/sid).resolve()
    if directory.parent != worker.root.resolve(): raise ValueError('invalid snapshot pointer')
    manifest = json.loads((directory/'manifest.json').read_text(encoding='utf-8'))
    date = state.get('daily_date')
    package = None
    if date and (worker.root/f'daily-{date}.json').is_file():
        package = json.loads((worker.root/f'daily-{date}.json').read_text(encoding='utf-8'))
    ready = bool(package and package.get('local_date') == date and package.get('snapshot',{}).get('valid'))
    valid = manifest.get('valid') and manifest.get('integrity_check') == ['ok']
    changed = manifest.get('unchanged')
    new_data = '—' if changed is None else ('hayır' if changed else 'evet')
    return ('🧪 ARAŞTIRMA PAKETİ\n'
            f'Günlük rapor tarihi: {date or "—"}\nSon snapshot: {sid}\n'
            f'Snapshot zamanı: {timestamp(manifest.get("created_time_ms"))}\n'
            f'Paket snapshot: {(package or {}).get("snapshot",{}).get("snapshot_id") or "—"}\n'
            f'DB kontrolü (kayıtlı sonuç): {"OK" if valid else "doğrulanamadı"}\n'
            f'Snapshot değişmiş: {new_data}\nGünlük paket: {"hazır" if ready else "bekleniyor"}')


async def show_positions(bot, session):
    lines = ['📈 AÇIK POZİSYONLAR']
    with closing(bot['db_connect']()) as c:
        c.row_factory = __import__('sqlite3').Row
        dry = [dict(r) for r in c.execute("SELECT * FROM autotrade_trades WHERE mode='DRY' AND status IN ('OPEN','PARTIAL','PROTECTIVE_PARTIAL')")]
    for tr in dry:
        st = bot['states'].get(tr['symbol'])
        price = st.last_price if st and st.last_trade_receive_ms and bot['now_ms']()-st.last_trade_receive_ms <= 10000 else None
        entry, lev = tr.get('entry_price'), tr.get('leverage')
        roe = (price/entry-1)*100*lev*(1 if tr.get('side') == 'LONG' else -1) if price and entry and lev else None
        lines.append(f'\n{tr["symbol"]} · {ownership("BOT", "DRY")} · {tr["side"]}\n'
                     f'Kaldıraç: {number(lev,"x")} | Giriş: {number(entry)} | Anlık: {number(price)}\nROE: {number(roe,"%",True)}')
    try:
        live = await bot['binance_signed_request'](session, 'GET', '/fapi/v3/positionRisk')
        if not isinstance(live, list): raise ValueError('positions unavailable')
        live = [p for p in live if abs(float(p.get('positionAmt') or 0)) > 1e-12]
        if live: await leverage_cache.refresh(session, bot['binance_signed_request'])
        for p in live:
            side = p.get('positionSide') or 'BOTH'
            direction = 'LONG' if side == 'LONG' or side == 'BOTH' and float(p['positionAmt']) > 0 else 'SHORT'
            entry, mark = float(p['entryPrice']), float(p['markPrice'])
            lev, _ = leverage_cache.get(p['symbol'])
            roe, pnl, _ = roe_values(p, direction, entry, mark, lev)
            lines.append(f'\n{p["symbol"]} · {ownership(bot["_po_bot_source"](p["symbol"], side))} · {direction}\n'
                         f'Kaldıraç: {number(lev,"x")} | Giriş: {number(entry)} | Anlık: {number(mark)}\n'
                         f'P/L: {number(pnl," USDT",True)} | ROE: {number(roe,"%",True)}')
        if not live and not dry: lines.append('Açık pozisyon yok.')
    except Exception:
        lines.append('\nLIVE pozisyonlar alınamadı; açık işlem yok anlamına gelmez.')
    return '\n'.join(lines)


async def handle(bot, session, text, chat_id, user_id):
    supported = ('/menu', '/help', '/earlyterminal', '/daytradeslist', '/positions', '/todaypositions', '/reportstatus',
                 '/daytradesraw', '/positionsraw', '/todaypositionsraw')
    if text not in supported: return False
    if not bot['_at_admin_allowed'](chat_id, user_id): return True
    if text in ('/positions','/positionsraw','/daytradeslist','/daytradesraw','/todaypositions','/todaypositionsraw'):
        if text.startswith('/positions'):
            view = await trade_views.opened(bot,session,leverage_cache)
        elif text.startswith('/todaypositions'):
            view = await today_positions.build(bot,session,leverage_cache)
        else:
            view = await trade_views.closed(bot,session)
        await visual_cards.deliver(bot,session,view,chat_id,raw=text.endswith('raw'))
        return True
    if text == '/menu':
        await bot['telegram_send'](session, '🏠 ANA MENÜ\nBir görünüm seçin. İşlem ayarları ⚙️ Ayarlar bölümündedir.',
                                   chat_id=chat_id, reply_markup=menu_markup())
        return True
    if text == '/earlyterminal':
        await bot['telegram_send'](
            session,
            early_notification_text(),
            chat_id=chat_id,
            reply_markup=early_notification_markup(),
        )
        return True
    if text == '/help':
        message = '❓ YARDIM\n'+'\n'.join(f'{label} — {command}' for label, command in MENU)
        message += ('\n/menu — ana menü\n/daytrade — /daytrades kısayolu\n'
                    '/reportstatus — araştırma paketi durumu\n\nGelişmiş: /latestexport, /latestexportfull, '
                    '/riskstatus, /analiz COIN, /daytradesraw, /positionsraw, /todaypositionsraw\n'
                    '🔒 LIVE kilitli. Ayar değişiklikleri onay gerektirir.')
    elif text == '/positions': message = await show_positions(bot, session)
    elif text == '/reportstatus':
        try: message = await asyncio.to_thread(report_status, bot['export_worker'])
        except Exception: message = '❌ Araştırma paketi durumu okunamadı; paket hazır kabul edilmedi.'
    else:
        async def request(path, params):
            if path not in ('/fapi/v1/income', '/fapi/v1/userTrades', '/fapi/v1/order'): raise ValueError('read-only history required')
            return await bot['binance_signed_request'](session, 'GET', path, params)
        with closing(bot['db_connect']()) as c:
            message = await daytrades.trade_list(c, request, bot['now_ms']())
    for page in pages(message): await bot['telegram_send'](session, page, chat_id=chat_id)
    return True
