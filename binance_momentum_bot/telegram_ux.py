"""Read-only Telegram navigation and account views. No scanner/startup hooks."""
import asyncio
from contextlib import closing
import json
import time
import secrets

import daytrades
import trade_views
import visual_cards
from position_observer import LeverageCache, roe_values
from telegram_cards import ownership, number, pages, timestamp

MENU = (
    ('📊 Durum', '/status'), ('📈 Açık Pozisyonlar', '/positions'),
    ('📋 Bugünün İşlemleri', '/daytrades'), ('🧾 İşlem Listesi', '/daytradeslist'),
    ('🔥 Isınan Coinler', '/top'), ('🧪 ALT Shadow', '/altstats'),
    ('⚙️ Ayarlar', '/settings'), ('❓ Yardım', '/help'),
)
ALIASES = {label.casefold(): command for label, command in MENU}
ALIASES.update({'menu': '/menu', 'menü': '/menu', '/menu': '/menu', '/start': '/menu', '/daytrade': '/daytrades'})
leverage_cache = LeverageCache()
pending_modes = {}


async def observer_open(bot, session, message):
    event = message.opening_event
    detail = json.loads(event.get('detail_json') or '{}')
    record = dict(symbol=event['symbol'], ownership=event.get('source'), side=event.get('direction'),
        entry_price=event.get('entry_price'), exit_price=event.get('current_price'),
        leverage=event.get('leverage'), net_pnl=detail.get('pnl'), roe=event.get('roe'),
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
    if text not in ('/menu', '/help', '/daytradeslist', '/positions', '/reportstatus', '/daytradesraw', '/positionsraw'): return False
    if not bot['_at_admin_allowed'](chat_id, user_id): return True
    if text in ('/positions','/positionsraw','/daytradeslist','/daytradesraw'):
        view = await trade_views.opened(bot,session,leverage_cache) if text.startswith('/positions') else await trade_views.closed(bot,session)
        await visual_cards.deliver(bot,session,view,chat_id,raw=text.endswith('raw'))
        return True
    if text == '/menu':
        await bot['telegram_send'](session, '🏠 ANA MENÜ\nBir görünüm seçin. İşlem ayarları ⚙️ Ayarlar bölümündedir.',
                                   chat_id=chat_id, reply_markup=menu_markup())
        return True
    if text == '/help':
        message = '❓ YARDIM\n'+'\n'.join(f'{label} — {command}' for label, command in MENU)
        message += '\n/menu — ana menü\n/daytrade — /daytrades kısayolu\n/reportstatus — araştırma paketi durumu\n\nGelişmiş: /latestexport, /latestexportfull, /riskstatus, /analiz COIN, /daytradesraw, /positionsraw\n🔒 LIVE kilitli. Ayar değişiklikleri onay gerektirir.'
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
