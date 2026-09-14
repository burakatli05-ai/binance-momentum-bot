"""Presentation models for account cards. All exchange access is read-only."""
import asyncio
from contextlib import closing
from datetime import datetime
import daytrades
import position_cycles
from telegram_cards import ownership, number, timestamp, IST, BOT_DRY, BOT_LIVE, MANUAL_LIVE
from position_observer import roe_values


def summary(records, opened=False):
    lines = []
    for label in (BOT_DRY, MANUAL_LIVE, BOT_LIVE):
        data = [r for r in records if ownership(r.get('ownership')) == label]
        pnl = [r.get('net_pnl') for r in data if r.get('net_pnl') is not None]
        total = sum(pnl) if pnl or not data else None
        name = label.split(' ',1)[1]
        lines.append(f'{name}: {len(data)} | P/L {number(total, " USDT", True)} ({len(pnl)}/{len(data)})')
    if opened:
        long = sum(r.get('side') == 'LONG' for r in records)
        lines.insert(0, f'Açık {len(records)}  •  LONG {long} / SHORT {len(records)-long}')
        for mode in ('DRY','LIVE'):
            data = [r for r in records if ('DRY' in ownership(r.get('ownership'))) == (mode=='DRY')]
            known = [r['notional'] for r in data if r.get('notional') is not None]
            lines.append(f'{mode} notional: {number(sum(known) if known or not data else None," USDT")} ({len(known)}/{len(data)})')
    else:
        complete = [r for r in records if r.get('cycle_complete', True)]
        lines.insert(0, f'Kapalı {len(complete)}  •  Kazanç {sum((r.get("net_pnl") or 0)>0 for r in complete)} / Kayıp {sum((r.get("net_pnl") or 0)<0 for r in complete)}  •  Kısmi {len(records)-len(complete)}')
    return lines


def card(r, opened=False):
    source = ownership(r.get('ownership'))
    provenance = r.get('provenance','BOT_LEDGER')
    evidence = {'ANCHORED_COMPLETE_FILLS':'Fill ve miktar eşleştirmesi tamam',
        'OPEN_BEFORE_LOOKBACK':'Açılış, erişilen geçmişten önce', 'UNANCHORED_CLOSE_RECORD':'Eksik geçmiş / kapanış kaydı',
        'CURRENT_ACCOUNT_SNAPSHOT':'Güncel hesap verisi', 'BOT_LEDGER':'Bot işlem kaydı'}.get(provenance,provenance)
    title = f'{r["symbol"]}  •  {r.get("side") or "—"}  •  {number(r.get("leverage"),"x")}'
    return dict(source=source, pnl=r.get('net_pnl'), provenance=provenance, fill_ids=r.get('fill_ids',[]), lines=[title, source,
        f'Açılış: {timestamp(r.get("opened_ts_ms"))}' + ('' if opened else f'  |  Kapanış: {timestamp(r.get("closed_ts_ms"))}'),
        f'Entry: {number(r.get("entry_price"))}  |  {"Anlık" if opened else "Exit"}: {number(r.get("exit_price"))}',
        f'P/L: {number(r.get("net_pnl")," USDT",True)}  |  ROE: {number(r.get("roe"),"%",True)}',
        f'Notional: {number(r.get("notional")," USDT")}' if opened else f'Kapanış nedeni: {r.get("close_reason") or "—"}',
        'Veri: '+evidence])


async def closed(bot, session):
    now = bot['now_ms'](); start = daytrades.local_start(now)
    with closing(bot['db_connect']()) as c:
        ledger = daytrades.rows(c, 'SELECT * FROM autotrade_trades')
    records = daytrades.dry_rows(ledger, start, now)
    notices = []; live_available = True
    async def request(path, params):
        if path not in ('/fapi/v3/positionRisk','/fapi/v1/income','/fapi/v1/userTrades','/fapi/v1/order'):
            raise ValueError('read-only account endpoint required')
        return await bot['binance_signed_request'](session, 'GET', path, params)
    try:
        live, notices = await asyncio.wait_for(position_cycles.load(request, start, now, ledger), 45)
        records.extend(live)
    except Exception:
        live_available = False
        notices.append('LIVE geçmiş alınamadı; bu listede LIVE sayısı ve P/L bilinmiyor.')
    notices += ['—: doğrulanamayan veri; kaldıraç/ROE geçmişten yoksa tahmin edilmez.',
        'LIVE net: bilinen açılış+kapanış ücretleri dahil; funding hariç. Kısmi döngüler tam işlem sayılmaz.',
        'MANUEL / LIVE: bu botla eşleştirilmeyen hesap faaliyeti; insan kökeni kanıtı değildir.',
        '/daytradesraw: tüm alanlar, veri kaynağı ve metin görünümü. /daytrades eski kapanış-emri özetidir.']
    totals = summary(records)
    if not live_available:
        totals = [s if 'LIVE' not in s else s.split(':')[0]+': — (alınamadı)' for s in totals]
        totals[0] = 'Yalnız DRY kayıtları • LIVE toplamı bilinmiyor'
    return dict(title='BUGÜN KAPANAN İŞLEMLER', date=datetime.fromtimestamp(now/1000, IST).strftime('%d.%m.%Y'),
                summary=totals, cards=[card(r) for r in sorted(records,key=lambda r:r.get('closed_ts_ms') or 0)], notices=notices)


async def opened(bot, session, cache):
    now = bot['now_ms'](); records = []; notices = []
    with closing(bot['db_connect']()) as c:
        ledger = daytrades.rows(c, "SELECT * FROM autotrade_trades WHERE status IN ('OPEN','PARTIAL','PROTECTIVE_PARTIAL')")
    for tr in ledger:
        if tr['mode'] != 'DRY': continue
        state = bot['states'].get(tr['symbol'])
        price = state.last_price if state and state.last_trade_receive_ms and 0 <= now-state.last_trade_receive_ms <= 10000 else None
        entry = tr.get('entry_price'); lev = tr.get('leverage')
        ret = (price/entry-1)*(1 if tr.get('side') == 'LONG' else -1) if price and entry else None
        # Current remaining quantity only; do not reuse original notional after a partial close.
        qty = tr.get('expected_qty') if tr.get('expected_qty') is not None else (tr.get('qty') if tr.get('status') == 'OPEN' else None)
        pnl = ret*entry*qty if ret is not None and qty is not None else None
        records.append(dict(tr, ownership='BOT DRY', exit_price=price, net_pnl=pnl,
            roe=ret*lev*100 if ret is not None and lev else None,
            notional=abs(qty*price) if qty is not None and price else None))
    async def request(session, method, path):
        if method != 'GET' or path not in ('/fapi/v3/positionRisk','/fapi/v1/symbolConfig'): raise ValueError('read-only positions required')
        return await asyncio.wait_for(bot['binance_signed_request'](session, method, path), 15)
    live_available = True
    dry_count = len(records)
    try:
        positions = await request(session, 'GET', '/fapi/v3/positionRisk')
        if not isinstance(positions, list): raise ValueError('positions unavailable')
        await cache.refresh(session, request)
        for p in positions:
            amount = float(p.get('positionAmt') or 0)
            if not amount: continue
            side = p.get('positionSide') or 'BOTH'
            direction = 'LONG' if side == 'LONG' or side == 'BOTH' and amount > 0 else 'SHORT'
            entry, mark = float(p['entryPrice']), float(p['markPrice'])
            lev, _ = cache.get(p['symbol'])
            roe, pnl, _ = roe_values(p, direction, entry, mark, lev)
            if p.get('unRealizedProfit',p.get('unrealizedProfit')) is None:
                pnl=None
                roe=(mark/entry-1)*lev*100*(1 if direction=='LONG' else -1) if lev and entry else None
            source = bot['_po_bot_source'](p['symbol'], side)
            records.append(dict(symbol=p['symbol'], side=direction, ownership=source, leverage=lev,
                entry_price=entry, exit_price=mark, net_pnl=pnl, roe=roe, notional=abs(amount*mark),
                opened_ts_ms=None, provenance='CURRENT_ACCOUNT_SNAPSHOT'))
    except Exception:
        live_available = False
        records = records[:dry_count]
        notices.append('LIVE pozisyon listesi alınamadı; LIVE toplamları bilinmiyor.')
    notices += ['DRY ve LIVE parasal toplamları ayrıdır; ücret/funding açık P/L içine eklenmez.',
                'MANUEL / LIVE: bu botla eşleştirilmeyen hesap faaliyeti. —: bilinmiyor.', '/positionsraw: metin görünümü.']
    totals=summary(records,True)
    if not live_available:
        totals=[s if 'LIVE' not in s else s.split(':')[0]+': — (alınamadı)' for s in totals]
        totals[0]='Yalnız DRY kayıtları • LIVE toplamı bilinmiyor'
    return dict(title='AÇIK POZİSYONLAR',date=timestamp(now),summary=totals,cards=[card(r,True) for r in records],notices=notices)
