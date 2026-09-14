"""Admin, read-only account history. No inferred manual ownership or mixed totals."""
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from research_reports import rows, pnl_stats
from telegram_cards import ownership, BOT_DRY, BOT_LIVE, MANUAL_LIVE, number, timestamp

IST=timezone(timedelta(hours=3))


def classify(symbol,order_id,client_id,ledger):
    for r in ledger:
        if r.get('mode')!='LIVE' or r.get('symbol')!=symbol:continue
        if any(str(r.get(k) or '')==str(order_id) for k in ('entry_order_id',)) or client_id and any(r.get(k)==client_id for k in ('entry_client_id','tp1_client_id','tp2_client_id','stop_client_id')):
            return 'BOT LIVE'
    # An unrecognized order could be another bot/API client. Never guess MANUAL.
    return 'UNKNOWN OWNERSHIP'


def group_fills(fills,orders,ledger):
    groups={}
    for f in fills:
        # Partial executions of one close order are reliably grouped. Position cycles
        # cannot be reconstructed from an arbitrary day boundary without opening history.
        key=(f['symbol'],f.get('positionSide','BOTH'),str(f['orderId']))
        r=groups.setdefault(key,dict(symbol=key[0],position_side=key[1],order_id=key[2],gross_pnl=0.,fees=0.,net_pnl=0.,fills=0,close=False,other_fees={},terminal_ts=0))
        pnl=float(f.get('realizedPnl') or 0);fee=float(f.get('commission') or 0)
        order=orders.get((key[0],key[2]),{})
        side=f.get('side');position=key[1]
        is_close=pnl!=0 or order.get('reduceOnly') is True or position=='LONG' and side=='SELL' or position=='SHORT' and side=='BUY'
        r['close']|=is_close;r['gross_pnl']+=pnl;r['fills']+=1;r['terminal_ts']=max(r['terminal_ts'],int(f['time']))
        asset=f.get('commissionAsset','UNKNOWN')
        if asset=='USDT':r['fees']+=fee
        else:r['other_fees'][asset]=r['other_fees'].get(asset,0)+fee
        r['ownership']=classify(key[0],key[2],order.get('clientOrderId'),ledger)
        r['net_pnl']=None if r['other_fees'] else r['gross_pnl']-r['fees']
        qty=float(f.get('qty') or 0);price=float(f.get('price') or 0)
        r['qty']=r.get('qty',0)+qty;r['quote']=r.get('quote',0)+qty*price
        r['exit_price']=r['quote']/r['qty'] if r['qty'] else None
        r['side']='LONG' if side=='SELL' else ('SHORT' if side=='BUY' else None)
        r['order_type']=order.get('origType') or order.get('type')
        r['close_reason']={'STOP':'STOP (emir türü)','STOP_MARKET':'STOP (emir türü)',
                           'TAKE_PROFIT':'TAKE PROFIT (emir türü)','TAKE_PROFIT_MARKET':'TAKE PROFIT (emir türü)'}.get(r['order_type'])
        r.setdefault('entry_price',None);r.setdefault('leverage',None);r.setdefault('opened_ts_ms',None)
        r['closed_ts_ms']=r['terminal_ts'];r.setdefault('roe',None)
        for trade in ledger:
            if classify(key[0],key[2],order.get('clientOrderId'),[trade])=='BOT LIVE':
                for field in ('entry_price','leverage','opened_ts_ms'):
                    r[field]=trade.get(field)
                margin=trade.get('margin_usdt')
                r['roe']=r['net_pnl']/margin*100 if r['net_pnl'] is not None and margin else None
                if trade.get('closed_ts_ms')==r['closed_ts_ms']:r['close_reason']=trade.get('close_reason') or r['close_reason']
                break
    return sorted((r for r in groups.values() if r['close']),key=lambda r:r['terminal_ts'])


async def load_real(request,start,end,ledger):
    income=[];complete=True
    for page in range(1,21):
        batch=await request('/fapi/v1/income',dict(startTime=start,endTime=end,limit=1000,page=page))
        if not isinstance(batch,list):raise ValueError('income history unavailable')
        income.extend(batch)
        if len(batch)<1000:break
    else:complete=False
    symbols=sorted({r['symbol'] for r in income if r.get('symbol','').endswith('USDT')})
    fills=[];orders={}
    for symbol in symbols:
        # Split saturated time windows, rather than silently dropping >1000 fills.
        windows=[(start,end)]
        while windows:
            lo,hi=windows.pop()
            batch=await request('/fapi/v1/userTrades',dict(symbol=symbol,startTime=lo,endTime=hi,limit=1000))
            if not isinstance(batch,list):raise ValueError('trade history unavailable')
            if len(batch)>=1000 and hi>lo:
                mid=(lo+hi)//2;windows.extend(((lo,mid),(mid+1,hi)));continue
            if len(batch)>=1000:complete=False
            fills.extend(batch)
        # Fetch metadata only for close groups, with a bounded request budget.
        candidates={str(f['orderId']) for f in fills if f['symbol']==symbol and (float(f.get('realizedPnl') or 0)!=0 or f.get('positionSide') in ('LONG','SHORT'))}
        if len(candidates)>100:complete=False
        for oid in sorted(candidates)[:100]:
            try:
                order=await request('/fapi/v1/order',dict(symbol=symbol,orderId=oid))
                if isinstance(order,dict):orders[(symbol,oid)]=order
            except Exception:pass  # Missing metadata => UNKNOWN OWNERSHIP.
    unique={(f['symbol'],str(f['id'])):f for f in fills}
    groups=group_fills(unique.values(),orders,ledger)
    return groups,complete


def section(title,data):
    p=pnl_stats(data)
    rate=f"{p['win_rate']:.1f}%" if p['win_rate'] is not None else 'UNKNOWN'
    return [title,f"Kayıt {p['count']} | win/loss {p['wins']}/{p['losses']} | win rate {rate}",
            f"Net {p['net']:.2f} USDT (fiyatlanmış {p['priced']}) | ücret {p['fees']:.2f} USDT",
            f"Best {p['best']} / worst {p['worst']}"]


def local_start(now):
    local=datetime.fromtimestamp(now/1000,IST)
    return int(local.replace(hour=0,minute=0,second=0,microsecond=0).timestamp()*1000)


def dry_rows(ledger,start,now):
    result=[]
    for trade in ledger:
        if trade['mode']!='DRY' or not start<=(trade.get('closed_ts_ms') or -1)<=now:continue
        r=dict(trade,ownership='BOT DRY')
        r['roe']=r.get('net_pnl')/r['margin_usdt']*100 if r.get('net_pnl') is not None and r.get('margin_usdt') else None
        result.append(r)
    return result


def summary_block(label,closed,opened):
    priced=[r['net_pnl'] for r in closed if r.get('net_pnl') is not None]
    roes=[r['roe'] for r in closed if r.get('roe') is not None]
    net=sum(priced) if priced or not closed else None
    return [label,f'Toplam: {len(closed)+len(opened)} | Kapalı: {len(closed)} | Açık: {len(opened)}',
            f'Realized P/L (kapanan): {number(net," USDT",True)} ({len(priced)}/{len(closed)} kayıt)',
            f'Ortalama kapanış ROE: {number(sum(roes)/len(roes) if roes else None,"%",True)} ({len(roes)}/{len(closed)} kayıt)']


async def report(c,request,now):
    start=local_start(now)
    ledger=rows(c,'SELECT * FROM autotrade_trades')
    dry=dry_rows(ledger,start,now)
    opened=[r for r in ledger if r['mode']=='DRY' and r['status'] in ('OPEN','PARTIAL','PROTECTIVE_PARTIAL')]
    lines=['📋 BUGÜNÜN İŞLEM ÖZETİ',datetime.fromtimestamp(now/1000,IST).strftime('%d.%m.%Y'),
           'Kapalı: bugün · Açık: devredenler dahil\n']+summary_block(BOT_DRY,dry,opened)
    live_open=None
    try:
        positions=await request('/fapi/v3/positionRisk',{})
        if not isinstance(positions,list):raise ValueError('positions unavailable')
        live_open=[p for p in positions if abs(float(p.get('positionAmt') or 0))>1e-12]
    except Exception:lines.append('LIVE açık pozisyonlar alınamadı; sayı bilinmiyor.')
    try:
        groups,complete=await load_real(request,start,now,ledger)
        for label in (MANUAL_LIVE,BOT_LIVE):
            closed=[r for r in groups if ownership(r['ownership'])==label]
            opened_live=[]
            for p in live_open or []:
                bot_owned=any(r['mode']=='LIVE' and r['symbol']==p['symbol'] and r['status'] in ('OPEN','PARTIAL','PROTECTIVE_PARTIAL')
                              and (r.get('position_side') or 'BOTH')==(p.get('positionSide') or 'BOTH') for r in ledger)
                if (BOT_LIVE if bot_owned else MANUAL_LIVE)==label:opened_live.append(p)
            block=summary_block(label,closed,opened_live)
            if live_open is None:block[1]=f'Kapalı: {len(closed)} | Açık: — | Toplam: —'
            lines.extend(['']+block)
        lines.append('\nKaynak dağılımı, yukarıdaki üç ayrı gruptadır; sanal ve gerçek P/L birleştirilmez.')
        if not complete:lines.append('⚠️ API sınırı nedeniyle LIVE geçmişi kısmi.')
    except Exception:
        lines.append('\nLIVE geçmiş alınamadı; sıfır işlem olarak yorumlanmamalı.')
    lines.append('LIVE kapalı sayısı kapanış emirlerini sayar; kısmi kapanış içerebilir. P/L kapanış ücretleri sonrası, açılış ücreti/funding hariçtir. —: veri yok.')
    lines.append('MANUEL / LIVE: bu botla eşleştirilmeyen hesap kayıtları da dahildir.')
    return '\n'.join(lines)


async def trade_list(c,request,now):
    start=local_start(now)
    ledger=rows(c,'SELECT * FROM autotrade_trades')
    data=dry_rows(ledger,start,now);notice=[]
    try:
        live,complete=await load_real(request,start,now,ledger)
        data.extend(live)
        if not complete:notice.append('⚠️ API sınırı nedeniyle LIVE listesi kısmi.')
    except Exception:notice.append('⚠️ LIVE geçmiş alınamadı; liste yalnız mevcut DRY kayıtlarını içeriyor.')
    lines=['🧾 BUGÜN KAPANAN İŞLEMLER',datetime.fromtimestamp(now/1000,IST).strftime('%d.%m.%Y')]
    for r in sorted(data,key=lambda r:r.get('closed_ts_ms') or 0):
        lines.append(f'\n{r["symbol"]} · {ownership(r["ownership"])} · {r.get("side") or "—"}\n'
                     f'Kaldıraç: {number(r.get("leverage"),"x")}\n'
                     f'Açılış: {timestamp(r.get("opened_ts_ms"))}\nKapanış: {timestamp(r.get("closed_ts_ms"))}\n'
                     f'Entry: {number(r.get("entry_price"))} | Exit: {number(r.get("exit_price"))}\n'
                     f'P/L: {number(r.get("net_pnl")," USDT",True)} | ROE: {number(r.get("roe"),"%",True)}\n'
                     f'Kapanış nedeni: {r.get("close_reason") or "—"}')
    if not data:lines.append('Gösterilecek kapanış kaydı yok.')
    lines.extend(notice)
    lines.append('\nLIVE kayıtlar kapanış emirleridir; kısmi kapanış olabilir. Eksik geçmiş kaldıraç/açılış/entry/ROE: —. Güncel kaldıraç geçmişe uygulanmaz.')
    lines.append('LIVE P/L: kapanış ücreti sonrası; açılış ücreti/funding hariç. MANUEL / LIVE, bu botla eşleştirilmeyen kayıtları da kapsar.')
    return '\n'.join(lines)
