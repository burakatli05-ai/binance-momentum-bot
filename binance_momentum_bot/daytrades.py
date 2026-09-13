"""Admin, read-only account history. No inferred manual ownership or mixed totals."""
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from research_reports import rows, pnl_stats

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


async def report(c,request,now):
    local=datetime.fromtimestamp(now/1000,IST)
    start=int(local.replace(hour=0,minute=0,second=0,microsecond=0).timestamp()*1000)
    ledger=rows(c,'SELECT * FROM autotrade_trades')
    dry=[r for r in ledger if r['mode']=='DRY' and start<=(r.get('closed_ts_ms') or -1)<=now]
    dry.sort(key=lambda r:r['closed_ts_ms'])
    lines=section('A) BOT / DRY — sanal',dry)
    lines.append('B) MANUAL BINANCE / REAL — USDT kontratları')
    try:
        groups,complete=await load_real(request,start,now,ledger)
        for ownership in ('MANUAL VERIFIED','BOT LIVE','UNKNOWN OWNERSHIP'):
            lines.extend(section(ownership,[r for r in groups if r['ownership']==ownership]))
        currencies=defaultdict(float)
        for r in groups:
            for asset,fee in r['other_fees'].items():currencies[asset]+=fee
        lines.append('Kapanış emri grupları; aynı emrin partial fill kayıtları birleştirildi. Tam pozisyon win rate değildir.')
        lines.append('Real net: kapanış fill ücretleri sonrası; önceki açılış ücretleri/funding dahil değil. Gün sınırında tam pozisyon maliyeti UNKNOWN.')
        if currencies:lines.append(f'Dönüştürülmeyen ücretler: {dict(currencies)}; ilgili net UNKNOWN.')
        if not complete:lines.append('INCOMPLETE: API sayfa/metadata sınırı; toplamlar kısmi.')
    except Exception as exc:
        lines.append(f'REAL geçmiş alınamadı: {type(exc).__name__}; sıfır işlem olarak yorumlanmamalı.')
    return '\n'.join(lines)
