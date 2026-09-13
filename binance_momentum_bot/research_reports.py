"""Read-only research summaries. Monetary units never mixed with percent cohorts."""
import json
from collections import Counter


def rows(c, sql, args=()):
    cur=c.execute(sql,args)
    return [dict(zip([v[0] for v in cur.description],r)) for r in cur]


def pnl_stats(records, field='net_pnl'):
    known=[r for r in records if r.get(field) is not None]
    values=[float(r[field]) for r in known]
    equity=peak=dd=0.0
    for v in values:
        equity+=v;peak=max(peak,equity);dd=max(dd,peak-equity)
    return dict(count=len(records),priced=len(values),wins=sum(v>0 for v in values),losses=sum(v<0 for v in values),
                gross_known_count=sum(r.get('gross_pnl') is not None for r in records),
                legacy_cost_count=sum('cost_model_version' in r and not r.get('cost_model_version') for r in records),
                win_rate=sum(v>0 for v in values)/len(values)*100 if values else None,
                net=sum(values),expectancy=sum(values)/len(values) if values else None,max_drawdown=dd,
                best=max(values) if values else None,worst=min(values) if values else None,
                gross=sum(r.get('gross_pnl') or 0 for r in known),fees=sum(r.get('fees',r.get('commission')) or 0 for r in known),
                slippage=sum(r.get('slippage_cost') or 0 for r in known))


def summary(c, start=0, end=2**62):
    result={'period_ms':[start,end],'units':'USDT for DRY/ALT; percent for causal/current outcomes'}
    for model,lower in [('ALT_WIDE_60',-3.5),('ALT_CONTROL_60',-3.0)]:
        data=rows(c,'SELECT * FROM alt_research_trades WHERE model_name=? AND decision_ts>=? AND decision_ts<? ORDER BY terminal_ts,research_trade_id',(model,start,end))
        closed=rows(c,"SELECT * FROM alt_research_trades WHERE model_name=? AND lifecycle_status='CLOSED' AND terminal_ts>=? AND terminal_ts<? ORDER BY terminal_ts,research_trade_id",(model,start,end))
        stats=pnl_stats(closed)
        stats.update(total=len(data),mature=len(closed),open=c.execute("SELECT COUNT(*) FROM alt_research_trades WHERE model_name=? AND lifecycle_status IN ('ARMED','OPEN')",(model,)).fetchone()[0],
                     terminals=dict(Counter(r['terminal_reason'] for r in closed)),
                     skips=dict(Counter(r['skip_reason'] for r in data if r['skip_reason'])),
                     age_le45=sum(r['candidate_to_premium_age'] is not None and r['candidate_to_premium_age']<=45 for r in data),
                     age_gt45=sum((r['candidate_to_premium_age'] or 0)>45 for r in data),
                     observation_gaps=sum(r['observation_gap'] for r in data))
        stats['terminals']['UNPRICED_TIMEOUT']=c.execute("SELECT COUNT(*) FROM alt_research_trades WHERE model_name=? AND lifecycle_status='UNPRICED_TIMEOUT' AND terminal_ts>=? AND terminal_ts<?",(model,start,end)).fetchone()[0]
        stats['period_basis']='admissions/skips by decision_ts; mature/PnL by terminal_ts; open is current inventory'
        edges=[lower,-2,-1,0,1,3,5,6]
        buckets={f'{a:g}..{b:g}':0 for a,b in zip(edges,edges[1:])};buckets['outside']=0
        for r in closed:
            if r['terminal_reason']!='TIME_EXIT':continue
            val=r['gross_return'];key='outside'
            for a,b in zip(edges,edges[1:]):
                if a<=val<b or b==6 and val==6:key=f'{a:g}..{b:g}';break
            buckets[key]+=1
        stats['timeout_return_buckets_pct']=buckets
        stats['forward_horizons']=rows(c,'SELECT o.horizon_s,o.status,COUNT(*) count,AVG(o.return_pct) mean_return_pct FROM alt_forward_outcomes o JOIN alt_research_trades t USING(research_trade_id) WHERE t.model_name=? AND t.decision_ts>=? AND t.decision_ts<? GROUP BY o.horizon_s,o.status',(model,start,end))
        result[model]=stats
    dry=rows(c,"SELECT * FROM autotrade_trades WHERE mode='DRY' AND closed_ts_ms>=? AND closed_ts_ms<? ORDER BY closed_ts_ms,id",(start,end))
    result['BOT_DRY']=pnl_stats(dry)
    result['CURRENT_OUTCOMES_PCT']=rows(c,'SELECT horizon_s,COUNT(*) count,AVG(return_pct) mean_return_pct FROM signal_outcomes WHERE ts*1000>=? AND ts*1000<? GROUP BY horizon_s',(start,end))
    result['CAUSAL_PERCENT_ONLY']=rows(c,'SELECT stage,status,first_event,COUNT(*) count,AVG(gross_pnl_pct) mean_gross_pct,AVG(net_pnl_pct) mean_net_pct,SUM(net_pnl_pct>0) wins,SUM(net_pnl_pct<0) losses,SUM(fees_pct) fees_pct_sum,SUM(slippage_pct) slippage_pct_sum FROM causal_cohorts WHERE decision_time_ms>=? AND decision_time_ms<? GROUP BY stage,status,first_event',(start,end))
    contexts=rows(c,'SELECT * FROM alt_premium_context WHERE ready_ts>=? AND ready_ts<?',(start,end))
    result['fatigue']=dict(Counter('FIRST' if r['premium_sequence_number']==1 else 'REPEAT' for r in contexts))
    result['fatigue_forward_comparison']=rows(c,"SELECT t.model_name,CASE WHEN a.premium_sequence_number=1 THEN 'FIRST' ELSE 'REPEAT' END ordinal_group,COUNT(*) mature,AVG(t.net_pnl) expectancy_usdt,SUM(t.net_pnl) net_usdt FROM alt_research_trades t JOIN alt_premium_context a USING(signal_id) WHERE t.lifecycle_status='CLOSED' AND t.terminal_ts>=? AND t.terminal_ts<? GROUP BY t.model_name,ordinal_group",(start,end))
    result['exhaustion_tags']=dict(Counter(tag for r in contexts for tag in json.loads(r['tags_json'])))
    result['late_entry_context']=dict(count=len(contexts),pre_candidate_10m_known=sum(json.loads(r['context_json']).get('pre_candidate_10m_return') is not None for r in contexts))
    return result


def altstats_text(c):
    report=summary(c)
    lines=['🧪 ALT SHADOW — production kararına etkisi yok']
    for model in ('ALT_WIDE_60','ALT_CONTROL_60'):
        r=report[model];t=r['terminals']
        lines.extend([model,f"Toplam {r['total']} | mature {r['mature']} | açık {r['open']}",
                      f"TP {t.get('TP',0)} / SL {t.get('SL',0)} / TIME_EXIT {t.get('TIME_EXIT',0)} / fiyat yok {t.get('UNPRICED_TIMEOUT',0)}",
                      f"Gross {r['gross']:.2f} / Net {r['net']:.2f} USDT | fee {r['fees']:.2f} + slip {r['slippage']:.2f}",
                      f"Expectancy {r['expectancy'] if r['expectancy'] is not None else 'UNKNOWN'} | max DD {r['max_drawdown']:.2f}",
                      f"Skip: {r['skips']} | Candidate→Premium ≤45s {r['age_le45']} / >45s {r['age_gt45']}",
                      f"60dk return %: {r['timeout_return_buckets_pct']}"])
    return '\n'.join(lines)
