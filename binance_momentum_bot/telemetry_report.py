"""Read-only common P0 metrics; usage: --db SNAPSHOT --start-ms N --end-ms N.

Recall denominator is observed CANDIDATE episodes, not the Binance universe.
Rates use the explicitly requested wall-clock window, with gaps reported separately.
"""
import argparse
import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from statistics import mean, median


def report(c, start_ms, end_ms, dedup='episode'):
    if end_ms <= start_ms or dedup not in ('episode','parent','none'):
        raise ValueError('Invalid window or dedup mode')
    c.row_factory=sqlite3.Row
    rows=[dict(r) for r in c.execute('''SELECT f.*,o.return_pct,o.mfe_pct,o.mae_pct,
        o.net_return_pct,o.peak_ts_ms,o.gap,o.missing_reason,o.due_ts_ms,o.observed_ts_ms,
        EXISTS(SELECT 1 FROM p0_gap_events g WHERE g.source_key=f.source_key AND g.first_observed_ts_ms<=?) AS any_gap
        FROM p0_forward f LEFT JOIN p0_forward_outcomes o
        ON f.source_key=o.source_key AND o.horizon_s=3600
        WHERE f.allow_decision_ts>=? AND f.allow_decision_ts<? ORDER BY f.allow_decision_ts,f.source_key''',(end_ms,start_ms,end_ms))]
    def episode(r):
        return (r['symbol'],r['episode_id']) if r['episode_id'] else (r['symbol'],r['source_key'])
    def good(r):
        return r['mfe_pct']>=3 and r['mae_pct']>=-1.5 and r['return_pct']>0
    def valid(r, clean=False):
        return all(r[k] is not None for k in ('return_pct','mfe_pct','mae_pct','observed_ts_ms')) and r['observed_ts_ms']<=end_ms and not r['missing_reason'] and (not clean or not (r['gap'] or r['any_gap']))
    candidates={}
    for r in rows:
        if r['layer']=='CANDIDATE':
            candidates.setdefault(episode(r),r)
    denominator={k:r for k,r in candidates.items() if valid(r,True) and good(r)}
    layers={k:[] for k in ('CLASSIC','FAST','REACQUIRE','IGNITION')}
    seen=set()
    for r in rows:
        if r['layer'] not in layers:
            continue
        identity = r['source_key'] if dedup=='none' else (r['symbol'],r['parent_id']) if dedup=='parent' and r['parent_id'] else episode(r)
        key=(r['layer'],str(identity))
        if key not in seen:
            seen.add(key); layers[r['layer']].append(r)
    metrics={}
    for layer, group in layers.items():
        mature=[r for r in group if r['allow_decision_ts']+3600000<=end_ms]
        valid_all=[r for r in mature if valid(r)]
        clean=[r for r in mature if valid(r,True)]
        matched={episode(r) for r in group if episode(r) in denominator and
                 r['allow_decision_ts']<=denominator[episode(r)]['peak_ts_ms'] and
                 r['allow_decision_ts']<=denominator[episode(r)]['allow_decision_ts']+3600000}
        def avg(items,k):
            values=[r[k] for r in items if r[k] is not None]
            return mean(values) if values else None
        candidate_delays=[(r['allow_decision_ts']-candidates[episode(r)]['allow_decision_ts'])/1000 for r in group if episode(r) in candidates]
        peak_delays=[(r['peak_ts_ms']-r['allow_decision_ts'])/1000 for r in clean if r['peak_ts_ms'] is not None]
        metrics[layer]={
            'decisions':len(group),'mature':len(mature),'valid_including_gaps':len(valid_all),'clean':len(clean),
            'quality_good_fraction':sum(good(r) for r in clean)/len(clean) if clean else None,
            'good_recall_proxy':len(matched)/len(denominator) if denominator else None,
            'good_recall_denominator':len(denominator),
            'false_positive_per_wall_clock_hour':sum(not good(r) for r in clean)/((end_ms-start_ms)/3600000),
            'cost_adjusted_forward_return_pct':avg(clean,'net_return_pct'),
            'cost_adjusted_return_including_gaps_pct':avg(valid_all,'net_return_pct'),
            'mae_pct':avg(clean,'mae_pct'),
            'candidate_to_decision_median_s':median(candidate_delays) if candidate_delays else None,
            'decision_to_peak_median_s':median(peak_delays) if peak_delays else None,
            'gap_rate':sum(bool(r['any_gap'] or r['gap'] or not valid(r)) for r in mature)/len(mature) if mature else None,
        }
    return {'window_ms':[start_ms,end_ms],'dedup':dedup,'horizon_s':3600,
            'label':'GOOD: MFE>=3%, MAE>=-1.5%, terminal return>0',
            'limitations':['Observed candidate universe only; not market recall.',
                          'First eligible row per layer/group, selected without outcome filtering.',
                          'False positive = mature clean non-GOOD; wall-clock exposure includes downtime.',
                          'Forward proxy return, not realized trade PnL. No funding or market impact.'],
            'layers':metrics,
            'gap_counts':[dict(r) for r in c.execute('''SELECT kind,COUNT(*) AS sources,SUM(count) AS events
              FROM p0_gap_events WHERE first_observed_ts_ms>=? AND first_observed_ts_ms<? GROUP BY kind''',(start_ms,end_ms))]}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db',required=True)
    parser.add_argument('--start-ms',type=int,required=True)
    parser.add_argument('--end-ms',type=int,required=True)
    parser.add_argument('--dedup',choices=('episode','parent','none'),default='episode')
    args=parser.parse_args()
    c=sqlite3.connect(Path(args.db).resolve().as_uri()+'?mode=ro',uri=True)
    try:
        print(json.dumps(report(c,args.start_ms,args.end_ms,args.dedup),indent=2))
    finally:
        c.close()
