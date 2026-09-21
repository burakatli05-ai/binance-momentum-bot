"""Read-only, purged forward research. Never exports production weights.

Input matrix is the audited labels_features.csv; provenance is an optional JSONL
of {kind,id,feature,value,source_ms,available_ms,max_age_ms,trusted} records.
Unknown source timestamps are excluded from predictors, not silently imputed.
Percentages are percentage points. Costs are explicit scenario assumptions.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import sqlite3
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, brier_score_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

PHASE = ['chg30','chg60','chg5','candidate_runup','distance_from_episode_low_pct',
         'dist_episode_peak_pct','seconds_since_episode_peak','episode_age_s']
MODELS = {
    'score_calibrated': ['score'],
    'phase': PHASE,
    'score_plus_phase': ['score'] + PHASE,
    'runner_core': ['flow30','chg30','book_imbalance'],
    'runner_compression': ['flow30','chg30','book_imbalance','compression_ratio'],
    'relative_rank': ['rel30','btc30','gainer_rank'],
    'relative_rank_extended': ['rel30','btc30','gainer_rank','rank_velocity','oi_accel5'],
}
BINS = {
    'score': [-np.inf,75,80,85,np.inf],
    'dist_episode_peak_pct': [-np.inf,-1,-.5,-.25,-.1,0,.01,np.inf],
    'seconds_since_episode_peak': [-np.inf,1,5,15,30,60,np.inf],
    'episode_age_s': [-np.inf,30,60,120,300,900,np.inf],
}

def pipeline():
    return Pipeline([('imputer',SimpleImputer(strategy='median')),
                     ('scale',StandardScaler()),
                     ('model',LogisticRegression(C=.1,max_iter=2000))])

def strict_features(d, records):
    """Availability AND source-age checks; outcome fields cannot be admitted."""
    names = sorted(set(sum(MODELS.values(), [])))
    x = pd.DataFrame(np.nan,index=d.index,columns=names)
    lookup = {(r.kind,int(r.id)): i for i,r in d.iterrows()}
    audit=[]
    seen=set()
    for r in records:
        key=(r.get('kind'),r.get('id')); f=r.get('feature')
        if key not in lookup or f not in names: continue
        i=lookup[key]; dec=d.loc[i,'decision_ts']*1000
        reason='OK'
        try:
            source=float(r['source_ms']); available=float(r['available_ms'])
            age=float(r['max_age_ms']); value=float(r['value'])
            if not all(np.isfinite(v) for v in (source,available,age,value)): reason='NONFINITE'
            elif not r.get('trusted'): reason='UNTRUSTED_SOURCE'
            elif source<=0 or source>available or available>dec or source>dec: reason='FUTURE_OR_INVALID_TIME'
            elif age<0 or dec-source>age: reason='STALE'
        except (KeyError,TypeError,ValueError): reason='MISSING_SOURCE_TIME'
        if (i,f) in seen: raise ValueError('Duplicate provenance record')
        seen.add((i,f))
        if reason=='OK': x.loc[i,f]=value
        audit.append(dict(kind=key[0],id=key[1],feature=f,status=reason))
    return x,pd.DataFrame(audit)

def forward_splits(d, symbol_out=False):
    """Labels must have matured before the start of the held-out calendar day."""
    days=sorted(d.day.unique())
    for day in days[2:]:
        day_rows=d[d.day==day]
        start=pd.Timestamp(day,tz='Europe/Istanbul').tz_convert('UTC').timestamp()
        parts=day_rows.groupby('symbol') if symbol_out else [('ALL',day_rows)]
        for symbol,te in parts:
            tr=d[(d.decision_ts<start)&(d.label_ready<start)&
                 ~d.episode_key.isin(te.episode_key)&~d.wave43200.isin(te.wave43200)]
            if symbol_out: tr=tr[tr.symbol!=symbol]
            assert not set(tr.episode_key)&set(te.episode_key)
            assert not set(tr.wave43200)&set(te.wave43200)
            yield str(day)+':'+str(symbol),tr.index,te.index

def exact_weights(values, fraction=.25):
    """Fractional ties: compare scores at exactly the same retrospective coverage."""
    values=np.asarray(values); budget=len(values)*fraction
    cut=np.sort(values)[::-1][min(len(values)-1,int(np.ceil(budget))-1)]
    w=(values>cut).astype(float)
    ties=values==cut; w[ties]=(budget-w.sum())/ties.sum()
    return w

def metric(d, weight='weight'):
    y=d.y.to_numpy(); p=d.p.to_numpy(); w=d[weight].to_numpy()
    selected=w.sum(); pos=y.sum(); base=y.mean()
    out=dict(n=len(d),positive=int(pos),coverage=selected/len(d),
             precision=np.dot(w,y)/selected if selected else np.nan,
             capture=np.dot(w,y)/pos if pos else np.nan,
             auc=roc_auc_score(y,p) if len(set(y))==2 else np.nan,
             brier=brier_score_loss(y,p) if d.model.iloc[0]!='production_score_raw' else np.nan)
    out['lift']=out['precision']/base if base else np.nan
    for col,name in [('bad','bad_rate'),('net_proxy','net_ev_proxy')]:
        valid=d[col].notna().to_numpy(); den=w[valid].sum()
        out[name]=np.dot(w[valid],d.loc[valid,col])/den if den else np.nan
        out[name+'_n']=int(valid.sum())
    return out

def clustered(d, repetitions=400):
    rows=[]; rng=np.random.default_rng(20260921)
    for group in ['day','wave43200']:
        blocks=[z for _,z in d.groupby(group)]; vals=[]
        if len(blocks)<2: continue
        for _ in range(repetitions):
            z=pd.concat([blocks[j] for j in rng.integers(0,len(blocks),len(blocks))])
            vals.append(metric(z))
        frame=pd.DataFrame(vals)
        for col in ['precision','capture','auc','brier','lift','bad_rate','net_ev_proxy']:
            rows.append(dict(cluster=group,clusters=len(blocks),metric=col,
                             low=frame[col].quantile(.025),high=frame[col].quantile(.975)))
    return rows

def evaluate(d,x,target,symbol_out=False):
    predictions=[]; skipped=[]
    for fold,train,test in forward_splits(d,symbol_out):
        tr=d.loc[train]; te=d.loc[test]
        if len(tr)<30 or tr.y.nunique()<2 or tr.y.value_counts().min()<5:
            skipped.append(dict(target=target,fold=fold,model='ALL',reason='INSUFFICIENT_TRAIN')); continue
        for name,features in MODELS.items():
            # Frozen feature families are never silently changed to fit coverage.
            valid=all(x.loc[train,f].notna().mean()>=.65 and x.loc[train,f].nunique()>1 for f in features)
            if not valid:
                skipped.append(dict(target=target,fold=fold,model=name,reason='FEATURE_PROVENANCE_OR_COVERAGE'));continue
            covered=x.loc[test,features].notna().mean(axis=1)>=.65
            idx=te.index[covered & te.score.notna()]
            if not len(idx): continue
            pipe=pipeline().fit(x.loc[train,features],tr.y)
            p=pipe.predict_proba(x.loc[idx,features])[:,1]
            cutoff=np.quantile(pipe.predict_proba(x.loc[train,features])[:,1],.75)
            for model,prob in [(name,p),('production_score_raw',te.loc[idx,'score'].to_numpy())]:
                z=d.loc[idx,['kind','id','symbol','episode_key','wave43200','day','bad','net_proxy','y']].copy()
                z['target']=target; z['fold']=fold; z['model']=model; z['comparison']=name
                z['p']=prob; z['weight']=exact_weights(prob)
                z['train_threshold_selected']=(prob>=cutoff).astype(float) if model==name else np.nan
                z['eligible_test_n']=len(te);z['training_n']=len(tr)
                predictions.append(z)
        # Raw score is descriptive forward baseline even if provenance blocks models.
        idx=te.index[te.score.notna()]
        if len(idx):
            z=d.loc[idx,['kind','id','symbol','episode_key','wave43200','day','bad','net_proxy','y']].copy()
            z['target']=target; z['fold']=fold; z['model']='production_score_raw';z['comparison']='standalone'
            z['p']=te.loc[idx,'score'];z['weight']=exact_weights(z.p)
            z['eligible_test_n']=len(te);z['training_n']=len(tr)
            predictions.append(z)
    return pd.concat(predictions,ignore_index=True) if predictions else pd.DataFrame(),skipped

def describe(g):
    out=dict(n=len(g),tp_label_n=int(g.tp_label.notna().sum()),quality_label_n=int(g.good.notna().sum()),
             tp1_pct=100*g.tp_label.mean(),invalidation_pct=100*(1-g.tp_label).mean(),
             good_pct=100*g.good.mean(),bad_pct=100*g.bad.mean(),
             target_before_entry_pct=100*g.first_event.eq('TARGET_BEFORE_ENTRY').mean(),
             net_ev_proxy=g.net_proxy.mean(),net_proxy_n=int(g.net_proxy.notna().sum()))
    for f in ['mfe60','mae60','end60']:out['median_'+f]=g[f].median()
    return out

def band_tables(d):
    rows=[]; condition=[]
    for f in ['score']+PHASE+['spread','qv24','book_imbalance','book_data_age_ms']:
        if f not in d:continue
        bins=BINS.get(f,[-np.inf,0,.5,1,1.5,2,3,5,np.inf])
        if f=='qv24':bins=[-np.inf,5e6,10e6,25e6,50e6,100e6,np.inf]
        if f=='spread':bins=[-np.inf,.02,.05,.1,.2,np.inf]
        if f=='book_data_age_ms':bins=[-np.inf,500,1000,3000,10000,np.inf]
        cats=pd.cut(d[f],bins,right=False).astype(str).where(d[f].notna(),'MISSING')
        for band,g in d.groupby(cats):rows.append(dict(feature=f,band=band,**describe(g)))
        if f in PHASE:
            sb=pd.cut(d.score,BINS['score'],right=False).astype(str)
            for (phase,score),g in d.groupby([cats,sb]):
                condition.append(dict(feature=f,phase_band=phase,score_band=score,**describe(g)))
    return pd.DataFrame(rows),pd.DataFrame(condition)

def load(db,matrix,roundtrip,funding):
    d=pd.read_csv(matrix).replace([np.inf,-np.inf],np.nan)
    if d.duplicated(['kind','episode_key']).any():raise ValueError('Duplicate cohort')
    with sqlite3.connect(Path(db).resolve().as_uri()+'?mode=ro&immutable=1',uri=True) as c:
        c.execute('PRAGMA query_only=ON')
        paths=pd.read_sql_query('SELECT * FROM signal_paths',c).rename(columns={'signal_id':'id'})
    d=d.merge(paths,on='id',how='left',suffixes=('','_path'))
    # Numeric IDs overlap between Early and Premium; never join Premium outcomes to Early.
    early=d.kind=='early'
    for col in paths.columns:
        if col!='id':d.loc[early,col]=np.nan
    fill=d.entry_touch_s.notna()&d.path_entry_price.gt(0)
    d['tp_label']=d.first_event.map({'TP1':1.,'INVALIDATION':0.}).where(fill)
    event_s=np.where(d.tp_label==1,d.tp1_hit_s,d.invalidation_hit_s)
    d['tp_ready']=d.ts+event_s
    d.loc[d.tp_ready<d.ts+d.entry_touch_s,'tp_label']=np.nan
    # Conservative availability: final persisted path record, not inferred touch time.
    d['tp_ready']=np.maximum(d.tp_ready,d.updated_ts)
    d['quality_label']=np.where(d.good.eq(1),1.,np.where(d.bad.eq(1),0.,np.nan))
    exit_price=np.where(d.tp_label==1,d.target1,d.invalidation)
    gross=100*(exit_price/d.path_entry_price-1)
    d['net_proxy']=(gross-roundtrip-funding).where(d.tp_label.notna())
    d['quality_class']=np.where(d.good.isna(),'MISSING',np.where(d.good.eq(1),'GOOD',np.where(d.bad.eq(1),'BAD','GRAY')))
    return d

def fast_reacquire(db,out):
    with sqlite3.connect(Path(db).resolve().as_uri()+'?mode=ro&immutable=1',uri=True) as c:
        q='''SELECT w.*,o.label,o.observation_gap AS outcome_gap,s.episode_id
        FROM runner_watch_v1_shadow w LEFT JOIN runner_watch_outcome_v1_shadow o USING(watch_id)
        JOIN runner_score_v1_shadow s ON s.id=w.score_id'''
        d=pd.read_sql_query(q,c)
    d['parent']=d.parent_watch_id.fillna(d.watch_id)
    both=d.groupby('parent').kind.nunique();d=d[d.parent.isin(both[both>1].index)].copy()
    # One predeclared first watch per parent and kind, not many correlated children.
    d=d.sort_values(['anchor_ts_ms','watch_id']).drop_duplicates(['parent','kind'],keep='first')
    d['decision_group']=d.decision.where(d.decision.isin(['ALLOW','BLOCK']),'NO_DECISION')
    # Parent pairing controls selection, but different anchors still prevent causal claims.
    rows=[]
    for (kind,decision),g in d.groupby(['kind','decision_group']):
        mature=g[g.label.notna()]
        rows.append(dict(kind=kind,decision=decision,n=len(g),parents=g.parent.nunique(),
                         matured_n=len(mature),runner_rate=mature.label.eq('RUNNER').mean(),
                         gap_rate=mature.outcome_gap.mean(),comparison='PAIRED_PARENT_DESCRIPTIVE_NOT_CAUSAL'))
    pd.DataFrame(rows).to_csv(out/'fast_reacquire.csv',index=False)
    d.to_csv(out/'fast_reacquire_paired_rows.csv',index=False)

def supplemental(d,out,repetitions):
    """Forward descriptive bands and full-cohort economic uncertainty."""
    premium=d[d.kind=='premium'].copy()
    first_days=sorted(premium.day.unique())[:2]
    forward=premium[~premium.day.isin(first_days)]
    b,c=band_tables(forward)
    b.to_csv(out/'forward_phase_bands.csv',index=False)
    c.to_csv(out/'forward_conditional_score_bands.csv',index=False)
    rng=np.random.default_rng(20260921);rows=[]
    for group in ['day','wave43200']:
        g=premium[premium.net_proxy.notna()];blocks=[v.net_proxy.to_numpy() for _,v in g.groupby(group)]
        values=[np.concatenate([blocks[i] for i in rng.integers(0,len(blocks),len(blocks))]).mean() for _ in range(repetitions)]
        rows.append(dict(cluster=group,clusters=len(blocks),n=len(g),mean=g.net_proxy.mean(),
                         low=np.quantile(values,.025),high=np.quantile(values,.975)))
    pd.DataFrame(rows).to_csv(out/'net_ev_uncertainty.csv',index=False)
    patterns=[]
    for name in PHASE:
        valid=premium[[name,'tp_label','net_proxy']].dropna()
        patterns.append(dict(feature=name,n=len(valid),spearman_tp=valid[name].corr(valid.tp_label,method='spearman'),
                             spearman_net_proxy=valid[name].corr(valid.net_proxy,method='spearman'),
                             interpretation='DESCRIPTIVE_NOT_CAUSAL_NO_THRESHOLD_PROMOTION'))
    pd.DataFrame(patterns).to_csv(out/'phase_associations.csv',index=False)

def provenance_inventory(db,out):
    counts={};n=0
    with sqlite3.connect(Path(db).resolve().as_uri()+'?mode=ro&immutable=1',uri=True) as c:
        for (raw,) in c.execute('SELECT raw_features_json FROM runner_score_v1_shadow'):
            f=json.loads(raw);n+=1
            for key in ['trade_event_ts_ms','trade_received_ts_ms','book_event_ts_ms','book_received_ts_ms',
                        'oi_source_ts_ms','rank_source_ts_ms','feature_ready_ts_ms','trade_data_age_ms','book_data_age_ms']:
                counts[key]=counts.get(key,0)+int(f.get(key) is not None)
    (out/'historical_source_inventory.json').write_text(json.dumps(dict(rows=n,nonmissing=counts,
      note='Snapshot ages are not source event timestamps. Unavailable fields are not promoted to predictors.'),indent=2))

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('db',type=Path);parser.add_argument('--matrix',type=Path,required=True)
    parser.add_argument('--provenance',type=Path);parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--roundtrip-cost-pct',type=float,default=.14)
    parser.add_argument('--funding-cost-pct',type=float,default=0.)
    parser.add_argument('--bootstrap',type=int,default=400)
    args=parser.parse_args();args.out.mkdir(parents=True,exist_ok=True)
    d=load(args.db,args.matrix,args.roundtrip_cost_pct,args.funding_cost_pct)
    records=[json.loads(s) for s in args.provenance.read_text().splitlines() if s.strip()] if args.provenance else []
    x,audit=strict_features(d,records);audit.to_csv(args.out/'provenance_audit.csv',index=False)
    x.notna().sum().rename('accepted_n').to_csv(args.out/'predictor_coverage.csv')
    premium=d[d.kind=='premium'];bands,conditional=band_tables(premium)
    bands.to_csv(args.out/'phase_bands.csv',index=False);conditional.to_csv(args.out/'conditional_score_bands.csv',index=False)
    d.to_csv(args.out/'cohort_labels.csv',index=False)
    specs=[('tp1','premium','tp_label',3600),('good_bad','premium','quality_label',3600)]
    for horizon,threshold in [(14400,6),(21600,8),(43200,10),(43200,15)]:
        name=f'runner_{horizon//3600}h{threshold}';col='mfe'+str(horizon)
        d[name]=np.where(d[col].notna(),d[col].ge(threshold).astype(float),np.nan)
        specs.append((name,'early',name,horizon))
    allpred=[];skipped=[]
    for name,kind,col,horizon in specs:
        z=d[(d.kind==kind)&d[col].notna()].copy();z['y']=z[col]
        z['label_ready']=z.tp_ready if name=='tp1' else z.label_end if kind=='premium' else z.ts+horizon
        for symbol_out in [False,True]:
            pred,skip=evaluate(z,x,name,symbol_out);scheme='symbol_out' if symbol_out else 'day'
            skipped.extend([dict(scheme=scheme,**r) for r in skip])
            if len(pred):pred['scheme']=scheme;allpred.append(pred)
    predictions=pd.concat(allpred,ignore_index=True) if allpred else pd.DataFrame()
    predictions.to_csv(args.out/'oof_predictions.csv',index=False)
    pd.DataFrame(skipped).to_csv(args.out/'skipped_models.csv',index=False)
    metrics=[];ci=[]
    if len(predictions):
        for key,g in predictions.groupby(['target','scheme','comparison','model']):
            fields=dict(zip(['target','scheme','comparison','model'],key));metrics.append(dict(**fields,**metric(g)))
            if fields['scheme']=='day':ci.extend([dict(**fields,**v) for v in clustered(g,args.bootstrap)])
    pd.DataFrame(metrics).to_csv(args.out/'metrics.csv',index=False)
    pd.DataFrame(ci).to_csv(args.out/'uncertainty.csv',index=False)
    fast_reacquire(args.db,args.out)
    supplemental(d,args.out,args.bootstrap)
    provenance_inventory(args.db,args.out)
    economic=[]
    for cost in [.10,.14,.20,.30]:
        for funding in [0,.01,.03]:
            v=premium.net_proxy+args.roundtrip_cost_pct+args.funding_cost_pct-cost-funding
            economic.append(dict(roundtrip_cost_pct=cost,funding_cost_pct=funding,n=int(v.notna().sum()),net_ev_proxy=v.mean()))
    pd.DataFrame(economic).to_csv(args.out/'cost_sensitivity.csv',index=False)
    limitations={
        'source_db_sha256':hashlib.file_digest(args.db.open('rb'),'sha256').hexdigest(),
        'matrix_sha256':hashlib.file_digest(args.matrix.open('rb'),'sha256').hexdigest(),
        'premium_n':len(premium),'quality':premium.quality_class.value_counts().to_dict(),
        'first_events':premium.first_event.fillna('MISSING').value_counts().to_dict(),
        'tp_filled_proxy_n':int(premium.tp_label.notna().sum()),
        'predictor_values_with_verified_time':int(x.notna().sum().sum()),
        'net_ev_kind':'ENTRY_TOUCH_AND_LEVEL_EXIT_PROXY_NOT_REAL_PNL',
        'funding':'ASSUMPTION_NOT_MEASURED','listing_age':'MISSING',
        'execution_scenarios':{s:'UNAVAILABLE_CONTINUOUS_EXECUTABLE_BID_ASK_PATH' for s in ['current_band','wider_0.1pct','wider_0.25pct','first_executable_ask']},
        'long_labels':'OBSERVED_LOWER_BOUND; zeros mean threshold not observed, not verified non-runner',
        'score_brier':'N/A for raw score; only train-calibrated model has Brier',
        'conditional_bands':'DESCRIPTIVE_ASSOCIATION_NOT_CAUSAL; not proof of forward effect',
        'production_ready':False,'deployed':False,
    }
    (args.out/'audit.json').write_text(json.dumps(limitations,indent=2),encoding='utf8')
    print(json.dumps(limitations,indent=2))

if __name__=='__main__':main()
