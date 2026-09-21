"""Quote-path counterfactual; never calls an exchange or manufactures missing fills."""
import math

def replay(quotes, *, decision_ms, deadline_ms, entry_low, entry_high, target, stop,
           scenario='current_band', widen_pct=0., fee_pct=.05, slip_pct=.02,
           funding_pct=None, path_complete=False, freshness_ms=3000):
    if scenario not in ('current_band','wider_band','marketable_limit'):raise ValueError('unknown scenario')
    if not (0<stop<target and 0<entry_low<=entry_high):raise ValueError('invalid levels')
    high=entry_high*(1+widen_pct/100);low=entry_low*(1-widen_pct/100)
    out=dict(scenario=scenario,kind='EXECUTION_PROXY_NOT_REAL_PNL',fill_price=None,fill_ms=None,
             exit_price=None,event='NO_FILL',mae_pct=None,gross_pct=None,net_ev_proxy=None,
             fee_pct=None,slippage_assumption_pct=slip_pct,funding_pct=funding_pct,
             path_complete=path_complete)
    previous=None;entry=None;mae=0.;last=None
    for q in sorted(quotes,key=lambda x:(x['received_ms'],x['event_ms'])):
        event=q['event_ms'];received=q['received_ms']
        bid=q['bid'];ask=q['ask']
        if not all(math.isfinite(x) for x in (event,received,bid,ask)):continue
        if event<=decision_ms or received>deadline_ms or event>received:continue
        if received-event>freshness_ms or bid<=0 or ask<bid:continue
        if previous is not None and event<=previous:continue
        previous=event;last=bid
        if entry is None:
            if bid>=target:out['event']='TARGET_BEFORE_ENTRY';break
            if bid<=stop:out['event']='INVALIDATION_BEFORE_ENTRY';break
            eligible=ask<=high if scenario=='marketable_limit' else low<=ask<=high
            if not eligible:continue
            entry=ask*(1+slip_pct/100)
            # A capped marketable limit cannot fill above its limit after impact.
            if entry>high:entry=None;continue
            out.update(fill_price=entry,fill_ms=received,event='OPEN_PROXY')
            mae=min(mae,100*(bid/entry-1))
            continue  # A later observed quote is required for the exit.
        mae=min(mae,100*(bid/entry-1))
        if bid>=target or bid<=stop:
            out['event']='TP1' if bid>=target else 'INVALIDATION'
            out['exit_price']=bid*(1-slip_pct/100);break
    if entry is not None:
        out['mae_pct']=mae
        if out['exit_price'] is None:
            out['event']='OPEN_UNPRICED_TIMEOUT'  # last stale quote is never a timeout fill
        else:
            out['gross_pct']=100*(out['exit_price']/entry-1)
            out['fee_pct']=fee_pct*(1+out['exit_price']/entry)
            if funding_pct is not None and path_complete:
                out['net_ev_proxy']=out['gross_pct']-out['fee_pct']-funding_pct
    return out
