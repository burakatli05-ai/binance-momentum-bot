"""Fixtures and differential runner; no application imports, network, SQLite, trading or outcomes."""
import argparse,asyncio,collections,concurrent.futures,contextlib,csv,hashlib,json,math,pathlib,statistics,sys,time,types
from Final_Candidate import build,take,serialize,Sideband,Context,Runtime,replace,disassembly,UNREAD
HERE=pathlib.Path(__file__).resolve().parent
REPO=HERE.parents[1]
class ReadGlobals(dict):
    def __init__(self,log,*a,**kw):super().__init__(*a,**kw);self.log=log
    def __getitem__(self,k):
        v=super().__getitem__(k)
        if k in ('EARLY_NOTIFY_MAX_BUY30','FAST_EARLY_V2_SCORE','IGNITION_V2_MIN_SCORE'):
            self.log.append(['read_global',k,plain(v)])
        return v
def plain(x):
    if type(x) in (type(None),bool,int,float,str):return x
    if type(x) in (list,tuple):return [plain(v) for v in x]
    if type(x) is dict:return {str(k):plain(v) for k,v in x.items()}
    return type(x).__name__
def digest(x):return hashlib.sha256(json.dumps(x,ensure_ascii=False,sort_keys=True).encode()).hexdigest()
class Bomb:
    def __init__(self,log):self.log=log
    def __ge__(self,other):self.log.append(['compare','cap','raises']);raise ValueError('comparison_bomb')
    def __le__(self,other):self.log.append(['compare','score','raises']);raise ValueError('comparison_bomb')
class DB:
    def __init__(self,f):self.f=f
    def execute(self,sql,params):
        self.f.log.append(['sql',list(params)])
        if self.f.scenario=='sql_failure':raise RuntimeError('sql_failure')
    def commit(self):self.f.log.append(['commit'])
    def close(self):self.f.log.append(['close'])
class ConfigRead:
    def __init__(self,f):self.f=f
    @property
    def min_score(self):
        f=self.f;i=f.read_index;f.read_index+=1
        if (f.scenario=='property_raises' and i==0) or (f.scenario=='sql_property_exception' and i==1):
            f.log.append(['read_min_score',i,'RAISE']);raise ValueError('min_score_property')
        vals={'qualification_sql_change':[90.,96.,96.,96.],
              'sql_event_change':[90.,90.,97.,97.],
              'aba':[90.,95.,90.,95.]}.get(f.scenario,[90.,90.,90.,90.])
        val=vals[min(i,len(vals)-1)]
        f.log.append(['read_min_score',i,val])
        if f.scenario=='same_value_republish':f.log.append(['config_republish',i+1,val])
        return val
class PilotFixture:
    mode='OFF'
    def __init__(self,f):self.f=f
    @property
    def cfg(self):self.f.log.append(['read_cfg']);return ConfigRead(self.f)
    def clock(self):self.f.log.append(['existing_clock']);return 123456789
    def connect(self):self.f.log.append(['connect']);return DB(self.f)
    def event(self,event,payload):
        self.f.log.append(['event',event,plain(payload)])
        if self.f.scenario=='event_failure' and event=='SELECTOR_DECISION':raise RuntimeError('event_failure')
    def record_selector(self,*args,**kw):
        r=self.f.functions['record_selector'](self,*args,**kw)
        self.f.log.append(['selector_return',take(r)])
        return r
class Step:
    def __init__(self,f):self.f=f
    def arm(self,*args):
        self.f.log.append(['step_lock',plain(args)])
        if self.f.scenario=='step_lock_exception':raise RuntimeError('step_failure')
class IntegrationFixture:
    def __init__(self,f):
        self.f=f;self.pilot=PilotFixture(f)
        self.step_lock=Step(f) if f.scenario in ('step_lock_branch','step_lock_exception') else None
        self.b={'states':{f.symbol:f.st},'ignition_shadow_score':f.scorer,
                'estimate_trade_plan':lambda *a:dict(entry_high=1,invalidation=.9,target1=1.1)}
        self.queue=types.SimpleNamespace(put_nowait=lambda x:f.log.append(['entry_queue',plain(x)]))
    def _queue_notify(self,*args):
        self.f.log.append(['notify',plain(args)])
        if self.f.scenario=='notify_failure':raise RuntimeError('notify_failure')
    def arm(self,*args,**kw):
        f=self.f;f.log.append(['arm_args',plain(args[:2])])
        if f.scenario=='reentrant_invocations' and not f.children:
            child=Fixture('gate_arm_link',f.candidate,on=f.on,symbol='CHILD',runtime=f.runtime)
            child.run();f.children.append(child)
            f.log.append(['child_transcript',child.log])
        if f.candidate:
            if f.scenario=='context_restart':kw['_w_runtime']=Runtime('fixture-runtime-B')
            if f.scenario=='context_missing':kw['_w_context']=None
            if f.scenario=='context_mismatch':kw['_w_context']=replace(kw['_w_context'],symbol='WRONG')
        r=f.functions['arm'](self,*args,**kw)
        if f.scenario=='context_reuse':
            f.arm_outputs.append(r);f.log.append(['arm_return',plain(take(r))])
            r=f.functions['arm'](self,*args,**kw)
        f.arm_outputs.append(r);f.log.append(['arm_return',plain(take(r))])
        return r
class State(types.SimpleNamespace):
    def __getattribute__(self,key):
        value=super().__getattribute__(key)
        if key in ('episode_id','active_radar_id'):
            super().__getattribute__('_log').append(['identity_read',key,plain(value)])
        return value
class Fixture:
    def __init__(self,scenario,candidate,on=True,symbol='TEST',runtime=None):
        self.scenario=scenario;self.candidate=candidate;self.on=on
        self.runtime=runtime or Runtime('fixture-runtime-A')
        self.symbol='X'*20000 if scenario=='witness_oversize' else symbol
        self.namespace='fixture-runtime-B' if scenario=='restart_namespace' else 'fixture-runtime-A'
        self.log=[];self.arm_outputs=[];self.children=[];self.read_index=0
        self.st=State(_log=self.log,candidate_passes=2,active_radar_id=17,active_radar_notified=False,
            early_alert_ts=0,early_alert_price=0,episode_id=23,candidate_since=1)
        self.m=dict(chg10=.4,chg30=.8,chg60=.9,flow10=2.,flow30=1.5,flow60=1.,
            buy30=.65,rel30=.9,flow_eff30=.7,dist15high_pct=1.2,spread=.05,
            chg5=.1,book_imbalance=.1,extended=False,breakout=True,price=1.)
        if scenario=='gate_reject':self.m['buy30']=.9
        if scenario=='gate_earlier_short_circuit':self.st.candidate_passes=1
        self.scope=ReadGlobals(self.log,{
            '__builtins__':__builtins__,'closing':contextlib.closing,'json':json,'math':math,'asyncio':asyncio,
            'EARLY_NOTIFY_MIN_SCORE':60,'EARLY_NOTIFY_MIN_CHG30':.1,'EARLY_NOTIFY_MIN_CHG60':.1,
            'EARLY_NOTIFY_MIN_FLOW30':1.,'EARLY_NOTIFY_MIN_BUY30':.5,'EARLY_NOTIFY_MAX_BUY30':.8,
            'EARLY_NOTIFY_MAX_CHG5':1.,'EARLY_ALERT_MAX_BOOK':1.,'MAX_SPREAD_PCT':.3,
            'FAST_EARLY_V2_SCORE':101 if scenario=='fast_false' else 85,'IGNITION_V2_MIN_SCORE':80,
            'EARLY_ALERT_COOLDOWN_SECONDS':60,
            'next_daily_notice_no':lambda *a: self.side('notice',a,return_value=1),
            'mark_radar_notified':lambda *a:self.side('mark_radar',a),
            'quality_arm':self.quality,
            'funnel_hit':lambda *a:self.side('funnel',a),
            'save_candidate_event':lambda *a:self.side('candidate_event',a),
            'early_checkpoint_shadow':types.SimpleNamespace(public_early=lambda *a,**kw:self.side('checkpoint',kw)),
            'db_connect':lambda: (_ for _ in ()).throw(AssertionError('forbidden DB')),
            '_arm_stage_entry':lambda *a,**kw:self.side('stage_entry',[a,kw]),
            'telegram_public_alert':self.telegram,
            'build_early_message':lambda *a:'fixture_message',
            'flag':lambda name: scenario!='on_disabled_notify'})
        if scenario=='global_invalid_type':self.scope['EARLY_NOTIFY_MAX_BUY30']='invalid'
        if scenario=='global_comparison_exception':self.scope['EARLY_NOTIFY_MAX_BUY30']=Bomb(self.log)
        if scenario=='scorer_exception':self.scope['FAST_EARLY_V2_SCORE']=Bomb(self.log)
        self.functions,self.ast_nodes=build(REPO,candidate,self.scope)
        self.scope['early_notify_pass']=self.gate
        self.adapter=IntegrationFixture(self)
        self.result=None;self.exception=None;self.direct_output=None
    def side(self,name,args,return_value=None):
        self.log.append([name,plain(args)])
        if name=='mark_radar' and self.scenario=='episode_before_quality':
            self.st.episode_id=24;self.log.append(['fixture_episode_write',24])
        return return_value
    def quality(self,*args):
        self.side('quality_arm',args)
        if self.scenario=='interrupted_before_arm':raise RuntimeError('interrupted_before_arm')
        if self.scenario=='identity_gap':self.st.active_radar_id=18
        if self.scenario in ('episode_persistent','episode_aba'):
            self.st.episode_id=24;self.log.append(['fixture_episode_write',24])
            if self.scenario=='episode_aba':
                self.st.episode_id=23;self.log.append(['fixture_episode_write',23])
    async def telegram(self,*args,**kw):self.side('public_notify',[args,kw])
    def gate(self,*args,**kw):
        r=self.functions['early_notify_pass'](*args,**kw)
        self.log.append(['gate_return',plain(take(r))]);self.direct_output=r
        return r
    def scorer(self,*args,**kw):
        r=self.functions['ignition_shadow_score'](*args,**kw)
        self.log.append(['scorer_return',plain(take(r))]);return r
    def run(self):
        try:
            kw={'_w_on':self.on} if self.candidate and self.scenario!='legacy_api' else {}
            if self.scenario=='missing_witness':
                self.result=take(self.adapter.arm(17,self.symbol,self.m,80,**dict(kw,**({'_w_runtime':self.runtime} if self.candidate else {}))))
            elif self.scenario=='qualification_short_circuit':
                args=('17',self.symbol,123456789,1.,80,100,'NONE',[],{})
                self.direct_output=self.adapter.pilot.record_selector(*args,**kw)
                self.result=take(self.direct_output)
            else:
                coro=self.functions['public_driver'](self.m,80,self.st,self.symbol,100000,None,self.adapter,**dict(kw,**({'_w_runtime':self.runtime} if self.candidate else {})))
                try:
                    yielded=coro.send(None)
                except StopIteration as end:self.result=end.value
                else:
                    coro.close();raise AssertionError('Unexpected real suspension '+repr(yielded))
        except Exception as e:self.exception=[type(e).__name__,str(e)]
        self.log.append(['driver_end',plain(self.result),self.exception])
        return self
    def report(self):
        sb=self.arm_outputs[-1] if self.arm_outputs else self.direct_output
        witness=serialize(sb,self.namespace)
        return dict(result=plain(self.result),exception=self.exception,reads=[x for x in self.log if x[0] in ('read_global','read_cfg','read_min_score','identity_read')],
            comparisons=[x for x in self.log if x[0] in ('gate_return','scorer_return','selector_return','compare')],
            branch_path=[x[0]+(':'+x[1] if x[0]=='event' else '') for x in self.log],
            transcript=self.log,final_state=plain({k:v for k,v in vars(self.st).items() if k!='_log'}),final_m=plain(self.m),witness=witness)
def pairs(r):return dict(r.get('witness',{}).get('operands',[]))
def run_tests(output):
    plan=json.loads((HERE/'Final_Test_Plan.json').read_text(encoding='utf-8'))
    results=[];traces=[]
    for test in plan['tests']:
        s=test['scenario'];on=s not in ('observer_off','legacy_api')
        if s=='concurrent_invocations':
            def group(candidate):
                runtime=Runtime('fixture-runtime-A')
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                    return list(pool.map(lambda sym:Fixture('gate_arm_link',candidate,symbol=sym,runtime=runtime).run(),['ONE','TWO']))
            originals=group(False);candidates=group(True)
        else:
            subcases=test.get('subcases',[s])
            originals=[Fixture(x,False,on=on).run() for x in subcases]
            shared=Runtime('fixture-runtime-A')
            candidates=[Fixture(x,True,on=on,runtime=shared).run() for x in subcases]
        comparisons=[];expectations=[]
        for o,c in zip(originals,candidates):
            a,b=o.report(),c.report()
            eq={k:a[k]==b[k] for k in ('result','exception','reads','comparisons','branch_path','transcript','final_state','final_m')}
            eq['read_count']=len(a['reads'])==len(b['reads'])
            eq['read_order']=a['reads']==b['reads']
            for name,tags in [('notification',('notify','public_notify')),('sql',('sql','connect','commit','close')),('step_lock',('step_lock',))]:
                eq[name]=[x for x in a['transcript'] if x[0] in tags]==[x for x in b['transcript'] if x[0] in tags]
            w=pairs(b);rmin=[x for x in b['reads'] if x[0]=='read_min_score']
            expected={}
            if s in ('global_numeric','fast_true','min_score_normal','gate_arm_link','reentrant_invocations','concurrent_invocations','observer_off','legacy_api'):
                expected['lower_unread']=not any(x[:2]==['read_global','IGNITION_V2_MIN_SCORE'] for x in b['reads'])
            if s=='fast_false':
                expected['lower_once']=sum(x[:2]==['read_global','IGNITION_V2_MIN_SCORE'] for x in b['reads'])==1
                expected['lower_witness']=w.get('lower')==80
            if s in ('global_invalid_type','global_comparison_exception'):
                expected['raised']=b['exception'] is not None and b['exception'][0]==('TypeError' if s=='global_invalid_type' else 'ValueError')
            if s in ('qualification_sql_change','sql_event_change','aba','same_value_republish','min_score_normal'):
                want={'qualification_sql_change':[90.,96.,96.,96.],'sql_event_change':[90.,90.,97.,97.],'aba':[90.,95.,90.,95.]}.get(s,[90.]*4)
                expected['four_separate_reads']=[x[2] for x in rmin]==want
                expected['four_exact_witnesses']=[w.get(k) for k in ('qual','sql','event','notify')]==want
            if s in ('property_raises','sql_property_exception'):
                expected['property_raised']=bool(rmin) and rmin[-1][2]=='RAISE'
                expected['existing_arm_catch']=any(x[:2]==['event','CANDIDATE_REJECTED'] for x in b['transcript'])
            if s in ('interrupted_before_arm','gate_reject','gate_earlier_short_circuit'):
                expected['no_arm']=len(c.arm_outputs)==0
            if s=='gate_earlier_short_circuit':expected['no_cap']=not any(x[:2]==['read_global','EARLY_NOTIFY_MAX_BUY30'] for x in b['reads'])
            if s in ('gate_arm_link','reentrant_invocations','concurrent_invocations'):
                ctx=c.arm_outputs[-1].context if c.arm_outputs else None
                expected['context_exact']=type(ctx) is Context and ctx.status=='EXACT_LOCAL_CHAIN' and (ctx.radar,ctx.episode,ctx.symbol)==(17,23,c.symbol)
                expected['cap_exact']=type(ctx) is Context and ctx.cap==.8
                if s=='reentrant_invocations':expected['nonce_isolated']=type(ctx) is Context and bool(c.children) and ctx.nonce is not c.children[0].arm_outputs[-1].context.nonce
            if s=='missing_witness':expected['unknown']=b['witness']['status']=='UNKNOWN_MISSING_WITNESS'
            if s=='restart_namespace':expected['unknown']=b['witness']['status']=='UNKNOWN_RESTART_NAMESPACE'
            if s=='identity_gap':expected['unknown']=b['witness']['status']=='UNKNOWN_IDENTITY_GAP'
            if s=='witness_oversize':expected['unknown']=b['witness']['status']=='UNKNOWN_OVERSIZE'
            if s in ('observer_off','legacy_api'):
                expected['no_sideband']=all(type(x) is not Sideband for x in c.arm_outputs) and type(c.direct_output) is not Sideband
            if s=='qualification_short_circuit':
                expected['qualification_unread']=len(rmin)==1 and w.get('qual')==UNREAD and w.get('sql')==90
            if s=='on_disabled_notify':expected['notify_unread']=len(rmin)==3 and w.get('notify')==UNREAD
            if s in ('step_lock_branch','step_lock_exception'):
                order=b['branch_path'];expected['step_first']='step_lock' in order and 'scorer_return' in order and order.index('step_lock')<order.index('scorer_return')
            if s in ('notify_failure','sql_failure','event_failure','scorer_exception'):
                expected['existing_rejection']=any(x[:2]==['event','CANDIDATE_REJECTED'] for x in b['transcript'])
            if s=='serialized_gate_cap':
                expected['serialized_consumed_cap']=w.get('cap')==.8 and b['witness'].get('binary64',{}).get('cap')==(.8).hex()
                expected['one_cap_read']=sum(x[:2]==['read_global','EARLY_NOTIFY_MAX_BUY30'] for x in b['reads'])==1
            if s=='serialized_invocation_identity':
                ident=b['witness'].get('identity') or {}
                expected['serialized_identity']=ident.get('invocation_identity')==[ident.get('runtime_namespace'),ident.get('decision_sequence')] and type(ident.get('decision_sequence')) is int and ident.get('scope')=='RUNTIME_LOCAL_GATE_ATTEMPT_NOT_DURABLE_RECEIPT'
            if s in ('episode_mutation','context_lifecycle'):
                expected['unknown_no_exact']=b['witness']['status'].startswith('UNKNOWN_')
                expected['continuity_not_claimed']=not (b['witness'].get('gate_to_arm') or {}).get('continuity_proven',False)
                if c.scenario=='episode_persistent':expected['change_detected']=b['witness']['status']=='UNKNOWN_EPISODE_CHANGE'
                if c.scenario=='context_reuse':expected['reuse_detected']=b['witness']['status']=='UNKNOWN_REUSED_CONTEXT'
                if c.scenario=='context_restart':expected['restart_detected']=b['witness']['status']=='UNKNOWN_RESTART_NAMESPACE'
                if c.scenario=='context_missing':expected['missing_detected']=b['witness']['status']=='UNKNOWN_MISSING_WITNESS'
                if c.scenario=='context_mismatch':expected['mismatch_detected']=b['witness']['status']=='UNKNOWN_IDENTITY_GAP'
            comparisons.append(eq);expectations.append(expected)
            traces.append(dict(id=test['id'],subcase=c.scenario,symbol=c.symbol if len(c.symbol)<100 else 'OVERSIZE_FIXTURE',original=a,candidate=b))
        if s=='concurrent_invocations':
            expectations.append(dict(nonce_isolation=all(c.arm_outputs for c in candidates) and candidates[0].arm_outputs[-1].context.nonce is not candidates[1].arm_outputs[-1].context.nonce))
        if s=='serialized_invocation_identity':
            identities=[c.report()['witness']['identity'] for c in candidates]
            expectations.append(dict(sequences_one_two=[i['decision_sequence'] for i in identities]==[1,2],
                same_runtime=len({i['runtime_namespace'] for i in identities})==1))
        if s=='reentrant_invocations':
            cs=candidates[0];parent=cs.arm_outputs[-1].context;child=cs.children[0].arm_outputs[-1].context
            expectations.append(dict(shared_owner=parent.grant.owner is child.grant.owner,
                unique_runtime_sequences=parent.grant.sequence!=child.grant.sequence))
        if s=='concurrent_invocations':
            contexts=[c.arm_outputs[-1].context for c in candidates]
            expectations.append(dict(shared_owner=contexts[0].grant.owner is contexts[1].grant.owner,
                unique_runtime_sequences=len({c.grant.sequence for c in contexts})==2))
        passed=all(all(x.values()) for x in comparisons+expectations)
        results.append(dict(**test,status='PASS' if passed else 'FAIL',comparisons=comparisons,expectations=expectations,
            original_transcript_sha256=digest([f.log for f in originals]),candidate_transcript_sha256=digest([f.log for f in candidates])))
    result=dict(unique_tests=len(results),passed=sum(x['status']=='PASS' for x in results),failed=sum(x['status']=='FAIL' for x in results),
                local_python=sys.version,production_pass=False,parent_required_tests_run=0,results=results)
    output.mkdir(parents=True,exist_ok=True)
    (output/'Final_Test_Results.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    (output/'Final_Raw_Transcripts.json').write_text(json.dumps(traces,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    with (output/'Final_Adversarial_Read_Results.csv').open('w',encoding='utf-8',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=['id','scenario','status','original_transcript_sha256','candidate_transcript_sha256','comparisons','expectations'])
        writer.writeheader()
        for r in results:writer.writerow({k:json.dumps(r[k],ensure_ascii=False) if isinstance(r[k],list) else r[k] for k in writer.fieldnames})
    print(json.dumps({k:v for k,v in result.items() if k!='results'},indent=2))
    for r in results:
        if r['status']=='FAIL': print(json.dumps(r,ensure_ascii=False))
    return result,traces
def benchmark(output):
    contract=json.loads((HERE/'Final_Performance_Budget.json').read_text(encoding='utf-8'))
    fixtures=[Fixture('gate_arm_link',False),Fixture('gate_arm_link',True,on=False),Fixture('gate_arm_link',True,on=True)]
    def call(f):
        f.log.clear();f.arm_outputs.clear();f.read_index=0
        f.st.active_radar_notified=False;f.st.early_alert_ts=0
        t=time.perf_counter_ns();f.run()
        if f.candidate and f.on:serialize(f.arm_outputs[-1],f.namespace)
        elapsed=time.perf_counter_ns()-t
        assert f.exception is None and len(f.arm_outputs)==1
        return elapsed
    for _ in range(contract['warmup_each']):
        for f in fixtures:call(f)
    samples=[]
    for i in range(contract['paired_samples']):
        row={'sample':i}
        for j in ((i%3),(i+1)%3,(i+2)%3):
            row[contract['variants'][j]]=call(fixtures[j])
        samples.append(row)
    def stat(values):
        values=sorted(values);return dict(p50_ns=int(statistics.median(values)),p99_ns=values[math.ceil(len(values)*.99)-1],max_ns=max(values))
    report=dict(sample_pairs=len(samples),timed_invocations=3*len(samples),warmups_per_variant=contract['warmup_each'],local_python=sys.version,production_latency_pass=False,variants={v:stat([r[v] for r in samples]) for v in contract['variants']},deltas={})
    checks={}
    for v in contract['variants'][1:]:
        stats=stat([max(0,r[v]-r['parent_original']) for r in samples]);report['deltas'][v]=stats
        key='capture_off' if v=='capture_off' else 'capture_on_serialized'
        checks[v+'_p99']=stats['p99_ns']<=contract['budgets_ns'][key+'_p99_positive_delta']
        checks[v+'_max']=stats['max_ns']<=contract['budgets_ns'][key+'_max_positive_delta']
        report['variants'][v]['p50_ratio']=report['variants'][v]['p50_ns']/report['variants']['parent_original']['p50_ns']
    report.update(checks=checks,status='PASS' if all(checks.values()) else 'FAIL',scope='LOCAL_FIXTURE_MICROBENCH_ONLY')
    output.mkdir(parents=True,exist_ok=True)
    (output/'Final_Performance_Result.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    with (output/'Microbenchmark_Raw.csv').open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=['sample']+contract['variants']);w.writeheader();w.writerows(samples)
    original=fixtures[0];candidate=fixtures[1]
    (output/'Bytecode_Comparison.json').write_text(json.dumps(dict(bytecode_identical=False,original=disassembly(original.functions),candidate=disassembly(candidate.functions)),indent=2)+'\n',encoding='utf-8')
    print(json.dumps(report,indent=2))
    return report
