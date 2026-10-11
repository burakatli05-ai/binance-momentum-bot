
import ast,csv,hashlib,json,pathlib,subprocess,datetime
ROOT=pathlib.Path(__file__).resolve().parents[2]
WORK=ROOT/'work/v2-witness-redesign'
REPO=ROOT/'work/v2-witness-redesign-branch'
RD=REPO/'research/v2_source_read_witness_redesign'
RD.mkdir(parents=True,exist_ok=True)
GIT=r'C:\Users\BURAK\.cache\codex-runtimes\codex-primary-runtime\dependencies\native\git\cmd\git.exe'
BASE='29cc15ea261c0a148cd57cee46b986946b4c492f'
PARENT='6be46a2d7a8c63430d35ed748b7dd0067e5a7d71'
def git(*args): return subprocess.check_output([GIT,'-c','safe.directory='+str(REPO),'-c','core.autocrlf=false','-C',str(REPO),*args])
def sha(b): return hashlib.sha256(b).hexdigest()
def save(n,x):
    p=RD/n
    if p.exists(): raise RuntimeError('Append-only freeze exists: '+n)
    p.write_text(json.dumps(x,ensure_ascii=False,indent=2)+'\n',encoding='utf-8',newline='\n')
def csvwrite(n,rows,fields):
    with (RD/n).open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)
files=git('ls-files').decode().splitlines()
sources=[]; inventory=[]; py_count=0
targets={'early_notify_pass','ignition_shadow_score','record_selector','configure','arm'}
readnames={'EARLY_NOTIFY_MAX_BUY30','FAST_EARLY_V2_SCORE','IGNITION_V2_MIN_SCORE','min_score'}
for rel in files:
    if not rel.endswith('.py'): continue
    data=(REPO/rel).read_bytes()
    if rel.startswith(('binance_momentum_bot/','tests/')):
        assert data==git('show',BASE+':'+rel),rel
    try: tree=ast.parse(data,filename=rel)
    except SyntaxError as e: raise RuntimeError(f'Unparsed tracked Python {rel}: {e}')
    py_count+=1
    sources.append(dict(path=rel,bytes=len(data),sha256=sha(data)))
    scope={}
    def walk(node,stack=()):
        name=getattr(node,'name',None)
        if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)): stack=stack+(name,)
        scope[id(node)]='.'.join(stack)
        for child in ast.iter_child_nodes(node): walk(child,stack)
    walk(tree)
    lines=data.decode('utf-8-sig').splitlines()
    for node in ast.walk(tree):
        kind=None;expr=''
        if isinstance(node,ast.Call):
            expr=ast.unparse(node.func)
            tail=expr.split('.')[-1]
            if tail in targets or any(t in expr for t in ('ignition_shadow_score','early_notify_pass','record_selector')):
                kind='CALL'
            elif expr in ('globals','setattr','exec','eval') or expr.endswith('.Integration'): kind='DYNAMIC_OR_CONSTRUCTOR'
        elif isinstance(node,(ast.Assign,ast.AnnAssign,ast.AugAssign)):
            tg=getattr(node,'targets',[getattr(node,'target',None)])
            expr='; '.join(ast.unparse(x) for x in tg if x is not None)
            if any(t in expr for t in readnames) or expr.endswith('.cfg'): kind='WRITER'
        elif isinstance(node,ast.Constant) and isinstance(node.value,str):
            if any(t in node.value for t in ('ignition_shadow_score','early_notify_pass','early_v2.arm','record_selector')) and len(node.value)<8000:
                kind='STRING_REFERENCE';expr=node.value[:180].replace('\n',' ')
        if kind:
            classification=('ARCHIVED_NON_ENTRYPOINT' if 'bot (0509).py' in rel else
              'TEST_FIXTURE_NOT_PRODUCTION' if rel.startswith('tests/') else
              'PRIOR_RESEARCH_NOT_PRODUCTION' if rel.startswith('research/') else 'CURRENT_SOURCE')
            resolution='REVIEW_REQUIRED'
            if kind=='CALL':
                if any(t in expr for t in ('early_notify_pass','ignition_shadow_score','record_selector','configure')) or expr=='early_v2.arm': resolution='TARGET_OR_TEST_CALL'
                else: resolution='OTHER_ARM_API'
            elif kind=='WRITER': resolution='SOURCE_WRITER'
            elif kind=='STRING_REFERENCE': resolution='DATA_OR_FIXTURE_REFERENCE'
            else: resolution='DYNAMIC_SURFACE_REQUIRES_OWNERSHIP_REVIEW'
            inventory.append(dict(file=rel,line=node.lineno,scope=scope[id(node)],kind=kind,expression=expr,classification=classification,resolution=resolution))
inventory.sort(key=lambda x:(x['file'],x['line'],x['kind']))
csvwrite('Call_Site_Coverage_Matrix.csv',inventory,list(inventory[0]))
save('Source_Inventory.json',dict(base=BASE,parent=PARENT,python_files_parsed=py_count,parse_failures=0,files=sources))
sites=[
('G1','bot.py',5558,'early_notify_pass','EARLY_NOTIFY_MAX_BUY30','LOAD_GLOBAL','0 or 1','after prior and terms and lower-bound comparison','_w_cap','gate sideband -> lexical caller -> arm context','TypeError or comparison hook; earlier exception means unread'),
('S1','bot.py',4272,'ignition_shadow_score','FAST_EARLY_V2_SCORE','LOAD_GLOBAL','1 if reached','after score computation','_w_fast','scorer sideband -> arm','TypeError or reflected comparator'),
('S2','bot.py',4272,'ignition_shadow_score','IGNITION_V2_MIN_SCORE','LOAD_GLOBAL','0 or 1','only FAST comparison false','_w_lower','scorer sideband -> arm','FAST true: NOT_READ_SHORT_CIRCUIT'),
('Q1','early_autotrader_v2.py',133,'Pilot.record_selector','self.cfg.min_score','LOAD_FAST self; LOAD_ATTR cfg; LOAD_ATTR min_score','0 or 1','FAST label and finite numeric score','_w_qual','selector sideband -> arm','cfg/property/comparison can raise'),
('Q2','early_autotrader_v2.py',149,'Pilot.record_selector','self.cfg.min_score','LOAD_FAST self; LOAD_ATTR cfg; LOAD_ATTR min_score','1 if reached','separate SQL argument evaluation then float','_w_sql','selector sideband -> arm','read/float/SQL/commit can raise'),
('A1','early_v2_adapter.py',371,'Integration.arm','self.pilot.cfg.min_score','LOAD_FAST self; LOAD_ATTR pilot; LOAD_ATTR cfg; LOAD_ATTR min_score','1 if reached','after selector returns; event dict evaluation','_w_event','arm sideband','attribute/event exception handled by existing arm catch'),
('A2','early_v2_adapter.py',385,'Integration.arm','self.pilot.cfg.min_score','LOAD_FAST self; LOAD_ATTR pilot; LOAD_ATTR cfg; LOAD_ATTR min_score','0 or 1','qualified and notify flag; f-string format g','_w_notify','arm sideband','attribute/format/notify exception handled by existing arm catch')]
rows=[]
for sid,file,line,scope,operand,bc,count,condition,local,prop,exc in sites:
    rows.append(dict(site=sid,file='binance_momentum_bot/'+file,line=line,scope=scope,operand=operand,original_bytecode=bc,read_count=count,condition=condition,current_surface='operand not retained; result only',proposed_surface='same load then COPY/STORE_FAST '+local,propagation=prop,exceptions=exc,comparison_side_effects='original comparator/truth conversion unchanged; custom finalizers/arbitrary scheduling excluded',evaluation_order='Python left-to-right; chained comparison middle evaluated once; nested conditional lazy'))
csvwrite('Exact_Read_Site_Map.csv',rows,list(rows[0]))
tests=[
('M01','global_numeric','ordinary cap numeric'),
('M02','global_invalid_type','cap str raises TypeError'),
('M03','global_comparison_exception','reflected comparator raises ValueError'),
('M04','fast_true','FAST true: lower read zero'),
('M05','fast_false','FAST false: lower read once'),
('M06','min_score_normal','distinct normal four min_score reads'),
('M07','property_raises','qualification property raises'),
('M08','qualification_sql_change','qualification=90 SQL=96'),
('M09','sql_event_change','qualification=90 SQL=90 event=97'),
('M10','aba','min_score 90 -> 95 -> 90 -> 95'),
('M11','same_value_republish','new config generation each read, same90'),
('M12','interrupted_before_arm','quality_arm fixture raises before arm'),
('M13','gate_reject','buy exceeds cap: no arm'),
('M14','gate_arm_link','immutable token exact same radar/episode/symbol'),
('M15','reentrant_invocations','nested invocation distinct token; no registry'),
('M16','concurrent_invocations','two threads independent state; exact own token'),
('M17','scorer_exception','FAST comparator raises in arm catch'),
('M18','notify_failure','existing _queue_notify raises'),
('M19','sql_failure','SQL stub execute raises'),
('M20','step_lock_branch','StepLock branch executes before scorer'),
('M21','missing_witness','direct arm without gate context: UNKNOWN'),
('M22','witness_oversize','oversize metadata rejected by witness serializer'),
('M23','observer_off','capture locals present; no sideband/token/serialization'),
('M24','restart_namespace','old namespace context rejected: UNKNOWN'),
('M25','gate_earlier_short_circuit','candidate passes<2 cap unread'),
('M26','qualification_short_circuit','invalid label qualification min_score unread; SQL read preserved'),
('M27','sql_property_exception','second min_score read raises; no reuse'),
('M28','step_lock_exception','StepLock failure existing event then scorer'),
('M29','event_failure','selector event fails; existing rejection event'),
('M30','legacy_api','default calls retain bool/tuple/bool/None return shapes'),
('M31','identity_gap','radar changes before arm: context UNKNOWN; strategy unchanged'),
('M32','on_disabled_notify','notify flag false -> notify min_score unread')]
model=dict(id='CLOSED_SOURCE_SCRIPTED_OBSERVABLE_MODEL_V1',
  includes=['exact parsed source functions and entire public gate If subtree','built-in primitive config values on known env/JSON paths','scripted read/compare/SQL/event/notify exceptions','scripted mutations at existing side-effect boundaries','separate invocations with independent inputs','original application exception handlers'],
  excludes=['arbitrary scheduler-dependent shared config mutation between added instructions','signals/asynchronous exceptions','memory exhaustion/allocation failure','tracing/profiling/custom frame inspection','custom refcount finalizers or mutable threshold object lifetime semantics','production loaded-function ownership attestation','external injected runtime writers'],
  arbitrary_concurrent_mutation='UNKNOWN',bytecode_identical=False,
  identity='same lexical caller; token object namespace/radar/episode/symbol; unknown on missing/gap/restart/mismatch; no post-hoc recovery',
  unsupported_operand='type must be exact int/float for serializable exact value; otherwise UNKNOWN; original comparison still runs')
budget=dict(candidate_count=1,proposed_application_files=['binance_momentum_bot/bot.py','binance_momentum_bot/early_v2_adapter.py','binance_momentum_bot/early_autotrader_v2.py','binance_momentum_bot/v2_witness_boundary.py (proposed only)'],
 max_proposed_files=4,new_strategy_locks=0,new_strategy_transactions=0,new_strategy_retries=0,new_strategy_clocks=0,global_mutable_registry=0,source_db_schema_or_writes=0,threshold_change=0,economic_model_change=0,production_implementation=0,exceed_action='REJECT_NO_EXPANSION')
sem=dict(contract_id='V2_SOURCE_READ_WITNESS_BOUNDARY_V2_CANDIDATE',status='PRETEST_PROPOSAL_NOT_LOCKED',
 parent_contract_id='MINIMUM_IN_PROCESS_RUNTIME_ATTESTATION_REQUIREMENTS_V1',
 parent_sha256='36cdcc504808d37ecb2d602262f30b4f4f63df18839761fb98c1518cdea38b8f',
 parent_blocker='No replacement of strategy reads prevented v1 implementation. v1 remains unchanged.',
 explicit_version_change='v2 evaluates exact operand local binding and opt-in result sideband, including OFF local store overhead; separate explicit production-code approval required.',
 candidate='ONE_COMPOSITE_EXACT_SITE_LOCAL_CAPTURE_PLUS_EPHEMERAL_CONTEXT_PLUS_OPT_IN_RESULT_SIDEBAND',
 classes=dict(A='candidate capture primitive',B='candidate local identity propagation',C='candidate optional keyword/return sideband; all legacy callers unchanged by default',D='negative control rejected: trace/profile/native/descriptor/monkeypatch/import hook not trusted'),
 capture='AST NamedExpr around existing seven operand loads; no load substitution/re-read; separate local per read',
 propagation='only immutable tuple sidebands and context on local call chain; original strategy result object unwrapped immediately; no strategy value read from context',
 invariants=['strategy output equality','exception type/message equality','threshold/config read count equality','threshold/config read order equality','short-circuit equality','no new threshold read','no removed threshold read','no reuse across separate threshold sites','no new clock','no lock/transaction/retry','no selector rerun','no DB behavior change','no notification/entry/exit change'],
 model=model,engineering_budget=budget,
 decision_precedence=['E identity/access failure','C unresolved required source caller/writer coverage','D any microprobe transcript mismatch or predeclared budget failure','B unacceptable design mechanism/budget','A only all gates satisfied'],
 proof_limits=['No target-runtime bytecode identity claim; local CPython 3.12 versus production reported3.13','No first-decision or receipt generation','No production latency approval'])
save('Frozen_Redesign_Question.json',dict(question='Can actual operands be captured at the same logical read sites under declared model without changing observable strategy transcript?',base=BASE,parent=PARENT,classes=['A','B','C','D'],single_candidate=True,freeze_before_prototype_tests=True,no_outcomes=True,engineering_budget=budget))
save('Source_Read_Semantics_Contract.json',sem)
save('V2_Source_Read_Witness_Boundary_v2_Candidate.json',sem)
(RD/'Candidate_Contract_SHA256.txt').write_text(sha((RD/'V2_Source_Read_Witness_Boundary_v2_Candidate.json').read_bytes())+'  V2_Source_Read_Witness_Boundary_v2_Candidate.json\n',encoding='utf-8',newline='\n')
save('Microprototype_Test_Plan.json',dict(status='FROZEN_BEFORE_PROTOTYPE',unique_tests=len(tests),tests=[dict(id=i,scenario=s,requirement=r) for i,s,r in tests],required_comparisons=['original return','original exception type/message','read order','read count','comparison results','branch/side-effect transcript','witness expected result'],fail_policy='preserve failed run; do not weaken criteria; new semantic regression implies D',parent_T001_T092='NOT_RUN_ALL_92'))
save('OFF_Path_Overhead_Contract.json',dict(status='FROZEN_BEFORE_MEASUREMENT',variants=['parent_original','capture_off','capture_on_serialized'],warmup_each=50,paired_samples=2000,measure='full extracted public-gate/arm path using no-I/O fixture, setup outside timing; perf_counter_ns harness only',budgets_ns={'capture_off_p99_positive_delta':500000,'capture_off_max_positive_delta':20000000,'capture_on_serialized_p99_positive_delta':2000000,'capture_on_serialized_max_positive_delta':50000000},report_ratios=True,production_latency_pass_allowed=False,off={'local_capture_store':True,'context_token':False,'observer_queue_worker_snapshot_io_clock':0}))
save('Pretest_Freeze_Manifest.json',dict(created_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),prototype_exists=(RD/'Isolated_Microprototype.py').exists(),tests_run=0,bench_samples=0,files=[dict(file=p.name,bytes=p.stat().st_size,sha256=sha(p.read_bytes())) for p in sorted(RD.iterdir()) if p.is_file()]))
print(json.dumps(dict(research_dir=str(RD),python_files=py_count,inventory_rows=len(inventory),sites=len(rows),tests=len(tests),candidate_sha256=sha((RD/'V2_Source_Read_Witness_Boundary_v2_Candidate.json').read_bytes())),indent=2))
