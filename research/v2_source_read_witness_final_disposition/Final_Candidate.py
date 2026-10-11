"""Single final research candidate. Source copies only; no production import or IO."""
import ast,copy,hashlib,json,pathlib,types,dis,itertools,uuid,math
from dataclasses import dataclass,replace
UNREAD='NOT_READ_SHORT_CIRCUIT'
class Runtime:
    """Caller-owned observer state; never global; CPython fixture scope only."""
    def __init__(self,namespace=None):
        self.namespace=namespace or str(uuid.uuid4())
        self.owner=object()
        self.sequence=itertools.count(1)
@dataclass(frozen=True)
class Grant:
    namespace:str
    sequence:int
    symbol:str
    radar:object
    owner:object
    nonce:object
@dataclass(frozen=True)
class Context:
    namespace:str
    nonce:object
    radar:object
    episode:object
    symbol:str
    cap:object
    status:str
    grant:object=None
    observations:tuple=()
    admitted:bool=False
@dataclass(frozen=True)
class Sideband:
    result:object
    operands:tuple
    context:object=None
def take(value):
    return value.result if type(value) is Sideband else value
def finish(result,operands,context=None):
    return Sideband(result,operands,context)
def begin(runtime,symbol,radar,on):
    if not on or type(runtime) is not Runtime:return None
    return Grant(runtime.namespace,next(runtime.sequence),symbol,radar,runtime.owner,iter((runtime.owner,)))
def context(gate,gate_radar,arm_radar,episodes,symbol,runtime,on):
    if not on:return None
    grant=gate.context if type(gate) is Sideband else None
    cap=dict(gate.operands).get('cap',UNREAD) if type(gate) is Sideband else UNREAD
    valid=(type(grant) is Grant and gate.result is True and type(symbol) is str
           and grant.symbol==symbol and type(gate_radar) in (int,str)
           and grant.radar==gate_radar==arm_radar)
    status='UNKNOWN_UNPROVEN_EPISODE_CONTINUITY' if valid else 'UNKNOWN_IDENTITY_GAP'
    # Unequal actual observations refute continuity. Equality cannot establish it.
    if valid and (any(type(x) not in (int,str) or not x for x in episodes) or len(set(episodes))!=1):
        status='UNKNOWN_EPISODE_CHANGE'
    return Context(grant.namespace if type(grant) is Grant else '',
        grant.nonce if type(grant) is Grant else None,arm_radar,episodes[0],symbol,cap,status,grant,episodes)
def admit(ctx,radar,symbol,runtime,on):
    if not on:return None
    if type(ctx) is not Context:return None
    status=ctx.status
    if ctx.admitted:status='UNKNOWN_REUSED_CONTEXT'
    elif type(runtime) is not Runtime or type(ctx.grant) is not Grant:status='UNKNOWN_MISSING_WITNESS'
    elif ctx.namespace!=runtime.namespace or ctx.grant.owner is not runtime.owner:status='UNKNOWN_RESTART_NAMESPACE'
    elif ctx.radar!=radar or ctx.symbol!=symbol:status='UNKNOWN_IDENTITY_GAP'
    elif next(ctx.nonce,None) is not runtime.owner:status='UNKNOWN_REUSED_CONTEXT'
    return replace(ctx,status=status,admitted=True)
def serialize(sb,namespace,max_bytes=16384):
    if type(sb) is not Sideband:return dict(status='OFF_OR_NO_SIDEBAND',bytes=0)
    ctx=sb.context
    status=('UNKNOWN_MISSING_WITNESS' if type(ctx) is not Context else
            'UNKNOWN_RESTART_NAMESPACE' if ctx.namespace!=namespace else ctx.status)
    entries=[];binary64={}
    def add(pairs):
        for k,v in pairs:
            if type(v) is Sideband:add(v.operands)
            elif k.endswith('_side'):entries.append((k,'UNKNOWN_EXCEPTION_OR_GAP'))
            elif type(v) is str and v==UNREAD:entries.append((k,UNREAD))
            elif type(v) in (int,float) and (type(v) is int or math.isfinite(v)):
                entries.append((k,v))
                if type(v) is float:binary64[k]=v.hex()
            else:entries.append((k,'UNKNOWN_UNSUPPORTED_OPERAND'))
    if type(ctx) is Context:add((('cap',ctx.cap),))
    add(sb.operands)
    grant=ctx.grant if type(ctx) is Context else None
    identity=None if type(grant) is not Grant else dict(
        runtime_namespace=grant.namespace,decision_sequence=grant.sequence,
        invocation_identity=[grant.namespace,grant.sequence],
        scope='RUNTIME_LOCAL_GATE_ATTEMPT_NOT_DURABLE_RECEIPT',
        radar=ctx.radar,episode=ctx.episode,symbol=ctx.symbol)
    payload=dict(status=status,operands=entries,binary64=binary64,identity=identity,
        gate_to_arm=None if type(ctx) is not Context else dict(
            gate_radar=grant.radar if type(grant) is Grant else None,
            arm_radar=ctx.radar,episode_read_sites=[6028,6033,6040],
            episode_observations=ctx.observations,gate_episode='NOT_OBSERVED_IN_SOURCE',
            continuity_proven=False,context_admitted_once=ctx.admitted))
    raw=json.dumps(payload,allow_nan=False,separators=(',',':')).encode()
    if len(raw)>max_bytes:return dict(status='UNKNOWN_OVERSIZE',bytes=len(raw))
    return dict(payload,bytes=len(raw))
class Capture(ast.NodeTransformer):
    def __init__(self,name): self.name=name;self.locals=[];self.nested=[]
    def binding(self,node,name):
        if name not in self.locals:self.locals.append(name)
        return ast.copy_location(ast.NamedExpr(ast.Name('_w_'+name,ast.Store()),node),node)
    def visit_Name(self,node):
        names={'EARLY_NOTIFY_MAX_BUY30':'cap','FAST_EARLY_V2_SCORE':'fast','IGNITION_V2_MIN_SCORE':'lower'}
        if isinstance(node.ctx,ast.Load) and node.id in names:return self.binding(node,names[node.id])
        return node
    def visit_Attribute(self,node):
        # Do not descend through the operand after selecting it; original chain remains intact.
        if node.attr=='min_score' and isinstance(node.ctx,ast.Load):
            key={133:'qual',149:'sql',371:'event',385:'notify'}.get(node.lineno)
            if key:return self.binding(node,key)
        return self.generic_visit(node)
    def visit_Call(self,node):
        original_line=node.lineno
        node=self.generic_visit(node)
        if self.name=='arm' and original_line in (357,365):
            key='score_side' if original_line==357 else 'selector_side'
            node.keywords += [ast.keyword('_w_on',ast.Name('_w_on',ast.Load())),ast.keyword('_w_context',ast.Name('_w_context',ast.Load()))]
            self.nested.append(key)
            bound=self.binding(node,key)
            return ast.copy_location(ast.Call(ast.Name('_take',ast.Load()),[bound],[]),node)
        return node
    def visit_Return(self,node):
        # Done after locals discovered, by finalize().
        return self.generic_visit(node)
    def finalize(self,fn):
        keys=self.locals
        class Returns(ast.NodeTransformer):
            def visit_Return(self,node):
                val=node.value or ast.Constant(None)
                pairs=ast.Tuple([ast.Tuple([ast.Constant(k),ast.Name('_w_'+k,ast.Load())],ast.Load()) for k in keys],ast.Load())
                out=ast.Call(ast.Name('_finish',ast.Load()),[val,pairs,ast.Name('_w_context',ast.Load())],[])
                return ast.copy_location(ast.Return(ast.IfExp(ast.Name('_w_on',ast.Load()),out,val)),node)
        fn=Returns().visit(fn)
        init=[ast.Assign([ast.Name('_w_'+k,ast.Store())],ast.Constant(UNREAD)) for k in keys]
        fn.body=init+fn.body
        return fn
def make_candidate(fn):
    fn=copy.deepcopy(fn)
    fn.args.kwonlyargs += [ast.arg('_w_on'),ast.arg('_w_context')]
    fn.args.kw_defaults += [ast.Constant(False),ast.Constant(None)]
    cap=Capture(fn.name)
    fn=cap.visit(fn)
    if fn.name=='arm':fn.body.append(ast.Return(ast.Constant(None)))
    fn=cap.finalize(fn)
    if fn.name=='arm':
        fn.args.kwonlyargs.append(ast.arg('_w_runtime'));fn.args.kw_defaults.append(ast.Constant(None))
        fn.body.insert(0,ast.parse('_w_context = _admit(_w_context, radar_id, symbol, _w_runtime, _w_on)').body[0])
    return fn
class PublicCapture(ast.NodeTransformer):
    def visit_Attribute(self,node):
        if node.attr=='active_radar_id' and node.lineno==6020:
            return ast.copy_location(ast.NamedExpr(ast.Name('_w_gate_radar',ast.Store()),node),node)
        if node.attr=='episode_id' and node.lineno in (6028,6033,6040):
            return ast.copy_location(ast.NamedExpr(ast.Name('_w_episode_'+str(node.lineno),ast.Store()),node),node)
        return self.generic_visit(node)
    def visit_Call(self,node):
        node=self.generic_visit(node)
        if isinstance(node.func,ast.Name) and node.func.id=='early_notify_pass':
            node.keywords += [ast.keyword('_w_on',ast.Name('_w_on',ast.Load())),
                ast.keyword('_w_context',ast.Call(ast.Name('_begin',ast.Load()),[ast.Name('_w_runtime',ast.Load()),ast.Name('symbol',ast.Load()),ast.Name('_w_gate_radar',ast.Load()),ast.Name('_w_on',ast.Load())],[]))]
            return ast.copy_location(ast.Call(ast.Name('_take',ast.Load()),[
                ast.NamedExpr(ast.Name('_w_gate_side',ast.Store()),node)],[]),node)
        if isinstance(node.func,ast.Attribute) and node.func.attr=='arm' and node.lineno==6042:
            node.args[0]=ast.NamedExpr(ast.Name('_w_arm_radar',ast.Store()),node.args[0])
            ctx=ast.Call(ast.Name('_context',ast.Load()),[
                ast.Name('_w_gate_side',ast.Load()),ast.Name('_w_gate_radar',ast.Load()),
                ast.Name('_w_arm_radar',ast.Load()),ast.Tuple([ast.Name('_w_episode_'+str(x),ast.Load()) for x in (6028,6033,6040)],ast.Load()),
                ast.Name('symbol',ast.Load()),ast.Name('_w_runtime',ast.Load()),ast.Name('_w_on',ast.Load())],[])
            node.keywords += [ast.keyword('_w_on',ast.Name('_w_on',ast.Load())),ast.keyword('_w_context',ctx),ast.keyword('_w_runtime',ast.Name('_w_runtime',ast.Load()))]
        return node
def sources(repo):
    inventory=json.loads((pathlib.Path(__file__).parent/'Source_Inventory.json').read_text(encoding='utf-8'))
    hashes={x['path']:x['sha256'] for x in inventory['files']}
    result={}
    for short in ('bot.py','early_autotrader_v2.py','early_v2_adapter.py'):
        rel='binance_momentum_bot/'+short;raw=(pathlib.Path(repo)/rel).read_bytes()
        assert hashlib.sha256(raw).hexdigest()==hashes[rel],rel
        result[short]=ast.parse(raw,filename=rel)
    return result
def build(repo,candidate,scope):
    trees=sources(repo);funcs={}
    for file,name in [('bot.py','early_notify_pass'),('bot.py','ignition_shadow_score'),
                      ('early_autotrader_v2.py','record_selector'),('early_v2_adapter.py','arm')]:
        fn=copy.deepcopy(next(n for n in ast.walk(trees[file]) if isinstance(n,ast.FunctionDef) and n.name==name))
        fn.returns=None
        for arg in fn.args.args:arg.annotation=None
        if candidate:fn=make_candidate(fn)
        funcs[name]=fn
    public=copy.deepcopy(next(n for n in ast.walk(trees['bot.py']) if isinstance(n,ast.If) and n.lineno==6020))
    if candidate:public=PublicCapture().visit(public)
    driver=ast.parse('async def public_driver(m, score, st, symbol, now, session, early_v2, *, _w_on=False, _w_runtime=None):\n    pass').body[0]
    driver.body=[public];funcs['public_driver']=driver
    scope.update(_take=take,_finish=finish,_context=context,_begin=begin,_admit=admit)
    for name,fn in funcs.items():
        module=ast.fix_missing_locations(ast.Module([fn],type_ignores=[]))
        exec(compile(module,'<isolated-source-copy:'+name+'>','exec'),scope)
    return {name:scope[name] for name in funcs},funcs
def disassembly(functions):
    return {name:[dict(offset=i.offset,opname=i.opname,argrepr=i.argrepr) for i in dis.get_instructions(fn)] for name,fn in functions.items()}
