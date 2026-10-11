"""Read-only exact-source feasibility audit, NOT a runtime receipt observer.
No target import, compile, eval, selector execution, network, database or mutation.
"""
import ast
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / 'binance_momentum_bot'
HERE = Path(__file__).resolve().parent
EXPECTED = {
 'bot.py':'93797adab9189df2b7780a006a2e59604114ffcdfb0c898d6768516af79d6e1a',
 'early_v2_adapter.py':'fb69fa58858f9458876554db5d9d373602006ae3172283b6e7ec1f72fe2a948f',
 'early_autotrader_v2.py':'9713db2d04f5d1f91622bc7097ed4afa7ea6a703e26042305e7c9da590356dce',
 'startup.py':'2da57131c324ad5edb012b5152abeda2f1f0ac02df1f219af1ed6657ae3457ba',
}
BLOCKER = 'V2_ATTESTATION_IMPLEMENTATION_BLOCKED_NO_NONINTERFERING_SOURCE_READ_WITNESS'


def read_sources():
    sources = {}
    for name, digest in EXPECTED.items():
        raw = (APP / name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError('EXACT_SOURCE_HASH_MISMATCH:' + name)
        sources[name] = (raw.decode('utf8'), ast.parse(raw, filename=name))
    return sources


def function(tree, name, owner=None):
    body = tree.body
    if owner is not None:
        body = next(n for n in body if isinstance(n, ast.ClassDef) and n.name == owner).body
    return next(n for n in body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name)


def name_reads(node, name):
    return sorted(n.lineno for n in ast.walk(node)
                  if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load) and n.id == name)


def attr_reads(node, attr):
    return sorted(n.lineno for n in ast.walk(node)
                  if isinstance(n, ast.Attribute) and isinstance(n.ctx, ast.Load) and ast.unparse(n) == attr)


def direct_local_captures(node, expression):
    result = []
    for n in ast.walk(node):
        if isinstance(n, (ast.Assign, ast.AnnAssign, ast.NamedExpr)):
            value = n.value
            if value is not None and ast.unparse(value) == expression:
                result.append(n.lineno)
    return sorted(result)


def audit():
    sources = read_sources()
    bot = sources['bot.py'][1]
    pilot = sources['early_autotrader_v2.py'][1]
    adapter = sources['early_v2_adapter.py'][1]
    gate = function(bot, 'early_notify_pass')
    scorer = function(bot, 'ignition_shadow_score')
    selector = function(pilot, 'record_selector', 'Pilot')
    configure = function(pilot, 'configure', 'Pilot')
    arm = function(adapter, 'arm', 'Integration')
    reads = [
      {'setting':'EARLY_NOTIFY_MAX_BUY30','file':'bot.py','function':'early_notify_pass','lines':name_reads(gate,'EARLY_NOTIFY_MAX_BUY30'),'existing_direct_local_captures':direct_local_captures(gate,'EARLY_NOTIFY_MAX_BUY30')},
      {'setting':'FAST_EARLY_V2_SCORE','file':'bot.py','function':'ignition_shadow_score','lines':name_reads(scorer,'FAST_EARLY_V2_SCORE'),'existing_direct_local_captures':direct_local_captures(scorer,'FAST_EARLY_V2_SCORE')},
      {'setting':'IGNITION_V2_MIN_SCORE','file':'bot.py','function':'ignition_shadow_score','lines':name_reads(scorer,'IGNITION_V2_MIN_SCORE'),'existing_direct_local_captures':direct_local_captures(scorer,'IGNITION_V2_MIN_SCORE')},
      {'setting':'V2_min_score','file':'early_autotrader_v2.py','function':'Pilot.record_selector','lines':attr_reads(selector,'self.cfg.min_score'),'existing_direct_local_captures':direct_local_captures(selector,'self.cfg.min_score')},
    ]
    publications = []
    for name, (source, tree) in sources.items():
        for n in ast.walk(tree):
            targets = n.targets if isinstance(n,ast.Assign) else [n.target] if isinstance(n,(ast.AnnAssign,ast.AugAssign)) else []
            for target in targets:
                expr = ast.unparse(target)
                if expr in {'EARLY_NOTIFY_MAX_BUY30','FAST_EARLY_V2_SCORE','IGNITION_V2_MIN_SCORE','self.cfg','self.generation'}:
                    publications.append({'file':name,'line':n.lineno,'target':expr,'statement':ast.get_source_segment(source,n)})
    calls = [{'line':n.lineno,'callee':ast.unparse(n.func)} for n in ast.walk(arm) if isinstance(n,ast.Call)]
    assumptions = {'base_hashes_exact':True,'current_disk_is_live_code':False,'writer_completeness':False,'immutable_global_ownership_proved':False,'runtime_UUID_created':False,'source_read_replaced':False,'application_imported':False,'selector_executed':False}
    return {'main_result':BLOCKER,'result_class':'B','scope':'Static exact-source feasibility; no live code or production acceptance','reads':reads,'publications':sorted(publications,key=lambda x:(x['file'],x['line'])),'arm_calls':sorted(calls,key=lambda x:x['line']),'configure_statements':[{'line':n.lineno,'statement':ast.get_source_segment(sources['early_autotrader_v2.py'][0],n)} for n in configure.body],'assumptions':assumptions,'reason':'Actual operands are consumed inline and not retained in existing locals/results. Additional reads cannot establish operand identity. Local capture/wrapper/read replacement is expressly prohibited by this Work. No certified immutable-owner alternative exists.'}


if __name__ == '__main__':
    print(json.dumps(audit(),ensure_ascii=False,indent=2))
