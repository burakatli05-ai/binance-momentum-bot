"""Bounded first-cycle Binance DEMO overlap falsification probe.

No bot/config/DB imports; no production adapter. One position, two conditional
creates, one close, one cancel per known stop; no retries. Later cycles are
conditional on this prerequisite, never performed after a failure.
"""
import hashlib
import hmac
import http.client
import ipaddress
import json
import os
from pathlib import Path
import re
import socket
import ssl
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from urllib.parse import urlencode, urlsplit

HOST = 'demo-fapi.binance.com'
BASE = 'https://' + HOST
PRODUCTION_DENY = {'fapi.binance.com','fapi1.binance.com','fapi2.binance.com',
                   'fapi3.binance.com','fapi4.binance.com','api.binance.com',
                   'dapi.binance.com','papi.binance.com','www.binance.com'}
KEY_NAMES = ('BINANCE_DEMO_API_KEY', 'BINANCE_DEMO_API_SECRET')
PUBLIC = {'/fapi/v1/time', '/fapi/v1/exchangeInfo', '/fapi/v1/ticker/price'}
ROUTES = {'GET': PUBLIC | {'/fapi/v3/positionRisk','/fapi/v1/openOrders',
          '/fapi/v1/openAlgoOrders','/fapi/v1/positionSide/dual',
          '/fapi/v1/symbolConfig','/fapi/v1/algoOrder','/fapi/v1/order','/fapi/v3/balance'},
          'POST': {'/fapi/v1/marginType','/fapi/v1/leverage','/fapi/v1/order','/fapi/v1/algoOrder'},
          'DELETE': {'/fapi/v1/algoOrder'}}
WORKING = {'NEW','WORKING'}
TERMINAL = {'CANCELED','CANCELLED','EXPIRED','REJECTED'}
SYMBOL = 'BTCUSDT'

class Blocked(Exception): pass
class ExchangeError(Blocked):
    def __init__(self, status, code):
        self.status, self.code = status, code
        super().__init__('EXCHANGE_ERROR')

def utc(): return datetime.now(timezone.utc).isoformat()
def dec(value):
    if isinstance(value,bool): raise Blocked('INVALID_DECIMAL')
    v=Decimal(str(value))
    if not v.is_finite(): raise Blocked('NONFINITE_DECIMAL')
    return v
def fixed(v): return format(v,'f')
def require(ok, reason):
    if not ok: raise Blocked(reason)
def validate_target(base, method, route):
    p=urlsplit(base)
    require(p.hostname not in PRODUCTION_DENY,'PRODUCTION_HOST_DENIED')
    require(base==BASE and p.scheme=='https' and p.hostname==HOST and
            p.port is None and not p.username and not p.password,'TARGET_DENIED')
    require(method in ROUTES and route in ROUTES[method],'METHOD_ROUTE_DENIED')
def credentials(env):
    require(all(env.get(k) for k in KEY_NAMES),'DEDICATED_CREDENTIAL_MISSING')
    require(all('\n' not in env[k] and '\r' not in env[k] for k in KEY_NAMES),'CREDENTIAL_FORMAT')
    return env[KEY_NAMES[0]],env[KEY_NAMES[1]]
def check_redirect(status): require(not 300<=status<400,'REDIRECT_DENIED')
def check_addresses(addresses):
    require(bool(addresses) and all(ipaddress.ip_address(a[4][0]).is_global for a in addresses),'DNS_DESTINATION_DENIED')
def clean_counts(positions,normal,algo):
    require(isinstance(positions,list) and isinstance(normal,list) and isinstance(algo,list),'ACCOUNT_SCHEMA')
    return dict(position_count=sum(dec(p['positionAmt'])!=0 for p in positions),
                normal_order_count=len(normal),algo_order_count=len(algo))
def require_clean(counts): require(all(v==0 for v in counts.values()),'DIRTY_ACCOUNT')
def require_mode(mode): require(mode.get('dualSidePosition') is False,'ONE_WAY_ONLY')
def verify_stop(o,cid,target,oid=None,working=True):
    require(isinstance(o,dict) and bool(o.get('algoId')),'MISSING_ALGO_ID')
    require(o.get('clientAlgoId')==cid and cid.startswith('DPSL-') and len(cid)<=36,'OWNERSHIP_MISMATCH')
    require(o.get('symbol')==SYMBOL and o.get('side')=='SELL' and o.get('positionSide')=='BOTH','SIDE_SYMBOL_MODE_MISMATCH')
    require(o.get('orderType',o.get('type'))=='STOP_MARKET' and o.get('algoType')=='CONDITIONAL','ORDER_TYPE_MISMATCH')
    require(str(o.get('closePosition')).lower()=='true' and dec(o.get('quantity','0'))==0,'CLOSE_ALL_MISMATCH')
    require(o.get('workingType')=='CONTRACT_PRICE' and dec(o.get('triggerPrice'))==dec(target),'TRIGGER_MISMATCH')
    require(not oid or str(o['algoId'])==str(oid),'ALGO_ID_MISMATCH')
    if working: require(o.get('algoStatus') in WORKING,'STOP_NOT_WORKING')
    return str(o['algoId'])
def verify_both(orders,stops):
    require(len(orders)==2 and {x.get('clientAlgoId') for x in orders}==set(stops),'OVERLAP_NOT_SIMULTANEOUS_OR_EXTRA')
    for o in orders:
        s=stops[o['clientAlgoId']]; verify_stop(o,o['clientAlgoId'],s['target'],s['algo_id'])
def size_for(symbol,price):
    f={x['filterType']:x for x in symbol['filters']}
    market=f['MARKET_LOT_SIZE']; lot=f['LOT_SIZE']; tick=dec(f['PRICE_FILTER']['tickSize'])
    step=max(dec(market['stepSize']),dec(lot['stepSize']))
    minimum=dec(f['MIN_NOTIONAL']['notional'])
    qty=(max(dec(market['minQty']),dec(lot['minQty']),minimum/price)/step).to_integral_value(rounding=ROUND_CEILING)*step
    require(qty>0 and qty<=min(dec(market['maxQty']),dec(lot['maxQty'])),'QUANTITY_FILTER')
    require(qty*price<=250,'DEMO_NOTIONAL_SAFETY_CAP')
    return qty,tick,minimum

def funding_for(balances,notional):
    require(isinstance(balances,list),'BALANCE_SCHEMA')
    rows=[b for b in balances if isinstance(b,dict) and b.get('asset')=='USDT']
    require(len(rows)==1,'USDT_BALANCE_IDENTITY')
    available=dec(rows[0]['availableBalance'])
    # Operational Demo reserve only; does not alter quantity or economic profile.
    required=notional+Decimal('1')
    require(available>=required,'INSUFFICIENT_DEMO_BALANCE')
    return available,required

class DemoConnection(http.client.HTTPSConnection):
    def connect(self):
        validate_target('https://'+self.host,'GET','/fapi/v1/time')
        require(self.port==443 and not self._tunnel_host,'PORT_OR_TUNNEL_DENIED')
        addresses=socket.getaddrinfo(HOST,443,type=socket.SOCK_STREAM);check_addresses(addresses)
        family,kind,proto,_,address=addresses[0]
        raw=socket.socket(family,kind,proto);raw.settimeout(self.timeout)
        try:
            raw.connect(address)
            require(raw.getpeername()[0]==address[0],'PEER_MISMATCH')
            self.sock=self._context.wrap_socket(raw,server_hostname=HOST)
        except BaseException:
            raw.close();raise

class Run:
    def __init__(self,out,run_id,transport=None):
        require(bool(re.fullmatch(r'[a-f0-9]{12}',run_id)),'RUN_ID_FORMAT')
        self.out=Path(out);self.out.mkdir(parents=True,exist_ok=True)
        self.ledger=self.out/'Demo_Order_Event_Ledger.jsonl'
        with self.ledger.open('x',encoding='utf-8'): pass
        self.state={'run_id':run_id,'started_utc':utc(),'phase':'PREFLIGHT',
                    'host':HOST,'symbol':SYMBOL,'account_mode':'BOTH',
                    'requests':0,'writes':0,'signed_requests':0,'production_requests':0,
                    'entry_posts':0,'close_posts':0,'algo_posts':0,'cancel_attempts':0,
                    'entry':None,'stops':{},'pending':[], 'unresolved':[],
                    'cycle_result':'NOT_STARTED','overlap_observed':False,
                    'new_confirmed_before_old_cancel':False,'final_cleanup':None}
        self.transport=transport;self.server=None;self.anchor=None;self.posted=set();self.cancelled=set()
        self.key,self.secret=credentials(os.environ) if transport is None else ('fixture','fixture')
    def event(self,name,**data):
        # Callers pass only explicit identities, enums, counts and prices. Never HTTP payloads/URLs.
        row={'utc':utc(),'event':name,**data}
        with self.ledger.open('a',encoding='utf-8') as f:
            f.write(json.dumps(row,ensure_ascii=True)+'\n');f.flush();os.fsync(f.fileno())
    def save(self):
        p=self.out/'local_state.json';tmp=self.out/'local_state.tmp'
        with tmp.open('w',encoding='utf-8') as f:
            json.dump(self.state,f,indent=2);f.flush();os.fsync(f.fileno())
        tmp.replace(p)
    def request(self,method,route,params=None):
        validate_target(BASE,method,route)
        require(self.state['requests']<80,'REQUEST_BUDGET')
        if method!='GET': require(self.state['writes']<8,'WRITE_BUDGET')
        signed=route not in PUBLIC
        params=dict(params or {}); safe_id=params.get('clientAlgoId') or params.get('newClientOrderId') or params.get('origClientOrderId')
        self.state['requests']+=1;self.state['signed_requests']+=int(signed);self.state['writes']+=int(method!='GET')
        self.save();self.event('REQUEST',method=method,route=route,client_id=safe_id)
        if self.transport is not None: return self.transport(method,route,params)
        if signed:
            require(self.server is not None,'NO_TIME_ANCHOR')
            params.update(timestamp=self.server+int((time.monotonic()-self.anchor)*1000),recvWindow=5000)
        query=urlencode(params)
        headers={'Accept':'application/json','Content-Type':'application/x-www-form-urlencoded','User-Agent':'MomentumDemoCapability/1'}
        if signed:
            query+='&signature='+hmac.new(self.secret.encode(),query.encode(),hashlib.sha256).hexdigest()
            headers['X-MBX-APIKEY']=self.key
        conn=DemoConnection(HOST,443,timeout=12,context=ssl.create_default_context())
        try:
            conn.request(method,route+('?' + query if query and method!='POST' else ''),
                         body=query if method=='POST' else None,headers=headers)
            r=conn.getresponse();check_redirect(r.status)
            raw=r.read(4*1024*1024+1);require(len(raw)<=4*1024*1024,'RESPONSE_SIZE')
            data=json.loads(raw)
            code=data.get('code') if isinstance(data,dict) else None
            self.event('RESPONSE',method=method,route=route,http_status=r.status,
                       exchange_code=code if type(code) is int else None)
            if r.status!=200:
                # Preserve semantic classification without retaining raw error strings.
                msg=data.get('msg','') if isinstance(data,dict) else ''
                self.event('ERROR_CLASS',close_position_mentioned='closePosition' in msg,
                           existing_order_mentioned='exist' in msg.lower(),duplicate_mentioned='duplicat' in msg.lower())
                raise ExchangeError(r.status,code if type(code) is int else None)
            return data
        finally: conn.close()
    def snapshot(self):
        p=self.request('GET','/fapi/v3/positionRisk')
        n=self.request('GET','/fapi/v1/openOrders')
        a=self.request('GET','/fapi/v1/openAlgoOrders')
        counts=clean_counts(p,n,a);self.event('ACCOUNT_COUNTS',**counts)
        return p,n,a,counts
    def position(self):
        p=self.request('GET','/fapi/v3/positionRisk')
        live=[x for x in p if dec(x['positionAmt'])!=0]
        if not live:return None
        require(len(live)==1 and live[0]['symbol']==SYMBOL and live[0]['positionSide']=='BOTH','POSITION_IDENTITY')
        entry=self.state['entry'];require(entry is not None and dec(live[0]['positionAmt'])==dec(entry['quantity']),'POSITION_QUANTITY')
        return live[0]
    def query_stop(self,cid,working=False):
        s=self.state['stops'][cid]
        q=self.request('GET','/fapi/v1/algoOrder',{'clientAlgoId':cid})
        oid=verify_stop(q,cid,s['target'],s.get('algo_id'),working)
        s.update(algo_id=oid,status=q['algoStatus']);self.save()
        self.event('EXACT_STOP_QUERY',client_id=cid,algo_id=oid,status=q['algoStatus'],trigger_price=s['target'],
                   symbol=q['symbol'],side=q['side'],position_side=q['positionSide'],close_position=q['closePosition'],
                   quantity=str(q.get('quantity','0')),working_type=q['workingType'],order_type=q['orderType'],
                   actual_order_id=str(q.get('actualOrderId','')))
        return q
    def create_stop(self,cid,target):
        require(cid not in self.posted,'DUPLICATE_CREATE_DENIED')
        require(len(self.state['stops'])<2,'THIRD_STOP_DENIED')
        self.posted.add(cid);self.state['algo_posts']+=1
        self.state['stops'][cid]={'target':fixed(target),'status':'INTENT','algo_id':None}
        self.state['pending'].append(cid);self.state['unresolved'].append(cid);self.save()
        try:
            self.request('POST','/fapi/v1/algoOrder',dict(algoType='CONDITIONAL',symbol=SYMBOL,
                         side='SELL',positionSide='BOTH',type='STOP_MARKET',workingType='CONTRACT_PRICE',
                         closePosition='true',triggerPrice=fixed(target),clientAlgoId=cid,priceProtect='false'))
        except ExchangeError as e:
            self.state['stops'][cid].update(http_status=e.status,exchange_code=e.code)
            if 400<=e.status<500 and e.status not in (408,429):
                self.state['stops'][cid]['status']='HTTP_REJECTED'
            self.save()
            # Even an explicit rejection is reconciled by exact query and open list during cleanup.
            raise
        q=self.query_stop(cid,True)
        self.state['pending'].remove(cid);self.state['unresolved'].remove(cid);self.save()
        return q
    def cancel(self,cid):
        q=self.query_stop(cid)
        if q['algoStatus'] in TERMINAL:return
        require(q['algoStatus'] in WORKING,'TERMINAL_UNKNOWN')
        # A repeated cleanup may verify an already-terminal order, but must never
        # resubmit a prior uncertain cancel while it is still working.
        require(cid not in self.cancelled,'DUPLICATE_CANCEL_DENIED')
        self.cancelled.add(cid);self.state['cancel_attempts']+=1;self.save()
        try:self.request('DELETE','/fapi/v1/algoOrder',{'algoId':q['algoId']})
        except Exception:self.event('CANCEL_RESPONSE_UNCERTAIN',client_id=cid)
        q=self.query_stop(cid);require(q['algoStatus'] in TERMINAL,'CANCEL_UNRESOLVED')
    def flatten(self):
        p=self.position()
        if p is None:return
        require(self.state['close_posts']==0,'SECOND_CLOSE_DENIED')
        cid='DPSL-'+self.state['run_id']+'-close'
        self.state['close_posts']+=1;self.state['pending'].append(cid);self.state['unresolved'].append(cid);self.save()
        try:
            self.request('POST','/fapi/v1/order',dict(symbol=SYMBOL,side='SELL',positionSide='BOTH',type='MARKET',
                         quantity=p['positionAmt'],reduceOnly='true',newClientOrderId=cid,newOrderRespType='RESULT'))
        except Exception:self.event('CLOSE_RESPONSE_UNCERTAIN',client_id=cid)
        q=self.request('GET','/fapi/v1/order',{'symbol':SYMBOL,'origClientOrderId':cid})
        require(q.get('clientOrderId')==cid and q.get('symbol')==SYMBOL and q.get('side')=='SELL' and
                q.get('positionSide')=='BOTH' and q.get('type')=='MARKET' and
                str(q.get('reduceOnly')).lower()=='true' and q.get('status')=='FILLED' and
                dec(q['executedQty'])==dec(p['positionAmt']),'CLOSE_IDENTITY')
        require(self.position() is None,'CLOSE_NOT_FLAT')
        self.state['pending'].remove(cid);self.state['unresolved'].remove(cid);self.save()
        self.event('CONTROLLED_FLAT_VERIFIED',client_id=cid,order_id=str(q['orderId']),executed_qty=q['executedQty'],avg_price=q['avgPrice'])
    def cleanup(self):
        self.state['phase']='CLEANUP';self.save()
        # An entry intent must be resolved before any quantity can be owned/closed.
        e=self.state['entry']
        if e and e.get('status')!='FILLED': self.resolve_entry()
        if e and e.get('status')=='FILLED':
            self.flatten()
        else:
            # An absent/rejected entry owns no position, even if size matches.
            _,_,_,counts=self.snapshot();require_clean(counts)
        open_algos=self.request('GET','/fapi/v1/openAlgoOrders')
        require(all(x.get('clientAlgoId') in self.state['stops'] for x in open_algos),'UNOWNED_ALGO_CLEANUP_BLOCK')
        for cid,s in self.state['stops'].items():
            try:q=self.query_stop(cid)
            except ExchangeError as ex:
                if ex.code==-2013 and s['status']=='HTTP_REJECTED' and not any(x.get('clientAlgoId')==cid for x in open_algos):
                    s['status']='REJECTION_AND_EXACT_ABSENCE_VERIFIED';q=None
                else:raise
            if q is not None:
                self.cancel(cid)
            for key in ('pending','unresolved'):
                if cid in self.state[key]:self.state[key].remove(cid)
            self.save()
        _,_,_,counts=self.snapshot();require_clean(counts)
        counts.update(pending_local_intent=len(self.state['pending']),unresolved_client_id=len(self.state['unresolved']))
        require_clean(counts);counts.update(status='PASS',utc=utc())
        self.state['final_cleanup']=counts;self.save();self.event('FINAL_CLEANUP_PASS',**counts)
    def resolve_entry(self):
        e=self.state['entry'];cid=e['client_id']
        require(cid=='DPSL-'+self.state['run_id']+'-entry' and dec(e['quantity'])>0,'ENTRY_INTENT_IDENTITY')
        rejected=e.get('post_http_status')==400 and e.get('post_exchange_code')==-2019
        try:q=self.request('GET','/fapi/v1/order',{'symbol':SYMBOL,'origClientOrderId':cid})
        except ExchangeError as ex:
            if not (rejected and ex.status==400 and ex.code==-2013):raise
            _,_,_,counts=self.snapshot();require_clean(counts)
            e.update(status='REJECTED_NO_ORDER_CREATED',resolution='EXPLICIT_REJECTION_EXACT_ABSENCE_CLEAN_ACCOUNT')
            for key in ('pending','unresolved'):
                if cid in self.state[key]:self.state[key].remove(cid)
            self.save();self.event('ENTRY_REJECTION_RECONCILED',client_id=cid,post_code=-2019,query_code=-2013,**counts)
            return False
        require(not rejected,'REJECTED_ENTRY_CONTRADICTORY_ORDER')
        require(q.get('clientOrderId')==cid and q.get('symbol')==SYMBOL and q.get('side')=='BUY' and
                q.get('positionSide')=='BOTH' and q.get('type')=='MARKET' and q.get('status')=='FILLED' and
                dec(q['executedQty'])==dec(e['quantity']) and dec(q['avgPrice'])>0,'ENTRY_IDENTITY')
        e.update(status='FILLED',order_id=str(q['orderId']),fill_vwap=q['avgPrice'])
        for key in ('pending','unresolved'):
            if cid in self.state[key]:self.state[key].remove(cid)
        self.save();self.event('ENTRY_FILL_VERIFIED',**e)
        return True
    def execute(self):
        started_clean=False
        try:
            t=self.request('GET','/fapi/v1/time');self.server=t['serverTime'];self.anchor=time.monotonic()
            _,_,_,c=self.snapshot();require_clean(c)
            require_mode(self.request('GET','/fapi/v1/positionSide/dual'));started_clean=True
            info=self.request('GET','/fapi/v1/exchangeInfo')
            spec=next(s for s in info['symbols'] if s['symbol']==SYMBOL)
            require(spec['status']=='TRADING' and spec['contractType']=='PERPETUAL','SYMBOL_UNAVAILABLE')
            price=dec(self.request('GET','/fapi/v1/ticker/price',{'symbol':SYMBOL})['price'])
            qty,tick,minimum=size_for(spec,price)
            self.event('SYMBOL_FILTERS',quantity=fixed(qty),tick=fixed(tick),min_notional=fixed(minimum),reference_price=fixed(price),notional=fixed(qty*price))
            available,required=funding_for(self.request('GET','/fapi/v3/balance'),qty*price)
            self.event('DEMO_FUNDING_VERIFIED',available_usdt=fixed(available),required_usdt=fixed(required))
            config=self.request('GET','/fapi/v1/symbolConfig',{'symbol':SYMBOL})
            require(len(config)==1 and config[0]['symbol']==SYMBOL,'SYMBOL_CONFIG_SCHEMA')
            if config[0]['marginType']!='ISOLATED':self.request('POST','/fapi/v1/marginType',{'symbol':SYMBOL,'marginType':'ISOLATED'})
            if int(config[0]['leverage'])!=1:self.request('POST','/fapi/v1/leverage',{'symbol':SYMBOL,'leverage':1})
            config=self.request('GET','/fapi/v1/symbolConfig',{'symbol':SYMBOL})
            require(len(config)==1 and config[0]['symbol']==SYMBOL and config[0]['marginType']=='ISOLATED' and int(config[0]['leverage'])==1,'ISOLATED_1X_UNVERIFIED')
            self.event('ISOLATED_1X_VERIFIED')
            _,_,_,c=self.snapshot();require_clean(c)
            cid='DPSL-'+self.state['run_id']+'-entry'
            self.state.update(phase='ENTRY',cycle_result='RUNNING',entry_posts=1,
                entry={'client_id':cid,'quantity':fixed(qty),'status':'INTENT'})
            self.state['pending'].append(cid);self.state['unresolved'].append(cid);self.save()
            try:self.request('POST','/fapi/v1/order',dict(symbol=SYMBOL,side='BUY',positionSide='BOTH',type='MARKET',quantity=fixed(qty),newClientOrderId=cid,newOrderRespType='RESULT'))
            except ExchangeError as ex:
                self.state['entry'].update(post_http_status=ex.status,post_exchange_code=ex.code)
                self.save();self.event('ENTRY_POST_ERROR_RECEIPT',client_id=cid,http_status=ex.status,exchange_code=ex.code)
            except Exception:self.event('ENTRY_RESPONSE_UNCERTAIN',client_id=cid)
            if not self.resolve_entry():
                self.state['cycle_result']='ENTRY_REJECTED_CLEANLY_RESOLVED';self.save()
                return self.state
            self.position()
            fill=dec(self.state['entry']['fill_vwap'])
            # Existing Premium test plan entry_mid=100/invalidation=98.7: 1.3% from actual fill.
            # Higher test stop is a technical overlap witness, NOT a new economic profile/step.
            old=(fill*Decimal('.987')/tick).to_integral_value(rounding=ROUND_FLOOR)*tick
            new=(fill*Decimal('.990')/tick).to_integral_value(rounding=ROUND_FLOOR)*tick
            market=dec(self.request('GET','/fapi/v1/ticker/price',{'symbol':SYMBOL})['price'])
            require(0<old<new<market,'STOP_PRICE_ORDER')
            old_id='DPSL-'+self.state['run_id']+'-old';new_id='DPSL-'+self.state['run_id']+'-new'
            self.state['phase']='OLD_STOP';self.save();self.create_stop(old_id,old)
            self.position();self.query_stop(old_id,True)
            only=self.request('GET','/fapi/v1/openAlgoOrders')
            require(len(only)==1 and only[0]['clientAlgoId']==old_id,'INITIAL_STOP_OPEN_VIEW')
            verify_stop(only[0],old_id,old,self.state['stops'][old_id]['algo_id'])
            self.state['phase']='SECOND_STOP';self.save()
            self.create_stop(new_id,new)
            both=self.request('GET','/fapi/v1/openAlgoOrders');verify_both(both,self.state['stops'])
            self.position();self.query_stop(new_id,True)
            self.state.update(overlap_observed=True,new_confirmed_before_old_cancel=True);self.save()
            self.event('BOTH_STOPS_SIMULTANEOUSLY_VERIFIED',client_ids=[old_id,new_id])
            self.cancel(old_id);self.query_stop(new_id,True);self.position()
            self.state['cycle_result']='STANDARD_OVERLAP_PASS';self.save()
        except ExchangeError as e:
            self.state.update(cycle_result='BLOCKED',failure_phase=self.state['phase'],http_status=e.status,exchange_code=e.code)
            if self.state['phase']=='SECOND_STOP' and 400<=e.status<500 and e.status not in (408,429):
                self.state['cycle_result']='SECOND_CLOSEPOSITION_STOP_REJECTED'
            self.event('CYCLE_EXCHANGE_FAILURE',phase=self.state['phase'],http_status=e.status,exchange_code=e.code)
        except Exception as e:
            self.state.update(cycle_result='BLOCKED',failure_phase=self.state['phase'],exception_class=type(e).__name__,
                              reason=e.args[0] if type(e) is Blocked else 'TRANSPORT_OR_SCHEMA')
            self.event('CYCLE_BLOCKED',phase=self.state['phase'],exception_class=type(e).__name__)
        finally:
            if started_clean:
                try:self.cleanup()
                except Exception as e:
                    self.state['cleanup_failure']={'exception_class':type(e).__name__,'reason':e.args[0] if type(e) is Blocked else 'TRANSPORT_OR_SCHEMA', 'exchange_code':getattr(e,'code',None)}
                    self.event('CLEANUP_BLOCKED',**self.state['cleanup_failure'])
            self.state['finished_utc']=utc();self.save()
            with (self.out/'First_Cycle_Result.json').open('x',encoding='utf-8') as f:json.dump(self.state,f,indent=2)
            self.key=self.secret=''
        return self.state
