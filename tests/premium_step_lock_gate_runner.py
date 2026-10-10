import os, sys, socket, json, unittest, pathlib, time, traceback, tempfile, gc
if os.getenv('PREMIUM_TEST_DEPS'):
    sys.path.insert(0,os.environ['PREMIUM_TEST_DEPS'])
root=pathlib.Path(sys.argv[1]).resolve()
out=pathlib.Path(sys.argv[2]).resolve()
out.parent.mkdir(parents=True,exist_ok=True)
os.environ.update(PYTHON_DOTENV_DISABLED='1',AUTO_TRADE_LIVE_ALLOWED='0',BINANCE_API_KEY='',BINANCE_API_SECRET='',TELEGRAM_BOT_TOKEN='',PYTHONIOENCODING='utf-8')
os.chdir(root)
sys.path[:0]=[str(root/'binance_momentum_bot'),str(root/'tests'),str(root)]
blocked=[]
original_connect=socket.socket.connect
original_connect_ex=socket.socket.connect_ex
original_create_connection=socket.create_connection
def no_network(*args,**kwargs):
    blocked.append('socket_connect_blocked')
    raise RuntimeError('NETWORK_DISABLED_IN_TESTS')
def guarded_connect(sock,address):
    if isinstance(address,tuple) and address[0] in ('127.0.0.1','::1'):
        return original_connect(sock,address)
    return no_network()
def guarded_connect_ex(sock,address):
    if isinstance(address,tuple) and address[0] in ('127.0.0.1','::1'):
        return original_connect_ex(sock,address)
    return no_network()
socket.socket.connect=guarded_connect
socket.socket.connect_ex=guarded_connect_ex
def guarded_create_connection(address,*args,**kwargs):
    if isinstance(address,tuple) and address[0] in ('127.0.0.1','::1','localhost'):
        return original_create_connection(address,*args,**kwargs)
    return no_network()
socket.create_connection=guarded_create_connection
# CPython sqlite connection cycles must be collected before Windows removes
# test-owned temporary directories. Assertions and DB behavior are unchanged.
original_cleanup=tempfile.TemporaryDirectory.cleanup
def collected_cleanup(self):
    gc.collect()
    return original_cleanup(self)
tempfile.TemporaryDirectory.cleanup=collected_cleanup
suite=(unittest.defaultTestLoader.loadTestsFromNames(sys.argv[3:]) if len(sys.argv)>3 else unittest.defaultTestLoader.discover(str(root/'tests')))
class Result(unittest.TextTestResult):
    def __init__(self,*a,**kw): super().__init__(*a,**kw); self.passed=[]
    def addSuccess(self,test): super().addSuccess(test); self.passed.append(test.id())
start=time.time()
with out.with_suffix('.log').open('w',encoding='utf-8') as stream:
    result=unittest.TextTestRunner(stream=stream,verbosity=2,resultclass=Result).run(suite)
def failures(items):
    return [{'test_id':t.id(),'traceback':detail,'terminal':detail.strip().splitlines()[-1]} for t,detail in items]
record=dict(tests_run=result.testsRun,passed=result.passed,failures=failures(result.failures),errors=failures(result.errors),skipped=[{'test_id':t.id(),'reason':why} for t,why in result.skipped],unexpected_successes=[t.id() for t in result.unexpectedSuccesses],network_attempts_blocked=len(blocked),real_network_calls=0,elapsed_seconds=round(time.time()-start,3))
out.write_text(json.dumps(record,indent=2),encoding='utf-8')
print(json.dumps({k:len(v) if isinstance(v,list) else v for k,v in record.items()}))
