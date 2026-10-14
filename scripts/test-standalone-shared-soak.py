#!/usr/bin/env python3
"""Actual Standalone client/server processes, shared loopback physical budgets, timed app churn.
All sockets bind loopback. No installed binary/config, network route or production
service is modified. Long flows verify every received prefix and are deliberately
cancelled; finite EOF is covered separately by test-native-slow-leg.py.
"""
import argparse,hashlib,importlib.util,json,os,socket,subprocess,threading,time,urllib.request,shlex
from pathlib import Path
CONTENT=bytes(range(256))*256

def resources(pid):
 try:
  p=Path('/proc')/str(pid);fields=dict(line.split(':',1) for line in (p/'status').read_text().splitlines() if ':' in line)
  return {'rss_kib':int(fields['VmRSS'].split()[0]),'os_threads':int(fields['Threads']),'fds':len(list((p/'fd').iterdir()))}
 except (OSError,KeyError):return {'status':'NOT AVAILABLE'}

def main():
 p=argparse.ArgumentParser();p.add_argument('--server',type=Path,required=True);p.add_argument('--mihomo',type=Path);p.add_argument('--standalone-client',type=Path);p.add_argument('--harness',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--seconds',type=int,default=1800);p.add_argument('--smoke',action='store_true',help='allow a >=120-second actual-process preflight; never counts as the required long soak');a=p.parse_args()
 if bool(a.mihomo)==bool(a.standalone_client):p.error('select exactly one of --mihomo or --standalone-client')
 if a.seconds<1800 and (not a.smoke or a.seconds<120):raise SystemExit('Long evidence must run at least 30 minutes; --smoke allows >=120 seconds only for an actual-process preflight')
 out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
 spec=importlib.util.spec_from_file_location('native_fixture',a.harness);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
 class SharedCarrier(m.Carrier):
  def __init__(self,target):self.clock_lock=threading.Lock();self.clock=0.;self.forwarded=0;super().__init__(target)
  def copy(self,source,target,paced):
   try:
    while not self.stop.is_set():
     data=source.recv(16384)
     if not data:break
     if paced:
      # A rate transition must retain this already-read unit. Retry reserving,
      # rather than dropping it if blackhole begins between the two checks.
      while True:
       while self.rate==0 and not self.stop.wait(.02):pass
       if self.stop.is_set():return
       with self.clock_lock:
        now=time.monotonic();rate=self.rate
        if rate:
         self.clock=max(self.clock,now-16384*8/rate)+len(data)*8/rate;deadline=self.clock
         break
      if self.stop.wait(max(0,deadline-time.monotonic())):break
     target.sendall(data)
     if paced:
      with self.clock_lock:self.forwarded+=len(data)
   except OSError:pass
   finally:source.close();target.close()
 server_port,native_port,telemetry_port=m.port(),m.port(),m.port();native_server_port=server_port if not a.standalone_client else m.port();carriers=[SharedCarrier(server_port),SharedCarrier(server_port)];target=m.listener();stop_target=threading.Event()
 def response(conn):
  try:
   with conn:
    request=bytearray()
    while b'\r\n\r\n' not in request:
     part=conn.recv(1024)
     if not part:return
     request.extend(part)
    conn.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 1099511627776\r\nConnection: close\r\n\r\n')
    while not stop_target.is_set():conn.sendall(CONTENT)
  except OSError:pass
 def accept():
  while not stop_target.is_set():
   try:conn,_=target.accept()
   except OSError:return
   threading.Thread(target=response,args=(conn,),daemon=True).start()
 threading.Thread(target=accept,daemon=True).start()
 config={'listen':f'127.0.0.1:{native_server_port}','password':'SMP3_LOOPBACK_TEST_ONLY','recovery_timeout':'15s','stream':{'scheduler_mode':'aggregation','capacity_mode':'dynamic','activation_threshold_mbps':80,'activation_window':'1s','bandwidth_mbps':[128,500],'chunk_size':65536,'queue_frames':256,'max_inflight_frames':1024,'max_reorder_frames':4096,'ack_interval':'20ms','retransmit_timeout':'1500ms'},'telemetry':{'enabled':True,'listen':f'127.0.0.1:{telemetry_port}','close_grace':'2s'}}
 if a.standalone_client:config['sidecar_listeners']=[f'127.0.0.1:{server_port}']
 (out/'server.json').write_text(json.dumps(config,indent=2))
 yaml=f'mixed-port: {native_port}\nallow-lan: false\nmode: rule\nlog-level: warning\nproxies:\n'
 for i,c in enumerate(carriers):yaml+=f'  - name: carrier{i}\n    type: socks5\n    server: 127.0.0.1\n    port: {c.listener.getsockname()[1]}\n'
 yaml+=f'''  - name: MP
    type: smp3
    server: 127.0.0.1
    port: {server_port}
    password: SMP3_LOOPBACK_TEST_ONLY
    legs:
      - proxy: carrier0
      - proxy: carrier1
    scheduler-mode: aggregation
    capacity-mode: dynamic
    activation-threshold-mbps: 80
    activation-window: 10s
    bandwidth-mbps: [128, 500]
    chunk-size: 65536
    queue-frames: 256
    max-inflight-frames: 1024
    max-reorder-frames: 4096
    ack-interval: 20ms
    retransmit-timeout: 1500ms
    recovery-timeout: 15s
    startup-policy: preferred
    startup-preferred-leg: 0
    startup-grace: 500ms
rules:
  - MATCH,MP
'''
 if not a.standalone_client:(out/'native.yaml').write_text(yaml)
 else:
  (out/'standalone-client.json').write_text(json.dumps({'listen':f'127.0.0.1:{native_port}','upstream_socks':{'connect_timeout':'2s','leg0':{'address':f'127.0.0.1:{carriers[0].listener.getsockname()[1]}'},'leg1':{'address':f'127.0.0.1:{carriers[1].listener.getsockname()[1]}'}},'smp3':{'password':'SMP3_LOOPBACK_TEST_ONLY','carrier_ready_timeout':'5s','routes':{'leg0':f'127.0.0.1:{server_port}','leg1':f'127.0.0.1:{server_port}'},'stream':{'scheduler_mode':'aggregation','capacity_mode':'dynamic','activation_mode':'legacy','startup_policy':'preferred','startup_preferred_leg':0,'startup_grace':'500ms','activation_threshold_mbps':80,'activation_window':'10s','chunk_size':65536,'queue_frames':256,'bandwidth_mbps':[128,500],'max_reorder_frames':4096,'max_inflight_frames':1024,'ack_interval':'20ms','retransmit_timeout':'1500ms','recovery_timeout':'15s','redial_interval':'1s'},'udp':{'enabled':False}}},indent=2))
 ready=out/'native.ready';children=[];logs=[];active=[];retired=[];errors=[];samples=[]
 class Flow:
  def __init__(self,index):
   self.index=index;self.state_lock=threading.Lock();self.bytes=0;self.digest=hashlib.sha256();self.ready=threading.Event();self.stop=threading.Event();self.socket=None;self.pause=0;self.max_pause=0;self.last=0;self.error=None;self.thread=threading.Thread(target=self.receive,daemon=True);self.thread.start()
  def receive(self):
   try:
    self.socket=socket.create_connection(('127.0.0.1',native_port),10);self.socket.settimeout(20);m.socks_connect(self.socket,target.getsockname()[1]);self.socket.sendall(b'GET /soak HTTP/1.1\r\nHost: localhost\r\n\r\n');header=bytearray()
    while not header.endswith(b'\r\n\r\n'):header.extend(m.exact(self.socket,1))
    if not header.startswith(b'HTTP/1.1 200'):raise RuntimeError('target response')
    self.ready.set();expected=CONTENT*2
    while not self.stop.is_set():
     data=self.socket.recv(65536)
     if not data:raise EOFError('unexpected body EOF during long prefix experiment')
     offset=self.bytes%len(CONTENT)
     if data!=expected[offset:offset+len(data)]:raise RuntimeError('payload prefix mismatch')
     with self.state_lock:self.digest.update(data);self.bytes+=len(data)
   except (OSError,EOFError,RuntimeError) as error:
    if not self.stop.is_set():self.error=repr(error);errors.append({'flow':self.index,'error':self.error})
   finally:self.ready.set()
  def close(self):
   self.stop.set()
   if self.socket:
    try:self.socket.shutdown(socket.SHUT_RDWR)
    except OSError:pass
    self.socket.close()
   self.thread.join(5)
   if self.thread.is_alive():errors.append({'flow':self.index,'error':'receiver close timeout'})
  def snapshot(self):
   with self.state_lock:return {'flow':self.index,'bytes':self.bytes,'sha256_verified_prefix':self.digest.hexdigest(),'payload_prefix_pass':self.error is None,'max_zero_progress_seconds':self.max_pause,'termination':'application cancellation; finite EOF NOT ASSERTED'}
 def telemetry():
  try:
   with urllib.request.urlopen(f'http://127.0.0.1:{telemetry_port}/api/v1/sessions',timeout=.5) as response:catalog=json.load(response)
   result=[]
   for item in catalog.get('items',[]):
    with urllib.request.urlopen(f"http://127.0.0.1:{telemetry_port}/api/v1/sessions/{item['display_session_id']}",timeout=.5) as response:result.append(json.load(response))
   return {'sessions':result}
  except (OSError,ValueError) as error:return {'read_error':repr(error)}
 def add():
  flow=Flow(len(retired)+len(active));active.append(flow)
  if not flow.ready.wait(15) or flow.error:raise RuntimeError('flow startup failed')
 def emit(data):print(json.dumps(data),flush=True)
 try:
  engine_binary=a.standalone_client or a.mihomo;engine_name='standalone-client' if a.standalone_client else 'native'
  engine_argv=['-c',str(out/'standalone-client.json')] if a.standalone_client else ['-post-up',f'touch {shlex.quote(str(ready))}','-d',str(out/'mihomo-home'),'-f',str(out/'native.yaml')]
  for binary,argv,name in [(a.server,['-c',str(out/'server.json')],'server'),(engine_binary,engine_argv,engine_name)]:
   log=(out/(name+'.log')).open('w');logs.append(log);children.append(subprocess.Popen([str(binary.resolve()),*argv],stdout=log,stderr=log))
  m.wait_port(server_port,children);m.wait_port(native_port,children)
  if not a.standalone_client:
   deadline=time.monotonic()+15
   while not ready.exists():
    if time.monotonic()>deadline:raise RuntimeError('Native post-up not ready')
    time.sleep(.02)
  add();add();start=time.monotonic();initial=sum(f.bytes for f in active);last=initial;base=initial;max_lag=0.
  for second in range(1,a.seconds+1):
   time.sleep(max(0,start+second-time.monotonic()));elapsed=time.monotonic()-start;phase=second%120;max_lag=max(max_lag,elapsed-second)
   if phase==10:add();add();emit({'event':'application_join','scheduled_second':second,'active':len(active)})
   if phase in [30,60,90]:carriers[1].rate={30:1_000_000,60:0,90:100_000_000}[phase];emit({'event':'physical_rate','scheduled_second':second,'leg1_bps':carriers[1].rate})
   if phase==115:
    for flow in active[2:]:flow.close();retired.append(flow);emit({'event':'application_exit','scheduled_second':second,**flow.snapshot()})
    active=active[:2]
   total=sum(f.bytes for f in [*retired,*active]);delta=total-last;last=total
   for flow in active:
    if flow.bytes==flow.last:flow.pause+=1
    else:flow.pause=0
    flow.max_pause=max(flow.max_pause,flow.pause);flow.last=flow.bytes
   if second%10==0:
    process_resources={name:resources(child.pid) for name,child in zip(['server',engine_name],children)}
    process_resources['test_tool']=resources(os.getpid())
    sample={'scheduled_second':second,'actual_seconds':time.monotonic()-start,'phase':phase,'active_flows':len(active),'delivered_total':total,'useful_fixed_10s_mbps':(total-base)*8/10/1e6,'last_second_mbps':delta*8/1e6,'leg1_bps':carriers[1].rate,'flows':[f.snapshot() for f in active],'physical_forwarded_bytes':[c.forwarded for c in carriers],'carrier_connections_cumulative':[c.connections for c in carriers],'process_resources':process_resources,'provider_refs':'NOT AVAILABLE in Standalone public telemetry; complementary Core shared soak checks refs/backlog directly','telemetry':telemetry()};samples.append(sample);base=total;emit(sample)
   if errors:raise RuntimeError('flow correctness or liveness error')
  for flow in active:flow.close();retired.append(flow)
  active=[];idle=[]
  for i in range(20):
   time.sleep(1)
   process_resources={name:resources(child.pid) for name,child in zip(['server',engine_name],children)}
   process_resources['test_tool']=resources(os.getpid())
   idle.append({'idle_second':i+1,'telemetry':telemetry(),'process_resources':process_resources})
  result={'scope':f'ACTUAL {engine_name} and server processes; calibrated SHARED LOOPBACK Carrier, not production Snell/HY2/Reality','run_classification':'smoke-only' if a.smoke else 'long-soak','duration_seconds':time.monotonic()-start,'scheduled_traffic_seconds':a.seconds,'flow_history':[f.snapshot() for f in retired],'samples':samples,'idle_after_cancel':idle,'errors':errors,'max_scheduler_lag_seconds':max_lag,'payload_prefix_pass':not errors,'finite_EOF':'NOT ASSERTED in deliberate-cancel soak; separate finite process tests cover EOF','binaries':{str(x):hashlib.sha256(x.read_bytes()).hexdigest() for x in [a.server,engine_binary]},'harness_sha256':hashlib.sha256(a.harness.read_bytes()).hexdigest(),'performance_acceptance':'SEPARATE; not implied by exit 0'}
  (out/'result.json').write_text(json.dumps(result,indent=2));emit({k:v for k,v in result.items() if k not in ['samples','flow_history','idle_after_cancel']})
  if max_lag>2:raise RuntimeError('test-tool schedule lag exceeded two seconds')
 except Exception as error:
  failure={'scope':f'ACTUAL {engine_name if "engine_name" in locals() else "client"}/server loopback process attempt; incomplete if startup or tool failed','error':repr(error),'errors':errors,'samples':samples,'flow_history':[f.snapshot() for f in [*retired,*active]],'production_actions':'NONE'}
  with (out/'failure-result.json').open('x') as file:json.dump(failure,file,indent=2)
  raise
 finally:
  for flow in active:flow.close()
  stop_target.set();target.close()
  for c in carriers:c.close()
  for child in children:child.terminate()
  for child in children:
   try:child.wait(5)
   except subprocess.TimeoutExpired:child.kill();child.wait()
  for log in logs:log.close()
if __name__=='__main__':main()
