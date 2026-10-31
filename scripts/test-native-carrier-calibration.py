#!/usr/bin/env python3
"""Raw loopback SOCKS Carrier calibration; no SMP3 Core in this measurement."""
import argparse,hashlib,importlib.util,json,socket,threading,time
from pathlib import Path

def main():
 p=argparse.ArgumentParser();p.add_argument('--harness',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
 spec=importlib.util.spec_from_file_location('fixture',a.harness);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 summaries=[]
 for kind,rates,framed in [('raw50',[50_000_000],False),('raw200',[200_000_000],False),('raw50_200',[50_000_000,200_000_000],False),('raw100_fragmented',[100_000_000],True)]:
  stop=threading.Event();targets=[];carriers=[];clients=[];threads=[];counts=[0]*len(rates);errors=[]
  pattern=bytes(range(256))*256 if not framed else bytes(13)+bytes(range(256))*128
  expected=pattern*2
  def source(listener):
   conn=None
   try:
    conn,_=listener.accept();conn.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
    with conn:
     while not stop.is_set():
      if framed:conn.sendall(pattern[:13]);conn.sendall(pattern[13:])
      else:conn.sendall(pattern)
   except OSError:pass
  def sink(conn,index):
   offset=0
   try:
    while not stop.is_set():
     data=conn.recv(16384)
     if not data:break
     if data!=expected[offset%len(pattern):offset%len(pattern)+len(data)]:errors.append('raw payload mismatch');return
     counts[index]+=len(data);offset+=len(data)
   except OSError:pass
  try:
   for index,rate in enumerate(rates):
    listener=module.listener();targets.append(listener);thread=threading.Thread(target=source,args=(listener,),daemon=True);thread.start();threads.append(thread)
    carrier=module.Carrier(listener.getsockname()[1]);carrier.rate=rate;carriers.append(carrier)
    conn=socket.create_connection(('127.0.0.1',carrier.listener.getsockname()[1]));clients.append(conn);module.socks_connect(conn,listener.getsockname()[1]);thread=threading.Thread(target=sink,args=(conn,index),daemon=True);thread.start();threads.append(thread)
   start=time.monotonic();base=[0]*len(rates);windows=[];samples=[]
   for second in range(1,41):
    time.sleep(max(0,start+second-time.monotonic()));current=counts.copy();samples.append({'second':second,'elapsed':time.monotonic()-start,'bytes':current})
    if second%10==0:
     windows.append([(n-b)*8/10/1e6 for n,b in zip(current,base)]);base=current
   stable=[sum(w) for w in windows[1:]];nominal=sum(rates)/1e6
   record={'kind':kind,'scope':'raw Native test-tool Carrier; no Core','nominal_mbps':nominal,'windows_mbps':windows,'aggregate_windows_mbps':[sum(w) for w in windows],'raw_calibration':'PASS' if not errors and min(stable)>=.95*nominal and max(stable)<=1.05*nominal else 'FAIL','payload_prefix_verified':not errors,'errors':errors,'samples':samples,'harness_sha256':hashlib.sha256(a.harness.read_bytes()).hexdigest()}
   (out/(kind+'.json')).write_text(json.dumps(record,indent=2));summaries.append(record);print(json.dumps({k:v for k,v in record.items() if k!='samples'}),flush=True)
  finally:
   stop.set()
   for conn in clients:conn.close()
   for carrier in carriers:carrier.close()
   for listener in targets:listener.close()
   for thread in threads:thread.join(2)
 (out/'summary.json').write_text(json.dumps(summaries,indent=2));raise SystemExit(0 if all(r['raw_calibration']=='PASS' for r in summaries) else 1)
if __name__=='__main__':main()
