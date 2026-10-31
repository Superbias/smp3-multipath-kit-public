#!/usr/bin/env python3
"""Analyze retained fixed windows; command exits and quantitative acceptance are separate."""
import argparse,bisect,json,re,statistics
from pathlib import Path

def rows(path):
 result=[]
 if path.exists():
  for line in path.read_text(encoding='utf-8',errors='replace').splitlines():
   if 'AUDIT {' in line:
    try:result.append(json.loads(line.split('AUDIT ',1)[1]))
    except ValueError:pass
 return result

def bytes_at(result,t):
 samples=[{'seconds':0,'bytes':0},*result['samples'],{'seconds':result['elapsed'],'bytes':result['received']}]
 times=[s['seconds'] for s in samples]
 if t>times[-1]:return None
 i=bisect.bisect_right(times,t)-1
 if i==len(samples)-1:return samples[i]['bytes']
 a,b=samples[i:i+2]
 return a['bytes']+(b['bytes']-a['bytes'])*(t-a['seconds'])/(b['seconds']-a['seconds'])

def native_summary(result,b):
 windows={}
 for lo,hi in [(15,25),(25,35),(35,45),(55,65)]:
  a,z=bytes_at(result,lo),bytes_at(result,hi);windows[f'{lo}-{hi}']=None if a is None or z is None else (z-a)*8/(hi-lo)/1e6
 final=result.get('final_transport') or {};written=sum(final.get('written_bytes',[]));useful=final.get('useful_ack_total',0)
 normal=[]
 if result.get('fault')=='none':
  full={f'{lo}-{hi}':(bytes_at(result,hi)-bytes_at(result,lo))*8/(hi-lo)/1e6 for lo,hi in [(0,10),(10,20),(20,30)] if bytes_at(result,hi) is not None}
  nominal=sum(result.get('carrier_rates_bps',[0,0]))/1e6
  return {'scope':'actual Native + server with calibrated loopback Carrier, healthy 50/200','elapsed':result['elapsed'],'payload_and_EOF':result.get('payload_pass'),'ledger_drained':result.get('drain_pass'),'sha256':result['sha256'],'fixed_windows_mbps':full,'nominal_sum_mbps':nominal,'at_least_30_seconds':result['elapsed']>=30,'throughput_acceptance':'PASS' if len(full)==3 and all(v>=.9*nominal for k,v in full.items() if k!='0-10') else 'FAIL','binaries':result['binaries']}
 for sample in result.get('telemetry',[]):
  for session in sample.get('sessions',[]):
   state=session.get('session',{}).get('stream_transport',{}).get('admission_state',[])
   if sample['seconds']>=result['fault_end'] and state==['normal','normal']:normal.append(sample['seconds'])
 payload=result.get('payload_pass') is True
 ledger=result.get('drain_pass')
 stable=[windows[f'{lo}-{hi}'] for lo,hi in [(15,25),(25,35),(35,45)]]
 performance=all(v is not None and v>=.9*b for v in stable) and windows['55-65'] is not None and windows['55-65']>=1.5*b
 return {'bytes':result['received'],'elapsed':result['elapsed'],'sha256':result['sha256'],'payload_and_EOF':payload,'ledger_drained':ledger,'carrier_connections':result['carrier_connections'],'fixed_windows_mbps':windows,'baseline_B_mbps':b,'throughput_acceptance':'PASS' if performance else 'FAIL','first_observed_normal_after_restore':min(normal) if normal else None,'actual_DATA_amplification':written/useful if useful else None,'binaries':result['binaries'],'measurement':'linear interpolation of cumulative bytes at declared boundaries; all source samples retained'}

def main():
 p=argparse.ArgumentParser();p.add_argument('--runs',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();out={'command_results':{},'native':{},'rtt':{},'calibration':{},'primary':{},'soak':{}}
 for d in sorted(a.runs.iterdir()):
  if d.is_dir() and (d/'command.json').exists():out['command_results'][d.name]=json.loads((d/'command.json').read_text())
 single=a.runs/'native-single/process/result.json'
 if single.exists():
  s=json.loads(single.read_text());b=statistics.median((bytes_at(s,hi)-bytes_at(s,lo))*8/(hi-lo)/1e6 for lo,hi in [(15,25),(25,35),(35,45)])
  # Original frozen old test-tool single baseline remains visible. Do not hide a decline with a lower new B.
  out['native_reference']={'new_single_fixed_B':b,'original_frozen_single_B':91.798,'acceptance_B':max(b,91.798)}
  for d in sorted(a.runs.glob('native-*')):
   path=d/'process/result.json'
   if path.exists() and d.name!='native-single':out['native'][d.name]=native_summary(json.loads(path.read_text()),max(b,91.798))
  for name in ['interop-old-server-new-native','interop-new-server-old-native']:
   path=a.runs/name/'process/result.json'
   if path.exists():out['native'][name]=native_summary(json.loads(path.read_text()),max(b,91.798))
 for label in ['baseline','patch']:
  matches=list(a.runs.glob('rtt-'+label+'-*/raw.log'))
  if matches:out['rtt'][label]=[r for r in rows(matches[-1]) if 'windows' in r]
  matches=list(a.runs.glob('calibration-'+label+'-*/raw.log'))
  if matches:out['calibration'][label]=[r for r in rows(matches[-1]) if 'windows' in r]
 if 'baseline' in out['rtt'] and 'patch' in out['rtt']:
  old={(r['rtt_ms'],r['kind']):r for r in out['rtt']['baseline']};comparisons=[]
  for r in out['rtt']['patch']:
   reference=old.get((r['rtt_ms'],r['kind']))
   bsingle=old.get((r['rtt_ms'],'single'))
   entry=dict(r)
   if reference:
    entry['old_healthy_B']=statistics.median(reference['windows'][1:]);entry['ratio_to_old_healthy']=statistics.median(r['windows'][1:])/entry['old_healthy_B'];entry['healthy_nonregression']='PASS' if entry['ratio_to_old_healthy']>=.9 else 'FAIL'
   if bsingle and r['kind'] in ['slow','recovery']:
    entry['old_single_B']=statistics.median(bsingle['windows'][1:]);entry['slow_fixed_acceptance']='PASS' if all(v>=.9*entry['old_single_B'] for v in r['windows'][1:2 if r['kind']=='recovery' else 4]) else 'FAIL'
    if r['kind']=='recovery':entry['recovered_dual_acceptance']='PASS' if r['windows'][-1]>=1.5*entry['old_single_B'] else 'FAIL'
   comparisons.append(entry)
  out['rtt']['comparison']=comparisons
 for path in a.runs.glob('primary-*/raw.log'):
  case=None;record={}
  for line in path.read_text(encoding='utf-8',errors='replace').splitlines():
   m=re.search(r'=== RUN\s+TestSlowLegReliableStream/(\w+)',line)
   if m:case=m[1]
   m=re.search(r'RESULT .*windows=\[([^]]+)\].*payload=PASS',line)
   if m and case:record[case]=[float(n) for n in m[1].split()]
  if record:out['primary'][path.parent.name]=record
 for name,record in out['primary'].items():
  if 'single' in record:
   b=max(statistics.median(record['single'][1:]),95.3);out.setdefault('primary_acceptance',{})[name]={'B':b,'result':'INCOMPLETE' if any(c not in record for c in ['single','cold_100_1','swapped_1_100','blackhole_recovery']) else 'PASS' if all(c in record and len(record[c])==4 and all(v>=.9*b for v in record[c][1:]) for c in ['cold_100_1','swapped_1_100']) and 'blackhole_recovery' in record and record['blackhole_recovery'][-1]>=1.5*b else 'FAIL'}
 paths=list(a.runs.glob('shared-soak-*/raw.log'))
 if paths:
  data=rows(paths[-1]);samples=[r for r in data if 'provider_refs' in r];closes=[r for r in data if r.get('event') in ['application_exit_cancel','final_cancel']]
  if samples:
   out['soak']={'scope':'Core with calibrated shared virtual carriers, not real Native/physical route validation','duration_observed':samples[-1]['second'],'samples':samples,'cancel_exit_samples':closes,'completion':[r for r in data if 'result' in r],'min_fixed_10s_mbps':min(r['goodput_fixed_10s'] for r in samples),'max_zero_second_pause':max(r['max_zero_second_pause'] for r in samples),'provider_refs_expected':all(r['provider_refs']==2*r['streams'] for r in samples),'entries_bounded':all(r['entries']==2 for r in samples),'goroutine_range':[min(r['goroutines'] for r in samples),max(r['goroutines'] for r in samples)],'heap_alloc_range':[min(r['heap_alloc'] for r in samples),max(r['heap_alloc'] for r in samples)]}
 out['numeric_overall']='FAIL if any declared acceptance fails; final release status also requires legacy/parity and physical environment prerequisites'
 with a.output.open('x',encoding='utf-8') as f:json.dump(out,f,ensure_ascii=False,indent=2)
 print(a.output)
if __name__=='__main__':main()
