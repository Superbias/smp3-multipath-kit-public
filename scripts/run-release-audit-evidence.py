#!/usr/bin/env python3
"""Local-only evidence runner. Never overwrite results; no production SSH or deployment."""
import argparse, hashlib, json, os, subprocess, time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
BASELINE='907b242c5f2532f774a3db1a877cf32740dcdfab'
def main():
 p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);p.add_argument('--soak',default='30m');a=p.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
 src={str(x.relative_to(ROOT)):hashlib.sha256(x.read_bytes()).hexdigest() for directory in ['core','server','adapters/mihomo'] for x in (ROOT/directory).rglob('*.go')}
 (out/'source-at-start.json').write_text(json.dumps({'baseline':BASELINE,'files':src},indent=2))
 def run(name,argv,cwd=ROOT,env=None):
  d=out/name;d.mkdir();record={'argv':argv,'cwd':str(cwd),'environment_overrides':env or {},'started_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'status':'RUNNING'}
  (d/'command.json').write_text(json.dumps(record,indent=2));print(name,flush=True);start=time.monotonic()
  with (d/'raw.log').open('w') as f:r=subprocess.run(argv,cwd=cwd,env=dict(os.environ,**(env or {})),stdout=f,stderr=subprocess.STDOUT)
  record.update(exit_code=r.returncode,elapsed_seconds=time.monotonic()-start,finished_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),status='PASS' if r.returncode==0 else 'FAIL')
  (d/'command.json').write_text(json.dumps(record,indent=2));print(name,r.returncode,flush=True)
 # Baseline production sources are pristine; only the documented fixtures are added.
 run('prepare-baseline',['python3',str(ROOT/'.work/prepare-release-baseline.py')])
 base=ROOT/'.work/release-audit-baseline'
 run('build-server',['go','build','-o',str(ROOT/'artifacts/release-audit-20261013/bin/new-server-linux-v3'),'./cmd/smp3-server'])
 run('module-tests',['go','test','./core/...','./client/...','./server/...','./cmd/smp3-server/...','./cmd/smp3-client/...','-count=1'])
 run('module-vet',['go','vet','./core/...','./client/...','./server/...','./cmd/smp3-server/...','./cmd/smp3-client/...'])
 run('module-race',['go','test','-race','./core/...','./client/...','./server/...','./cmd/smp3-server/...','./cmd/smp3-client/...','-count=1'])
 run('native-adapter-race',['go','test','-race','-mod=mod','./adapter/outbound','-run','Smp3|SMP3','-count=1'],ROOT/'.work/native-build/mihomo',{'GOWORK':'off'})
 native=ROOT/'artifacts/release-audit-20261013/bin/native-v3/mihomo-smp3-linux-amd64';server=ROOT/'artifacts/release-audit-20261013/bin/new-server-linux-v3'
 def native_run(name,fault,flags=None,old_server=False):
  srv=ROOT/'artifacts/release-audit-20261013/bin/old-production-server-linux' if old_server else server
  if old_server:srv.chmod(0o755)
  argv=['python3',str(ROOT/'scripts/test-native-slow-leg.py'),'--mihomo',str(native),'--server',str(srv),'--fault',fault,'--fault-end','45','--output',str(out/name/'process'),*(flags or [])]
  run(name,argv)
 native_run('native-single','slow',['--single'])
 for i in range(1,4):
  for fault in ['slow','blackhole']:native_run(f'native-{fault}-{i}',fault)
 for fault in ['slow','blackhole']:native_run(f'native-production-profile-{fault}',fault,['--production-profile'])
 native_run('interop-old-server-new-native','slow',['--legacy-server'],True)
 for i in range(1,4):run(f'primary-v3-{i}',['go','test','./core/...','-run','^TestSlowLegReliableStream$','-count=1','-timeout','5m','-v'],env={'SMP3_SLOW_LEG_DURATION':'40s'})
 run('rtt-baseline-v3',['go','test','./core/...','-run','TestReleaseRTTMatrix/(20|100|200|400)ms_(single|healthy)$','-timeout','10m','-count=1','-v'],base,{'SMP3_RELEASE_MATRIX':'1'})
 run('rtt-patch-v3',['go','test','./core/...','-run','^TestReleaseRTTMatrix$','-timeout','15m','-count=1','-v'],env={'SMP3_RELEASE_MATRIX':'1'})
 run('calibration-baseline-v3',['go','test','./core/...','-run','TestReleaseCarrierCalibration/core','-timeout','5m','-count=1','-v'],base,{'SMP3_RELEASE_CALIBRATION':'1'})
 run('calibration-patch-v3',['go','test','./core/...','-run','^TestReleaseCarrierCalibration$','-timeout','5m','-count=1','-v'],env={'SMP3_RELEASE_CALIBRATION':'1'})
 run('shared-soak-v3',['go','test','./core/...','-run','^TestReleaseSharedSoak$','-timeout','40m','-count=1','-v'],env={'SMP3_RELEASE_SOAK':a.soak})
 changed=[name for name,sha in src.items() if hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=sha]
 (out/'source-at-end.json').write_text(json.dumps({'changed':changed,'overall_numeric_acceptance':'SEPARATE FROM COMMAND EXIT STATUS'},indent=2))
 if changed:raise SystemExit('source changed during evidence run')
if __name__=='__main__':main()
