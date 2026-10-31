#!/usr/bin/env python3
"""Compare checker outputs on pristine baseline and patch; restore golden bytes."""
from pathlib import Path
import argparse,hashlib,io,json,re,subprocess,tarfile
def main():
 p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);a=p.parse_args();root=Path.cwd();a.output.mkdir(parents=True,exist_ok=False)
 base=root/'.work/release-gate-baseline';base.mkdir(exist_ok=True)
 with tarfile.open(fileobj=io.BytesIO(subprocess.check_output(['git','archive','907b242c5f2532f774a3db1a877cf32740dcdfab']))) as src:src.extractall(base,filter='data')
 results={}
 for label,directory in [('baseline',base),('patch',root)]:
  golden=directory/'CORE_SOURCE_MANIFEST_AFTER';saved=golden.read_bytes()
  try:
   result=subprocess.run(['python3',str(directory/'scripts/check-core-migration-parity.py'),str(directory)],capture_output=True,text=True)
  finally:golden.write_bytes(saved)
  assert golden.read_bytes()==saved
  text=result.stdout+result.stderr;(a.output/f'parity-{label}.log').write_text(text)
  results[label]={'exit_code':result.returncode,'failures':{m[0]:{'before':m[1],'after':m[2]} for m in re.findall(r'([^\s:]+\.go): before=(\S+) after=(\S+)',text)}}
 expected={'carrier_capacity.go','stream_assignment_debug.go','stream_capacity.go','stream_engine.go','stream_repair.go','stream_tx.go','stream_tx_test.go','stream_admission.go','stream_slow_leg_test.go','stream_slow_leg_integration_test.go','stream_slow_leg_matrix_test.go','stream_slow_leg_shared_test.go','stream_slow_leg_drain_test.go','stream_release_audit_test.go','stream_release_invariants_test.go','stream_release_generation_test.go','stream_release_matched_config_test.go'}
 table=[]
 for name in sorted(set(results['baseline']['failures'])|set(results['patch']['failures'])):
  old=results['baseline']['failures'].get(name);new=results['patch']['failures'].get(name)
  classification='original_unchanged' if old==new else ('expected_patch_change' if name in expected else 'unexpected_change')
  path=root/'core'/name
  head=subprocess.run(['git','show','907b242c5f2532f774a3db1a877cf32740dcdfab:core/'+name],capture_output=True)
  raw=path.read_bytes() if path.exists() else b''
  normalized=lambda b:b.replace(b'\r\n',b'\n')
  normalized_equal=head.returncode==0 and normalized(raw)==normalized(head.stdout)
  if classification=='unexpected_change' and normalized_equal:classification='preexisting_checkout_eol'
  table.append({'file':name,'baseline':old,'patch':new,'classification':classification,'head_raw_sha256':hashlib.sha256(head.stdout).hexdigest() if head.returncode==0 else None,'working_raw_sha256':hashlib.sha256(raw).hexdigest(),'normalized_equal_to_head':normalized_equal})
 (a.output/'parity-difference.json').write_text(json.dumps({'runs':results,'differences':table,'overall':'FAIL'},indent=2))
 for label,directory in [('baseline',base),('patch',root)]:
  argv=['python3',str(directory/'scripts/run-standalone-go.py'),str(directory),'test','--enable-cgo','-race','-count=3']
  with (a.output/f'legacy-{label}.log').open('w') as log:code=subprocess.run(argv,stdout=log,stderr=subprocess.STDOUT).returncode
  (a.output/f'legacy-{label}-command.json').write_text(json.dumps({'command':argv,'exit_code':code},indent=2))
 print(json.dumps({'baseline_parity':results['baseline']['exit_code'],'patch_parity':results['patch']['exit_code'],'classifications':{k:sum(x['classification']==k for x in table) for k in ['original_unchanged','expected_patch_change','preexisting_checkout_eol','unexpected_change']}}))
if __name__=='__main__':main()
