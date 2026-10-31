#!/usr/bin/env python3
"""Isolated baseline/patch activation regression; no production connection."""
import argparse,hashlib,json,os,pathlib,shutil,subprocess,time

def main():
 p=argparse.ArgumentParser();p.add_argument('--baseline-source',type=pathlib.Path,required=True);p.add_argument('--patch-source',type=pathlib.Path,default=pathlib.Path('core'));p.add_argument('--output',type=pathlib.Path,required=True);p.add_argument('--go',default='go');a=p.parse_args()
 a.output.mkdir(parents=True,exist_ok=False)
 fixture=pathlib.Path(__file__).parent/'fixtures/activation_budget_test.go'
 for label,src in [('baseline',a.baseline_source),('patch',a.patch_source)]:
  d=a.output/(label+'-source');d.mkdir();hashes={}
  for f in src.glob('*.go'):
   if not f.name.endswith('_test.go'):
    shutil.copyfile(f,d/f.name);hashes[f.name]=hashlib.sha256(f.read_bytes()).hexdigest()
  shutil.copyfile(fixture,d/fixture.name);hashes[fixture.name]=hashlib.sha256(fixture.read_bytes()).hexdigest()
  (d/'go.mod').write_text('module audit\n\ngo 1.22\n')
  argv=[a.go,'test','-count=3','-run','^TestAuditProductionBudgetActivation$','-timeout','30s','-v'];start=time.monotonic()
  with (a.output/(label+'.log')).open('wb') as f:r=subprocess.run(argv,cwd=d,env=dict(os.environ,GOWORK='off'),stdout=f,stderr=subprocess.STDOUT)
  (a.output/(label+'-command.json')).write_text(json.dumps({'argv':argv,'cwd':str(d.resolve()),'environment':{'GOWORK':'off'},'exit_code':r.returncode,'elapsed_seconds':time.monotonic()-start,'compiled_files_sha256':hashes},indent=2))
  print(label,r.returncode)
if __name__=='__main__':main()
