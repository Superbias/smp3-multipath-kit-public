#!/usr/bin/env python3
"""Read sanitized prerequisites and print a canary plan; performs no network or production action."""
import argparse,json,hashlib,math
from pathlib import Path
def main():
 p=argparse.ArgumentParser();p.add_argument('input',type=Path);p.add_argument('--output',type=Path);a=p.parse_args();cfg=json.loads(a.input.read_text(encoding='utf-8-sig'))
 blockers=[]
 if cfg.get('release_gate')!='PASS':blockers.append('Release gate remains FAIL; unresolved regression and engineering gates require closure or separately documented human risk acceptance.')
 for key in ['client_active_config_verified','leg0_business_health_verified','independent_leg0_entry_verified','client_transport_observability_verified']:
  if cfg.get(key) is not True:blockers.append(key+' is unverified')
 if not all(isinstance(x,(int,float)) and x>0 for x in cfg.get('measured_rtt_ms',[])) or len(cfg.get('measured_rtt_ms',[]))!=2:blockers.append('two measured production RTTs are missing')
 floor=cfg.get('production_goodput_floor_fraction')
 floor_source=cfg.get('production_goodput_floor_source')
 if isinstance(floor,bool) or not isinstance(floor,(int,float)) or not math.isfinite(floor) or floor<=0:blockers.append('production goodput floor must be explicitly supplied from the pre-existing SLO; no default fraction is applied')
 if not isinstance(floor_source,str) or not floor_source.strip():blockers.append('source for the pre-existing production goodput floor is missing')
 plan={'production_actions':'NONE','status':'ENVIRONMENT BLOCKED' if blockers else 'READY FOR HUMAN REVIEW ONLY','blockers':blockers,'human_authorization_required':True,'existing_sessions_preserved':'NOT GUARANTEED','policy_off':'fixed disables dynamic aggregation protection; activation/queue/CLOSE code fixes remain; not a validated business emergency path','code_rollback':'exact old binary/config backup; service restart can interrupt sessions','business_emergency_candidate':'manually select the verified independent leg0 business route for new flows; retry or terminate stalled old flows only under application owner direction','production_goodput_floor':{'fraction_of_unchanged_healthy_single_B':floor,'source':floor_source},'stages':[{'stage':0,'action':'Collect active client config, both RTTs, healthy old single baseline B, the already-approved production SLO goodput floor and its source, direct leg0 business proof and observable per-direction counters.'},{'stage':1,'action':'Local/isolated destination runs with identical production settings; no production route change. Retain all fixed windows and samples.'},{'stage':2,'action':'Only after explicit human authorization: one isolated test application/connection, five minutes, then stop and review. Do not replace the production binary as part of this plan.'},{'stage':3,'action':'Manually authorized expansion only after reviewed payload/EOF, fixed-window goodput meeting the pre-existing production SLO against unchanged healthy single B, no >2s halt, bounded resources/ledger and demonstrated recovery. Do not recompute B from a degraded new single-leg result.'}],'stop_conditions':['payload or EOF failure','fixed 10s useful goodput below the pre-declared production SLO floor after its existing detection window','application pause >2s','repeated recovery without healthy dual contribution','ledger/provider references or process resources grow across idle/exit','loss of observability'],'parameters':cfg.get('parameters',{})}
 plan['input_sha256']=hashlib.sha256(a.input.read_bytes()).hexdigest()
 text=json.dumps(plan,ensure_ascii=False,indent=2)
 if a.output:
  with a.output.open('x',encoding='utf-8') as f:f.write(text+'\n')
 print(text)
 raise SystemExit(2 if blockers else 0)
if __name__=='__main__':main()
