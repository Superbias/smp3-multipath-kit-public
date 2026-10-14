#!/usr/bin/env python3
"""Serial extended fixed-window verification. Run after verify-slow-leg-fix.py."""
from pathlib import Path
import hashlib
import importlib.util
import json
import os
import statistics
import subprocess
import sys
import time


def main():
    root = Path(__file__).resolve().parents[1]
    out = root / 'artifacts/slow-leg'
    ledger = []

    def run(name, argv, env=None):
        start = time.monotonic()
        with (out / f'{name}.log').open('w') as log:
            code = subprocess.run(argv, cwd=root, env=dict(os.environ, **(env or {})), stdout=log, stderr=subprocess.STDOUT).returncode
        row = dict(name=name, command=argv, env=env or {}, exit_code=code, seconds=time.monotonic()-start)
        ledger.append(row)
        (out / 'extended-ledger.json').write_text(json.dumps(ledger, indent=2))
        print(json.dumps(row), flush=True)

    if '--check-only' not in sys.argv:
        run('extended-performance', ['go', 'test', './core/...', '-run', 'TestSlowLegReliableStream/(single_80|cold_80_1|hot_100_1|hot_blackhole)$', '-count=3', '-timeout=20m', '-v'],
            {'SMP3_SLOW_LEG_EXTENDED':'1', 'SMP3_SLOW_LEG_DURATION':'50s'})
        run('extended-wave', ['go', 'test', './core/...', '-run', 'TestSlowLegReliableStream/wave_01_05_1_5$', '-count=3', '-v'],
            {'SMP3_SLOW_LEG_EXTENDED':'1', 'SMP3_SLOW_LEG_DURATION':'50s'})
        for name in ['single', 'slow', 'blackhole']:
            run(f'extended-native-{name}', ['python3', 'scripts/test-native-slow-leg.py', '--mihomo', 'artifacts/slow-leg/native/mihomo-smp3-linux-amd64',
                '--server', 'artifacts/slow-leg/smp3-server', '--fault', 'slow' if name=='single' else name, '--fault-end', '45', '--output', f'artifacts/slow-leg/extended-native-{name}',
                *(['--single'] if name=='single' else [])])
        run('extended-fixture-race', ['go', 'test', '-race', './core/...', '-run', 'TestSlowLegReliableStream|TestSlowLegRescueDoesNotChangeGuardOwnerOrProveRejoin', '-count=1'])
    spec = importlib.util.spec_from_file_location('compare', root/'scripts/test-slow-leg-compare.py')
    compare = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(compare)
    rows = compare.results(out/'extended-performance.log')
    primary = json.loads((out/'final-performance.json').read_text())['runs']
    accepted = True
    for name in ['cold_80_1', 'hot_100_1', 'hot_blackhole']:
        if len(rows.get(name, [])) != 3:
            accepted = False
            continue
        for index, windows in enumerate(rows[name]):
            baseline = rows['single_80'][index] if name == 'cold_80_1' else primary['single'][index]
            b = statistics.median(baseline[1:4])
            stable = windows[1:4] if name == 'cold_80_1' else windows[2:5]
            if len(stable) != 3 or any(v < .9*b for v in stable):
                accepted = False
    (out/'extended-performance.json').write_text(json.dumps({'runs':rows, 'result':'PASS' if accepted else 'FAIL'}, indent=2))
    wave=compare.results(out/'extended-wave.log') if (out/'extended-wave.log').exists() else {}
    if len(wave.get('wave_01_05_1_5',[]))!=3:
        accepted=False
    for index,windows in enumerate(wave.get('wave_01_05_1_5',[])):
        b=statistics.median(primary['single'][index][1:4])
        if len(windows)!=5 or any(v<.9*b for v in windows[1:5]):
            accepted=False
    (out/'extended-wave.json').write_text(json.dumps({'runs':wave,'result':'PASS' if accepted else 'FAIL'},indent=2))
    def interpolate(samples, t):
        points=[(0,0)]+[(x['seconds'],x['bytes']) for x in samples]
        for (a,x),(b,y) in zip(points,points[1:]):
            if a<=t<=b:
                return x+(y-x)*(t-a)/(b-a)
        raise ValueError('window outside measured application samples')
    def window(samples,a):
        return (interpolate(samples,a+10)-interpolate(samples,a))*8/10/1e6
    native={}
    try:
        for name in ['single','slow','blackhole']:
            result=json.loads((out/f'extended-native-{name}/result.json').read_text())
            native[name]={'stable_mbps':[window(result['samples'],a) for a in [15,25,35]],
                'restored_mbps':window(result['samples'],55), 'drain_pass':result['drain_pass'],
                'payload_pass':result['payload_pass'], 'carrier_connections':result['carrier_connections'],
                'write_amplification':sum(result['final_transport']['written_bytes'])/result['final_transport']['useful_ack_total']}
        b=statistics.median(native['single']['stable_mbps'])
        for name in ['slow','blackhole']:
            if any(v<.9*b for v in native[name]['stable_mbps']) or native[name]['restored_mbps']<1.5*b:
                accepted=False
    except (OSError,KeyError,ValueError):
        accepted=False
    (out/'extended-native-windows.json').write_text(json.dumps({'native':native,'result':'PASS' if accepted else 'FAIL'},indent=2))
    # No runtime source changed after Native build; retain original full build
    # manifest and explicitly verify only its compiled production GoFiles.
    proof=json.loads((out/'final-native-source-proof.json').read_text())
    matches=all(hashlib.sha256((root/'core'/n).read_bytes()).hexdigest()==proof['core_sha256'][n] for n in proof['runtime_files'])
    (out/'extended-runtime-proof.json').write_text(json.dumps({'all_compiled_core_files_match_build':matches,
        'test_fixture_added_after_build':'stream_slow_leg_integration_test.go', 'runtime_files':proof['runtime_files']},indent=2))
    print('extended_fixed_windows', 'PASS' if accepted else 'FAIL', 'runtime_source', matches, flush=True)
    return 0 if accepted and matches and all(x['exit_code']==0 for x in ledger) else 1


if __name__=='__main__':
    raise SystemExit(main())
