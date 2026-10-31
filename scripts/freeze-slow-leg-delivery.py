#!/usr/bin/env python3
"""Freeze complete source patch including new files, and immutable evidence."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import zipfile

def sha(data): return hashlib.sha256(data).hexdigest()
def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--baseline',default='907b242c5f2532f774a3db1a877cf32740dcdfab')
    parser.add_argument('--evidence',type=Path,action='append')
    parser.add_argument('--exclude-evidence-prefix',action='append',default=[])
    args=parser.parse_args(); root=Path.cwd(); out=args.output.resolve()
    if out.exists(): raise SystemExit('Refusing to overwrite a freeze directory')
    out.mkdir(parents=True)
    tracked=subprocess.check_output(['git','ls-files','-z']).decode().split('\0')
    new=subprocess.check_output(['git','ls-files','--others','--exclude-standard','-z']).decode().split('\0')
    paths=sorted({p for p in tracked+new if p and not p.startswith('artifacts/') and (root/p).is_file()})
    contents={name:(root/name).read_bytes() for name in paths}
    env=dict(os.environ,GIT_INDEX_FILE=str(out/'temporary-index'))
    subprocess.run(['git','read-tree',args.baseline],env=env,check=True)
    stage_paths=sorted({p for p in tracked+new if p and not p.startswith('artifacts/')})
    subprocess.run(['git','add','-A','--',*stage_paths],env=env,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    patch=subprocess.check_output(['git','diff','--cached','--binary',args.baseline],env=env)
    (out/'complete.patch').write_bytes(patch)
    # Supplemental reviewer view only; complete.patch remains authoritative.
    (out/'semantic-review.patch').write_bytes(subprocess.check_output(
        ['git','diff','--cached','--binary','--ignore-space-at-eol',args.baseline],env=env))
    (out/'temporary-index').unlink()
    inventory=[]
    with zipfile.ZipFile(out/'source.zip','w',zipfile.ZIP_DEFLATED) as archive:
        for name in paths:
            data=contents[name]; archive.writestr(name,data)
            inventory.append({'path':name,'sha256':sha(data),'bytes':len(data),'new':name in new})
    evidence=[]
    with zipfile.ZipFile(out/'original-evidence.zip','w',zipfile.ZIP_DEFLATED) as archive:
        seen=set()
        for evidence_root in args.evidence or [Path('artifacts/slow-leg')]:
            for path in sorted(evidence_root.resolve().rglob('*')):
                if not path.is_file() or path.is_relative_to(out): continue
                name=path.relative_to(root).as_posix()
                if name in seen or any(name.startswith(p) for p in args.exclude_evidence_prefix): continue
                seen.add(name); data=path.read_bytes()
                archive.writestr(name,data); evidence.append({'path':name,'sha256':sha(data),'bytes':len(data)})
    base=subprocess.check_output(['git','archive',args.baseline])
    (out/'baseline.tar').write_bytes(base)
    if any((root/name).read_bytes()!=data for name,data in contents.items()):
        raise SystemExit('Source changed during freeze; package is incomplete, retain failure and retry in a new directory')
    current_names=set(subprocess.check_output(['git','ls-files','-z']).decode().split('\0'))
    current_names.update(subprocess.check_output(['git','ls-files','--others','--exclude-standard','-z']).decode().split('\0'))
    current_paths=sorted(p for p in current_names if p and not p.startswith('artifacts/') and (root/p).is_file())
    if current_paths!=paths:
        raise SystemExit('Source file set changed during freeze; retain incomplete package and retry')
    manifest={'baseline':args.baseline,'head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        'source':inventory,'evidence':evidence,'packages':{p.name:sha(p.read_bytes()) for p in out.iterdir() if p.is_file()}}
    (out/'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    print(json.dumps({'output':str(out),'source_files':len(paths),'new_files':[x['path'] for x in inventory if x['new']],
        'evidence_files':len(evidence),'package_sha256':manifest['packages']},indent=2))
if __name__=='__main__': main()
