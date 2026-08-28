#!/usr/bin/env python3
from pathlib import Path
import shutil, sys

root = Path(sys.argv[1]).resolve()
payload = Path(__file__).resolve().parents[1] / 'src'

def edit(path, fn):
    p=root/path; s=p.read_text(); n=fn(s)
    if n==s: print(f'[=] {path}: already patched or no change')
    else: p.write_text(n); print(f'[+] patched {path}')

def constant(s):
    if 'TypeMultipath' not in s:
        if '\tTypeBridge             = "bridge"\n' in s:
            s=s.replace('\tTypeBridge             = "bridge"\n','\tTypeBridge             = "bridge"\n\tTypeMultipath          = "multipath"\n',1)
        else:
            s=s.replace('\tTypeDirect             = "direct"\n','\tTypeDirect             = "direct"\n\tTypeMultipath          = "multipath"\n',1)
    if 'case TypeMultipath:' not in s:
        marker='\tcase TypeBridge:\n\t\treturn "Bridge"\n'
        if marker in s: s=s.replace(marker, marker+'\tcase TypeMultipath:\n\t\treturn "Multipath"\n',1)
        else:
            marker='\tcase TypeDirect:\n\t\treturn "Direct"\n'
            s=s.replace(marker, marker+'\tcase TypeMultipath:\n\t\treturn "Multipath"\n',1)
    return s

def registry(s):
    if 'protocol/multipath' not in s:
        s=s.replace('\t"github.com/sagernet/sing-box/protocol/mixed"\n','\t"github.com/sagernet/sing-box/protocol/mixed"\n\t"github.com/sagernet/sing-box/protocol/multipath"\n',1)
    if 'multipath.RegisterInbound(registry)' not in s:
        s=s.replace('\tdirect.RegisterInbound(registry)\n','\tdirect.RegisterInbound(registry)\n\tmultipath.RegisterInbound(registry)\n',1)
    if 'multipath.RegisterOutbound(registry)' not in s:
        marker='\tbridge.RegisterOutbound(registry)\n'
        if marker in s: s=s.replace(marker, marker+'\tmultipath.RegisterOutbound(registry)\n',1)
        else: s=s.replace('\tdirect.RegisterOutbound(registry)\n','\tdirect.RegisterOutbound(registry)\n\tmultipath.RegisterOutbound(registry)\n',1)
    return s

edit(Path('constant/proxy.go'), constant)
edit(Path('include/registry.go'), registry)
(root/'option').mkdir(exist_ok=True)
(root/'protocol/multipath').mkdir(parents=True, exist_ok=True)
shutil.copy2(payload/'option/multipath.go', root/'option/multipath.go')
for f in (payload/'protocol/multipath').glob('*.go'): shutil.copy2(f, root/'protocol/multipath'/f.name)
print('[+] SMP3 multipath alpha2.3-r10 source installed')
