#!/usr/bin/env python3
from pathlib import Path
import shutil, sys
import subprocess
import stat

root = Path(sys.argv[1]).resolve()
payload = Path(__file__).resolve().parents[1] / 'src'
work = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else payload.parent / '.work'

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


def patch_sing_udp_contract():
    """Create a Go overlay for the pinned sing SOCKS UDP ingress.

    The dependency is intentionally not modified in the module cache. The
    overlay keeps the downstream fix reproducible while leaving the upstream
    checkout and shared sing buffer defaults untouched.
    """
    package_dir = Path(subprocess.check_output(
        [
            'go', 'list', '-mod=mod', '-f', '{{.Dir}}',
            'github.com/sagernet/sing/protocol/socks',
        ],
        cwd=root,
        text=True,
    ).strip())
    module_source = package_dir.parents[1]
    dependency_dir = work / 'sing-dependency'
    if dependency_dir.exists():
        for path in dependency_dir.rglob('*'):
            if path.is_file():
                path.chmod(path.stat().st_mode | stat.S_IWRITE)
    shutil.copytree(module_source, dependency_dir, dirs_exist_ok=True, copy_function=shutil.copy)
    for path in dependency_dir.rglob('*'):
        if path.is_file():
            path.chmod(path.stat().st_mode | stat.S_IWRITE)
    packet_path = dependency_dir / 'protocol' / 'socks' / 'packet.go'
    handshake_path = dependency_dir / 'protocol' / 'socks' / 'handshake.go'
    packet = packet_path.read_text()
    handshake = handshake_path.read_text()

    if 'const maxAssociatePacketSize = (1 << 16) - 1' not in packet:
        marker = 'var ErrInvalidPacket = E.New("socks5: invalid packet")\n'
        replacement = marker + '\nconst maxAssociatePacketSize = (1 << 16) - 1\n'
        if marker not in packet:
            raise RuntimeError(f'cannot locate SOCKS packet marker in {packet_path}')
        packet = packet.replace(marker, replacement, 1)
    if 'func (c *AssociatePacketConn) ReaderMTU() int {' not in packet:
        marker = 'func (c *AssociatePacketConn) ReaderOverhead() int {\n\treturn 3 + M.MaxSocksaddrLength\n}\n'
        replacement = marker + '\n// ReaderMTU reports payload capacity; ReaderOverhead accounts for the SOCKS5\n// UDP header when sing allocates the raw datagram receive buffer.\nfunc (c *AssociatePacketConn) ReaderMTU() int {\n\treturn maxAssociatePacketSize - c.ReaderOverhead()\n}\n'
        if marker not in packet:
            raise RuntimeError(f'cannot locate SOCKS ReaderOverhead in {packet_path}')
        packet = packet.replace(marker, replacement, 1)

    old_first_packet = 'firstPacket := buf.NewPacket()'
    new_first_packet = 'firstPacket := buf.NewSize(maxAssociatePacketSize)'
    if old_first_packet in handshake:
        handshake = handshake.replace(old_first_packet, new_first_packet, 1)
    elif new_first_packet not in handshake:
        raise RuntimeError(f'cannot locate SOCKS first packet allocation in {handshake_path}')

    packet_path.write_text(packet)
    handshake_path.write_text(handshake)

    go_mod = root / 'go.mod'
    go_mod_text = go_mod.read_text()
    replace_prefix = 'replace github.com/sagernet/sing => '
    replace_line = replace_prefix + dependency_dir.as_posix()
    lines = go_mod_text.splitlines()
    for index, line in enumerate(lines):
        if line.startswith(replace_prefix):
            lines[index] = replace_line
            break
    else:
        if lines and lines[-1] != '':
            lines.append('')
        lines.append(replace_line)
    go_mod.write_text('\n'.join(lines) + '\n')
    print(f'[+] scoped sing SOCKS UDP dependency copy: {dependency_dir}')


patch_sing_udp_contract()
print('[+] SMP3 multipath alpha2.3-r11 source installed')
