#!/usr/bin/env python3
"""Inject the Phase 5A Mihomo skeleton into a pinned Mihomo checkout."""

from pathlib import Path
import shutil
import subprocess
import sys


MODULE = "github.com/Superbias/smp3-multipath-kit-public/smp3core"
PIN = "v1.19.28"
COMMIT = "cbd11db1e13a75d8e680e0fe7742c95be4cba2be"


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: apply_mihomo_adapter.py MIHOMO_ROOT KIT_ROOT", file=sys.stderr)
        return 2
    mihomo = Path(sys.argv[1]).resolve()
    kit = Path(sys.argv[2]).resolve()
    source = kit / "adapters" / "mihomo"
    commit = subprocess.check_output(["git", "-C", str(mihomo), "rev-parse", "HEAD"], text=True).strip()
    if commit != COMMIT:
        raise SystemExit(f"unexpected Mihomo commit: {commit}; want {COMMIT}")
    outbound = mihomo / "adapter" / "outbound"
    if not (source / "outbound" / "smp3.go").is_file():
        raise SystemExit(f"missing Mihomo skeleton source: {source}")
    outbound.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source / "outbound" / "smp3.go", outbound / "smp3.go")
    shutil.copy2(source / "outbound" / "smp3_udp.go", outbound / "smp3_udp.go")
    shutil.copy2(source / "outbound" / "smp3_udp_b2.go", outbound / "smp3_udp_b2.go")
    shutil.copy2(source / "outbound" / "smp3_udp_test.go", outbound / "smp3_udp_test.go")
    shutil.copy2(source / "outbound" / "smp3_udp_b1_test.go", outbound / "smp3_udp_b1_test.go")
    shutil.copy2(source / "outbound" / "smp3_udp_b2_test.go", outbound / "smp3_udp_b2_test.go")
    shutil.copy2(source / "outbound" / "smp3_udp_r3_test.go", outbound / "smp3_udp_r3_test.go")
    shutil.copy2(source / "outbound" / "smp3_test.go", outbound / "smp3_test.go")
    shutil.copy2(source / "adapter_parse_test.go", mihomo / "adapter" / "smp3_bootstrap_test.go")
    shutil.copy2(source / "config_parse_test.go", mihomo / "config" / "smp3_parse_test.go")

    host = source / "host"
    socks = mihomo / "listener" / "socks"
    shutil.copy2(host / "socks_udp_headroom.go", socks / "udp_headroom.go")
    shutil.copy2(host / "socks_udp_headroom_test.go", socks / "udp_headroom_test.go")
    udp_listener = socks / "udp.go"
    udp_text = udp_listener.read_text(encoding="utf-8")
    if "waitReadSocksUDP(l)" not in udp_text:
        import_line = '\tN "github.com/metacubex/mihomo/common/net"\n'
        setup_line = "\tconn := N.NewEnhancePacketConn(l)\n"
        read_line = "\t\t\tdata, put, remoteAddr, err := conn.WaitReadFrom()"
        if import_line not in udp_text or setup_line not in udp_text or read_line not in udp_text:
            raise SystemExit(f"unexpected pinned SOCKS UDP listener context: {udp_listener}")
        udp_text = udp_text.replace(import_line, "", 1)
        udp_text = udp_text.replace(setup_line, "", 1)
        udp_text = udp_text.replace(read_line, "\t\t\tdata, put, remoteAddr, err := waitReadSocksUDP(l)", 1)
        udp_listener.write_text(udp_text, encoding="utf-8")

    parser = mihomo / "adapter" / "parser.go"
    text = parser.read_text(encoding="utf-8")
    marker = '\tdefault:\n\t\treturn nil, fmt.Errorf("unsupport proxy type: %s", proxyType)'
    block = "\tcase \"smp3\":\n\t\tsmp3Option := &outbound.SMP3Option{BasicOption: basicOption}\n\t\terr = decoder.Decode(mapping, smp3Option)\n\t\tif err != nil {\n\t\t\tbreak\n\t\t}\n\t\tproxy, err = outbound.NewSMP3(*smp3Option)\n"
    if 'case "smp3":' not in text:
        if marker not in text:
            raise SystemExit(f"cannot locate proxy parser default case: {parser}")
        text = text.replace(marker, block + marker, 1)
        parser.write_text(text, encoding="utf-8")

    config = mihomo / "config" / "config.go"
    config_text = config.read_text(encoding="utf-8")
    validation_marker = "\t// parse proxy\n\tfor idx, mapping := range proxiesConfig {"
    validation_block = "\tavailableSMP3ProxyNames := make(map[string]struct{}, len(proxiesConfig))\n\tfor _, mapping := range proxiesConfig {\n\t\tif name, ok := mapping[\"name\"].(string); ok && name != \"\" {\n\t\t\tavailableSMP3ProxyNames[name] = struct{}{}\n\t\t}\n\t}\n\n"
    if "availableSMP3ProxyNames :=" not in config_text:
        if validation_marker not in config_text:
            raise SystemExit(f"cannot locate proxy parse loop: {config}")
        config_text = config_text.replace(validation_marker, validation_block + "\t// parse proxy\n\tfor idx, mapping := range proxiesConfig {", 1)
    call_marker = "\tfor idx, mapping := range proxiesConfig {\n\t\tproxy, err := adapter.ParseProxy(mapping, adapter.WithTunnelForAPI(T.Tunnel))"
    call_replacement = "\tfor idx, mapping := range proxiesConfig {\n\t\tif err := outbound.ValidateSMP3Config(mapping, availableSMP3ProxyNames); err != nil {\n\t\t\treturn nil, nil, fmt.Errorf(\"proxy %d: %w\", idx, err)\n\t\t}\n\t\tproxy, err := adapter.ParseProxy(mapping, adapter.WithTunnelForAPI(T.Tunnel))"
    if "outbound.ValidateSMP3Config(mapping, availableSMP3ProxyNames)" not in config_text:
        if call_marker not in config_text:
            raise SystemExit(f"cannot locate proxy parse call: {config}")
        config_text = config_text.replace(call_marker, call_replacement, 1)
    config.write_text(config_text, encoding="utf-8")

    go_mod = mihomo / "go.mod"
    mod_text = go_mod.read_text(encoding="utf-8")
    if f"require {MODULE} v0.0.0" not in mod_text:
        mod_text += f"\nrequire {MODULE} v0.0.0\n"
    replace = f"replace {MODULE} => ../../core"
    if replace not in mod_text:
        mod_text += replace + "\n"
    go_mod.write_text(mod_text, encoding="utf-8")
    print(f"mihomo skeleton injected pin={PIN} commit={COMMIT} module={MODULE} socks_udp_headroom=yes replace=../../core")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
