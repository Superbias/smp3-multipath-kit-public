# SMP3 v2.3.2 unified release build and artifact note

The formal `dist/` directory contains the Standalone, Native, compatibility,
and R15 Panel release binaries plus `SHA256SUMS`. They are built with
explicit target settings by:

```bash
./scripts/build-phase6-artifacts.sh
```

Expected targets:

```text
smp3-server-linux-amd64
smp3-server-windows-amd64.exe
smp3-client-linux-amd64
smp3-client-windows-amd64.exe
smp3-panel-linux-amd64
smp3-panel-windows-amd64.exe
mihomo-smp3-linux-amd64
mihomo-smp3-windows-amd64.exe
smp3-proxy-linux-amd64
smp3-proxy-windows-amd64.exe
```

The release version is read from `VERSION` and injected into the Standalone
server, Standalone client, and Panel. Native Mihomo and the optional sing-box
compatibility artifact retain their upstream/runtime identities. The SMP3
wire, Core, scheduler, Native adapter semantics, and Carrier behavior are
unchanged.
