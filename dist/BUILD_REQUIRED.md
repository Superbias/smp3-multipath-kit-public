# SMP3 v2.3.3 unified release build and artifact note

The formal `dist/` directory contains the Standalone, Native, compatibility,
and integrated server Dashboard release binaries plus `SHA256SUMS`. The
standalone `smp3-panel` is retired and is not a v2.3.3 asset. They are built with
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
mihomo-smp3-linux-amd64
mihomo-smp3-windows-amd64.exe
smp3-proxy-linux-amd64
smp3-proxy-windows-amd64.exe
```

The release version is read from `VERSION` and injected into the Standalone
server, Standalone client, and Native Mihomo; the optional sing-box compatibility
artifact carries the same SMP3 product suffix. The Dashboard is part of
`smp3-server`. The SMP3 wire, Core, scheduler, Native adapter semantics, and
Carrier behavior are unchanged.
