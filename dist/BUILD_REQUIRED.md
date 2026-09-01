# SMP3 2.0.0 build and artifact note

The formal `dist/` directory contains the six release binaries and
`SHA256SUMS`. They are built with explicit target settings by:

```bash
./scripts/build-phase6-artifacts.sh
```

Expected targets:

```text
smp3-server-linux-amd64
smp3-server-windows-amd64.exe
mihomo-smp3-linux-amd64
mihomo-smp3-windows-amd64.exe
smp3-proxy-linux-amd64
smp3-proxy-windows-amd64.exe
```

The release is `2.0.0`. The sing client reports
`1.14.0-beta.14-smp3-2.0.0`; the standalone server reports `2.0.0`.
