# SMP3 2.0.0 build status

Status: **release closure candidate**.

## Pinned inputs

- sing-box `v1.14.0-beta.14`
- sing-box commit `4902660f8424fef3c2a60dfcdce7aeadfe3f3b88`
- Mihomo `v1.19.28`
- Mihomo commit `cbd11db1e13a75d8e680e0fe7742c95be4cba2be`
- Go `1.25.5`

## Build workflow

The explicit-target pipeline is:

```bash
./validate-kit.sh
./scripts/build-phase6-artifacts.sh
```

It builds and checks these six release targets:

```text
dist/smp3-server-linux-amd64
dist/smp3-server-windows-amd64.exe
dist/mihomo-smp3-linux-amd64
dist/mihomo-smp3-windows-amd64.exe
dist/smp3-proxy-linux-amd64
dist/smp3-proxy-windows-amd64.exe
```

The sing client reports `1.14.0-beta.14-smp3-2.0.0`; the standalone server
reports `2.0.0`.

## Verified gates

The extracted Core, legacy semantic tests, sing adapter tests, source injector,
target checker, and release scans are run by the Phase 6E closure. The locked
Phase 6C/6D live evidence remains the basis for the network acceptance matrix;
Phase 6E does not repeat live fault injection or production migration.

The H3 100 MiB result remains `INCONCLUSIVE / EXTERNAL HARNESS-INTEROP ISSUE`:
the same failure reproduces on direct official aioquic without bridge, SOCKS5,
or SMP3 and is not an SMP3 release blocker.
