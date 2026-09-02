# SMP3 2.2.0 Integration Review Report

## STATUS

```text
SMP3 2.2.0 INTEGRATION REVIEW: PASS
BRANCH: integration/smp3-2.2.0-sidecar
VERSION: 2.2.0
READY_FOR_RELEASE: YES
```

This report records the integration review and release-candidate preparation.
It must not be read as a GitHub publication notice. No tag, GitHub Release, or
production deployment is part of this phase.

## MAIN BASE

```text
MAIN_BASE_SOURCE: origin/main
MAIN_BASE: b172cfafeec7165845234f6f4afd55dc46e703e3
REMOTE_REFRESHED: YES
```

## INTEGRATED COMMITS

```text
qualification/standalone-sidecar-rc merged without conflicts
merge commit: 053ea69
```

## DIFF CLASSIFICATION

- Standalone Sidecar client: `client/`, `cmd/smp3-client/`.
- Standalone server host extension: `server/` sidecar listener and readiness
  support.
- Configuration: sidecar listener and client examples.
- Tests/harness: standalone client tests and qualification harnesses.
- Documentation/examples: deployment, Sidecar, README, and examples.
- Reports/data: qualification evidence and this integration report.
- Version metadata: SMP3-controlled client/server/kit version set to 2.2.0.

Canonical Core, canonical Wire, native Mihomo adapter, and native sing adapter
are outside the expected Sidecar integration diff and must remain unchanged.

## CORE/WIRE INTEGRITY

```text
CANONICAL_WIRE_CHANGED: NO
CORE_SEMANTICS_CHANGED: NO
HELLO_STREAM_VERSION: 4
HELLO_DATAGRAM_VERSION: 5
CORE_CHECK: 18/18 PASS
WIRE_CHECK: 22/22 PASS
```

`SMP3RDY1` is a sidecar-only host readiness extension. It is emitted only by
`sidecar_listeners` after authenticated canonical HELLO admission; native and
legacy listeners remain READY-free.

## SERVER BACKWARD COMPATIBILITY

Existing `listen`/`listeners` configuration remains valid without
`sidecar_listeners`. Native legacy TCP/UDP behavior and READY-free operation
passed the focused regression; `SERVER_CONFIG_BACKWARD_COMPATIBLE: YES`.

## SIDECAR HOST CONTRACT

The Sidecar depends only on standard SOCKS5 TCP CONNECT from its configured
upstream host. Destination routing and outer carrier termination remain
external responsibilities. It does not depend on Mihomo, sing-box, Xray, or a
specific carrier protocol type.

## READY CONTRACT

The authenticated `SMP3RDY1` record binds the current implementation's session,
leg, and mode readiness context after HELLO authentication, nonce replay checks,
and session admission. It is not canonical SMP3 Wire v6 and does not alter
HELLO v4/v5.

## NATIVE REGRESSION

```text
Native TCP 8 MiB exact: PASS
Native UDP 64/64: PASS
Native RX activation and same-session leg1: PASS
Legacy READY-free: PASS
```

## STOCK MIHOMO SIDECAR REGRESSION

```text
Stock TCP 8 MiB exact: PASS
Stock UDP 64/64: PASS
READY auth: PASS
False-success fallback: PASS
Persistent UDP association behavior: inherited from valid Phase 2A evidence
```

## VERSION

```text
SMP3 release candidate: 2.2.0
smp3-client -version: 2.2.0
smp3-server -version: 2.2.0
Upstream Mihomo identity: unchanged
```

## BUILD MATRIX / ASSETS

```text
smp3-client-windows-amd64.exe: BUILT
smp3-client-linux-amd64: BUILT
smp3-server-windows-amd64.exe: BUILT
smp3-server-linux-amd64: BUILT
SHA256SUMS-RC: GENERATED AND VERIFIED
```

Local RC staging: `D:\smp3-singbox\.release\smp3-2.2.0-rc`.

| Asset | Size | SHA256 |
|---|---:|---|
| `smp3-client-windows-amd64.exe` | 4,314,112 | `a677150fe68d189acb4e008a1e4a9d19894f3ebd8d42d0c36910ab043c5f356e` |
| `smp3-client-linux-amd64` | 4,206,288 | `51712311352607d7eeb35b84904af7f5b03a2fb52f4a0f8aa4405d0ffc034f87` |
| `smp3-server-windows-amd64.exe` | 4,322,304 | `087a92694cda5114050a41843b5704d4917a3e355e12f99f789487d0fec0e0a0` |
| `smp3-server-linux-amd64` | 4,200,026 | `7f9d50c45bdfbf044d7faa854f3690a5eb9e5606e270c24a0c6ec21c7d0f29b6` |

`SHA256SUMS-RC` SHA256:
`70f579cf06a9c688d511f9b83b1a540f5636ea92fd2eddaa83edc89301a1b86b`.

Windows and Linux Sidecar bundles were generated under the same staging
directory and each bundle `SHA256SUMS` was independently verified (`4/4` and
`5/5`).

## SECURITY SCAN

```text
SENSITIVE_FINDINGS: 0
PUBLIC_IPV4_FINDINGS: 0
```

## RELEASE NOTES

Draft: `SMP3_2.2.0_RELEASE_NOTES_DRAFT.md`.

## KNOWN LIMITATIONS

- Sidecar carries extra local proxy/socket/process overhead.
- Native custom Mihomo and stock Mihomo Sidecar are the qualified hosts.
- Other SOCKS5 hosts require independent qualification.
- Android wrapper/APK is not included.
- `SMP3RDY1` is a Sidecar host readiness extension, not canonical SMP3 Wire.

## FINAL GATE

```text
READY_FOR_RELEASE: YES
```

No push, merge into main, tag, GitHub Release, or production deployment has
been performed.
