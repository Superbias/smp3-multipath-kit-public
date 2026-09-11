# Unified Production Release v2.3.0

## Scope and disposition

This release aligns the three product lines:

- Standalone SMP3 client/server/Core and compatible sidecar artifacts.
- Native Mihomo SMP3 adapter built against the pinned Mihomo source.
- R15 read-only Panel/Observability REST, SSE, history, events, health, and UI.

The release source is the `release/v2.3.0` branch at `bb1f23f`. The only
change after the qualified product snapshot `6338e15` is a test-only increase
of a race-instrumented telemetry observation window; no product/data-plane
source changed.

Existing R14 and R15 qualification verdicts are preserved. No synthetic C5,
performance test, production deployment, or production restart was performed
for this release operation.

## Repository and source roots

```text
CURRENT_HEAD: bb1f23ff96745096286e0e61cce279918013106b
CURRENT_BRANCH: release/v2.3.0
LATEST_EXISTING_TAG: v2.2.0
PROPOSED_RELEASE_VERSION: v2.3.0
VERSION_RULE: next semantic product tag after the existing v2.2.0 line

STANDALONE_SOURCE_ROOT: core/, client/, server/, cmd/smp3-client/, cmd/smp3-server/
NATIVE_SOURCE_ROOT: adapters/mihomo/ plus pinned MetaCubeX/mihomo v1.19.28
PANEL_SOURCE_ROOT: panel/
SERVER_SOURCE_ROOT: server/, cmd/smp3-server/
```

The Standalone R14 qualified snapshot and the Native adapter qualified
snapshot are `6338e15`. Product source is unchanged since that qualification;
therefore no R14 synthetic C5 requalification was required.

```text
STANDALONE_SOURCE_HEAD: bb1f23ff96745096286e0e61cce279918013106b
STANDALONE_LAST_QUALIFIED_HEAD: 6338e15374eb7190c6320f5197df8a187d3364d0
STANDALONE_SOURCE_CHANGED_SINCE_QUALIFICATION: NO (product source)

NATIVE_SOURCE_HEAD: bb1f23ff96745096286e0e61cce279918013106b
NATIVE_LAST_RELEASE_HEAD: 8173afdffa184ea574a8612f5f514b92020da86c
NATIVE_LAST_QUALIFIED_HEAD: 6338e15374eb7190c6320f5197df8a187d3364d0
NATIVE_SOURCE_CHANGED_SINCE_LAST_RELEASE: YES (qualified adapter promotion)
NATIVE_SOURCE_CHANGED_SINCE_QUALIFICATION: NO

PANEL_SOURCE_HEAD: bb1f23ff96745096286e0e61cce279918013106b
R15_IMPLEMENTATION_PRESENT: YES
```

## Native provenance and semantics

```text
NATIVE_APPLICATION_NAME: Mihomo Native / Clash Party sidecar
NATIVE_ARTIFACT_NAME: mihomo-smp3-linux-amd64, mihomo-smp3-windows-amd64.exe
NATIVE_SOURCE_PATH: adapters/mihomo/ + pinned MetaCubeX/mihomo checkout
NATIVE_BUILD_ENTRYPOINT: scripts/apply_mihomo_adapter.py; go build -trimpath -mod=mod .
NATIVE_CURRENT_VERSION: Mihomo v1.19.28 + SMP3 adapter
NATIVE_LAST_RELEASE_VERSION: v2.2.0
NATIVE_CURRENT_PRODUCTION_BINARY_SHA256: 2C0B0C36C85A89BF6E0AE1AEB1E543B0BF74531A6153AF4B2D36E1F66E55FE1C
NATIVE_CURRENT_PRODUCTION_SOURCE_PROVENANCE: known
NATIVE_RELEASE_SOURCE_MATCHES_CURRENT_HEAD: YES (source-equivalent; build-info paths may differ)
```

The release Native build pins upstream Mihomo commit
`cbd11db1e13a75d8e680e0fe7742c95be4cba2be`. The audit found no new
post-qualification change to Native adapter semantics, leg mapping, child
outbound selection, fallback behavior, server listener semantics, wire, Core,
or scheduler. The current Native topology remains the qualified primary/native
SMP3 path.

## Native/Standalone identity

| Product | Source HEAD | Release artifact SHA | Qualified |
| --- | --- | --- | --- |
| Standalone | `bb1f23f` | see `SHA256SUMS` for client/server artifacts | YES |
| Native | `bb1f23f` + pinned Mihomo `cbd11db1` | see `SHA256SUMS` for Mihomo artifacts | YES |
| Panel | `bb1f23f` | see `SHA256SUMS` for Panel artifacts | YES |

## Official artifact provenance

All artifacts are `GOARCH=amd64`; Standalone/Panel builds use `CGO_ENABLED=0`
and `-trimpath -ldflags=-buildid=`. Native artifacts use the pinned upstream
checkout plus the tracked SMP3 adapter overlay. The official asset set is the
following ten binaries plus `SHA256SUMS`:

| Artifact | Build profile | Size | SHA256 |
| --- | --- | ---: | --- |
| `smp3-client-linux-amd64` | Go, linux/amd64 | 4291805 | `22fbfb5821422b29042906664cc84c737b2a9da4ca2c60e0bb5bd22325d19137` |
| `smp3-client-windows-amd64.exe` | Go, windows/amd64 | 4400640 | `65471cf2e36caaca888abc53e1567f2e1a6513286025f82db31f91b4df42b9c0` |
| `smp3-server-linux-amd64` | Go, linux/amd64 | 8239441 | `adb5ebddbb1ae286b0f083aa0b7995ccf57a6ecf48c75dec20568f157c063175` |
| `smp3-server-windows-amd64.exe` | Go, windows/amd64 | 8336896 | `84cd3de73e3fb11427b0f268322f8ec18efd2e03dc399f960d2cec7d6ea67adb` |
| `smp3-proxy-linux-amd64` | pinned sing-box, linux/amd64 | 79876244 | `e09614e1bbcb8cfb614cb2b524f37cb4237f4a98946d9784d594b6e23af9d98e` |
| `smp3-proxy-windows-amd64.exe` | pinned sing-box, windows/amd64 | 80488960 | `1b29a2bfca0b0527faf60a0dad749cf406e3387fa342633aaee3895cad28ef4d` |
| `mihomo-smp3-linux-amd64` | pinned Mihomo, linux/amd64 | 51406766 | `f59a4f10c7db346a6968c7a134a4631a987651f05d912d2f32d7f8ed02101515` |
| `mihomo-smp3-windows-amd64.exe` | pinned Mihomo, windows/amd64 | 49423360 | `275a5b4d2796683c1153c97ed55d0f1742da36a253930151aed6af4f4bc140af` |
| `smp3-panel-linux-amd64` | Go, linux/amd64 | 8260482 | `043fef53b28a6d6028f6cc8da8c1e2da64b73908a3eea4dd8ee2568f82603bfb` |
| `smp3-panel-windows-amd64.exe` | Go, windows/amd64 | 8372736 | `f1b7e3887d170275cfdc9c994d5a826df52abf0cb711509a7e8c8a5ed67a5d3f` |

`ALL_RELEASE_ARTIFACT_HASHES_VERIFIED: YES` and all ten format/architecture
checks passed.

## Required release gates

```text
STANDALONE_TESTS: PASS
NATIVE_TESTS: PASS (adapter/config tests and pinned multipath regression evidence)
PANEL_TESTS: PASS
SERVER_TESTS: PASS
RACE_TESTS: PASS (Core, Client, Panel, and Server under WSL Ubuntu with GOMAXPROCS=8)
RELEASE_BUILD: PASS
```

Additional checks passed: Go vet for Core/Client/Server/Panel, command-module
tests, frontend Node checks, binary format checks, and SHA256 verification.
The pinned sing-box package test was already passed in the qualified build
run; a later clean-cache rebuild attempt was not used as a substitute because
the external dependency cache returned invalid partial ZIP files. The
official sing-box binaries are the previously successful pinned-build outputs;
the only source change afterward was the server test observation-window
comment described above, which cannot affect those binaries.

## Secret and provenance audit

```text
SOURCE_SECRET_SCAN: PASS
STAGED_SECRET_SCAN: PASS
RELEASE_ASSET_SECRET_SCAN: PASS
SECRET_VALUE_MATCH_COUNT: 0
PRODUCTION_CREDENTIAL_MATCH_COUNT: 0
RAW_SESSION_ID_EXPOSED: NO
DESTINATION_EXPOSED_BY_DIAGNOSTICS: NO
PAYLOAD_EXPOSED: NO
```

Runtime directories, logs, rollback copies, credential configs, work trees,
cache files, and private telemetry dumps are excluded from the official asset
set and are not staged.

## Read-only production snapshot

No production process was restarted or replaced. Fresh reads observed:

```text
R14 client: PID 34320, 127.0.0.1:18080 LISTENING
Carrier-A: PID 20608, 127.0.0.1:17898 LISTENING
Carrier-B: PID 29404, 127.0.0.1:17899 LISTENING
Native Mihomo: PID 33676, current binary hash recorded above
R15 Panel: PID 20216, 127.0.0.1:24600 LISTENING
Telemetry forward: 127.0.0.1:24500 LISTENING
Remote smp3-standalone: active, MainPID 1686275, NRestarts 0
Remote 10.66.66.1:24444: LISTENING
Remote 10.66.66.1:24445: LISTENING
Remote 127.0.0.1:24500: LISTENING
Remote telemetry /api/v1/status: HTTP 200
```

The release operation performed no synthetic C5 request, no performance test,
and no business traffic generation.

```text
SYNTHETIC_C5_REQUEST_COUNT: 0
PERFORMANCE_TEST_RUN: NO
R14_PRODUCTION_RUNTIME_CHANGED: NO
CARRIER_A_CHANGED: NO
CARRIER_B_CHANGED: NO
NATIVE_PRODUCTION_RUNTIME_CHANGED: NO
Q53_SERVER_CHANGED: NO
WIRE_CHANGED: NO
FIREWALL_NAT_ROUTING_CHANGED: NO
PANEL_PRODUCTION_RUNTIME_CHANGED: NO
```

## Release publication record

```text
RELEASE_COMMIT: recorded by the v2.3.0 tag and final stdout
RELEASE_TAG: v2.3.0
GITHUB_RELEASE_ASSET_COUNT: 11 (10 binaries + SHA256SUMS)
```

The release notes explicitly cover Standalone, Native, and Panel/
Observability. No commit, push, tag, or GitHub Release action is represented
as a production runtime mutation.
