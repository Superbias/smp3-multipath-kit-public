# SMP3 Standalone Sidecar Phase 2A RC Final Report

## STATUS

```text
SMP3 SIDECAR PHASE 2A RC QUALIFICATION: PASS
BRANCH: qualification/standalone-sidecar-rc

NATIVE_MODE: PASS
SIDECAR_MODE: PASS
NATIVE_UPLOAD: PASS
NATIVE_DOWNLOAD: PASS
SIDECAR_UPLOAD: PASS
SIDECAR_DOWNLOAD: PASS
BIDIRECTIONAL: PASS

RTT_COMPARISON: COMPLETE
CPU_COMPARISON: COMPLETE
MEMORY_COMPARISON: COMPLETE
TCP_CHURN_NATIVE: PASS
TCP_CHURN_SIDECAR: PASS
UDP_NATIVE_10K: PASS
UDP_SIDECAR_10K: PASS
PERSISTENT_UDP_SIDECAR: PASS
HOST_RESTART_RECOVERY: PASS
CARRIER_FAILURE_RECOVERY: PASS
FALSE_SUCCESS_REGRESSION: PASS
NATIVE_COMPATIBILITY: PASS
LEGACY_READY_FREE: PASS

SIDECAR_PERFORMANCE_ACCEPTABLE: YES
WINDOWS_CLIENT_RC: BUILT
LINUX_CLIENT_RC: BUILT
SHA256SUMS_RC: GENERATED

CORE_CHANGED: NO
WIRE_CHANGED: NO
RUNTIME_CHANGED: NO
PRODUCTION_TOUCHED: NO
READY_FOR_INTEGRATION_REVIEW: YES
```

Sidecar is accepted as a compatibility-first host-agnostic deployment. This
is not a claim of Native performance parity; raw throughput and resource data
are included below.

## BASELINE / FIXTURES

```text
R2_BASE_HEAD: 4320a6b
Native: Mihomo Meta 1.10.0 windows amd64
Native SHA256: 5f48edd5f52bdf80d5493a876971eddcf441c00f24fb77118140ad36c364b8db
Stock: Mihomo Meta v1.19.29 windows amd64
Stock SHA256: 82cd796a23492f43a71c1ec27e4e5e0b3d58932014da5a36e79ed9b11fee8162
```

Both binaries were copied to disposable temporary directories. All traffic
used loopback relays, a disposable standalone server, and local targets. The
formal process never used the Clash Party production process.

R1 evidence remains in `SMP3_NATIVE_256M_UPLOAD_ISOLATION_REPORT.md`; the
original RC failure report remains unchanged.

## BENCHMARK METHODOLOGY

One-way upload uses an exact-N sink: the sink counts exactly N bytes and then
returns `OK\n`. One-way download uses a fixed-size source and exact client byte
counting. Full-duplex echo is used only for the separate bidirectional gate.
Upload and download medians therefore do not contain the prior echo benchmark
interaction.

Formal parameters:

```text
sizes: 64 MiB, 256 MiB, 512 MiB
runs per size/direction/mode: 3
stream scheduler: adaptive
activation threshold: 1 Mbps diagnostic threshold
activation window: 20ms
chunk size: 1024
queue frames: 256
bandwidth weights: [128, 500] Mbps
```

Each mode also ran one warmup TCP exchange, one small upload, and one small
download before formal counters.

## RTT

| Mode | Payload | Median ms | P95 ms | P99 ms |
|---|---:|---:|---:|---:|
| Native | 64 B | 0.4111 | 0.6130 | 0.7937 |
| Sidecar | 64 B | 0.3688 | 0.5901 | 0.7469 |
| Native | 1 KiB | 0.4097 | 0.6044 | 0.7497 |
| Sidecar | 1 KiB | 0.3392 | 0.5276 | 0.6373 |

## UPLOAD

| Size | Native median Mbps | Sidecar median Mbps | Delta |
|---:|---:|---:|---:|
| 64 MiB | 104.34 | 229.73 | +120.18% |
| 256 MiB | 103.31 | 132.26 | +28.02% |
| 512 MiB | 205.67 | 125.03 | -39.21% |

All 18 upload runs were exact-N complete with completion response and
`leg1_same_session=true`.

## DOWNLOAD

| Size | Native median Mbps | Sidecar median Mbps | Delta |
|---:|---:|---:|---:|
| 64 MiB | 230.61 | 141.85 | -38.49% |
| 256 MiB | 241.32 | 139.85 | -42.05% |
| 512 MiB | 250.15 | 143.99 | -42.44% |

All 18 download runs received the exact fixed source size without truncation.

```text
median overall upload delta: +28.02%
median overall download delta: -42.05%
```

The Sidecar path has a measurable extra-process/socket cost, especially for
sustained download. It remained stable and exact at all formal sizes, so the
result is acceptable for compatibility deployment but should not be treated
as a Native performance benchmark.

## BIDIRECTIONAL

256 MiB each direction, three runs:

| Mode | Run 1 combined Mbps | Run 2 combined Mbps | Run 3 combined Mbps | Result |
|---|---:|---:|---:|---|
| Native | 424.99 | 436.04 | 433.13 | PASS |
| Sidecar | 209.50 | 207.97 | 209.33 | PASS |

Each run delivered exact 268435456 bytes in both directions.

## CPU / MEMORY

The sampler ran during the mode's warmup, RTT, formal throughput, bidirectional,
churn, and UDP phases. Sidecar CPU is the sum of stock Mihomo and
`smp3-client`; Sidecar memory is the sum of both processes.

| Mode | Process scope | CPU seconds delta | Peak working set |
|---|---|---:|---:|
| Native | custom Mihomo | 259.72 s | 511,442,944 B |
| Sidecar | stock Mihomo + smp3-client | 311.02 s | 194,510,848 B |

The samples are whole-run steady-load observations, not idle-vs-loaded
comparisons.

## ACTIVATION

Every formal Native and Sidecar one-way run recorded `leg1_same_session=true`.
Relay byte deltas confirmed both A and B carried traffic, with C unused during
healthy runs. The final exact-N data records application bytes and target bytes;
representative successful 256 MiB upload records are:

```text
Native: 268435456 application bytes -> 268435456 target bytes
Sidecar: 268435456 application bytes -> 268435456 target bytes
completion: OK\n
```

## TCP CHURN

```text
Native: 1000 short connections, FAIL=0, PASS
Sidecar: 1000 short connections, FAIL=0, PASS
```

Both processes remained usable after churn.

## UDP STABILITY

```text
Native:   SENT=10000 RECEIVED=10000 LOST=0 BAD=0 DUPLICATE=0 REORDERED=0
Sidecar:  SENT=10000 RECEIVED=10000 LOST=0 BAD=0 DUPLICATE=0 REORDERED=0
```

## PERSISTENT UDP

Inherited valid pre-stop evidence, as allowed by R2 because runtime semantics
and the persistent-association test path were unchanged:

```text
SENT=16 RECEIVED=16 LOST=0
source: SMP3_SIDECAR_RC_BENCHMARK_REPORT.md
```

This is explicitly inherited, not falsely presented as a new 301-second run.

## HOST RESTART

```text
disposable stock Mihomo stopped: PASS
smp3-client remained alive: PASS
stock Mihomo restarted: PASS
traffic recovered: PASS
recovery transfer time: 0.069379 s
```

## CARRIER FAILURE

```text
healthy A/B/C established: PASS
leg1 B failed during 128 MiB fixed download: PASS
fallback C established: PASS
same logical session: PASS
exact bytes: 134217728
```

## FALSE-SUCCESS

```text
primary B accepted local SOCKS connection without remote readiness: PASS
READY timeout: 2 s
fallback C: PASS
same SessionID/LegID=1 join: PASS
```

## READY COST

Twenty healthy Sidecar leg0 samples used relay-observable milestones and kept
app SOCKS timing separate:

```text
SOCKS reply -> first SMP3 HELLO -> first authenticated SMP3RDY1
HELLO -> READY median: 0.1453 ms
HELLO -> READY p95: 0.3121 ms
app SOCKS CONNECT -> usable stream median: 0.5544 ms
app SOCKS CONNECT -> usable stream p95: 22.0983 ms
```

The 1-byte readiness probe intentionally does not cross the stream activation
threshold, so leg1 was not expected in this microprobe. Formal large-flow
records independently prove leg1 activation and same-session joining. READY is
emitted once per leg establishment/rejoin, not per DATA frame.

## NATIVE COMPATIBILITY

```text
Native TCP smoke: PASS
Native exact-N upload/download: PASS
Native UDP 10000: PASS
legacy listener READY-free: PASS
```

No `SMP3RDY1` was emitted by the legacy Native listener path.

## RC ARTIFACTS

Staging directory:

```text
D:\smp3-singbox\.work\phase2a-r2-staging
```

| Filename | Size | SHA256 | Version |
|---|---:|---|---|
| `smp3-client-windows-amd64.exe` | 4,314,112 | `35062becba71c4554f401216c4c4444d4360d74ae8eca257bbf287b9e79c0d02` | `2.1.1-sidecar-dev` |
| `smp3-server-windows-amd64.exe` | 4,322,304 | `5f86c1330ef050683a48d6117dd2327335558be4f422368398d73289b3d0743c` | `2.0.0` |
| `smp3-client-linux-amd64` | 4,206,288 | `0853c2d0c6eb39f9e1fa1c23248ee26136b1507112644443d414411e5d373e33` | `2.1.1-sidecar-dev` |
| `smp3-server-linux-amd64` | 4,200,026 | `b85fc251d6f15e6c9cf9d2cca83d287f215a2b976ed6cfb16c302f4aff7db85a` | `2.0.0` |

```text
SHA256SUMS-RC SHA256:
e497313bcfb0e7c2d86572173f2a8e5967602127d276a6f7ed44a4d2393112d1
```

The Windows Sidecar staging bundle contains:

```text
smp3-client.exe
smp3-client-config.example.json
mihomo-sidecar.example.yaml
README.zh-CN.md
SHA256SUMS
```

Its bundle checksum file was generated and contains no real credentials or
production endpoints.

## FROZEN INTEGRITY

```text
test count: smp3core=47, legacy_semantic=106, sing_adapter=17, total=170
Core canonical HEAD-blob check: 18/18 PASS
Wire golden HEAD-blob check: 22/22 PASS
validate-kit on LF-normalized disposable source tree: PASS
Core/client/server normal tests: PASS
Core/client/server race tests: PASS
Core/client/server vet: PASS
git diff --check: PASS
```

The current Windows checkout causes the legacy hash checker to read CRLF
working-tree bytes; the same committed HEAD source in an LF-normalized
disposable tree passes migration parity and validate-kit. No Core/Wire file was
modified in R2.

## INTERPRETATION

```text
Native Mode = shortest path and preferred highest-performance integration
Sidecar Mode = compatibility-first host-agnostic deployment
```

Qualified in this RC:

```text
Native custom Mihomo: qualified
Stock Mihomo Sidecar: qualified
```

Architecture-compatible but not formally qualified in this RC:

```text
sing-box host
Xray/V2Ray host
```

## FINAL GATE

All requested formal data gates completed and passed. The only qualification
is that Sidecar throughput is not performance-equivalent to Native; the raw
delta is retained and `SIDECAR_PERFORMANCE_ACCEPTABLE` means stable,
non-pathological compatibility operation in this localhost environment.

```text
SMP3 SIDECAR PHASE 2A RC QUALIFICATION: PASS
READY_FOR_INTEGRATION_REVIEW: YES
```

Stop here. Do not push, merge, tag, release, deploy production, or begin a
new qualification phase without authorization.
