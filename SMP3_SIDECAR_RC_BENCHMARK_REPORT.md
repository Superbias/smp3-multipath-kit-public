# SMP3 Standalone Sidecar Phase 2A RC Benchmark Report

## STATUS

```text
SMP3 SIDECAR PHASE 2A RC QUALIFICATION: FAIL
STOP: RC_NATIVE_256M_UPLOAD_TIMEOUT
```

This qualification ran on the dedicated `qualification/standalone-sidecar-rc`
branch. No production process, Clash Party configuration, remote server,
firewall, Core, Wire, or adapter source was changed.

## ENVIRONMENT / BUILD IDs

```text
Native fixture:  Mihomo Meta 1.10.0 windows amd64 with go1.25.5
Native SHA256:   5f48edd5f52bdf80d5493a876971eddcf441c00f24fb77118140ad36c364b8db
Stock fixture:   Mihomo Meta v1.19.29 windows amd64 with go1.26.5
Stock SHA256:    82cd796a23492f43a71c1ec27e4e5e0b3d58932014da5a36e79ed9b11fee8162
```

The stock fixture is the previously verified pre-SMP3 local backup and rejects
`type: smp3`. Both binaries were copied to disposable temporary directories.

## TOPOLOGY

Native tests used the custom SMP3-enabled Mihomo through two observable local
SOCKS carrier relays into the standalone server's legacy listener. Sidecar
tests use stock Mihomo -> local `smp3-client` -> stock Mihomo discriminator
rules -> observable carrier relays -> standalone sidecar listeners.

Both paths used the same localhost target and canonical SMP3 server. The RC
sidecar configuration used `SMP3RDY1`, `connect_timeout=2s`,
`carrier_ready_timeout=2s`, and the same low activation threshold for bounded
diagnostic runs.

## RTT

The quick bounded smoke completed 1000 sequential exchanges for both modes at
64-byte and 1 KiB payload sizes. The formal RC run completed the same RTT phase.
Representative formal Native results were:

```text
Native 64B:  median 0.4311 ms, p95 1.1816 ms, p99 4.1272 ms
Native 1KiB: median 0.4364 ms, p95 0.9011 ms, p99 1.5460 ms
```

The formal Sidecar RTT phase was not reached after the Native throughput gate
failed; quick smoke evidence showed Sidecar RTT operating but is not used as
the formal performance comparison.

## DOWNLOAD / UPLOAD

The formal Native phase completed:

| Direction | Size | Runs | Median Mbps | Result |
|---|---:|---:|---:|---|
| download | 64 MiB | 3 | 223.165 | PASS |
| download | 256 MiB | 3 | 232.528 | PASS |
| upload | 64 MiB | 3 | 203.692 | PASS |
| upload | 256 MiB | 1 attempted | — | TIMEOUT |

The first formal Native 256 MiB upload did not complete within the harness's
60-second transfer timeout. It timed out while the Native custom Mihomo path
was sending/receiving through the local relay and standalone server. A bounded
8 MiB Native upload and the earlier 1 MiB upload both passed, so this report
does not claim a universal Native failure or blame Sidecar without a separate
root-cause investigation.

Because the Native 256 MiB upload gate failed, formal Sidecar 64/256/512 MiB
three-run throughput comparison, CPU comparison, and final performance
classification were not run.

## BIDIRECTIONAL / ACTIVATION

The bounded diagnostic run passed simultaneous upload/download and observed
leg1 activation for both Native and Sidecar. Stock Sidecar RX/TX activation
and same-session leg1 joins also passed in the Phase 1 qualification and in
the quick RC harness. The formal RC comparison is incomplete because of the
Native 256 MiB upload stop.

## CPU / MEMORY

The harness collected Windows Working Set and process CPU samples during the
completed quick bounded runs, but no formal RC CPU/memory comparison is
reported because the required three-size throughput matrix did not complete.
The Native process remained alive until the harness aborted the failing
transfer; disposable processes were cleaned up afterward.

## TCP CHURN

The quick diagnostic run completed a bounded single-connection smoke. The
formal required 1000 short TCP association churn for both modes was not
started after the Native large-upload blocker.

## UDP STABILITY

The quick diagnostic path completed:

```text
Native UDP smoke:       64/64, LOST=0, BAD=0
Sidecar UDP smoke:      64/64, LOST=0, BAD=0
Sidecar UDP 10,000:     10,000/10,000, LOST=0, BAD=0
Persistent association: 16/16 over 301.158s, LOST=0
```

The persistent run used a 5-second disposable UDP idle timeout to force
DatagramEngine recreation while retaining the same local SOCKS association.
The formal RC matrix remains incomplete because the Native large-upload gate
stopped the run before all required RC sections could be classified.

## HOST RESTART / CARRIER FAILURE / FALSE SUCCESS

Quick bounded localhost diagnostics passed:

```text
stock Mihomo restart with Sidecar alive: PASS
stock false-success -> READY timeout -> fallback C: PASS
same-session fallback join: PASS
128 MiB Sidecar leg1 failure -> fallback C: exact transfer PASS
```

These results are evidence that the Phase 1 readiness and recovery paths still
operate in the RC branch. They do not override the failed formal performance
gate.

## READY ESTABLISHMENT COST

The quick harness measured establishment through the local proxy path, not an
isolated protocol microbenchmark. Sidecar READY is emitted once per leg
establishment/rejoin, after canonical HELLO admission, and never per DATA
frame. No final overhead classification is made because the formal comparison
was stopped.

## NATIVE COMPATIBILITY

The custom Native Mihomo disposable copy passed bounded TCP, UDP smoke,
activation, and 64 MiB upload/download paths against the legacy listener. The
legacy listener remained READY-free. The 256 MiB Native upload timeout is the
open compatibility/performance blocker for this RC.

## RC ARTIFACTS

The source-side RC artifact matrix was not packaged as a release candidate
after the mandatory benchmark failed. No tag, release, push, or production
deployment was performed.

## FROZEN INTEGRITY

```text
CORE_CHANGED: NO
WIRE_CHANGED: NO
PRODUCTION_TOUCHED: NO
```

The stop occurred in the benchmark harness at the Native custom Mihomo
large-upload layer. No runtime patch was attempted.

## INTERPRETATION / NEXT

The available data shows a measurable, bounded Native-vs-Sidecar local path,
but it is not a complete RC benchmark. The exact blocker is:

```text
Native custom Mihomo 1.10.0
-> local observable carrier relays
-> standalone legacy listener
-> 256 MiB upload
-> no completion within 60s
```

Do not mark `SIDECAR_PERFORMANCE_ACCEPTABLE`, `NATIVE_MODE`, or the Phase 2A
RC gate as PASS. Next action is a separate Native 256 MiB upload root-cause
isolation (fixture vs custom Mihomo integration vs standalone path); do not
change runtime in this qualification branch without a new repair instruction.
