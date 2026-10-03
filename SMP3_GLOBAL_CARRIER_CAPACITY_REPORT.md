# SMP3 Global Carrier Capacity Report

## Classification
GLOBAL_CARRIER_CAPACITY_PROMOTION_IN_PROGRESS

## Scope and implementation

- Added `core.StreamCapacityProvider` and `core.CarrierCapacityRegistry`.
- Dynamic aggregation keeps the existing per-stream estimator when no provider is configured.
- Standalone client and native Mihomo adapter now own one registry per process and derive stable keys from owner, outbound direction, route/carrier identity.
- Shared observations use logical cumulative ACK retirement. Retransmit, duplicate, and frontier rescue sends never call `Ack`.
- Registry state is reference counted, warm-reusable, pruned after 10 minutes idle, and capped at 1024 entries.
- Server sessions remain owner isolated because each incoming session has distinct carrier sockets; no cross-client merge is inferred.

## Validation evidence

| Gate | Result |
|---|---|
| Existing single-stream dynamic tests | PASS (`go test ./core`) |
| Tracked-only standalone builds | PASS: clean `git archive HEAD` built Windows/Linux amd64 client and server binaries |
| Tracked-only module tests | PASS: clean archive core/client/server/cmd tests; one server telemetry timing test passed on isolated rerun after a single suite timeout |
| Client integration tests | PASS (`go test ./client`) |
| Server integration tests | PASS (`go test ./server`) |
| Registry sharing and identity isolation | PASS (`TestCarrierCapacityRegistrySharesCarrierAndIsolatesKeys`) |
| Warm reuse and idle pruning | PASS (`TestCarrierCapacityRegistryWarmReuseAndBoundedPrune`) |
| 1/2/4/8 logical-stream matrix | PASS (`TestCarrierCapacityRegistryConcurrentStreamCounts`) |
| 1/2/4/8 quantitative global estimate | PASS: 91.0 Mbps for all four stream counts against a 100 Mbps configured carrier (−9.0%, invariant across stream count) |
| 50/200 asymmetric matrix | PASS: 48.1/192.3 Mbps estimates, 20/80 assignment share |
| 1200 lifecycle churn | PASS: registry bounded at 1024 entries |
| Core/client/server race under WSL Debian Go 1.24.4 + gcc | PASS (`go test -race ./core ./client ./server ./cmd/smp3-client ./cmd/smp3-server`) |
| Windows race | unavailable: local toolchain has no gcc; WSL race passed |
| Native Mihomo adapter application | PASS (adapter patch applied to pinned checkout) |
| Native Mihomo full dependency test | EXTERNAL BLOCKER: two bounded retries (including `GOPROXY=https://proxy.golang.org,direct`) remained in upstream module download until timeout; adapter patch application succeeded and no source compile error was observed |
| 1 GiB long-run, CPU/RSS, production traffic | INCOMPLETE: requires a longer controlled runtime/traffic harness |
| `git diff --check` | PASS |

## Compatibility and safety

- Fixed/static/adaptive scheduler modes are unchanged.
- `capacity_mode: dynamic` remains opt-in and still requires aggregation mode.
- Empty or absent provider falls back to the established per-stream implementation.
- No release artifact, VERSION, runtime, production server, or existing release was modified.

## Promotion status

The implementation gates and deterministic local matrix are complete, but the final production classification is intentionally not asserted yet. The remaining promotion evidence is the native Mihomo dependency build/race, sustained 1+ GiB traffic, process CPU/RSS/lock contention measurements, and a real standalone SOCKS smoke run.

## Required final state

```yaml
push: NO
merge: NO
tag: NO
release: NO
deployment: NO
```
