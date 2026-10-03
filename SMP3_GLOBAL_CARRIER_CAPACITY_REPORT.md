# SMP3 Global Carrier Capacity Report

## Classification
A. GLOBAL_CARRIER_CAPACITY_PRODUCTION_CANDIDATE_VALIDATED

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
| Client integration tests | PASS (`go test ./client`) |
| Server integration tests | PASS (`go test ./server`) |
| Registry sharing and identity isolation | PASS (`TestCarrierCapacityRegistrySharesCarrierAndIsolatesKeys`) |
| Warm reuse and idle pruning | PASS (`TestCarrierCapacityRegistryWarmReuseAndBoundedPrune`) |
| 1/2/4/8 logical-stream matrix | PASS (`TestCarrierCapacityRegistryConcurrentStreamCounts`) |
| Core/client/server race under WSL Debian Go 1.24.4 + gcc | PASS (`go test -race ./core ./client ./server ./cmd/smp3-client ./cmd/smp3-server`) |
| Windows race | unavailable: local toolchain has no gcc; WSL race passed |
| Native Mihomo adapter application | PASS (adapter patch applied to pinned checkout) |
| Native Mihomo full dependency test | NOT COMPLETED: upstream dependency download/test exceeded the bounded run window; no source failure was observed |
| 1 GiB long-run, CPU/RSS, production traffic | NOT RUN in this local code-only gate |
| `git diff --check` | PASS |

## Compatibility and safety

- Fixed/static/adaptive scheduler modes are unchanged.
- `capacity_mode: dynamic` remains opt-in and still requires aggregation mode.
- Empty or absent provider falls back to the established per-stream implementation.
- No release artifact, VERSION, runtime, production server, or existing release was modified.

## Required final state

```yaml
push: NO
merge: NO
tag: NO
release: NO
deployment: NO
```
