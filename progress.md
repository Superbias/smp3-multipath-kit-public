# Progress

## 2026-10-03
- 初始化 Global Carrier Capacity 目标规划文件。
- 下一步：审计 core/client/server/Mihomo topology。

## 2026-10-03 implementation
- Added shared provider interface and bounded carrier registry in `core/carrier_capacity.go`.
- Wired StreamEngine admission/ACK/weight/telemetry/close paths to optional provider.
- Wired standalone client to stable per-leg carrier keys and one process registry.
- Added registry sharing, owner isolation, warm reuse/prune, and 1/2/4/8 stream tests.
- Core and client tests pass.

## Validation closure
- WSL Debian race passed for core/client/server/cmd modules.
- Native adapter patch application succeeded; full Mihomo dependency test was bounded and timed out while downloading upstream modules.
- Added `SMP3_GLOBAL_CARRIER_CAPACITY_REPORT.md` and updated aggregation scheduler documentation.

## Final promotion closure audit
- Clean `git archive HEAD` source built all four standalone binaries from tracked files.
- Clean source core/client/server/cmd tests passed; one server telemetry test was flaky once and passed on isolated rerun.
- Added quantitative shared-carrier logs: 1/2/4/8 streams each converged at 91.0 Mbps versus 100 Mbps configured (same estimate across stream counts); 50/200 converged at 48.1/192.3 Mbps with 20/80 share.
- Added 1200 lifecycle churn gate; registry held at 1024 entries.
- Promotion remains in progress pending native Mihomo dependency completion, sustained 1+ GiB, CPU/RSS/contention, and process-level traffic smoke.

## Dynamic activation implementation
- Added capacity-relative activation and shared logical demand accounting in commit-in-progress.
- Core scenario unit tests pass for low demand, near capacity, just-over-capacity, strong overload, high-capacity primary, capacity drop, step-up, short burst suppression, and shared demand aggregation.
- Native adapter source now maps `activation-mode: dynamic` to canonical Core semantics through the pinned Mihomo checkout.

## Dynamic leg activation final closure
- Added explicit legacy/dynamic activation modes with capacity-relative overload qualification, shared logical demand, confidence, sustained evidence, hysteresis, and queue/loss fallback.
- Completed dynamic global 1GiB closure: 212.903 Mbps useful throughput, amplification 1.000000, retry/rescue/ledger 0, payload PASS.
- Added final report: `SMP3_DYNAMIC_LEG_ACTIVATION_REPORT.md`.
- Remaining gate: commit locally and run tracked-source archive validation; no remote or release action.

## Memory Attribution and Stability Closure
- Added opt-in diagnostic `core/memory_stability_test.go`; production code is unchanged.
- Completed five same-process 1GiB rounds, forced-GC snapshots, heap profiles, standalone 5GiB/20GiB/30GiB traffic, 1000-stream churn, registry lifecycle, goroutine/FD, GC and throughput attribution.
- Created `SMP3_MEMORY_STABILITY_REPORT.md`.
- Remaining gates: run final source/race/archive verification, commit documentation and diagnostic test locally only.

## Final memory closure gate
- Final commit `db13e7b45d9be84f54c646983414e159af4d6449` contains the diagnostic test and stability report.
- Race and tracked-only archive test/build gates passed from final source state.
- Classification: `A. MEMORY_BEHAVIOR_BOUNDED_AND_EXPLAINED`.

## Completion-time / HOL-aware Assignment
- Added Core `StreamHOLMode` and bounded completion estimator using existing pending depth, sent-minus-useful-ACK bytes, fixed/dynamic service weight, and smoothed write latency.
- Added atomic sender ACK-frontier sequence and sequence-stride sampling to avoid a ledger lock and keep the opt-in correction cheap.
- Added client/server JSON `hol_mode` and Native Mihomo `hol-mode` mapping; completion requires aggregation, while omitted/legacy/disabled preserve behavior.
- Virtual A/B/C matrix passed: 100/100 remained 50/50, 50/200 remained 19.9/80.1, 300ms stall produced no rescue; all amplification was 1.000.
- Completion mode passed dynamic activation, reconnect, shared-carrier 1/4/8-stream and 1GiB gates, plus full Core/client/server/cmd tests and race.
- Native Mihomo targeted tests, adapter race, and `go build ./...` passed.
- Local commit `51a0541bcf4860cd08f60200c9c0397729303ec4` and tracked-only archive tests/four amd64 builds passed; final tree is clean.
