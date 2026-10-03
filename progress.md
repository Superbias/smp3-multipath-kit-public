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
