# SMP3 Production Promotion Report

## Classification

`A. PRODUCTION_AGGREGATION_MERGE_CLEAN` for an explicit opt-in
`aggregation` scheduler mode. Production defaults remain frozen until a
separate release decision.

## Proposed production stack

```text
assigned-service planner
  -> bounded per-leg pending queues (depth 32)
  -> independent per-leg feeders
  -> existing writer/carrier
  -> existing ACK/retry path
  -> frontier rescue with exposure-age lower bound
```

## Files changed

- `core/stream_engine.go`: production aggregation mode, decoupled admission,
  feeder wake path, frontier exposure hook, and epoch-rebase fields.
- `core/stream_assignment_debug.go`: pending ownership rollback/reassignment
  and normalized-service epoch comparison used by aggregation mode.
- `core/stream_frontier_debug.go`: frontier exposure clock and grace candidate;
  its bounded diagnostic samples are only emitted by benchmark tests.
- `core/stream_aggregation_benchmark_test.go` and
  `core/stream_promotion_benchmark_test.go`: deterministic gates and reports.
- `client/config.go`, `server/config.go`, `server/stream.go`: explicit
  `scheduler_mode: aggregation` configuration mapping and validation.
- `docs/AGGREGATION_SCHEDULER.md`: mode-selection and memory-bound guidance.

## Defaults and runtime impact

- No production default is switched in this phase.
- The epoch rebase and exposure grace are enabled by production
  `scheduler_mode: aggregation`; no benchmark environment variable is needed.
- The candidate adds two bounded pending queues and two feeder goroutines per
  stream when enabled; queue memory is bounded by `2 * 32 * ChunkSize`.
- Feeder shutdown is tied to the engine `done` channel and is awaited by the
  promotion harness. Pending channels are engine-owned and not individually
  closed, so no sender races a channel close. Normal `Close` and graceful drain
  preserve existing shutdown behavior.

## Failure and reconnect behavior

- Before physical admission, failed pending ownership rolls back and is
  reassigned to a live leg.
- After admission, sequence identity, ACK state, retry state, and ledger
  ownership remain unchanged; existing invalidation/retry/rescue handles the
  record.
- Active-set changes rebase only normalized scheduling comparison origins.
  Historical assigned bytes and telemetry are not rewritten.

## Rescue timing and compatibility

- Exposure grace adds `frontierBecameCurrentAt` as a lower timing bound while
  preserving `createdAt`, `lastSentAt`, `lastRescueAt`, `transitSince`, and the
  one-second timeout.
- No wire-format change. Mihomo/external proxy integration, SOCKS5 local
  interface, and standalone sidecar architecture are unchanged.

## Evidence

- Reconnect prefixes returned to approximately 20/80 in both failure
  directions; max reconnect streak was 5 records.
- Historical benchmark-only pre-production run: 214.37 Mbps, 1.000214x,
  ledger 0. The current production-mode 1 GiB ×3 results are 248.68, 249.58,
  and 249.60 Mbps; median 249.58 Mbps, CV about 0.21%, retry 0, rescue 0,
  ledger 0, payload PASS.
- 50 + 100 reconfirmation: 143.07 / 143.19 / 146.48 Mbps, median 143.19 Mbps,
  CV about 1.3%, physical/raw 95.38% / 95.46% / 97.65%, assignment 33.40/66.60,
  retry 0, rescue 0, ledger 0. The earlier 125.25 Mbps measurement is a
  non-repeatable outlier.
- Continuous forward-progress smoke gate passed; deterministic failure, drop,
  stall, low-leg failure, and native Linux race gates passed.

## v2.5.0 release-preparation status
The aggregation runtime is an explicit opt-in feature. Adaptive remains default, static is unchanged, and wire/SOCKS5/Mihomo/external proxy contracts remain unchanged. Real-world validation is complete on the final RC; release preparation does not alter runtime behavior.

