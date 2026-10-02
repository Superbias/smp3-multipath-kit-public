# SMP3 Production Aggregation Validation Report

## Final classification

`A. PRODUCTION_AGGREGATION_MERGE_CLEAN`

The new path is explicit opt-in through `smp3.stream.scheduler_mode:
aggregation`. Existing `adaptive` remains the default; existing `static`
behavior is unchanged.

## Production extraction

`StreamSchedulerAggregation` enables the validated assigned-service planner,
32-record per-leg pending queues, independent feeders, pending rollback and
reassignment, normalized-service epoch rebasing, and frontier exposure grace.
The implementation does not add a wire format or a second scheduler family.

Source-of-truth functions are:

- `core/stream_engine.go: NewStreamEngine`: aggregation mode selects bounded
  pending depth 32, enables epoch rebasing, and constructs frontier grace.
- `core/stream_engine.go: chooseOrdinaryLeg` and
  `core/stream_assignment_debug.go: assignBenchmark`: normalized assigned-byte
  entitlement and active-set epoch comparison.
- `core/stream_assignment_debug.go: pendingFeeder` and
  `enqueueAssignedDecoupled`: independent feeders and physical admission.
- `core/stream_engine.go: handleLegFailure` plus
  `reassignFailedPending`: pending rollback/reassignment versus admitted retry.
- `core/stream_frontier_debug.go: observe/apply`: frontier exposure timestamp
  and one-second grace without resetting an unchanged frontier.

Selecting `aggregation` activates these paths directly. No benchmark
environment variable is read by the production configuration path.

## Matrix

| Case | Useful throughput | Share | Amp | Retry | Rescue | Ledger |
|---|---:|---:|---:|---:|---:|---:|
| 100 + 100 | 196.65 Mbps | 50.00/50.00 | 1.000x | 0 | 0 | 0 |
| 50 + 100 | 125.25 Mbps | 33.40/66.60 | 1.000x | 0 | 0 | 0 |
| 50 + 150 | 189.81 Mbps | 25.00/75.00 | 1.000x | 0 | 0 | 0 |
| 50 + 200 | 244.40 Mbps | 19.97/80.03 | 1.000x | 0 | 0 | 0 |
| 50/20 + 200/100 | 228.97 Mbps | 19.92/80.08 | 1.000x | 0 | 0 | 0 |

The 50 + 100 case was repeated with fresh engines: 143.07, 143.19, and
146.48 Mbps; median 143.19 Mbps, CV about 1.3%, physical/raw 95.38%, 95.46%,
and 97.65%. Assignment stayed 33.40/66.60 with retry 0, rescue 0, ledger 0,
and payload PASS. The earlier 125.25 Mbps point is a non-repeatable outlier,
not a scheduler regression.

## Long flow

Production-mode 1 GiB ×3 results were 248.68, 249.58, and 249.60 Mbps;
median 249.58 Mbps and CV approximately 0.21%. Each run had 20/80 assignment,
amplification 1.000x, retry 0, rescue 0, ledger 0, and payload PASS.

## Failure matrix

- High-leg hard failure after 64 KiB: 49.23 Mbps, 1.004x, 65,536 B retry,
  rescue 0, ledger 0.
- Low-leg hard failure after 64 KiB: 231.42 Mbps, 1.000x, retry 0, rescue 0,
  ledger 0.
- Deterministic drop: payload PASS, bounded recovery, ledger retirement.
- Two-second stall: payload PASS, one bounded rescue after the normal timeout,
  ledger retirement.
- Reconnect: normalized prefixes return to approximately 20/80 by 256 records;
  maximum consecutive selection is 5.

## Compatibility and lifecycle

Wire framing, sequence identity, ACK format, retry semantics after physical
admission, SOCKS5, Mihomo/external proxy integration, and standalone sidecar
architecture are unchanged. Pending memory is bounded to approximately 2 MiB
for two legs at 32 KiB chunks. Feeder goroutines exit through the engine done
path; pending channels are owned by the engine and are intentionally not closed
individually, avoiding sends racing a close. Existing close and graceful-drain
behavior remains in use.

## Verification

- `go test ./...` passed for core, client, server, and cmd/smp3-server.
- Native Linux `CGO_ENABLED=1 /usr/local/go/bin/go test -race ./...` passed.
- No default scheduler or release artifact was changed.
