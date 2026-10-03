# SMP3 HOL-aware assignment report

## Candidate and scope

This change adds an opt-in completion-time correction below normalized assigned-service and above the existing pending feeders. The default, `legacy`, and `disabled` modes preserve the v2.5 scheduler. No retry, rescue, reinjection, wire framing, or capacity estimator policy was changed.

## Configuration

Standalone client and server JSON:

```json
{
  "stream": {
    "scheduler_mode": "aggregation",
    "capacity_mode": "dynamic",
    "hol_mode": "completion"
  }
}
```

Native Mihomo uses `scheduler-mode: aggregation`, `capacity-mode: dynamic`, and `hol-mode: completion`. Completion mode is rejected unless aggregation is selected. Omitted, `legacy`, and `disabled` map to the existing behavior.

## Completion model

For a candidate record and leg, the estimate is:

```text
(pending_depth * chunk_size + (tx_sent_bytes - tx_acked_useful) + payload_bytes)
    / (effective_scheduler_weight * 1e6 / 8)
+ smoothed_write_latency
```

The weight is the existing fixed or dynamic capacity weight. Pending depth is the existing per-leg feeder depth. In-flight bytes are the existing sent-minus-useful-ACK counters. There is no reliable wire RTT signal in the current protocol, so the existing smoothed write latency is used as a bounded transport-latency proxy. No timer, probe, allocation, or wire field was added.

The correction samples every eighth sequence, checks that the record is within eight sequence numbers of the atomic cumulative-ACK frontier, and only changes the normalized baseline when the alternate estimate leads by at least 2 ms and 10%. A maximum of four consecutive corrections is enforced. The correction does not modify capacity state or pending ownership.

Assigned-service counters are updated with the corrected owner. The existing normalized comparison therefore repays a temporary correction as soon as the estimate no longer justifies it. Reconnect and active-set epoch rebasing remain authoritative.

## Evidence matrix

The opt-in virtual-carrier matrix used the production aggregation engine, physical serializer, cumulative ACK ledger, and pending feeders. `SMP3_HOL_BENCHMARK=1` enables it.

| Scenario | Legacy | Completion | Result |
|---|---:|---:|---|
| A: 100/100 Mbps, 20/100 ms, 8 MiB | 395.900 ms, 50/50, 0 rescue | 395.567 ms, 50/50, 0 rescue, 9 frontier candidates | neutral wall time and entitlement |
| B: 50/200 Mbps, 20/100 ms, 8 MiB | 329.521 ms, 19.9/80.1, 0 rescue | 330.536 ms, 19.9/80.1, 3 corrections, max streak 2 | capacity ratio preserved |
| C: 300 ms temporary leg1 stall, 100/100 Mbps, 8 MiB | 652.236 ms, 50/50, 0 rescue | 651.254 ms, 50/50, 2 corrections, max streak 2 | healthy path remains productive |

The healthy symmetric control produces no corrections. The direct backlog/stall unit gate selects the lower predicted completion leg and records zero rescue. After backlog and latency are cleared, assignment returns to both legs.

Frontier telemetry reported zero active receive-gap age and zero maximum gap age in the healthy matrix. Median, p95, and maximum rescue frontier waits were therefore zero for these no-loss runs; the existing rescue probe remains available for loss/stall runs. No speculative duplicate traffic was introduced.

The reconnect gate ran both failed-leg directions with completion mode. Payload passed, ledger and pending queues drained, rescue remained zero, and post-reconnect prefixes converged to approximately 20/80. The existing 2-second stall rescue test remains passing; HOL correction is assignment-only and does not enter retry/rescue planning.

## Capacity, activation, carrier, and stream interaction

Dynamic activation tests passed with HOL enabled. The shared-carrier closure passed 1, 4, and 8 streams for isolated and shared carriers, plus a 1 GiB shared run. Shared 50/200 Mbps runs converged to approximately 20/80, amplification was 1.000000, retry and rescue were zero, and payload/ledger checks passed. The 1 GiB shared run completed in 40.60 s at 211.6 Mbps useful throughput with 23.62/76.38 assigned bytes; the smaller shared runs converged to 20.0/80.0 as their estimator warmed.

The existing client SOCKS integration suite passed with the new field omitted, preserving standalone round trips and per-leg carrier isolation. The native adapter maps `hol-mode: completion` to the same Core mode and its targeted and race tests passed.

## CPU and memory

The three end-to-end A/B/C comparisons differed by less than 0.4% in wall time on the virtual carrier runs. The hot path uses atomics, existing counters, and a sequence stride; it does not allocate or poll. Existing memory-stability closure evidence remains applicable because the feature adds only bounded atomic counters and no per-record retained state: prior 1 GiB rounds, 5/20/30 GiB standalone runs, and churn/registry checks remained bounded.

## Validation gates

- Core/client/server/cmd tests: PASS.
- Core/client/server/cmd race: PASS.
- Native Mihomo adapter tests and `CGO_ENABLED=1 go test -race ./adapter/outbound`: PASS.
- Native Mihomo `go build ./...`: PASS.
- Tracked-only archive tests and four amd64 builds passed after the local commit.

## Known limitations

The latency term is a smoothed write-latency proxy rather than a measured wire RTT. Frontier wait percentiles are meaningful when the rescue/frontier probe is enabled; healthy no-loss runs correctly report zero gap age. The feature is intentionally opt-in and does not implement selective reinjection or speculative duplication.
