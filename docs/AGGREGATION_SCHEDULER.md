# Aggregation scheduler

SMP3 supports an explicit stream scheduler mode for bulk throughput-oriented
traffic:

```json
{
  "smp3": {
    "stream": {
      "scheduler_mode": "aggregation"
    }
  }
}
```

`aggregation` is opt-in. `adaptive` remains the default and existing
`static` behavior is unchanged.

The mode uses normalized assigned DATA bytes, bounded pending admission of 32
records per leg, independent per-leg feeders, active-set epoch rebasing, and
frontier exposure age for repair. With the default 32 KiB chunk size, pending
payload is bounded to approximately 2 MiB across two legs, in addition to the
existing physical queues.

Logical sequence identity, ACK framing, retry state, and wire framing are
unchanged. Pending ownership is rolled back only before physical admission;
admitted records use the existing invalidation and retry path. The mode does
not change the SOCKS5 interface, Mihomo integration, external proxy contract,
or standalone sidecar architecture.

## Dynamic capacity estimation (2.6.0)

Standalone client/server JSON enables dynamic weights with
`"scheduler_mode": "aggregation"` and `"capacity_mode": "dynamic"` inside
`stream`. Native Mihomo uses `scheduler-mode: aggregation` and
`capacity-mode: dynamic` on the SMP3 proxy. Fixed capacity remains the default
when the capacity field is omitted.

The estimator uses ACK-retired useful DATA bytes per leg. Retry and rescue
copies do not count twice. Demand-limited and idle windows freeze the estimate;
valid samples update smoothed capacities and future assignment weights.
Reconnect resets the affected leg to its configured bandwidth baseline.
Startup activation thresholds remain independent of capacity estimation.

Runtime capacity, confidence, sample validity, and assignment weights are
available in Core `StreamStats`; ordinary Mihomo logs do not expose them.

## Global physical-carrier capacity (v2.6 development)

When `scheduler_mode: aggregation` and `capacity_mode: dynamic` are enabled, standalone and native Mihomo clients create one bounded carrier-capacity registry per process. Concurrent logical streams sharing the same configured leg route/upstream identity contribute logical ACK-retired useful bytes and queue demand to the same estimator. A key includes the client owner domain, outbound direction, and carrier identity; different owners, directions, or leg carriers cannot share state. Registry entries retain warm estimates after stream close and are pruned after 10 minutes idle (with a 1024-entry bound). If no provider is supplied by an embedding application, the original per-stream estimator remains active.

The server intentionally does not merge sessions: each incoming session has independent physical carrier sockets, so cross-client aggregation would misattribute capacity. Core telemetry exposes the shared estimate through the existing `StreamStats.Capacity*` fields; ordinary Mihomo logs still do not print these fields unless a host exports `StreamStats`.

## Capacity-relative leg activation

Leg activation is opt-in through `stream.activation_mode`:

```json
{
  "scheduler_mode": "aggregation",
  "capacity_mode": "dynamic",
  "activation_mode": "dynamic",
  "activation_window": "1s"
}
```

Omitting `activation_mode` or setting it to `legacy` preserves the historical leg0-first trigger (`activation_threshold_mbps` plus the queue/congestion fallback). `dynamic` keeps leg0 as the startup carrier, then compares logical application demand against the active primary's global carrier estimate. It uses a 10% safety margin, sustained evidence, and a 200ms decision for high-confidence overload. A short burst decays without opening the secondary. Queue congestion and transport failure remain emergency fallbacks. Once opened, the existing dynamic capacity and assigned-service planner controls the traffic ratio; activation does not force a split.

Demand is counted when logical application DATA enters the stream and is retired on cumulative useful ACK. Retransmit, rescue, and duplicate physical sends do not increase demand. The registry aggregates demand across streams sharing the same CarrierKey, so individually small streams can activate the secondary when aggregate demand exceeds shared primary capacity. Standalone and native Mihomo use the same core semantics.
