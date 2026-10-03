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
