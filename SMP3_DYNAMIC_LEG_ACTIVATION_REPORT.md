# SMP3 Dynamic Leg Activation Report

## Scope

This report records the production candidate for capacity-relative secondary-leg activation. The implementation is based on the frozen global carrier-capacity closure at `df900ab`. This phase does not push, merge, tag, publish, or deploy.

## Compatibility and activation policy

- `activation_mode` is explicit in standalone JSON and Mihomo proxy options.
- Omitted or `legacy` keeps the existing fixed threshold and queue-congestion behavior.
- `dynamic` is accepted only with aggregation scheduling.
- The primary leg is still selected and started first.
- Secondary activation is one-way for the lifetime of a stream; an activated set is not forcibly split or shrunk.
- Queue congestion and primary-leg loss remain emergency fallback paths.
- Retry and rescue traffic are excluded from application demand accounting.

## Dynamic decision

For an aggregation stream, application DATA and useful cumulative ACKs update logical demand. When a shared carrier-capacity provider is present, demand is aggregated by the primary physical carrier key so multiple logical streams cannot multiply the same service clock. Capacity uses the global estimator when valid and falls back to the configured primary capacity when unavailable.

The overload ratio is:

```
overload_ratio = logical_demand_bps / (active_global_capacity_bps * 1.10)
```

A valid, sufficiently confident ratio at or above 1.50 may qualify after 200 ms. Normal overload requires the configured activation window (default 1 s). Outstanding logical backlog can shorten qualification only when it is itself above the guarded capacity fraction. Demand is stale-decayed, so a short burst cannot permanently arm activation.

## Evidence

Tracked source tests:

- `go test ./core ./client ./server ./cmd/smp3-client ./cmd/smp3-server -count=1 -timeout 15m`: PASS.
- `go test -race ./core ./client ./server ./cmd/smp3-client ./cmd/smp3-server -count=1 -timeout 20m`: PASS.
- Dynamic low/near/overload/high-capacity-primary, capacity step-up/drop, shared demand, and short-burst tests: PASS.
- Configuration mapping tests for standalone and server: PASS.
- Native pinned Mihomo adapter dynamic mapping test and `CGO_ENABLED=1 go test -race ./adapter/outbound -count=1`: PASS.
- Native full build and full test suite: PASS; the previously isolated VMess concurrent fixture also passed on rerun.
- Dynamic global 1GiB closure:
  - shared=true, streams=4, bytes=1073741824
  - wall time 40.3468 s
  - useful throughput 212.903 Mbps
  - capacity estimates 49.992 / 168.388 Mbps
  - share 0.2347 / 0.7653
  - amplification 1.000000
  - retry=0, rescue=0, ledger=0, payload=PASS
  - RSS samples 8.88 MB start, 326.79 MB peak, 326.92 MB end
  - CPU time 5.2600 s
- Real standalone Windows smoke:
  - low 4 KiB transfer completed without activation;
  - high 64 MiB transfer completed and logged `multipath stream activated` plus `leg1 joined`;
  - two simultaneous 64 MiB transfers both activated and joined leg 1.

## Native and standalone configuration

Standalone stream configuration:

```json
{
  "scheduler_mode": "aggregation",
  "activation_mode": "dynamic",
  "capacity_mode": "dynamic",
  "activation_window": "1s"
}
```

Mihomo SMP3 proxy option:

```yaml
scheduler-mode: aggregation
activation-mode: dynamic
capacity-mode: dynamic
activation-window: 1s
```

The native adapter source is tracked under `adapters/mihomo/outbound/smp3.go`; the pinned Mihomo checkout is updated only for validation and remains ignored.

## Limitations

Dynamic activation intentionally does not deactivate an already joined secondary leg mid-stream. Capacity and confidence telemetry is exposed in stream statistics and the activation reason records whether dynamic overload, legacy threshold, or queue fallback caused activation. The final release artifact and production deployment remain separate, explicitly authorized actions.
 
