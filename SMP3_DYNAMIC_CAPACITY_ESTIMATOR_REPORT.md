# SMP3 Dynamic Capacity Estimator

## Scope

This change adds an explicit, opt-in capacity estimator for the v2.5
aggregation scheduler. Fixed aggregation remains the default and keeps using
the configured `bandwidth_mbps` weights.

## Runtime behavior

- `capacity_mode: dynamic` is accepted only with `scheduler_mode: aggregation`.
- Each leg starts from its configured bandwidth prior.
- Samples use cumulative ACK-retired useful DATA bytes, attributed once by the
  TX ledger. Retry and frontier rescue wire attempts are not counted as useful
  capacity a second time.
- A sample is valid only when the leg had service demand or queue pressure.
  Demand-limited and idle windows freeze the previous estimate.
- Valid samples use a one-second window, EWMA smoothing (`0.4` previous /
  `0.6` new), and confidence ramping (`0.45` toward one per valid window).
  The effective scheduler weight moves gradually and is bounded against
  invalid values.
- When a previously reduced leg is active but under-demanded, the estimator
  only reopens toward its configured prior when the observed ACK rate exceeds
  the current estimate. This is bounded passive recovery probing; idle legs
  do not decay or probe because they produce no ACK evidence.
- Reattaching a leg rebases that leg to its configured prior with zero
  confidence, avoiding stale reconnect debt.
- Startup activation (`first-ready`, its threshold, and its activation window)
  is unchanged.

## Configuration

Standalone client and server:

```json
{
  "stream": {
    "scheduler_mode": "aggregation",
    "capacity_mode": "dynamic"
  }
}
```

Mihomo uses the equivalent proxy fields `scheduler-mode: aggregation` and
`capacity-mode: dynamic`. Omitting `capacity_mode` preserves fixed behavior.

## Verification

- `go test ./...` passed in `core` (`smp3core`).
- `go test ./...` passed in `client`.
- `go test ./...` passed in `server`.
- New estimator tests cover demand-limited freeze, useful-ACK learning, idle
  freeze, and fixed-mode behavior.
- Dynamic stationary matrix (fixed control versus dynamic, 32 MiB each):
  `100+100 = 1.000x`, `50+100 = 1.005x`, `50+150 = 1.007x`,
  `50+200 = 1.000x`, and RTT-only `100 Mbps / 20 ms + 100 Mbps / 100 ms =
  0.983x`; a mixed `50 Mbps / 20 ms + 200 Mbps / 100 ms` run measured
  `1.006x`. No retry/rescue occurred in these runs.
- Longer stationary convergence runs (128 MiB each) produced valid high-leg
  samples and converged weights of approximately `100/100`, `50/100`,
  `50/149`, and `50/200`; assignment shares were within the requested 10
  percentage points of the physical ratios. Timer-boundary repair remained
  bounded at zero in the recorded replay (the gate permits at most two
  single-chunk repairs from the virtual serializer).
- Step-down (`50 + 200`, leg1 `200 -> 80 Mbps`) converged to approximately
  `50 / 95 Mbps` in a 7-second run, moving the entitlement from `20/80` toward
  `34/66` without a persistent rescue cascade.
- Step-up (`50 + 80`, configured prior `50 + 200`, leg1 `80 -> 200 Mbps`)
  recovered to approximately `50 / 189 Mbps` in 5.3 seconds.
- Transient (`200 -> 50 -> 200 Mbps`) returned toward the configured prior;
  the extended 512 MiB controlled run ended at approximately `50 / 159 Mbps`,
  remained bounded, and payload passed. The recovery probe now reopens only
  when under-demand ACK service reaches a substantial fraction of the prior;
  the step tests assert the direction and bounded final weight ranges.
- Demand-limited control now sustains a 40 Mbps application feed over 32 MiB
  (about 15.8 seconds). Both 100 Mbps legs remain at their 100 Mbps priors,
  with no valid downward samples and no confidence collapse.
- Dynamic failure/reconnect gate passed for both failed leg IDs: payload PASS,
  ledger zero, pending queues empty, and first 32/64/128/256 assignment
  shares stayed near `20/80` with maximum streak five.
- Dynamic 1 GiB long flow passed with the benchmark's default 1.5-second
  retransmit timeout (matching the production default): payload PASS, ledger
  zero, amplification `1.000000x`, retry zero, rescue zero, and useful
  throughput `237.15 Mbps` in the recorded run.
- CPU/alloc benchmark on Windows amd64 (3 iterations, 8 MiB): fixed
  `63.49 ms/op`, `16,448,152 B/op`, `3,540 allocs/op`; dynamic `61.56 ms/op`,
  `16,417,442 B/op`, `3,559 allocs/op`. The estimator adds no timer or
  goroutine and its per-leg state is bounded. A GC-normalized 1 GiB monitor
  recorded live heap `97 MB / 181 MB / 252 MB / 111 MB` at 25/50/75/100% and
  returned to `111 MB` with ledger and pending queues at zero; Go heap
  reservation grew to `532 MB`, so this is bounded live state rather than an
  allocation-free claim.
- A tracked-only Linux benchmark binary (`go test -c`) measured fixed versus
  dynamic aggregation over three 8 MiB iterations: `85.75 ms/op` versus
  `86.25 ms/op` (about `+0.6%` wall time), with `0` rescues in both. Repeated
  process runs showed max RSS in the same `51-52 MB` band and user/system CPU
  within run noise, so no material CPU or memory overhead was observed.
- A tracked-only `git archive` source passed core/client/server/cmd tests and
  built all four standalone amd64 binaries.
- Native Linux `CGO_ENABLED=1 go test -race` passed for tracked core, client,
  server, and command modules using Go 1.24.4 (core was also replayed on
  `192.168.112.104`). The external Mihomo adapter tests and race tests passed
  with the cached module graph, and fresh Linux/Windows amd64 Mihomo builds
  completed with version `2.5.0`.

## Classification

The validated candidate classification is:

`A. DYNAMIC_CAPACITY_PRODUCTION_CANDIDATE_VALIDATED`

The current HEAD has passing stationary convergence, step-down, step-up,
transient recovery, sustained demand-limited, RTT-only, mixed RTT/capacity,
failure/reconnect, retry/rescue, 1 GiB, CPU/RSS, tracked-only build, and native
race evidence. Fixed aggregation remains the default and dynamic capacity is
explicit opt-in. CPU/RSS figures are bounded benchmark measurements rather
than a substitute for production fleet profiling.

Current correction: normalized assigned-service finish is preserved across
dynamic weight changes. A deterministic 10,000-assignment history followed by
step-down and step-up verifies the next 256 assignments follow the new ratio
without lifetime debt. The existing fixed-mode path is unchanged.

This goal does not create a tag, release, deployment, or remote push.
