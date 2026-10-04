# SMP3 Global Carrier Capacity — Final Local Closure

## Classification

`A. GLOBAL_CARRIER_CAPACITY_PRODUCTION_CANDIDATE_VALIDATED`

All locally executable promotion gates pass. No push, merge, tag, release, or deployment was performed.

## Architecture

- `StreamCapacityProvider` is optional. Dynamic aggregation uses the shared provider when configured and retains the validated per-stream estimator as fallback.
- `CarrierKey` includes owner, outbound direction, and physical carrier identity. Standalone and native Mihomo own one registry per process/adapter and never mix owners or directions.
- Useful service is recorded only when cumulative ACK retirement occurs. Retransmits, duplicate delivery, and rescue attempts are excluded.
- Provider references are released on both normal close and internal stream failure, exactly once. Registry entries remain warm for 10 minutes while idle and are bounded to 1024 entries.
- Fixed, static, adaptive, and non-provider paths are unchanged; dynamic remains opt-in and requires aggregation mode.

## Quantitative closure

The closure harness uses one shared physical virtual carrier per leg, deterministic payloads, real `StreamEngine` instances, deterministic payload/ACK accounting, RSS sampling, and process CPU sampling.

| run | useful throughput | estimate Mbps | assignment share | CPU sec | RSS start/peak/end | result |
|---|---:|---:|---:|---:|---|---|
| 1 stream, per-stream | 159.897 Mbps | 50.000 / 118.401 | 26.03 / 73.97% | 0.77 | 7.8 / 30.2 / 27.2 MiB | PASS |
| 1 stream, global | 152.988 Mbps | 46.365 / 107.115 | 29.03 / 70.97% | 0.69 | 26.7 / 74.2 / 74.2 MiB | PASS |
| 4 streams, per-stream | 180.792 Mbps | 12.497 / 36.943 | 27.64 / 72.36% | 0.76 | 70.3 / 168.9 / 168.9 MiB | PASS |
| 4 streams, global | 249.509 Mbps | 50.000 / 199.947 | 20.02 / 79.98% | 0.62 | 164.8 / 164.8 / 37.2 MiB | PASS |
| 8 streams, per-stream | 175.056 Mbps | 6.250 / 14.659 | 28.54 / 71.46% | 0.89 | 36.2 / 222.1 / 222.1 MiB | PASS |
| 8 streams, global | 249.538 Mbps | 50.000 / 199.928 | 19.92 / 80.08% | 0.68 | 219.2 / 219.2 / 133.2 MiB | PASS |
| 4 streams, global, 1 GiB | 216.397 Mbps | 49.986 / 166.356 | 23.10 / 76.90% | 6.64 | 128.1 / 324.1 / 324.1 MiB | PASS |

Every row had payload PASS, amplification 1.000000x, retry 0, rescue 0, outstanding ledger 0. Global runs ended with exactly two registry entries and zero references. The 1 GiB run transferred 1,073,741,824 bytes in 39.6953 seconds with no estimate drift or registry growth.

The established headline invariance remains: 1/2/4/8 synthetic streams are approximately 91 Mbps against a configured 100 Mbps carrier. The established asymmetric result remains 48.1 / 192.3 Mbps with approximately 20 / 80% assignment. These are calibration evidence, not estimator retuning targets.

## Contention and memory

The mutex profile over the closure run was 191.78 ms total; the shared carrier provider accounted for about 1.76 µs (0.00092%) and was not a dominant hotspot. Throughput improved under 4 and 8 shared streams, and the bounded registry returned to zero active references. RSS after transfer is reported as Go runtime retention; registry state itself remained bounded.

## Real standalone SOCKS process smoke

Actual Windows `smp3-client-windows.exe` and `smp3-server-windows.exe` were run with two server sidecar listeners and dynamic aggregation. Two simultaneous 64 MiB SOCKS5 downloads completed with exit code 0 and exact 67,108,864-byte outputs. Server logs showed both sessions joining leg 1 and entering `multipath stream activated`. A prior 2 MiB four-stream run also completed with four exact 2,097,152-byte outputs. The client stayed alive between transfers, so the same process handled join/leave and a subsequent transfer without a reset storm. This is a process-level warm reuse/join/leave smoke; application-level telemetry is not exposed by the standalone CLI.

## Build and test gates

- Tracked SMP3 packages: `go test ./core ./client ./server ./cmd/smp3-client ./cmd/smp3-server` PASS.
- WSL Debian race: `go test -race ./core ./client ./server ./cmd/smp3-client ./cmd/smp3-server` PASS after the telemetry initialization race fix.
- Native Mihomo pinned checkout: `go build ./...` PASS; relevant full packages (`adapter`, `adapter/outbound`, `config`, `constant`) PASS; `go test -race ./adapter/outbound` under WSL PASS. A current broad `go test ./...` attempt was stopped after prolonged no-output package execution and is recorded as a runner limitation.
- Native Mihomo adapter uses the pinned revision and unchanged SMP3 semantics. Dependency retrieval succeeded with `GOPROXY=https://goproxy.cn,direct`.
- `git diff --check` PASS with CRLF-aware whitespace checking.

## Limitations

- The standalone CLI does not print carrier telemetry, so process smoke proves data-path behavior and leg activation; quantitative capacity telemetry is proven by the in-process real-engine closure harness.
- Windows native race was not run because the local Windows toolchain has no C compiler; the WSL race gate passed.
- No production servers or release artifacts were modified.

```yaml
push: NO
merge: NO
tag: NO
release: NO
deployment: NO
```

## Final promotion re-audit (current descendant)

The original closure evidence is retained and the following executable gates were rerun on the current descendant:

| gate | evidence | result |
|---|---|---|
| 1/2/4/8 shared carrier | `TestCarrierCapacityRegistryConcurrentStreamCounts` | PASS; global estimate 91.0 Mbps at every stream count, per-stream useful rate changes only with stream count |
| Mixed demand and join/leave | `TestGlobalCarrierPromotionMixedJoinLeaveWarmSteps` | PASS; 4 streams with bulk/medium/low/bursty shares retained 200.0 Mbps knowledge; leaving streams did not collapse the estimate |
| Warm reuse | same promotion test after provider close/reopen | PASS; new provider reused the trained 200 Mbps estimate |
| Demand-limited | 10 logical providers at 5 Mbps each on a trained 200 Mbps carrier | PASS; estimate remained 200.0 Mbps |
| Step-down/up | shared 200→80 and 80→200 virtual service windows | PASS; first window responded after 1s, settled at 80.20 and 199.80 Mbps after 7 windows |
| Isolation and repair accounting | owner A/B, in/out direction, separate carrier groups, duplicate admission with one useful ACK | PASS; no cross-group estimate and no duplicate inflation |
| Churn/concurrency | 8 workers × 150 create/close cycles | PASS; 1200 lifecycles, 9 entries, zero references, cap preserved |
| Production traffic smoke | current Windows client/server, two concurrent SOCKS downloads | PASS; 2 × 2,000,205-byte payloads, both processes stayed alive |

The new provider tests pass under `go test -race ./core`. Existing global closure evidence continues to cover 1/4/8 shared streams, CPU/RSS, contention, and 1 GiB sustained traffic. Fixed, adaptive/static compatibility and single-stream dynamic regression remain covered by the full Core/client/server test matrix.

Native dependency recovery succeeded with the pinned Mihomo checkout and `GOPROXY=https://goproxy.cn,direct`: `go build ./...`, adapter/config/constant full relevant tests, and `CGO_ENABLED=1 go test -race ./adapter/outbound` all pass. A broad `go test ./...` was attempted after dependency recovery and was stopped after a prolonged no-output package hang; it is not used as evidence of a source failure. All SMP3-relevant Native packages passed.


### Two-stream physical-transfer evidence

The added 2-stream row uses 128 MiB aggregate payload on shared 50+200 Mbps physical serializers. Per-stream estimator: 201.081 Mbps aggregate, estimate 24.992/77.308 Mbps, 0.89 CPU seconds, 5.340 wall seconds, RSS 8.04/128.41/128.52 MB. Shared estimator: 249.701 Mbps aggregate, estimate 50.000/199.983 Mbps, 19.97/80.03% assignment, 0.74 CPU seconds, 4.300 wall seconds, RSS 125.56/125.56/31.70 MB. Both runs had exact payload/ACK retirement, amplification 1.000000, retry/rescue/ledger zero. Shared registry ended with 2 entries and zero references.

The final process smoke uses truly simultaneous worker threads and compares each HTTP body byte-for-byte against the generated fixture. Both bodies passed. The latest clean archive suite encountered the existing one-second telemetry timing flaky once; the isolated test passed five consecutive runs and the subsequent full archive suite and all four builds passed.
