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
- Native Mihomo pinned checkout: `go build ./...` PASS; `go test ./... -count=1` PASS; `go test -race ./adapter/outbound` under WSL PASS.
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
