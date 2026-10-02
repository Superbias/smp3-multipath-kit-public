# SMP3 Aggregation Source Closure and RC Validation Report

## Decision

`A. SOURCE_CLOSURE_VALIDATED`

The previously frozen commit `2b0d02e262a5e33838cc1318df24d2c8217813c4` is
source-incomplete and is not RC eligible. Source closure was completed in
separate commits on top of it; the latest tracked-only validation SHA is
`4e695b0415d50f998a3ac03731041d1940a7093a`.

No push, tag, release, deployment, or default scheduler change was performed.

## Source closure

- `36d474091ba297b5394192435a4468c2034c7cf3` adds the missing production
  declarations and runtime state for `streamLoadProbe`, `streamWakeTelemetry`,
  and `streamSpaceNotification`.
- `4e695b0415d50f998a3ac03731041d1940a7093a` adds only the benchmark support
  needed by committed aggregation tests: the backpressure snapshot and the
  required pacing metric helpers.
- Historical untracked debug/calibration files remain preserved as
  `.historical`; they were not blindly included in the closure.

## Clean archive provenance

- Archive source: `git archive --format=zip` of `4e695b0`; extracted outside
  the dirty checkout. No untracked research files were copied.
- Archive SHA256:
  `FF301808F9D987043B2488C41419D725840629EA061552DA057914B13315275B`
- Build host: Windows/amd64, Go 1.22.x, `CGO_ENABLED=0`, explicit GOOS/GOARCH,
  archived `go.work`, `go build -trimpath -buildid=`.

## Standalone builds

| Artifact | Result | Size | SHA256 |
|---|---:|---:|---|
| `smp3-client-windows-amd64.exe` | PASS | 4,512,768 | `DD7E816B4E820004ED51E87EE1B904BA2474FD87837B7F3766EE0CD9F316ADB7` |
| `smp3-client-linux-amd64` | PASS | 4,399,125 | `87714F7056B2A69861528D15541B6EC428D3838E7C02E62BC3D8C6348AC77D45` |
| `smp3-server-windows-amd64.exe` | PASS | 8,630,272 | `9571892377018A53DFFD1C2D9B30440EE18C0BCE47CED5EF32A511E849EB794B` |
| `smp3-server-linux-amd64` | PASS | 8,477,230 | `E6BCA5A7F28E4CEA6FB782035677A0E93F6CDD4BCEED540CB4CDE8C3BAAC82F4` |

The complete checksum file is `D:\SMP3\aggregation-rc-4e695b0\SHA256SUMS`.

## Test gates

- Clean archive `core`: `go test ./...` — PASS.
- Clean archive `client`: `go test ./...` — PASS.
- Clean archive `server`: `go test ./...` — PASS.
- Clean archive `cmd/smp3-client`: `go test ./...` — PASS.
- Clean archive `cmd/smp3-server`: `go test ./...` — PASS.
- Native Linux `192.168.112.104`, `/usr/local/go/bin/go1.22.12`,
  `CGO_ENABLED=1 GOWORK=off go test -race ./...` — PASS.
  The host default `/usr/bin/go1.17.8` is too old for this source and was not
  used for the gate.

## Tracked-only aggregation regression

Command ran from the extracted clean archive with production aggregation,
serializer pacer, `bw_50_200`, one 64 MiB repetition, 32 pending frames,
256 queue depth, and 32 KiB chunks.

Result: PASS. Raw aggregate `245.77 Mbps`; single-leg controls `49.81` and
`197.02 Mbps`; useful and physical raw efficiency `98.31%`; payload efficiency
`100.00%`; leg utilization `98.16%` / `98.34%`; share `19.97%` / `80.03%`;
amplification `1.000x`; retransmit `0`; rescue `0`; ledger `0`; reorder `0`.

## Correction to the prior report

The prior report classified `2b0d02e...` as an artifact provenance blocker and
correctly recorded that its clean build failed. That SHA remains
`SOURCE-INCOMPLETE / NOT RC-ELIGIBLE`. The validated source and artifacts in
this report belong only to `4e695b0...` and its archive hash above.
