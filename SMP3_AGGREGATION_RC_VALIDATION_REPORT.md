# SMP3 Aggregation RC Validation Report

## Decision

`F. ARTIFACT_PROVENANCE_BLOCKER`

The frozen tracked source cannot build standalone binaries. Real-world gates
have not been executed and cannot be classified as passing.

## Source and build context

- Source SHA: `2b0d02e262a5e33838cc1318df24d2c8217813c4`
- Branch: `main`; HEAD was unchanged throughout this gate.
- Date: 2026-10-02 (Asia/Shanghai).
- Builder: Go 1.22.6, Windows/amd64; cross-build CGO_ENABLED=0.
- Clean source: `git archive --format=zip` of the exact SHA, extracted outside
  the dirty checkout. No untracked research sources were copied into it.
- Archive SHA256:
  `50dda0d8c527ec6615f5f73a327bbe301dc27f0f24edc186ea6d786dbe3c8db0`

## Build attempts

| Component | Target | Result |
|---|---|---|
| smp3-client | windows/amd64 | FAIL |
| smp3-server | windows/amd64 | FAIL |
| smp3-client | linux/amd64 | FAIL |
| smp3-server | linux/amd64 | FAIL |

Commands used the repository's standalone flags: CGO_ENABLED=0, GOARCH=amd64,
explicit GOOS and archived GOWORK, `go build -trimpath`, and `-buildid=`.
The existing Version symbol was set to `2.4.0-aggregation-rc`; no version
machinery or tracked source was changed.

All attempts fail while compiling core/stream_engine.go with undefined
`streamLoadProbe`, `streamWakeTelemetry`, and `streamSpaceNotification`.
Definitions reside in untracked `core/stream_load_debug.go` and
`core/stream_wake_debug.go`, neither present in the committed tree.

Earlier tests on the dirty checkout and manually synced native tree do not
establish that this committed source builds independently. The earlier commit
closure omitted required dependencies; the previous merge-clean assessment
must be read with this correction.

## Artifact provenance

No new RC binaries were successfully produced. Consequently there is no
candidate binary SHA256SUMS and no tested artifact hash claim. Existing dist
binaries are not substituted for the frozen-source build.

## Standalone and real-world gates

Topology, single-leg controls, dual throughput, efficiency, leg distribution,
browser interaction, sustained media, large download, high/low-leg failure,
reconnect, temporary interruption, long-run memory stability, SOCKS5, Mihomo,
and adaptive/static binary controls: NOT RUN due to the clean-source build
blocker. No real-world validation claim is made.

## Required next action

Repair committed-source completeness in a separate authorized source-closure
step, verify builds and tests from a tracked-only checkout, then choose a new
frozen source SHA for RC qualification. Do not mix untracked files into this
SHA's build or claim the resulting binary came from the unchanged commit.

No tag, release, upload, production deployment, commit amendment, or default
scheduler switch was performed. Historical untracked files were preserved.
