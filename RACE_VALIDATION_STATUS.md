# Race validation status

## Dynamic Leg Activation

- WSL Debian: `go test -race ./core ./client ./server ./cmd/smp3-client ./cmd/smp3-server -count=1` PASS.
- Native Mihomo pinned checkout: `CGO_ENABLED=1 go test -race ./adapter/outbound -count=1` PASS.
- Shared demand state is registry-mutex protected; stream-local activation observations use atomics.

## Memory closure
- Added diagnostic-only memory stability test; no production lifecycle code changed.
- Existing race evidence remains applicable; final core/client/server race gate will be rerun after the diagnostic test is committed.

## Completion-time / HOL-aware assignment
- WSL Debian: `go test -race ./core ./client ./server ./cmd/smp3-client ./cmd/smp3-server -count=1` PASS.
- Native Mihomo pinned checkout: `CGO_ENABLED=1 go test -race ./adapter/outbound -count=1` PASS.
- HOL correction state is atomic; assignment and assigned-service remain under the existing weighted mutex; ACK frontier publication is atomic.
- Shared-carrier 1/4/8-stream and 1GiB HOL-enabled closure passed with retry/rescue zero.
