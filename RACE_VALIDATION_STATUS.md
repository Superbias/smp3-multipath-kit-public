# Race validation status

## Dynamic Leg Activation

- WSL Debian: `go test -race ./core ./client ./server ./cmd/smp3-client ./cmd/smp3-server -count=1` PASS.
- Native Mihomo pinned checkout: `CGO_ENABLED=1 go test -race ./adapter/outbound -count=1` PASS.
- Shared demand state is registry-mutex protected; stream-local activation observations use atomics.
