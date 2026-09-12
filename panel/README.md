# SMP3 v2.3.4 read-only Panel monitor

This module is a separate, loopback-only management surface. It reads the
existing telemetry API with bounded GET requests and serves a same-origin UI;
the browser never connects to `127.0.0.1:24500` directly.

Run locally:

```text
GOWORK=off go test ./...
GOWORK=off go build ./cmd/smp3-panel
smp3-panel -listen 127.0.0.1:24600 -telemetry http://127.0.0.1:24500 -history monitor-history.jsonl
```

Open `http://127.0.0.1:24600/` after startup. In the qualified deployment,
`127.0.0.1:24500` is the loopback-only telemetry source; the browser does not
connect to it directly.

The monitor exposes only read-only routes:

- `GET /api/monitor/status`
- `GET /api/monitor/history?scope=memory|persistent|all&limit=...`
- `GET /api/monitor/events?limit=...`
- `GET /api/monitor/stream`
- `/`, `/monitor.js`, `/monitor.css`

Memory history is retained for one hour at one-second collection resolution;
persistent history is aggregated at ten seconds and retained for seven days.
The monitor records only aggregate counters, health, and privacy-safe
operational events. It does not expose raw session identifiers, destinations,
targets, payloads, or credentials. See the repository
[deployment guide](../DEPLOYMENT.md) for the full Standalone/Native/Panel
startup order and port layout.
