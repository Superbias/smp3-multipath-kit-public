# SMP3 Multipath Kit v2.3.3

[简体中文](README-zh_CN.md) | English

SMP3 is an independent application-layer multipath transport with three
product lines: Standalone, Native, and the integrated server Dashboard.

## Choose a mode

| Mode | Use case | Requires sing-box |
| --- | --- | --- |
| Native | Mihomo / Clash Party integration; recommended | No |
| Standalone | A local SOCKS5 endpoint for ordinary applications | No |
| `smp3-proxy` | Compatibility with existing sing-box configs | Yes, only here |
| Integrated Dashboard | Read-only status, rates, legs, traffic, events, and history | No |

`smp3-client` implements SMP3 and local SOCKS5 only. It does not implement
VLESS, Reality, Hysteria2, Snell, or other carrier protocols. Those node
definitions and dialers belong to Mihomo/Carrier. Standalone reaches its two
legs through the Carrier SOCKS5 endpoints.

## Release

- Version: `v2.3.3` (R16 Dashboard/accounting; data-plane semantics unchanged)
- Native artifact: Mihomo `v2.3.3` built from pinned upstream `v1.19.28` with the SMP3 adapter
- Standalone artifacts: `smp3-client` and `smp3-server`
- Compatibility artifact: `smp3-proxy` with its pinned sing-box runtime suffix
- Observability: integrated into `smp3-server`; standalone `smp3-panel` is retired
- [Download v2.3.3](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.3.3)
- Verify every download with `SHA256SUMS` before running it.

## Qualified deployment shape

```text
Native:     application -> Mihomo / Clash Party -> two SMP3 legs -> server
Standalone: application -> 127.0.0.1:18080 -> smp3-client
            -> Carrier-A / Carrier-B -> server
Dashboard:  browser -> SMP3 server -> 127.0.0.1:24500 telemetry
```

The current qualified local ports are:

| Component | Address | Purpose |
| --- | --- | --- |
| SMP3 primary/native | `:24444` | Native entry |
| SMP3 sidecar | `:24445` | Standalone entry |
| Carrier-A | `127.0.0.1:17898` | Leg0 carrier |
| Carrier-B | `127.0.0.1:17899` | Leg1 carrier |
| `smp3-client` | `127.0.0.1:18080` | Application SOCKS5 |
| telemetry | `127.0.0.1:24500` | Loopback-only monitoring |
| Integrated Dashboard | `127.0.0.1:24500` | Loopback REST/SSE and browser UI |

## Quick start

See the complete [deployment guide](DEPLOYMENT.md) or the
[Chinese deployment guide](DEPLOYMENT.zh-CN.md).

### Native / Clash Party

1. Download `mihomo-smp3-windows-amd64.exe` or the Linux artifact.
2. Start from [config/mihomo.example.yaml](config/mihomo.example.yaml).
3. Configure two different child outbounds and an `smp3` proxy.
4. Select the binary as Clash Party's custom core.
5. Point applications to the Mihomo mixed/SOCKS5 port, for example
   `127.0.0.1:7890`.

### Standalone

Start Carrier-A and Carrier-B first, then configure and run:

```bash
./smp3-client-linux-amd64 -c ./config/smp3-client.json -check
./smp3-client-linux-amd64 -c ./config/smp3-client.json
```

Point applications to `socks5://127.0.0.1:18080`.

### Server

```bash
cp config/standalone-server.example.json config/server.json
./smp3-server-linux-amd64 -c config/server.json -check
```

The standalone server uses its own SMP3 config; it is not a sing-box server.

### Integrated Dashboard

The Dashboard is served by `smp3-server`; no separate Panel process is needed.
Keep telemetry bound to loopback and open the server's Dashboard URL. The
read-only API includes `/api/v1/status`, `/api/v1/legs`, `/api/v1/sessions`,
`/api/v1/traffic`, `/api/v1/traffic/history`, and the `/api/v1/events` SSE
stream.

The former standalone `smp3-panel` process and port `24600` are retired.

## Process lifecycle

The `smp3-client` does not start external Carrier processes. Use the existing
service manager, scheduled tasks, or supervisor to manage them together:

```text
start: Carrier-A -> Carrier-B -> smp3-client
stop:  smp3-client -> Carrier-B -> Carrier-A
```

## Further documentation

- [Deployment and usage](DEPLOYMENT.md)
- [Standalone Sidecar](SIDECAR.md)
- [Integrated Dashboard deployment](DEPLOYMENT.md)
- [Security](SECURITY.md)
- [Release notes](RELEASE_NOTES.md)
