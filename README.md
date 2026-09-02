# SMP3 Multipath Kit 2.1.1

[English](README.md) | [简体中文](README-zh_CN.md)

Independent application-layer multipath transport built around a reusable SMP3 Core and standalone server.

- **Release:** `2.1.1` (bidirectional Stream activation bugfix)
- **Runtime baseline:** `2.0.0` (runtime semantics unchanged)
- **Optional sing-box compatibility client build input:** `v1.14.0-beta.14`
- **Compatibility build commit:** `4902660f8424fef3c2a60dfcdce7aeadfe3f3b88`
- **Expected sing client binary:** `1.14.0-beta.14-smp3-2.0.0`
- **Expected standalone server binary:** `2.0.0`
- **TCP stream HELLO:** v4 (compatible with r10 stream mode)
- **UDP datagram HELLO:** v5 (2.0.0 endpoints required)

> This is an independent SMP3 release. It is not an official SagerNet/sing-box or MetaCubeX/Mihomo release.

## Deployment and usage

See the complete [Deployment and Usage guide](DEPLOYMENT.md). The short path
is: configure and validate `config/standalone-server.example.json`, install
`smp3-server`, then configure either `config/mihomo.example.yaml` or the
optional sing-box client profile. The standalone server is not a sing-box
server and does not terminate any child carrier protocol.

Windows Mihomo one-click install:

```powershell
Invoke-WebRequest https://raw.githubusercontent.com/Superbias/smp3-multipath-kit-public/main/scripts/install-mihomo-smp3.ps1 -OutFile install-mihomo-smp3.ps1
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe"
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe" -Check
```

Linux standalone one-click install:

```bash
curl -fsSL https://raw.githubusercontent.com/Superbias/smp3-multipath-kit-public/main/scripts/install-smp3-server.sh -o install-smp3-server.sh
chmod +x install-smp3-server.sh
sudo ./install-smp3-server.sh --config ./config.json
smp3ctl status
```

Installer operations:

```powershell
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe" -Check
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe" -Update
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe" -Restore
```

```bash
sudo ./scripts/install-smp3-server.sh --check
sudo smp3ctl status
sudo smp3ctl logs -f
sudo smp3ctl update
sudo smp3ctl rollback
```

The Windows installer replaces only the explicitly selected Mihomo executable.
The Linux installer manages only the standalone server and preserves the
configuration on uninstall unless `--purge` is explicitly requested. Both
installers verify exact GitHub stable Release assets against `SHA256SUMS`.

## Standalone SOCKS5 sidecar client (development)

This branch also contains a cross-platform standalone sidecar client. It is a
separate local SOCKS5 endpoint for applications that need SMP3 without a
native Mihomo or sing-box integration:

```text
application -> sidecar SOCKS5 -> host SOCKS5 CONNECT
            -> two carrier routes -> standalone SMP3 server -> destination
```

It uses only upstream TCP `CONNECT` and never uses upstream SOCKS UDP. The
sidecar's `leg0` and `leg1` routes must independently reach the same SMP3
listener; `leg1_fallback` is optional. It supports TCP CONNECT and SOCKS5 UDP
ASSOCIATE, with IPv4/IPv6/domain addresses. See [SIDECAR.md](SIDECAR.md),
[SIDECAR.zh-CN.md](SIDECAR.zh-CN.md), and the placeholder-only examples
`examples/smp3-client-config.example.json` and
`examples/mihomo-sidecar.example.yaml`.

`upstream_socks.connect_timeout` bounds the complete carrier SOCKS5 CONNECT
transaction, including the upstream host's CONNECT reply. This prevents a
host proxy's internal retries from indefinitely blocking leg1 fallback.

Sidecar routes use the standalone server's `sidecar_listeners`, which return an
authenticated `SMP3RDY1` record only after canonical HELLO admission. A SOCKS
CONNECT success alone is not treated as remote SMP3 readiness; legacy
`listen`/`listeners` endpoints remain canonical-only.

The sidecar is development-only on this branch. It does not replace or modify
the native Mihomo/sing adapters, carrier definitions, Clash Party, firewall
rules, or production configuration.

## Architecture in 2.1.1

The release separates the reusable, standard-library-only SMP3 Core from its hosts:

```text
Mihomo / sing-box client adapters
        ↓  SMP3 adapter: two independent child outbounds
        ↓  external carrier servers terminate the outer protocols
        ↓  raw TCP streams carrying SMP3 HELLO and frames
standalone SMP3 server
        ↓
canonical SMP3 Core
        ↓
Internet destinations
```

The standalone server is the production landing endpoint. It only handles
authenticated SMP3 HELLO, Stream frames, and Datagram frames. External carrier
servers/terminators handle Snell, Hysteria2, VLESS, or other outer protocols and
forward raw TCP streams to the SMP3 listener; standalone does not implement or
listen for those child carrier protocols. The two child outbounds independently
reach the same listener and share one SMP3 logical session, not one carrier
connection.

## What 2.1.1 packages

2.1.1 keeps the validated 2.0.0 wire/runtime behavior and packages the
carrier-agnostic sing adapter policy together with the extracted Core,
standalone server, Mihomo adapter, compatibility integration, and operations
tooling:

1. **Adaptive TCP scheduler** — per-leg useful ACK/write throughput and write latency reshape static bandwidth weights so slow paths receive fewer early sequence numbers and cause less HOL pressure.
2. **Bootstrap failover** — leg0 gets a configurable head start; if it fails or is still pending after `bootstrap_fallback_delay`, leg1 is dialed in parallel and the first authenticated HELLO establishes the logical session.
3. **MP-UDP Datagram Mode** — UDP can use both child paths instead of one `udp_outbound`, with `stripe`, `duplicate`, and `adaptive` policies.
4. **Bidirectional Stream activation** — each logical Stream session observes application payload in both directions and activates leg1 using `max(txRate, rxRate)` rather than client TX only.

## Data planes

```text
                         SMP3 2.0.0
                            │
                 ┌──────────┴──────────┐
                 │                     │
               TCP                   UDP
          ordered stream          datagrams
                 │                     │
       seq + cumulative ACK      flow PacketConn
       reorder + retransmit      datagram id + dedup
       ACK-paced rescue          no global ordering/HOL
                 │                     │
            leg0 + leg1             leg0 + leg1
```

### TCP stream mode

The release retains the r10-compatible stream wire protocol (HELLO v4), including:

- one logical byte stream over two child TCP paths;
- bounded outstanding window;
- cumulative ACK retirement;
- receiver reorder/dedup;
- cross-leg retransmission;
- ACK-paced single-frontier rescue;
- per-leg ACK/control isolation;
- graceful tail drain;
- leg rejoin and same numeric leg-ID transport replacement;
- session tombstones and retirement barriers.

The new `scheduler_mode: adaptive` uses observed per-leg performance in addition to `bandwidth_mbps` priors.

### UDP datagram mode

The release uses a separate v5 datagram HELLO and `DATAGRAM` frame. UDP does **not** reuse TCP cumulative ACK or ordered reassembly.

Each datagram carries:

```text
datagram_id + destination + payload
```

The server delivers unique datagrams immediately, even when an older datagram ID has not arrived. Duplicate copies are removed by a bounded dedup window.

Modes:

- `stripe`: one path per datagram, weighted by configured bandwidth/queue state;
- `duplicate`: copy each datagram over all live paths, deliver the first unique copy;
- `adaptive`: dynamically weight paths by observed throughput/queue delay; optionally duplicate small latency-sensitive packets with `adaptive_duplicate_threshold`.

R11 caps one routed UDP datagram at **16384 bytes**. This matches the packet-buffer boundary used by sing's packet routing path and comfortably covers normal MTU-sized DNS/game/QUIC traffic. Larger IP-fragmented UDP is rejected instead of silently truncated; protocol-level UDP fragmentation is intentionally deferred.

Important: child carriers are still reliable TCP-capable outbounds (for example a line path, Hy2 TCP stream, or Snell TCP carrier). Therefore one individual carrier can still experience its own stream HOL. SMP3 Datagram Mode removes **SMP3-wide/global ordered HOL between the two paths**; it does not turn the underlying carriers into native unreliable UDP transports.

## Adaptive behavior

There are two different adaptive layers:

- `scheduler_mode: adaptive`: TCP DATA scheduling between leg0/leg1.
- `udp_multipath.mode: adaptive`: UDP datagram scheduling between leg0/leg1.
- Existing `leg1_adaptive`: optional primary-carrier -> fallback-carrier policy based on sustained logical-stream health. 2.1.1 keeps the same mechanism without assuming a protocol type.

For each Stream session, adaptive activation observes application payload
throughput in both directions. It computes the TX and RX payload rates over
the same `activation_window` and compares the higher directional rate
(`max(txRate, rxRate)`) with `activation-threshold-mbps`. This is per logical
session, not an aggregate across connections, so download-heavy flows can
activate leg1 without adding a second activation algorithm to the adapters.

SMP3 does not require a specific proxy protocol. Each leg may use any
configured child outbound that provides the required reliable TCP dial
capability. Snell, Hysteria2, VLESS, Trojan, TUIC, Shadowsocks, VMess, and
Direct can be used when the host outbound supports that capability. IPv4/IPv6
selection remains delegated to the child outbound.

These layers are intentionally separate so carrier replacement and path load balancing do not become one opaque state machine.

### Review hardening in the current closeout

The pre-release review found and fixed several boundary cases before live acceptance:

- replicated UDP datagrams that fall behind the dedup window are rejected as stale duplicates, while pure `stripe` datagrams remain fully unordered and are not discarded merely for arriving very late;
- UDP-only traffic now participates in the shared primary/fallback carrier health/cooldown path on hard carrier failure; a probation primary carrier is not marked recovered by Dial/HELLO alone and requires real useful UDP payload, while an unused probation slot is released on close;
- bootstrap leg1 retries the configured fallback carrier immediately when its selected primary carrier fails, so `leg0 down + primary down + fallback healthy` can still establish the logical session;
- graceful TCP drain determines failure from observed ACK progress instead of treating an already-readable timer channel as proof of a stall;
- UDP scheduling accounts for queued **bytes**, not just queued frame count;
- routed address metadata is bounded to 512 bytes and one UDP datagram to 16384 bytes before wire allocation;
- a saturated preferred TCP send queue activates the booster immediately instead of waiting for the whole activation window.

For r11 v5 Datagram sessions, configure the client and server with matching `udp_multipath.mode`, `max_datagram_size`, and duplication policy. HELLO v5 identifies Datagram mode but does not negotiate these sub-policy values.

## Recommended client configuration

Start from `config/client-adaptive.example.json`.

The r11-specific part is:

```json
{
  "type": "multipath",
  "outbounds": ["line-path", "public-hy2"],
  "preferred": "line-path",
  "scheduler_mode": "adaptive",
  "bootstrap_fallback_delay": "250ms",
  "udp_multipath": {
    "enabled": true,
    "mode": "adaptive",
    "queue_frames": 256,
    "max_datagram_size": 16384,
    "dedup_window": 4096,
    "idle_timeout": "2m",
    "adaptive_queue_delay": "120ms",
    "adaptive_duplicate_threshold": 0
  },
  "endpoints": [
    { "server": "YOUR_LANDING_PRIVATE_AGGREGATION_IP", "server_port": 24444 },
    { "server": "YOUR_LANDING_PRIVATE_AGGREGATION_IP", "server_port": 24444 }
  ],
  "password": "YOUR_SMP3_PASSWORD"
}
```

`adaptive_duplicate_threshold: 0` disables replication by default. For latency-sensitive small UDP flows, a future deployment can set a non-zero threshold after testing bandwidth cost.

## Standalone server configuration

The production server uses its own standalone schema, not a sing-box config.
Start from `config/standalone-server.example.json`:

```json
{
  "listen": "0.0.0.0:24444",
  "password": "CHANGE_TO_A_LONG_RANDOM_SMP3_PASSWORD",
  "stream": { "scheduler_mode": "adaptive" },
  "udp": {
    "enabled": true,
    "mode": "adaptive",
    "max_datagram_size": 16384,
    "idle_timeout": "2m"
  }
}
```

Validate/install on Linux:

```bash
./dist/smp3-server-linux-amd64 -c config/server.json -check
sudo ./scripts/install-smp3-server.sh --config config/server.json
sudo systemctl status smp3-standalone
```

Keep the raw aggregation listener private/internal and use encrypted carriers for public paths. For full client configuration, startup, verification, upgrade, rollback, and troubleshooting, use `DEPLOYMENT.md`.

## Compatibility

| Client | Server | TCP | MP-UDP |
|---|---|---|---|
| 2.0.0 client | 2.0.0 server | Yes | Yes |
| r11 client | 2.0.0 server | Yes (v4) | No |
| 2.0.0 client | r11 server | Yes (v4) | No |
| <= alpha2.1 | 2.0.0 server | No (HELLO v3) | No |

The TCP path continues writing HELLO v4. Only the Datagram mode writes v5.

## Build

```bash
./validate-kit.sh
./scripts/build-phase6-artifacts.sh
```

Expected outputs:

```text
dist/smp3-server-linux-amd64
dist/smp3-server-windows-amd64.exe
dist/mihomo-smp3-linux-amd64
dist/mihomo-smp3-windows-amd64.exe
dist/smp3-proxy-linux-amd64
dist/smp3-proxy-windows-amd64.exe
dist/SHA256SUMS
```

Expected version:

```text
1.14.0-beta.14-smp3-2.0.0 (sing client)
2.0.0 (standalone server)
```

`scripts/build-phase6-artifacts.sh` verifies the explicit Linux/amd64 and
Windows/amd64 targets, builds the standalone server, injects the pinned
sing-box and Mihomo sources, and builds all six formal release binaries.

## Source verification in this closeout

The kit contains mutually-exclusive `smp3core`, legacy semantic, and sing adapter/integration `Test*` functions. `validate-kit.sh` reports each current category and the total, and guards against losing the pre-Phase-1 test baseline; the count is intentionally not duplicated as a drifting literal in this document.

Completed in the artifact environment:

```text
SMP3 package go test: PASS
SMP3 package go test -race -count=5: PASS
SMP3 package go vet: PASS
gofmt: PASS
```

The release regression includes:

- TCP adaptive scheduler prefers the useful lower-latency leg;
- static scheduler remains deterministic/config-weight based;
- v4 TCP HELLO remains readable and v5 Datagram HELLO round-trips;
- duplicate UDP copies are delivered once;
- UDP never waits for a missing earlier datagram ID;
- adaptive UDP avoids a slow leg;
- adaptive small-packet duplication obeys its threshold;
- same-ID Datagram leg replacement succeeds;
- intentional Datagram session close is not reported as a path failure.

### Final 2.0.0 status

This archive is the **2.0.0 source package**. The pinned sing-box injection/build
produces Linux and Windows clients reporting `1.14.0-beta.14-smp3-2.0.0`, while
the standalone server reports `2.0.0`. The live TCP/UDP acceptance matrix is
recorded in `TEST_RESULTS.txt`.

The full generated-tree test command still has non-SMP3 environment/toolchain failures (namespace permission, current-Go runtime linkname compatibility, and internet-dependent TLS-fragment tests). Full-tree vet likewise reports only upstream unsafe-pointer diagnostics; the SMP3 multipath package gates pass. The H3 100 MiB case remains INCONCLUSIVE because direct aioquic reproduces the same failure without SMP3.

## Recommended 2.0.0 acceptance matrix

For future deployments, after building both endpoints, validate at least:

```text
TCP 500 MiB exact upload
TCP leg0 forced disconnect -> same-session rejoin
TCP leg1 forced disconnect -> same-session rejoin
TCP bootstrap with leg0 unavailable
TCP bootstrap with leg0 artificially slow
TCP adaptive throughput comparison: leg0 / leg1 / aggregate
UDP DNS/small datagram round trip
UDP stripe traffic on both legs
UDP duplicate: exactly-once delivery
UDP adaptive: slow one leg and verify traffic shifts
UDP same-ID leg reconnect
QUIC/HTTP3 or iperf UDP application test through the local SOCKS/TUN path
```

## Security

SMP3 HELLO authentication does not make raw SMP3 a public encrypted proxy protocol. Keep the aggregation listener private when possible and use encrypted child carriers.

Never publish live passwords, PSKs, TLS private keys, provider credentials, or concrete deployment configs.

See `SECURITY.md`, `RELEASE_NOTES.md`, `TEST_RESULTS.txt`, and `CHANGELOG.md` for details.
