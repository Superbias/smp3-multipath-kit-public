# SMP3 v2.6.2 Deployment and Usage

> **v2.7.0 recommended tutorial:** [Existing proxy paths → encrypted Landing proxy → local SMP3](docs/native/PROXY_CONVERGENCE.md). Includes a sanitized Native configuration and verification/rollback steps. The v2.6.2 steps below are historical.


## Preferred deployment topology

Use existing proxy nodes to carry independent SMP3 legs to one Landing SMP3 server. Deploy `smp3-server` on the Landing; a front machine only needs its existing proxy service and a working outbound route to that server.

```text
Application -> SMP3 client (Native Mihomo or Standalone)
              |-- Leg 0 -> front-machine proxy node
              |             -> WireGuard / IP routing / another forwarding path --+
              |-- Leg 1 -> proxy node on the Landing -----------------------------+-> Landing SMP3 listener
              `-- Leg N -> another independently selected proxy path -------------+        -> application_target
```

**Realm and `smp3-front` are optional. Front machines do not need to run SMP3.** In the WireGuard example, only the front machine and Landing are WireGuard peers; the client connects to the front proxy node normally and does not need to join WireGuard. The front proxy's outbound routing sends the connection to the Landing SMP3 listener through WireGuard. The other leg reaches that listener through a proxy node on the Landing.

- **Native Mihomo:** `legs[].proxy` selects the existing proxy node for each leg. The common `server` and `port` identify the Landing SMP3 endpoint to dial through those nodes, not the front proxy's listening port.
- **Standalone:** each `upstream_socks.legs[]` selects a local SOCKS5 endpoint supplied by external proxy software; the corresponding `smp3.routes.legs[].address` is the SMP3 endpoint reached through it. Node credentials and the remote front-to-Landing route belong to that proxy software and the operating system.
- Choose an SMP3 endpoint reachable from every selected proxy exit. A Landing WireGuard IP can work if both the front proxy and Landing-local proxy can reach it; a public or routed private address can also work. `127.0.0.1` refers to the host making the final dial, so it is unsuitable as a common endpoint when one exit is on the front machine.
- The client creates the carriers with the same authenticated Session ID and distinct Leg IDs. Client and Landing Core instances perform scheduling, ACK handling, reassembly and recovery. Arbitrary unrelated connections reaching the listener are not automatically combined.
- Preserve the SMP3 byte stream and its configured end-to-end authentication/TLS. `application_target` is the application destination after SMP3 terminates, not the SMP3 listener or the front node address. Fixed-target destination checks still apply.

SMP3 does not configure WireGuard, routes or remote proxy egress. This topology describes the supported carrier abstraction, not a claim that a particular WireGuard/WAN deployment has passed qualification. Shared bottlenecks can limit aggregation gains. Realm TCP forwarding remains an alternative when a transparent forwarding endpoint is useful.


> The remaining v2.6.2 instructions below are historical. Use the [current README](README.md) for v2.7.0 configuration.


This is the short operational guide for the current release. Keep production
passwords, PSKs, Reality keys, subscriptions, and real node configs outside
the repository.

## 1. Understand the components

```text
Native:     application -> Mihomo / Clash Party -> SMP3 server
Standalone: application -> 127.0.0.1:18080 -> smp3-client
            -> Carrier-A / Carrier-B -> SMP3 server
Dashboard:  browser -> SMP3 server -> telemetry 127.0.0.1:24500
```

`smp3-server` handles SMP3 only. `smp3-client` handles SMP3 and local SOCKS5
only; it does not implement VLESS, Reality, Hysteria2, Snell, or another
carrier protocol. Mihomo/Carrier owns the node definitions and outer-protocol
dialing.

## 2. Download and verify

Download the v2.6.2 assets from the
[GitHub Release](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.6.2):

| Asset | Purpose |
| --- | --- |
| `smp3-server-*` | Standalone server |
| `smp3-client-*` | Standalone local SOCKS5 |
| `mihomo-smp3-*` | Native / Clash Party |
| `smp3-proxy-*` | sing-box compatibility artifact |
| `smp3-android-standalone-2.6.2-debug.apk` | Android ARM64 Standalone |
| `SHA256SUMS` | Integrity manifest |

```bash
sha256sum -c SHA256SUMS
```

Run an asset only after its hash matches the manifest.

## 3. Deploy the server

```bash
cp config/standalone-server.example.json config/server.json
```

Set the private listen address, a long random SMP3 password, and the required
sidecar/telemetry settings. Check and run it:

To enable the integrated Dashboard and telemetry, include at least:

```json
"telemetry": {
  "enabled": true,
  "listen": "127.0.0.1:24500"
}
```

Telemetry must remain loopback-only.

```bash
./smp3-server-linux-amd64 -c config/server.json -check
./smp3-server-linux-amd64 -c config/server.json
```

For a Linux service installation:

```bash
sudo ./scripts/install-smp3-server.sh --config ./config/server.json
sudo smp3ctl status
sudo smp3ctl logs -f
```

The standalone server uses its own SMP3 config; it is not a sing-box server.
Keep the raw SMP3 listener private or restricted to the Carrier terminators.

## 4. Native / Clash Party

Native mode does not require `smp3-client` or sing-box:

1. Download the `mihomo-smp3` artifact.
2. Start from [config/mihomo.example.yaml](config/mihomo.example.yaml).
3. Configure two different child outbounds and one `type: smp3` proxy.
4. Select the binary as Clash Party's custom core.
5. Point applications to the Mihomo mixed/SOCKS5 port, for example
   `127.0.0.1:7890`.

Mihomo is responsible for VLESS, Reality, Hysteria2, Snell, and other outer
protocols. Keep a backup before replacing a custom core.

## 5. Standalone SOCKS5

### 5.1 Start the Carriers

Standalone does not start external Carrier processes. The qualified local
shape is:

```text
Carrier-A -> 127.0.0.1:17898 -> Leg0
Carrier-B -> 127.0.0.1:17899 -> Leg1
```

Carrier node parameters belong in the Carrier Mihomo configuration, not in
`smp3-client`.

### 5.2 Configure and run the client

Copy [examples/smp3-client-config.example.json](examples/smp3-client-config.example.json)
to a private config and set:

- `listen`, normally `127.0.0.1:18080`;
- `upstream_socks.leg0`, the SOCKS5 listener identity for Leg0;
- `upstream_socks.leg1`, the SOCKS5 listener identity for Leg1;
- the SMP3 password;
- `smp3.routes.leg0` and `leg1`;
- optional `leg1_fallback`.

```bash
./smp3-client-linux-amd64 -c ./config/smp3-client.json -check
./smp3-client-linux-amd64 -c ./config/smp3-client.json
```

On Windows use the `.exe` with the same arguments. Point applications to:

```text
socks5://127.0.0.1:18080
```

The legacy global `upstream_socks.address`, username/password, and timeout
fields remain supported. A missing per-leg override uses that global endpoint;
when an override is present it applies only to its own leg. The client does not
switch Leg1 to Leg0's SOCKS endpoint automatically.

See [the Standalone external proxy contract](docs/standalone/EXTERNAL_PROXY.md)
for the one-process/two-listener model and vendor-neutral responsibility split.

## 6. Android Standalone

Install `smp3-android-standalone-2.6.2-debug.apk` on an `arm64-v8a` Android
device. In the external proxy core, expose two local SOCKS5 listeners (for
example `127.0.0.1:20001` and `127.0.0.1:20002`) backed by the desired nodes.
The APK does not implement VLESS, Reality, Hysteria2, Snell, or other node
protocols; those remain in the external proxy core.

In the app, enter:

- local SOCKS: normally `127.0.0.1:18080`;
- the reachable SMP3 server sidecar endpoint;
- the same SMP3 password as the server;
- Carrier-A and Carrier-B listener host/ports.

Tap **Save**, then **Start**. Configure applications to use the app's
`127.0.0.1:18080` SOCKS5 endpoint. Stop the app before changing its runtime
configuration. See [the Android guide](docs/android/README.md) for lifecycle,
logs, and troubleshooting details.

### 6.1 Leg behavior

- Leg0 is normally the preferred/primary leg.
- Leg1 joins after the configured activation condition is met.
- A short or low-rate request may legitimately leave Leg1 inactive.
- Do not lower thresholds, change windows, or manually dial a leg for a normal
  production check.

## 7. Lifecycle management

Use the existing service manager, scheduled tasks, or supervisor to manage
Carrier-A, Carrier-B, and `smp3-client` together:

```text
start: Carrier-A -> Carrier-B -> smp3-client
stop:  smp3-client -> Carrier-B -> Carrier-A
```

The client itself manages only its own SMP3 process. It does not create,
start, or replace Carrier processes.

## 8. Integrated Server Dashboard

The Dashboard is served by `smp3-server`; do not start the retired standalone
`smp3-panel` process or bind port `24600`. The read-only API surface is:

```text
GET /api/v1/status
GET /api/v1/legs
GET /api/v1/sessions
GET /api/v1/traffic
GET /api/v1/traffic/history
GET /api/v1/events       # SSE
```

The UI shows leg and Carrier state, TX rate, Useful ACK rate, cumulative
Carrier/Useful traffic, Native/Standalone breakdown, events, health, and
bounded history. It does not expose raw session IDs, destinations, payloads,
passwords, or private keys.

## 9. First-use checks

1. Server `-check` succeeds.
2. Both Carrier endpoints are listening.
3. Client `-check` succeeds and `18080` is listening.
4. A normal TCP request succeeds through the local SOCKS5 endpoint.
5. Dashboard status, traffic, history, events, and SSE are readable.
6. Observe Leg0 first; observe Leg1 only during a sufficiently long flow.

Linux:

```bash
ss -ltnp | grep -E '17898|17899|18080|24500'
```

Windows PowerShell:

```powershell
Get-NetTCPConnection -State Listen -LocalPort 17898,17899,18080,24500
```

## 10. Troubleshooting

| Symptom | First checks |
| --- | --- |
| Both legs fail | Carrier SOCKS5, routes, server address, and password |
| Leg1 stays down | Flow rate/duration, activation condition, and Carrier-B listener |
| Client appears to need VLESS/Reality | Those are dialed by Mihomo/Carrier, not the client |
| Only TCP works | Application SOCKS5 UDP support and UDP settings on both sides |
| Dashboard has no data | Telemetry `127.0.0.1:24500` and server logs |
| SSE does not update | `/api/v1/events` and telemetry logs |

## 11. Security

- Use a unique long SMP3 password per deployment.
- Keep the raw SMP3 listener private.
- Use encrypted Carrier paths for public traffic.
- Never commit passwords, PSKs, Reality keys, subscription URLs, API keys, or
  real deployment configs.
- Verify `SHA256SUMS` and keep a rollback copy before upgrades.
