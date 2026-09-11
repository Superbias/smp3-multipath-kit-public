# SMP3 v2.3.1 Deployment and Usage

This is the short operational guide for the current release. Keep production
passwords, PSKs, Reality keys, subscriptions, and real node configs outside
the repository.

## 1. Understand the components

```text
Native:     application -> Mihomo / Clash Party -> SMP3 server
Standalone: application -> 127.0.0.1:18080 -> smp3-client
            -> Carrier-A / Carrier-B -> SMP3 server
Panel:      browser -> 127.0.0.1:24600 -> telemetry 127.0.0.1:24500
```

`smp3-server` handles SMP3 only. `smp3-client` handles SMP3 and local SOCKS5
only; it does not implement VLESS, Reality, Hysteria2, Snell, or another
carrier protocol. Mihomo/Carrier owns the node definitions and outer-protocol
dialing.

## 2. Download and verify

Download the v2.3.1 documentation release and its unchanged v2.3.0 product
artifacts from the
[GitHub Release](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.3.1):

| Asset | Purpose |
| --- | --- |
| `smp3-server-*` | Standalone server |
| `smp3-client-*` | Standalone local SOCKS5 |
| `mihomo-smp3-*` | Native / Clash Party |
| `smp3-panel-*` | R15 Panel |
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

To enable the R15 Panel, include at least:

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
- the upstream SOCKS5 address;
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

### 5.3 Leg behavior

- Leg0 is normally the preferred/primary leg.
- Leg1 joins after the configured activation condition is met.
- A short or low-rate request may legitimately leave Leg1 inactive.
- Do not lower thresholds, change windows, or manually dial a leg for a normal
  production check.

## 6. Lifecycle management

Use the existing service manager, scheduled tasks, or supervisor to manage
Carrier-A, Carrier-B, `smp3-client`, and Panel together:

```text
start: Carrier-A -> Carrier-B -> smp3-client -> Panel
stop:  Panel -> smp3-client -> Carrier-B -> Carrier-A
```

The client itself manages only its own SMP3 process. It does not create,
start, or replace Carrier processes.

## 7. R15 Panel

Run the read-only monitor against loopback telemetry:

```bash
./smp3-panel-linux-amd64 \
  -listen 127.0.0.1:24600 \
  -telemetry http://127.0.0.1:24500 \
  -history monitor-history.jsonl
```

Open `http://127.0.0.1:24600/`. The read-only endpoints are:

```text
GET /api/monitor/status
GET /api/monitor/history
GET /api/monitor/events
GET /api/monitor/stream   # SSE
```

The UI shows leg and Carrier state, TX rate, Useful ACK rate, traffic share,
events, health, and bounded history. It does not expose raw session IDs,
destinations, payloads, passwords, or private keys.

## 8. First-use checks

1. Server `-check` succeeds.
2. Both Carrier endpoints are listening.
3. Client `-check` succeeds and `18080` is listening.
4. A normal TCP request succeeds through the local SOCKS5 endpoint.
5. Panel status, history, events, and SSE are readable.
6. Observe Leg0 first; observe Leg1 only during a sufficiently long flow.

Linux:

```bash
ss -ltnp | grep -E '17898|17899|18080|24500|24600'
```

Windows PowerShell:

```powershell
Get-NetTCPConnection -State Listen -LocalPort 17898,17899,18080,24500,24600
```

## 9. Troubleshooting

| Symptom | First checks |
| --- | --- |
| Both legs fail | Carrier SOCKS5, routes, server address, and password |
| Leg1 stays down | Flow rate/duration, activation condition, and Carrier-B listener |
| Client appears to need VLESS/Reality | Those are dialed by Mihomo/Carrier, not the client |
| Only TCP works | Application SOCKS5 UDP support and UDP settings on both sides |
| Panel has no data | Telemetry `127.0.0.1:24500` and Panel telemetry URL |
| SSE does not update | `/api/monitor/stream` and telemetry logs |

## 10. Security

- Use a unique long SMP3 password per deployment.
- Keep the raw SMP3 listener private.
- Use encrypted Carrier paths for public traffic.
- Never commit passwords, PSKs, Reality keys, subscription URLs, API keys, or
  real deployment configs.
- Verify `SHA256SUMS` and keep a rollback copy before upgrades.
