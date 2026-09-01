# SMP3 2.1.1 Deployment and Usage

This guide deploys the independent SMP3 product and its 2.1.1 bidirectional
Stream activation bugfix release and carrier-agnostic
adapter release.
The server side does **not**
require sing-box: it uses `smp3-server`, the standalone host for the canonical
SMP3 Core. sing-box is only an optional compatibility client; Mihomo is the
other supported client integration.

## 1. Topology

```text
Applications
    ↓ SOCKS5 / mixed proxy
Mihomo custom core or optional sing-box client
    ↓ SMP3 adapter: two independent child outbounds
    ↓ reliable TCP-capable carriers (Snell / Hysteria2 / VLESS / Trojan / direct)
external carrier servers/terminators
    ↓ raw TCP carrier streams carrying SMP3 HELLO and frames
standalone SMP3 server (:24444)
    ↓
canonical SMP3 Core
    ↓
Internet destinations
```

Carrier servers/terminators are outside the standalone server. They may run on
the same host or on a separate relay host; they terminate the outer Snell,
Hysteria2, VLESS, or other carrier protocol and forward the resulting raw TCP
stream to `smp3-server`. The standalone server only authenticates and handles
SMP3 HELLO, Stream frames, and Datagram frames; it does not implement or listen
for any child carrier protocol.

The two client child outbounds must independently establish reliable TCP
streams to the same SMP3 listener. They share one SMP3 logical session, not one
underlying carrier connection. MP-UDP datagrams are encoded as SMP3 Datagram
frames over those child streams; the standalone server is not expected to
receive native UDP from the carrier. SMP3 does not require a specific proxy
protocol: each configured child must provide the reliable TCP dial capability
required by the adapter. IPv4/IPv6 selection and carrier reachability remain
responsibilities of the host outbound and external carrier deployment.

## 2. Download and verify

Download the assets from the [v2.1.1 Release](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.1.1):

- Server: `smp3-server-linux-amd64` or `smp3-server-windows-amd64.exe`.
- Mihomo client: `mihomo-smp3-linux-amd64` or `mihomo-smp3-windows-amd64.exe`.
- Optional sing-box compatibility client:
  `smp3-proxy-linux-amd64` or `smp3-proxy-windows-amd64.exe`.
- `SHA256SUMS`.

Verify before running a binary:

```bash
sha256sum -c SHA256SUMS
```

Do not publish or reuse the passwords in the example files. They are
placeholders only.

## 3. Deploy the standalone server

Copy `config/standalone-server.example.json` to a private deployment config:

```bash
cp config/standalone-server.example.json config/server.json
```

Edit at least:

- `listen`: the local/private bind address and port, normally `:24444` or a
  private landing address;
- `password`: one long random SMP3 password shared with the client;
- `stream` and `udp`: keep the defaults until the basic path works.

Keep the raw SMP3 listener private or firewall it to the carrier terminators.
Do not expose it as an unauthenticated public proxy port.

Validate and install on Linux:

```bash
./dist/smp3-server-linux-amd64 -c config/server.json -check
sudo ./scripts/install-smp3-server.sh --config config/server.json
sudo systemctl status smp3-standalone
sudo ss -ltnp | grep 24444
```

`scripts/install-smp3-server.sh` installs only
`/opt/smp3-standalone/smp3-server` and `smp3-standalone.service`; it does not
overwrite a legacy `smp3-proxy` installation. After installation, use:

```bash
sudo smp3ctl status
sudo smp3ctl logs
sudo smp3ctl restart
```

Run directly on Windows when a service wrapper is not desired:

```powershell
& .\dist\smp3-server-windows-amd64.exe -c .\config\server.json -check
& .\dist\smp3-server-windows-amd64.exe -c .\config\server.json
```

The server must be running before testing the client. The server config must
use the same `udp.mode`, `max_datagram_size`, `idle_timeout`, and duplication
policy as the client; these Datagram sub-policies are not negotiated by HELLO.

## 4. Configure a Mihomo client

Use `config/mihomo.example.yaml` as a complete starting point, or merge its
`proxies` and `proxy-groups` entries into an existing Mihomo configuration.
Replace every `YOUR_...` value with the real carrier/SMP3 deployment value.

The important custom proxy shape is:

```yaml
- name: MP-SMP3
  type: smp3
  server: YOUR_LANDING_PRIVATE_AGGREGATION_IP
  port: 24444
  password: YOUR_SMP3_PASSWORD
  legs:
    - proxy: line-path
    - proxy: public-hy2
  leg1-fallback: public-snell
  scheduler-mode: adaptive
  udp:
    enabled: true
    mode: adaptive
    max-datagram-size: 16384
    idle-timeout: 2m
```

Requirements:

- `legs` contains exactly two different existing child proxy names;
- `leg1-fallback`, when used, names a separate child proxy;
- both child carriers are reliable paths to the same SMP3 endpoint;
- `udp.enabled: true` is required for MP-UDP;
- use `adaptive` first; use `stripe` for unordered single-copy traffic and
  `duplicate` when application-level exactly-once delivery is more important
  than carrier bandwidth.

Start and validate a disposable Mihomo process:

```bash
./dist/mihomo-smp3-linux-amd64 -t -f config/mihomo.yaml
./dist/mihomo-smp3-linux-amd64 -f config/mihomo.yaml
```

To replace an existing Mihomo executable safely, download the installer and
provide the exact path. It verifies the stable Release `SHA256SUMS`, creates a
sibling `smp3-backup` directory, stops only that path, and refuses to continue
if a supervisor relaunches the selected core:

```powershell
Invoke-WebRequest https://raw.githubusercontent.com/Superbias/smp3-multipath-kit-public/main/scripts/install-mihomo-smp3.ps1 -OutFile install-mihomo-smp3.ps1
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe"
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe" -Check
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe" -Update
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe" -Restore
```

With no `-CorePath`, the Windows installer uses running `mihomo.exe` process
evidence and a small finite set of common paths. Multiple candidates fail
closed and require an explicit path; it never scans the whole `C:\` drive.

On Windows, run the corresponding `.exe`. Set applications to the configured
Mihomo mixed/SOCKS port, for example `127.0.0.1:7890`. For UDP, the application
must actually use SOCKS5 UDP ASSOCIATE; an HTTP-only client does not exercise
the MP-UDP path.

If using Clash Party, point it at the separately downloaded custom Mihomo
binary according to its custom-core mechanism. Do **not** overwrite
`Clash Party/resources/sidecar/mihomo.exe` unless you have an independent
backup and an explicit maintenance plan.

## 5. Use the optional sing-box compatibility client

`config/client-adaptive.example.json` is a sing-box-shaped client config. It
is not required by the standalone server. Copy it and replace the carrier,
SMP3 endpoint, and password placeholders:

```bash
cp config/client-adaptive.example.json config/client.json
./dist/smp3-proxy-linux-amd64 check -c config/client.json
./dist/smp3-proxy-linux-amd64 run -c config/client.json
```

The example exposes a local mixed inbound at `127.0.0.1:2080`. Configure an
application to use `socks5h://127.0.0.1:2080`. On Windows, use the `.exe` with
the same `check` and `run` subcommands.

## 6. First-use verification

Run checks in this order:

1. Server `-check` succeeds and the service owns the configured listen port.
2. The client config check succeeds and client logs show SMP3 leg0 bootstrap.
3. A TCP request through the local proxy succeeds, for example:

   ```bash
   curl --proxy socks5h://127.0.0.1:7890 https://example.com/
   ```

4. Use a SOCKS5 UDP-capable DNS or application client and confirm the server
   logs show a Datagram session.
5. For a long enough TCP flow, confirm the second leg joins. Stream activation
   measures application payload in both directions per logical session and
   uses the higher directional rate against `activation-threshold-mbps`; it is
   not an aggregate across connections. Very short or low-throughput flows may
   intentionally remain on one leg below the activation threshold.

A normal successful setup has one logical SMP3 session and two carrier legs;
leg repair replaces a carrier generation without rebuilding the logical
session. A terminal client-side UDP association can be recreated by the
Mihomo adapter while the application association remains open.

## 7. Datagram modes and limits

- `adaptive`: recommended default; schedules by observed path health and
  queue pressure.
- `stripe`: sends each datagram on one live leg. Delivery is unordered.
- `duplicate`: sends copies on both live legs and the Core delivers one copy;
  carrier bytes are intentionally higher.
- `max_datagram_size`: maximum application UDP payload is 16384 bytes.
  Larger datagrams are safely dropped rather than truncated.
- UDP is unreliable. A leg transition can lose a small number of datagrams;
  SMP3 does not add retransmission that would turn UDP into TCP.

Keep `mode`, `max_datagram_size`, `idle_timeout`, and duplication settings
aligned on both endpoints. Change one setting at a time and rerun a small
application smoke test.

## 8. Logs and operations

Linux server logs:

```bash
sudo journalctl -u smp3-standalone -f
sudo systemctl restart smp3-standalone
```

Look for server session creation, `leg joined/rejoined`, and datagram leg-down
messages. Do not interpret a single UDP timeout during a process replacement
as a protocol guarantee; validate steady state after the new listener is up.

For a Linux upgrade, preserve the current config, then let the installer fetch
the latest stable Release and verify its exact asset checksum. Use the
installed operations tool for status, logs, update, and rollback:

```bash
sudo smp3ctl check
sudo smp3ctl update
sudo smp3ctl rollback
```

The installer keeps at most five verified binary backup generations and does
not change config during a normal binary update. A manual rollback to a
separately preserved legacy service is:

```bash
sudo systemctl disable --now smp3-standalone
# restore the separately preserved legacy service only if your deployment uses it
sudo systemctl enable --now smp3-proxy
```

Verify ownership after either operation:

```bash
sudo ss -ltnp | grep 24444
```

## 9. Troubleshooting

| Symptom | Check |
|---|---|
| HELLO rejected | SMP3 password, endpoint, port, and carrier destination match. |
| Only TCP works | Client `udp.enabled`, app SOCKS5 UDP support, and server `udp.enabled`. |
| Second leg never appears | Confirm the flow is on SMP3 and that one logical Stream session sustains either upload or download above `activation-threshold-mbps` for `activation-window`; the threshold is directional-per-session, not aggregate. Then check child proxy names and carrier reachability. |
| Oversize UDP disappears | Expected for payloads above 16384 bytes; it must not be silently truncated. |
| Business pauses during a leg fault | Check carrier detection time and logs; small UDP loss is allowed, prolonged steady-state failure is not. |
| H3 100 MiB fails | The known aioquic/quic-go harness issue reproduces without SMP3 and is not an SMP3 diagnosis. |

## 10. Security checklist

- Generate a unique long SMP3 password per deployment.
- Keep example files and released configs placeholder-only.
- Restrict the standalone listener with firewall/network policy.
- Use encrypted child carriers for public paths.
- Never publish TLS private keys, Snell PSKs, Hy2 passwords, provider
  credentials, or real deployment configs.
