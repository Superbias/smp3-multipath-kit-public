[English](README.md) | [简体中文](README.zh-CN.md)
# SMP3 Multipath Kit alpha2.3-r10

> Experimental application-layer multipath transport for a fixed sing-box base.
>
> **Release:** `alpha2.3-r10`  
> **Wire protocol:** `SMP3` hello version `4`  
> **Pinned upstream:** sing-box `v1.14.0-beta.14`  
> **Pinned revision:** `4902660f8424fef3c2a60dfcdce7aeadfe3f3b88`  
> **Expected binary version:** `1.14.0-beta.14-smp3-alpha2.3-r10`

SMP3 Multipath Kit is a downstream experimental transport that carries one logical TCP byte stream over two independent child paths, reorders/reassembles the stream at the landing server, and keeps the logical connection alive across many single-path failures.

It is **not an official sing-box feature or SagerNet release**. The generated executable is named `smp3-proxy` to avoid implying otherwise.

---

## 1. What this release is for

The typical deployment has two independent paths between the client and one landing server:

```text
Application / Mihomo
        │
        │ SOCKS5 127.0.0.1:2080
        ▼
SMP3 client
        │
        ├── leg0: preferred line/private path
        │        e.g. dedicated line + Snell / private tunnel
        │
        └── leg1: public booster
                 Hy2 preferred -> Snell fallback (optional adaptive mode)
                         │
                         ▼
                 SMP3 landing listener
                         │
                         ▼
                      Internet
```

SMP3 aggregates **TCP**. UDP is not multipath-aggregated; it is sent through the configured `udp_outbound`.

### Recommended security boundary

The SMP3 listener authenticates its HELLO with HMAC, but SMP3 itself is not intended to be a public-facing encrypted proxy protocol. Keep the aggregation listener on a private/WireGuard/internal address and carry public-path traffic through an encrypted carrier such as Hysteria2 or Snell.

Example conceptual layout:

```text
Private line -----------> private SMP3 listener :24444
Public Hy2/Snell -------> encrypted carrier -----> private SMP3 listener :24444
```

Do **not** publish a raw SMP3 listener to the Internet unless you have independently protected the transport.

---

## 2. r10 highlights

`alpha2.3-r10` focuses on cumulative-ACK head-of-line repair and same-leg-ID transport replacement.

### ACK-paced single-frontier rescue

SMP3 v4 has cumulative ACK but no SACK. When an early sequence is delayed on a slow path, later frames may already be buffered by the receiver but cannot be cumulatively acknowledged.

R10 therefore repairs only the currently proven blocker:

```text
outstanding[ackedNext]
```

If that frontier is already overdue and an ACK advances `ackedNext`, R10 immediately checks the newly exposed frontier instead of waiting for the next periodic retransmit tick. It intentionally does **not** send a speculative fixed multi-frame burst.

### Same-ID transport replacement hardening

When a concrete transport generation for a leg fails, R10 invalidates stale outstanding ownership associated with that failed generation. A replacement transport using the same numeric leg ID can replay old unacknowledged DATA correctly.

Dead-leg replay candidates are also sorted by sequence so replay starts from the cumulative ACK frontier instead of Go map iteration order.

### No wire/config migration

R10 does not change:

- SMP3 wire/HELLO version (`4`)
- JSON configuration schema
- rescue queue size
- adaptive fallback thresholds

Upgrade both endpoints together for deterministic testing and support.

---

## 3. Current capabilities

- One logical TCP connection over two independent child outbounds.
- Preferred `leg0` plus on-demand `leg1` booster.
- Either authenticated leg may arrive first at the server and establish the logical core.
- Independent per-leg redial/rejoin.
- Bounded outstanding TX window.
- Cumulative ACK and ACK-driven retirement.
- Cross-leg retransmission of unacknowledged DATA.
- ACK-paced rescue of an overdue cumulative frontier.
- Receiver-side sequence deduplication/reordering.
- Per-leg ACK/control writers so one blocked path does not synchronously block control traffic on the other.
- Logical `CLOSE` frame and ACK-progress-based graceful tail drain.
- Completed-session tombstones to reject delayed old legs.
- Same-ID retirement barrier on the server.
- Optional `leg1` adaptive carrier: Hy2 preferred, Snell fallback.
- Adaptive decisions based on sustained logical-stream impact rather than raw UDP imperfection alone.
- UDP routed through `udp_outbound` instead of multipath aggregation.

---

## 4. Known limitations

This is still an experimental release. In particular:

1. **No SACK.** SMP3 v4 only has cumulative ACK. Rescue is deliberately conservative and repairs one proven frontier blocker at a time.
2. **Initial preferred-leg dial failure is not bootstrap failover.** If `leg0` fails during the initial `DialContext` before the logical connection is established, the current outbound does not automatically create that logical connection through leg1. Once a logical session exists, normal leg failure/rejoin recovery applies.
3. **TCP only for aggregation.** UDP does not use the SMP3 multipath data plane.
4. **Not a general-purpose public encrypted tunnel.** Use encrypted child carriers and keep the raw SMP3 aggregation listener private.
5. **Performance depends on path asymmetry, RTT, carrier congestion and configured queue/window values.** Benchmark numbers below are observations from one test environment, not throughput guarantees.

---

## 5. Verified r10 acceptance

### Source/unit verification

The release source has standalone SMP3 tests covering core, protocol and adaptive logic.

Verified for this release:

```text
65 standalone tests
Go test: PASS
Go race test (-count=5): PASS
Go vet: PASS
Gofmt: PASS
JSON examples: PASS
Shell/Python syntax: PASS
```

See `TEST_RESULTS.txt` and `BUILD_STATUS.md` for the detailed scope.

### Live 500 MiB tests

All live upload tests used an exact payload size of:

```text
524288000 bytes
```

and completed with:

```text
HTTP=200
UPLOADED=524288000
```

Observed scenarios:

| Scenario | Result | Observed result |
|---|---|---:|
| R9 reference baseline | PASS integrity, throughput limited | 456.219 s, 1.149 MB/s (~9.19 Mbps) |
| R10 normal multipath run | PASS | 37.503 s, 13.980 MB/s (~111.8 Mbps) |
| R10 controlled +300 ms preferred-path delay | PASS | 119.762 s, 4.378 MB/s (~35.0 Mbps) |
| R10 forced preferred-leg TCP destruction and same-ID rejoin | PASS | 22.665 s, 23.132 MB/s (~185 Mbps) |

The forced-rejoin run additionally verified:

- `leg0` was destroyed while hundreds of DATA frames were still outstanding;
- the same logical session stayed alive through `leg1`;
- the replacement `leg0` used a new TCP connection;
- the server logged `multipath leg 0 joined/rejoined session` rather than creating a new logical session;
- the client returned to `multipath leg 0 ready` and leg0 resumed data-plane participation;
- the full 500 MiB payload completed without application reconnect;
- no false Hy2 fallback was observed;
- no actual `tx_ack_stall` event was observed in the accepted runs.

These numbers are environment-specific and should only be used to compare behavior within the same test setup.

---

## 6. Release contents

A clean **source release** contains:

```text
README.md
README-zh_CN.md
RELEASE_NOTES.md
CHANGELOG.md
BUILD_STATUS.md
TEST_RESULTS.txt
SECURITY.md
NOTICE.md
VERSION
build.sh
validate-kit.sh
package-release.sh
install-server.sh
install-client.ps1
config/
patches/
scripts/
src/
dist/BUILD_REQUIRED.md
MANIFEST.sha256
```

No deployment `config.json`, private key, PSK, password, test log, `.git`, `.work`, build cache or local editor metadata is intended to be included.

If prebuilt binaries are absent, build them with `./build.sh`. A binary release produced by `package-release.sh` includes:

```text
dist/smp3-proxy-linux-amd64
dist/smp3-proxy-windows-amd64.exe
dist/SHA256SUMS
```

---

## 7. Requirements and build

Recommended environment: Debian/Ubuntu/WSL Linux filesystem.

Required bootstrap tools:

```text
git
python3
Go 1.21+
```

The pinned upstream currently requires Go `1.25.5`. `build.sh` sets:

```text
GOTOOLCHAIN=go1.25.5+auto
```

Network access is required for the pinned sing-box source and, when needed, the Go toolchain/module cache.

### Validate the source kit

```bash
mkdir -p "$HOME/go-tmp" "$HOME/go-build-cache"

GOTMPDIR="$HOME/go-tmp" \
GOCACHE="$HOME/go-build-cache" \
./validate-kit.sh
```

### Build Linux + Windows binaries

```bash
GOTMPDIR="$HOME/go-tmp" \
GOCACHE="$HOME/go-build-cache" \
./build.sh
```

Expected outputs:

```text
dist/smp3-proxy-linux-amd64
dist/smp3-proxy-windows-amd64.exe
dist/SHA256SUMS
```

Check version:

```bash
./dist/smp3-proxy-linux-amd64 version
```

PowerShell:

```powershell
.\dist\smp3-proxy-windows-amd64.exe version
```

Expected version string:

```text
1.14.0-beta.14-smp3-alpha2.3-r10
```

---

## 8. Generate deployment secrets

Generate fresh secrets for every deployment:

```bash
./scripts/gen-secrets.sh
```

It prints placeholders for values such as:

```text
SMP3_PASSWORD=...
PUBLIC_SNELL_PSK=...
```

Generate the Hysteria2 password separately with a cryptographically secure random generator.

Never commit or publish:

- SMP3 password
- Snell PSK
- Hysteria2 password
- TLS private key
- real deployment configuration
- provider credentials/API keys

All JSON files shipped under `config/*.example.json` use placeholder values and must be copied before editing.

---

## 9. Configuration model

### Recommended adaptive client

Start from:

```text
config/client-adaptive.example.json
```

Important fields:

```json
{
  "type": "multipath",
  "outbounds": ["line-path", "public-hy2"],
  "preferred": "line-path",
  "udp_outbound": "line-path",
  "leg1_fallback": "public-snell",
  "endpoints": [
    { "server": "YOUR_LANDING_PRIVATE_AGGREGATION_IP", "server_port": 24444 },
    { "server": "YOUR_LANDING_PRIVATE_AGGREGATION_IP", "server_port": 24444 }
  ],
  "password": "YOUR_SMP3_PASSWORD"
}
```

The two `endpoints` correspond to the two child paths. They may resolve to the same private aggregation listener while the child outbound determines how each connection reaches it.

### Server

Start from:

```text
config/server-hy2-snell.example.json
```

The server example exposes encrypted public carriers while binding SMP3 itself to:

```text
YOUR_LANDING_PRIVATE_AGGREGATION_IP:24444
```

Replace all `YOUR_*` / `CHANGE_*` fields before deployment.

### Validate a concrete config

Linux:

```bash
./dist/smp3-proxy-linux-amd64 check -c /path/to/config.json
```

Windows:

```powershell
.\dist\smp3-proxy-windows-amd64.exe check -c .\config\client.json
```

---

## 10. Linux landing-server installation

Copy an example first; do not edit the shipped example in place:

```bash
cp config/server-hy2-snell.example.json config/server.json
```

Replace all placeholder addresses/passwords/certificate paths, then validate it.

To install the binary and systemd unit:

```bash
sudo ./install-server.sh ./config/server.json
```

The installer writes:

```text
/usr/local/bin/smp3-proxy
/etc/smp3-proxy/config.json
/etc/systemd/system/smp3-proxy.service
```

Check status:

```bash
systemctl status smp3-proxy --no-pager
```

Follow logs:

```bash
journalctl -u smp3-proxy -f --no-pager
```

Useful filtered view:

```bash
journalctl -u smp3-proxy -f --no-pager | \
grep -Ei 'multipath|session|leg 0|leg 1|join|rejoin|closed|failed|error'
```

---

## 11. Windows client installation

Copy the adaptive example:

```powershell
Copy-Item .\config\client-adaptive.example.json .\config\client.json
```

Edit `client.json` and replace all placeholders.

Validate:

```powershell
.\dist\smp3-proxy-windows-amd64.exe check -c .\config\client.json
```

Install/update the scheduled task:

```powershell
PowerShell -ExecutionPolicy Bypass -File .\install-client.ps1
```

Default local SOCKS/mixed listener from the example:

```text
127.0.0.1:2080
```

Mihomo can then use the supplied `config/mihomo-snippet.yaml` as a local SOCKS5 proxy entry.

Test the local listener:

```powershell
Test-NetConnection 127.0.0.1 -Port 2080
```

---

## 12. Operational verification

### Client health log

For detailed validation, temporarily use debug logging and filter the client log for:

```text
mp health
frontier_rescues
ack_frontier_leg
ack_frontier_multi
ack_frontier_age
ack_progress_age
tx_outstanding
tx_goodput
tx_ack_stall
fallback
leg 0 down
leg 0 ready
```

Interpretation:

- `tx_outstanding`: DATA still waiting for cumulative ACK.
- `frontier_rescues`: number of alternate-leg rescue attempts for the cumulative frontier.
- `ack_frontier_leg`: most recent concrete leg attribution for the current ACK blocker.
- `ack_frontier_multi=true`: the current blocker has attempts on multiple legs; adaptive logic should not assign exclusive blame to one carrier.
- low/repeated `ack_progress_age`: cumulative ACK is continuing to advance.

### Server rejoin evidence

A successful same-session transport repair should look like:

```text
multipath server leg 0 down ...
inbound connection from ...
multipath leg 0 joined/rejoined session ...
```

A repaired leg should **join/rejoin the existing session**, not create a new destination session for the same still-active logical flow.

---

## 13. Adaptive Hy2 -> Snell behavior

`client-adaptive.example.json` configures Hy2 as the preferred public `leg1` carrier and Snell as fallback.

The controller is deliberately conservative. Normal UDP packet loss, jitter, QUIC retransmission, or short-term rate variation alone do not cause fallback. It looks for sustained **logical-stream impact**, including relevant combinations of:

- logical RX/TX goodput degradation;
- cumulative ACK progress stall;
- outstanding-frame pressure;
- reorder/pending pressure;
- leg1 useful contribution degradation;
- persistence through the configured SUSPECT window.

Default example values include:

```text
evaluation interval:       1s
warmup:                    5s
suspect window:            8s
hard failure:              2 events / 15s
cooldown:                  90s
max cooldown:              5m
recovery stable window:    20s
goodput degradation ratio: 0.4
```

During cooldown, new logical connections use Snell. When cooldown expires, recovery uses a limited Hy2 probation canary with real useful traffic before global Hy2 health is restored.

See `CHANGELOG.md` for the detailed adaptive evolution and regression fixes.

---

## 14. Upgrade compatibility

Wire compatibility:

```text
alpha2 / alpha2.1: hello version 3
alpha2.2 / alpha2.3: hello version 4
```

Therefore:

- alpha2.3-r10 is wire-compatible with alpha2.2 at the SMP3 HELLO level;
- alpha2/alpha2.1 are not wire-compatible with alpha2.2/alpha2.3;
- for operational support, run matching r10 client/server binaries whenever possible.

R10 itself does not require a JSON schema migration from earlier alpha2.3 revisions.

---

## 15. Reproducibility and release integrity

The build script:

1. checks out the exact pinned upstream revision;
2. injects the source under `src/`;
3. formats/tests the injected package;
4. builds Linux/amd64 and Windows/amd64;
5. emits binary SHA256 checksums.

The release package also includes `MANIFEST.sha256` for its source/release files.

Verify a source release after extraction:

```bash
sha256sum -c MANIFEST.sha256
```

If binaries are present:

```bash
cd dist
sha256sum -c SHA256SUMS
```

---

## 16. Clean-release policy

The release packaging script uses a whitelist/staging directory and explicitly excludes common local/deployment artifacts.

A public release must not contain:

```text
.git/
.work/
.release-stage/
__pycache__/
*.pyc
*.pyo
config.json
client.json
server.json
*.log
private keys
real credentials
local test uploads
```

Example JSON secret fields are checked to ensure they remain `YOUR_*` / `CHANGE_*` placeholders.

---

## 17. Troubleshooting

### `missing/non-executable final Linux binary`

You are running binary packaging before building. Run:

```bash
./build.sh
```

or use the source release as-is.

### Build cannot fetch sing-box/toolchain

`build.sh` requires network access to the pinned upstream revision and possibly the pinned Go toolchain/modules. Build in an environment with working DNS/HTTPS access.

### Client SOCKS port is not listening

Run the client interactively with the concrete config and inspect startup errors:

```powershell
.\dist\smp3-proxy-windows-amd64.exe run -c .\config\client.json
```

### Only one leg appears

Check both the client and server logs. The server may receive leg0/leg1 in either order; arrival order does not define scheduling role. Confirm the child outbound itself can reach the aggregation endpoint.

### Large upload stalls with `tx_outstanding=1024`

Inspect `ack_progress_age`, `ack_frontier_age`, `ack_frontier_leg`, `ack_frontier_multi` and `frontier_rescues`. R10 should ACK-pace repair of an already-overdue cumulative blocker, while avoiding speculative rescue bursts beyond `ackedNext`.

---

## 18. Development notes

Important r10 implementation files:

```text
src/protocol/multipath/core.go
src/protocol/multipath/core_test.go
src/protocol/multipath/adaptive.go
src/protocol/multipath/adaptive_test.go
src/protocol/multipath/outbound.go
src/protocol/multipath/inbound.go
src/protocol/multipath/protocol.go
src/option/multipath.go
```

Historical diffs are retained under `patches/` for traceability. `patches/alpha2.3-r9-to-r10.diff` is the focused r9 -> r10 change set; `patches/alpha2.2-to-alpha2.3.diff` is the cumulative alpha2.3 patch history artifact.

---

## 19. License / attribution

See `NOTICE.md`.

Upstream project: sing-box / SagerNet. This kit is an experimental downstream derivative and is not affiliated with or endorsed by SagerNet.
