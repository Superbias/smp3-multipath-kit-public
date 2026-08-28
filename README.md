# SMP3 Multipath Kit alpha2.3-r11

[English](README.md) | [简体中文](README-zh_CN.md)

Experimental application-layer multipath transport for a pinned sing-box base.

- **Revision:** `alpha2.3-r11`
- **Pinned sing-box:** `v1.14.0-beta.14`
- **Pinned commit:** `4902660f8424fef3c2a60dfcdce7aeadfe3f3b88`
- **Expected binary:** `1.14.0-beta.14-smp3-alpha2.3-r11`
- **TCP stream HELLO:** v4 (compatible with r10 stream mode)
- **UDP datagram HELLO:** v5 (r11 endpoints required)

> This is an experimental downstream derivative, not an official SagerNet/sing-box release.

## What r11 adds

R11 keeps r10's ACK-paced frontier rescue and same-ID recovery, then adds three major capabilities:

1. **Adaptive TCP scheduler** — per-leg useful ACK/write throughput and write latency reshape static bandwidth weights so slow paths receive fewer early sequence numbers and cause less HOL pressure.
2. **Bootstrap failover** — leg0 gets a configurable head start; if it fails or is still pending after `bootstrap_fallback_delay`, leg1 is dialed in parallel and the first authenticated HELLO establishes the logical session.
3. **MP-UDP Datagram Mode** — UDP can use both child paths instead of one `udp_outbound`, with `stripe`, `duplicate`, and `adaptive` policies.

## Data planes

```text
                         SMP3 r11
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

R11 retains the r10 stream wire protocol (HELLO v4), including:

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

R11 introduces a separate v5 datagram HELLO and `DATAGRAM` frame. UDP does **not** reuse TCP cumulative ACK or ordered reassembly.

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
- Existing `leg1_adaptive`: optional Hy2 -> Snell carrier fallback based on sustained logical-stream health. R11 keeps this mechanism.

These layers are intentionally separate so carrier replacement and path load balancing do not become one opaque state machine.

### Review hardening in the current closeout

The pre-release review found and fixed several boundary cases before live acceptance:

- replicated UDP datagrams that fall behind the dedup window are rejected as stale duplicates, while pure `stripe` datagrams remain fully unordered and are not discarded merely for arriving very late;
- UDP-only traffic now participates in the existing Hy2 -> Snell carrier health/cooldown path on hard carrier failure; a probation Hy2 carrier is not marked recovered by Dial/HELLO alone and requires real useful UDP payload, while an unused probation slot is released on close;
- bootstrap leg1 retries Snell immediately when its selected Hy2 carrier fails, so `leg0 down + Hy2 down + Snell healthy` can still establish the logical session;
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

## Server configuration

The multipath inbound must also enable the Datagram data plane:

```json
{
  "type": "multipath",
  "listen": "YOUR_LANDING_PRIVATE_AGGREGATION_IP",
  "listen_port": 24444,
  "password": "CHANGE_TO_THE_SAME_LONG_RANDOM_SECRET",
  "scheduler_mode": "adaptive",
  "udp_multipath": {
    "enabled": true,
    "mode": "adaptive",
    "queue_frames": 256,
    "max_datagram_size": 16384,
    "dedup_window": 4096,
    "idle_timeout": "2m",
    "adaptive_queue_delay": "120ms",
    "adaptive_duplicate_threshold": 0
  }
}
```

Keep the raw aggregation listener private/internal and use encrypted carriers for public paths.

## Compatibility

| Client | Server | TCP | MP-UDP |
|---|---|---|---|
| r11 | r11 | Yes | Yes |
| r11 | r10 | Yes (v4) | No |
| r10 | r11 | Yes (v4) | Legacy client UDP only |
| <= alpha2.1 | r11 | No (HELLO v3) | No |

R11 deliberately continues writing HELLO v4 for TCP. Only the new Datagram mode writes v5.

## Build

```bash
./validate-kit.sh
./build.sh
```

Expected outputs:

```text
dist/smp3-proxy-linux-amd64
dist/smp3-proxy-windows-amd64.exe
dist/SHA256SUMS
```

Expected version:

```text
1.14.0-beta.14-smp3-alpha2.3-r11
```

`build.sh` checks out the exact pinned sing-box commit, injects the SMP3 source, runs the injected package tests, then builds Linux/amd64 and Windows/amd64.

## Source verification in this closeout

The kit contains **101 standalone multipath `Test*` functions** and the injected multipath package contains the same 101. The generated full sing-box source tree contains 441 `Test*` functions.

Completed in the artifact environment:

```text
SMP3 package go test: PASS
SMP3 package go test -race -count=5: PASS
SMP3 package go vet: PASS
gofmt: PASS
```

R11-specific tests include:

- TCP adaptive scheduler prefers the useful lower-latency leg;
- static scheduler remains deterministic/config-weight based;
- v4 TCP HELLO remains readable and v5 Datagram HELLO round-trips;
- duplicate UDP copies are delivered once;
- UDP never waits for a missing earlier datagram ID;
- adaptive UDP avoids a slow leg;
- adaptive small-packet duplication obeys its threshold;
- same-ID Datagram leg replacement succeeds;
- intentional Datagram session close is not reported as a path failure.

### Final closeout status

This archive is the **r11 closeout source package**. The pinned sing-box injection/build produced Linux and Windows binaries reporting `1.14.0-beta.14-smp3-alpha2.3-r11`, and the live TCP/UDP acceptance matrix is recorded in `TEST_RESULTS.txt`.

The full generated-tree test command still has non-SMP3 environment/toolchain failures (namespace permission, current-Go runtime linkname compatibility, and internet-dependent TLS-fragment tests). Full-tree vet likewise reports only upstream unsafe-pointer diagnostics; the SMP3 multipath package gates pass. The H3 100 MiB case remains INCONCLUSIVE because direct aioquic reproduces the same failure without SMP3.

## Recommended r11 acceptance matrix

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
