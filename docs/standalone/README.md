# SMP3 Standalone

Standalone provides a local application SOCKS5 ingress and a vendor-neutral
two-leg SMP3 client. Its positioning is **Universal External Proxy
Compatibility**.

## Data path

```text
Application
  -> smp3-client local SOCKS5
      -> Leg0 -> external SOCKS5 endpoint A -> SMP3 server :24445
      -> Leg1 -> external SOCKS5 endpoint B -> SMP3 server :24445
```

The two SOCKS5 endpoints may be owned by one external proxy process or by two
different processes. SMP3 does not require one proxy process per leg.

## Responsibility split

The user supplies:

- one SOCKS5 listener identity for Leg0;
- one SOCKS5 listener identity for Leg1;
- the SMP3 server route(s) and shared SMP3 password.

`smp3-client` supplies:

- local application SOCKS5 ingress;
- SMP3 multipath scheduling and aggregation;
- leg lifecycle, recovery, retransmission, and session handling;
- SMP3 TCP and UDP transport.

External proxy software supplies:

- VLESS, Reality, HY2, Snell, Trojan, Shadowsocks, WireGuard, or other node
  protocols;
- node credentials and TLS material;
- node selection and switching behind each SOCKS5 endpoint.

Changing Node A1 to Node A2 behind the same SOCKS5 listener does not require an
SMP3 configuration change; the endpoint identity remains the contract.

## Canonical configuration

Start from [the example](../../examples/smp3-client-config.example.json). The
important shape is:

```json
{
  "listen": "127.0.0.1:18080",
  "upstream_socks": {
    "address": "127.0.0.1:7898",
    "leg0": { "address": "127.0.0.1:10001" },
    "leg1": { "address": "127.0.0.1:10002" }
  },
  "smp3": {
    "password": "CHANGE_ME",
    "routes": {
      "leg0": "smp3-server.example.invalid:24445",
      "leg1": "smp3-server.example.invalid:24445"
    }
  }
}
```

The global `upstream_socks.address`, username/password, and timeout fields
remain supported for v2.3.4 compatibility. Per-leg overrides take precedence
for their own leg only. There is no automatic cross-leg SOCKS fallback.
`smp3.routes.leg1_fallback` is a route-level SMP3 target fallback, not a
fallback from Leg1's SOCKS endpoint to Leg0's endpoint.

## Build

```bash
./build.sh
```

This builds only `smp3-client` and `smp3-server` for Linux/amd64 and
Windows/amd64. It does not fetch or build Mihomo, sing-box, or `smp3-proxy`.

## Verification

```bash
./smp3-client-windows-amd64.exe -c config/smp3-client.json -check
```

The check output reports safe Leg0/Leg1 SOCKS endpoint addresses only. It never
prints upstream usernames or passwords.
