# SMP3 v2.4.0 Standalone SOCKS5 Sidecar

The Standalone Sidecar gives ordinary applications a local SOCKS5 endpoint. It
implements SMP3 only; VLESS, Reality, Hysteria2, Snell, and other outer
protocols are implemented and configured by Mihomo/Carrier.

```text
application
  -> 127.0.0.1:18080 (Sidecar SOCKS5)
  -> Carrier-A / Carrier-B SOCKS5 endpoints
  -> SMP3 server sidecar listener
```

## Configure

Download `smp3-client-windows-amd64.exe` or the Linux artifact and copy
`examples/smp3-client-config.example.json`. Replace every placeholder:

- `listen`: normally `127.0.0.1:18080`;
- `upstream_socks.address`: the default Carrier/host SOCKS5 endpoint;
- `upstream_socks.leg0` and `leg1`: optional per-leg Carrier endpoints;
- `smp3.routes.leg0` and `leg1`: routes to the SMP3 server;
- `smp3.password`: the server's SMP3 password.

For the qualified dual-Carrier shape:

```json
{
  "upstream_socks": {
    "address": "127.0.0.1:17898",
    "leg1": { "address": "127.0.0.1:17899" }
  },
  "smp3": {
    "routes": {
      "leg0": "SERVER_IP:24445",
      "leg1": "SERVER_IP:24445"
    }
  }
}
```

Both legs may target the same SMP3 sidecar listener, but they must use two
independent Carrier connections. The address and password above are examples.

## Check and run

```bash
./smp3-client-linux-amd64 -c ./config/smp3-client.json -check
./smp3-client-linux-amd64 -c ./config/smp3-client.json
```

On Windows use the `.exe` with the same arguments. Point applications to:

```text
SOCKS5 127.0.0.1:18080
```

Start Carrier-A/B before the Sidecar. The Sidecar does not start external
Carrier processes.

## Leg behavior

- Leg0 is normally the preferred/primary leg.
- Leg1 joins after the activation condition is met.
- A short or low-rate flow may legitimately leave Leg1 inactive.
- `connect_timeout` bounds the complete SOCKS5 CONNECT transaction.
- `carrier_ready_timeout` bounds the wait for remote SMP3 readiness after
  SOCKS CONNECT succeeds.

The Sidecar uses Stream HELLO v4 over TCP and Datagram HELLO v5 for UDP. It does
not modify Mihomo, Clash Party, Carrier, the server, firewall rules, or routes.

## Use it with stock Mihomo

Start the Sidecar first, then configure stock Mihomo to use
`127.0.0.1:18080` as a SOCKS5 proxy. See
`examples/mihomo-sidecar.example.yaml`; put the explicit Sidecar route before
broad rules to avoid a proxy loop.

## Security and troubleshooting

- The Sidecar is loopback-only and is not a public SOCKS5 server.
- Outer node settings belong in Carrier Mihomo, not in the Sidecar config.
- `-check` validates local config only; it does not connect to production.
- If both legs fail, check Carrier SOCKS5, routes, password, and the server
  listener first.
- Never use production passwords, PSKs, or Reality private keys in local tests.
