# SMP3 Standalone SOCKS5 Sidecar (development)

This document describes the standalone sidecar client in the
`feature/standalone-sidecar-client` branch. It is not a published release
artifact yet.

The sidecar is a small local SOCKS5 server. It uses the host's existing
SOCKS5 endpoint only for outbound TCP `CONNECT` to each configured SMP3 route;
it does not use upstream SOCKS UDP ASSOCIATE. Each route must lead, through its
carrier, to the same standalone SMP3 listener and must carry a reliable TCP
stream.

Sidecar routes must target the standalone server's explicit
`sidecar_listeners`. Those listeners send an authenticated `SMP3RDY1` host
readiness record only after the canonical HELLO has been parsed, authenticated,
and admitted. The legacy `listen`/`listeners` endpoints keep their existing
canonical-only behavior and do not send READY bytes.

```text
application
    -> local sidecar SOCKS5 (127.0.0.1:18080)
    -> host SOCKS5 CONNECT (leg0 / leg1)
    -> child carrier termination
    -> standalone SMP3 server
    -> destination
```

## Build and run

From the repository root:

```bash
go build -o smp3-client ./cmd/smp3-client
./smp3-client -c ./examples/smp3-client-config.example.json -check
./smp3-client -c ./config/client.json
```

The example contains invalid placeholder endpoints and `CHANGE_ME`; replace
all placeholders before connecting to a real server. The listener is restricted
to loopback by design.

On Windows:

```powershell
go build -o smp3-client.exe .\cmd\smp3-client
.\smp3-client.exe -c .\config\client.json -check
.\smp3-client.exe -c .\config\client.json
```

The command also supports `-version`. `-check` validates JSON and values only;
it does not connect to the upstream SOCKS server or SMP3 endpoint.

## Configuration

`upstream_socks.address` is the host-side SOCKS5 endpoint. Optional
`username` and `password` enable RFC 1929 authentication. `smp3.routes.leg0`
and `leg1` are the two distinct endpoints requested through that upstream
SOCKS service. `connect_timeout` (default `10s`) bounds the complete TCP
connect, greeting, authentication, CONNECT request, and CONNECT reply
transaction. Host-proxy internal retries cannot block primary-to-fallback
selection indefinitely. `leg1_fallback` is tried only for leg 1 when its
primary route fails or reaches that timeout.

`smp3.carrier_ready_timeout` (default `5s`) bounds the post-CONNECT wait for
the authenticated remote READY record. A SOCKS CONNECT success alone is not
considered remote carrier readiness.

TCP `CONNECT` uses SMP3 Stream HELLO v4. The second stream leg is activated by
the canonical Core's application-traffic callback, including download-heavy
flows, and repair keeps the same logical session ID. UDP uses Datagram HELLO
v5, a lazy first-packet bootstrap, per-datagram routing, and the configured
adaptive/stripe/duplicate policy. A terminal UDP engine can be recreated while
the local SOCKS UDP association remains open; closing the association disables
further recreation.

## Mihomo host integration

Run the sidecar separately and configure stock Mihomo to use it as a local
SOCKS5 proxy. See `examples/mihomo-sidecar.example.yaml`. The sidecar does not
modify Mihomo, Clash Party, carrier definitions, firewall rules, or the
standalone server. The route discriminator for the sidecar's own endpoints must
precede broad rules to prevent a host-SOCKS-to-sidecar loop.

This phase intentionally does not replace the native Mihomo or sing adapters.
Those integrations remain separate products and are not changed by the
sidecar client.

## SOCKS5 behavior

- `CONNECT` supports IPv4, IPv6, and domain destinations.
- `UDP ASSOCIATE` binds a loopback UDP port and carries packets through the
  canonical DatagramEngine.
- UDP `FRAG != 0` is discarded safely; it does not tear down the association.
- `BIND` and unknown commands return command-not-supported.
- The local SOCKS listener accepts no authentication; protect it with the
  loopback bind and local host policy.

## Validation

```bash
go test ./client ./cmd/smp3-client
go vet ./client ./cmd/smp3-client
```

The live carrier/standalone acceptance matrix is intentionally separate from
this local package test. Do not use production configuration or an existing
Clash Party process for the local tests.
