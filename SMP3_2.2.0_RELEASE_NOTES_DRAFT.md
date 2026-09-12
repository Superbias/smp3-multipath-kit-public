# SMP3 2.2.0 Release Notes (Draft)

SMP3 2.2.0 adds the standalone Sidecar deployment mode while preserving the
canonical SMP3 wire and extracted Core behavior from the 2.1.x line.

## What's New

- Cross-platform `smp3-client` with a local SOCKS5 listener.
- SOCKS5 TCP CONNECT and UDP ASSOCIATE ingress.
- One upstream SOCKS5 host model with bounded CONNECT transactions.
- External destination discriminator routing for two independent carrier legs.
- Stream and Datagram support over reliable TCP-capable child routes.
- Adaptive TX/RX Stream activation and same-session leg repair.
- Primary/fallback carrier selection and bounded bootstrap behavior.
- Authenticated `SMP3RDY1` readiness on sidecar listeners.
- Standalone server support for explicit `sidecar_listeners`.

## Deployment Modes

### Native

```text
application -> SMP3-enabled Mihomo -> child outbounds -> SMP3 server
```

Native mode is the shortest path and remains recommended for maximum
performance.

### Sidecar

```text
application -> stock proxy host -> smp3-client SOCKS5
            -> discriminator routes -> sidecar listener -> SMP3 server
```

Sidecar mode is compatibility-first and host-agnostic. It does not replace a
host's native core or configure carrier protocols, firewall rules, or proxy
subscriptions.

## Compatibility

- Native custom Mihomo: qualified.
- Stock Mihomo with standalone Sidecar: qualified.
- sing-box host: architecture-compatible, not formally qualified in 2.2.0.
- Xray/V2Ray and other SOCKS5 routing hosts: architecture-compatible, not
  formally qualified in 2.2.0.
- Android application wrapper/APK: not included.

## Performance

Native remains the performance-first integration. The Sidecar adds a local
SOCKS5 host, process, and socket path; the localhost RC qualification showed
measurable overhead, especially for sustained download and full-duplex traffic.
Those synthetic measurements are not universal performance guarantees.

## Upgrade Notes

- The legacy `listen`/`listeners` server endpoints remain compatible with
  existing Native clients.
- Native users do not need to switch to `sidecar_listeners`.
- `SMP3RDY1` is a Sidecar host-readiness extension, not a canonical SMP3 Wire
  version or a new HELLO version.
- Each Sidecar discriminator route should reach an equivalent
  `sidecar_listener` on the same logical SMP3 server through an independent
  reliable TCP-capable carrier path. Listener ports are routing discriminators
  only; SessionID/LegID determine logical leg identity.

## Security

Example files contain placeholders only. Replace `CHANGE_ME` values, keep the
raw SMP3 listener private, and do not publish passwords, PSKs, private keys, or
real deployment configurations.
