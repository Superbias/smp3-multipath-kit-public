# Standalone external proxy contract

This page is conceptual. It deliberately avoids vendor-specific configuration
syntax and real node credentials. Consult each proxy's own documentation for
the exact listener and outbound syntax.

## Required contract

```text
SOCKS-A: local SOCKS5 listener -> deterministic external outbound A
SOCKS-B: local SOCKS5 listener -> deterministic external outbound B

SMP3 Leg0 -> SOCKS-A
SMP3 Leg1 -> SOCKS-B
```

The contract is TCP SOCKS5 CONNECT with IPv4, IPv6, and domain ATYP support,
optional username/password authentication, bounded timeout, and cancellation.
SMP3 Datagram frames use the established carrier TCP connections, so an
upstream SOCKS5 UDP ASSOCIATE is not required.

## Ecosystem examples

The following are architecture examples, not normative config snippets:

```text
Mihomo / Clash:
  listener A -> fixed proxy/node A
  listener B -> fixed proxy/node B

Xray:
  SOCKS inbound A -> fixed outbound A
  SOCKS inbound B -> fixed outbound B

sing-box:
  mixed/SOCKS inbound A -> fixed outbound A
  mixed/SOCKS inbound B -> fixed outbound B
```

The two listeners can be declared by one proxy process. SMP3 does not identify
or select the underlying VLESS, Reality, HY2, Snell, or other node. If the
external process changes an outbound behind listener A, the SMP3 endpoint and
configuration remain unchanged.

## Security

Keep proxy credentials, UUIDs, Reality keys, PSKs, subscription URLs, and
server passwords in private external configuration. Do not place them in SMP3
examples or source control.
