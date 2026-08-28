# Security and deployment notes

SMP3 is experimental transport code. Treat it as a private aggregation protocol, not as a public encrypted proxy protocol.

## Required practices

- Keep the raw SMP3 aggregation listener on a private/internal/WireGuard address when possible.
- Use encrypted/authenticated public child carriers such as Hysteria2 or Snell.
- Generate unique random SMP3, Snell and Hysteria2 credentials for every deployment.
- Protect TLS private keys and concrete deployment configs with restrictive filesystem permissions.
- Do not commit `config.json`, `client.json`, `server.json`, private keys, PSKs, passwords or provider credentials.
- Validate downloaded release/archive SHA256 values and `MANIFEST.sha256` before deployment.

## Authentication versus encryption

The SMP3 HELLO uses HMAC authentication/replay checks, but that does not make the complete SMP3 data plane a replacement for a public encrypted tunnel. Confidentiality should be provided by the selected child transport/network boundary.

## Secret rotation

If a deployment configuration has ever been published or committed to a public repository, rotate all values that could have been exposed rather than relying only on repository deletion/history rewriting.
