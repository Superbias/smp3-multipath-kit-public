# Realm TCP Forwarding Reference

> Optional transparent-forwarding recipe. Start with the [preferred proxy-node topology](../../../README.md#preferred-deployment-topology). Front machines need not run SMP3 or Realm; a proxy outbound can reach the Landing over WireGuard or ordinary IP routing. The historical phase results do not qualify a WireGuard deployment.

SMP3 N-leg Client can reach the same Landing TCP/TLS listener through multiple Realm TCP forwarding endpoints. Each forwarding endpoint must preserve the byte stream in both directions and must not parse or modify SMP3 frames.

```text
Client Leg 0 ── TCP/TLS ──────────────────────────► Landing:8443/TCP
Client Leg 1 ── TCP/TLS ──► Realm listener ── TCP ► Landing:8443/TCP
Client Leg 2 ── TCP/TLS ──► another Realm ── TCP ─► Landing:8443/TCP
```

Requirements:

- Realm listeners forward TCP to the exact same Landing address and port.
- The end-to-end TLS handshake is between the SMP3 Client and Landing. Realm must not terminate TLS or replace certificates.
- This recipe forwards a TCP byte stream. An underlying WireGuard tunnel may carry that TCP connection; SMP3 does not require Realm or prescribe the underlying IP transport. Configure routing, return paths and MTU separately, preserve end-to-end SMP3 authentication/TLS, and qualify the resulting path. This does not add a native UDP/QUIC Carrier or a second SMP3 scheduler.
- Allow only trusted Client/Realm egress addresses at the Landing firewall. Use the same random SMP3 session password on Client and Landing.
- Use the real Realm version's documented TCP forwarding syntax. This reference intentionally does not invent a Realm config file or assert Realm integration was run in Phase32.
- Different Realm hostnames or public IPs do not prove independent physical routes. Validate route/ASN/provider separation separately if that matters to operations.

The SMP3 Front package is not required for deployment. Realm is an external transparent TCP forwarder; it has no SMP3 session, ACK, retry, or scheduling state.
