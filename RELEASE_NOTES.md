# SMP3 2.0.1

SMP3 2.0.1 is an installer and operations patch release. It adds Windows
Mihomo Core replacement/update/restore tooling, Linux standalone server
installation and lifecycle management, `smp3ctl`, examples, and deployment
tutorials.

There are **no SMP3 protocol, Core, Wire, HELLO, scheduler, Stream, Datagram,
repair, or recovery semantic changes**. The six runtime binaries retain the
accepted 2.0.0 runtime semantics; the canonical 2.0.1 hashes are recorded in
the release report.

See `SMP3_2.0.1_INSTALLER_RELEASE_REPORT.md` for the closure matrix.

## 2.0.0 runtime baseline (historical)

SMP3 2.0.0 is the first release of the extracted canonical Core and standalone
server architecture. It packages the independently reusable SMP3 Core together
with the Mihomo client adapter, sing-box compatibility integration, and a
standalone landing server.

## Included

- Standalone SMP3 server for the production landing endpoint.
- Canonical standard-library-only Stream and Datagram Core.
- Mihomo integration, including persistent UDP association recreation after a
  terminal DatagramEngine.
- sing-box compatibility integration for TCP and UDP packet routing.
- TCP and UDP multipath with adaptive, stripe, and duplicate policies.
- Same-session leg repair/rejoin for recoverable carrier failures.
- Production migration completed with rollback validation.
- Explicit Linux/amd64 and Windows/amd64 artifact target verification.

The data path is:

```text
client adapters → Snell / Hysteria2 / direct reliable carrier
→ standalone SMP3 server → canonical Core → Internet destination
```

The standalone server does not implement Snell or Hysteria2 itself. Those
encrypted carriers terminate externally and connect to the SMP3 listener.

## Validation

The optional sing-box compatibility client was validated with the pinned
`v1.14.0-beta.14` at commit
`4902660f8424fef3c2a60dfcdce7aeadfe3f3b88`, pinned Mihomo `v1.19.28` at
commit `cbd11db1e13a75d8e680e0fe7742c95be4cba2be`, and Go `1.25.5`.

The final matrix is in `TEST_RESULTS.txt` and covers TCP 500 MiB exact
transfer, same-ID leg repair, MP-UDP adaptive/stripe/duplicate operation,
16384-byte boundary isolation, idle cleanup, 2000-association churn,
standalone server interop, and production cutover/rollback evidence.

## Known limitations

1. MP-UDP datagrams ride over child carrier streams. SMP3 removes its own global
   UDP head-of-line ordering, but cannot remove carrier-specific stream HOL.
2. UDP remains unreliable. A single-leg transition may lose a small number of
   datagrams; SMP3 intentionally does not add retransmission that would turn
   UDP into TCP.
3. Small adaptive streams below the activation threshold may remain single-leg
   by design.
4. The aioquic 1.3.0 ↔ quic-go large-transfer/control-plane interoperability
   issue remains reproducible on the direct aioquic path without SMP3. The H3
   100 MiB harness result is therefore `INCONCLUSIVE / EXTERNAL HARNESS-
   INTEROP ISSUE`, not an SMP3 protocol failure or release blocker.
5. Hysteria2 blackhole detection may take noticeable time before Snell fallback;
   correctness is covered, while detection latency remains a future tuning item.

## Security

Raw SMP3 HELLO authentication is not a public encrypted proxy protocol. Keep
the aggregation listener private when possible and use encrypted child carriers.
Never publish passwords, PSKs, TLS private keys, provider credentials, or
credential-bearing deployment configs.
