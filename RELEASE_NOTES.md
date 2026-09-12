# SMP3 2.3.4

R18 release hardening normalizes the dual-mode product identity in this
release. Standalone and Native remain separate product lines; the optional
sing-box compatibility artifact remains isolated from the Standalone build.

## Included

- Standalone: `smp3-client` and `smp3-server` with independent per-leg generic
  SOCKS5 endpoint configuration.
- Native: `mihomo-smp3` with the pinned Mihomo adapter workflow.
- Compatibility: `smp3-proxy` remains optional and uses its pinned sing-box
  runtime with an SMP3 `2.3.4` release suffix.
- `build.sh` builds Standalone only; Native and Compatibility have explicit
  separate workflows.
- Core, wire, scheduler, ACK, retransmission, server data-plane, and Native
  child-outbound semantics are unchanged.

# SMP3 2.3.3

R16 integrates the read-only Dashboard and accurate telemetry accounting into
`smp3-server`. The standalone `smp3-panel` process is retired and is not a
v2.3.3 release asset.

## Included

- Realtime status, Sessions, Traffic, History, Events, and SSE views.
- Native/Standalone ingress classification from authoritative listener role.
- Leg0/Leg1 rates and cumulative Carrier/Useful traffic.
- Current Session, Today, Last 24 Hours, 7 Days, and All Recorded periods.
- Restart-safe, counter-reset-safe, session-replacement-safe accounting with
  explicit telemetry gap and staleness status.
- Standalone and Native routing, Core transfer semantics, scheduler semantics,
  per-leg routing, and SMP3 wire protocol are unchanged.
- All official v2.3.3 product binaries use the unified package version. Mihomo
  is built from pinned upstream `v1.19.28`; `smp3-proxy` retains its pinned
  sing-box runtime identity with the SMP3 release suffix.

## Official binary assets

The release contains eight binaries: `smp3-client`, `smp3-server`,
`smp3-proxy`, and `mihomo-smp3`, each for Linux/amd64 and Windows/amd64,
plus `SHA256SUMS`. No standalone `smp3-panel` binary is shipped.

# SMP3 2.3.2

This release unifies the public runtime version metadata across the
Standalone server, Standalone Sidecar client, and R15 Panel. Official builds
read `kit_version` from `VERSION` and inject it into each binary.

## Included

- `smp3-server -version` reports `2.3.2`.
- `smp3-client -version` reports `2.3.2`.
- `smp3-panel -version` reports `2.3.2`.
- The formal build script now builds and checks server, client, Panel, Native,
  and optional compatibility artifacts from one release version.
- SMP3 wire/Core, scheduler, Native adapter semantics, Carrier behavior, and
  configuration semantics are unchanged.

The optional sing-box compatibility artifact keeps its upstream identity
separate from the SMP3 Kit release version.

# SMP3 2.3.1

This is a documentation-only patch release. It does not change the SMP3 Core,
wire protocol, scheduler, Native adapter, Standalone runtime, Carrier setup,
or R15 Panel behavior. Product artifacts remain based on the qualified v2.3.0
baseline.

## Documentation updates

- Replaced the old 2.2.0 deployment text with a concise v2.3.x quick start.
- Added a clear Native vs Standalone vs optional sing-box compatibility guide.
- Documented where Carrier node definitions live and how Leg0/Leg1 use them.
- Documented the qualified local port layout and recommended process order.
- Added R15 Panel startup, REST/SSE, history, and troubleshooting instructions.
- Added Chinese and English Sidecar usage guides.

# SMP3 2.1.1

SMP3 2.1.1 is a bugfix release for bidirectional Stream activation. It keeps
the accepted 2.0.0 runtime and Wire behavior and the 2.1.0 carrier-agnostic
adapter policy.

## Fixed

- Before 2.1.1, adaptive Stream activation observed only client/application TX
  ingress, so download-heavy sessions did not activate leg1.
- 2.1.1 observes application payload in both directions per logical Stream
  session and activates when `max(txRate, rxRate)` reaches the configured
  threshold over the existing activation window.
- Mihomo and sing continue to consume the canonical Core `OnActivate` callback;
  no adapter-local activation algorithm was added.

There is **no Wire, HELLO, Datagram, retry, frontier, rescue, ACK, reorder,
retransmit, carrier-policy, or recovery semantic change**. Existing 2.1.0
carrier-agnostic configurations remain compatible.

See `SMP3_2.1.1_BIDIRECTIONAL_ACTIVATION_RELEASE_REPORT.md` for the closure
matrix.

## 2.1.0 — carrier-agnostic sing adapter baseline

- Replaced protocol-named adaptive roles with generic primary and fallback
  carrier roles.
- Replaced the shared Hy2 health manager with generic primary-carrier health,
  cooldown, probation, and recovery state.
- Preserved Stream and Datagram adaptive state-machine behavior, thresholds,
  timing, same-session repair, and configured `legs`/`leg1_fallback` fields.
- Runtime logs now identify the configured outbound tags rather than guessing
  a protocol type.
- Added generic VLESS-style, Trojan-style, and Direct-style role coverage using
  fake child abstractions; no public protocol deployment is implied.
- IPv4/IPv6 selection remains delegated to the child outbound.

The six release binaries are built from the same validated 2.0.0 Core/server
runtime baseline plus the carrier-neutral sing adapter source.

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
3. Adaptive Stream activation is evaluated per logical session from application
   payload in both directions and uses the higher directional rate. Small
   streams below the threshold may remain single-leg by design; throughput is
   not aggregated across separate connections.
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
