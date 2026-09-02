# SMP3 Standalone Sidecar Client Phase 1 Report

## STATUS

```text
IMPLEMENTATION: COMPLETE
LOCAL FUNCTIONAL ACCEPTANCE: PASS
STATIC/BUILD ACCEPTANCE: PASS
STOCK MIHOMO FIXTURE: VERIFIED
STOCK MIHOMO ROUTE DISCRIMINATION: PASS
STOCK MIHOMO TCP/RX/TX ACCEPTANCE: PASS
R2 BOUNDED SOCKS TIMEOUT: PASS
R2 DIRECT TIMEOUT FALLBACK: PASS
R3 REMOTE READY EXTENSION: PASS
STOCK MIHOMO FALLBACK ACCEPTANCE: PASS
STOCK MIHOMO UDP/LOOP PREVENTION: PASS
PHASE 1 FINAL GATE: PASS
```

This work is on `feature/standalone-sidecar-client`, based on `origin/main`.
R2 base head: `b172cfafeec7165845234f6f4afd55dc46e703e3`.
No remote server, production configuration, Clash Party process, native Mihomo
adapter, native sing adapter, Core implementation, or Wire implementation was
changed. The branch also contains the earlier minimal standalone-server host
change that adds equivalent `Config.Listeners` endpoints for route testing;
Core/session semantics remain unchanged.

## IMPLEMENTED

- `client/`: strict JSON configuration, loopback-only SOCKS5 listener, upstream
  SOCKS5 TCP CONNECT, Stream HELLO v4, and Datagram HELLO v5.
- TCP CONNECT supports IPv4, IPv6, and domain destinations. Stream activation
  is driven by the canonical Core callback; leg1 uses the configured fallback
  route when its primary route fails, and repair keeps the same session ID.
- UDP ASSOCIATE binds a loopback UDP port, uses only upstream TCP CONNECT, and
  carries datagrams through the canonical DatagramEngine. UDP FRAG packets are
  isolated and discarded; oversized/error packets do not close the association.
- A terminal UDP engine is recreated at one serialized replacement point while
  the SOCKS UDP association remains open. Association close cancels further
  recreation and closes carrier/local resources.
- `cmd/smp3-client/`: `-c`, `-check`, and `-version` entry points.
- `examples/`, `SIDECAR.md`, `SIDECAR.zh-CN.md`, and deployment/README guidance.
- `validate-kit.sh` now includes sidecar normal/race/vet checks and the new JSON
  example.
- `upstream_socks.connect_timeout` bounds each complete upstream SOCKS5 TCP
  transaction and closes pending sockets on timeout or parent cancellation.
- `smp3.carrier_ready_timeout` bounds authenticated remote readiness after
  SOCKS success; only sidecar-designated server listeners emit READY.

## R2 ROOT CAUSE

The Sidecar previously depended on the upstream SOCKS host eventually returning
a CONNECT success/failure. Stock Mihomo may internally retry an unhealthy
outbound and can also return local SOCKS CONNECT success before its selected
SOCKS proxy has completed the downstream transaction. The first behavior can
block fallback; the second is a host false-success condition that carries no
failure signal in the frozen SMP3 protocol.

This is not a Core scheduler, Wire, route-discrimination, or direct fallback
policy defect.

## BOUNDED CARRIER BOOTSTRAP

`upstream_socks.connect_timeout` is a host-level maximum wall-clock duration
for TCP dial, greeting, RFC 1929 authentication, CONNECT request, and CONNECT
reply. Each call owns a fresh timeout context and socket-close watcher. Parent
cancellation returns immediately and prevents fallback; primary timeout is
treated as a failure and allows the configured leg1 fallback.

```text
UPSTREAM_SOCKS_CONNECT_TIMEOUT: PASS
SIDECAR_TIMEOUT_FALLBACK: PASS (direct hanging fake SOCKS)
PARENT_CANCEL_NO_FALLBACK: PASS
100 HANGING CONNECT RESOURCE CLEANUP: PASS
```

## TESTS

Client package top-level tests: 18. Coverage includes:

- SOCKS IPv4/IPv6/domain address encoding and decoding;
- upstream SOCKS5 CONNECT;
- local BIND rejection and UDP ASSOCIATE loopback reply;
- local SOCKS5 CONNECT through a real standalone server and TCP echo;
- bidirectional Stream activation of the second route;
- leg1 primary-to-fallback route selection;
- UDP FRAG isolation and address round trips;
- local SOCKS5 UDP through standalone server and two routes;
- terminal DatagramEngine recreation with concurrent callers creating one
  replacement engine.
- bounded upstream SOCKS CONNECT timeout, parent-cancel propagation, and 100
  concurrent hanging CONNECT resource cleanup.
- authenticated READY v1 binding and malformed READY rejection.

Observed results:

```text
go test ./client ./cmd/smp3-client                  PASS (18 package/subtest results)
go test ./core ./server ./cmd/smp3-server ./client ./cmd/smp3-client
                                                       PASS (129 tests)
client go test -count=3                              PASS
timeout/fallback focused -count=300                  PASS
client go test -race (WSL, CGO_ENABLED=1)            PASS
core go test -race (WSL)                             PASS
server go test -race (WSL)                           PASS
cmd/smp3-server go test -race (WSL)                  PASS
go vet core/server/client/commands                   PASS
```

## BUILD

```text
linux/amd64, CGO_ENABLED=0 smp3-client               PASS
windows/amd64, CGO_ENABLED=0 smp3-client.exe         PASS
```

The Windows native environment could not link `-race` because MinGW runtime
libraries are absent; the same client race suite passed under WSL with the
Linux race toolchain.

## STOCK MIHOMO FIXTURE

```text
STOCK_FIXTURE_PROVENANCE: local pre-SMP3 Mihomo installer backup
STOCK_FIXTURE_VERSION: Mihomo Meta v1.19.29 windows amd64
STOCK_FIXTURE_SHA256: 82cd796a23492f43a71c1ec27e4e5e0b3d58932014da5a36e79ed9b11fee8162
STOCK_FIXTURE_NATIVE_SMP3_SUPPORT: NO
```

The fixture was copied to a disposable Temp directory. Its negative config
probe returned `unsupport proxy type: smp3` and a failed config test. The
current Clash Party executable was not run, replaced, or stopped.

## STOCK MIHOMO ROUTE DISCRIMINATION

```text
STOCK_MIHOMO_ROUTE_A: PASS (DST-PORT/19441 -> relay A only)
STOCK_MIHOMO_ROUTE_B: PASS (DST-PORT/19442 -> relay B only)
STOCK_MIHOMO_ROUTE_C: PASS (DST-PORT/19443 -> relay C only)
STOCK_MIHOMO_ROUTE_DISCRIMINATION: PASS
```

Each relay recorded the requested local discriminator and transparently
connected to the corresponding standalone listener.

## STOCK MIHOMO SIDECAR E2E

```text
STOCK_MIHOMO_SIDECAR_TCP: PASS (8 MiB exact download)
STOCK_MIHOMO_SIDECAR_RX_ACTIVATION: PASS
STOCK_MIHOMO_SIDECAR_TX_ACTIVATION: PASS
LEG1_SAME_SESSION_JOIN: PASS
STOCK_MIHOMO_PRIMARY_TIMEOUT: BLOCKED (R2 historical baseline)
STOCK_MIHOMO_SIDECAR_FALLBACK: PASS (R3 readiness)
STOCK_MIHOMO_SIDECAR_UDP: PASS (64/64, lost=0, bad=0)
SIDECAR_LOOP_PREVENTION: PASS (11 relay connections, bounded)
```

R2 direct fake-SOCKS tests passed: a CONNECT request that never returns a reply
is bounded by `connect_timeout=200ms`, the underlying socket closes, and a
healthy fallback is attempted with the same SessionID and LegID=1. The 100-way
hanging-connect test also returned with all accepted handlers closed.

In the R2 pre-readiness stock-host topology, B's disposable SOCKS relay was made to accept
the upstream connection and never complete its SOCKS greeting/CONNECT path.
Stock Mihomo nevertheless returned `CONNECT success` to the Sidecar in about a
millisecond, while its log continued retrying `ROUTE-B` internally. The
Sidecar therefore correctly observed a success, attached B, and had no
protocol-level failure signal that would authorize C fallback. C was verified
healthy by a direct stock SOCKS control request, but was not selected by this
false-success flow. This is a stock-host interop blocker, not evidence of a
Core, Wire, route-discrimination, or direct Sidecar fallback bug.

## R3 REMOTE READINESS CONTRACT

```text
CANONICAL_WIRE_CHANGED: NO
SIDECAR_HOST_READINESS_EXTENSION: ADDED
READY_FORMAT: SMP3RDY1 / 50 bytes / HMAC-SHA256
READY_BINDING: HELLO nonce + SessionID uint64 prefix + LegID + Mode
```

The standalone server emits READY only on `sidecar_listeners`, after existing
canonical HELLO parsing, authentication, nonce replay checks, and session
admission. It writes the complete READY record before attaching the carrier to
the existing Core/session. Legacy `listen`/`listeners` accept canonical HELLO
without emitting READY. Client Stream and Datagram legs verify magic, session,
leg, mode, HMAC, timeout, and parent cancellation before Core attachment.

## FALSE-SUCCESS RECOVERY

```text
FALSE_SUCCESS_READY_TIMEOUT: PASS
SIDECAR_FALSE_SUCCESS_FALLBACK: PASS
STOCK_MIHOMO_FALSE_SUCCESS_OBSERVED: YES
STOCK_MIHOMO_READY_TIMEOUT: PASS
STOCK_MIHOMO_SIDECAR_FALLBACK: PASS
LEG1_SAME_SESSION_JOIN_AFTER_FALLBACK: PASS
```

The real localhost stock Mihomo harness used A=healthy, B=accepted-but-not
ready, and C=healthy. B returned local SOCKS success while its downstream
carrier remained unavailable; Sidecar rejected B after the 2-second READY
timeout, then used C. The standalone logs and relay observations confirmed
LegID=1 joined the new session with the same SessionID.

## STOCK MIHOMO FINAL QUALIFICATION

```text
STOCK_MIHOMO_FIXTURE: VERIFIED
STOCK_MIHOMO_ROUTE_A: PASS
STOCK_MIHOMO_ROUTE_B: PASS
STOCK_MIHOMO_ROUTE_C: PASS
STOCK_MIHOMO_ROUTE_DISCRIMINATION: PASS
STOCK_MIHOMO_SIDECAR_TCP: PASS (8 MiB exact)
STOCK_MIHOMO_SIDECAR_RX_ACTIVATION: PASS
STOCK_MIHOMO_SIDECAR_TX_ACTIVATION: PASS
STOCK_MIHOMO_SIDECAR_FALLBACK: PASS
STOCK_MIHOMO_SIDECAR_UDP: PASS (64/64)
SIDECAR_LOOP_PREVENTION: PASS
LEGACY_LISTENER_CHANGED: NO
NATIVE_MODE_CHANGED: NO
MULTI_LISTENER_PORT_SEMANTICS: NONE
```

Relay totals were bounded at `A=5`, `B=3`, `C=3` (11 connections total), with
no recursive connection storm. The reverse-port test also passed: a listener
receiving LegID=1 and another receiving LegID=0 still formed the expected
logical sessions; port number did not assign leg identity.

## STOCK FALLBACK REPAIR

```text
STOCK_MIHOMO_PRIMARY_TIMEOUT: BLOCKED BY STOCK FALSE SUCCESS
STOCK_MIHOMO_SIDECAR_FALLBACK: BLOCKED
LEG1_SAME_SESSION_JOIN_AFTER_FALLBACK: NOT RUN
```

With the verified stock fixture, a B relay that accepts the upstream TCP
connection but does not complete its SOCKS greeting causes stock Mihomo to
return local CONNECT success to Sidecar immediately. Sidecar therefore cannot
observe the required timeout at the upstream boundary. The direct fake-SOCKS
test demonstrates the intended R2 behavior, but the real stock-host fallback
gate remains open without a host-side readiness/error signal.

## STOCK UDP / LOOP ACCEPTANCE

```text
STOCK_MIHOMO_SIDECAR_UDP: NOT RUN
SIDECAR_LOOP_PREVENTION: NOT RUN
```

Per R2 stop semantics these gates were not run after the stock fallback blocker.

## FROZEN INTEGRITY

```text
Core source files: 18/18 match HEAD canonical hashes
Wire fixtures:     22/22 match HEAD golden hashes
Core migration parity: PASS (existing allowed stream activation files only)
validate-kit.sh:   PASS in LF-normalized disposable copy
git diff --check:  PASS
```

The Windows checkout uses `core.autocrlf=true`; direct hash commands against
the working-tree bytes therefore show CRLF differences. The canonical
comparison was made against `HEAD` blobs and the repository's existing
manifests, while the actual source files remain unmodified by this feature.

## STATIC/SECURITY

```text
release-scan.py:   SENSITIVE_FINDINGS=0
public-ip-scan.py: PUBLIC_IPV4_FINDINGS=0
JSON example parse: PASS
gofmt on new Go files: clean
```

No credentials, private keys, user-home paths, or production endpoints were
added. Example secrets use `CHANGE_ME`; route names use `.example.invalid`.

## R1/R2 STOP HISTORY

Before R3 READY, the stock-host UDP and loop-prevention gates were intentionally
not run after the false-success fallback blocker. R3 reran those two required
gates after readiness was added; both now pass as recorded above.

No remote/live carrier test was run in this phase by design.

## NEXT

Review the final diff and create the allowed local commits on
`feature/standalone-sidecar-client`. Do not merge, push, tag, release, or deploy
automatically.
