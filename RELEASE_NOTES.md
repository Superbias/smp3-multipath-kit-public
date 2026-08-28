# SMP3 alpha2.3-r11

R11 extends the live-validated r10 TCP reliability base with adaptive scheduling, bootstrap failover, and a new multipath UDP Datagram data plane.

## Added

- TCP `scheduler_mode=adaptive` using observed useful ACK/write throughput and write latency.
- Delayed parallel bootstrap with configurable `bootstrap_fallback_delay`.
- Preferred-leg hard-failure bootstrap through leg1.
- Datagram HELLO v5 while retaining TCP HELLO v4 compatibility.
- UDP `stripe`, `duplicate`, and `adaptive` modes.
- Per-datagram destination, ID and bounded dedup window.
- UDP same-ID transport rejoin and independent leg repair.
- Adaptive UDP path weighting by throughput and queue/write delay.
- Optional adaptive small-datagram duplication threshold.
- sing-box packet routing integration through `RoutePacketConnectionEx`.
- Native sing `N.PacketConn` address-preserving adapter for the UDP router path.
- 16384-byte routed Datagram cap in r11 to avoid silent truncation at the packet-buffer boundary.

## Pre-release review hardening

- Fixed stale replicated UDP datagrams being accepted again after dedup-map eviction without breaking very-late unique `stripe` delivery.
- Wired UDP hard carrier failures into the shared Hy2 -> Snell cooldown/probation state; probation recovery now requires real useful UDP payload and unused probation releases its slot.
- Added immediate Snell retry when bootstrap leg1 selects Hy2 and Hy2 fails.
- Removed a graceful-drain stale-timer race by basing stall failure on observed ACK progress.
- Removed the UDP leg-repair initialization window before worker startup.
- Made UDP path pressure byte-aware instead of frame-count-only.
- Bounded routed UDP address metadata before allocation and retained the 16384-byte Datagram cap.
- Activated the TCP booster immediately on preferred-queue saturation.

Client/server r11 Datagram sub-policy values are not negotiated by HELLO v5; keep UDP mode, maximum Datagram size and duplication policy aligned on both endpoints.

## Retained from r10

ACK-paced single-frontier rescue, rescue cooldown rollback, same-ID stale ownership invalidation, frontier-first dead-leg replay, per-leg ACK/control isolation, graceful drain, tombstones and adaptive Hy2/Snell logic remain retained.

## Final closeout

The canonical closeout was rebuilt from the pinned sing-box commit `4902660f8424fef3c2a60dfcdce7aeadfe3f3b88` with Go `1.25.5`. The standalone and injected multipath suites each contain 101 `Test*` functions; the generated sing-box source tree contains 441 `Test*` functions. The pinned Linux/amd64 and Windows/amd64 builds are reproducible from this source tree and report `1.14.0-beta.14-smp3-alpha2.3-r11`.

The SMP3 package gates passed: `validate-kit.sh`, standalone normal/race/vet checks, injected multipath normal/race/vet checks, gofmt, manifest validation, and the clean source-injection build. Full generated-tree checks were also attempted: `go test ./...` has only external environment/toolchain failures (namespace permission, current-Go `runtime/pprof.parseProcSelfMaps` linkname incompatibility, and internet-dependent TLS-fragment tests), while `go vet ./...` reports only upstream `unsafe.Pointer` diagnostics in `daemon` and `experimental/libbox`; neither has an SMP3 multipath failure.

Final live regression used `mode=adaptive`, `idle_timeout=2m`, and `max_datagram_size=16384`. TCP 500 MiB completed with HTTP 200 and exactly 524288000 bytes. UDP DNS was 10/10; the 20,000-packet 1200-byte burst had `BAD=0`, `DUPLICATE=0`, and 8 lost packets; 16384-byte datagrams were 100/100; and valid→oversize→valid isolation passed. The detailed matrix is in `TEST_RESULTS.txt`.

The H3 100 MiB diagnostic remains `INCONCLUSIVE / EXTERNAL HARNESS-INTEROP ISSUE`: the same failure reproduces on direct official aioquic paths with no bridge, SOCKS5, or SMP3. It is not an SMP3 release blocker and is not marked PASS.

## Known issues

1. MP-UDP datagrams ride over child carrier streams. SMP3 removes its own global UDP HOL, but cannot eliminate carrier-specific stream HOL.
2. UDP remains unreliable. A single-leg transition can lose a small number of packets; SMP3 intentionally does not add retransmission that would turn UDP into TCP.
3. The aioquic 1.3.0 ↔ quic-go large-transfer/control-plane interoperability issue remains unresolved and reproduces without SMP3.
4. Hysteria2 blackhole detection can take noticeable time before Snell fallback; correctness is covered, while detection latency remains a future tuning item.

## Release identity

Repository history contains no formal r11 tag/release marker, so the recommendation is to retain the r11 name for this candidate closeout. Any previously circulated candidate hashes should be treated as obsolete; use one canonical final hash set when publishing. No GitHub push or release publication is performed by this closeout.
