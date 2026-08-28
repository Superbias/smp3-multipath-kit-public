# alpha2.3-r11 acceptance checklist / closeout

This checklist is closed out against binaries built from the same source tree. The artifacts are not pushed or published by this task.

## Build / config

```bash
./validate-kit.sh
./build.sh
./dist/smp3-proxy-linux-amd64 version
```

Windows:

```powershell
.\dist\smp3-proxy-windows-amd64.exe version
.\dist\smp3-proxy-windows-amd64.exe check -c .\config\client.json
```

Expected version: `1.14.0-beta.14-smp3-alpha2.3-r11`.

## TCP regression

1. 500 MiB exact upload, HTTP 200 and exact byte count.
2. Force-kill leg0 accepted TCP socket while outstanding DATA exists; verify same logical session survives and same leg ID rejoins on a new TCP connection.
3. Repeat for leg1.
4. Start a new logical connection with leg0 unavailable; verify leg1 bootstrap succeeds.
5. Delay leg0 initial dial beyond `bootstrap_fallback_delay`; verify leg1 races and the first successful HELLO establishes the session.
6. Compare leg0-only, leg1-only and dual-leg large-flow throughput; record `tx_goodput`, queue depth, ACK progress and per-leg useful bytes.

## UDP Datagram

1. Enable `udp_multipath` on both endpoints and `udp: true` in the Mihomo SOCKS node.
2. DNS/small UDP round trip.
3. `stripe`: verify both accepted leg sockets carry Datagram frames under sustained UDP load.
4. `duplicate`: verify packet copies travel both paths while the application receives each datagram once.
5. `adaptive`: add delay/queue pressure to one path and verify useful traffic shifts toward the healthier path.
6. Force-kill each UDP carrier leg independently and verify the PacketConn session remains usable while the other path is alive, then verify same-ID rejoin.
7. Run a QUIC/HTTP3 or iperf3 UDP workload through the actual client entry path.

## UDP carrier adaptive / probation

1. Run a UDP-only flow, break leg1 Hy2 repeatedly, and verify that the logical PacketConn remains alive while leg1 switches to Snell after the configured hard-failure threshold.
2. Keep leg0 unavailable during a new UDP bootstrap, break Hy2, keep Snell healthy, and verify the same bootstrap attempt succeeds through Snell.
3. After cooldown expires, verify only one probation Hy2 owner is admitted. Dial/HELLO success alone must **not** clear cooldown.
4. Send real UDP payload through the probation Hy2 leg and verify useful traffic completes recovery.
5. Close an unused probation flow and verify the global probation/canary slot is released for another flow.

Keep client and server `udp_multipath.mode`, `max_datagram_size`, and duplicate policy aligned; HELLO v5 does not negotiate those sub-policy values.

## Closeout result

- Full injected build and SMP3 multipath test gates: PASS.
- Linux and Windows versions: `1.14.0-beta.14-smp3-alpha2.3-r11`.
- TCP r10 acceptance, MP-UDP adaptive/stripe/duplicate coverage, boundaries, lifecycle and resource checks: PASS; see `TEST_RESULTS.txt` for evidence and caveats.
- H3 100 MiB large download: INCONCLUSIVE / EXTERNAL HARNESS-INTEROP ISSUE, not an SMP3 failure and not marked PASS.
