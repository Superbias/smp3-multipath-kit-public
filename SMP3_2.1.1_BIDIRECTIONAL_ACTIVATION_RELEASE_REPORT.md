# SMP3 2.1.1 Bidirectional Activation Release Report

Date: 2026-09-01

## STATUS

```text
SMP3 2.1.1: RELEASED
BIDIRECTIONAL ACTIVATION RELEASE: PASS
TX_ACTIVATION: PASS
RX_ACTIVATION: PASS
DOWNLOAD_LEG1_LIVE: PASS
ACTIVATION_RATE_POLICY: MAX_DIRECTIONAL_RATE
WIRE_CHANGED: NO
DATAGRAM_CHANGED: NO
NON_ACTIVATION_STREAM_SEMANTICS_CHANGED: NO
STANDALONE_PRODUCTION_CHANGED: NO
LEG1_HELLO_REACHED_STANDALONE: YES
LEG1_SAME_SESSION_JOIN: YES
SMP3_RUNTIME_PATCH_REQUIRED: NO
```

The source, binaries, tag, and GitHub Release remain unchanged. The final live
gate passed after synchronizing the local child-carrier configuration with the
verified remote Hysteria2 and Snell server configuration.

## INITIAL LIVE QUALIFICATION

The first 2.1.1 live attempt correctly stopped as incomplete:

```text
Core RX activation: PASS
stream activated: YES
leg1 join: NOT OBSERVED
DOWNLOAD_LEG1_LIVE: FAIL
```

That failure was later proven to be stale local carrier configuration, not an
SMP3 runtime defect. It is retained here as historical evidence.

## VERSION/TAG/COMMIT

```text
version: 2.1.1
tag: v2.1.1
release commit: eec11d0
commit message: release: SMP3 2.1.1 bidirectional stream activation
```

GitHub Release: https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.1.1

## ACTIVATION FIX

`core/stream_engine.go` now samples both existing atomic application-payload
counters in the existing window:

```text
txRate = ingressBytes delta / elapsed
rxRate = rxDeliveredBytes delta / elapsed
activationRate = max(txRate, rxRate)
```

The existing `>=` threshold comparison, window/ticker behavior, and one-shot
callback are unchanged. RX counts only payload delivered to the application;
ACK, HELLO, frame headers, retransmission overhead, and control frames are not
included.

## TESTS/RACE

```text
Core activation focused -count=1000: PASS (8000 passed)
Core activation focused -race -count=100: PASS
Core full normal/race/vet: PASS
Server/standalone normal/race/vet: PASS
Mihomo adapter/config normal -count=3: PASS
Mihomo RX activation -> leg1 focused -race: PASS
sing tagged normal/race/vet: PASS
validate-kit.sh: PASS
```

Current validation count:

```text
smp3core=47
legacy_semantic=106
sing_adapter=17
total=170
```

The deterministic Mihomo test uses a real Core and injected pinned Mihomo
adapter. It sends sustained server-to-client payload, observes RX activation,
and verifies the leg1 HELLO uses the same SessionID.

## CORE/WIRE INTEGRITY

```text
CORE_CANONICAL_SHA256SUMS: 18/18 match
Core migration parity: PASS; only stream_engine.go and stream_activation_test.go allowlisted
WIRE_GOLDEN_SHA256SUMS: 22/22 match
```

No Wire, HELLO, Datagram, retry, frontier, rescue, ACK, reorder, retransmit,
repair, recovery, or carrier-policy production source changed.

## FORMAL ASSETS

All six targets passed the Linux/amd64 or Windows/amd64 checker:

| Asset | SHA256 |
|---|---|
| `smp3-server-linux-amd64` | `16dcd42dc38b88d2d17cd4e4f9c387131cdb35ab6d1d6d2cb036008f2a90b01a` |
| `smp3-server-windows-amd64.exe` | `efa3469706511eaf45244ba8ec8d03d78382dc0748ace788fd0d3dd72466f623` |
| `mihomo-smp3-linux-amd64` | `991d072baf16b57414491f42d7e539b003e590ebc5d7e60125cb1d261daef894` |
| `mihomo-smp3-windows-amd64.exe` | `5f48edd5f52bdf80d5493a876971eddcf441c00f24fb77118140ad36c364b8db` |
| `smp3-proxy-linux-amd64` | `b2888ebe479ad00b626f8f093e98bad5f4d7e439f0fc8f40a119e8cfdb07058c` |
| `smp3-proxy-windows-amd64.exe` | `987d167e7582e557e40c59f652e93176ccd46dafdf79a9aff6ffcdc2c4d320c9` |

```text
SHA256SUMS SHA256: 51802b9d7c03ff2c6991caa205f69b4b7742b2ae9e4067ddaa0ee4cb09c498a5
source: smp3-multipath-kit-2.1.1-source.zip
source SHA256: b765378db1edd9aad4fe1a1671631803139b3b13c01486f64870c3612cd5d08f
```

## SOURCE ARCHIVE

```text
unzip -t: PASS
fresh extract validate-kit.sh: PASS (170 tests)
credential/private-key scan: PASS (0 findings)
public IPv4 scan: PASS (0 findings)
forbidden-file scan: PASS
```

## GITHUB RELEASE

```text
name: SMP3 2.1.1
tag: v2.1.1
prerelease: false
custom assets: 8
asset digest verification: PASS (8/8 match local SHA256)
```

The eight assets are exactly six binaries, `SHA256SUMS`, and the source
archive.

## MIHOMO DEPLOYMENT

The local Clash Party sidecar was replaced only after the release asset had
passed local and GitHub digest verification.

```text
path: C:\Program Files\Clash Party\resources\sidecar\mihomo.exe
old SHA256: 6b3d4ccb058440d14f869bc5bc1d285e5f310a8f9beb8c5dfb3fb3ac767c6e6e
new SHA256: 5f48edd5f52bdf80d5493a876971eddcf441c00f24fb77118140ad36c364b8db
new PID: 31948
parent Clash Party PID: 37396
backup: sidecar\smp3-backup\mihomo.exe.20260901T122340122Z.bak
backup SHA256: 6b3d4ccb058440d14f869bc5bc1d285e5f310a8f9beb8c5dfb3fb3ac767c6e6e
```

The post-restart process path matches the replaced file and its SHA matches
the v2.1.1 Release asset. The original binary remains recoverable from the
verified backup.

The temporary diagnostic threshold was restored in the active Clash Party
profile `profiles/19f5fb50a64.yaml` and regenerated work config to:

```text
activation-threshold-mbps: 50
activation-window: 1s
```

## LEG0 BASELINE

```text
request: https://example.com/ through SOCKS5 127.0.0.1:7898
HTTP: 200
bytes: 559
time: 0.277928 s
remote evidence: session created with leg=0 only
result: PASS
```

The low-throughput request did not pull leg1 unconditionally.

## CARRIER ROOT CAUSE

The initial local carrier configuration was stale/incomplete:

```text
Hysteria2: obsolete port/endpoint shape; missing salamander obfs; auth and obfs passwords mismatched
Snell: obsolete port 23456; PSK mismatched
```

The verified remote configuration is:

```text
Hysteria2: UDP :443, SNI port-0dual.xyuanai.xyz, salamander obfs, password auth
Snell: UDP :6350
```

Real auth/password material is intentionally not recorded. The local values
were synchronized with verified remote configuration.

```text
ROOT_CAUSE_LAYER: PRIMARY_CARRIER_ENDPOINT
SMP3_RUNTIME_PATCH_REQUIRED: NO
```

## PRIMARY DIRECT QUALIFICATION

After correction, an isolated direct `public-hy2` test passed:

```text
HTTP=200
BYTES=559
TIME=0.667895 s
```

## FALLBACK DIRECT QUALIFICATION

After correction, an isolated direct `public-snell` test passed:

```text
HTTP=200
BYTES=559
TIME=0.403658 s
```

Both child carriers independently proxy HTTPS outside SMP3.

## FINAL DOWNLOAD LIVE QUALIFICATION

The final test used the formal production threshold:

```text
activation-threshold-mbps: 50
activation-window: 1s
```

```text
URL: https://speed.cloudflare.com/__down?bytes=50000000
HTTP=200
BYTES=50000000
TIME=3.687421 s
SPEED=13559627 B/s
```

The flow was confirmed through SMP3. Client log:

```text
SMP3 session bootstrapped via leg 0 / line-path
SMP3 leg 1 ready/rejoined via public-hy2
```

Remote standalone log:

```text
session=0cee77c6 leg=0 mode=stream
session=0cee77c6 leg=1 joined/rejoined
```

```text
LEG1_HELLO_REACHED_STANDALONE: YES
LEG1_SAME_SESSION_JOIN: YES
DOWNLOAD_LEG1_LIVE: PASS
```

## SPEEDTEST

```text
NOT RUN
```

Speedtest was not needed for this closure because the mandatory deterministic
Cloudflare download gate passed. No browser result is falsely marked PASS.

## HY2/FALLBACK OBSERVATION

Remote read-only state at diagnosis:

```text
hysteria.service: active/running, PID 507, UDP *:443
snell.service: active/running, PID 514, UDP *:6350
smp3-standalone.service: active/enabled, PID 260816, TCP 10.66.66.1:24444
```

No remote service was restarted, edited, upgraded, removed, or reconfigured.
The evidence is insufficient to claim whether external provider port mapping
exists for `23457`; it is sufficient to show that no leg1 carrier reached the
standalone session during the qualified downloads.

## PRODUCTION SAFETY

```text
standalone production: unchanged
server listener 10.66.66.1:24444: unchanged
remote firewall/NAT: unchanged
remote carrier services: unchanged
local Clash Party: restarted only for the authorized Mihomo replacement/config reload
old Mihomo: verified backup preserved
```

## FINAL LIVE CLOSURE

```text
SMP3 2.1.1: RELEASED
BIDIRECTIONAL ACTIVATION RELEASE: PASS
TX_ACTIVATION: PASS
RX_ACTIVATION: PASS
DOWNLOAD_LEG1_LIVE: PASS
PRIMARY_CARRIER_DIRECT_REACHABILITY: PASS
FALLBACK_CARRIER_DIRECT_REACHABILITY: PASS
LEG1_HELLO_REACHED_STANDALONE: YES
LEG1_SAME_SESSION_JOIN: YES
ACTIVATION_RATE_POLICY: MAX_DIRECTIONAL_RATE
WIRE_CHANGED: NO
DATAGRAM_CHANGED: NO
NON_ACTIVATION_STREAM_SEMANTICS_CHANGED: NO
SMP3_RUNTIME_PATCH_REQUIRED: NO
```

The already-published v2.1.1 binaries, `SHA256SUMS`, source archive, tag, and
GitHub Release remain unchanged. No new production operation was performed
in this report-only closure; the carrier configuration correction and live
tests were completed before this closure phase.
