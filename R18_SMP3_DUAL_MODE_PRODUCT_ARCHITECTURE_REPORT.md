# R18 SMP3 Dual-Mode Product Architecture Report

## Verdict

```text
R18-A
SMP3_DUAL_MODE_PRODUCT_ARCHITECTURE_QUALIFIED
```

This is a qualification result for the local feature worktree. No production
runtime was accessed, restarted, or changed. No commit, push, merge, tag, or
release was performed.

Baseline: `v2.3.3`, commit
`f9d512e0a1b2291c5886aa8e89abbf5489e0f3f6`.

## Product architecture

```text
                         Shared SMP3
                  Core / Wire / Scheduler
                         /       \
                        /         \
     Standalone adapter             Native adapter
       generic SOCKS5                Mihomo integration
          /     \                    /          \
       Leg0     Leg1             child A      child B
          \     /                    \          /
          external carriers          SMP3 server :24444
```

| Product | Canonical artifacts | Carrier boundary | Protocol owner |
|---|---|---|---|
| Standalone | `smp3-client`, `smp3-server` | one generic SOCKS5 endpoint per leg | external proxy software |
| Native | `mihomo-smp3` plus shared server | Mihomo child outbounds | Mihomo Native adapter |
| Compatibility | `smp3-proxy` | compatibility overlay | sing-box compatibility runtime |

Standalone is **Universal External Proxy Compatibility**. Native is
**High-Performance Mihomo Integration**. Compatibility is separate legacy
integration and is not part of either primary mode.

Standalone does not implement or inspect VLESS, Reality, XTLS, Hysteria/HY2,
Snell, Trojan, Shadowsocks, VMess, TUIC, SSH, or WireGuard. It consumes only
per-leg SOCKS5 TCP CONNECT endpoints. External proxy software owns node
protocols, credentials, TLS, selection, and switching behind those endpoints.

The two SOCKS5 listeners may belong to one external proxy process or to two
different processes. SMP3 does not require one process per leg.

## Source changes

- Added explicit safe `upstream_socks.leg0` and `.leg1` binding documentation
  and retained the v2.3.3 global upstream fields for compatibility.
- Added `Config.LegUpstreamAddresses`, which exposes only endpoint identities;
  it never exposes usernames or passwords.
- Updated `smp3-client -check` to identify Standalone mode and both safe
  endpoint addresses.
- Added deterministic generic SOCKS same-process listener fixture coverage and
  per-leg TCP/UDP/failure-isolation coverage.
- Split build entrypoints into Standalone, Native, and Compatibility workflows.
  `build.sh` is now Standalone-only; the old all-product name remains an
  explicit wrapper.
- Added product documentation under `docs/standalone`, `docs/native`, and
  `docs/compatibility`, and included it in source packaging.
- Sourced the shared build helper in the Standalone workflow and fixed the
  checker to build inside its own Go module.

No files under `core/`, `server/`, Native adapter implementation, wire logic,
scheduler logic, or production configuration were changed by R18.

## Config compatibility

Existing fields remain supported:

```text
upstream_socks.address
upstream_socks.username
upstream_socks.password
upstream_socks.connect_timeout
upstream_socks.leg0
upstream_socks.leg1
smp3.routes.leg0
smp3.routes.leg1
smp3.routes.leg1_fallback
```

Per-leg overrides apply only to their own leg. A missing override preserves the
legacy global endpoint. Same endpoint values remain legal for compatibility;
independent carrier/node binding normally uses independent listener identities.
`leg1_fallback` remains an SMP3 route destination fallback and never becomes a
fallback from Leg1's SOCKS endpoint to Leg0's endpoint.

## Build workflow separation

```text
Standalone:     ./build.sh
Native:         ./scripts/build-native.sh
Compatibility:  ./scripts/build-compatibility.sh
All explicitly:  ./scripts/build-phase6-artifacts.sh
```

The Standalone dependency gate reported:

```text
STANDALONE_MIHOMO_PRODUCTION_IMPORT_COUNT: 0
STANDALONE_SING_PRODUCTION_IMPORT_COUNT: 0
STANDALONE_DEPENDENCY_GATE: PASS
```

The Standalone build does not fetch or build Mihomo, sing-box, `smp3-proxy`, or
any vendor adapter. Native uses the pinned Mihomo source and existing adapter
injection. Compatibility uses the pinned sing-box source and existing overlay.

## Artifact provenance

All artifacts below were built from the R18 worktree source state at HEAD
`f9d512e0a1b2291c5886aa8e89abbf5489e0f3f6` plus the uncommitted R18 working
tree delta. The binary checker reported the expected OS/architecture and each
manifest was verified with `sha256sum -c`.

Build profiles were Linux/amd64 and Windows/amd64 with `CGO_ENABLED=0`.

| Product | Artifact | Size | SHA256 |
|---|---|---:|---|
| Standalone | `smp3-client-linux-amd64` | 4,292,407 | `500DB964E18D622C0FC696426964D5FA230664B1519FC38D218A93A93B338122` |
| Standalone | `smp3-client-windows-amd64.exe` | 4,401,664 | `B78185817D374284CD6778943566F5F3C4BCD2D9DF372544E4EDC85D34C0E9E6` |
| Standalone | `smp3-server-linux-amd64` | 8,369,536 | `228E4C8E99A287E073AD48F1C95C076E4FF3B40CF93FF3F12036FFB5E38C477D` |
| Standalone | `smp3-server-windows-amd64.exe` | 8,517,632 | `3523838BE642225C903656CA50689F405E6D81920C70BF655C84058B9BB1C8E5` |
| Native | `mihomo-smp3-linux-amd64` | 50,485,596 | `CD4AE0157511DC6E2A31AA9E87ACA829115BDDDAE20059434703BE95293CF157` |
| Native | `mihomo-smp3-windows-amd64.exe` | 48,447,488 | `8F251472EA3C8F9CA32859CFE94E3B80B3F92241FD7D33D737FEE307B60947E7` |
| Compatibility | `smp3-proxy-linux-amd64` | 79,876,244 | `3475F179B0FE7FBAEA0964C7BAF033D037113ABDFFF12212CD1D95CF850F6CC8` |
| Compatibility | `smp3-proxy-windows-amd64.exe` | 80,539,136 | `630771465D5E7768E72BC5D15FB2FBE412B09FC6C35B5F429F824F19E5E0B71E` |

```text
ALL_RELEASE_ARTIFACT_HASHES_VERIFIED: YES
```

Build identities:

```text
Mihomo tag/revision: v1.19.28 / cbd11db1e13a75d8e680e0fe7742c95be4cba2be
sing-box tag/revision: v1.14.0-beta.14 / 4902660f8424fef3c2a60dfcdce7aeadfe3f3b88
```

## Binding and transport qualification

The disposable external-proxy fixture used two SOCKS5 listeners owned by one
fixture manager, with independent listener identities and connection counters.
The same binding code was also exercised through the actual Standalone
stream/datagram carrier paths.

Evidence passed:

- Leg0 connected only through configured SOCKS-A.
- Leg1 connected only through configured SOCKS-B.
- Both legs attached to the same logical SMP3 session and reached the same
  sidecar test endpoint.
- One-process/multi-listener behavior passed; the generic contract does not
  depend on process count.
- TCP deterministic payload integrity and SHA passed.
- UDP SOCKS ingress round trip and SMP3 datagram integrity passed; upstream
  SOCKS5 UDP ASSOCIATE was not required.
- An unavailable Leg0 upstream did not cause a connection through Leg1's
  upstream.

The binding test intentionally does not require equal useful-ACK distribution:
that is scheduler behavior, not the per-leg endpoint contract. A short
single-direction transfer may legitimately use one attached leg for DATA.

## Regression results

```text
STANDALONE_TESTS: PASS
SERVER_TESTS: PASS
CORE_REGRESSION: PASS
RACE_TESTS: PASS
GO_VET: PASS
NATIVE_BUILD: PASS
NATIVE_REGRESSION: PASS
COMPATIBILITY_BUILD: PASS
CLEAN_STANDALONE_BUILD: PASS
FRONTEND_STATIC_TESTS: PASS
SOURCE_SECRET_SCAN: PASS
```

Executed coverage included client/Core/server/Panel/cmd Go tests, tagged
Standalone integration tests, TCP/UDP and failure isolation tests, race tests,
`go vet`, Node frontend tests, shell/Python syntax checks, dependency gate, and
the three build workflows.

Two server tests that bind the already occupied fixed loopback telemetry port
`127.0.0.1:24500` were excluded from local reruns. The existing dashboard
browser smoke was also excluded because the host headless browser exited with
status 21. No process was stopped or changed to make those tests pass. A
pre-existing `TestPreferredStandaloneStress` had one timing-only Leg1 DATA
flake; the deterministic R18 binding/UDP subset and all other tagged tests
passed when it was isolated.

## Freeze and dependency audit

```text
CORE_SEMANTICS_CHANGED: NO
WIRE_CHANGED: NO
SCHEDULER_CHANGED: NO
ACK_SEMANTICS_CHANGED: NO
RETRANSMISSION_SEMANTICS_CHANGED: NO
NATIVE_ROUTING_CHANGED: NO
NATIVE_CHILD_OUTBOUND_SEMANTICS_CHANGED: NO
NATIVE_WIRE_CHANGED: NO
DIRECT_DIAL_MODE_IMPLEMENTED: NO
PROXY_PROTOCOL_IMPLEMENTED_IN_STANDALONE: NO
STANDALONE_VENDOR_ADAPTER_COUNT: 0
STANDALONE_PROXY_PROTOCOL_IMPLEMENTATION_COUNT: 0
```

The dependency graph is:

```text
Standalone client/server -> SMP3 Core
Native mihomo-smp3       -> pinned Mihomo + existing Native adapter + Core
Compatibility smp3-proxy -> pinned sing-box + compatibility overlay
```

No Mihomo or sing-box production import exists in the Standalone graph.

## Security and production boundary

```text
PRODUCTION_ACCESSED: NO
PRODUCTION_CHANGED: NO
R14_CLIENT_CHANGED: NO
CARRIER_A_CHANGED: NO
CARRIER_B_CHANGED: NO
PRODUCTION_SERVER_CHANGED: NO
NATIVE_PRODUCTION_CHANGED: NO
SYNTHETIC_PRODUCTION_C5: 0
PRODUCTION_PERFORMANCE_TEST: NO
```

The source scan covered 211 text files and found zero sensitive findings. No
credentials, node secrets, private keys, subscription URLs, or machine-home
paths were added.

## Release recommendation

The dual-mode architecture is qualified for a separately authorized release
stage. The next stage may select a release version and package the artifacts;
it must independently decide commit/push/tag/release actions. R18 itself does
not select a new version, modify production, or publish anything.

```text
READY_FOR_DUAL_MODE_RELEASE: YES
```
