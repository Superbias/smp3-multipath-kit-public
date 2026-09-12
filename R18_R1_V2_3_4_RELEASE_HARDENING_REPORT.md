# R18-R1 v2.3.4 Release Hardening Report

## Verdict

```text
R18-R1-A
DUAL_MODE_V2_3_4_RELEASE_CANDIDATE_QUALIFIED
```

This report closes the R18 dual-mode hardening stage on the feature branch.
The source release-hardening commit is `06a7d9972f57eef92f9d094e70668d9247461f14`.
This stage stops before merge, push, tag, GitHub Release, and deployment.

## Fresh repository audit

```text
CURRENT_BRANCH: feature/r18-dual-mode-product
CURRENT_HEAD_AT_AUDIT: f9d512e0a1b2291c5886aa8e89abbf5489e0f3f6
ORIGIN_MAIN_HEAD: f9d512e0a1b2291c5886aa8e89abbf5489e0f3f6
LATEST_TAG: v2.3.3
BASE_HEAD: f9d512e0a1b2291c5886aa8e89abbf5489e0f3f6
SOURCE_RELEASE_COMMIT: 06a7d9972f57eef92f9d094e70668d9247461f14
WORKTREE_BEFORE_FINAL_REPORT: clean except internal planning files
```

The R18 source delta was explicitly staged; no blind `git add -A` was used.
The internal `task_plan.md`, `findings.md`, and `progress.md` files are working
memory only and are not release files.

## Diff classification

The 34-file source release-hardening commit contains only these categories:

| Category | Scope |
|---|---|
| Standalone config/interface | `client/config.go`, `cmd/smp3-client/main.go`, examples |
| Standalone observability | safe per-leg endpoint identity output |
| Generic SOCKS fixtures/tests | `client/generic_socks_contract_test.go` and related tests |
| Dual-leg binding tests | independent Leg0/Leg1 TCP, UDP, integrity, and isolation tests |
| Build workflow split | Standalone, Native, Compatibility scripts and explicit wrapper |
| Documentation split | product architecture, Standalone, Native, Compatibility, deployment docs |
| Packaging | source archive includes the new product documentation |
| Version metadata | `VERSION`, client/server/Panel fallback identity, current docs |
| Reports | existing R18 architecture qualification report |
| Unexpected | none |

```text
UNEXPECTED_TRACKED_DIFF_COUNT: 0
CORE_DIFF_COUNT: 0
UNEXPECTED_PROTECTED_DIFF_COUNT: 0
```

No files under `core/` were changed. The only server changes are the formal
version identity and test-only telemetry fixture isolation. There are no
changes to wire semantics, scheduler, ACK, retransmission, server data plane,
Native routing, or Native child-outbound semantics.

## Frozen dual-mode architecture

```text
Standalone:
  smp3-client + smp3-server
  one generic SOCKS5 endpoint per logical leg

Native:
  mihomo-smp3 + smp3-server
  Mihomo child outbound integration

Compatibility:
  optional smp3-proxy / sing-box integration
```

Standalone does not implement VLESS, Reality, Hysteria2/HY2, Snell, or other
outer node protocols. It only dials configured SOCKS5 endpoints. Node
protocols, credentials, TLS, selection, and switching remain in external
proxy software. Changing a node behind the same SOCKS5 endpoint does not
require an SMP3 configuration change.

## Exception closure

### Fixed telemetry port

The host had an existing read-only listener on both `127.0.0.1:24500` and
`[::1]:24500`, owned by PID 38320. It was not stopped or modified. The two
aggregation tests do not require the production port; their disposable
fixtures now use `127.0.0.1:0`. The production default remains the literal
loopback `127.0.0.1:24500`.

```text
FIXED_24500_SERVER_TESTS: PASS
```

### Browser smoke

The clean Windows native headless Chrome invocation passed
`TestDashboardBrowserSmoke`. A WSL race run encountered Chrome exit 21 in the
WSL browser environment; that infrastructure-only test was excluded from the
WSL race invocation and is covered by the passing Windows native browser run.

```text
DASHBOARD_BROWSER_SMOKE: PASS
FRONTEND_STATIC_TESTS: PASS
```

### Preferred startup stress

The known timing flake was a test expectation problem. A tiny first write may
be delivered by either attached leg; attachment does not imply equal DATA
distribution. The test now requires DATA on either attached leg and separately
requires both legs to attach. Core and scheduler code were not changed.

```text
STRESS_TIMING_FLAKE_CLASSIFICATION: TEST_FLAKE
STRESS_TEST_FINAL: PASS
```

## Version normalization

```text
TARGET_PRODUCT_VERSION: v2.3.4
VERSION: kit_version=2.3.4 / kit_revision=2.3.4
smp3-client: 2.3.4
smp3-server: 2.3.4
smp3-panel source identity: 2.3.4 (retired standalone fallback)
mihomo-smp3: 2.3.4, upstream Mihomo v1.19.28
smp3-proxy: 1.14.0-beta.14-smp3-2.3.4
```

The Compatibility binary retains the pinned sing-box runtime identity and
adds the SMP3 release suffix. Upstream runtime versions remain separate from
the SMP3 product version.

```text
ALL_BINARY_VERSIONS_MATCH_TARGET: YES
STANDALONE_CONFIG_BACKWARD_COMPATIBLE: YES
```

## Test and static-gate results

| Gate | Result | Evidence |
|---|---|---|
| Standalone Go tests | PASS | client, server, core, command modules |
| Native build/regression | PASS | pinned Mihomo adapter/config tests and artifact build |
| Compatibility build | PASS | pinned sing-box multipath test and artifact build |
| Panel tests | PASS | Panel and monitor modules |
| Server tests | PASS | full Windows native suite |
| Race tests | PASS | Core, Client, Panel, Server excluding WSL-only browser smoke |
| Go vet | PASS | Core, Client, Server, Panel, command modules |
| Frontend static tests | PASS | Node syntax checks for Dashboard and Panel assets |
| Dashboard browser smoke | PASS | Windows native clean headless Chrome |
| Standalone dependency gate | PASS | Mihomo imports 0; sing-box imports 0 |
| Shell/Python syntax | PASS | build and audit scripts |
| Source secret scan | PASS | 212 text files, zero findings |

```text
STANDALONE_TESTS: PASS
NATIVE_TESTS: PASS
PANEL_TESTS: PASS
SERVER_TESTS: PASS
RACE_TESTS: PASS
RELEASE_BUILD: PASS
```

The first `cmd` test attempt used `GOWORK=off` and failed only because the
workspace replacement was intentionally disabled; rerunning with the
repository `go.work` passed. The first Compatibility attempt required a
network clone; the build then passed using the existing pinned checkout and
the required Go 1.25.5 toolchain.

## Standalone contract and transport gates

```text
PER_LEG_GENERIC_SOCKS_CONTRACT: PASS
LEG0_INDEPENDENT_UPSTREAM: PASS
LEG1_INDEPENDENT_UPSTREAM: PASS
LEG0_CROSS_USES_LEG1_UPSTREAM: NO
LEG1_CROSS_USES_LEG0_UPSTREAM: NO
SAME_PROCESS_MULTI_LISTENER_SUPPORTED: YES
DIFFERENT_PROCESS_MULTI_LISTENER_SUPPORTED: YES
BOTH_LEGS_ATTACHED: YES
TCP_INTEGRITY: PASS
UDP_DATAGRAM_INTEGRITY: PASS
FAILURE_ISOLATION: PASS
```

The endpoint contract is process-agnostic: each leg receives an independent
SOCKS5 address, so one-process and two-process external proxy layouts do not
change SMP3 behavior. The deterministic fixtures prove independent listener
binding, TCP/UDP integrity, and no cross-leg fallback.

## Build ownership and artifact provenance

```text
STANDALONE_BUILD_FETCHES_MIHOMO: NO
STANDALONE_BUILD_FETCHES_SING_BOX: NO
STANDALONE_BUILD_BUILDS_MIHOMO: NO
STANDALONE_BUILD_BUILDS_SMP3_PROXY: NO
CLEAN_STANDALONE_BUILD: PASS
BUILD_WORKFLOW_SPLIT: PASS
```

Build commands:

```text
./build.sh
./scripts/build-native.sh
./scripts/build-compatibility.sh
```

All release candidate artifacts were built into the ignored temporary output
directory `.work/r18-r1-final-dist`. `SHA256SUMS` was generated and verified
with `sha256sum -c`. The source-only archive was also created and tested at
`.work/smp3-multipath-kit-2.3.4-source.zip`.

| Product | Artifact | GOOS/GOARCH | Size | SHA256 | Qualified |
|---|---|---|---:|---|---|
| Standalone | `smp3-client-linux-amd64` | linux/amd64 | 4,292,407 | `F21F7C077F6CE54B4C0DABB85829BB519760031219CF09A7CCE26DF81A1F367A` | YES |
| Standalone | `smp3-client-windows-amd64.exe` | windows/amd64 | 4,401,664 | `3288B9FB04B04C52142808F6E9DF3366C0F8B8D44EA5B4DD70CB98511D88214B1` | YES |
| Standalone | `smp3-server-linux-amd64` | linux/amd64 | 8,369,536 | `33B5D987C0B535A864B53F832D224B31C8B092B011491122EE13003735D315AD` | YES |
| Standalone | `smp3-server-windows-amd64.exe` | windows/amd64 | 8,517,632 | `9BD675152F461C5B26E61587197F17A12697A1922BE93A089968745CC677AAAD` | YES |
| Native | `mihomo-smp3-linux-amd64` | linux/amd64 | 50,485,596 | `2CBFE65DFF29F2764D6636C6147F735BCBE439BE3491425C192D0EAB85A899B8` | YES |
| Native | `mihomo-smp3-windows-amd64.exe` | windows/amd64 | 48,447,488 | `416F5BD9C6C192E1160B8FFFB428FA634149E01518D7C97B1EC9E4828266A27B` | YES |
| Compatibility | `smp3-proxy-linux-amd64` | linux/amd64 | 79,876,244 | `295A98A025965AA6FF8B788A6FBD692FCC86D54EB7CC1B8BAFD11E3C7E83E5F4` | YES |
| Compatibility | `smp3-proxy-windows-amd64.exe` | windows/amd64 | 80,539,136 | `DD924F0CBF30BFEEAEB0C0E131987797BBF9685E2669334722C554C40BDF7087` | YES |

Retired standalone Panel binaries were built separately for version identity
verification only and are not an official v2.3.4 release asset. The integrated
Dashboard remains part of `smp3-server`.

## Native/Standalone identity table

| Product | Source HEAD | Artifact SHA | Qualified |
|---|---|---|---|
| Standalone | `06a7d9972f57eef92f9d094e70668d9247461f14` | `F21F7C077F6CE54B4C0DABB85829BB519760031219CF09A7CCE26DF81A1F367A` (client Linux; manifest covers all four) | YES |
| Native | `06a7d9972f57eef92f9d094e70668d9247461f14` + pinned Mihomo `cbd11db1e13a75d8e680e0fe7742c95be4cba2be` | `2CBFE65DFF29F2764D6636C6147F735BCBE439BE3491425C192D0EAB85A899B8` (Linux; manifest covers both) | YES |
| Panel | `06a7d9972f57eef92f9d094e70668d9247461f14` | `A9964C22A8666EA959160B28376BE4838BF118CD82177C8176C852799A670A73` (retired fallback Linux) | YES |

## Security and provenance audit

```text
SOURCE_SECRET_SCAN: PASS
STAGED_SECRET_SCAN: PASS
REPORT_SECRET_SCAN: PASS
RELEASE_ASSET_SECRET_SCAN: PASS
ALL_RELEASE_ARTIFACT_HASHES_VERIFIED: YES
```

The scans found no private keys, credential-bearing deployment files,
subscription URLs, user-home paths, or non-placeholder JSON secret values.
The report and release assets contain hashes and safe artifact metadata only.

## Production boundary

```text
PRODUCTION_CHANGED: NO
R14_CLIENT_CHANGED: NO
CARRIER_A_CHANGED: NO
CARRIER_B_CHANGED: NO
PRODUCTION_SERVER_CHANGED: NO
NATIVE_PRODUCTION_CHANGED: NO
SYNTHETIC_PRODUCTION_C5: 0
PRODUCTION_PERFORMANCE_TEST: NO
MERGED_TO_MAIN: NO
TAG_CREATED: NO
RELEASE_CREATED: NO
```

No production process, configuration, carrier, server, Native runtime,
firewall, route, scheduler, or wire behavior was accessed or mutated.

## Final hardening stdout

```text
R18_R1_VERDICT: A
TARGET_PRODUCT_VERSION: v2.3.4
R18_BRANCH: feature/r18-dual-mode-product
BASE_HEAD: f9d512e0a1b2291c5886aa8e89abbf5489e0f3f6
UNEXPECTED_TRACKED_DIFF_COUNT: 0
FIXED_24500_SERVER_TESTS: PASS
DASHBOARD_BROWSER_SMOKE: PASS
FRONTEND_STATIC_TESTS: PASS
STRESS_TIMING_FLAKE_CLASSIFICATION: TEST_FLAKE
STRESS_TEST_FINAL: PASS
PER_LEG_GENERIC_SOCKS_CONTRACT: PASS
LEG0_INDEPENDENT_UPSTREAM: PASS
LEG1_INDEPENDENT_UPSTREAM: PASS
LEG0_CROSS_USES_LEG1_UPSTREAM: NO
LEG1_CROSS_USES_LEG0_UPSTREAM: NO
SAME_PROCESS_MULTI_LISTENER_SUPPORTED: YES
DIFFERENT_PROCESS_MULTI_LISTENER_SUPPORTED: YES
BOTH_LEGS_ATTACHED: YES
TCP_INTEGRITY: PASS
UDP_DATAGRAM_INTEGRITY: PASS
FAILURE_ISOLATION: PASS
CLEAN_STANDALONE_BUILD: PASS
STANDALONE_BUILD_FETCHES_MIHOMO: NO
STANDALONE_BUILD_FETCHES_SING_BOX: NO
STANDALONE_BUILD_BUILDS_MIHOMO: NO
STANDALONE_BUILD_BUILDS_SMP3_PROXY: NO
NATIVE_BUILD: PASS
NATIVE_REGRESSION: PASS
COMPATIBILITY_BUILD: PASS
CORE_SEMANTICS_CHANGED: NO
WIRE_CHANGED: NO
SCHEDULER_CHANGED: NO
ACK_SEMANTICS_CHANGED: NO
RETRANSMISSION_SEMANTICS_CHANGED: NO
NATIVE_ROUTING_CHANGED: NO
NATIVE_CHILD_OUTBOUND_SEMANTICS_CHANGED: NO
STANDALONE_CONFIG_BACKWARD_COMPATIBLE: YES
ALL_BINARY_VERSIONS_MATCH_TARGET: YES
SOURCE_SECRET_SCAN: PASS
DIFF_SECRET_SCAN: PASS
STAGED_SECRET_SCAN: PASS
REPORT_SECRET_SCAN: PASS
DOCUMENTATION_SPLIT: PASS
BUILD_WORKFLOW_SPLIT: PASS
STAGED_FILE_COUNT: 35
R18_COMMIT: 06a7d9972f57eef92f9d094e70668d9247461f14
VERSION_RELEASE_COMMIT: 06a7d9972f57eef92f9d094e70668d9247461f14
WORKTREE_CLEAN: YES after final report commit
PRODUCTION_CHANGED: NO
R14_CLIENT_CHANGED: NO
CARRIER_A_CHANGED: NO
CARRIER_B_CHANGED: NO
PRODUCTION_SERVER_CHANGED: NO
NATIVE_PRODUCTION_CHANGED: NO
SYNTHETIC_PRODUCTION_C5: 0
PRODUCTION_PERFORMANCE_TEST: NO
MERGED_TO_MAIN: NO
TAG_CREATED: NO
RELEASE_CREATED: NO
READY_FOR_V2_3_4_RELEASE: YES
```

This stage is complete and stops here. Merge, push, tag, GitHub Release, and
production deployment require a separate explicit authorization.
