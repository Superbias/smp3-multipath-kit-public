# SMP3 shared Core recovery audit and release evidence

**Decision: C1 targeted recovery result PASS; overall release remains FAIL / NOT READY.** The measured frontier-clock defect is fixed in the candidate and the isolated Native and Standalone recovery matrices pass. Migration parity still reports 59 failures, the legacy race/queue gate has baseline failures, Windows raw-carrier calibration is below its floor, and production validation was not run. No production service, profile, route, installed binary, release, or remote repository was changed.

## Candidate identity and deliverables

- B0 is baseline commit 907b242c5f2532f774a3db1a877cf32740dcdfab.
- C0 is the exact 341-file frozen source tree extracted from artifacts/blocker-closure-20261013/frozen-delivery/source.zip, SHA256 ed7a83d9d60bbe9d7c75d98b3b4644d6db2bdcba1c88a053a9306ef36b01fd4a.
- C1 is the final local candidate after this audit's source and documentation edits. The delivery directory contains the complete B0-to-C1 patch, C0-to-C1 incremental patch, source snapshot, patch-application checks, SHA256 manifests, and raw evidence index. Ordinary git diff omits new files, so the delivered patches are generated from complete trees and include untracked production source, tests, scripts, and reports.
- The prior archive SMP3-blocker-closure-all-deliverables-20261014.zip is retained byte-for-byte; SHA256 dd283ec9778d82f67f609cc63acc5f72dfaf384a309e833f0906b042a8877018. The new package includes the prior archive as an unchanged nested package and adds the C1 supplement.
- C1 does not change protocol/API shape, dependency pins, or the 200 ms probe and 700 ms proof limits. Native's Mihomo pin remains commit cbd11db1e13a75d8e680e0fe7742c95be4cba2be.

Native and Standalone resolve github.com/Superbias/smp3-multipath-kit-public/smp3core to the same canonical local core/ directory. The exact go list resolution, GoFiles, production Core file hashes, Go version, and worktree revision are retained under artifacts/shared-core-recovery-20261014/source-resolution-final2/. The current Go test workspace used Go 1.22.6; the final C1 Native and Standalone binary builds used Go 1.24.4. These toolchains are recorded separately.

The six final C1 test binaries were built from this source tree and their binary hashes are bound to the build logs and process matrices in the delivery crosswalk:

| Test binary | SHA256 |
|---|---|
| Native Linux amd64 | 6e7df40d6e6115c028141cdde677fcbd4387b4e34d89f05f5f110c4843e4b82f |
| Native Windows amd64 | b9b11fe0eb335dd5eedff0433c4c9279d84c0f80f4b4a1f0cea698eeb5fda5a2 |
| Standalone server Linux amd64 | ee60ce6a7865421d621bba0aec956648dd9b5dd40d939f92b89038480635e58f |
| Standalone server Windows amd64 | 569f678168cc61caf031d55a8e3f1dfa18419306ca8d51cb7a8f60713929861b |
| Standalone client Linux amd64 | c6558f983bc50a6e5c2049f782e20570b62d2315f330688cc685270860951f95 |
| Standalone client Windows amd64 | 9474b3b11166ca4116ac68182ad64f6b6cb8de7cc010caf88c195d90b29b71b0 |

Native build command/log and sha256 manifest are in c1-native-build-final2/; its pinned Mihomo checkout used the recorded upstream commit and local Core replace. Standalone build command/log and sha256 manifest are in c1-standalone-build-final2/; it built the four targets from the root module workspace. Per-file C1 source hashes are in delivery/C1-source-sha256-manifest.json, and the compiled Core GoFiles list is in source-resolution-final2/c1-core-production-files-sha256.txt. These are isolated test artifacts, not deployment instructions.

## Root cause and minimal change

When a recovering leg's cumulative-ACK frontier was blocked behind an older record, the later candidate could wait before its first write. At higher measured feedback, protectFrontier called probeReference as an ordinary record; that path could count pre-frontier residence toward probe age and declare the probe overdue before it had been exposed at the frontier. The recovery controller could then restrict it without giving the unique bytes a fair bounded opportunity to produce ACK evidence.

C1 changes only the recovering branch in core/stream_admission.go, protectFrontier around lines 501–529: it always obtains the recovering probe reference through probeReference(candidate.record, s.recovering). StreamTXLedger.probeReference in core/stream_tx.go uses the record's frontier exposure time, advanced to first-write time when later. The existing feedback-based horizon, 0.25-second frame budget, 200 ms probe limit, 700 ms unique-ACK proof, and bounded retry/rescue behavior remain in force.

TestClosureMeasuredFeedbackStartsProbeExposureAtFrontier in core/stream_blocker_feedback_test.go:137 is red on C0 and green on C1. The raw outputs are retained at c0-frontier-red-run1/ and c1-frontier-green-run1/. TestClosureRecoveryBudgetBoundariesRemainDistinct at line 162 checks 48 frames at 100 Mbps with 64 KiB chunks, the 200 ms probe boundary, and acceptance at 700 ms versus rejection at 700 ms plus 1 ns. No timeout was enlarged to obtain a pass.

## Invariant audit

| Invariant | Actual implementation and test evidence | Remaining boundary |
|---|---|---|
| Pending owner migration and virtual-service rollback happen once | core/stream_assignment_debug.go:159–282 serializes assignment under weightedMu; core/stream_tx.go:595–619 makes same-owner selection idempotent and moves pending bytes only on an owner change. Covered by TestReleaseSamePendingOwnerDoesNotRollback, TestSlowLegAdmissionMigrationAndACKRace, and TestSlowLegRetiredUnassignedPendingDoesNotStallPlanner. | No exhaustive schedule combines feeder shutdown, planner wake-up, ACK retirement, and owner migration. |
| ACK, rescue, Close, and ReplaceLeg retire work once | core/stream_tx.go:346–420, 423–554 matches attempt cleanup to its exact attempt start and retires only extant records on an advancing cumulative ACK. core/stream_engine.go:616–710, 865–971, 1634+ handles generation replacement, close, and worker cleanup. Tests include concurrent ACK/attempt invalidation, rescue ACK before write commit, stale rescue and normal cleanup, leg replacement, and graceful tail drain. | There is no exhaustive four-actor permutation test for ACK/rescue/Close/ReplaceLeg. |
| Old references or generations cannot prove new recovery | core/stream_admission.go:238–355 resets feedback/cohorts on rejoin and rejects old epochs, frontier, or generations; core/stream_tx.go:815–842 associates references with current exposure. Covered by TestReleaseRetiredGenerationCannotProveRetainedRecovery, TestReleaseStaleACKResultCannotProveNewRecovery, TestSlowLegRescueDoesNotChangeGuardOwnerOrProveRejoin, and stale-attempt-clear tests. | Arbitrary external route changes, hidden transport reuse, and every mixed-version timing are not exhaustively modeled. |
| A smaller admission budget does not strand already admitted work | core/stream_admission.go:131–175 restricts new ordinary admission; already admitted records remain in the TX ledger and can leave through ACK, bounded rescue, recovery/replacement, or close. Covered by short-record/hard-bound, ACK/rescue/drain, and ReplaceLeg tests. | No delivery guarantee exists after all carriers stop. A dedicated test does not combine budget shrink, final-leg loss, and close in every ordering. |
| Recovery seed and recurring probes remain bounded | core/stream_admission.go:195–235, 299–480 seeds from actual records and requires fresh unique bytes/service to advance one bounded stage; frameBudget bounds the normal pipeline to a quarter second. Count-only, stale-meter, frontier-clock, and boundary tests cover limits. | Per-stream cooldown permits periodic probes; streams sharing a physical WAN are not globally phase-staggered when physical carrier identity is unavailable. The full timer/ACK/reorder interleaving is not exhaustively tested. The real-process soak shows an early post-restore dip described below. |
| Control, last available sender, EOF, and cancellation can progress | core/stream_engine.go:865–971, 1073–1118, 2785–2835 bounds CLOSE, separates graceful drain from fatal close, and releases providers once. Tests cover full control queues, bounded EOF notification, graceful drain, replacement, and provider lifecycle; finite Native/Standalone transfers verify payload and EOF/drain. | CLOSE has no peer receipt ACK. The deliberate-cancel soak is not an orderly EOF test. Production route/reload effects remain untested. |

The six checks found no new Core defect requiring more code changes. The remaining boundaries above are audit limits, not claims of proof.

## RTT, carrier, and real-process evidence

All process matrices use sanitized loopback client and server configurations retained with each run. Each run records both endpoint configs, binary hashes, RTT injection provenance, raw log, JSON result, exit code, payload hash, EOF/drain, and carrier connection counts. The performance window is fixed at seconds 55–65. The comparison threshold is based on frozen C0; C1 measurements never lower B.

The frozen historical single-leg reference is B=99.978370 Mbps, with a fixed 1.5B=149.967555 Mbps recovery threshold. The separately measured C0 medians are lower at some RTTs, but the historical floor is retained for acceptance. For 20/100/200/400 ms, C0-to-C1 single-leg median changes are:

| RTT | Native C0 → C1 median (Mbps) | Native delta | Standalone C0 → C1 median (Mbps) | Standalone delta |
|---:|---:|---:|---:|---:|
| 20 ms | 99.660 → 99.843 | +0.184% | 99.801 → 99.865 | +0.065% |
| 100 ms | 99.843 → 99.708 | −0.135% | 99.863 → 99.852 | −0.011% |
| 200 ms | 99.611 → 99.848 | +0.238% | 99.857 → 99.945 | +0.088% |
| 400 ms | 99.641 → 99.679 | +0.039% | 99.668 → 99.632 | −0.036% |

This is the required pre/post healthy-single comparison against the unchanged C0 measurement. No observed median drop exceeded 0.14%; acceptance did not use a lowered B. Healthy dual medians were also compared; the largest negative median change was −1.312% for Standalone at 400 ms and −0.460% for Native at 200 ms. These loopback results do not certify production Carrier behavior.

Native C0 and Standalone C0 each completed 24/24 healthy runs (20/100/200/400 ms × single/double × 3). C1 Native and C1 Standalone each completed 24/24 healthy runs and 24/24 recovery runs (four RTTs × slow/blackhole × 3). All recovery cases passed against the frozen 1.5B threshold with exact payload SHA, normal EOF/server drain, expected carrier connections [1,1], and the tested binary hashes. The minimum C1 fixed recovery window was 159.646 Mbps for Native (400 ms slow, run 3) and 171.232 Mbps for Standalone (400 ms slow); both exceed the unchanged 149.968 Mbps threshold. Complete per-run samples are in native-c1-recovery-vs-c0-acceptance.json and standalone-c1-recovery-vs-c0-acceptance.json.

The C1 Native Core unit-level RTT matrix passed 16 scenarios at 20/100/200/400 ms across healthy single, healthy dual, slow, and recovery. Each scenario records 41 one-second samples. It uses a simulated Core carrier and is not counted as a real Native process run.

Raw Carrier calibration is kept separate from Core. On Linux, the test tool passed 50 Mbps, 200 Mbps, simultaneous 50+200 Mbps, and fragmented 100 Mbps calibration, with verified payload prefixes. The Windows in-memory raw calibration **failed**: at 400 ms RTT its 100 Mbps configuration delivered 33.397 and 33.974 Mbps in the two fixed windows, below the 95 Mbps floor. This is a test-harness/platform result (releaseCarrier without StreamEngine/Core), not a Core throughput result; Windows throughput certification remains **FAIL / NOT ESTABLISHED**. The failure is preserved in windows-raw-rtt400-100m-attempt1/. Linux carrier calibration does not erase it.

## Thirty-minute shared-flow soak

Both final C1 Native+server and C1 Standalone client+server ran 1,800 scheduled traffic seconds with 15 repeated slow/blackhole/recovery cycles. Each cycle changes the second loopback carrier from healthy to 1 Mbps, blackholes it, restores it, and adds and cancels application flows under a shared pair of physical test carriers. The experiments produced 180 fixed 10-second windows, 15 flow joins, and 30 flow exits. Both ended with exit code 0, verified flow prefixes, and no recorded liveness errors. These are calibrated loopback tests, not the user's Snell/HY2/Reality carriers.

For the final Native run, the first 10-second post-restore window fell below frozen 1.5B in 10/15 cycles (minimum 132.802 Mbps); the next fixed 10-second window passed in 15/15 cycles (minimum 190.344 Mbps). This dip is retained as a measured limitation; the standalone soak retains every window and its exact values in its raw log. Maximum Native harness scheduler lag was 1.594 ms; the Standalone raw log records up to 2 seconds of per-flow zero progress in sampled windows. The Native soak reported server RSS 37,608–232,664 KiB, Native RSS 43,492–136,620 KiB, and stable FD ranges; the Standalone soak separately logs process resources. Public process telemetry does not expose provider reference counts, so process logs do not claim them. The Core long-soak test separately checks provider references, pending work, cancellation, and resource release. Long soaks cancel prefix-verified flows and do not assert orderly EOF; separate finite process tests cover EOF/drain.

The soak confirms the 10–20-second recovery window and bounds resource trends in this test environment; it does not justify changing the 200 ms rescue or 700 ms proof boundary. Do not reclassify early-window dips as PASS by shifting a window.

## Cross-version interoperability

Both local new-connection directions passed with exact process binaries and hashes: C1 Standalone server with C0 Native client, and C0 Standalone server with C1 Native client. Each transferred and verified a 1 GiB payload, completed EOF/drain, and reported both carrier connections. The first C0-server/C1-Native attempt failed before protocol setup because its telemetry loopback port was already occupied; that raw failure is retained, and the retry on a fresh port passed. The old server is not expected to gain the new downlink recovery behavior. These tests establish only the tested local protocol combinations and new sessions; they do not establish arbitrary-version or in-flight session survival.

## Engineering gates and remaining blockers

| Gate | Evidence | Result |
|---|---|---|
| Core Go tests, vet, race | go-test-all-modules-*, go-vet-all-modules-*, go-race-all-modules-*; all modules in the local workspace | PASS, exit 0; Go 1.22.6 |
| Module integrity | go-mod-verify-summary.txt; core/client/server/cmd-smp3-client/cmd-smp3-server, with GOWORK disabled | PASS, all five exit 0 |
| Legacy full race x3 | B0/C0/C1 legacy-*-race3-run1-* | B0 FAIL: TestCoreSilentPrimaryStallActivatesBooster failed all 3 iterations; C0 and C1 PASS. The runner is the generated Standalone-shaped test façade that calls shared Core, not the actual Standalone client process. This B0 failure predates C1 but does not by itself rule out interaction. |
| Legacy preferred-queue race x30 | legacy-*-queue-race30-run* | C0 and C1 pass on both retained runs. B0 run 1 failed; B0 run 2 had 3 failed iterations, including preferred-queue activation and logical-writer wake-up. It is timing-sensitive and remains a pre-existing risk, not a harmless failure. The test is TestCorePrimaryQueueSaturationImmediatelyActivatesBooster in the façade's src/protocol/multipath/core_test.go; the fixture does not enable the slow-leg guard, but the shared ACK/accounting/writer/lifecycle code changed, so interaction remains relevant. |
| Migration parity | parity-matrix-run1/parity-matrix.json, raw checker output | **FAIL**: B0 31, C0 57, C1 59 failures. B0→C0 added 26 and removed 0. C0→C1 added 2 and removed 0: the two new C1 test paths stream_release_matched_config_test.go and stream_release_raw_rtt_test.go. Related source records changed in stream_admission.go, stream_blocker_feedback_test.go, and stream_release_audit_test.go. These test additions are expected for this task, but still require explicit parity review/approval; no golden, allowlist, checker, or skip was changed. The full failure set and per-record source/hash classification are retained. |

The parity helper ran the original checker against B0, C0, and C1 and restored/verified the original CORE_SOURCE_MANIFEST_AFTER bytes each time. Across the comparison, files are classified as identical, newline-only normalization, source-content change, or added/removed source. C0's 57 must not be confused with an earlier historical input-51 snapshot. The release remains FAIL until all 59 C1 parity records are reviewed and the policy owner resolves the two new paths.

## Rollback and limited-validation plan

Three operations have different effects:

1. **Policy off:** capacity-mode fixed disables the dynamic admission/recovery policy for comparison. It leaves shared ACK, queue, rescue, CLOSE, and lifecycle code active. It is not a reliable production stop-loss based on these tests.
2. **Code rollback:** restore exact reviewed client/server binaries and their matching configs. Restart or client exit can interrupt existing sessions. Protocol interop does not promise existing-session continuity.
3. **Business stop-loss candidate:** route one observable test application's **new** connection through the separately selectable leg0 Snell route. Prior local operator evidence records three lightweight HTTPS probes through that selector (119/155/147 ms); that establishes basic reachability only. It does not establish current production destination, sustained business health, or the effective route after profile changes. Confirm those manually before relying on it. Application owners decide whether to retry or terminate old stalled connections.

The executable manual aids are scripts/manual-smp3-native-rollback.ps1 and scripts/manual-smp3-server-rollback.sh. Their Plan mode is read-only. Backup/Restore were not run. Before a limited validation, an authorized operator must record both installed versions/commits and binary hashes; effective sanitized settings on both ends; live per-leg RTT; the unchanged healthy leg0 single-path B; and the selected test application's route. Compare old and new connections separately. Use the existing production SLO and stop on integrity/EOF error, the existing throughput floor, an application pause above the existing stop threshold, cycling recovery, growing ledger/resources, or loss of telemetry. Observe one test application and one new connection first; review before any expansion. Any route reload, binary replacement, or restart may drop old sessions.

Current live server/client versions, real production RTT, end-to-end leg0 application capacity, and provider-reference telemetry are not present in this local evidence set. Production validation and canary are **NOT RUN / ENVIRONMENT BLOCKED**. No operator authorization was inferred from the provided connection details or node name.

The delivery verification applies the complete B0 patch and the incremental C0 patch to separate clean copies outside the active repository. Both reconstruct the C1 tree after normalizing text line endings; the C0 application has 15 raw-byte text differences, and each is confirmed to be CRLF/LF-only. The C1 source ZIP and per-file source manifest preserve the exact C1 source bytes and hashes used to build.

## Evidence map

The release supplement preserves original raw logs, JSON, exact commands, and exit-code files under artifacts/shared-core-recovery-20261014/. Native and Standalone per-run folders contain both endpoint configurations and raw process outputs; C0, C1, and B0 build directories contain build inputs/logs and binary hash manifests. The delivery index maps source files to builds, binaries to tests, and tests to logs/configs. The source patches include untracked files; the evidence archive excludes credentials and production access material.
