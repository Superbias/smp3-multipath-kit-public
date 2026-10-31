# SMP3 shared Core: incremental audit, final C1

**Current result: targeted frontier recovery PASS; release remains FAIL.** This replaces earlier interim incremental reports whose run counts and gates predated the final Native and Standalone matrices. Full report: docs/SMP3_SHARED_CORE_RECOVERY_FIX.md.

## Increment from frozen C0

C0 is the frozen 341-file source ZIP with SHA256 ed7a83d9d60bbe9d7c75d98b3b4644d6db2bdcba1c88a053a9306ef36b01fd4a. The production behavior change in C1 is localized to core/stream_admission.go protectFrontier: recovering probes use the frontier exposure reference even when measured feedback exceeds the probe interval. Pre-frontier queue residence no longer consumes the probe's recovery opportunity. The 200 ms and 700 ms boundaries, quarter-second frame budget, APIs, wire format, dependencies, and Mihomo pin are unchanged.

The red C0/green C1 regression test is TestClosureMeasuredFeedbackStartsProbeExposureAtFrontier. The boundary test is TestClosureRecoveryBudgetBoundariesRemainDistinct. C1 Native and Standalone binaries both resolve the canonical smp3core package to this core/ tree. Build and per-file hashes are indexed with the exact artifacts.

## Audit result

Pending owner migration, idempotent retirement, generation fences, budget-shrink exit paths, bounded seed, control/EOF/cancel progress were inspected in source and cross-checked against focused tests. The audit found no additional production defect requiring an expanded change. Gaps remain: no exhaustive four-actor retirement permutation; no all-carriers-dead delivery guarantee; no physical-WAN global probe staggering; and production route/reload/session behavior is untested. See the full invariant table in the final report.

## Evidence and gates

Final C1 Native and Standalone each pass 24 healthy process cases and 24 slow/blackhole recovery cases across 20/100/200/400 ms, three runs per condition. Each host has a separate 1,800-second shared-flow soak. C0 healthy single-leg medians remain the before comparison; the historical B floor and 55–65 second window are unchanged.

Core test/vet/race and all five go mod verify runs pass. Linux raw Carrier calibration passes. Windows raw calibration fails at 33.397/33.974 Mbps versus a 95 Mbps floor; this is a test-harness/platform outcome, not a Core result. B0 legacy race3 and queue race30 failures remain documented. Migration parity is B0 31 / C0 57 / C1 59 and remains FAIL; two new C1 test paths await parity policy review. Production validation is NOT RUN / ENVIRONMENT BLOCKED.

Exact raw logs, JSON, commands, exits, two-endpoint configs, patches, source snapshots, binaries, and SHA256 crosswalks are included in the shared-core-recovery-20261014 evidence delivery.
