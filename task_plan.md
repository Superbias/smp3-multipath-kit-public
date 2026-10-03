# SMP3 Global Carrier Capacity

## 目标
实现共享物理载波容量模型，保留单流 dynamic 语义，完成多流、隔离、生命周期、并发与 native race 验证。本目标只本地完成，push/merge/tag/release/deployment 均为 NO。

## 阶段
- [x] 1. 拓扑审计与 CarrierKey/ownership 设计
- [x] 2. Core 全局载波容量 provider/registry 实现
- [x] 3. client/server/Mihomo 接线与隔离
- [x] 4. 单流兼容与多流/生命周期/身份测试
- [~] 5. 基准、压力、CPU/RSS 与 1GiB 长流验证 (local deterministic matrix complete; long-run/CPU/RSS deferred)
- [~] 6. native Linux race 与 tracked-only 验证 (WSL race complete; remote native unavailable)
- [~] 7. 报告、文档、版本状态与最终工作树 (interim promotion report; final gates remain)

## 约束
- 不修改 release/tag/deployment；不自动 push/merge。
- 保留 per-stream estimator 的现有语义与配置兼容性。
- 不引入跨客户端、跨方向、跨物理载波的共享状态。

## Dynamic Leg Activation continuation (2026-10-03)
- [x] Audit legacy activation path and preserve legacy trigger as explicit compatibility mode.
- [x] Add explicit `activation_mode` validation/mapping to standalone client/server and native Mihomo adapter.
- [x] Add capacity-relative activation using logical demand, shared Carrier Capacity, margin, sustained evidence, and congestion fallback.
- [x] Add shared demand aggregation and activation telemetry.
- [ ] Complete final scenario matrix, tracked-only validation, race, and production candidate report.

## Phase 5 — Dynamic activation final closure
- [x] Implement explicit legacy/dynamic activation modes and shared capacity-relative demand logic
- [x] Validate unit, integration, native Mihomo, standalone, global 1GiB, and race evidence
- [x] Write `SMP3_DYNAMIC_LEG_ACTIVATION_REPORT.md`
- [ ] Commit local candidate and validate a clean tracked-source archive
- [ ] Final classification only after all gates pass

## Final gate result
- [x] Local candidate committed at `3609b7055c02218ec02fea71e70d2bef2bc8c390`
- [x] Clean tracked-source archive tests and Windows/Linux amd64 builds passed
- [x] Working tree clean; no push, tag, release, or deployment performed
- [x] Final classification: `A. DYNAMIC_LEG_ACTIVATION_PRODUCTION_CANDIDATE_VALIDATED`

## Memory Attribution and Stability Closure (2026-10-04)
- [ ] Instrument and reproduce baseline; run 5 same-process 1GiB rounds with GC snapshots and heap profiles
- [ ] Attribute payload harness, queues, pools, registry, goroutines and FDs; compare 1/4/8 stream GC pressure
- [ ] Validate actual standalone client/server >=5GiB and churn; native smoke where practical
- [ ] Fix causal lifecycle defects if found and rerun required gates
- [ ] Report, local commit, final worktree checks; no push/merge/tag/release/deploy

## Phase 6 — Memory attribution and stability closure
- [x] Reproduce five same-process 1GiB rounds with before/after/idle GC memory samples
- [x] Capture heap profiles and attribute live allocations to bounded RX pool / harness objects
- [x] Validate standalone 5GiB+, extended repeated traffic, 1000-stream churn, registry, goroutine and FD stability
- [x] Validate GC pressure, throughput and native evidence; no production fix justified
- [x] Write `SMP3_MEMORY_STABILITY_REPORT.md`
- [ ] Run final tracked-only archive/race gates and commit locally
- [ ] Final classification after clean worktree

## Final closure gate
- [x] Core/client/server/cmd race passed after diagnostic test addition
- [x] Final `git archive HEAD` tests passed
- [x] Windows amd64 and Linux amd64 client/server archive builds passed
- [x] Final classification: `A. MEMORY_BEHAVIOR_BOUNDED_AND_EXPLAINED`
- [x] Final worktree clean; push/merge/tag/release/deployment all NO
