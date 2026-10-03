# Findings

- 当前 HEAD: cc00d40，工作树初始干净。
- 现有 dynamic capacity 是 StreamEngine 内部的 per-stream estimator，StreamStats 已有容量遥测字段。
- 现有 scheduler aggregation 由 StreamConfig.SchedulerMode 控制；CapacityMode dynamic 才创建 estimator。
- 需要先确认 standalone client/server 的 carrier/session ownership，才能定义稳定 CarrierKey。

## Global capacity implementation
- `core/CarrierCapacityRegistry` is an explicit ownership-domain registry. Providers share one estimator per stable key and retain warm state while pruning idle entries after 10 minutes; hard cap 1024 entries.
- `StreamConfig.CapacityProvider` is optional. Existing callers retain the exact per-stream estimator when nil.
- Standalone client derives keys from `client|outbound|<route>|<upstream address>` for each leg and owns one registry for all concurrent SOCKS sessions.
- Server sessions remain owner-isolated because each incoming session has independent carrier sockets; no server-side cross-client aggregation is inferred.
- ACK input remains at cumulative logical ACK retirement; retransmit/rescue paths do not call provider.Ack.

## Dynamic Leg Activation audit
- Legacy activation lives in `core/stream_engine.go:activationLoop`: max logical TX/RX rate against `ThresholdBytesPS` for `ActivationWindow`, plus primary queue 80% fallback; leg0 loss still activates immediately.
- Logical demand enters at `txLoop` after application pipe read (`ingressBytes`) and is retired only by cumulative useful ACK in `handleAck`; retry/rescue paths do not add demand.
- `rxDeliveredBytes` represents data delivered to the local application. Remote download demand is therefore measured at the remote sender's logical ingress, while the same core behavior remains valid for each scheduling owner.
- Dynamic mode now consumes optional `StreamActivationProvider` capacity and aggregate demand. Registry demand is keyed by primary CarrierKey and shared across streams. Invalid/stale capacity falls back to configured baseline; high-confidence overload can use a 200ms evidence window, normal overload uses the configured window, with a 1.10 margin.
- `activation_mode` omitted/legacy preserves v2.5 behavior; dynamic requires aggregation in client, server, and native Mihomo parsing.

## Dynamic leg activation final closure
- Dynamic activation implementation and evidence are complete; report and race status are tracked.
- Final gate is local commit plus clean tracked-source archive verification. Push, tag, release, and deployment are explicitly out of scope.

## Memory closure initial audit
- Base HEAD 7f57ebe9fb5d50efb6b3d0fbda2f5cd578c97e7e, initial worktree clean.
- closureRun uses io.CopyN with generated payload and streaming checker; no whole-payload storage.
- Core RX has a per-engine sync.Pool; pending channels and inflight bound require quantitative attribution.

## Memory Attribution and Stability Closure (2026-10-04)
- Five same-process 1GiB rounds passed with payload PASS, retry/rescue/ledger 0, goroutines 2, FDs 6, registry entries 2 and refs 0; HeapAlloc after GC remained 55–64MB and HeapObjects 3.7–4.6k.
- Heap profiles attribute retained bytes to bounded RX `sync.Pool` buffers and diagnostic net.Pipe objects; no registry, ACK, pending-record, or demand-accounting owner dominated live heap.
- Real standalone 5GiB and 20GiB repeated traffic completed without restart; 1000 real SOCKS stream churn cycles returned stable FDs and bounded RSS.
- A 30GiB gctrace run kept live heap 2–4MB and RSS about 9–14MB client / 12–20MB server, explaining high-RSS runs as allocator/GC retention.
- Classification is `A. MEMORY_BEHAVIOR_BOUNDED_AND_EXPLAINED`; no production runtime fix justified.

## HOL-aware assignment findings
- Production aggregation planning is `assignBenchmark`; normalized `assignedService` remains the entitlement authority.
- Existing wire state has no trustworthy RTT sample. Smoothed per-leg write latency is the smallest available proxy and is explicitly documented.
- Pending depth and sent-minus-useful-ACK bytes are already bounded, event-driven scheduler state; retry/rescue paths never enter normal HOL correction.
- An atomic sender ACK frontier avoids taking the TX ledger mutex on each assignment. Sequence-stride sampling keeps correction work out of most bulk records.
- Shared-carrier and 1GiB evidence show no amplification or rescue cascade; dynamic activation and reconnect preserve their existing ownership/epoch rules.

## Global carrier promotion re-audit
- Current baseline is 3f0c52b, descendant of the original deca4b2 implementation and df900ab closure.
- Report already contains real process smoke, 1/4/8 stream CPU/RSS and 1GiB evidence. Provider event tests lack explicit mixed demand, trained warm reuse and shared step-down/up assertions; these will be added without changing production semantics.

## Global carrier final re-audit findings
- Capacity invariance is proven at the provider layer: global estimate remains 91 Mbps while per-stream useful service scales from 100 to 12.5 Mbps across 1 to 8 streams.
- Mixed demand does not reduce a trained carrier estimate when demand is below physical service; valid capacity knowledge is frozen under underload.
- Shared step transitions are observed by all providers because the estimator is shared by CarrierKey; 80 Mbps and 200 Mbps settling values match across providers.
- The process smoke confirmed exact real SOCKS payloads and process liveness, while quantitative carrier telemetry remains an in-process API rather than a CLI log.
- Native broad test hang is a test-runner/package lifecycle issue after dependencies were available; build, adapter/config/constant tests, and adapter race are clean.
