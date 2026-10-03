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
