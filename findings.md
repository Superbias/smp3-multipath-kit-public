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
