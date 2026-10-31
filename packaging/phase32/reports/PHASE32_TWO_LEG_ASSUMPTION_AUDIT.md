# PHASE32_TWO_LEG_ASSUMPTION_AUDIT

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

## 审计结果

审查 Core、TX/RX、ACK/Recovery、Client/Server 配置、Carrier、Session/Leg、Telemetry、Native adapter、测试和部署样板。没有机械替换所有双元素数组，按语义区分 leg 与方向/端点对。

| 子系统 | Phase32 处置 | 仍有限制 |
|---|---|---|
| Core leg state | 动态 registry/LegID/Generation | 长时压力未测 |
| LegID | 保留 uint8 线协议，0–255 | 上限不等于资源保证 |
| ACK/RX | 单一 sequence/frontier/reassembly | 外部 WAN 未测 |
| Client config | 变长 legs list | UDP N>2 不支持 |
| Landing | 同端口 Session join/rejoin | 公网 Realm 未跑 |
| Telemetry | 按注册腿导出 | 监控负载未测 |
| Native | N-list adapter 调用 shared Core | 精确 pin/runtime 未验证 |
| B1 | N>2 限制/隔离 | N-way B1 未验收 |

仅代表当前候选源码及列出的测试覆盖，不声称审计所有 downstream fork。
