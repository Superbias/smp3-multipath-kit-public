# PHASE32_N_LEG_CORE_DESIGN

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

每个应用流对应一个 client Core session 与一个 Landing Core session，各自管理动态 legs。DATA sequence、ACK frontier、TX ledger、RX pending/reassembly 与 close 由共享 Core 持有。LegID uint8 保持线协议兼容，0–255；配置默认/实用上限 16，硬范围最多 256，不是资源承诺。Registry 同步保护，调度读取快照，Carrier Write 不持有全局 Session 锁。Rejoin 前进 generation。N=2 功能通过，逐包分配或时延完全等价于旧版未证明。
