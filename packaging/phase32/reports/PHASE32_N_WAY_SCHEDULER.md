# PHASE32_N_WAY_SCHEDULER

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

B0 是默认 N-way 调度路径，在同一个 Core Scheduler 中管理变长 leg 集合。loopback 测试每个 N 的所有 leg 都有唯一 DATA 并存在 100 ms overlap。物理 RTT/服务率未测，不作线性聚合结论。N=2 功能兼容通过，时序等价未证明。B1 不作为 N-way 功能交付。
