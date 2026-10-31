# PHASE32_N_LEG_SCALING_AND_RESOURCES

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

N=16 测试建立16条 leg 并每条贡献 unique DATA，结束后 ledger empty。全 Core 集成测试进程 max RSS 294464 KiB，为整个进程，不是 N=16 的 client/server 实测。按 N CPU/RSS/alloc/锁争用、goroutine/FD 长时趋势均 NOT MEASURED。建议在受控主机从小 N 起步并观测。
