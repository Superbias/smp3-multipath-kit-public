# PHASE32_N_LEG_FAILURE_RECOVERY

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

本地失败矩阵：N=3 fail1；N=4 fail1/2/3；N=8 fail1–7；N=16 fail8。每例 1 MiB，严格 EOF、SHA、全 ACK、唯一字节、TX/RX ledger empty。Leg2 generation 1 故障后以 generation 2 rejoin，Session 保持。全腿失效重复10次返回有界 no-transport error。真实 TCP 黑洞/WAN 本轮 NOT RUN，不借用旧阶段证据。
