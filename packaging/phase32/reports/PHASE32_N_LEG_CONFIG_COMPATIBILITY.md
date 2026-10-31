# PHASE32_N_LEG_CONFIG_COMPATIBILITY

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

Client 用变长 leg list，配置校验 LegID 唯一及 max_active_legs。2/3/4/8 样板校验通过。uint8 ID 0–255，配置最多256，建议从小 N 开始。当前 N>2 限 TCP+B0；Client UDP 仍最多两腿；N-way B1 隔离。运行中改 JSON 需重启。
