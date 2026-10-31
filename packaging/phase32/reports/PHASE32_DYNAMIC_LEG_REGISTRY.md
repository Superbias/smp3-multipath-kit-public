# PHASE32_DYNAMIC_LEG_REGISTRY

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

Core 使用动态 leg registry。N=1/2/3/4/8/16、失败/rejoin、generation 递增和 Landing 同端口 join 已测。Leg2 以 generation 2 回归且 Session 不变。Core/Landing attach API 2→4 通过。Standalone 运行时 JSON 编辑不热添加；任意健康 Leg 的 CLI 在线删除能力未宣称。
