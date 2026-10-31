# PHASE32_NATIVE_STANDALONE_PARITY

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

Standalone Client/Landing 共用 canonical Core，N-way loopback 通过。Native adapter config/tests 已扩展，但未达到 release parity：scratch HEAD 4e6b1063f7e7c8b765b1f0551d85ab17556c1512，不匹配要求 pin cbd11db1e13a75d8e680e0fe7742c95be4cba2be，helper 绕过严格 pin。Native 构建/运行对 GA 均 NOT RUN，不把 scratch 二进制放入自用包。
