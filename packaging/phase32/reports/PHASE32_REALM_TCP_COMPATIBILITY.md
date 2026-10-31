# PHASE32_REALM_TCP_COMPATIBILITY

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

Realm 是透明 TCP 字节转发，不解析 SMP3 Session/Leg/DATA/ACK/Recovery。Direct 和多个 Realm 转发腿可指向同一 Landing listener，TLS 由 Landing 终止；不需要部署新 SMP3 Front。Phase32 未运行 Realm 进程或公网路径，真实 Realm 集成 NOT RUN。
