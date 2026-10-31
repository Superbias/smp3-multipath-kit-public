# PHASE32_FINAL_REPORT

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

## 结论

统一 Core 已从固定双腿改为运行时 N-leg 注册与 B0 调度。TCP loopback 集成测试覆盖 N=1/2/3/4/8/16，每条配置腿均贡献唯一 DATA，传输有时间重叠；Landing 在单监听端口接受 N 条腿。P0-C=OPEN/FAIL，v2.7 GA=FAIL/NOT READY。

| N | Core 唯一贡献 | Landing 单监听 | SHA/EOF/ACK/账本 |
|---:|---|---|---|
| 1 | 1/1 | — | PASS |
| 2 | 2/2 | 兼容路径 | PASS |
| 3 | 3/3 | PASS，另有 TLS | PASS |
| 4 | 4/4 | PASS | PASS |
| 8 | 8/8 | PASS | PASS |
| 16 | 16/16 | PASS | PASS |

Core 每个 N 传输 32 MiB；Landing 每个 N 为 4 MiB。所有为本机 loopback，不是 WAN 容量结果。LegID uint8 表示 0–255；默认/实用上限 16，可配置到 256，受 OS、内存和 FD 限制。Core/Landing attach API 的 2→4 同 Session 热添加已测；Standalone JSON 改动需要重启。

Windows Go1.25.5 full modules/Vet 与 WSL Go1.25.5 full/Race 通过。Linux amd64/arm64 Client+Landing 及 Windows amd64 Client 构建通过；ARM64 未实机运行。Native scratch HEAD 不匹配 upstream pin，不能视为 release build。Realm、公网 WAN、物理 RTT/速率、按 N CPU/RSS/锁争用、长时多会话均未测。无推送、发布、部署或生产配置更改。
