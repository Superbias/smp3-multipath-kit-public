# 已知问题与未完成验收

1. **Windows Core RTT 测试失败（继承的 Phase 30 结果）**：`TestPhase28ActiveCarrierHOLPilotTCP` 在 Windows 等待双向新鲜 RTT 样本超时。Phase 31 没有通过增大超时或跳过该测试伪造 PASS。Standalone Windows Client 生产二进制可构建和启动检查，但不代表该 Core 测试失败已关闭。
2. **Kernel TCP Blackhole：NOT RUN**。本轮没有获准/可用的独立内核网络命名空间验收。
3. **Native Mihomo：NOT BUILT / NOT RUN**。源码 adapter 注入工作流存在，但本轮没有构建第三方 Mihomo 或运行 Native 进程。
4. **真实 WAN/物理双路由：NOT RUN**。Phase 30 的本地 TCP/TLS 三进程结果不代表实际公网路径。
5. **严格零补发门禁：FAIL / NOT CLOSED**。Phase 17–20 的健康慢腿观测仍显示 Core Retry/Rescue 需按既定规范评估；Phase 31 未扩大或掩盖该门禁。
6. **B1：EXPERIMENTAL / DISABLED**。300 bp 自然预算耗尽未验证，生产 CLI 不暴露 B1 开关。
7. **Windows/Linux 512 MiB 60 秒发布门禁：NOT RUN in Phase 31**。已有历史 WSL/Windows 数据不能替代本轮自用包矩阵。
8. **长时多会话、真实 Carrier WAN、Go 1.25.5 整体协议验收：NOT RUN，除非构建清单明确记录工具链。**
9. **Native/Standalone 跨版本互通：NOT RUN**。不得推断 v2.6.2 与该候选完全兼容。
10. **当前配置限制**：Landing `application_target` 是精确单一 `host:port`，一个 Client CONNECT 目标对应一条 Core Session。Front 只转发固定 Landing TCP 端点；部署时需维护准确源 CIDR。
11. **目标责任边界**：Landing HMAC 是 Client/Leg 的共享密钥认证，不是每用户独立账号或 mTLS。共享密钥泄露时应轮换并重启双方。
12. **客户端 TCP 写半关闭：已观测限制**。本机独立进程 smoke 曾在 4 MiB 请求后立即执行 TCP `SHUT_WR`，收到端仅得到 320 KiB，随后 Client 关闭整个 Core session。SMP3 当前 CLOSE 是整条逻辑流关闭，不携带单向 EOF；此测试不满足当前协议语义。通过的 smoke 改为长度明确的完整请求/响应，验证 4 MiB SHA 与目标端正常 EOF。不要将此版本用于要求“请求方先 FIN、服务端随后继续响应”的 SOCKS5 应用。修复需要单独设计并验证协议级半关闭语义，未在本阶段修改。

## 整体状态

Self-Use Build Delivery 与 General Production Release 分开。即使本地构建/Smoke 达到 READY WITH DOCUMENTED LIMITATIONS，仍必须保持 `P0-C = OPEN / FAIL`、`v2.7 GENERAL RELEASE = FAIL / NOT READY`，直到真实发布阻塞项均有证据关闭。
