# SMP3 v2.7.0-selfuse.1

> **SELF-USE PRODUCTION BUILD — NOT GENERAL AVAILABILITY.** 本版本仅用于维护者自行部署试运行。部署包有运行程序与安装材料，不等于正式生产批准。General Production Release 仍为 `FAIL / NOT READY`，P0-C 仍 `OPEN / FAIL`。

## 内容

- Linux Landing Server、TCP-only Front Relay、Standalone Client；并提供 Windows amd64 Standalone Client。
- B0 默认。B1 仍为实验状态且禁用。Client/Landing 示例显式设置 `B0 + Explicit PathAware Configuration`，Path-aware 并未成为全局默认。
- Landing 公网 listener 必须设置 TLS 与 fixed application target；增加 max sessions/connections、可选 idle timeout、日志级别和有界 CLI shutdown。
- 新增 TCP-only Front 生产 CLI，固定目标、来源 CIDR allowlist、连接上限和双向半关闭转发。
- JSON 配置、systemd units、安装/升级/卸载/回滚/哈希校验脚本和中文运行文档。

## Phase 30 背景结果与限制

- Phase 30 本机三进程 TCP/TLS 聚合测试报告通过，Direct + Front + Landing 使用一条应用连接和同一个 Core Session。报告的本地 n=3 配对中位增益约 11.89%。它是本地用户态 shaping 结果，不保证真实 WAN 性能。
- Windows Core RTT 测试仍有已知失败：`TestPhase28ActiveCarrierHOLPilotTCP` 等待双向新鲜 RTT 样本超时。
- Kernel TCP Blackhole 未验证；Native Mihomo 未完成实机验证；真实 WAN 未验证。
- 原 Strict Zero Retry/Rescue 门禁没有关闭。B1/300 bp 自然预算处理效果未经验证。
- 本轮授权跳过物理链路、生产服务器、Native 实机、黑洞专测和 30 分钟真实网络压力；这些项目仍是 NOT RUN，不会在交付包中写成 PASS。

## 安全与兼容

Landing 要求共享密码 HMAC、TLS 及固定 `application_target` 用于对外监听；Front 固定 Landing target 并要求非回环的来源 CIDR。Client 两条 TLS Carrier 均验证 Landing 身份，Front 只转发 TCP 字节流。默认绑定、端口和目标必须由维护者根据环境配置。未验证 v2.6.2 与该候选完全兼容；滚动升级会中断连接。

Phase 30 原始记录的完整补丁曾出现 144 个意外删除。Phase 31 生成了从干净基线构建并独立应用校验的更正补丁；原始 Phase 30 证据保留，未被重写。
