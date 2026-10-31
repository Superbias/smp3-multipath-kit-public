# SMP3 v2.7.0-selfuse.2 — N-Leg Self-Use

本包用于维护者在隔离或自管环境进行 N 路 TCP 自用试运行。不是 SMP3 v2.7 GA，不包含可用于发布的 Native Mihomo 核心，也没有进行生产部署。

- 配置样板：configs/
- 中文部署、Realm TCP、升级与回退：docs/
- 安装脚本与 systemd：scripts/、systemd/
- 二进制 SHA-256：manifest/SHA256SUMS

建议先使用 2 条 Leg，再按主机资源逐步扩展到 3/4/8。证书、密码、Realm 路由、监听地址和防火墙规则需由维护者自行配置。Standalone 配置变更需要重启，当前不支持通过编辑 JSON 热添加 Leg。N>2 仅限 TCP+B0 自用候选。

真实 WAN、Realm 实例、Native Mihomo 固定版本构建、长时资源和物理容量门禁仍未通过。P0-C 与 v2.7 General Release 均为 FAIL / NOT READY。
