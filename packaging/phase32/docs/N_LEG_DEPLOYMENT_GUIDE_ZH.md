# SMP3 v2.7 N-Leg 自用部署指南

本包提供 Standalone Client 与 Landing Server。默认采用 B0（共享 Core 的 N 路聚合调度）；实验性 B1 不从这些 Standalone 配置暴露。该构建适合维护者自用试运行，不代表 v2.7 GA 已通过。

## 部署拓扑

Client 为一条本地应用连接创建一个 SMP3 Core Session，再按 `smp3.routes.legs` 建立多个独立 TCP/TLS Carrier。直连 Leg 与 Realm TCP 转发 Leg 最终连接到同一个 Landing `listen` 地址；Landing 以 HELLO 中经 HMAC 认证的 Session ID、Leg ID、时间戳和随机 nonce，将不同 Leg 接入同一 Core。Realm 只转发 TCP 字节，不解析 SMP3 帧，也不终止客户端到 Landing 的 TLS。

本包示例是配置样板，所有域名、端口、密码、上游 SOCKS 监听地址和证书路径都要替换成实际值。不同 Front 地址本身不能证明底层物理路由独立。

## 1. 配置 Landing

1. 将 `bin/linux-amd64/smp3-server` 安装到 Landing Linux 主机。
2. 准备有正确 DNS 名称的 TLS 证书和私钥。私钥不能放入 ZIP；服务账户需要只读权限，建议 `root:smp3`、模式 `0640`。
3. 复制 `configs/landing.example.json` 到 `/etc/smp3/landing.json`，填写 `password`、证书路径、业务 `application_target` 和防火墙策略。设置 `stream.max_active_legs` 为允许值；最大值是资源保护，不会要求每个 Session 必须连满。
4. 限制 Landing 8443/TCP 到可信 Client/Realm 出口地址。TLS 和 HELLO HMAC 都要启用；不要公开未限制的任意目标转发。
5. 以服务方式安装：`sudo bash scripts/install-landing.sh`。安装脚本保留现有配置；修改后执行 `sudo systemctl enable --now smp3-landing`。

## 2. 配置 Realm TCP 转发

按 `REALM_TCP_FORWARDING_REFERENCE.md` 建立纯 TCP 监听转发，目标是同一个 Landing `IP:8443`。每个 Realm 前置提供一个 Client 可连接的地址。不得使用 UDP 隧道或 TLS 终止；转发侧不需要 SMP3 认证或调度状态。Realm 自身的配置语法请使用对应 Realm 版本文档，本包不猜测或生成未经验证的 Realm 配置。

## 3. 配置 Client 的 N 条 Leg

1. 选择 `configs/client-2leg.example.json`、`client-3leg.example.json`、`client-4leg.example.json` 或 `client-8leg.example.json`。
2. `smp3.routes.legs` 的 ID 必须从 0 连续递增；每项的 `address` 是 Landing 或 Realm TCP 入口。
3. 为每条路径设置 `upstream_socks.legs[i].address`，并保证对应 SOCKS 出口可达其路由终点。示例中的 `127.0.0.1:7890+i` 是占位值，必须改成实际本地 SOCKS 代理。缺省 Carrier 模式使用 SOCKS 拨号；若宿主使用 host bridge，TLS 与拨号安全由宿主 Carrier 负责。
4. `smp3.carrier_tls.enabled` 应保持 `true`，`server_name` 必须匹配 Landing 证书。默认使用系统 CA；如使用私有 CA，可设置受信任的 `root_ca_file`。不要关闭证书验证。
5. `smp3.password` 与 Landing 密码一致，使用随机高熵秘密。不要把实际秘密提交到源码或分享至诊断日志。
6. 选用 `stream.max_active_legs` 至少覆盖配置的 Leg 数；`bandwidth_mbps` 是调度先验/容量输入，不是线路实测承诺。确认业务 UDP 关闭（N>2 时现行 Client 明确要求关闭 UDP）。
7. 用 Windows 包时执行 `smp3-client.exe -c client.json -check`；Linux 包可执行同等 `-check` 命令。确认输出 `config OK` 后再启动。

运行后本地应用继续连接 Client 的 `listen`（示例为 `127.0.0.1:10880`）。一个应用 TCP 连接对应一个 Core Session，不是每条 Leg 各建一个应用 Session。

## 4. 安装、状态与停止

Linux Client：`sudo bash scripts/install-client.sh`，编辑 `/etc/smp3/client.json`，再执行 `sudo systemctl enable --now smp3-client`。检查：

```sh
systemctl status smp3-client smp3-landing
journalctl -u smp3-client -u smp3-landing --since '15 min ago'
curl --fail http://127.0.0.1:24500/api/v1/status
curl --fail http://127.0.0.1:24500/api/v1/legs
curl --fail http://127.0.0.1:24500/api/v1/sessions
```

Telemetry 只应绑定 loopback 或受控管理网。`/api/v1/legs` 用于核对逻辑 Leg 状态与逐 Leg 计数；需要业务唯一 DATA 贡献时，要结合 Session/Core telemetry 和应用 payload 证据，不能仅凭连接数或 HELLO/ACK 计数。

停止服务：`sudo systemctl stop smp3-client` 或 `sudo systemctl stop smp3-landing`。运行时新增 Leg 暂不支持热添加；修改配置需要重启 Client。已配置 Leg 的断开、Replace/Rejoin 和 Generation 变化由现有 Core 路径处理，不能把它描述成动态 Hot Add。

## 5. 回滚

先记录当前包和服务状态。Phase31 双路版本使用旧的 `routes.leg0` / `routes.leg1` 配置格式；部署回退包时先停止对应服务，再按 `UPGRADE_AND_ROLLBACK.md` 指定的备份目录恢复旧二进制、配置和 systemd unit。确认恢复的 Client 与 Landing 版本互通后再启动。不要将 N-leg 配置原样交给不识别 `routes.legs` 的旧 Client。

## 6. 预期边界

- 本地 TCP N=3/4/8/16 Landing 集成有真实数据、统一 Session、SHA、EOF、ACK 和账本证据；单次固定负载和 loopback 不等于真实线路容量。
- TLS 在独立的两端 Carrier 测试覆盖；N=3/4/8/16 的 Landing 集成具体使用何种 Carrier，以对应机器结果和报告为准。
- 更换多个 Realm 出口不自动形成物理路径独立性；上线后先以低风险流量观测每 Leg 唯一 DATA、ACK、Retry/Rescue、吞吐与资源。
- Go 1.25.5、Windows/WSL Race、Native Mihomo、Linux arm64 运行、内核黑洞和真实 WAN 状态见发布门禁报告；没有实际运行的项目标记 NOT RUN。
