# SMP3 v2.7 Self-Use 自行部署指南

> 这是维护者自用试运行构建，不是 GA。部署包不会连接或修改任何生产机器。开始前先执行 `scripts/verify-checksums.sh`，从安全渠道生成共享密钥，并确认 Landing、Front 与 Client 的系统时间和网络路由。

## 1. 准备 Landing

在 Landing Linux 服务器上传并解压完整 ZIP，进入 `SMP3-v2.7-selfuse` 目录：

```sh
unzip SMP3_v2.7_SelfUse_Production_Bundle_20261029.zip
cd SMP3-v2.7-selfuse
sudo scripts/install-landing.sh
```

安装器校验当前架构的 `smp3-server` 哈希、建立无登录的 `smp3` 用户、保留已有配置并安装 systemd unit。它不会改防火墙，也不会自动启动服务。

1. 将 `configs/landing.example.json` 的副本安装为 `/etc/smp3/landing.json`（首次安装器会复制模板）。把 `password` 换成随机高熵共享密钥；不要把该密钥发到聊天、工单或日志。
2. 将证书链和私钥放在配置指定位置。私钥属主建议 `root:smp3`、模式 `0640`，证书链 `0644`。不把私钥放进发布 ZIP。
3. 将 `application_target` 改成此服务器上实际应用的精确 `host:port`。该 Listener 对每个 HELLO 只允许此目标。
4. 保持 `listen` 为 `0.0.0.0:8443` 或指定服务器接口，保留 TLS 和固定目标。Telemetry 只绑定回环 `127.0.0.1:24500`。
5. 检查配置并启动：

```sh
sudo -u smp3 /usr/local/bin/smp3-server -c /etc/smp3/landing.json -check
sudo systemctl enable --now smp3-landing
sudo systemctl status smp3-landing
sudo journalctl -u smp3-landing -f
```

按自己的防火墙工具人工仅开放 TCP `8443` 给预期 Client 源地址，以及应用目标端口只允许本机访问。安装器不会改防火墙。不要把 `24500` Telemetry 暴露到公网。

## 2. 准备 Front

在 Front Linux 主机上传并解压相同版本：

```sh
sudo scripts/install-front.sh
```

编辑 `/etc/smp3/front.json`：`landing_target` 指向 Front 到 Landing 可达的地址；`allowed_sources` 必须换成 Client 经其上游 SOCKS 出口后，Front 实际看到的源 IP/CIDR。Front 若绑定非回环地址而没有白名单会拒绝启动。它只建立 TCP 到一个固定 Landing 目标，TLS 由 Client 端到 Landing 端验证，不在 Front 终止。

```sh
sudo -u smp3 /usr/local/bin/smp3-front -c /etc/smp3/front.json -check
sudo systemctl enable --now smp3-front
sudo systemctl status smp3-front
sudo journalctl -u smp3-front -f
```

只在你控制的网络边界开放 Front TCP 端口；Landing 上可按源地址允许直连 Client 与 Front 出口。需人工配置并复核防火墙规则。

## 3. 配置 Client

Linux 常驻 Client 可运行 `sudo scripts/install-client.sh`；Windows 使用 `bin/windows-amd64/smp3-client.exe`，并将程序与配置放在仅当前用户可读的目录。复制 `configs/client.example.json`，然后：

- `upstream_socks.leg0` 选择到 Landing 的直连出口，`upstream_socks.leg1` 选择能到达 Front 的出口。可以是同一个本机 SOCKS 地址，也可以是两个有不同路由策略的 SOCKS 端口。
- `smp3.routes.leg0` 填 Landing `host:port`；`leg1` 填 Front `host:port`。
- 两条 Leg 的 TLS `server_name` 都填 Landing 证书名称。不要把 `server_name` 改成 Front。
- 公有 CA 使用系统信任即可；私有 CA 时设置 PEM `root_ca_file`。证书验证始终开启。
- `password` 与 Landing 完全一致。保留 B0 默认；示例中的 `experimental_path_aware: true` 是显式的 Version 6 配置，服务端也要显式开启。B1 没有被启用。
- 按实测或运营商提供值填写 `bandwidth_mbps` 先验；示例 `[100,10]` 只是模板。

检查后启动：

```sh
smp3-client -c ./client.json -check
smp3-client -c ./client.json
```

Windows PowerShell：

```powershell
.\smp3-client.exe -c .\client.json -check
.\smp3-client.exe -c .\client.json
```

Linux 常驻 Client：

```sh
sudo systemctl enable --now smp3-client
sudo systemctl status smp3-client
sudo journalctl -u smp3-client -f
```

## 4. 接入业务 TCP

Standalone Client 在本机提供 SOCKS5 CONNECT，默认只监听 `127.0.0.1:10880`。把支持 SOCKS5 的应用配置到 `127.0.0.1:10880`，并请求与 Landing `application_target` 完全一致的目标地址，例如示例中的 `127.0.0.1:9080`。每个应用 CONNECT 建立一个 SMP3 Core stream session；这个 Session 里 Leg0 直连 Landing、Leg1 通过 Front，再由同一个 Landing Core 重组并连接固定应用目标。不是两个独立应用连接。

可以使用 curl 验证 SOCKS5：

```sh
curl --socks5-hostname 127.0.0.1:10880 http://127.0.0.1:9080/
```

如果用户业务是其他 TCP 协议，应在应用本身设置 SOCKS5；本版本不提供任意 TCP 本地透明代理，也不把 Native Mihomo 可运行性冒充为 Standalone 能力。

## 5. 查看状态

Landing Telemetry 仅绑定 loopback：

```sh
SMP3_TELEMETRY_URL=http://127.0.0.1:24500 scripts/show-status.sh
curl -fsS http://127.0.0.1:24500/api/v1/legs
curl -fsS http://127.0.0.1:24500/api/v1/sessions
sudo journalctl -u smp3-landing --since '15 minutes ago'
```

需要远程查询时先建立 SSH 本地端口转发：`ssh -L 24500:127.0.0.1:24500 user@landing-host`。状态输出包含在线 Leg、generation、线速字节、唯一业务字节、ACK、Retry/Rescue 和恢复状态。不要将含目的地址的原始故障记录公开发布。

## 6. 停止或回滚

```sh
sudo systemctl disable --now smp3-client smp3-front smp3-landing
```

按 [UPGRADE_AND_ROLLBACK.md](UPGRADE_AND_ROLLBACK.md) 恢复安装器在 `/var/backups/smp3/` 下生成的旧二进制/配置。回滚后检查 `systemctl status`、`-version`、客户端 TLS 和 Landing 监听。会话断开后需由应用重新建立连接；不能承诺升级、回滚或路由切换时保留旧会话。
