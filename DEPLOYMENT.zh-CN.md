# SMP3 2.2.0 部署与使用教程

本教程针对 SMP3 2.2.0 独立产品，包括 standalone Sidecar client 和认证的
server host extension。2.1.1 的双向 Stream activation 行为仍是 runtime 基线。
服务端不需要 sing-box，使用
`smp3-server` 运行 canonical SMP3 Core 的 standalone server。sing-box
只是可选的兼容 client，另一个 client 集成是 Mihomo custom core。

## 1. 架构

```text
应用
  ↓ SOCKS5 / mixed proxy
Mihomo custom core 或可选 sing-box client
  ↓ SMP3 adapter：两个独立的 child outbound
  ↓ 各自的可靠 TCP-capable carrier（Snell / Hysteria2 / VLESS / Trojan / direct）
外部 carrier 服务端/终止端
  ↓ 原始 TCP carrier stream（承载 SMP3 HELLO 和 frames）
standalone SMP3 server（:24444）
  ↓
canonical SMP3 Core
  ↓
Internet destination
```

这里的 carrier 服务端/终止端位于 standalone server 之外，可以与其同机，
也可以位于另一台中转主机。它负责终止 Snell、Hysteria2 等外层 carrier，
再把解封装后的原始 TCP stream 转交给 `smp3-server`；它不是 SMP3 Core。
standalone server 只认证和处理 SMP3 HELLO、Stream frame、Datagram frame，
不实现或监听任何 Snell/Hysteria2/VLESS 等 child carrier 协议。

客户端的两个 child outbound 必须分别能够把可靠 TCP stream 建立到同一个
SMP3 listener `:24444`。两条 leg 共享的是 SMP3 logical session，不是共享
同一个底层 carrier 连接。MP-UDP 也以 Datagram frame 运行在这些 child
stream 之上，并不是要求 standalone 直接接收原生 UDP carrier。SMP3 不要求
特定代理协议，只要求 child outbound 提供所需的可靠 TCP dial capability；
IPv4/IPv6 选择和具体 carrier 的可达性继续由 host outbound/外部 carrier
部署负责。

## 2. 下载与校验

本 release candidate 使用 integration review 生成的本地 staging 产物；正式
发布后从 [v2.2.0 Release](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.2.0)
下载：

- 服务端：`smp3-server-linux-amd64` 或 Windows 版本；
- Mihomo：`mihomo-smp3-linux-amd64` 或 Windows 版本；
- 可选 sing-box client：`smp3-proxy-*`；
- `SHA256SUMS`。

```bash
sha256sum -c SHA256SUMS
```

示例配置中的密码都是占位符，不能直接用于生产。

## 3. 部署 standalone server

使用专用配置，不要把旧的 sing-box `config/server.example.json` 直接交给
standalone server：

```bash
cp config/standalone-server.example.json config/server.json
```

至少修改：

- `listen`：私有绑定地址和端口，通常为 `:24444` 或内网地址；
- `password`：与 client 相同的长随机 SMP3 密码；
- 初次部署先保留 `stream`、`udp` 默认值。

建议通过防火墙只允许 carrier 终止端访问裸 SMP3 listener。

Linux：

```bash
./dist/smp3-server-linux-amd64 -c config/server.json -check
sudo ./scripts/install-smp3-server.sh --config config/server.json
sudo systemctl status smp3-standalone
sudo ss -ltnp | grep 24444
```

`scripts/install-smp3-server.sh` 只写入 `/opt/smp3-standalone/` 和
`smp3-standalone.service`，不会覆盖旧的 `smp3-proxy` 安装。安装后可使用：

```bash
sudo smp3ctl status
sudo smp3ctl logs
sudo smp3ctl restart
```

Windows 可直接运行：

```powershell
& .\dist\smp3-server-windows-amd64.exe -c .\config\server.json -check
& .\dist\smp3-server-windows-amd64.exe -c .\config\server.json
```

服务端启动后再启动 client。双方的 `udp.mode`、`max_datagram_size`、
`idle_timeout` 和 duplicate 策略必须一致；这些 Datagram 子策略不会由
HELLO 协商。

## 4. 配置 Mihomo custom core

从 `config/mihomo.example.yaml` 开始，或把其中的 `proxies`、
`proxy-groups` 合并到已有 Mihomo 配置。替换所有 `YOUR_...` 占位符。

关键 SMP3 proxy：

```yaml
- name: MP-SMP3
  type: smp3
  server: YOUR_LANDING_PRIVATE_AGGREGATION_IP
  port: 24444
  password: YOUR_SMP3_PASSWORD
  legs:
    - proxy: line-path
    - proxy: public-hy2
  leg1-fallback: public-snell
  scheduler-mode: adaptive
  udp:
    enabled: true
    mode: adaptive
    max-datagram-size: 16384
    idle-timeout: 2m
```

要求：`legs` 必须正好有两个不同的已存在 child proxy；`leg1-fallback`
必须是独立 child；两条 carrier 都要能到达同一 SMP3 endpoint；需要 MP-UDP
时必须设置 `udp.enabled: true`。

先检查再启动：

```bash
./dist/mihomo-smp3-linux-amd64 -t -f config/mihomo.yaml
./dist/mihomo-smp3-linux-amd64 -f config/mihomo.yaml
```

替换现有 Mihomo 时，下载 installer 并指定精确路径。它只停止该路径的
进程，校验 stable Release 的 `SHA256SUMS`，在同目录创建 `smp3-backup`；
如果 supervisor 自动拉起同一路径，则立即安全停止：

```powershell
Invoke-WebRequest https://raw.githubusercontent.com/Superbias/smp3-multipath-kit-public/main/scripts/install-mihomo-smp3.ps1 -OutFile install-mihomo-smp3.ps1
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe"
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe" -Check
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe" -Update
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe" -Restore
```

不指定 `-CorePath` 时，Windows installer 只根据正在运行的 `mihomo.exe`
和少量有限常见路径检测；发现多个候选会 fail-closed，不会全盘扫描
`C:\`。

应用可连接 Mihomo 的 `127.0.0.1:7890` mixed/SOCKS 端口。UDP 应用必须真
正使用 SOCKS5 UDP ASSOCIATE；只支持 HTTP 的应用不会覆盖 MP-UDP。

## 4.1 使用 standalone SOCKS5 sidecar

仓库还包含一个独立的跨平台 `smp3-client` sidecar。当应用需要直接使用
本地 SOCKS5，而不使用 native Mihomo 或 sing-box SMP3 adapter 时，可以使用
它。它不会替换这些 adapter；2.2.0 将其作为兼容性优先的部署模式，Native
mode 仍是最短、性能最高的集成方式。

复制 `examples/smp3-client-config.example.json` 为私有配置并替换全部占位符。
两个 `smp3.routes` 必须经宿主 SOCKS5 独立到达同一个 standalone SMP3
listener；它们不是 Snell 或 Hysteria2 原生配置。sidecar 对每条 route
使用 TCP `CONNECT`，再在可靠 stream 上承载 SMP3 Stream v4 或 Datagram v5。
如果宿主代理会重试不健康 route，可设置 `upstream_socks.connect_timeout`；
它限制完整 SOCKS 事务，并允许按配置执行 leg1 fallback。
这些 route 应指向 server 的 `sidecar_listeners`。Sidecar listener 在
canonical HELLO admission 后发送带认证的 `SMP3RDY1`；旧的
`listen`/`listeners` 保持不变，不发送 READY。

```bash
go build -o smp3-client ./cmd/smp3-client
./smp3-client -c ./config/smp3-client.json -check
./smp3-client -c ./config/smp3-client.json
```

本地 listener 只绑定 loopback（默认 `127.0.0.1:18080`）。应用或 stock
Mihomo 应把该地址作为 SOCKS5 代理使用；stock Mihomo 示例见
`examples/mihomo-sidecar.example.yaml`。sidecar endpoint 的 route
discriminator 必须位于宽泛规则之前，避免把 sidecar 自身再路由回 sidecar。
sidecar 不会修改 Mihomo、Clash Party、carrier 定义、防火墙策略或
standalone server。

如果使用 Clash Party，请通过其 custom-core 机制指定独立下载的 Mihomo
文件，不要直接覆盖 `Clash Party/resources/sidecar/mihomo.exe`。

## 5. 使用可选 sing-box 兼容 client

`config/client-adaptive.example.json` 是 sing-box 形状的 client 配置，
不是 standalone server 的依赖：

```bash
cp config/client-adaptive.example.json config/client.json
./dist/smp3-proxy-linux-amd64 check -c config/client.json
./dist/smp3-proxy-linux-amd64 run -c config/client.json
```

默认本地 mixed inbound 为 `127.0.0.1:2080`，应用可使用
`socks5h://127.0.0.1:2080`。Windows 使用对应 `.exe`。

## 6. 首次验证

按顺序检查：

1. 服务端 `-check` 通过并监听正确端口；
2. client 配置检查通过，日志出现 leg0 bootstrap；
3. 通过本地 proxy 发送 TCP 请求：

   ```bash
   curl --proxy socks5h://127.0.0.1:7890 https://example.com/
   ```

4. 使用支持 SOCKS5 UDP 的 DNS/应用客户端测试 UDP；
5. 较长 TCP 流量中确认第二条 leg 加入。Stream activation 会按单个逻辑
   session 同时观察 application payload 的上传和下载速率，并使用较高方向
   的速率与 `activation-threshold-mbps` 比较，不会聚合多个连接。短流量或两
   个方向都低于 threshold 时按设计可能只使用一条 leg。

正常状态是一个逻辑 SMP3 session 加两条 carrier leg；可恢复故障只替换
   carrier generation，不重建逻辑 session。Mihomo adapter 也支持在应用
   association 仍存在时重建已经 terminal 的 UDP engine。

## 7. UDP 模式与限制

- `adaptive`：推荐默认值，根据路径健康度和队列压力调度；
- `stripe`：每个包只走一条 live leg，交付无序；
- `duplicate`：两条 live leg 都发送，Core 只交付一份，但 carrier 流量会增加；
- 最大 UDP payload 为 `16384`，更大的包会安全丢弃，不会静默截断；
- UDP 仍然不可靠，单腿切换时允许少量丢包，SMP3 不会用重传把 UDP 变成 TCP。

## 8. 日志、升级与回滚

```bash
sudo journalctl -u smp3-standalone -f
sudo systemctl restart smp3-standalone
sudo ss -ltnp | grep 24444
```

Linux 升级时保留现有 config，由 installer 获取最新 stable Release 并校验
精确 asset。通过 `smp3ctl` 管理状态、日志、升级和回滚：

```bash
sudo smp3ctl check
sudo smp3ctl update
sudo smp3ctl rollback
```

installer 最多保留五代已校验 binary backup，普通 binary update 不修改
config。手动回滚到单独保留的旧服务：

```bash
sudo systemctl disable --now smp3-standalone
# 仅当部署中确实保留旧服务时执行
sudo systemctl enable --now smp3-proxy
```

## 9. 常见问题

| 现象 | 检查 |
|---|---|
| HELLO rejected | 密码、endpoint、端口和 carrier 目标是否一致。 |
| TCP 正常但 UDP 不通 | client/server 的 `udp.enabled`、应用 SOCKS5 UDP 能力。 |
| 第二条 leg 不出现 | 确认流量确实经过 SMP3，并且单个逻辑 Stream session 的上传或下载速率在 `activation-window` 内达到 `activation-threshold-mbps`；该阈值按方向/单 session 判断，不是多连接聚合。然后检查 child proxy 名称和 carrier 连通性。 |
| 大 UDP 消失 | `>16384` 属于预期安全丢弃，不应被截断转发。 |
| 故障后长时间中断 | 检查 carrier detection 日志；少量 UDP 丢包允许，持续失败不允许。 |
| H3 100 MiB 失败 | 已知 aioquic/quic-go harness 问题，可脱离 SMP3 复现。 |

## 10. 安全清单

- 每个部署使用独立长随机 SMP3 密码；
- 不发布真实密码、PSK、TLS 私钥、provider 凭据或真实 config；
- 用防火墙限制裸 SMP3 listener；
- 公网路径使用加密 child carrier。
