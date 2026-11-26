# SMP3 v2.6.2 部署与使用教程

> **v2.7.0 推荐教程：**[现有代理双路径 → 落地加密代理 → 本机 SMP3](docs/native/PROXY_CONVERGENCE.zh-CN.md)，包含脱敏配置、验证和回退。下方 v2.6.2 步骤保留为历史说明。


## 首选部署拓扑

优先复用现有代理节点，让多条独立的 SMP3 Leg 最终汇入同一个落地 SMP3 服务。落地机运行 `smp3-server`；前置机只需保留现有代理服务，并具备到落地 SMP3 入口的出站路径。

```text
应用 -> SMP3 客户端（Native Mihomo 或 Standalone）
        |-- Leg 0 -> 前置机代理节点 -> WG / IP 路由 / 其他转发路径 --+
        |-- Leg 1 -> 落地机上的代理节点 ----------------------------+-> 落地 SMP3 监听入口
        `-- Leg N -> 另一条独立选择的代理路径 ----------------------+       -> application_target
```

**不要求前置机运行 SMP3，Realm 和 `smp3-front` 都是可选项。** 以 WireGuard 为例，WG 仅建立在前置机与落地机之间；客户端正常连接前置机的代理节点，无需加入 WG。前置节点的出站通过 WG 访问落地 SMP3 入口，另一条 Leg 则经落地机上的代理节点访问该入口。

- **Native Mihomo：**`legs[].proxy` 为每条 Leg 选择现有代理节点。共同的 `server`、`port` 填写经这些节点拨号的落地 SMP3 目标，不是前置代理节点的监听端口。
- **Standalone：**`upstream_socks.legs[]` 分别选择外部代理软件提供的本地 SOCKS5 入口；对应的 `smp3.routes.legs[].address` 是经该入口到达的 SMP3 目标。节点凭据、前置到落地的路由由代理软件和操作系统配置。
- SMP3 目标必须对各代理出口可达。若前置节点和落地本机节点都能访问落地 WG 地址，就可以使用该地址；也可以使用公网或其他可路由私网地址。`127.0.0.1` 指最终发起拨号的主机自身，有前置出口时不宜把它当作共同目标。
- 客户端为同一业务连接创建具有相同认证 Session ID、不同 Leg ID 的 Carrier。客户端与落地端的共享 Core 负责调度、ACK、重组和恢复；普通无关连接不会因为到达同一端口就自动聚合。
- 中间路径须保留 SMP3 字节流及配置的端到端认证/TLS。`application_target` 是 SMP3 终止后的应用目标，与 SMP3 监听入口、前置节点地址不同；固定目标校验仍然生效。

SMP3 不负责配置 WG、系统路由或远端代理出站。此拓扑说明当前 Carrier 抽象支持的接入方式，不表示某个 WG/WAN 部署已经通过实测验收；路径共享瓶颈时，带宽收益也可能受限。需要透明端口转发时，仍可选用 Realm。


> 下方 v2.6.2 操作步骤属于历史说明；v2.7.0 配置请以[当前 README](README-zh_CN.md)为准。


这是一份面向实际使用的简明教程。生产密码、PSK、Reality 私钥和真实节点
参数只放在本机配置中，不要提交到仓库。

## 1. 先理解三个组件

```text
Native：应用 → Mihomo / Clash Party → SMP3 server

Standalone：应用 → 127.0.0.1:18080 → smp3-client
            → 本机 Carrier-A / Carrier-B → SMP3 server

Dashboard：浏览器 → SMP3 server → telemetry 127.0.0.1:24500
```

- `smp3-server`：只处理 SMP3，不是 sing-box server。
- `smp3-client`：提供本机 SOCKS5 和 SMP3 Leg，不实现 VLESS、Reality、
  Hysteria2、Snell 等外层协议。
- Mihomo/Carrier：保存真正的节点信息，并负责拨号和外层协议。
- Integrated Dashboard：集成在 SMP3 server 中，只读展示 telemetry。

因此，Standalone 的 Leg0/Leg1 不是节点配置本身，而是通过两个 Carrier
入口去连接 SMP3 server。节点信息配置在 Mihomo/Carrier。

## 2. 下载和校验

从 [v2.6.2 Release](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.6.2)
下载教程和产品制品。该版本加入 Android Standalone，同时不改变 SMP3 数据面语义：

| 文件 | 用途 |
| --- | --- |
| `smp3-server-linux-amd64` / Windows 版 | Standalone 服务端 |
| `smp3-client-linux-amd64` / Windows 版 | Standalone 本地 SOCKS5 |
| `mihomo-smp3-linux-amd64` / Windows 版 | Native / Clash Party |
| `smp3-proxy-linux-amd64` / Windows 版 | sing-box 兼容模式 |
| `smp3-android-standalone-2.6.2-debug.apk` | Android ARM64 Standalone |
| `SHA256SUMS` | 文件完整性校验 |

```bash
sha256sum -c SHA256SUMS
```

Windows PowerShell：

```powershell
Get-FileHash .\smp3-client-windows-amd64.exe -Algorithm SHA256
```

只在校验值与 `SHA256SUMS` 完全一致后运行文件。

## 3. 部署 SMP3 server

复制模板并修改私有配置：

```bash
cp config/standalone-server.example.json config/server.json
```

至少修改：

- `listen`：SMP3 监听地址；
- `password`：与客户端一致的长随机密码；
- `sidecar_listeners`、telemetry：按你的部署方案配置。

要启用集成 Dashboard，至少加入以下 telemetry 配置；它只能绑定 loopback：

```json
"telemetry": {
  "enabled": true,
  "listen": "127.0.0.1:24500"
}
```

检查并启动：

```bash
./smp3-server-linux-amd64 -c config/server.json -check
./smp3-server-linux-amd64 -c config/server.json
```

Linux 正式部署可使用安装脚本：

```bash
sudo ./scripts/install-smp3-server.sh --config ./config/server.json
sudo smp3ctl status
sudo smp3ctl logs -f
```

裸 SMP3 listener 应只允许 Carrier/内部网络访问，不要直接暴露成公共代理端口。

## 4. Native / Clash Party 模式

Native 模式不需要 `smp3-client`，也不需要 sing-box。

1. 使用 `mihomo-smp3-windows-amd64.exe` 或 Linux 版本。
2. 复制 [config/mihomo.example.yaml](config/mihomo.example.yaml)。
3. 在 `proxies` 中填写真实的外层节点，例如 line-path、VLESS、Reality、
   Hysteria2 等；这些由 Mihomo 负责实现。
4. 用两个不同的 child outbound 配置 `type: smp3` 的 `legs`。
5. 在 Clash Party 中选择这个 Mihomo custom core。
6. 应用连接 Clash Party 的 mixed/SOCKS5 端口，例如 `127.0.0.1:7890`。

配置检查和运行方式以 Clash Party 的 custom-core 机制为准。替换内核前保留
原文件备份；不要覆盖未知路径下的 `mihomo.exe`。

## 5. Standalone SOCKS5 模式

### 5.1 先启动 Carrier

Standalone 不会自动启动外部 Carrier。先保证两个 Carrier 入口可用：

```text
Carrier-A → 127.0.0.1:17898 → Leg0
Carrier-B → 127.0.0.1:17899 → Leg1
```

Carrier 的 VLESS/Reality/Hysteria2/其他节点参数配置在 Carrier Mihomo，
不是配置在 `smp3-client` 中。

### 5.2 配置并启动 smp3-client

复制 [examples/smp3-client-config.example.json](examples/smp3-client-config.example.json)：

```bash
cp examples/smp3-client-config.example.json config/smp3-client.json
```

修改以下内容：

- `listen`：默认 `127.0.0.1:18080`；
- `upstream_socks.leg0`：Leg0 的 SOCKS5 listener；
- `upstream_socks.leg1`：Leg1 的 SOCKS5 listener；
- `smp3.password`：与 server 相同；
- `smp3.routes.leg0`、`leg1`：分别指向 SMP3 server 的两个入口；
- `leg1_fallback`：可选备用路径。

检查并启动：

```bash
./smp3-client-linux-amd64 -c ./config/smp3-client.json -check
./smp3-client-linux-amd64 -c ./config/smp3-client.json
```

Windows：

```powershell
.\smp3-client-windows-amd64.exe -c .\config\smp3-client.json -check
.\smp3-client-windows-amd64.exe -c .\config\smp3-client.json
```

应用设置为：

```text
SOCKS5: 127.0.0.1:18080
```

旧配置中的 `upstream_socks.address`、账号密码和 timeout 仍然兼容。缺少某条
Leg override 时，该 Leg 使用全局地址；存在 override 时只作用于自己的 Leg，
不会把 Leg1 自动切换到 Leg0 的 SOCKS5 endpoint。

一个外部代理进程可以同时提供两个独立 listener，也可以由两个进程分别提供。
Standalone 不读取节点协议配置，只连接通用 SOCKS5。详见
[Standalone 外部代理 contract](docs/standalone/EXTERNAL_PROXY.md)。

## 6. Android Standalone

在 `arm64-v8a` Android 设备安装 `smp3-android-standalone-2.6.2-debug.apk`。
先在外部代理核心中提供两个本机 SOCKS5 监听，例如
`127.0.0.1:20001` 和 `127.0.0.1:20002`，分别连接你选择的两个节点。
APK 不实现 VLESS、Reality、Hysteria2、Snell 等节点协议，这些仍由外部
代理核心负责。

在 App 中填写：

- Local SOCKS：通常为 `127.0.0.1:18080`；
- 可被手机访问的 SMP3 server sidecar 地址；
- 与服务端相同的 SMP3 密码；
- Carrier-A、Carrier-B 的监听地址和端口。

点击 **Save**，再点击 **Start**。其他应用的代理设置为 App 的
`127.0.0.1:18080`。修改运行配置前先点击 **Stop**。详细说明见
[Android 使用说明](docs/android/README.md)。

### 6.1 Leg0/Leg1 行为

- `leg0` 通常先启动，是 preferred/primary leg；
- 满足 activation threshold 后，`leg1` 才会加入；
- 短连接、低速请求或尚未达到阈值时，Leg1 保持 down/未激活可能是正常现象；
- 不要通过修改 threshold、window 或手动拨号来判断生产行为。

## 7. 启动、停止和持久化

推荐由同一个服务管理器、计划任务或 supervisor 管理外部 Carrier、
`smp3-client` 和集成 Dashboard：

```text
启动：Carrier-A → Carrier-B → smp3-client
停止：smp3-client → Carrier-B → Carrier-A
```

`smp3-client` 自身只管理 SMP3 client 进程，不会自动创建或启动 Carrier
进程。若 Carrier-A/B 已经有独立服务定义，应由上层服务管理器负责依赖关系、
自动重启和开机启动。

## 8. 集成 Dashboard

Dashboard 由 `smp3-server` 提供，不要再启动已经退役的独立 `smp3-panel`，也不要使用 `24600`：

```text
GET /api/v1/status
GET /api/v1/legs
GET /api/v1/sessions
GET /api/v1/traffic
GET /api/v1/traffic/history
GET /api/v1/events       # SSE
```

页面可以查看：

- Leg0 / Leg1 状态；
- Carrier-A / Carrier-B 状态；
- TX 速率、Useful ACK 速率、流量占比；
- 事件、健康分类和有限历史。

Dashboard 不展示 raw SessionID、目标地址、payload、密码或私钥。

## 9. 首次验证

按以下顺序检查：

1. server `-check` 成功；
2. Carrier-A/B 的监听端口正常；
3. `smp3-client -check` 成功且 `18080` 已监听；
4. 应用通过 `127.0.0.1:18080` 发起一个普通 TCP 请求；
5. Dashboard 能读取 status、traffic、history、events，SSE 页面保持更新；
6. 确认 Leg0 先 ready；产生足够持续流量后再观察 Leg1 是否加入。

推荐端口检查：

```bash
ss -ltnp | grep -E '17898|17899|18080|24500'
```

Windows PowerShell：

```powershell
Get-NetTCPConnection -State Listen -LocalPort 17898,17899,18080,24500
```

## 10. 常见问题

| 现象 | 优先检查 |
| --- | --- |
| `smp3-client` 启动但 Leg0/Leg1 都失败 | Carrier SOCKS5、route、server 地址和密码 |
| Leg1 一直 down | 流量是否足够、是否达到 activation threshold、Carrier-B 是否监听 |
| 误以为 client 需要 VLESS/Reality | 这些由 Mihomo/Carrier 拨号，client 不实现 |
| 只能访问 TCP | 应用是否支持 SOCKS5 UDP，且两端 UDP 已启用 |
| Dashboard 无数据 | `24500` 是否监听、服务端 telemetry 是否启用 |
| Dashboard 页面能开但 SSE 不更新 | 检查 `/api/v1/events` 和 telemetry 日志 |

## 11. 安全要求

- 每个部署使用独立的长随机 SMP3 密码；
- 不公开裸 SMP3 listener；
- 外层公共路径使用加密 Carrier；
- 不提交密码、PSK、Reality 私钥、订阅 URL、API key 或真实配置；
- 生产升级前先校验 `SHA256SUMS` 并保留旧版本回滚副本。
