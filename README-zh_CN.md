# SMP3 Multipath Kit v2.4.1

[English](README.md) | 简体中文

SMP3 是一个独立的应用层多路径传输组件，包含 Standalone、Native 和
集成在服务端中的 Dashboard 三条产品线。

正式产品划分：

```text
Standalone     = Universal External Proxy Compatibility
Native         = High-Performance Mihomo Integration
Compatibility  = 可选的 legacy smp3-proxy / sing-box 集成
```

详细架构见 [产品架构](docs/PRODUCT_ARCHITECTURE.md)、
[Standalone](docs/standalone/README.md)、[Native](docs/native/README.md) 和
[Compatibility](docs/compatibility/README.md)。

## 先选使用模式

| 模式 | 适合场景 | 是否需要 sing-box |
| --- | --- | --- |
| Native | Clash Party / Mihomo 直接使用 SMP3，推荐 | 否 |
| Standalone | 给普通应用提供本机 SOCKS5 入口 | 否 |
| `smp3-proxy` | 兼容已有 sing-box 配置 | 是，仅此模式 |
| Integrated Dashboard | 查看状态、Leg、流量、事件和历史 | 否 |

最重要的一点：`smp3-client` 只负责 SMP3 和本地 SOCKS5。它不会实现
VLESS、Reality、Hysteria2、Snell 等外层协议，也不知道节点密码。外层节点
配置在 Mihomo/Carrier 中；Standalone 通过 Carrier 提供的 SOCKS5 入口
建立 Leg0、Leg1。

## 当前发布版本

- Release：`v2.4.1`（Android Standalone 多实例管理/UI；数据面语义不变）
- Native：Mihomo `v2.4.1`（基于固定的上游 `v1.19.28`）+ SMP3 adapter
- Standalone：`smp3-client`、`smp3-server`
- Android Standalone：`smp3-android-standalone-2.4.1-debug.apk`（`arm64-v8a`）
- 兼容：`smp3-proxy`（固定 sing-box 运行时，版本后缀包含 SMP3 版本）
- 监控：集成在 `smp3-server` 中，独立 `smp3-panel` 已退役
- 官方下载：[GitHub Releases v2.4.1](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.4.1)
- 下载后先用 `SHA256SUMS` 校验文件，不要直接使用示例配置中的占位密码。

## 当前资格化拓扑

```text
Native：应用 → Mihomo / Clash Party → Leg0 + Leg1 → SMP3 server

Standalone：应用 → 127.0.0.1:18080 → smp3-client
            → Carrier-A / Carrier-B → SMP3 server

监控：浏览器 → SMP3 server Dashboard → 127.0.0.1:24500 telemetry
```

当前生产约定端口如下；如果你的部署配置不同，以实际配置为准：

| 组件 | 地址 | 作用 |
| --- | --- | --- |
| SMP3 primary/native | `:24444` | Native 主入口 |
| SMP3 sidecar | `:24445` | Standalone 入口 |
| Carrier-A | `127.0.0.1:17898` | Leg0 外层代理入口 |
| Carrier-B | `127.0.0.1:17899` | Leg1 外层代理入口 |
| `smp3-client` | `127.0.0.1:18080` | 应用使用的 SOCKS5 |
| telemetry | `127.0.0.1:24500` | 仅本机监控数据 |
| Integrated Dashboard | `127.0.0.1:24500` | 服务端 loopback REST/SSE 和页面 |

## 快速开始

完整中文教程见 [DEPLOYMENT.zh-CN.md](DEPLOYMENT.zh-CN.md)。

### Native / Clash Party

1. 下载 `mihomo-smp3-windows-amd64.exe` 或 Linux 版本。
2. 在 Mihomo 配置中定义两个不同的 child outbound，再定义一个 `type: smp3`
   节点。
3. 将 Clash Party 的 custom core 指向该 Mihomo 二进制。
4. 应用使用 Clash Party 的 mixed/SOCKS5 端口，例如 `127.0.0.1:7890`。

配置模板：[config/mihomo.example.yaml](config/mihomo.example.yaml)。

### Standalone SOCKS5

1. 启动 Carrier-A、Carrier-B，确保它们分别提供本机 `17898`、`17899`。
2. 复制并修改 [examples/smp3-client-config.example.json](examples/smp3-client-config.example.json)，
   为 `upstream_socks.leg0` 和 `upstream_socks.leg1` 分别填写两个 SOCKS5
   监听地址，并设置 SMP3 密码、服务端地址和两个 route。
3. 检查并启动：

```bash
./smp3-client-linux-amd64 -c ./config/smp3-client.json -check
./smp3-client-linux-amd64 -c ./config/smp3-client.json
```

Windows 使用同名 `.exe`。应用代理设置为 `socks5://127.0.0.1:18080`。

### Android Standalone

在 ARM64 Android 设备安装 `smp3-android-standalone-2.4.1-debug.apk`。先让
外部代理核心提供 Carrier-A、Carrier-B 两个本机 SOCKS5 监听，再在 App 中
填写 SMP3 server 地址、相同密码和两个监听端口。App 自身的本地 SOCKS5
入口是 `127.0.0.1:18080`。详细步骤见 [Android 使用说明](docs/android/README.md)。

### Server

服务端使用自己的 SMP3 配置，不是 sing-box 配置：

```bash
cp config/standalone-server.example.json config/server.json
# 修改 listen、password，以及实际 sidecar/telemetry 配置
./smp3-server-linux-amd64 -c config/server.json -check
```

### Integrated Dashboard

Dashboard 由 `smp3-server` 直接提供，不需要再启动独立 Panel。可读接口包括：

```text
GET /api/v1/status
GET /api/v1/legs
GET /api/v1/sessions
GET /api/v1/traffic
GET /api/v1/traffic/history
GET /api/v1/events       # SSE
```

保持 telemetry 只绑定 `127.0.0.1:24500`。原独立 `smp3-panel` 和 `24600` 已退役。

## 启动和停止顺序

`smp3-client` 不会自动启动外部 Carrier-A/B。生产环境应由已有的服务管理器、
计划任务或 supervisor 统一管理，推荐顺序为：

```text
启动：Carrier-A → Carrier-B → smp3-client
停止：smp3-client → Carrier-B → Carrier-A
```

如果只手动启动 `smp3-client`，需要先保证两个 Carrier 入口已经监听。

Standalone 不认识 VLESS、Reality、HY2、Snell 等节点协议，只认识每条 Leg
对应的 SOCKS5 endpoint。节点和协议仍由外部代理软件负责。

## 常用检查

```bash
sha256sum -c SHA256SUMS
curl http://127.0.0.1:24500/api/v1/status
curl 'http://127.0.0.1:24500/api/v1/traffic?period=today'
curl 'http://127.0.0.1:24500/api/v1/traffic/history?resolution=hour'
```

Dashboard 页面应能看到 Leg0、Leg1、Carrier-A、Carrier-B、实时速率、Useful ACK、
流量占比、历史和事件。短连接或低流量请求可能不会触发 Leg1，这是正常的；
不要把“未激活”直接当作 Leg1 故障。

## 相关文档

- [中文部署与使用教程](DEPLOYMENT.zh-CN.md)
- [Standalone Sidecar 说明](SIDECAR.zh-CN.md)
- [双模式产品架构](docs/PRODUCT_ARCHITECTURE.md)
- [Standalone 外部代理 contract](docs/standalone/EXTERNAL_PROXY.md)
- [部署与使用教程](DEPLOYMENT.md)
- [安全说明](SECURITY.md)
- [发布说明](RELEASE_NOTES.md)

不要把生产密码、PSK、Reality 私钥、订阅地址或真实配置提交到仓库。
