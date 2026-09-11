# SMP3 Multipath Kit v2.3.2

[English](README.md) | 简体中文

SMP3 是一个独立的应用层多路径传输组件，包含 Standalone、Native 和
R15 Panel 三条产品线。

## 先选使用模式

| 模式 | 适合场景 | 是否需要 sing-box |
| --- | --- | --- |
| Native | Clash Party / Mihomo 直接使用 SMP3，推荐 | 否 |
| Standalone | 给普通应用提供本机 SOCKS5 入口 | 否 |
| `smp3-proxy` | 兼容已有 sing-box 配置 | 是，仅此模式 |
| R15 Panel | 查看状态、Leg、速率、Useful ACK 和历史 | 否 |

最重要的一点：`smp3-client` 只负责 SMP3 和本地 SOCKS5。它不会实现
VLESS、Reality、Hysteria2、Snell 等外层协议，也不知道节点密码。外层节点
配置在 Mihomo/Carrier 中；Standalone 通过 Carrier 提供的 SOCKS5 入口
建立 Leg0、Leg1。

## 当前发布版本

- Release：`v2.3.2`（运行时版本标识已统一；数据面语义不变）
- Native：Mihomo `v1.19.28` + SMP3 adapter
- Standalone：`smp3-client`、`smp3-server`
- Panel：只读 REST/SSE 监控
- 官方下载：[GitHub Releases v2.3.2](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.3.2)
- 下载后先用 `SHA256SUMS` 校验文件，不要直接使用示例配置中的占位密码。

## 当前资格化拓扑

```text
Native：应用 → Mihomo / Clash Party → Leg0 + Leg1 → SMP3 server

Standalone：应用 → 127.0.0.1:18080 → smp3-client
            → Carrier-A / Carrier-B → SMP3 server

监控：SMP3 telemetry 127.0.0.1:24500 → R15 Panel 127.0.0.1:24600
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
| R15 Panel | `127.0.0.1:24600` | 浏览器访问地址 |

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
   设置 SMP3 密码、服务端地址和两个 route。
3. 检查并启动：

```bash
./smp3-client-linux-amd64 -c ./config/smp3-client.json -check
./smp3-client-linux-amd64 -c ./config/smp3-client.json
```

Windows 使用同名 `.exe`。应用代理设置为 `socks5://127.0.0.1:18080`。

### Server

服务端使用自己的 SMP3 配置，不是 sing-box 配置：

```bash
cp config/standalone-server.example.json config/server.json
# 修改 listen、password，以及实际 sidecar/telemetry 配置
./smp3-server-linux-amd64 -c config/server.json -check
```

### R15 Panel

Panel 只读读取 `127.0.0.1:24500`，不会直接读取或修改数据面：

```bash
./smp3-panel-linux-amd64 \
  -listen 127.0.0.1:24600 \
  -telemetry http://127.0.0.1:24500 \
  -history monitor-history.jsonl
```

浏览器打开 `http://127.0.0.1:24600/`。

## 启动和停止顺序

`smp3-client` 不会自动启动外部 Carrier-A/B。生产环境应由已有的服务管理器、
计划任务或 supervisor 统一管理，推荐顺序为：

```text
启动：Carrier-A → Carrier-B → smp3-client → Panel
停止：Panel → smp3-client → Carrier-B → Carrier-A
```

如果只手动启动 `smp3-client`，需要先保证两个 Carrier 入口已经监听。

## 常用检查

```bash
sha256sum -c SHA256SUMS
curl http://127.0.0.1:24600/api/monitor/status
curl http://127.0.0.1:24600/api/monitor/history
curl http://127.0.0.1:24600/api/monitor/events
```

Panel 页面应能看到 Leg0、Leg1、Carrier-A、Carrier-B、实时速率、Useful ACK、
流量占比、历史和事件。短连接或低流量请求可能不会触发 Leg1，这是正常的；
不要把“未激活”直接当作 Leg1 故障。

## 相关文档

- [中文部署与使用教程](DEPLOYMENT.zh-CN.md)
- [Standalone Sidecar 说明](SIDECAR.zh-CN.md)
- [Panel 说明](panel/README.md)
- [安全说明](SECURITY.md)
- [发布说明](RELEASE_NOTES.md)

不要把生产密码、PSK、Reality 私钥、订阅地址或真实配置提交到仓库。
