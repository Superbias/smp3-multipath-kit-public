# SMP3 2.2.0 独立 SOCKS5 Sidecar

本文说明 SMP3 2.2.0 release candidate 中的独立 sidecar 客户端。它是
面向兼容性的、与 host 无关的 client；已使用 Windows stock Mihomo
v1.19.29 完成验收。

Sidecar 是一个仅绑定本机回环地址的 SOCKS5 服务。它只通过宿主已有的
SOCKS5 端点，使用 TCP `CONNECT` 连接两个 SMP3 route；不会使用上游的
SOCKS UDP ASSOCIATE。两个 route 必须经过各自 carrier 最终到达同一个
standalone SMP3 listener，并且提供可靠的 TCP stream。

Sidecar route 必须指向 standalone server 明确声明的 `sidecar_listeners`。
这些 listener 只有在 canonical HELLO 完成解析、认证和 admission 后才发送
带认证的 `SMP3RDY1` readiness 记录。旧的 `listen`/`listeners` 继续保持
原有 canonical-only 行为，不会发送 READY 字节。

```text
应用
    -> 本机 sidecar SOCKS5（127.0.0.1:18080）
    -> 宿主 SOCKS5 CONNECT（leg0 / leg1）
    -> child carrier 终止端
    -> standalone SMP3 server
    -> 目标地址
```

## 构建和运行

在仓库根目录：

```bash
go build -o smp3-client ./cmd/smp3-client
./smp3-client -c ./examples/smp3-client-config.example.json -check
./smp3-client -c ./config/client.json
```

示例文件中的 endpoint 和 `CHANGE_ME` 都是占位符；连接真实服务前必须全部
替换。监听地址设计上限制为 loopback。

Windows：

```powershell
go build -o smp3-client.exe .\cmd\smp3-client
.\smp3-client.exe -c .\config\client.json -check
.\smp3-client.exe -c .\config\client.json
```

命令还支持 `-version`。`-check` 只校验 JSON 和参数，不会连接上游 SOCKS
或 SMP3 endpoint。

## 配置说明

`upstream_socks.address` 是宿主侧 SOCKS5 端点；同时提供 `username` 和
`password` 时启用 RFC 1929 认证。`smp3.routes.leg0`、`leg1` 是经上游
SOCKS 请求的两个不同 endpoint。`connect_timeout`（默认 `10s`）限制一次
完整的 TCP 建连、greeting、认证、CONNECT 请求和 CONNECT 回复事务；宿主
代理的内部重试不能无限阻塞 primary 到 fallback 的选择。leg1 主 route
失败或达到该超时后，才尝试 `leg1_fallback`。

`smp3.carrier_ready_timeout`（默认 `5s`）限制 SOCKS CONNECT 成功后等待远端
认证 READY 的时间。仅 SOCKS CONNECT success 不能视为远端 carrier ready。

TCP `CONNECT` 使用 SMP3 Stream HELLO v4。第二条 stream leg 由 canonical
Core 的应用流量回调激活，下载型流量也能触发；修复时保持同一个 logical
session ID。UDP 使用 Datagram HELLO v5、首包 lazy bootstrap、逐 datagram
寻址以及 adaptive/stripe/duplicate 策略。UDP engine 终止后可以在原 SOCKS
UDP association 仍存在时重建；association 关闭后禁止再次重建。

## Mihomo 宿主集成

先单独运行 sidecar，再让 stock Mihomo 将它作为本地 SOCKS5 代理使用，参考
`examples/mihomo-sidecar.example.yaml`。Sidecar 不会修改 Mihomo、Clash
Party、carrier 定义、防火墙规则或 standalone server。必须把 sidecar
endpoint 的 route discriminator 放在宽泛规则之前，避免宿主 SOCKS 到
sidecar 的循环。

sidecar 不替换 native Mihomo 或 sing adapter；两者是独立集成，sidecar 不会
修改它们。Native Mihomo 和 stock Mihomo Sidecar 已完成验收；sing-box、
Xray/V2Ray 及其他 SOCKS5 host 目前仅属于架构兼容，仍需各自验收。

## SOCKS5 行为

- `CONNECT` 支持 IPv4、IPv6 和 domain。
- `UDP ASSOCIATE` 分配 loopback UDP 端口，并通过 canonical DatagramEngine
  承载数据。
- `FRAG != 0` 的 UDP 包会安全丢弃，不会关闭 association。
- `BIND` 和未知 command 返回 command-not-supported。
- 本地 SOCKS 不启用认证；依靠 loopback 绑定和本机策略保护。

## 校验

```bash
go test ./client ./cmd/smp3-client
go vet ./client ./cmd/smp3-client
```

真实 carrier/standalone 的实机验收矩阵与本地 package 测试分开。不要使用
生产配置或正在运行的 Clash Party 进行本地测试。
