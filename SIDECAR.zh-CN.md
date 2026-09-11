# SMP3 v2.3.1 Standalone SOCKS5 Sidecar

Standalone Sidecar 为普通应用提供本机 SOCKS5 入口。它负责 SMP3，不负责
实现或保存 VLESS、Reality、Hysteria2、Snell 等外层节点协议。

```text
应用
  -> 127.0.0.1:18080（Sidecar SOCKS5）
  -> Carrier-A / Carrier-B 提供的 SOCKS5
  -> SMP3 server sidecar listener
```

## 使用方式

下载 `smp3-client-windows-amd64.exe` 或 Linux 版本，复制
`examples/smp3-client-config.example.json`，替换所有占位值：

- `listen`：默认 `127.0.0.1:18080`；
- `upstream_socks.address`：默认 Carrier/宿主 SOCKS5；
- `upstream_socks.leg0`、`leg1`：需要分别使用不同 Carrier 时配置；
- `smp3.routes.leg0`、`leg1`：到达 SMP3 server 的 route；
- `smp3.password`：与 server 一致的密码。

例如当前双 Carrier 形态：

```json
{
  "upstream_socks": {
    "address": "127.0.0.1:17898",
    "leg1": { "address": "127.0.0.1:17899" }
  },
  "smp3": {
    "routes": {
      "leg0": "SERVER_IP:24445",
      "leg1": "SERVER_IP:24445"
    }
  }
}
```

两个 leg 可以指向同一个 SMP3 sidecar listener，但必须通过两个独立的
Carrier 连接。示例中的地址和密码只是格式示例，不能直接用于生产。

## 检查和启动

```bash
./smp3-client-linux-amd64 -c ./config/smp3-client.json -check
./smp3-client-linux-amd64 -c ./config/smp3-client.json
```

Windows 使用同名 `.exe`。应用连接：

```text
SOCKS5 127.0.0.1:18080
```

先启动 Carrier-A/B，再启动 Sidecar。Sidecar 自己不会启动外部 Carrier。

## Leg 行为

- Leg0 通常是 preferred/primary leg；
- Leg1 在满足 activation 条件后加入；
- 短连接或低速流量下 Leg1 未激活可能是正常现象；
- `connect_timeout` 限制完整 SOCKS5 CONNECT 事务；
- `carrier_ready_timeout` 限制 CONNECT 成功后等待远端 SMP3 readiness 的时间。

Sidecar 使用 TCP CONNECT 承载 SMP3 Stream HELLO v4；UDP 使用 Datagram
HELLO v5。它不会修改 Mihomo、Clash Party、Carrier、server、防火墙或路由。

## Mihomo 使用 Sidecar

先启动 Sidecar，再让 stock Mihomo 将 `127.0.0.1:18080` 作为 SOCKS5 代理。
参考 `examples/mihomo-sidecar.example.yaml`，并把 Sidecar 的明确路由规则
放在宽泛规则之前，避免代理回环。

## 安全和排障

- Sidecar 只绑定 loopback，不提供公网 SOCKS5；
- 外层节点配置在 Carrier Mihomo，不在 Sidecar；
- `-check` 只检查本地配置，不会连接生产端；
- 两个 leg 都失败时，先查 Carrier SOCKS5、route、SMP3 密码和 server listener；
- 不要把生产配置、密码、PSK 或 Reality 私钥用于本地测试。
