# 配置参考

本版本的三个配置文件均为严格 JSON。未知字段会导致程序拒绝启动。先复制 `configs/*.example.json`，替换保留地址、密码、证书路径和容量先验，再运行对应程序的 `-check`。修改配置后需要重启对应服务。

## Landing：`landing.json`

| 字段 | 必需/默认 | 用途与安全影响 |
|---|---|---|
| `listen` | 否；默认 `127.0.0.1:24444` | SMP3 TCP/TLS listener。对外监听时必须同时设置 `carrier_tls` 和 `application_target`。端口由配置控制，默认示例使用 8443。 |
| `carrier_tls.cert_file`, `key_file` | 对外监听必需 | Landing TLS 证书与私钥路径；私钥不随包分发。TLS 至少 1.2，ALPN 为 `smp3/1`。只读权限应限于 root 与 `smp3` 组。 |
| `password` | 必需 | HELLO HMAC 的共享认证密钥。生成至少 32 随机字节的高熵值并与 Client 保持一致。不要写入工单或日志。 |
| `application_target` | 对外监听必需 | 允许的唯一 TCP 目标，要求 `host:port`。其他 HELLO 目标会在创建 Session 前拒绝。目标应是专用应用端口，不能指向任意代理服务。 |
| `max_sessions` | 否；256 | 活动 Core Session 上限。到达上限后拒绝新 Session；已存在 Session 的另一条 Leg 可继续加入。 |
| `max_connections` | 否；1024 | 包括握手中连接的 TCP Carrier 上限。超额连接在 TLS/HELLO 处理前关闭。 |
| `idle_timeout` | 否；0 表示禁用 | Carrier 读写均无活动时关闭连接。示例 10m；需按业务空闲时长调整。 |
| `graceful_shutdown_timeout` | 否；30s | CLI 等待 Core 关闭的最长时间；systemd 的 `TimeoutStopSec` 应更长。 |
| `log_level` | 否；`info` | `debug/info/warn/error`。Debug 仍不输出密码。 |
| `hello_read_timeout`, `recovery_timeout` | 否；10s / 15s | HELLO 握手与会话超时。恢复算法内部的 200/50/700/5000ms 值不由该字段改写。 |
| `stream.scheduler_mode` | 否；`adaptive` | 示例为 `aggregation`。 |
| `stream.capacity_mode` | 否；`fixed` | 示例为 `dynamic`，需要 aggregation。`bandwidth_mbps` 是容量先验，不是自动测量结果。 |
| `stream.activation_mode` | 否；`legacy` | 示例为 `dynamic`，需要 aggregation。 |
| `stream.experimental_path_aware` | 否；false | 示例显式为 true，以协商 Version 6 同 Carrier 反馈；不改变代码默认值。 |
| `stream.chunk_size`, `queue_frames`, `max_reorder_frames`, `max_inflight_frames` | 有 Core 默认值 | 示例保留此前验证过的 64 KiB/256/4096/1024。没有新的证据不要放大队列或 inflight。 |
| `udp.enabled` | 否；false | 示例关闭。不要仅为绕过 TCP 网络限制打开 UDP。 |
| `telemetry.enabled`, `telemetry.listen` | 否；关闭 | 示例绑定 `127.0.0.1:24500`。服务端只接受回环地址；不要配置公网监控入口。 |
| `telemetry.accounting_path` | 否 | 运行账户可写的数据目录。systemd 模板使用 `/var/lib/smp3`。 |

## Front：`front.json`

| 字段 | 必需/默认 | 用途与安全影响 |
|---|---|---|
| `listen` | 必需 | TCP relay 入口。非回环监听必须提供 `allowed_sources`。 |
| `landing_target` | 必需 | 唯一允许连接的 Landing TCP 地址。Front 不提供任意目标代理。 |
| `allowed_sources` | 非回环监听必需 | CIDR 白名单，例如 `198.51.100.25/32`；替换为 Client 经上游 SOCKS 出口后实际可见的源地址。空列表只允许 loopback listener。 |
| `connect_timeout` | 默认 10s | 建立 Front→Landing TCP 的超时。 |
| `idle_timeout` | 默认 10m | 双向都无字节活动时结束转发；设 `0s` 可禁用。 |
| `buffer_bytes` | 默认 32768 | 每个复制方向的缓冲区，允许 4 KiB–1 MiB。 |
| `max_connections` | 默认 512 | 活动隧道上限。 |
| `graceful_shutdown_timeout` | 默认 30s | 停止接受新连接并等待的时间，超时后关闭现存连接。 |
| `log_level` | 默认 `info` | `debug/info/warn/error`。不记录载荷。 |

## Standalone Client：`client.json`

| 字段 | 必需/默认 | 用途与安全影响 |
|---|---|---|
| `listen` | 必需 | 仅允许 `127.0.0.1` 或 `::1` 的本地 SOCKS5 CONNECT 入口。不要把它改成公网地址。 |
| `upstream_socks.address` | 必需或默认本机 7898 | Client 用于建立 Carrier 的上游 SOCKS5。`leg0` / `leg1` 可分别指定不同出口。凭据可选，配置文件应限制为 Client 服务账户可读。 |
| `smp3.password` | 必需 | 与 Landing 完全相同的共享密钥。 |
| `smp3.routes.leg0`, `leg1` | 必需 | Direct Landing 与 Front Relay 的 `host:port`。两条 TLS 都验证 Landing 的证书身份。 |
| `smp3.carrier_tls.enabled` | 自用示例必需 true | 启用端到端 TLS。`server_name` 是 Landing 证书身份，即使 leg1 的 TCP 目的地是 Front 也不变。 |
| `smp3.carrier_tls.root_ca_file` | 私有 CA 必需 | 自签或私有 CA 的 PEM。若使用系统信任的公有 CA，可省略该字段；不会跳过证书验证。 |
| `smp3.stream.scheduler_mode` | 示例 `aggregation` | Core 调度。一个 SOCKS CONNECT 对应一个 Core Session；同一 Session 建两条独立 Carrier Leg。 |
| `smp3.stream.capacity_mode`, `bandwidth_mbps` | 示例 dynamic / `[100,10]` | 两条腿的配置先验，按实际线路填写；错误先验会影响分配，不代表实测带宽。 |
| `smp3.stream.experimental_path_aware` | 默认 false；示例 true | 显式启用同 Carrier RTT Feedback；要求服务端也显式启用，并通过 TLS 验证 Landing。 |
| `allow_path_rtt_v4_fallback*` | 默认 false | 本自用配置关闭降级。不得为规避 TLS 或认证错误而打开。 |
| `smp3.udp.enabled` | 默认 false | 此发布拓扑为 TCP/TLS Carrier。 |

### 常见错误

- `unknown field`：配置字段拼写或版本不匹配；不要从旧 YAML 直接改扩展名冒充 JSON。
- `application_target is required`：公网/非回环 Landing 必须固定一个应用目的地。
- `carrier_tls certificate and key are required`：公网 Landing 不能明文监听。
- `SMP3 carrier TLS handshake` / `ALPN mismatch`：检查证书链、ServerName、Front 是否透明转发完整 TLS 字节流以及 Landing 是否配置 `smp3/1`。
- `destination is not allowed`：本地 SOCKS 请求的目标必须与 Landing 的固定 `application_target` 一致。
- Leg join 被拒：确认两条 Leg 使用同一密钥、同一目标、不同 Leg ID，且来自同一个 Client Core Session。
