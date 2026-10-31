# SMP3 Multipath Kit v2.7.0

[English](README.md) | 简体中文

SMP3 是应用层多路径传输组件。一个应用数据流可以同时使用多条独立建立的 Carrier 连接，称为 **Leg**。共享 SMP3 Core 负责分帧、调度、有序重组、ACK、重传、恢复和关闭。

本仓库提供四种使用方式：

| 产品 | 适用场景 |
| --- | --- |
| **Standalone** | 外部代理 Core 提供 SOCKS5 入口，希望 SMP3 将多条路径汇聚到一个本地 SOCKS5 入口。 |
| **Native Mihomo** | 使用 Mihomo/Clash，希望 SMP3 直接选择 Mihomo 中已有的子代理出口。 |
| **Android Standalone** | 希望 Android App 以独立进程方式管理 Standalone 客户端。 |
| **Compatibility proxy** | 需要可选的 sing-box 兼容构建 smp3-proxy。 |

Standalone 与 Native 的流式传输共用 core/ 中的 canonical Core。宿主适配层负责建立 Carrier 并将其交给 Core，不会各自实现一套调度器或恢复算法。

> **发布验证范围：**v2.7.0 已发布，但发布不代表所有验收门禁均已通过。现有发布证据没有证明普遍适用于真实 WAN、所有跨主机故障场景或正式匹配性能 A/B。选择部署范围前请先阅读 [v2.7.0 发布说明](https://github.com/Superbias/smp3-multipath-kit-public/blob/v2.7.0/RELEASE_NOTES_2.7.0.md)。

## 目录

- [本版包含什么](#本版包含什么)
- [下载与校验](#下载与校验)
- [Standalone 快速开始](#standalone-快速开始)
- [N-Leg 流配置](#n-leg-流配置)
- [协议协商与兼容](#协议协商与兼容)
- [Native Mihomo](#native-mihomo)
- [运行时增删 Leg](#运行时增删-leg)
- [服务端 Dashboard](#服务端-dashboard)
- [Android Standalone](#android-standalone)
- [Front 中继与 Compatibility 兼容版](#front-中继与-compatibility-兼容版)
- [源码构建](#源码构建)
- [安全与运行提示](#安全与运行提示)
- [已知限制](#已知限制)

## 本版包含什么

### 共享多路径 Core

- **N-Leg TCP 流。**一个流最多可使用 256 个协议 Leg ID。每流活动 Leg 数默认上限为 16，可根据主机与部署能力设置 max_active_legs。路由列表中的 Leg ID 从 0 开始且必须连续。
- **多种调度模式。**默认 adaptive；static 保留静态调度；aggregation 使用归一化服务/容量调度。Aggregation 可使用固定带宽先验或动态容量估计，并可选择动态激活和 HOL 完成模式。
- **启动策略。**first-ready 在任一 Carrier 就绪后开始；preferred 为指定 Leg 提供有界宽限期，再释放给其他已就绪路径。
- **慢腿保护与有界修复。**Core 跟踪有序交付和 ACK 进度、限制普通在途工作、约束落后的路径，并可使用有界 frontier 修复/Rescue 减少队头阻塞。恢复由 Core 统一管理。
- **可靠流生命周期。**共享 Core 实现分帧、有序重组、累计 ACK、重传、优雅 EOF/CLOSE、强制关闭、取消和 Carrier generation 替换。
- **协商式 V7 coordinated drain。**双方认证并协商此能力后，计划内移除 Leg 可以先协调暂停及未完成工作结算，再退休 Carrier。它不保证进程重启、Carrier 故障或强制移除时保留现有会话。
- **可选的同 Carrier RTT 反馈。**实验性 Path-aware 调度可使用来自当前 SMP3 Carrier、与身份及 generation 绑定的认证反馈。功能默认关闭，且两端必须同时启用；它不使用 Mihomo URL-test 历史或其他连接的 RTT。
- **UDP 数据报。**UDP 使用独立的数据报传输，本版最多支持两条已配置 Leg。N-Leg 能力针对 TCP 流。

### 宿主产品与运维功能

- **Standalone 客户端/服务端。**JSON 配置、启动前配置检查、可选的 Standalone TCP Carrier TLS、本地 SOCKS5 应用入口、逐 Leg 上游 SOCKS5 和路由/备用目标配置。高级 host_bridge 模式供由宿主负责创建 Carrier 和传输安全的嵌入式集成使用。
- **Native Mihomo 适配。**以 Mihomo 子代理作为 Carrier，与 Standalone 共用同一 Core。节点协议、认证信息以及代理专有的 TLS/Reality/Hysteria2/Snell 行为仍由 Mihomo 管理。
- **Standalone 客户端运行时 Leg 更新。**在支持范围内可追加或移除尾部 Leg，无需重启。更新会生成结果记录；不支持的变更需要重启。
- **集成式服务端 Dashboard。**可选的只读状态、Leg、会话、流量、事件和流量记账视图。默认关闭，仅绑定字面 loopback 地址。
- **Standalone Front。**可选 TCP 转发跳点，不实现代理节点协议，也不额外提供 TLS。
- **Compatibility 构建。**可选的 smp3-proxy，基于固定版本的 sing-box 源码。

## 下载与校验

从 [v2.7.0 GitHub Release](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.7.0) 下载。请用随包 SHA256SUMS 校验每个二进制或 APK；BUILD_MANIFEST.json 记录源码及构建身份。

| 产物 | 平台/用途 |
| --- | --- |
| smp3-client-linux-amd64、smp3-client-linux-arm64 | Standalone 客户端 |
| smp3-client-windows-amd64.exe | Standalone 客户端 |
| smp3-server-linux-amd64、smp3-server-linux-arm64 | Standalone 服务端 |
| smp3-server-windows-amd64.exe | Standalone 服务端 |
| mihomo-smp3-linux-amd64、mihomo-smp3-windows-amd64.exe | 内置 Native SMP3 适配器的 Mihomo；上游 Mihomo v1.19.28 |
| smp3-proxy-linux-amd64、smp3-proxy-windows-amd64.exe | Compatibility 兼容版；sing-box v1.14.0-beta.14 |
| smp3-front-linux-amd64、smp3-front-windows-amd64.exe | 可选 TCP Front 中继 |
| smp3-client-android-arm64 | Android ARM64 Standalone 运行时 |
| smp3-android-standalone-2.7.0-debug.apk | Android App；debug 签名，用于侧载/测试 |

Linux 在下载目录运行：

~~~sh
sha256sum -c SHA256SUMS
~~~

Windows 可用 Get-FileHash .\mihomo-smp3-windows-amd64.exe -Algorithm SHA256（或选择的其他产物），并与 SHA256SUMS 中对应文件的哈希比较。Android APK 使用 debug 签名，不是维护者正式发布签名密钥。

## Standalone 快速开始

Standalone 对应用提供一个本地 SOCKS5 入口。每条 Leg 通过各自的上游 SOCKS5 入口连接到 SMP3 服务端路由。

~~~text
应用程序 -> 127.0.0.1:18080（smp3-client SOCKS5）
           -> Leg 0 -> 上游 SOCKS5 A -> SMP3 服务端
           -> Leg 1 -> 上游 SOCKS5 B -> SMP3 服务端
~~~

外部代理软件提供上游 SOCKS5 入口，并负责 VLESS、Reality、Hysteria2、Snell、Shadowsocks、Trojan 等节点协议。Standalone 模式下 SMP3 不解析这些节点协议。

### 1. 准备服务端

从[服务端 JSON 示例](examples/smp3-server-config.example.json)复制配置，设置高强度随机 password，并选择监听地址和流参数。本机冒烟验证时将 SMP3 listener 绑定到 127.0.0.1。服务端提供启动前配置检查：

~~~sh
./smp3-server-linux-amd64 -c ./server.json -check
./smp3-server-linux-amd64 -c ./server.json
~~~

Windows 使用相同参数：

~~~powershell
.\smp3-server-windows-amd64.exe -c .\server.json -check
.\smp3-server-windows-amd64.exe -c .\server.json
~~~

SMP3 listener 绑定到**非 loopback** 地址时，必须同时配置 Carrier TLS 证书/私钥和固定的 application_target。固定目标是目的地址白名单：请求其他目的地址会被拒绝。不要把无约束的服务端 listener 直接开放到公网。防火墙只开放实际需要的 SMP3 端口。

公网 listener 配置字段示例（需替换所有路径和值，不是完整服务端配置）：

~~~json
{
  "listen": "0.0.0.0:24444",
  "sidecar_listeners": ["0.0.0.0:24445"],
  "password": "替换为长随机密钥",
  "application_target": "service.example.net:443",
  "carrier_tls": {
    "cert_file": "/etc/letsencrypt/live/service.example.net/fullchain.pem",
    "key_file": "/etc/letsencrypt/live/service.example.net/privkey.pem"
  }
}
~~~

固定目标适用于只允许访问指定服务的部署。若需更广泛的代理用途，请仔细设计服务器、Front 和防火墙边界，不要通过移除校验把服务端变成公网任意目的地址代理。

### 2. 准备客户端

复制[客户端 JSON 示例](examples/smp3-client-config.example.json)，设置 SMP3 密码、服务端路由，以及外部代理 Core 提供的实际 SOCKS5 listener。下面是紧凑的双腿结构；三条及以上 TCP 路径请参考下方 N-Leg 示例。

~~~json
{
  "listen": "127.0.0.1:18080",
  "upstream_socks": {
    "address": "127.0.0.1:7898",
    "legs": [
      { "address": "127.0.0.1:17898", "connect_timeout": "10s" },
      { "address": "127.0.0.1:17899", "connect_timeout": "10s" }
    ]
  },
  "smp3": {
    "password": "替换为服务端密码",
    "carrier_tls": {
      "enabled": true,
      "server_name": "service.example.net"
    },
    "routes": {
      "legs": [
        { "id": 0, "name": "path-a", "address": "service.example.net:24444" },
        { "id": 1, "name": "path-b", "address": "service.example.net:24445" }
      ]
    },
    "stream": {
      "scheduler_mode": "aggregation",
      "capacity_mode": "dynamic",
      "max_active_legs": 2,
      "bandwidth_mbps": [100, 100],
      "chunk_size": 65536,
      "queue_frames": 256,
      "max_reorder_frames": 4096,
      "max_inflight_frames": 1024,
      "ack_interval": "20ms",
      "retransmit_timeout": "1500ms",
      "recovery_timeout": "15s",
      "redial_interval": "1s"
    },
    "udp": { "enabled": false }
  }
}
~~~

Carrier TLS 配置必须匹配服务端证书。如果每条 Carrier 已由其他层提供认证加密传输，应先评估部署要求，避免重复套 TLS。bandwidth_mbps 是调度先验，不是实测速率或线路保证。

### 3. 校验、启动并连接应用

~~~sh
./smp3-client-linux-amd64 -c ./client.json -check
./smp3-client-linux-amd64 -c ./client.json
~~~

示例客户端监听 127.0.0.1:18080。将应用设置为使用这个 SOCKS5 代理。Windows 下改用 smp3-client-windows-amd64.exe。

-check 会校验配置并创建客户端对象，但不会开始监听。输出中包含用于排错的 endpoint 标识；共享日志时不要包含认证信息。-version 可查看二进制版本。

## N-Leg 流配置

Standalone v2.7.0 支持有序的 smp3.routes.legs 与 upstream_socks.legs 数组。Leg ID 必须从 0 开始连续递增，配置的 Leg 数不能超过 stream.max_active_legs。协议支持 256 个 Leg ID，默认活动 Leg 上限为 16。若主机或网络无法承担大量并发 Carrier，请设置更低的上限。

~~~json
{
  "upstream_socks": {
    "address": "127.0.0.1:7898",
    "legs": [
      { "address": "127.0.0.1:17898" },
      { "address": "127.0.0.1:17899" },
      { "address": "127.0.0.1:17900" }
    ]
  },
  "smp3": {
    "routes": {
      "legs": [
        { "id": 0, "name": "direct", "address": "landing.example.net:24444" },
        { "id": 1, "name": "relay-a", "address": "relay-a.example.net:24444" },
        { "id": 2, "name": "relay-b", "address": "relay-b.example.net:24444" }
      ]
    },
    "stream": {
      "scheduler_mode": "aggregation",
      "capacity_mode": "dynamic",
      "activation_mode": "dynamic",
      "max_active_legs": 3,
      "bandwidth_mbps": [100, 50, 20]
    },
    "udp": { "enabled": false }
  }
}
~~~

配置说明：

- 路由和上游 SOCKS 项按相同 Leg 顺序对应。逐 Leg 上游项覆盖全局 upstream_socks.address；一条 Leg 失败时，SMP3 不会静默改走另一条 Leg 的上游。
- bandwidth_mbps 是初始容量先验，不是测速结果或线路承诺。
- scheduler_mode 支持 adaptive、static、aggregation。capacity_mode: dynamic、activation_mode: dynamic 和 hol_mode: completion 需要 aggregation。
- startup_policy 支持 first-ready 和 preferred；startup_preferred_leg 与 startup_grace 用于配置首选路径启动宽限期。
- UDP 当前最多支持两条已配置 Leg。TCP 使用三条及以上 Leg 时，请将 udp.enabled 设为 false。
- 旧双腿配置中的 leg1_fallback 或路由 fallback 是该 Leg 的备用 SMP3 服务端路由，不是切换到另一条 Leg 的 SOCKS 上游。

参考[双腿客户端配置](examples/smp3-client-config.example.json)和[三腿配置示例](https://github.com/Superbias/smp3-multipath-kit-public/blob/v2.7.0/packaging/phase33/configs/client-3leg.example.json)。

## 协议协商与兼容

流式 Carrier 首先尝试经过认证的 V7 握手，用于协商 coordinated drain 和 Path RTT 能力。若对端明确拒绝某项可选能力，客户端可在**新建的 Carrier 连接**上使用兼容的旧握手重试。客户端不会在已发送首个 HELLO 的连接上追加第二个 HELLO。

V7 传输中断不等于对端不支持 V7。遇到此类不确定错误后是否重试旧握手由 allow_v7_legacy_fallback 控制，默认关闭。Path RTT 的 V6 握手遇到不确定传输错误时，若要回退到 V4，则还必须同时启用 allow_path_rtt_v4_fallback 与 allow_path_rtt_v4_fallback_on_transport_error；两者默认关闭。认证失败不属于兼容性拒绝。只有充分了解对端兼容要求后才考虑开启回退。

新握手作用于新建 Carrier 连接，不保证任意二进制替换、重启、强制移除或网络故障下的现有流都能存活。

### 常用流配置项

| 配置项 | 含义 |
| --- | --- |
| scheduler_mode | adaptive、static 或 aggregation。 |
| capacity_mode | fixed 或 dynamic；dynamic 需要 aggregation。 |
| activation_mode | legacy 或 dynamic；dynamic 需要 aggregation。 |
| hol_mode | legacy、disabled 或 completion；completion 需要 aggregation。 |
| bandwidth_mbps | 按路由顺序为每条 Leg 提供的初始容量先验。 |
| max_active_legs | 每流活动 Leg 上限，默认 16。 |
| activation_threshold_mbps / activation_window | Aggregation 的激活阈值与观察窗口。 |
| chunk_size | DATA 帧负载大小，默认 65536 字节。 |
| queue_frames / max_inflight_frames / max_reorder_frames | 有界队列、发送窗口及接收重排控制。仅凭负载证据调整。 |
| ack_interval / retransmit_timeout / recovery_timeout / redial_interval | ACK 周期、超时与重连控制；配置应符合两端和部署策略。 |
| startup_policy / startup_preferred_leg / startup_grace | first-ready 或 preferred 启动行为。 |

带宽先验是调度输入，不是实测线路能力保证。建议先使用示例配置，仅依据可复现的测量结果调整参数。
## Native Mihomo

使用 Release 中的 mihomo-smp3，不要使用不带 SMP3 适配器的普通 Mihomo 构建。Mihomo 管理节点协议和子代理；SMP3 代理通过子代理名称选择各条 Leg。

~~~yaml
mixed-port: 7890
allow-lan: false
mode: rule

proxies:
  - name: smp3-main
    type: smp3
    server: service.example.net
    port: 24444
    password: 替换为服务端密码
    scheduler-mode: aggregation
    capacity-mode: dynamic
    activation-mode: dynamic
    max-active-legs: 3
    bandwidth-mbps: [100, 50, 20]
    experimental-path-aware: false
    legs:
      - proxy: direct-path
      - proxy: relay-path-a
      - proxy: relay-path-b

proxy-groups:
  - name: SMP3
    type: select
    proxies: [smp3-main]

rules:
  - MATCH,SMP3
~~~

请在同一 YAML 中定义 direct-path、relay-path-a 和 relay-path-b 这些常规 Mihomo 代理。legs 中的名称必须引用现有且互不相同的子代理。将应用或系统代理指向 Mihomo 的 mixed/SOCKS 端口（本例为 7890）。

Native 使用共享流 Core，不需要再启动 smp3-client。UDP 当前最多支持两条已配置 Leg。experimental-path-aware 默认关闭，且需服务端同时开启；只有在有计划的实验中才开启。

参考 [Mihomo 示例](examples/mihomo-sidecar.example.yaml)和 [Native 适配说明](docs/native/README.md)。

## 运行时增删 Leg

目前只有 Standalone 客户端支持运行时修改 Leg 列表。进程每 500 ms 检查一次本地 reload marker；只编辑 JSON 文件不会自动应用。

1. 备份配置，修改 routes.legs 与对应 upstream_socks.legs 的尾部条目。
2. 请求运行中的客户端加载配置：

~~~sh
./smp3-client-linux-amd64 -c ./client.json -reload
~~~

如需强制移除 Leg，显式执行：

~~~sh
./smp3-client-linux-amd64 -c ./client.json -force-reload
~~~

3. 查看运行日志和 client.json.reload.result.json。结果文件包含操作 ID、提交/部分提交状态、新增/移除 Leg ID，以及补偿处理信息。

热更新只支持规定范围内的尾部追加和移除；它不是所有字段的通用热加载，也不是跨会话分布式原子事务。现有 endpoint/上游身份、监听地址、密码、TLS、调度/恢复参数和 max_active_legs 变化需要重启。启用 UDP 时改变路由数量也需要重启。移除正在使用的 Leg 可能影响活动流；只有双方已协商 V7 时才能使用优雅 drain。强制移除可能中断在途数据。该机制仅适用于 Standalone 客户端，不适用于服务端或 Native Mihomo。

## 服务端 Dashboard

服务端可选启用集成式只读 Dashboard 和本地遥测 API，默认关闭。启用服务端 JSON 配置中的 telemetry 后，telemetry.listen 必须使用字面 loopback 地址；默认是 127.0.0.1:24500。不要把遥测 listener 暴露到公网；远程查看时使用 SSH 端口转发。

可在服务端 JSON 中这样启用：

~~~json
{
  "telemetry": {
    "enabled": true,
    "listen": "127.0.0.1:24500",
    "accounting_timezone": "Asia/Shanghai"
  }
}
~~~

Dashboard 地址为 http://127.0.0.1:24500/。只读 API 包括 /api/v1/status、/api/v1/legs、/api/v1/sessions、/api/v1/sessions/{display_id}、/api/v1/traffic、/api/v1/traffic/history 和 /api/v1/events（SSE）。Dashboard 不提供配置修改或服务端控制。持久化流量记账默认放在服务端配置同目录；如运行目录受限，请设置受保护且可写的路径。

## Android Standalone

Release 中的 smp3-android-standalone-2.7.0-debug.apk 通过 Android foreground service 管理 ARM64 Standalone 客户端独立进程，提供设置、启停/状态与有界日志查看。

1. 在支持的 ARM64 Android 设备上侧载安装 APK。
2. 启动外部代理 Core，并确保其提供两个 SOCKS5 Carrier 入口。
3. 在 SMP3 Android 中配置 SMP3 服务端、密码、本地 SOCKS listener 和两个 Carrier SOCKS 地址。
4. 启动服务，再让支持手动 SOCKS 代理设置的应用连接本地 listener（默认 127.0.0.1:18080）。

APK 使用 debug 签名，仅用于侧载/测试。本版不实现 VPN/TUN、VLESS/Reality/Hysteria2/Snell、订阅、路由规则或代理 Core。此次发布没有完成实体 Android 设备验证。

## Front 中继与 Compatibility 兼容版

### smp3-front

可选 Front 是 public/edge listener 与落地服务之间的 TCP 中继。它不是 SMP3 服务端，不解析代理节点协议，也不额外提供 TLS。配置使用 JSON，参考 [Front 配置示例](https://github.com/Superbias/smp3-multipath-kit-public/blob/v2.7.0/packaging/phase31/configs/front.example.json)。先用 smp3-front -c front.json -check 检查，再用 smp3-front -c front.json 启动。listen 绑定非 loopback 时必须配置 allowed_sources CIDR。连接/空闲超时、缓冲区和最大连接数应与主机能力相匹配。

### smp3-proxy / sing-box 兼容版

只有需要兼容集成时才选择单独构建的 smp3-proxy。它不是 Standalone 客户端，也不能替代 mihomo-smp3。Release 固定使用 sing-box v1.14.0-beta.14。请使用随 Release 提供、适用于该兼容集成的配置；不要把 Mihomo YAML 直接交给它。

## 源码构建

Release 二进制使用 Go 1.25.5 构建。从源码目录运行：

~~~sh
# 只构建 Standalone client 和 server
./build.sh

# Native Mihomo 构建
./scripts/build-native.sh

# sing-box Compatibility 构建
./scripts/build-compatibility.sh
~~~

./build.sh 只构建 Standalone 客户端和服务端。Native 与 Compatibility 是独立构建流程，使用固定版本的上游源码。固定版本、源码身份和产物目标见 BUILD_MANIFEST.json 及 Release 构建脚本。Android APK 使用独立 Gradle 构建与签名配置；已发布 APK 为 debug 签名。

## 安全与运行提示

- 设置高强度随机 SMP3 密码；不要将密码放进日志、源码管理、截图或支持包。
- 升级前备份配置，并记录原有二进制哈希。
- SMP3 listener 绑定非 loopback 时必须配置 Carrier TLS 和固定 application_target 白名单。不要移除此边界来创建公网任意目的地址代理。
- 服务端 Dashboard/telemetry 仅监听 loopback；远程查看请使用 SSH 端口转发。
- 外部代理的 TLS/Reality/Hysteria2/Snell 参数由外部代理或 Mihomo 子代理管理，与可选 Standalone Carrier TLS 是不同层。
- UDP 与 TCP N-Leg 分开，UDP 仅支持两条 Leg。
- 运行时移除、强制关闭、路由调整、重启和 Carrier 故障均可能影响现有会话。协商式 V7 drain 可帮助处理计划内 Leg 移除，但不保证任何情况下会话都不受影响。

## 已知限制

验收范围以发布说明为准。当前已记录的限制包括：

- 尚未建立普遍适用的真实 WAN 资格证明，也未完成匹配的 .3/.4 性能 A/B。
- 现有发布证据未完成正式严格 TCP 黑洞、若干跨主机和强制恢复故障矩阵。
- 历史 L4 的 1,384,448 字节峰值以及 Pilot08 的线上 FIN/RST 发起方仍未查明。
- 没有实体 Android 设备测试；APK 为 debug 签名。
- v2.7.0 发布没有改变阶段证据中的 P0-C OPEN/FAIL 结论。

不要把本地构建、单元测试通过、两条 Carrier 已建立或一次冒烟测试当作真实 WAN 多路径性能证明。请采用可观测的小范围验证，并保留恢复旧二进制和配置的能力。

## 参考资料

- [v2.7.0 发布说明](https://github.com/Superbias/smp3-multipath-kit-public/blob/v2.7.0/RELEASE_NOTES_2.7.0.md)
- [客户端 JSON 示例](examples/smp3-client-config.example.json)
- [服务端 JSON 示例](examples/smp3-server-config.example.json)
- [Mihomo 示例](examples/mihomo-sidecar.example.yaml)
- [Native 适配说明](docs/native/README.md)
- [Compatibility 说明](docs/compatibility/README.md)
- [安全策略](SECURITY.md)
- [GitHub Release 产物与校验文件](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.7.0)
