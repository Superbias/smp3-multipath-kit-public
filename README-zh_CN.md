# SMP3 Multipath Kit 2.1.1

[English](README.md) | [简体中文](README-zh_CN.md)

围绕可复用 SMP3 Core 和 standalone server 构建的独立应用层多路径传输产品。

- **版本：** `2.1.1`（双向 Stream activation bugfix）
- **Runtime baseline：** `2.0.0`（未改变且 byte-identical）
- **可选 sing-box 兼容 client 构建输入：** `v1.14.0-beta.14`
- **兼容构建 commit：** `4902660f8424fef3c2a60dfcdce7aeadfe3f3b88`
- **预期 sing client 二进制版本：** `1.14.0-beta.14-smp3-2.0.0`
- **预期 standalone server 二进制版本：** `2.0.0`
- **TCP Stream HELLO：** v4（继续兼容 r10 TCP）
- **UDP Datagram HELLO：** v5（MP-UDP 需要 2.0.0 两端）

> 本项目是独立的 SMP3 发布版，不是 SagerNet / sing-box 或 MetaCubeX/Mihomo 官方发布。

## 部署与使用

完整教程见 [DEPLOYMENT.zh-CN.md](DEPLOYMENT.zh-CN.md)。基本流程是：先
配置并检查 `config/standalone-server.example.json`，部署 `smp3-server`，
再配置 `config/mihomo.example.yaml` 或可选的 sing-box client。standalone
server 不是 sing-box server，也不负责终止任何 child carrier 协议。

Windows Mihomo 一键安装：

```powershell
Invoke-WebRequest https://raw.githubusercontent.com/Superbias/smp3-multipath-kit-public/main/scripts/install-mihomo-smp3.ps1 -OutFile install-mihomo-smp3.ps1
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe"
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe" -Check
```

Linux standalone 一键安装：

```bash
curl -fsSL https://raw.githubusercontent.com/Superbias/smp3-multipath-kit-public/main/scripts/install-smp3-server.sh -o install-smp3-server.sh
chmod +x install-smp3-server.sh
sudo ./install-smp3-server.sh --config ./config.json
smp3ctl status
```

Installer 操作：

```powershell
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe" -Check
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe" -Update
.\install-mihomo-smp3.ps1 -CorePath "C:\path\to\mihomo.exe" -Restore
```

```bash
sudo ./scripts/install-smp3-server.sh --check
sudo smp3ctl status
sudo smp3ctl logs -f
sudo smp3ctl update
sudo smp3ctl rollback
```

Windows installer 只替换明确指定的 Mihomo executable；Linux installer 只
管理 standalone server，卸载时默认保留 config，只有显式 `--purge` 才删除。
两者都会用 `SHA256SUMS` 校验 GitHub stable Release 的精确 asset。

## 2.1.1 架构

本版本把可复用的、仅依赖标准库的 SMP3 Core 与 host 解耦：

```text
Mihomo / sing-box client adapter
        ↓  SMP3 adapter：两个独立的 child outbound
        ↓  外部 carrier 服务端终止外层协议
        ↓  原始 TCP stream（承载 SMP3 HELLO 和 frames）
standalone SMP3 server
        ↓
canonical SMP3 Core
        ↓
Internet destination
```

standalone server 是生产 landing endpoint，只处理经过认证的 SMP3 HELLO、
Stream frame 和 Datagram frame。Snell、Hysteria2、VLESS 等外层 carrier 在
standalone 之外的服务端/终止端解封装，再把原始 TCP stream 转交给 SMP3
listener；standalone 不实现也不监听这些 child carrier 协议。两个 child
outbound 分别连接同一个 listener，共享一个 SMP3 logical session，而不是
共享同一个底层 carrier 连接。

## 2.1.1 打包什么

2.1.1 保留已经验证的 2.0.0 wire/runtime 行为，并打包 carrier-agnostic
sing adapter policy、抽离后的 Core、standalone server、Mihomo adapter、
sing-box compatibility integration 与 installer/operations 工具：

1. **TCP 带宽感知 Adaptive Scheduler**：根据每条 leg 的有效 ACK/写入吞吐、写入延迟和队列压力动态修正 `bandwidth_mbps` 权重，尽量减少慢路径拿到过多早期 sequence 后造成 HOL。
2. **Bootstrap Failover**：leg0 先获得一个可配置的抢跑时间；如果硬失败，立即拨 leg1；如果超过 `bootstrap_fallback_delay` 仍未完成，则并行拨 leg1，谁先完成认证 HELLO 谁先建立逻辑 session。
3. **MP-UDP Datagram Mode**：UDP 不再只能走单个 `udp_outbound`，可以真正通过 leg0 + leg1，支持 `stripe`、`duplicate`、`adaptive` 三种模式。
4. **双向 Stream activation**：每个逻辑 Stream session 同时观察 application payload 的上传和下载速率，使用 `max(txRate, rxRate)` 激活 leg1，不再只观察 client TX。

## 两套数据面

```text
                         SMP3 2.0.0
                            │
                 ┌──────────┴──────────┐
                 │                     │
               TCP                   UDP
           Stream Mode          Datagram Mode
                 │                     │
       seq + cumulative ACK      datagram_id + 地址
       reorder + retransmit      bounded dedup
       ACK-paced rescue          不做全局有序等待
                 │                     │
            leg0 + leg1             leg0 + leg1
```

### TCP Stream Mode

TCP 继续使用兼容 r10 的 HELLO v4，保留：

- 单逻辑 TCP 字节流双路径承载；
- bounded outstanding window；
- cumulative ACK；
- 接收端重排与去重；
- 跨 leg retransmit；
- ACK-paced 单 frontier rescue；
- per-leg ACK/control isolation；
- graceful tail drain；
- leg rejoin / same numeric leg-ID replacement；
- session tombstone / retirement barrier。

新增的：

```json
"scheduler_mode": "adaptive"
```

会使用实际链路反馈动态调度，而不是只看静态带宽权重。

### UDP Datagram Mode

UDP 使用独立的 HELLO v5 和 Datagram frame，**不会**强行复用 TCP 的累计 ACK、重传和全局顺序重排。

每个 Datagram 携带：

```text
datagram_id + destination + payload
```

即使 #100 没到，#101/#102 也可以立刻交给应用；duplicate 模式产生的重复包通过有界 dedup window 丢弃。

三种模式：

- `stripe`：每个 Datagram 选择一条 leg，根据带宽权重和队列进行分配；
- `duplicate`：同一 Datagram 双路发送，接收端只交付第一份唯一副本；
- `adaptive`：根据实际吞吐和排队/写入延迟动态偏流，并可通过 `adaptive_duplicate_threshold` 只复制小型延迟敏感包。

R11 将单个路由 UDP Datagram 的上限设为 **16384 bytes**。这与 sing packet routing 的 buffer 边界一致，足够覆盖常规 MTU 大小的 DNS、游戏和 QUIC/HTTP3 流量。更大的 IP 分片 UDP 会明确拒绝而不是静默截断；协议层 UDP fragmentation 暂留给后续版本。

需要注意：当前 child carrier 仍然是支持 TCP 的可靠 outbound。单条 carrier 自己仍可能出现 stream HOL；r11 解决的是 **SMP3 两条路径之间不再为了 UDP 做全局有序等待**，并不是把底层 carrier 变成原生不可靠 UDP。

## Adaptive 分层

r11 有三个不同层次的 Adaptive：

- `scheduler_mode: adaptive`：TCP DATA 在 leg0 / leg1 之间如何分配；
- `udp_multipath.mode: adaptive`：UDP Datagram 在两条 leg 之间如何分配；
- 原有 `leg1_adaptive`：根据持续的逻辑流健康状态决定公网 leg1 使用 primary carrier 还是回退到 fallback carrier；不再假设具体协议类型。

对于每个 Stream session，Adaptive activation 会在同一个
`activation_window` 内同时观察 application payload 的上传和下载速率，
并使用两者较高值 `max(txRate, rxRate)` 与 `activation-threshold-mbps`
比较。这个判断按单个逻辑 session 计算，不会把多个连接聚合；因此下载型
流量也可以激活 leg1，adapter 不需要再实现第二套吞吐算法。

SMP3 不要求特定代理协议。每条 leg 只要使用已配置、并提供可靠 TCP
dial capability 的 child outbound 即可。Snell、Hysteria2、VLESS、Trojan、
TUIC、Shadowsocks、VMess 和 Direct 都可以在 child outbound 支持该能力时使用；
IPv4/IPv6 选择继续交给 child outbound。

三层分离，避免把“链路负载均衡”和“carrier 替换”塞进一个不可解释的状态机。


### 当前 Closeout 的 Review 加固

正式实机验收前的代码 Review 又补掉了一批边界问题：

- 对本来会复制发送的 UDP Datagram，超出 dedup 滑动窗口后再次到达会按 stale duplicate 丢弃；纯 `stripe` Datagram 仍保持完全无序语义，不会因为“到得太晚”而误删唯一包；
- 只有 UDP 流量时，leg1 的 primary carrier 硬故障也会进入共享的 primary/fallback health/cooldown；probation primary 不能仅靠 Dial/HELLO 成功恢复，必须真正成功承载 UDP payload，未使用的 probation 会在关闭时释放全局 canary 槽；
- bootstrap 阶段如果 leg1 选中的 primary carrier 失败，会在同一次 bootstrap 中立即尝试配置的 fallback carrier，因此 `leg0 不可用 + primary 不可用 + fallback 正常` 仍可建 session；
- TCP graceful drain 不再把一个已经 readable 的旧 timer channel 直接当成 stall，而是依据实际观测到的 ACK progress 判断；
- UDP scheduler 改为按排队**字节数**而不是仅按 frame 数计算压力；
- routed address metadata 在分配前限制为 512 bytes，单 Datagram 仍限制为 16384 bytes；
- preferred TCP send queue 真正打满时立即激活 booster，不再必须等完整 `activation_window`。

r11 v5 Datagram 的 HELLO 只协商“这是 Datagram session”，不会协商子策略。因此客户端和服务端应保持 `udp_multipath.mode`、`max_datagram_size` 和 duplicate policy 一致。

## 推荐客户端配置

直接参考：

```text
config/client-adaptive.example.json
```

r11 关键字段：

```json
{
  "type": "multipath",
  "outbounds": ["line-path", "public-hy2"],
  "preferred": "line-path",
  "scheduler_mode": "adaptive",
  "bootstrap_fallback_delay": "250ms",
  "udp_multipath": {
    "enabled": true,
    "mode": "adaptive",
    "queue_frames": 256,
    "max_datagram_size": 16384,
    "dedup_window": 4096,
    "idle_timeout": "2m",
    "adaptive_queue_delay": "120ms",
    "adaptive_duplicate_threshold": 0
  },
  "endpoints": [
    { "server": "YOUR_LANDING_PRIVATE_AGGREGATION_IP", "server_port": 24444 },
    { "server": "YOUR_LANDING_PRIVATE_AGGREGATION_IP", "server_port": 24444 }
  ],
  "password": "YOUR_SMP3_PASSWORD"
}
```

`adaptive_duplicate_threshold: 0` 表示默认不双发 UDP，避免直接翻倍流量。需要游戏/DNS 等低延迟小包双发时，再根据实测设置非 0 阈值。

## Standalone server 配置

生产服务端使用自己的 standalone schema，不是 sing-box 配置。直接参考
`config/standalone-server.example.json`：

```json
{
  "listen": "0.0.0.0:24444",
  "password": "CHANGE_TO_A_LONG_RANDOM_SMP3_PASSWORD",
  "stream": { "scheduler_mode": "adaptive" },
  "udp": {
    "enabled": true,
    "mode": "adaptive",
    "max_datagram_size": 16384,
    "idle_timeout": "2m"
  }
}
```

Linux 检查和安装：

```bash
./dist/smp3-server-linux-amd64 -c config/server.json -check
sudo ./scripts/install-smp3-server.sh --config config/server.json
sudo systemctl status smp3-standalone
```

裸 SMP3 aggregation listener 仍建议放在私网 / WireGuard / 内部地址。完整
的 client、启动、验证、升级、回滚和故障排查流程见 `DEPLOYMENT.zh-CN.md`。

## 兼容性

| Client | Server | TCP | MP-UDP |
|---|---|---|---|
| 2.0.0 client | 2.0.0 server | ✅ | ✅ |
| r11 client | 2.0.0 server | ✅ v4 | ❌ |
| 2.0.0 client | r11 server | ✅ v4 | ❌ |
| alpha2.1 及更早 | 2.0.0 server | ❌ HELLO v3 | ❌ |

2.0.0 的 TCP **仍然写 HELLO v4**；只有 Datagram Mode 才写 v5。

## 构建

```bash
./validate-kit.sh
./scripts/build-phase6-artifacts.sh
```

预期产物：

```text
dist/smp3-server-linux-amd64
dist/smp3-server-windows-amd64.exe
dist/mihomo-smp3-linux-amd64
dist/mihomo-smp3-windows-amd64.exe
dist/smp3-proxy-linux-amd64
dist/smp3-proxy-windows-amd64.exe
dist/SHA256SUMS
```

版本应为：

```text
1.14.0-beta.14-smp3-2.0.0（sing client）
2.0.0（standalone server）
```

## 当前源码验证状态

当前 kit 包含 standalone/Core 与 adapter 两组 multipath `Test*` 测试。`validate-kit.sh` 会输出 standalone、adapter 和总数，并守住 Phase 1 之前的测试基线；本文不重复维护一个会漂移的硬编码数字。

本次环境已完成：

```text
SMP3 package go test            PASS
SMP3 package go test -race      PASS
SMP3 package go vet             PASS
gofmt                           PASS
```

2.0.0 新测试覆盖：

- TCP adaptive scheduler 会避开高延迟路径；
- static scheduler 仍保持静态权重语义；
- TCP v4 HELLO 不被破坏，UDP v5 HELLO 可正常往返；
- UDP duplicate 双发只交付一次；
- UDP 不等待更早的 datagram ID；
- UDP adaptive 会避开慢 leg；
- Adaptive 小包双发严格遵守 threshold；
- UDP 同 numeric leg ID 可以换新 transport generation rejoin；
- 正常关闭 Datagram session 不会被误报为 carrier failure。

### Final Closeout 状态

这个包是 **2.0.0 source package**。固定 sing-box 注入和构建生成的
Linux/Windows client 版本为 `1.14.0-beta.14-smp3-2.0.0`，standalone
server 版本为 `2.0.0`；TCP/UDP 实机验收矩阵见 `TEST_RESULTS.txt`。

完整生成树测试仍有与 SMP3 无关的环境/工具链问题（namespace 权限、当前 Go 的 runtime linkname 兼容性、依赖公网的 TLS fragment 测试）；完整 tree vet 也只有上游 unsafe-pointer 诊断。SMP3 multipath package 门禁通过。H3 100 MiB 仍为 INCONCLUSIVE，因为绕过 SMP3 的 direct aioquic 也能复现同一失败。

## 建议 2.0.0 验收矩阵

```text
TCP 500 MiB 完整上传
TCP 强杀 leg0 -> same-session rejoin
TCP 强杀 leg1 -> same-session rejoin
TCP leg0 完全不可用时 bootstrap
TCP leg0 首拨人为延迟时 bootstrap race
TCP 单 leg0 / 单 leg1 / 双路 aggregate 对照吞吐
UDP DNS/小包 round trip
UDP stripe 确认两条 leg 都有流量
UDP duplicate 确认应用只收到一次
UDP adaptive 给一条 leg 加延迟，确认流量自动偏移
UDP same-ID leg 断线重连
QUIC/HTTP3 或 iperf UDP 通过本地 SOCKS/TUN 实测
```

这些矩阵用于后续部署复核；2.0.0 Release 资产与 checksums 见 GitHub Release。

## 安全说明

SMP3 HELLO 的认证不代表裸 SMP3 是公网加密代理协议。尽量把 aggregation Listener 放私网，并使用加密 child carrier。

不要提交真实 SMP3 密码、Snell PSK、Hy2 密码、TLS 私钥、API Key 或实际部署配置。

更多信息见：`SECURITY.md`、`RELEASE_NOTES.md`、`TEST_RESULTS.txt`、`CHANGELOG.md`。
