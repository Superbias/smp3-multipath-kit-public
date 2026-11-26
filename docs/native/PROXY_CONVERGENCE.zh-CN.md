# 推荐部署：现有代理双路径汇入落地 SMP3

[English](PROXY_CONVERGENCE.md) | [Native 入口](README.md) | [项目说明](../../README-zh_CN.md)

本样例来自一次 v2.7.0 Native Mihomo / Clash Party 实际部署，已脱敏。所有地址、名称和密码均为示例或占位符，不能原样投入使用。

## 1. 推荐拓扑与职责

```text
应用 → 本机 Mihomo Native SMP3
       ├─ Leg0 → 前置代理 A → 落地加密代理入口 ─┐
       └─ Leg1 → 前置代理 B → 落地加密代理入口 ─┤
                                               ↓ 在落地机最终拨号
                                      127.0.0.1:24444 SMP3
                                               ↓
                                         真实业务目标
```

两条独立 Carrier 通过不同前置路径，最终在同一落地机汇合。前置到落地可以使用现有代理链、WG、普通路由或其他可靠传输；SMP3 只要求各条路径能正确传输到同一服务，并使用同一认证会话及不同 Leg ID。共享 Core 负责聚合、ACK、恢复和有序交付。

**前置机不需要安装 SMP3、Realm 或 smp3-front。** 本样例也不运行本机 `smp3-client`。可将其中一条路径改为直连落地加密代理；共享的落地入口或其他瓶颈可能限制总带宽，不保证两条路径带宽相加。

关键区别：这里的两个 Mihomo 子节点都以落地加密代理为最终代理出口。它们收到 `127.0.0.1:24444` 目标后，由落地机拨号。若子节点仅终止在前置机，`127.0.0.1` 就指向前置机，不能使用本样例的地址。那种部署应改用所有出口均能到达的落地私网/WG地址，并满足对应监听安全要求。

## 2. 落地接入边界

先准备已有的、经过认证及加密的落地代理，例如 sing-box 提供的 Shadowsocks 2022 TCP 入站：

- 示例地址 `192.0.2.20:28443`（文档专用地址，请替换）。
- 外层代理处理节点认证和加密，允许授权用户拨号至本机 `127.0.0.1:24444`。
- 防火墙只允许预期前置出口访问该入口；IP 白名单是附加限制，不能替代密码和加密。
- 检查代理路由没有把本机目标错误转发到其他出口，也没有私网目标拦截导致连接失败。
- 授权使用此入口的用户可以借落地代理访问本机服务，必须将其视为受信任访问者并控制权限。

SMP3 本身的监听保持 loopback，**不要用公开 Front/DNAT 把无限定目标的监听暴露到公网**。loopback 是只接受落地机本机连接的地址，不是加密协议；实际安全边界还包括外层代理、凭据、路由和防火墙。

## 3. 落地 SMP3 配置

Native 与 Standalone 共用 `smp3-server`，不需要另一套 Native 服务端。从 [v2.7.0 Release](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.7.0) 获取匹配平台的二进制并核对 `SHA256SUMS`。

保存为 `/etc/smp3/server.json`，将占位符替换为长随机密码，并限制文件权限：

```json
{
  "listen": "127.0.0.1:24444",
  "password": "REPLACE_WITH_LONG_RANDOM_SMP3_PASSWORD",
  "stream": {
    "scheduler_mode": "aggregation",
    "capacity_mode": "dynamic",
    "experimental_path_aware": true,
    "bounded_rescue_mode": "bounded",
    "bounded_rescue_budget_basis_points": 300,
    "bounded_rescue_burst_bytes": 0
  },
  "udp": { "enabled": false },
  "telemetry": { "enabled": true, "listen": "127.0.0.1:24500" }
}
```

本配置刻意不设置 `application_target`：当前 loopback 监听允许 Native HELLO 携带不同业务目标，由服务端拨号。这是受保护的代理接入样例。若改成非 loopback 监听，当前服务端要求固定 `application_target` 及 Carrier TLS 证书/私钥；固定目标必须与 HELLO 目的地完全一致，不可直接套用本样例。详见 [固定目标边界](../../README-zh_CN.md)。

检查后再交给 systemd 等服务管理器运行：

```sh
chmod 600 /etc/smp3/server.json
/opt/smp3/releases/v2.7.0/smp3-server -c /etc/smp3/server.json -check
/opt/smp3/releases/v2.7.0/smp3-server -c /etc/smp3/server.json
```

服务启动命令使用上面的绝对路径；启用开机启动，并检查 `ss -lntp` 中 24444、24500 都只绑定 `127.0.0.1`。遥测不应公开，可通过 SSH 本地转发查看。

## 4. Native Mihomo / Clash Party

使用包含 SMP3 adapter 的发布版 Native Mihomo，保留现有规则与代理组。在 `proxies` 中保留已有 `前置-A`、`前置-B` 的真实配置，再增加以下三项。两个落地子节点使用同一落地代理凭据，但通过不同 `dialer-proxy` 到达它：

```yaml
proxies:
  # 前置-A、前置-B：已有代理节点，完整协议配置由用户提供。
  - name: 落地经A
    type: ss
    server: 192.0.2.20
    port: 28443
    cipher: 2022-blake3-aes-128-gcm
    password: REPLACE_WITH_LANDING_SS2022_KEY
    dialer-proxy: 前置-A
    udp: false

  - name: 落地经B
    type: ss
    server: 192.0.2.20
    port: 28443
    cipher: 2022-blake3-aes-128-gcm
    password: REPLACE_WITH_LANDING_SS2022_KEY
    dialer-proxy: 前置-B
    udp: false

  - name: SMP3-落地聚合
    type: smp3
    server: 127.0.0.1
    port: 24444
    password: REPLACE_WITH_LONG_RANDOM_SMP3_PASSWORD
    legs:
      - proxy: 落地经A
      - proxy: 落地经B
    scheduler-mode: aggregation
    capacity-mode: dynamic
    experimental-path-aware: true
    bounded-rescue-mode: bounded
    bounded-rescue-budget-basis-points: 300
    bounded-rescue-burst-bytes: 0
    startup-policy: preferred
    startup-preferred-leg: 0
    startup-grace: 500ms
    udp:
      enabled: false
```

Shadowsocks 2022 密钥必须符合对应算法的长度与编码要求，不能用上述文字占位符启动。SMP3 密码和外层代理密钥是不同的凭据。

将 `SMP3-落地聚合` 加入实际使用的选择组，并选择该节点。避免任何子节点再引用该 SMP3 节点或其选择组形成循环。此处 `dialer-proxy` 是外层 Carrier 的代理链；它不是 fixed-target 部署的“两级 SOCKS 应用握手”。

Clash Party 推荐导入包含原有规则和新增节点的**独立本地配置**，例如 `Native SMP3 示例`，再启用它。原远程订阅保留，便于回退。只修改运行时配置会在重载后丢失；本地快照也不会自动合并远程订阅后续变更，需要自行同步。

本样例显式启用 aggregation/dynamic、Path-aware 与 Bounded Rescue，不依赖不同发布构建的默认值。Path-aware 需要两端支持及可信新鲜反馈；反馈缺失时回退基准调度。300 basis points 是预算参数，不是“所有故障、所有时间窗的总重传永远不超过3%”保证。UDP 本样例关闭。

## 5. 验证与回退

1. 校验二进制哈希；分别完成服务端 `-check` 和 Mihomo 配置检查。
2. 先验证两个落地子节点分别可用，再选择 SMP3 节点。
3. 经本机 HTTP/mixed 代理查询出口；出口应为落地，单靠出口一致不能证明双腿聚合。
4. 传输已知 SHA-256 的足量数据，观察同一 SMP3 会话两个 Leg 的唯一 ACK/有效字节增量；仅看到两条 Established 连接不足以证明聚合。
5. 检查本地配置已持久化、无临时测试规则，服务已开机启动，遥测保持本地监听。

来源部署通过了一次 16 MiB 单连接完整性校验及同会话双腿有效数据检查。测试源有人工限速；该结果是部署冒烟验证，不是 WAN 容量、黑洞、长时或全发布门禁通过声明。

回退时先在 Clash Party 切回保留的原配置及原节点；必要时停止/禁用新增 SMP3 服务。不要直接覆盖外层代理及整套防火墙。配置切换或停止服务可能终止现有连接，不承诺会话保留。始终保留修改前配置和防火墙备份，不将生产密码、真实地址或私有配置提交到仓库。
