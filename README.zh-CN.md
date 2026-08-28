[English](README.md) | [简体中文](README.zh-CN.md)
# SMP3 Multipath Kit alpha2.3-r10

> 面向固定 sing-box 基线的实验性应用层多路径传输方案。
>
> **发布版本：** `alpha2.3-r10`  
> **Wire 协议：** `SMP3` HELLO version `4`  
> **固定上游版本：** sing-box `v1.14.0-beta.14`  
> **固定 revision：** `4902660f8424fef3c2a60dfcdce7aeadfe3f3b88`  
> **预期二进制版本：** `1.14.0-beta.14-smp3-alpha2.3-r10`

SMP3 Multipath Kit 是一个下游实验性多路径传输方案，用于将**一条逻辑 TCP 字节流**同时承载在两条独立子链路上，并在落地服务器端完成乱序重排与字节流重组。在大量单链路故障场景下，逻辑连接仍可保持存活。

它**不是 sing-box 官方功能，也不是 SagerNet 官方发布版本**。生成的可执行文件使用 `smp3-proxy` 名称，以避免造成官方项目的误解。

---

## 1. 快速开始（使用方法）

下面是“Linux 落地服务器 + Windows 客户端”的最短部署路径。正式部署前，请替换所有 `YOUR_*` 和 `CHANGE_*` 占位符。网络结构和安全边界见[这个版本适合做什么](#2-这个版本适合做什么)。

### 1. 构建并验证

在 Debian、Ubuntu 或 WSL 中执行。构建过程会下载固定版本的 sing-box 源码，并可能下载所需的 Go toolchain，因此需要网络访问。

```bash
mkdir -p "$HOME/go-tmp" "$HOME/go-build-cache"

GOTMPDIR="$HOME/go-tmp" \
GOCACHE="$HOME/go-build-cache" \
./validate-kit.sh

GOTMPDIR="$HOME/go-tmp" \
GOCACHE="$HOME/go-build-cache" \
./build.sh
```

预期生成：`dist/smp3-proxy-linux-amd64` 和 `dist/smp3-proxy-windows-amd64.exe`。

### 2. 生成独立密钥

```bash
./scripts/gen-secrets.sh
```

将生成的 SMP3 password 和 Snell PSK 写入配置；Hysteria2 password 需要另外使用密码学安全的随机生成器生成。不要提交实际密钥、证书或部署配置。

### 3. 配置并安装 Linux 落地服务器

```bash
cp config/server-hy2-snell.example.json config/server.json
```

编辑 `config/server.json`，替换落地地址、carrier 凭据、证书路径和 SMP3 password 等全部占位符。然后在 Linux 落地服务器上校验并安装：

```bash
./dist/smp3-proxy-linux-amd64 check -c ./config/server.json
sudo ./install-server.sh ./config/server.json
```

查看服务状态和实时日志：

```bash
systemctl status smp3-proxy --no-pager
journalctl -u smp3-proxy -f --no-pager
```

### 4. 配置并安装 Windows 客户端

在 Windows 上复制 adaptive 客户端示例并替换占位符。两个 endpoint 都填写私网 SMP3 聚合地址；每条 leg 如何到达该地址由对应的 child outbound 决定。

```powershell
Copy-Item .\config\client-adaptive.example.json .\config\client.json
.\dist\smp3-proxy-windows-amd64.exe check -c .\config\client.json
PowerShell -ExecutionPolicy Bypass -File .\install-client.ps1
```

安装脚本会创建 `smp3-multipath` 计划任务并启动客户端。

### 5. 接入本地代理并验证

在 Mihomo 中使用 `127.0.0.1:2080` 作为本地 SOCKS5 地址，例如参考 `config/mihomo-snippet.yaml`。然后检查客户端是否监听：

```powershell
Test-NetConnection 127.0.0.1 -Port 2080
```

排查问题时可以以前台方式启动客户端：

```powershell
.\dist\smp3-proxy-windows-amd64.exe run -c .\config\client.json
```

后续章节提供详细的配置、安装、运行状态检查和故障排查说明。

---

## 2. 这个版本适合做什么

典型部署结构是在客户端和同一台落地服务器之间准备两条互相独立的链路：

```text
应用程序 / Mihomo
        │
        │ SOCKS5 127.0.0.1:2080
        ▼
SMP3 客户端
        │
        ├── leg0：优选线路 / 私有线路
        │        例如：专线 + Snell / 私有隧道
        │
        └── leg1：公网辅助线路
                 优先 Hy2 -> Snell 回退（可选自适应模式）
                         │
                         ▼
                 SMP3 落地监听器
                         │
                         ▼
                      Internet
```

SMP3 聚合的是 **TCP** 流量。

UDP 不参与 SMP3 多路径聚合，而是通过配置中的 `udp_outbound` 单独发送。

### 推荐的安全边界

SMP3 Listener 会通过 HMAC 对 HELLO 进行认证，但 SMP3 本身并不是用于直接暴露公网的加密代理协议。

推荐将聚合 Listener 绑定在私网、WireGuard 或内部网络地址，并让公网路径通过 Hysteria2、Snell 等加密载体进入。

概念结构如下：

```text
私有线路 -------------> 私网 SMP3 Listener :24444
公网 Hy2 / Snell ------> 加密载体 ------------> 私网 SMP3 Listener :24444
```

除非你已经在外层独立完成了可靠的加密与访问控制，否则**不要直接将裸 SMP3 Listener 暴露到公网**。

---

## 3. r10 主要改进

`alpha2.3-r10` 主要解决两个问题：

- 累计 ACK 场景下的队头阻塞（HOL）修复；
- 同一数字 leg ID 对应的底层连接被替换后的恢复问题。

### ACK 驱动的单 frontier Rescue

SMP3 v4 使用累计 ACK，但当前没有 SACK。

当一个较早的 sequence 因为慢链路而延迟时，后续 DATA 即使已经到达接收端并进入缓存，也无法被累计 ACK 确认。

R10 因此只修复当前已经确认的真正阻塞点：

```text
outstanding[ackedNext]
```

如果当前 frontier 已经超过重传阈值，并且某次 ACK 推进了 `ackedNext`，R10 会立即检查新暴露出来的 frontier，而不是继续等待下一次周期性 retransmit tick。

R10 **不会**无依据地固定批量发送多个 rescue frame。

### 同 ID 底层连接替换增强

当某个 leg 对应的具体 transport generation 失效时，R10 会清除与该失效 generation 相关的陈旧 outstanding ownership。

这样，即使后续重新建立的 transport 仍然使用相同的数字 leg ID，也可以正确 replay 尚未 ACK 的旧 DATA。

同时，dead-leg replay 的候选 DATA 会按照 sequence 排序，从累计 ACK frontier 开始优先恢复，而不是依赖 Go map 的随机遍历顺序。

### 不需要 Wire / 配置迁移

R10 没有修改：

- SMP3 Wire / HELLO version（仍然是 `4`）；
- JSON 配置 schema；
- rescue queue 大小；
- adaptive fallback 阈值。

为了获得确定性的测试结果和更方便的问题定位，仍然建议客户端和服务端同时升级到相同的 r10 版本。

---

## 4. 当前能力

- 一条逻辑 TCP 连接可承载在两条独立 child outbound 上。
- 优选 `leg0` + 按需参与的 `leg1` Booster。
- 服务端允许任意一个认证成功的 leg 先到达，并由它创建逻辑 session。
- 每条 leg 独立重拨、重连、重新加入 session。
- 有界 TX outstanding window。
- 累计 ACK 与基于 ACK 的 DATA retirement。
- 未确认 DATA 可以跨 leg 重传。
- 对超时累计 ACK frontier 进行 ACK-paced rescue。
- 接收端 sequence 去重与乱序重排。
- 每条 leg 独立 ACK / control writer，避免一条阻塞链路同步拖死另一条链路的控制流量。
- 逻辑 `CLOSE` frame 与基于 ACK progress 的 graceful tail drain。
- 已完成 session 使用 tombstone 拒绝延迟到达的旧 leg。
- 服务端支持 same-ID retirement barrier。
- 可选的 `leg1` 自适应载体：Hy2 优先，Snell fallback。
- Adaptive 决策关注持续的逻辑数据流影响，而不是单纯根据 UDP 抖动或丢包判断。
- UDP 通过 `udp_outbound` 路由，不进入 SMP3 多路径数据面。

---

## 5. 已知限制

当前仍然属于实验版本，尤其需要注意：

1. **没有 SACK。** SMP3 v4 目前只有累计 ACK。Rescue 策略刻意保持保守，一次只修复当前已经确认的 frontier blocker。
2. **初始优选 leg Dial 失败目前不等于 bootstrap failover。** 如果 `leg0` 在逻辑连接建立前的初次 `DialContext` 阶段就失败，当前 outbound 不会自动改用 leg1 创建该逻辑连接。只要逻辑 session 已经建立，后续正常的 leg failure / rejoin 恢复机制才会生效。
3. **只有 TCP 会进入多路径聚合。** UDP 不进入 SMP3 multipath data plane。
4. **SMP3 不是通用公网加密隧道。** 请使用加密 child carrier，并尽量保持裸 SMP3 Listener 位于私网。
5. **性能取决于链路 RTT、链路不对称程度、carrier 拥塞情况以及 queue/window 配置。** 下方实测数据只代表同一测试环境中的行为对比，不代表带宽保证。

---

## 6. r10 已完成验收

### 源码 / 单元测试验证

正式发布源码包含 SMP3 Core、Protocol 和 Adaptive 相关 standalone 测试。

当前版本已验证：

```text
65 standalone tests
Go test: PASS
Go race test (-count=5): PASS
Go vet: PASS
Gofmt: PASS
JSON examples: PASS
Shell/Python syntax: PASS
```

详细范围见：

```text
TEST_RESULTS.txt
BUILD_STATUS.md
```

### 500 MiB 实机测试

所有实机上传测试的 Payload 都严格为：

```text
524288000 bytes
```

并且最终均完成：

```text
HTTP=200
UPLOADED=524288000
```

实测场景：

| 场景 | 验收 | 实测结果 |
|---|---|---:|
| R9 参考基线 | 完整性 PASS，但吞吐受限 | 456.219 s，1.149 MB/s（约 9.19 Mbps） |
| R10 正常多路径 | PASS | 37.503 s，13.980 MB/s（约 111.8 Mbps） |
| R10 优选路径人为增加 300 ms 延迟 | PASS | 119.762 s，4.378 MB/s（约 35.0 Mbps） |
| R10 强制销毁优选 leg TCP 并进行 same-ID rejoin | PASS | 22.665 s，23.132 MB/s（约 185 Mbps） |

强制 rejoin 测试还额外验证了：

- `leg0` 被强制销毁时仍有数百个 DATA frame 尚未 ACK；
- 原逻辑 session 在 `leg1` 的支撑下保持存活；
- 新 `leg0` 使用了新的 TCP 连接；
- 服务端记录的是 `multipath leg 0 joined/rejoined session`，而不是重新创建一个逻辑 session；
- 客户端重新进入 `multipath leg 0 ready`，并且 leg0 再次参与数据面；
- 完整 500 MiB Payload 在应用层没有重新建连的情况下正常上传完成；
- 验收测试中没有观察到错误的 Hy2 fallback；
- 验收测试中没有观察到真实 `tx_ack_stall` 事件。

以上数据均与测试环境强相关，仅适合用于同一测试环境内的版本行为对比。

---

## 7. 发布包内容

纯净的 **source release** 包含：

```text
README.md
README.zh-CN.md
RELEASE_NOTES.md
CHANGELOG.md
BUILD_STATUS.md
TEST_RESULTS.txt
SECURITY.md
NOTICE.md
VERSION
build.sh
validate-kit.sh
package-release.sh
install-server.sh
install-client.ps1
config/
patches/
scripts/
src/
dist/BUILD_REQUIRED.md
MANIFEST.sha256
```

正式发布包不应包含：

- 实际部署使用的 `config.json`；
- 私钥；
- PSK；
- 密码；
- 测试日志；
- `.git`；
- `.work`；
- build cache；
- 本地编辑器元数据。

如果包内没有预编译二进制，可以执行：

```bash
./build.sh
```

由 `package-release.sh` 生成的 binary release 会额外包含：

```text
dist/smp3-proxy-linux-amd64
dist/smp3-proxy-windows-amd64.exe
dist/SHA256SUMS
```

---

## 8. 环境要求与构建

推荐环境：Debian / Ubuntu / WSL，并建议在 Linux 文件系统内完成构建。

基础工具：

```text
git
python3
Go 1.21+
```

当前固定的上游版本实际需要 Go `1.25.5`。

`build.sh` 会设置：

```text
GOTOOLCHAIN=go1.25.5+auto
```

首次构建需要能够访问网络，以获取固定 sing-box 源码，以及必要时下载 Go toolchain 和 module cache。

### 验证源码包

```bash
mkdir -p "$HOME/go-tmp" "$HOME/go-build-cache"

GOTMPDIR="$HOME/go-tmp" \
GOCACHE="$HOME/go-build-cache" \
./validate-kit.sh
```

### 构建 Linux + Windows 二进制

```bash
GOTMPDIR="$HOME/go-tmp" \
GOCACHE="$HOME/go-build-cache" \
./build.sh
```

预期输出：

```text
dist/smp3-proxy-linux-amd64
dist/smp3-proxy-windows-amd64.exe
dist/SHA256SUMS
```

检查版本：

```bash
./dist/smp3-proxy-linux-amd64 version
```

PowerShell：

```powershell
.\dist\smp3-proxy-windows-amd64.exe version
```

预期版本：

```text
1.14.0-beta.14-smp3-alpha2.3-r10
```

---

## 9. 生成部署密钥

每套部署环境都应重新生成独立密钥：

```bash
./scripts/gen-secrets.sh
```

脚本会输出类似：

```text
SMP3_PASSWORD=...
PUBLIC_SNELL_PSK=...
```

Hysteria2 密码建议另外使用密码学安全的随机生成器生成。

永远不要提交或公开：

- SMP3 password；
- Snell PSK；
- Hysteria2 password；
- TLS private key；
- 实际部署配置；
- 云厂商、服务商 API Key / Credential。

发布包中的 `config/*.example.json` 只使用占位符，请先复制一份再修改。

---

## 10. 配置模型

### 推荐 Adaptive Client

从下面的示例开始：

```text
config/client-adaptive.example.json
```

关键配置：

```json
{
  "type": "multipath",
  "outbounds": ["line-path", "public-hy2"],
  "preferred": "line-path",
  "udp_outbound": "line-path",
  "leg1_fallback": "public-snell",
  "endpoints": [
    { "server": "YOUR_LANDING_PRIVATE_AGGREGATION_IP", "server_port": 24444 },
    { "server": "YOUR_LANDING_PRIVATE_AGGREGATION_IP", "server_port": 24444 }
  ],
  "password": "YOUR_SMP3_PASSWORD"
}
```

两个 `endpoints` 分别对应两条 child path。

它们可以同时指向同一个私网聚合 Listener，实际每条连接如何到达该 Listener，由对应的 child outbound 决定。

### 服务端

从下面的示例开始：

```text
config/server-hy2-snell.example.json
```

服务端示例允许通过加密公网 carrier 接入，但 SMP3 本身仍绑定到：

```text
YOUR_LANDING_PRIVATE_AGGREGATION_IP:24444
```

部署前必须替换所有：

```text
YOUR_*
CHANGE_*
```

字段。

### 校验实际配置

Linux：

```bash
./dist/smp3-proxy-linux-amd64 check -c /path/to/config.json
```

Windows：

```powershell
.\dist\smp3-proxy-windows-amd64.exe check -c .\config\client.json
```

---

## 11. Linux 落地服务器安装

先复制示例配置，不要直接修改发布包中的 example：

```bash
cp config/server-hy2-snell.example.json config/server.json
```

替换其中所有地址、密码、证书路径等占位符，然后先执行配置校验。

安装二进制与 systemd 服务：

```bash
sudo ./install-server.sh ./config/server.json
```

安装脚本会写入：

```text
/usr/local/bin/smp3-proxy
/etc/smp3-proxy/config.json
/etc/systemd/system/smp3-proxy.service
```

查看状态：

```bash
systemctl status smp3-proxy --no-pager
```

实时日志：

```bash
journalctl -u smp3-proxy -f --no-pager
```

推荐过滤：

```bash
journalctl -u smp3-proxy -f --no-pager | \
grep -Ei 'multipath|session|leg 0|leg 1|join|rejoin|closed|failed|error'
```

---

## 12. Windows 客户端安装

复制 adaptive 示例：

```powershell
Copy-Item .\config\client-adaptive.example.json .\config\client.json
```

编辑 `client.json` 并替换全部占位符。

配置校验：

```powershell
.\dist\smp3-proxy-windows-amd64.exe check -c .\config\client.json
```

安装或更新计划任务：

```powershell
PowerShell -ExecutionPolicy Bypass -File .\install-client.ps1
```

示例配置中的默认本地 SOCKS / Mixed Listener：

```text
127.0.0.1:2080
```

Mihomo 可以使用仓库提供的：

```text
config/mihomo-snippet.yaml
```

将 SMP3 作为本地 SOCKS5 节点接入。

测试本地 Listener：

```powershell
Test-NetConnection 127.0.0.1 -Port 2080
```

---

## 13. 运行状态验证

### 客户端 Health Log

进行详细验证时，可以临时启用 debug logging，并重点过滤以下字段：

```text
mp health
frontier_rescues
ack_frontier_leg
ack_frontier_multi
ack_frontier_age
ack_progress_age
tx_outstanding
tx_goodput
tx_ack_stall
fallback
leg 0 down
leg 0 ready
```

含义：

- `tx_outstanding`：尚未被累计 ACK 确认的 DATA。
- `frontier_rescues`：当前累计 ACK frontier 通过另一条 leg 进行 rescue 的累计次数。
- `ack_frontier_leg`：当前 ACK blocker 最近一次对应的具体 leg。
- `ack_frontier_multi=true`：当前 blocker 已经在多条 leg 上尝试过发送，Adaptive 不应把问题简单归咎于某一个 carrier。
- 较低且持续刷新的 `ack_progress_age`：说明累计 ACK 仍在持续向前推进。

### 服务端 Rejoin 证据

一次成功的 same-session transport repair 通常应看到：

```text
multipath server leg 0 down ...
inbound connection from ...
multipath leg 0 joined/rejoined session ...
```

修复后的 leg 应该**重新加入原有 session**，而不是为同一条仍然存活的逻辑流重新创建新的 destination session。

---

## 14. Adaptive Hy2 -> Snell 行为

`client-adaptive.example.json` 默认将 Hy2 配置为公网 `leg1` 的优选 carrier，并将 Snell 设置为 fallback。

Adaptive Controller 的设计刻意保持保守。

普通 UDP 丢包、抖动、QUIC retransmission 或短时间速率波动，本身不会直接触发 fallback。

它主要观察持续的**逻辑流影响**，包括以下因素的组合：

- 逻辑 RX / TX goodput 明显下降；
- 累计 ACK progress 长时间停滞；
- outstanding frame 压力；
- reorder / pending 压力；
- leg1 有效贡献下降；
- 问题是否持续超过配置的 SUSPECT 窗口。

示例默认值：

```text
evaluation interval:       1s
warmup:                    5s
suspect window:            8s
hard failure:              2 events / 15s
cooldown:                  90s
max cooldown:              5m
recovery stable window:    20s
goodput degradation ratio: 0.4
```

进入 cooldown 后，新逻辑连接使用 Snell。

Cooldown 到期后，恢复阶段会通过有限的 Hy2 probation canary 承载真实有效流量。只有 Canary 表现稳定之后，Hy2 才会恢复为全局健康状态。

Adaptive 演进和回归修复细节见：

```text
CHANGELOG.md
```

---

## 15. 升级兼容性

Wire 兼容关系：

```text
alpha2 / alpha2.1: hello version 3
alpha2.2 / alpha2.3: hello version 4
```

因此：

- alpha2.3-r10 在 SMP3 HELLO 层面与 alpha2.2 Wire Compatible；
- alpha2 / alpha2.1 与 alpha2.2 / alpha2.3 不兼容；
- 实际部署和问题排查时，尽量使用相同的 r10 客户端和服务端二进制。

R10 本身不要求从之前的 alpha2.3 revision 迁移 JSON schema。

---

## 16. 可复现构建与发布完整性

Build Script 会执行：

1. checkout 精确固定的上游 revision；
2. 注入 `src/` 中的 SMP3 源码；
3. 对注入 package 执行 format / test；
4. 构建 Linux/amd64 和 Windows/amd64；
5. 生成二进制 SHA256 校验值。

Release Package 另外提供：

```text
MANIFEST.sha256
```

用于校验源码 / 发布文件完整性。

解压 source release 后执行：

```bash
sha256sum -c MANIFEST.sha256
```

如果包含二进制：

```bash
cd dist
sha256sum -c SHA256SUMS
```

---

## 17. 纯净发布策略

发布脚本通过 whitelist / staging directory 生成正式包，并明确排除常见的本地文件与实际部署文件。

公开 Release 不应包含：

```text
.git/
.work/
.release-stage/
__pycache__/
*.pyc
*.pyo
config.json
client.json
server.json
*.log
private keys
real credentials
local test uploads
```

Example JSON 中涉及 secret 的字段也应保持：

```text
YOUR_*
CHANGE_*
```

占位符状态。

---

## 18. 常见问题排查

### `missing/non-executable final Linux binary`

说明你正在没有完成 build 的情况下执行 binary packaging。

先执行：

```bash
./build.sh
```

或者直接使用 source release。

### 无法下载 sing-box / Go toolchain

`build.sh` 需要网络访问固定 upstream revision，并且可能需要下载指定 Go toolchain / module。

请在 DNS / HTTPS 工作正常的环境内构建。

### Client SOCKS 端口没有监听

使用实际配置以前台方式启动客户端：

```powershell
.\dist\smp3-proxy-windows-amd64.exe run -c .\config\client.json
```

然后检查启动错误。

### 只看到一条 leg

同时检查客户端和服务端日志。

服务端允许 leg0 或 leg1 任意一个先到达，**到达顺序不代表调度角色**。

还需要确认对应的 child outbound 本身能够正常访问 aggregation endpoint。

### 大文件上传卡在 `tx_outstanding=1024`

重点观察：

```text
ack_progress_age
ack_frontier_age
ack_frontier_leg
ack_frontier_multi
frontier_rescues
```

R10 应该对已经超时的累计 ACK blocker 进行 ACK-paced repair，同时避免对 `ackedNext` 之后尚未证实阻塞的 DATA 进行无依据批量 rescue。

---

## 19. 开发说明

R10 主要实现文件：

```text
src/protocol/multipath/core.go
src/protocol/multipath/core_test.go
src/protocol/multipath/adaptive.go
src/protocol/multipath/adaptive_test.go
src/protocol/multipath/outbound.go
src/protocol/multipath/inbound.go
src/protocol/multipath/protocol.go
src/option/multipath.go
```

历史 Diff 保留在：

```text
patches/
```

用于版本追溯。

其中：

```text
patches/alpha2.3-r9-to-r10.diff
```

是 r9 -> r10 的聚焦修改集。

```text
patches/alpha2.2-to-alpha2.3.diff
```

是 alpha2.3 的累计历史 Patch Artifact。

---

## 20. License / Attribution

参见：

```text
NOTICE.md
```

上游项目：sing-box / SagerNet。

本项目是实验性的下游衍生版本，与 SagerNet 无隶属关系，也不代表 SagerNet 官方认可或背书。
