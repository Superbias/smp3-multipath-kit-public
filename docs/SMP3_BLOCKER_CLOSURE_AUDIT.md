# SMP3 blocker closure 独立审计与发布前证据

> 历史报告（reviewed8 / 2026-10-14 早期快照）。运行次数和门禁计数已被最终 C1 证据取代；当前结论、完整 B0/C0/C1 对照和恢复审计以 docs/SMP3_SHARED_CORE_RECOVERY_FIX.md 为准。

**结论：整体 FAIL；本交付是可审阅、可回滚的本地候选，不是发布批准。** 基准为 `907b242c5f2532f774a3db1a877cf32740dcdfab`，输入候选完整补丁 SHA256 为 `a3746db9c9443f28087c3ff628739c6dd11b2393160de50ad5b6b8e215571823`。本轮没有访问生产或 Clash Party，没有执行部署、推送、发布、配置更改、路由切换、服务重启或安装二进制替换。

## 关键不变量的实际代码审计

完整逐项审计、源码 SHA、测试名与边界见 `artifacts/blocker-closure-20261013/six-invariant-code-audit-run8.json`。以下结论基于实际实现和回归，不是对旧报告的复述。

| 不变量 | 实际位置与结论 | 覆盖 | 尚未证明 |
|---|---|---|---|
| owner 迁移与虚拟服务量只退一次 | `core/stream_assignment_debug.go:assignBenchmark` 持 `weightedMu` 串起 owner 和 accounting。已有 owner 快速返回；不同 owner 才把该记录的实际字节从旧 owner 扣除一次并加入新 owner。毕业仅归一 epoch origin，不重置累计量。 | 同 owner、不重基 epoch、probation debt 回归；队列/重试同步点。 | 未形式化枚举所有调度器交错。 |
| ACK、rescue、Close、ReplaceLeg 只退一次 | `core/stream_tx.go:ApplyACK` 在账本锁下只删除活记录并撤销一次 frame/实际 byte；旧/重复/非进展 ACK 不退账。`clearAttemptAt` 以原 attempt 时间戳拒绝旧异步清理。`capacityCloseOnce` 和 provider close guard 负责共享引用。`AttachLeg` 等待旧 worker 退出后再装同 ID generation；单一 `retransmitLoop` 生产 rescue。 | 并发 ACK/失效 race、rescue ACK-before-write、过期清理、provider close、local enqueue ownership；modules race。 | 没有穷举四参与者的所有人为 API 调度。明确取消可让已放弃的 bounded ledger snapshot 留到对象销毁；这不是正常 EOF drain。 |
| 旧引用/旧 generation 不能证明恢复 | `core/stream_tx.go:InvalidateLeg` 标记旧记录失效；`core/stream_admission.go:admissionRejoined/admissionACK` 检查 generation、frontier、阶段时间与新鲜 cohort；旧 normal meter 不进入新 probe。 | 旧 ACK、旧 generation、复制/未写记录、旧 cohort/meter 的确定性回归。 | 不证明任意外部 fallback、路由更换或任意版本对的恢复。 |
| budget 收缩后已接纳记录有退出路 | `core/stream_admission.go:ordinaryEligible` 只限制新普通分配；已接纳义务不会单因 quota 满而撤销。后续依赖真实 ACK、bounded rescue、leg replacement 或用户取消/有界 EOF 收尾。 | budget/ACK pipeline、tail drain、bounded EOF；Linux/Windows 实际 leg1-only 2GiB/EOF/drain。 | carrier 无进展时不能保证 payload；先限制再失去最后腿的所有组合未穷尽。 |
| recovery seed 有预算但可能周期探测 | `ordinaryAssigned/admissionACK` 用单记录实际帧/字节 seed、当前阶段实际字节与唯一 ACK 数、服务下限、cooldown、全局 frame/byte ceiling；每 ACK 最多增长一次，丢弃 surplus。 | 1/4/16/32/64 KiB、只计数不可增额、低服务、fresh clock 与硬边界测试。 | per-stream cooldown 会产生受限周期探测，不是零突发；未按共享 WAN 做全局错峰，因为没有可靠 carrier 身份。 |
| 控制通道、最后发送腿、EOF、取消可推进 | `core/stream_engine.go:sendCloseFrame/Close/fail`：激活意图跨 generation 保留，控制入队非阻塞，CLOSE 有界且可取消；provider 释放一次。 | 满控制队列、attach/rejoin/cancel、bounded EOF；真实 Native 仅 leg1 可用在 Linux/Windows 都完成2GiB、正常 EOF、账本 drain。 | 未加入 wire-level peer receipt ACK；真实生产路由与现存会话行为未测。 |

代码锁顺序和函数 SHA 在上述 JSON 中。窗口/取消压力测试与有限正常 EOF 测试分开判定；取消后的 prefix 完整不能替代正常 EOF。

## 回归与真实进程结果

| 场景 | 结果 | 说明 |
|---|---|---|
| Core 原慢腿主场景 | **3/3 PASS** | 原始 `B=95.6301312 Mbps` 与冻结窗口不变。 |
| Core RTT 20/100/200/400 ms | **3轮 × 16项 PASS** | 每个 RTT 有单路、健康双路、慢腿和恢复，使用冻结旧 B。 |
| Core 短共享流 2/4/8 条 | **3/3 PASS** | 每个被 demand 的 flow 都单独验收。 |
| Core 不同读块有限传输 | **PASS** | 五种 read quantum、同总字节、SHA/正常 EOF/账本 drain。 |
| Core 1800 秒共享实验 | **15/15固定恢复窗 PASS** | refs/entries 有界；无整秒零进展；共享应用加入退出覆盖；没有把显式取消算作 EOF。 |
| Native slow / blackhole 实际进程 | **各3/3 PASS** | 每项2 GiB，SHA `382045c648d7c2a42a01bb3132186a0397d40e2e4e85a99377dbc79c20e6671e`，正常 EOF、服务端账本/队列排空；slow最差 fault窗95.745 Mbps，恢复最差199.662 Mbps，B=99.978370 Mbps。 |
| Native 50/200 Mbps健康进程 | **preferred 3/3；first-ready 3/3** | 约68.8秒，2 GiB/EOF/drain，固定窗约249 Mbps。first-ready 是独立启动语义控制，不把它称作 preferred 双路。 |
| Native RTT 真实进程矩阵 | **13/16 PASS，3 FAIL** | 16项均有2 GiB SHA、正常 EOF/账本 drain。健康单路/双路8项全部 PASS；严格旧 B 不下降。见下表。 |
| Native 1800 秒共享实验 | **13/15窗 PASS；2 FAIL** | 30分钟实际进程，32条流历史，15次加入/30次退出事件，prefix SHA 均通过，最长逐流零进展1秒；资源和恢复状态保留。 |
| Windows Native slow / blackhole | **PASS** | 实际 Windows 服务端与 Native，2 GiB、SHA、EOF/drain；这不是 Windows raw carrier 校准通过的替代证明。 |
| 仅 leg1 可用补测 | **Linux、Windows 均 PASS** | preferred0 profile 下 leg0 拒绝拨号；连接向量 `[0,1]`，固定窗约99.9 Mbps，2 GiB/EOF/drain；仅是 loopback last-leg 进程检查。 |
| 精确新旧二进制互通 | **payload/SHA/EOF 两方向 PASS；旧服务端慢腿吞吐 FAIL** | 旧服务端+新 Native 在故障期固定窗约1.99 Mbps，恢复窗199.652 Mbps；旧服务端 ledger telemetry 404，排空状态未知。新服务端+旧 Native 故障窗约92–95 Mbps，恢复窗196.552 Mbps，server ledger drain PASS。旧服务端已知慢下行故障明确保留；只证明这两个精确本地二进制组合。 |
| 400 ms Linux raw carrier 工具校准 | **4/4 PASS** | raw50、raw200、同时50+200、100 Mbps fragmented 均通过原95%门槛；不包含 Core。 |
| Windows raw carrier 工具校准 | **FAIL** | 当前有效原始校准有200单路与50+200合计低于95%的窗口；保留原始失败和两种诊断工具版本，未改阈值/窗口。测试工具不确定性不混入 Core/Native 成绩。 |

RTT 对照使用相同 Native RX 和 baseline commit 构建的旧服务端作匹配参考，再与此前冻结 B 取较高值；基线 server 不提供新 ledger telemetry，但完整 payload/EOF可验证。这里的 RTT 是隔离 loopback delay 注入，**不是生产 Carrier RTT**。

| 注入 RTT | 单路 B / 结果 | 健康双路 B / 结果 | 慢腿恢复 | 黑洞恢复 |
|---:|---:|---:|---|---|
| 20 ms | 99.959 Mbps / PASS | 199.597 Mbps / PASS | PASS | PASS |
| 100 ms | 99.901 Mbps / PASS | 198.778 Mbps / PASS | **FAIL：55–65秒 109.156 Mbps；恢复到 normal 首次观察111.288秒** | PASS |
| 200 ms | 99.938 Mbps / PASS | 199.410 Mbps / PASS | **FAIL：55–65秒 144.762 Mbps；恢复到 normal 首次观察62.168秒** | PASS |
| 400 ms | 99.956 Mbps / PASS | 201.853 Mbps / PASS | PASS | **FAIL：55–65秒 100.687 Mbps；恢复到 normal 首次观察71.173秒** |

原 Native B=99.978370 Mbps，因此 1.5B 固定恢复门槛=149.968 Mbps。三项故障窗未达门槛；同一项的 payload/EOF/账本虽通过，也不能改为吞吐 PASS。首轮失败后 RTT 的第2、3轮是 **NOT RUN（已知失败后停止，不是环境阻塞）**，没有挑一次后续通过覆盖失败。

互通升级顺序只从这两个精确 loopback 组合作有限判断：新服务端接受旧 Native 的这条2GiB路径通过内容、EOF、排空和吞吐；旧服务端配新 Native 内容/EOF正确，但slow故障窗约1.99 Mbps，吞吐FAIL，且旧服务端没有 ledger API。它仍保留已知故障，不把“能互通”写成修复。若维护者继续做隔离验证，服务端先升级是可测假设；这不是任意版本、UDP/fallback、生产 Carrier 或既有 session 的兼容保证。

Native 长共享实验两条失败固定窗为第590秒118.227 Mbps、第1430秒124.571 Mbps。每个失败时仍有4条活跃流，状态样本为 `[normal,recovering]`；frontier 在健康 leg0。该关系是相关性记录，不证明因果。服务器/Native/工具最大RSS分别266484/167392/42576 KiB；取消后20秒活跃会话数0，Native公开 telemetry 不提供 provider 引用计数。Core refs/entries由互补测试直接断言；Core heap/goroutine/backlog趋势原样在 numerical JSON，RSS本身不构成无泄漏证明。

## 发布门禁：保留 FAIL

原始迁移 parity checker 未改 golden、allowlist、逻辑或跳过路径，未刷新冻结快照：baseline 31项/exit1，冻结 input candidate 51项/exit1，reviewed8 57项/exit1。当前57按分类为14项原有失败、22项原始字节不同但规范化 EOL 后等于 baseline、21项需逐路径审阅；这只是分类，raw 仍 FAIL。input 到 reviewed8 新增6条测试路径、删除0条；6个旧路径失败值变化。完整 before/after 哈希和失败集合在 `artifacts/blocker-closure-20261013/gates-reviewed8/comparison.json`；逐路径建议在 `gate-evolution-proposal-final8.json`，审批均 NOT GRANTED。

Legacy 属于独立生成的 sing-box-shaped `github.com/sagernet/sing-box/protocol/multipath` 模块，其 facade 调本仓库 canonical Core。baseline full race3 的 `TestCoreSilentPrimaryStallActivatesBooster` 失败3/3；baseline queue race30 通过。冻结 input candidate queue为27/30（两次 preferred queue 满时未立即 activate、一次 primary释放后 logical writer未唤醒）；reviewed8 full race3 与 queue race30 当前都通过。该测试默认没有启用 slow-leg guard，但 ACK、账本、writer wake/lifecycle 属共享生产路径，baseline先失败不免除本次交互风险。每一原始日志和退出码保留在 `legacy-*` evidence 目录，不刷新整批 golden或放宽白名单。

## 源码、构建与证据对应

Native 运行时 Core 由 Mihomo build source 的 `go list` 解析到当前仓库 `core/`；20个 runtime GoFiles 的 hash 全部与实际 Native build 起点一致。固定 pin：`cbd11db1e13a75d8e680e0fe7742c95be4cba2be`。生产源58项前后 hash 相同。测试二进制与独立重建逐字节相同：

- Linux server：`229732bc136322dffb945e66f1c7615b746d0c956f803b2816afdd0b1469e5b6`
- Windows server：`3a06e13f5db328d962ec724ef279f7b8542b24bfe51e0b2eb9384e65f9cb8e55`
- Linux Native：`1329b3e2945ebb390d60db50e713c0d303b5bf49c9e9ab94590a30d5cadd1d95`
- Windows Native：`48418d109ce22c7a71b4f369e4b33d6de4cb03c10967802345ed904f6178782c`

四个 build 命令退出0并复现上述四个 hash。附加 `GOWORK=off go mod verify` 两次因 WSL 无法访问 `proxy.golang.org` 下载 `golang.org/x/mod@v0.20.0.mod` 退出1，记作 **ENVIRONMENT BLOCKED**；不影响已完成的逐字节二进制复建，但不能说该模块完整性命令通过。每个测试的实际命令、原始日志/JSON、exit code、二进制与配置见 `artifacts/blocker-closure-20261013/`；命令索引和 manifest 列出源/证据哈希。前一冻结包原728条证据逐文件复核不变。

## 回退与小范围验证边界

三类操作不可混称：`capacity-mode: fixed` 仅关闭本次 dynamic 策略作对照，不能证明固定比例可止损，也不撤回激活/队列/CLOSE公共源码；完整代码回退必须按 reviewed 精确二进制与配置执行并会中断会话；业务止损候选是已验证健康的 leg0 独立直连业务路径，只能由人工先确认实际目的路径、独立入口、实时单路B和RTT，再给新流手工选路。已卡住的旧流由应用负责人决定关闭/重试，不能承诺路由切换迁移现有会话。

`docs/SMP3_RELEASE_ROLLBACK_AND_CANARY.md`、`scripts/manual-smp3-server-rollback.sh`、`scripts/manual-smp3-native-rollback.ps1` 和 plan-only readiness checker 给出人工步骤；均未对生产执行。旧服务端/新 Native 及新服务端/旧 Native 的实验不保证任意版本互通或升级后保留会话。当前生产 RTT、真实 Snell/HY2容量、运行配置热加载等价、现时 leg0业务健康均未重新采集，故生产验证 **NOT RUN / ENVIRONMENT BLOCKED**。没有人工授权，不采取生产操作。

完整全量与输入候选增量补丁、未跟踪新源码/测试、不可变旧证据包、原始新证据包、应用复核、差异清单、回退步骤与 SHA manifest 由仓库根目录 `SMP3-blocker-closure-all-deliverables-20261014.zip` 一并交付。包内失败样本仍是失败；总体结论仍为 FAIL。
