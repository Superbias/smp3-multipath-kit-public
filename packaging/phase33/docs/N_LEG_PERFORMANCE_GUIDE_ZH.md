# N-Leg 性能与资源验证指南

## 先区分正确性与性能

单条连接的多 Leg 应以同 Session、逐 Leg 唯一 DATA、ACK、payload SHA、严格 EOF 和账本为证。多个 Carrier 都处于 Established 不能证明聚合，也不能证明独立物理路由。

## 本轮结果边界

Core WSL 32MiB pilot 的中位值（Mbps）为 N=1 4971、N=2 5711、N=3 5592、N=4 5836、N=8 4628、N=16 3890；无带宽整形，N=8/16低于N=1，结果 INCONCLUSIVE。Windows 本地进程 N=1/N=8 的 128MiB 对照约 24.35/24.33 Mbps，64KiB/20ms Python 输入节流主导结果；N=8 的 8 条 Leg 均有唯一字节贡献，但没有性能提升证据。

## 自行校准

按相同有效 Carrier 条数、RTT、带宽上限和故障时间线执行随机配对；先验证 Raw Carrier 有足够容量余量，再跑 Core。记录每秒 app accepted、Core assigned、Carrier written/delivered、ACK、重传、每 Leg unique bytes、CPU、RSS、FD、goroutine、队列与暂停。任何 512MiB/60s、WAN 或长时间结果缺失时必须写 NOT RUN，不能由 loopback 替代。

B0 为默认调度。B1 保持关闭；不要通过增加队列、inflight 或放松恢复期限来制造吞吐结果。
