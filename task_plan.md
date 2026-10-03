# SMP3 Global Carrier Capacity

## 目标
实现共享物理载波容量模型，保留单流 dynamic 语义，完成多流、隔离、生命周期、并发与 native race 验证。本目标只本地完成，push/merge/tag/release/deployment 均为 NO。

## 阶段
- [x] 1. 拓扑审计与 CarrierKey/ownership 设计
- [x] 2. Core 全局载波容量 provider/registry 实现
- [x] 3. client/server/Mihomo 接线与隔离
- [x] 4. 单流兼容与多流/生命周期/身份测试
- [~] 5. 基准、压力、CPU/RSS 与 1GiB 长流验证 (local deterministic matrix complete; long-run/CPU/RSS deferred)
- [~] 6. native Linux race 与 tracked-only 验证 (WSL race complete; remote native unavailable)
- [~] 7. 报告、文档、版本状态与最终工作树 (interim promotion report; final gates remain)

## 约束
- 不修改 release/tag/deployment；不自动 push/merge。
- 保留 per-stream estimator 的现有语义与配置兼容性。
- 不引入跨客户端、跨方向、跨物理载波的共享状态。
