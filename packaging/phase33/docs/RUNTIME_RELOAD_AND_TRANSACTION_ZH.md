# Standalone Runtime Reload 与配置事务

## 请求方式

- Linux 服务：`systemctl reload smp3-client`，unit 向 Client 发送 SIGHUP。
- CLI：`smp3-client -c <配置路径> --reload`。
- 强制移除：`smp3-client -c <配置路径> --force-reload`，会对被移除 Leg 执行强制失效，当前 Session 数据依赖 Core 的既有恢复路径。
- Windows 的 `registerReloadSignals` 不处理 Unix 信号；Windows 使用 CLI marker 入口。
- 配置文件保存本身不会触发 reload。marker 与结果文件放在配置文件旁边，文件系统权限属于管理安全边界；默认没有公网管理 API。

## 阶段与可见性

实现先读取并完整校验候选配置、路线 ID/上限、TLS 等字段，计算 route-count/name/bandwidth diff 并拒绝不支持的字段。新 Leg 在每个活动 Session 中逐一拨号并 attach 到原 Core Session，成功后发布候选 runtime，再执行删除。删除前停止 desired route；普通路径调用 Core `DrainLeg` 并受 drain timeout 限制，强制路径调用 `ReplaceLeg`/`WaitLegRetired`。成功后更新 bandwidth prior 与缺失 Leg repair 状态。

若添加失败，会强制撤销已经在其他 Session staged 的新 Leg。若删除失败，尝试恢复旧 runtime/routes 并补回已经删除的 Leg，再撤销新增 Leg。所有无法补偿的动作进入 `Compensations`，`Partial=true` 和错误会返回。由于 Sessions 并非在一个底层资源事务内共同 prepare/commit，此机制是“按 Session 分阶段收敛 + 最佳努力补偿”，不保证跨所有 Session 的强原子性。

## 支持矩阵

| 变更 | 行为 |
|---|---|
| 密集列表末尾新增 Leg | 热加，使用新连接并加入现存 Session |
| 密集列表末尾删除 Leg | 普通请求 drain；强制请求 replace/retire |
| bandwidth prior、display name | 热应用 |
| 已存在 Leg endpoint/fallback/upstream | 拒绝热改，需要 restart/明确的连接替换流程 |
| TLS、安全身份、密码、Carrier mode | 拒绝热改，需要 restart |
| listener、scheduler/recovery policy、UDP | 拒绝热改，需要 restart；UDP route-count 变更不支持 |
| `max_active_legs` | Session 创建时固定，需 restart |
| 空路线、超过上限、重复/非密集 ID、无效 TLS | 在连接变更前拒绝 |

## 运行后检查

对比 request ID、operation result、`before_sessions` / `after_sessions`、Session ID、Leg ID/generation、`partial`/compensations。reload 成功表示 reload 操作已按实现完成，并不承诺旧应用会话跨重启、endpoint 替换或生产路由切换存活。强制删除只在确认业务允许受影响后手动请求。
