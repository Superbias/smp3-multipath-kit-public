# N-Leg 热重载与线路排查

## 重载未发生

保存 JSON 不会自动重载。Linux 使用 `systemctl reload smp3-client` 或 SIGHUP；Windows 使用 `--reload`。检查配置旁 `.reload.request`/`.reload.result.json`、operation ID、日志权限与 Client 存活状态。

## 重载被拒绝

检查 route IDs 是否从0连续递增、路线数是否超过 `max_active_legs`、TLS 参数/证书名是否有效、N>2 时 UDP 是否关闭。已有 Leg endpoint/upstream/TLS、安全身份、listener、调度/恢复、UDP 或 max 上限变更要求重启。

## partial 或补偿失败

不要重复提交更多 reload。保存完整 result、Client/Landing 日志和 `/api/v1/legs`、`/api/v1/sessions` 快照；确认哪些 Sessions/Legs 已生效，按人工回退步骤恢复。跨 Session 不是原子事务。

## 只有 leg0 有数据

`first-ready` 可能先只连接 leg0。若验收需要所有 Leg 在传输前加入，使用 `preferred` 并确认所有上游可达；否则检查 activation、repair、配置 route 数与 Landing connection limit。固定 N=8 Phase33 测试使用 preferred，并在开始发送前读取遥测确认8条活动 generation。

## 吞吐或恢复异常

先比对源码/二进制/配置 SHA、真实 RTT、每 Leg 唯一 DATA 与重传。Phase33 本地节流结果不是 WAN 性能结论。Realm、真实黑洞和WAN门禁未验证；不得把多个 TCP 连接误当作多条独立物理路径。
