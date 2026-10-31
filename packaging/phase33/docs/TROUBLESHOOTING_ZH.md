# SMP3 N-Leg 故障排查

先保存 Client/Landing 版本、配置哈希（删除密码后）、`systemctl status`、近期 journal 与 loopback telemetry。不要分享密码、TLS 私钥、Session secret 或完整含认证材料的配置。

## 只有 Direct Leg 能连接

逐一用 `nc -vz` 检查 Realm listener 到 Landing `TCP/8443`；核对 Realm 目标是同一 Landing listener，Landing 防火墙放行的是 Realm 出口地址；查看 Landing `journalctl -u smp3-landing` 的 TLS、HELLO、nonce、Leg ID 或 capacity 拒绝原因。Realm 必须透明转发，不可终止 TLS。

## Realm 可连接但 Landing 未登记 Leg

检查端到端证书链、Client `server_name`、Client/Landing 密码是否一致，`routes.legs[i].address` 是否是 Realm listener，`upstream_socks.legs[i]` 是否指向能连接该 listener 的 SOCKS 出口。多个 Leg 必须使用唯一 Leg ID 且同一个 Session ID；不要用来源 IP/端口区分 Session。

## 多个 Leg 在线但某条无唯一 DATA

查询 Landing `GET /api/v1/legs`、`/api/v1/sessions` 和单 Session telemetry。分别看各 Leg `unique_rx_bytes`/ACK 与 Retry/Rescue，不要把握手、ACK、Probe 或重复 DATA 当作唯一贡献。短流、慢腿限制、调度先验或队列拥塞可能让某条健康腿在短窗口内没有唯一贡献；用足够长、相同配置的业务负载再判断。

## N 路比 Fast-only 慢

从同一负载、同一 endpoint、同一时间窗比较 Fast-only 与聚合 goodput；检查真实每腿服务率、RTT、重传、Rescue、HOL、ACK frontier、队列、CPU/RSS。`bandwidth_mbps` 是配置输入，不是实测结果。确认不同 Realm IP 是否真的走了不同链路。不要先增大 queue/inflight 或关闭慢腿保护。

## TLS 握手失败

校验系统时钟、证书有效期/SAN、完整证书链、Client 的 `server_name`，以及 Landing 私钥文件权限。禁止通过跳过证书验证解决错误。

## Session/Leg 身份冲突

检查是否将旧 Session/Leg 连接复制到新进程、同一 Session 下重复配置 ID、密码不一致、旧 Generation 延迟连接，或 180 秒内重复 HELLO nonce。重新启动整个 Client 会创建新 Session；单 Leg 重连应使用原逻辑 ID 并走 Generation 更新。

## 重连失败

查看客户端重连日志、Landing 当前连接与 session limit、Realm/SOCKS 健康、TLS 和密码。检查某 Leg 是否超过 `max_active_legs` 或 Landing connection limit。服务端会拒绝同一 Session 里同时占用中的重复 Leg ID。

## 导出脱敏日志与回滚

只导出版本、配置结构（密码替换成 `REDACTED`）、日志和 telemetry JSON；先用文本检索检查 token、password、private key、Session secret。停止服务并按 `UPGRADE_AND_ROLLBACK.md` 使用 install/upgrade 生成的备份目录恢复 Phase31 二进制和旧式双 Leg 配置。
