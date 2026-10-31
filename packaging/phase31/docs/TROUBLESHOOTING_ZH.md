# SMP3 自用部署故障排查

## 常用检查

```sh
systemctl status smp3-landing smp3-front smp3-client
journalctl -u smp3-landing -u smp3-front -u smp3-client --since '15 minutes ago'
curl -fsS http://127.0.0.1:24500/api/v1/status
curl -fsS http://127.0.0.1:24500/api/v1/legs
curl -fsS http://127.0.0.1:24500/api/v1/sessions
```

只收集必要时间段的日志，并先删除共享密钥、认证配置、私钥路径中可能带有用户名的部分。Telemetry 包含目标地址和 Session 元数据，分享前做脱敏。

## 只有 Direct Leg 能连

检查 Client 的 `routes.leg1` 是否指向 Front 的 TCP 地址；再核对 `upstream_socks.leg1` 的出口是否可到 Front，以及 Front `allowed_sources` 是否包含它实际看到的出口地址。Front 日志中的 connect timeout 表示 Front 到 Landing 的 TCP 路径不可达。

## Front 能连但 Landing 收不到

检查 `front.json.landing_target` 是否是 Front 能访问的 Landing 地址与端口；检查 Landing 的 TCP 8443 listener、TLS 证书文件权限、Landing 源地址规则。Front 是 TLS 透明转发，不会把 TLS 转换成 SMP3 明文。

## 两 Leg 在线但只有一个有 DATA

在 Landing `/api/v1/legs` 和 `/api/v1/sessions` 确认两条 Leg 属于同一 Session 且 generation 正常。比较每 Leg wire bytes 与 unique payload bytes；确认 Client/Server 两边均显式启用了 Path-aware 示例配置、TLS ServerName 仍是 Landing 身份，Client `bandwidth_mbps` 没有把慢腿估得过高或把健康腿估得过低。短小流可能只被单腿完成，不等于第二条连接本身失败。

## 吞吐低于 Fast-only

先检查实际路线、RTT、带宽先验、Front/TLS CPU 和应用目标吞吐。保留 `RetryBytes`、`RescueBytes`、唯一 ACK、重复数据与恢复状态，不要增加队列/inflight 或放松恢复阈值来掩盖问题。Fast-only 对照应在同一设备、同一时间窗口执行。

## TLS 握手失败

核对证书覆盖 Client 配置的 `server_name`、完整证书链、Client CA 信任、系统时间和 ALPN `smp3/1`。Front 不应配置证书；只有 Landing 终止 TLS。不要设置跳过证书验证，也不要为绕过错误打开 V4 transport fallback。

## Session / Leg 身份冲突

Landing 会拒绝错误 HMAC、nonce 重放、同 Session 的 mode/destination 不一致、重复活动 Leg 或 retired Session。确认两条 Leg 的 password 和应用目标完全一致，Client 运行的是同一个 Core session；如需重试，关闭旧客户端连接并重新建立应用 CONNECT，不要固定或复用 Session ID。

## 重连失败

检查 `redial_interval`、Front/Landing Listener、DNS、TLS 信任及共享密钥。Server 的 session/connection 上限会拒绝超额新连接；检查 `/api/v1/status` 和服务日志。Idle timeout 到期也会关闭空闲 Carrier；按业务空闲长度调整两端部署值。

## 安全导出日志

用 `journalctl -u smp3-landing --since ... > landing.log` 等方式导出短时间段；不要导出 `/etc/smp3/*.json`、TLS 私钥、完整环境变量或原始配置。脱敏 Session ID、目标地址、固定 IP 和用户信息。共享密钥不应出现在程序日志中，但仍需复核操作者手工附加内容。

## 回滚

停止对应服务，使用 `scripts/rollback.sh COMPONENT BACKUP_DIR` 恢复升级前二进制和备份配置；启动后检查版本、TLS、固定目标和 Telemetry。详细步骤见 [UPGRADE_AND_ROLLBACK.md](UPGRADE_AND_ROLLBACK.md)。
