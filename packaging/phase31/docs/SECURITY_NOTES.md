# 安全说明

- Landing 对外监听要求 TLS certificate/key 与精确 `application_target`。不要删除这两个边界或绑定任意目标。
- 认证使用共享密码 HMAC 和 nonce/session/leg 校验。建议至少 32 随机字节、每次泄漏立即轮换；本协议不是按用户区分的账号系统，也不是 TLS client certificate/mTLS。
- Client TLS 始终验证 Landing 的证书链、`server_name` 和 `smp3/1` ALPN。Front 不终止 TLS。不要设置 InsecureSkipVerify 或其他跳过验证的路径。
- Front 固定一个 Landing target；非回环监听必须配置 `allowed_sources` CIDR。Landing 与 Front 的主机防火墙仍建议仅允许已知对端源地址/网段和所需 TCP 端口，脚本不会替你更改网络策略。
- Landing 示例固定一个应用目标，遥测绑定 loopback。不要公开 `/api/v1/*` 或将其反向代理到公网。
- 安装配置使用 root owner、`smp3` group、0640；证书私钥使用 root:smp3、0640 或更严。配置文件包含共享密钥，不应进入备份之外的普通发布包或日志。
- Session/Leg 身份通过共享密钥认证的 HELLO 绑定。Host 侧 Carrier ID/generation 是本地跟踪信息，不应被解释成远端认证凭据。
- systemd 以无登录 `smp3` 用户运行，限制写目录和系统能力。不得以 root 手动启动常驻服务（安装器需要 root 执行文件安装）。
- 未部署的 Native Mihomo 适配和真实 WAN 仍需单独安全审计。此自用包不构成公开服务安全审批。
