# Realm TCP 多路部署参考（中文）

## 验证状态

本次没有安装或发现 Realm 可执行文件，因此 Realm 真实版本、配置语法、单 Realm、多 Realm、断线重连均为 **NOT EXERCISED**。本文只说明部署拓扑与操作前提，不提供未经解析器验证的 Realm TOML。

## 拓扑

Client 的每条 Leg 建立独立 TCP Carrier。可以将直连路线和一个或多个 Realm TCP 转发路线配置为不同 `address`，最终都指向同一个 Landing TCP listener。Realm 只转发字节流，不解析 SMP3 数据、不做调度/ACK/重组/恢复，也不负责 TLS 终止。不要把 `smp3-front` 当作必装组件。

## 上线前检查

1. 记录实际 Realm 版本和官方配置文档版本。
2. 在 Realm 自带配置校验器中验证纯 TCP listener/forward 配置；确认没有 UDP 隧道、TLS 终止或协议改写。
3. 确认多条 Leg 到 Landing 的实际路由确实不同；不同 Realm 地址或 TCP Established 状态不能证明物理路径独立。
4. 保护 Landing TLS 私钥和 SMP3 密码；限制 Landing listener 来源地址。
5. 先进行单个新应用连接的小流量验证，观察每 Leg 唯一 DATA、ACK、EOF、重试、资源和错误；任何数据完整性问题立即停止扩展。

配置格式由所安装 Realm 版本决定，本项目包不猜测具体字段。生产部署与回退必须由授权运维人员执行；本轮没有做任何生产操作。
