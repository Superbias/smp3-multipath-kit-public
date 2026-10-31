# Standalone Client N-Leg 热重载指南

## 触发

Linux：编辑配置后执行 `sudo systemctl reload smp3-client`（unit 发送 SIGHUP），或 `/usr/local/bin/smp3-client -c /etc/smp3/client.json --reload`。Windows：`smp3-client.exe -c client.json --reload`。更改文件不会自动触发重载；没有公网管理 API。

`--force-reload` 用于明确授权的强制删除路径，可能影响当前应用连接。配置目录旁的 request/result marker 是本地管理入口，必须限制写权限。

## 支持变化

支持密集尾部 route append/remove，以及显示名/每 Leg 带宽先验。现有 Leg 的 endpoint、fallback、upstream、TLS/认证身份，监听、调度/恢复策略、UDP 与 `max_active_legs` 需重启。一个应用 TCP 连接继续对应原 Core Session。

## 事务语义

流程先校验候选和资源，再逐 Session 加入新 Leg，发布候选 runtime，之后 drain 或强制撤销被删 Leg；失败会尝试补偿。跨 Session 不是强原子事务。结果里的 `partial` 和 `compensations` 必须检查；出现部分失败时停止继续更改并按回滚流程处理。

## 启动策略

`first-ready` 可以先以首条路线服务并稍后接入 companion。若要求开始传输前所有路线都已建立，使用 `preferred`，但它等待全部候选线路，可能增加启动时延。

Phase33 进程回归证明本机同一 Session 的 reload 与 forced rejoin，不保证真实 WAN 上所有应用会话无中断。进程升级/重启可能断开活动会话。
