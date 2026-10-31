# SMP3 v2.7.0-selfuse.3 N-Leg 自用包

本包包含 Standalone Client/Landing Server 的 Linux amd64、Linux arm64（交叉构建）和 Windows amd64 二进制、示例配置、systemd units、安装/升级/回退辅助脚本，以及配置参考。版本用途是受控自用验证；这不是 v2.7 GA 发布，也没有包含 Native Mihomo 核心或 `smp3-front`。

## 校验并选择二进制

在 Linux 上从解压目录运行 `bash scripts/verify-checksums.sh`，再根据 `uname -m` 选择 `bin/linux-amd64/` 或 `bin/linux-arm64/`。Windows amd64 二进制位于 `bin/windows-amd64/`，SHA-256 见 `manifest/SHA256SUMS`。Linux arm64 本轮只交叉构建，未在 arm64 主机运行。

## 最小使用流程

1. 复制一个 `configs/client-*.example.json` 和 `configs/landing.example.json` 到受保护目录，生成新的随机密码并保持两端一致。
2. 配置实际 SOCKS 出口、Landing 地址和已存在的 TLS 证书；私钥不在此包内。
3. 确认 `max_active_legs` 大于等于路线数；N>2 时必须关闭 Client UDP。
4. 执行 `smp3-client -c client.json -check` 或 `smp3-server -c landing.json -check`。
5. 先在实验环境用单条应用 TCP 流核对 Session、每 Leg 唯一 DATA、ACK、EOF、账本及资源，然后再自行决定是否安装。

## 运行时重载

更改 JSON 文件不会自动重载。Linux systemd Client 支持 `systemctl reload smp3-client`（SIGHUP）；也可从本机执行 `smp3-client -c /etc/smp3/client.json --reload`。Windows 用 `smp3-client.exe -c client.json --reload`，由配置旁的本地 marker/result 文件请求运行中的实例处理。保护配置目录，避免不可信本地用户写入 marker。

普通重载只支持密集 Leg 列表尾部增加/移除和少量非身份调优项；现有 endpoint/upstream/TLS、监听、UDP、调度恢复策略及 `max_active_legs` 变更要重启。新增 Leg 会拨号并加入原 Session。普通移除 drain；`--force-reload` 会中断被移除 Carrier 上的未确认 DATA 并依赖 Core recovery，可能影响当前流。操作按每个 Session 分阶段执行并尝试补偿，不是跨 Session 原子事务。检查 reload result、Client 日志与 telemetry；出现 `partial` 或补偿失败应停止继续变更。

`startup_policy=first-ready` 优先启动第一条可用 Leg，companion 可能由后续激活/repair 加入；`preferred` 在初始 Session 就绪前并发准备所有配置 Leg，可能增加启动等待并要求全部上游可达。

## 配置和回退安全

先保存当前配置和二进制 SHA-256。`scripts/upgrade.sh`、`scripts/install-*.sh`、`scripts/rollback.sh` 是人工运维辅助脚本；本次没有在生产主机上执行这些脚本。回退时不要将 N-Leg 配置交给不识别该格式的旧版本。服务重启或切换版本可能终止现存应用会话；不承诺会话保留。

## 验收范围

Go 1.25.5 Windows/WSL 模块测试、WSL Race/Vet、Standalone 本地双进程 reload 和固定 N=1/8 本机 relay 数据完整性有本地证据。N=1 与 N=8 固定负载约 24.3 Mbps、完成时间几乎相同，测试夹具的 64 KiB/20 ms 输入节流主导结果，不能声称扩展效率提升。Realm 精确版本兼容、真实 WAN、Linux 内核黑洞、Windows Race、512 MiB/60 s、长时间资源趋势和 Native 热重载仍未验证；参见随包报告和门禁文件。
