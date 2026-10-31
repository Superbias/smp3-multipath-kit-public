# 升级与回滚

## 升级前

1. 校验当前 ZIP：`scripts/verify-checksums.sh`。
2. 记录当前 `smp3-server -version`、`smp3-front -version`、`smp3-client -version`，并备份 `/etc/smp3` 与 `/var/lib/smp3`。
3. 确认新配置的 fixed target、Front source CIDR、TLS 链、server name、密码来源和版本策略。
4. 评估已有连接：升级会重启进程并中断当前应用会话。不能保证跨任意版本或路由变化保留现有会话。

## 执行升级

解压新 Bundle 后按组件执行，例如：

```sh
sudo scripts/upgrade.sh landing
sudo scripts/upgrade.sh front
sudo scripts/upgrade.sh client
```

升级器为现有二进制、配置和 unit 创建权限为 0700 的 `/var/backups/smp3/upgrade-UTC时间` 目录，然后运行安装器。配置存在时会备份并保留，不会被模板覆盖。安装器校验新二进制 SHA-256。更新配置需要人工编辑。

按拓扑先 Landing、再 Front、最后 Client；每一层单独检查日志和端口，再继续下一层。

## 回滚

```sh
sudo scripts/rollback.sh landing /var/backups/smp3/upgrade-YYYYMMDDTHHMMSSZ
sudo scripts/rollback.sh front /var/backups/smp3/upgrade-YYYYMMDDTHHMMSSZ
sudo scripts/rollback.sh client /var/backups/smp3/upgrade-YYYYMMDDTHHMMSSZ
```

该目录必须是对应组件升级器输出的目录。若升级前服务处于 active 状态，回滚器会恢复旧二进制和备份配置后重新启动；否则仅恢复文件。再运行 `systemctl status`、`journalctl`、CLI `-version`、TLS 检查和一次应用 CONNECT。

## 卸载

默认保留配置、证书和数据：

```sh
sudo scripts/uninstall.sh landing front client
```

`sudo scripts/uninstall.sh --purge-config landing` 会明确删除所选组件配置，但不会删除 TLS 文件或 `/var/lib/smp3`。删除证书、数据、共享密钥或 systemd 用户应由维护者分别检查并执行。
