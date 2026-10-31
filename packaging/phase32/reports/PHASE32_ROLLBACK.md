# PHASE32_ROLLBACK

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

包内脚本提供本地 install/upgrade/status/rollback。先审阅脚本、目标路径及备份目录；从未在生产运行。隔离试用从 1–2 条 TCP leg 开始。失败时停止服务、恢复备份配置与 Phase31 binary 后重启验证；Realm 无需更新。生产切换须维护者自行安排授权维护窗口。
