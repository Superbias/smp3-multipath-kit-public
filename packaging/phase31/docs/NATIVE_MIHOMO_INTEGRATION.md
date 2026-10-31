# Native Mihomo 接入边界

仓库包含 Mihomo adapter 源码和 `scripts/build-native.sh` 注入构建工作流；构建固定上游 Mihomo `v1.19.28` / commit `cbd11db1e13a75d8e680e0fe7742c95be4cba2be`。Phase 31 没有运行该外部构建，不随自用 Bundle 提供 Mihomo 二进制。

在 Linux 主机上安装 Go、Git 与 Python 后，可从完整源码树按仓库脚本构建：

```sh
./scripts/build-native.sh
```

检查脚本 checkout/revision 及其输出 manifest，再由操作者根据 Native 所用 Mihomo 配置单独配置 SMP3 proxy、child outbound 和两个腿。Native 由 Mihomo 管理 Carrier/Child proxy；这与 Standalone Client 的本机 SOCKS5 入口不同。不能把 `adapters/mihomo` Go 源文件单独复制后声称得到可运行 Mihomo。

此工作流不证明 Native 进程、该宿主的 TLS/活动 Carrier RTT 或真实 WAN 行为已在 Phase 31 验收。
