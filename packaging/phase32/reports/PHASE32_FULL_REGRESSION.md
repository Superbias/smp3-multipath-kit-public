# PHASE32_FULL_REGRESSION

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

Windows Go1.25.5 Core/Client/Server/CMD/Vet、最终 Server telemetry 定向测试 PASS；Windows Race NOT RUN。WSL Go1.25.5 Core full/Race、Client/Server/CMD Race、TLS Landing N-leg Race、Vet 和配置校验 PASS。Linux amd64/arm64 Client/Landing、Windows amd64 Client build PASS；linux amd64 与 Windows client version smoke PASS，ARM64仅交叉编译/ELF检查。中间失败和错误 runner 记录保留在原目录，不计最终 PASS。
