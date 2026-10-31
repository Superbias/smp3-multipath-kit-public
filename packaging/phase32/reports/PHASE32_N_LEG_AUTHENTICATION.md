# PHASE32_N_LEG_AUTHENTICATION

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

Join/Rejoin 绑定 Session credential、Session ID、LegID、Generation，不只依靠源 IP。TLS TCP loopback N=3，ALPN smp3/1，使用测试证书。公网 PKI、证书轮换、Realm 生产身份、连接耗尽攻击和 WAN 安全均 NOT RUN。部署请使用受信证书并保护 session password。
