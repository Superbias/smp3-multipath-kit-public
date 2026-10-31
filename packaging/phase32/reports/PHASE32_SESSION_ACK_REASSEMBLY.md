# PHASE32_SESSION_ACK_REASSEMBLY

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

每次传输使用共享 session sequence、ACK frontier、TX ledger 和 RX reassembly。Core N=1/2/3/4/8/16 的 32 MiB payload SHA 为 c26136c68c111674ae69ae7669dd0cde12eeeee8b4dc130ba9b11072f61c1789，ACK=payload，双端 strict EOF，TX outstanding=0、RX pending=0。Landing 每 N 4 MiB，各腿唯一字节合计等于 payload。未输出 duplicate counter 的测试不宣称 duplicate=0。
