# PHASE32_N_LEG_AGGREGATION_RESULTS

> Source tree SHA-256: 584ac3f131f3b2dd1446720660cb3756cb4577d088d85aedee87d86f8a5b1928 (449 files; see PHASE32_SOURCE_IDENTITY.json).
> Baseline: ace2f7db8b4fd78c842a4fa86c98e3fe983de4e1 / tree ee46b6d2e71c33397d9f5171728d971f37789902. Candidate is isolated and uncommitted.
> Build: Go 1.25.5, version 2.7.0-selfuse.2; binary SHA-256 values are in PHASE32_BUILD_IDENTITY.json.

| N | payload | unique legs | overlap | ms | loopback app goodput Mbps | integrity |
|---:|---:|---:|---:|---:|---:|---|
|1|32 MiB|1/1|100ms|70|3834.8|PASS|
|2|32 MiB|2/2|100ms|53|5064.8|PASS|
|3|32 MiB|3/3|100ms|53|5064.8|PASS|
|4|32 MiB|4/4|100ms|62|4329.6|PASS|
|8|32 MiB|8/8|100ms|240|1118.5|PASS|
|16|32 MiB|16/16|100ms|143|1877.2|PASS|

表中 goodput 是本机 loopback payload/runtime 的十进制 Mbps；单次结果不是 WAN 或物理扩展性指标。Landing 各 N 精确 per-leg bytes 见 JSON。
