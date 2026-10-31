# Known Issues and Open Gates

- Self-use build is not a general v2.7 release. Keep `P0-C` and General Release at FAIL / NOT READY until the Phase32 gate report says otherwise.
- Native Mihomo N-leg runtime/build parity is not established by Standalone tests. Adapter source/config support must not be described as Native production verification.
- The local N=3/4/8/16 Landing evidence uses loopback TCP and fixed payloads; it does not prove WAN/physical path independence or production capacity.
- Linux network-namespace/TCP blackhole tests are separate gates; if not run, they remain NOT RUN/BLOCKED.
- `experimental_path_aware` defaults false in examples. B1 bounded rescue is not configurable through the Standalone JSON schema.
- Runtime hot-add is not provided. Startup config determines the logical leg list; reconnect/rejoin of an already configured ID is distinct from adding a new ID.
- Client N>2 requires `udp.enabled=false` because current Client UDP topology is limited to two legs.
- `LegID` is uint8 (the protocol can encode 256 values), while a `max_active_legs` setting, OS limits and per-leg goroutines/queues constrain practical scale.
- Go Windows timer behavior, strict-zero retransmission, native host parity and WAN QoE retain their separately reported status.
