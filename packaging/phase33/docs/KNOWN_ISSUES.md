# Known Issues and Open Gates

- Self-use build is not a general v2.7 release. Keep `P0-C` and General Release at FAIL / NOT READY until the Phase32 gate report says otherwise.
- Native Mihomo N-leg runtime/build parity is not established by Standalone tests. Adapter source/config support must not be described as Native production verification.
- The local N=3/4/8/16 Landing evidence uses loopback TCP and fixed payloads; it does not prove WAN/physical path independence or production capacity.
- Linux network-namespace/TCP blackhole tests are separate gates; if not run, they remain NOT RUN/BLOCKED.
- `experimental_path_aware` defaults false in examples. B1 bounded rescue is not configurable through the Standalone JSON schema.
- Runtime route-count reload supports dense tail append/removal and selected non-identity tuning fields. It is stage-applied per live Session with compensation, not a globally atomic transaction. Existing endpoint/upstream/TLS identity, listener, scheduler/recovery/UDP policy and `max_active_legs` require restart.
- Editing config does not auto-reload. Linux systemd reload sends SIGHUP; Windows and generic CLI reload use a same-directory local request/result marker. Protect the config directory from untrusted writers.
- `startup_policy=first-ready` may start an application Session with only leg0 while companion activation/repair is pending. Use `preferred` when all configured Legs must attach before initial readiness; verify startup latency and route availability.
- Fixed N=1 versus N=8 process comparison used a 64 KiB/20 ms paced local Python harness. Both transfers were about 24.3 Mbps with nearly equal completion time; this is evidence of per-Leg contribution and correctness, not sustained scaling efficiency.
- Client N>2 requires `udp.enabled=false` because current Client UDP topology is limited to two legs.
- `LegID` is uint8 (the protocol can encode 256 values), while a `max_active_legs` setting, OS limits and per-leg goroutines/queues constrain practical scale.
- Go Windows timer behavior, strict-zero retransmission, native host parity and WAN QoE retain their separately reported status.
