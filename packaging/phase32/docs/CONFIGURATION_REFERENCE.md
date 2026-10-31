# Phase32 Configuration Reference

The examples in `configs/` are generated from the actual JSON structs in `client/config.go` and `server/config.go`. The target-schema illustration from the engineering prompt is not the runtime schema.

## Client

- `listen`: local TCP/SOCKS ingress address.
- `upstream_socks.address`: fallback SOCKS endpoint; `upstream_socks.legs` can override the SOCKS endpoint per configured leg.
- `smp3.password`: shared Client/Landing session secret.
- `smp3.carrier_mode`: `legacy_socks` or `host_bridge`.
- `smp3.carrier_tls`: when enabled in `legacy_socks`, `server_name` is required and peer verification remains enabled; `root_ca_file` is optional.
- `smp3.routes.legs`: ordered list of `{id,name,address,fallback}`. IDs are `uint8` on wire and runtime config requires list index to agree with the ID. Use dense IDs starting at zero.
- `smp3.stream.max_active_legs`: configured resource ceiling, 1–256; examples use exact configured count. Practical limits also depend on OS descriptors, queues, and memory.
- `scheduler_mode=aggregation`, `capacity_mode=dynamic`, `activation_mode=dynamic`: common N-way B0 path.
- `experimental_path_aware=false`: Path-aware is not required for N-way B0. Missing feedback retains the baseline scheduler.
- `smp3.udp.enabled`: must be false when more than two stream legs are configured.

For compatibility, the old `routes.leg0` and `routes.leg1` fields remain accepted as a two-element route list. Do not mix legacy fields and `routes.legs` in one config.

## Landing

- `listen`: one shared TCP listener for all Legs; public listeners require `carrier_tls.cert_file` and `key_file`.
- `password`: validates each HELLO HMAC; use the same secret as Client.
- `application_target`: fixed target; the server rejects a HELLO destination that differs from it.
- `stream.max_active_legs`: maximum Legs accepted per Core engine; this is a safety cap, not a promise of that many physical routes.
- `stream.bandwidth_mbps`: scheduler capacity priors, not measurements.
- `telemetry.listen`: keep on loopback/management network; the example uses `127.0.0.1:24500`.

## Strategy

Standalone configs do not expose a B1 bounded-rescue selector. B0 is the shipped behavior; `experimental_path_aware` is a separate opt-in and is false in all Phase32 examples. B1 must not be inferred from an experimental path-aware setting.
