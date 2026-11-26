# Recommended deployment: proxy paths converge on a Landing SMP3 server

[简体中文](PROXY_CONVERGENCE.zh-CN.md) | [Native overview](README.md) | [Project README](../../README.md)

This sanitized example is based on an actual v2.7.0 Native Mihomo / Clash Party deployment. All addresses, names and credentials below are documentation examples or placeholders.

## 1. Topology and responsibilities

```text
Application -> local Native Mihomo SMP3
               |-- Leg0 -> front proxy A -> encrypted Landing proxy --+
               `-- Leg1 -> front proxy B -> encrypted Landing proxy --+
                                                                      | final dial on Landing
                                                                      v
                                                           127.0.0.1:24444 SMP3
                                                                      -> requested destination
```

**Front machines need neither SMP3 nor Realm nor smp3-front.** Native does not run a separate local smp3-client. Existing proxy chains, WireGuard, IP routing or other reliable paths can carry the legs to the Landing. One leg may instead connect directly to the encrypted Landing proxy. Shared bottlenecks can limit aggregation gains.

Both child outbounds in this example terminate at the encrypted proxy on the Landing. That proxy makes the final dial to `127.0.0.1:24444`; loopback therefore refers to the Landing. If a child terminates on a front machine, loopback refers to that front machine instead. Use a Landing address reachable from every exit for that different topology, subject to listener security requirements.

The client binds carriers to the same authenticated session and distinct leg IDs. The shared Core handles aggregation, ACKs, recovery and ordered delivery; unrelated connections do not aggregate automatically.

## 2. Landing access boundary

Reuse an authenticated, encrypted Landing proxy, such as a sing-box Shadowsocks 2022 TCP inbound. This example uses `192.0.2.20:28443`, a documentation-only address. Replace it and configure strong credentials. Its routing must permit authorized clients to dial Landing loopback and must not send that target to another exit. Review any private-address blocking rules.

Restrict access to intended front exits with a firewall as an additional control. An IP allowlist does not replace encryption or authentication. Users of this proxy can reach local services through it and must be trusted accordingly.

Keep SMP3 on loopback. Do not expose an unrestricted loopback listener through a public Front or DNAT. Loopback is a local access address, not encryption; protection depends on the complete proxy, credential, routing and firewall boundary.

## 3. Landing SMP3 configuration

Native and Standalone use the same `smp3-server`. Download the matching [v2.7.0 Release](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.7.0) binary and verify SHA256SUMS. Save the following as `/etc/smp3/server.json`, replace the password, and restrict file permissions:

```json
{
  "listen": "127.0.0.1:24444",
  "password": "REPLACE_WITH_LONG_RANDOM_SMP3_PASSWORD",
  "stream": {
    "scheduler_mode": "aggregation",
    "capacity_mode": "dynamic",
    "experimental_path_aware": true,
    "bounded_rescue_mode": "bounded",
    "bounded_rescue_budget_basis_points": 300,
    "bounded_rescue_burst_bytes": 0
  },
  "udp": { "enabled": false },
  "telemetry": { "enabled": true, "listen": "127.0.0.1:24500" }
}
```

`application_target` is deliberately omitted: the current unrestricted loopback listener accepts Native HELLO destinations and connects to the requested application target. This example relies on protected proxy access. A non-loopback listener instead requires a fixed application_target and carrier TLS certificate/key; the HELLO destination must exactly match the fixed target. Do not apply the unrestricted example to that public-listener mode.

```sh
chmod 600 /etc/smp3/server.json
/opt/smp3/releases/v2.7.0/smp3-server -c /etc/smp3/server.json -check
/opt/smp3/releases/v2.7.0/smp3-server -c /etc/smp3/server.json
```

After validation, run the absolute command through a service manager such as systemd, enable startup at boot and inspect `ss -lntp`: ports 24444 and 24500 must bind only to 127.0.0.1. Keep telemetry private; use SSH local forwarding for remote viewing.

## 4. Native Mihomo / Clash Party

Use the release Native Mihomo containing the SMP3 adapter. Keep the complete existing `front-A` and `front-B` node definitions, rules and proxy groups. Add these entries to proxies:

```yaml
proxies:
  # Keep complete existing front-A/front-B proxy definitions.
  - name: landing-via-A
    type: ss
    server: 192.0.2.20
    port: 28443
    cipher: 2022-blake3-aes-128-gcm
    password: REPLACE_WITH_LANDING_SS2022_KEY
    dialer-proxy: front-A
    udp: false

  - name: landing-via-B
    type: ss
    server: 192.0.2.20
    port: 28443
    cipher: 2022-blake3-aes-128-gcm
    password: REPLACE_WITH_LANDING_SS2022_KEY
    dialer-proxy: front-B
    udp: false

  - name: SMP3-Landing
    type: smp3
    server: 127.0.0.1
    port: 24444
    password: REPLACE_WITH_LONG_RANDOM_SMP3_PASSWORD
    legs:
      - proxy: landing-via-A
      - proxy: landing-via-B
    scheduler-mode: aggregation
    capacity-mode: dynamic
    experimental-path-aware: true
    bounded-rescue-mode: bounded
    bounded-rescue-budget-basis-points: 300
    bounded-rescue-burst-bytes: 0
    startup-policy: preferred
    startup-preferred-leg: 0
    startup-grace: 500ms
    udp:
      enabled: false
```

The SS2022 key must have the algorithm's required encoding and length; the placeholder is not runnable. The SMP3 password is a separate credential. Both Landing child nodes use the same Landing proxy credentials but distinct dialer-proxy routes.

Add and select `SMP3-Landing` in your actual selector group. Avoid reference cycles back to SMP3 or its selector group. Here dialer-proxy constructs the outer carrier chain, not the inner application SOCKS handshake used in a fixed-target SOCKS deployment.

In Clash Party, import the original rules plus these additions as an independent local profile, then activate it. Keep the original remote subscription for rollback. Runtime-only edits disappear on reload; a local snapshot does not automatically merge later subscription changes.

This example explicitly enables aggregation/dynamic, Path-aware and Bounded Rescue rather than relying on defaults that differ between builds. Path-aware needs peer support and trusted fresh feedback; missing feedback falls back to baseline scheduling. The 300 basis-point budget parameter is not a guarantee that all retransmissions in every fault and time window stay below 3%. UDP is disabled in this example.

## 5. Verification and rollback

1. Verify binary hashes, server config with -check and the Mihomo configuration.
2. Test each Landing child route independently before selecting SMP3.
3. Check the public exit through your local mixed/HTTP proxy. The exit should be the Landing; this alone does not prove aggregation.
4. Transfer a known SHA-256 payload. Verify useful unique ACK/byte increments on both legs of the same SMP3 session; two Established sockets alone are insufficient.
5. Confirm the local profile is persistent, temporary test rules are absent, the server starts at boot and telemetry remains private.

The source deployment passed one 16 MiB single-stream integrity check with useful data on both legs of the same session. The test source was deliberately paced. This is a deployment smoke test, not WAN capacity, fault recovery, endurance or complete release qualification.

For rollback, select the retained original Clash Party profile and node; stop/disable the new SMP3 service if needed. Do not blindly replace the outer proxy or the entire firewall. Switching configurations or stopping a service may terminate sessions. Keep original configurations and firewall backups; never commit production secrets, real infrastructure addresses or private profiles.
