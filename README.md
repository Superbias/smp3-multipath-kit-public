# SMP3 Multipath Kit v2.7.0

[简体中文](README-zh_CN.md) | English

SMP3 is an application-layer multipath transport. One application stream can use multiple independently established carrier connections, called **legs**. The shared SMP3 Core handles framing, scheduling, ordered reassembly, acknowledgements, retransmission, recovery, and shutdown.

This repository ships four ways to use SMP3:

| Product | Use it when |
| --- | --- |
| **Standalone** | External proxy cores expose SOCKS5 endpoints and you want SMP3 to combine them behind one local SOCKS5 listener. |
| **Native Mihomo** | You use Mihomo/Clash and want an SMP3 proxy that selects existing Mihomo child outbounds. |
| **Android Standalone** | You want the Standalone client managed by the Android app as a separate process. |
| **Compatibility proxy** | You need the optional sing-box-based smp3-proxy build. |

Standalone and Native stream traffic use the same canonical Core in core/. Host adapters establish carriers and pass them to that Core; they do not implement separate schedulers or recovery algorithms.

> **Release qualification:** v2.7.0 is published, but publication does not mean every qualification gate passed. Available release evidence does not establish general real-WAN qualification, every cross-host failure case, or a formal matched performance A/B. Review the [v2.7.0 release notes](https://github.com/Superbias/smp3-multipath-kit-public/blob/v2.7.0/RELEASE_NOTES_2.7.0.md) before choosing a rollout scope.

## Contents

- [What is included](#what-is-included)
- [Download and verify](#download-and-verify)
- [Standalone quick start](#standalone-quick-start)
- [N-Leg stream configuration](#n-leg-stream-configuration)
- [Protocol negotiation and compatibility](#protocol-negotiation-and-compatibility)
- [Native Mihomo](#native-mihomo)
- [Runtime leg reload](#runtime-leg-reload)
- [Server Dashboard](#server-dashboard)
- [Android Standalone](#android-standalone)
- [Front relay and compatibility build](#front-relay-and-compatibility-build)
- [Build from source](#build-from-source)
- [Security and operational notes](#security-and-operational-notes)
- [Known limits](#known-limits)

## What is included

### Shared multipath Core

- **N-Leg TCP streams.** A stream can use up to 256 protocol leg IDs. The default per-stream active-leg limit is 16; set max_active_legs to fit the host and deployment. Leg IDs in a route list start at 0 and are contiguous.
- **Scheduling modes.** adaptive is the default; static preserves static scheduling; aggregation enables normalized service/capacity scheduling. Aggregation can use fixed priors or dynamic capacity estimation. Optional activation and HOL completion modes are available with aggregation.
- **Startup policy.** first-ready starts when a carrier is ready. preferred gives one selected leg a bounded grace period before startup is released to other ready legs.
- **Slow-leg protection and bounded repair.** The Core tracks ordered delivery and ACK progress, bounds ordinary in-flight work, restricts paths whose service falls behind, and can use bounded frontier repair/Rescue to limit head-of-line stalls. Recovery remains owned by the Core.
- **Reliable stream lifecycle.** Shared framing, ordered reassembly, cumulative ACKs, retransmission, graceful EOF/CLOSE, forced close, cancellation, and carrier-generation replacement.
- **Negotiated V7 coordinated drain.** When both peers authenticate and negotiate the capability, planned leg removal can coordinate quiescing and outstanding-work settlement before retirement. It does not guarantee sessions survive a process restart, carrier failure, or forced removal.
- **Optional same-carrier RTT feedback.** Experimental Path-aware scheduling can use authenticated, generation-bound feedback from the active SMP3 carrier. It is off by default and must be enabled at both ends. It does not use Mihomo URL-test history or an unrelated connection.
- **UDP datagrams.** UDP uses the datagram transport and supports at most two configured legs in this release. N-Leg support applies to TCP streams.

### Host products and operations

- **Standalone client/server.** JSON configuration, config validation mode, optional carrier TLS for Standalone TCP carriers, SOCKS5 app ingress, per-leg upstream SOCKS5 endpoints, and leg-level route/fallback configuration. Advanced host_bridge mode is available when an embedding host owns carrier creation and transport security.
- **Native Mihomo adapter.** Uses Mihomo child outbounds as carriers and the same Core as Standalone. Mihomo owns node protocols, credentials, and proxy-specific TLS/Reality/Hysteria2/Snell behavior.
- **Runtime leg reload for Standalone client.** Append or remove tail legs within the supported change set without restarting. A result record is written; unsupported changes require restart.
- **Integrated server Dashboard.** Optional read-only status, leg, session, traffic, event, and accounting views. Telemetry is disabled by default and binds only to literal loopback.
- **Standalone Front.** Optional TCP relay for deployments that need a forwarding hop. It is not a proxy protocol implementation and does not add TLS.
- **Compatibility build.** Optional smp3-proxy based on the pinned sing-box source.

## Download and verify

Download assets from the [v2.7.0 GitHub Release](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.7.0). Verify each downloaded binary or APK against the supplied SHA256SUMS. BUILD_MANIFEST.json records source and build identities.

| Asset | Platform / purpose |
| --- | --- |
| smp3-client-linux-amd64, smp3-client-linux-arm64 | Standalone client |
| smp3-client-windows-amd64.exe | Standalone client |
| smp3-server-linux-amd64, smp3-server-linux-arm64 | Standalone server |
| smp3-server-windows-amd64.exe | Standalone server |
| mihomo-smp3-linux-amd64, mihomo-smp3-windows-amd64.exe | Mihomo with Native SMP3 adapter; upstream Mihomo v1.19.28 |
| smp3-proxy-linux-amd64, smp3-proxy-windows-amd64.exe | Compatibility build; sing-box v1.14.0-beta.14 |
| smp3-front-linux-amd64, smp3-front-windows-amd64.exe | Optional TCP Front relay |
| smp3-client-android-arm64 | Android ARM64 Standalone runtime |
| smp3-android-standalone-2.7.0-debug.apk | Android app; debug-signed for sideload/testing |

On Linux, run this in the download directory:

~~~sh
sha256sum -c SHA256SUMS
~~~

On Windows, compare the output of Get-FileHash .\mihomo-smp3-windows-amd64.exe -Algorithm SHA256 (or the selected asset) with its line in SHA256SUMS. The Android APK is debug-signed, not signed with a maintainer release key.

## Standalone quick start

Standalone exposes one local SOCKS5 listener to applications. Each configured leg connects through its own upstream SOCKS5 endpoint to an SMP3 server route.

~~~text
Application -> 127.0.0.1:18080 (smp3-client SOCKS5)
             -> leg 0 -> upstream SOCKS5 A -> SMP3 server
             -> leg 1 -> upstream SOCKS5 B -> SMP3 server
~~~

External proxy software supplies the upstream SOCKS5 endpoints and owns protocols such as VLESS, Reality, Hysteria2, Snell, Shadowsocks, or Trojan. SMP3 does not parse those node protocols in Standalone mode.

### 1. Prepare the server

Start from [the server JSON example](examples/smp3-server-config.example.json), set a long random password, and choose listener and stream settings. For a local smoke test, bind the SMP3 listener to 127.0.0.1. Validate configuration before starting:

~~~sh
./smp3-server-linux-amd64 -c ./server.json -check
./smp3-server-linux-amd64 -c ./server.json
~~~

Windows uses the same flags:

~~~powershell
.\smp3-server-windows-amd64.exe -c .\server.json -check
.\smp3-server-windows-amd64.exe -c .\server.json
~~~

A non-loopback SMP3 listener requires both a carrier TLS certificate/key and a fixed application_target. The fixed target is an allowlist: requests for a different destination are rejected. Do not expose an unrestricted server listener to the public Internet. Configure the firewall only for intended SMP3 ports.

Example public-listener fields (replace all paths and values; this is not a complete server config):

~~~json
{
  "listen": "0.0.0.0:24444",
  "sidecar_listeners": ["0.0.0.0:24445"],
  "password": "REPLACE_WITH_A_LONG_RANDOM_SECRET",
  "application_target": "service.example.net:443",
  "carrier_tls": {
    "cert_file": "/etc/letsencrypt/live/service.example.net/fullchain.pem",
    "key_file": "/etc/letsencrypt/live/service.example.net/privkey.pem"
  }
}
~~~

A fixed target is suitable for a deployment that authorizes one destination. For broader proxy use, design the server, Front, firewall, and access boundary deliberately rather than removing validation to create a public catch-all listener.

### 2. Prepare the client

Copy [the client JSON example](examples/smp3-client-config.example.json). Set the SMP3 password, server routes, and actual SOCKS5 listeners provided by your external proxy core. This compact example shows two legs; use the N-Leg form below for more than two TCP paths.

~~~json
{
  "listen": "127.0.0.1:18080",
  "upstream_socks": {
    "address": "127.0.0.1:7898",
    "legs": [
      { "address": "127.0.0.1:17898", "connect_timeout": "10s" },
      { "address": "127.0.0.1:17899", "connect_timeout": "10s" }
    ]
  },
  "smp3": {
    "password": "REPLACE_WITH_SERVER_PASSWORD",
    "carrier_tls": {
      "enabled": true,
      "server_name": "service.example.net"
    },
    "routes": {
      "legs": [
        { "id": 0, "name": "path-a", "address": "service.example.net:24444" },
        { "id": 1, "name": "path-b", "address": "service.example.net:24445" }
      ]
    },
    "stream": {
      "scheduler_mode": "aggregation",
      "capacity_mode": "dynamic",
      "max_active_legs": 2,
      "bandwidth_mbps": [100, 100],
      "chunk_size": 65536,
      "queue_frames": 256,
      "max_reorder_frames": 4096,
      "max_inflight_frames": 1024,
      "ack_interval": "20ms",
      "retransmit_timeout": "1500ms",
      "recovery_timeout": "15s",
      "redial_interval": "1s"
    },
    "udp": { "enabled": false }
  }
}
~~~

Carrier TLS settings must match the server certificate. If each carrier already has authenticated transport security, review the deployment before layering TLS a second time. bandwidth_mbps values are scheduling priors, not measured rates or guarantees.

### 3. Validate, start, and connect an application

~~~sh
./smp3-client-linux-amd64 -c ./client.json -check
./smp3-client-linux-amd64 -c ./client.json
~~~

The example client listens on 127.0.0.1:18080. Configure an application to use that SOCKS5 proxy. On Windows, substitute smp3-client-windows-amd64.exe.

The -check option validates configuration and constructs the client but does not start listening. It prints endpoint identities for troubleshooting; do not put credentials in shared logs. Use -version to print the binary version.

## N-Leg stream configuration

Standalone v2.7.0 supports ordered smp3.routes.legs and upstream_socks.legs arrays. IDs must start at 0 and be contiguous; configured leg count must fit stream.max_active_legs. The protocol supports 256 leg IDs; the default active limit is 16. Set a lower limit if the host or network cannot sustain many simultaneous carriers.

~~~json
{
  "upstream_socks": {
    "address": "127.0.0.1:7898",
    "legs": [
      { "address": "127.0.0.1:17898" },
      { "address": "127.0.0.1:17899" },
      { "address": "127.0.0.1:17900" }
    ]
  },
  "smp3": {
    "routes": {
      "legs": [
        { "id": 0, "name": "direct", "address": "landing.example.net:24444" },
        { "id": 1, "name": "relay-a", "address": "relay-a.example.net:24444" },
        { "id": 2, "name": "relay-b", "address": "relay-b.example.net:24444" }
      ]
    },
    "stream": {
      "scheduler_mode": "aggregation",
      "capacity_mode": "dynamic",
      "activation_mode": "dynamic",
      "max_active_legs": 3,
      "bandwidth_mbps": [100, 50, 20]
    },
    "udp": { "enabled": false }
  }
}
~~~

Configuration notes:

- Keep route and upstream SOCKS entries in the same leg order. A per-leg upstream entry overrides global upstream_socks.address. SMP3 does not silently substitute another leg's upstream when a carrier fails.
- bandwidth_mbps is an initial capacity prior, not a speed test or service guarantee.
- scheduler_mode accepts adaptive, static, or aggregation. capacity_mode: dynamic, activation_mode: dynamic, and hol_mode: completion require aggregation.
- startup_policy accepts first-ready or preferred; startup_preferred_leg and startup_grace tune the preferred-start option.
- UDP currently supports at most two configured legs. Set udp.enabled to false for TCP with more than two legs.
- leg1_fallback in the legacy two-leg schema, or a route fallback, selects an alternate SMP3 server route for that leg. It is not fallback to another leg's SOCKS endpoint.

See the [two-leg client example](examples/smp3-client-config.example.json) and [three-leg configuration example](https://github.com/Superbias/smp3-multipath-kit-public/blob/v2.7.0/packaging/phase33/configs/client-3leg.example.json).

## Protocol negotiation and compatibility

Stream carriers try the authenticated V7 handshake, which can negotiate coordinated drain and Path RTT capabilities. If a peer explicitly refuses an optional capability, the client can retry a compatible older handshake on a **new carrier connection**. It never appends a second HELLO to a connection that already received the first HELLO.

An ambiguous V7 transport failure does not prove that the peer lacks V7. Retrying an older handshake after that kind of failure is controlled by allow_v7_legacy_fallback and is disabled by default. Path RTT V4 fallback after an ambiguous V6 transport error also requires both allow_path_rtt_v4_fallback and allow_path_rtt_v4_fallback_on_transport_error; both default to false. Authentication failures are not a compatibility refusal. Keep fallback disabled unless the endpoint's compatibility requirements are understood.

New handshakes affect new carrier connections. They do not promise existing streams survive arbitrary binary replacement, restart, forced removal, or network failure.

### Common stream settings

| Setting | Meaning |
| --- | --- |
| scheduler_mode | adaptive, static, or aggregation. |
| capacity_mode | fixed or dynamic; dynamic requires aggregation. |
| activation_mode | legacy or dynamic; dynamic requires aggregation. |
| hol_mode | legacy, disabled, or completion; completion requires aggregation. |
| bandwidth_mbps | Per-leg initial capacity priors, in route order. |
| max_active_legs | Per-stream active-leg ceiling; defaults to 16. |
| activation_threshold_mbps / activation_window | Aggregation activation threshold and observation window. |
| chunk_size | DATA frame payload size; default 65536 bytes. |
| queue_frames / max_inflight_frames / max_reorder_frames | Bounded queue, send-window, and receive-reorder controls. Change them only with workload evidence. |
| ack_interval / retransmit_timeout / recovery_timeout / redial_interval | ACK cadence and timeout/reconnect controls. Keep peer settings and deployment policy consistent. |
| startup_policy / startup_preferred_leg / startup_grace | first-ready or preferred startup behavior. |

Values such as bandwidth priors are inputs to scheduling, not measured line guarantees. Use the example config first and tune only with measured, reproducible results.
## Native Mihomo

Use the mihomo-smp3 binary from the release instead of a stock Mihomo build that lacks the SMP3 adapter. Mihomo owns node protocols and child outbounds; the SMP3 proxy references those child names as legs.

~~~yaml
mixed-port: 7890
allow-lan: false
mode: rule

proxies:
  - name: smp3-main
    type: smp3
    server: service.example.net
    port: 24444
    password: REPLACE_WITH_SERVER_PASSWORD
    scheduler-mode: aggregation
    capacity-mode: dynamic
    activation-mode: dynamic
    max-active-legs: 3
    bandwidth-mbps: [100, 50, 20]
    experimental-path-aware: false
    legs:
      - proxy: direct-path
      - proxy: relay-path-a
      - proxy: relay-path-b

proxy-groups:
  - name: SMP3
    type: select
    proxies: [smp3-main]

rules:
  - MATCH,SMP3
~~~

Define direct-path, relay-path-a, and relay-path-b as normal Mihomo proxies elsewhere in the YAML. Names under legs must match existing, unique child outbounds. Configure apps/system proxy to use the Mihomo mixed/SOCKS port (7890 in this example).

Native uses the shared stream Core and does not start a separate smp3-client. UDP currently supports at most two configured legs. experimental-path-aware is opt-in, must also be enabled by the server, and should remain off unless deliberately testing it.

See [the Mihomo example](examples/mihomo-sidecar.example.yaml) and [Native adapter notes](docs/native/README.md).

## Runtime leg reload

Only the Standalone client currently supports runtime leg-list changes. It polls a local reload marker every 500 ms; editing the JSON file alone does not apply it.

1. Back up the config and edit the tail entries in routes.legs and matching upstream_socks.legs.
2. Ask the running client to reload:

~~~sh
./smp3-client-linux-amd64 -c ./client.json -reload
~~~

For a force-removal request:

~~~sh
./smp3-client-linux-amd64 -c ./client.json -force-reload
~~~

3. Review the log and client.json.reload.result.json. The result includes an operation ID, committed/partial status, added/removed leg IDs, and any compensation details.

Reload supports append and tail removal within documented constraints. It is not a general live edit for all settings or a distributed atomic transaction across sessions. Changes to existing endpoints/upstream identities, listener, password, TLS, scheduler/recovery settings, or max_active_legs require restart. Route-count changes while UDP is enabled require restart. Removing a leg used by active sessions can affect those streams; graceful drain depends on negotiated V7 support. Force removal can interrupt in-flight data. This mechanism applies to the Standalone client, not the server or Native Mihomo.

## Server Dashboard

The server can optionally expose an integrated read-only Dashboard and local telemetry API. It is disabled by default. When enabled, telemetry.listen must be a literal loopback address; the default is 127.0.0.1:24500. Do not bind it to a public interface. Use SSH port forwarding for remote viewing.

For example, enable it in the server JSON with:

~~~json
{
  "telemetry": {
    "enabled": true,
    "listen": "127.0.0.1:24500",
    "accounting_timezone": "UTC"
  }
}
~~~

The Dashboard is served at http://127.0.0.1:24500/. Its read-only API includes /api/v1/status, /api/v1/legs, /api/v1/sessions, /api/v1/sessions/{display_id}, /api/v1/traffic, /api/v1/traffic/history, and /api/v1/events (SSE). It does not modify configuration or control the server. Persistent traffic accounting is stored next to the server config by default; choose a protected writable path if needed.

## Android Standalone

The release APK smp3-android-standalone-2.7.0-debug.apk manages the ARM64 Standalone client as a separate process through an Android foreground service. It provides settings, start/stop/status, and bounded log viewing.

1. Sideload the APK on a supported ARM64 Android device.
2. Start an external proxy core that exposes two SOCKS5 carrier endpoints.
3. Configure the SMP3 server, password, local SOCKS listener, and the two carrier SOCKS endpoints in the app.
4. Start the service and point an app that supports a manual SOCKS proxy at the local listener (default 127.0.0.1:18080).

The APK is debug-signed for sideload/testing. It does not implement VPN/TUN mode, VLESS/Reality/Hysteria2/Snell, subscriptions, routing rules, or a proxy core. No physical Android-device qualification was completed for this release.

## Front relay and compatibility build

### smp3-front

The optional Front is a TCP relay between a public/edge listener and a landing endpoint. It is not an SMP3 server, does not understand proxy node protocols, and does not add TLS. Configuration is JSON; see the [Front example](https://github.com/Superbias/smp3-multipath-kit-public/blob/v2.7.0/packaging/phase31/configs/front.example.json). Validate it and start with smp3-front -c front.json -check, then smp3-front -c front.json. When listen is outside loopback, configure allowed_sources CIDRs. Keep connection/idle timeouts, buffer size, and maximum connections appropriate for the host.

### smp3-proxy / sing-box compatibility

Use the separate smp3-proxy build only when you need the compatibility integration. It is not the Standalone client and does not replace mihomo-smp3. The release pins sing-box v1.14.0-beta.14. Use a configuration intended for that integration; do not paste Mihomo YAML into it.

## Build from source

Release binaries were built with Go 1.25.5. From a source checkout:

~~~sh
# Standalone client and server only
./build.sh

# Native Mihomo
./scripts/build-native.sh

# sing-box Compatibility
./scripts/build-compatibility.sh
~~~

./build.sh builds only the Standalone client and server. Native and Compatibility are separate builds with pinned upstream source. See BUILD_MANIFEST.json and the release scripts for pinned revisions and output targets. The Android APK has its own Gradle build and signing requirements; the published APK is debug-signed.

## Security and operational notes

- Use a long random SMP3 password; keep it out of logs, source control, screenshots, and support bundles.
- Back up configurations and record old binary hashes before upgrading.
- Public SMP3 listeners require carrier TLS and a fixed application_target allowlist. Do not remove this boundary to create a public arbitrary-destination proxy.
- Keep server Dashboard/telemetry on loopback. Use SSH port forwarding for remote viewing.
- TLS/Reality/Hysteria2/Snell settings for an external proxy belong to that proxy or Mihomo child outbound. They are distinct from optional Standalone carrier TLS.
- UDP is separate from TCP N-Leg and limited to two legs.
- Runtime removal, forced close, route changes, restarts, and carrier failures can affect active sessions. Negotiated V7 drain helps with planned leg removal but is not a universal session-preservation guarantee.

## Known limits

See the release notes for the qualification scope. Documented gaps include:

- No general real-WAN qualification or matched .3/.4 performance A/B was established.
- Formal strict TCP blackhole and several cross-host/forced-recovery matrices were not completed in the available release evidence.
- The historical L4 1,384,448-byte peak and Pilot08 wire FIN/RST initiator remain unexplained.
- No physical Android-device test was run; the APK is debug-signed.
- Publication of v2.7.0 does not change the P0-C OPEN/FAIL assessment in the phase evidence.

Do not treat a local build, passing unit tests, two established carriers, or a smoke test as proof of real-WAN multipath performance. Use a staged, observable rollout and keep the previous binaries/configuration available for rollback.

## Reference material

- [v2.7.0 release notes](https://github.com/Superbias/smp3-multipath-kit-public/blob/v2.7.0/RELEASE_NOTES_2.7.0.md)
- [Client JSON example](examples/smp3-client-config.example.json)
- [Server JSON example](examples/smp3-server-config.example.json)
- [Mihomo example](examples/mihomo-sidecar.example.yaml)
- [Native adapter notes](docs/native/README.md)
- [Compatibility notes](docs/compatibility/README.md)
- [Security policy](SECURITY.md)
- [GitHub Release assets and checksums](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.7.0)
