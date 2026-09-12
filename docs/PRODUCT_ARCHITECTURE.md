# SMP3 Dual-Mode Product Architecture

SMP3 v2.3.4 has two primary product modes and one optional compatibility
product. The modes share SMP3 Core, wire, scheduler semantics, ACK handling,
retransmission/recovery, session semantics, and `smp3-server` listener roles.

```text
                         Shared SMP3
                  Core / Wire / Scheduler
                         /       \
                        /         \
     Standalone adapter             Native adapter
       generic SOCKS5                Mihomo integration
          /     \                    /          \
       Leg0     Leg1             child A      child B
          \     /                    \          /
          external carriers          SMP3 primary :24444
```

| Product | External proxy software | SMP3 client process | Per-leg interface | Protocol ownership |
| --- | --- | --- | --- | --- |
| Standalone | Required, vendor-neutral | `smp3-client` | one generic SOCKS5 endpoint per leg | external proxy |
| Native | Mihomo itself | no separate client | Mihomo child outbound | Mihomo Native adapter |
| Compatibility | sing-box runtime | `smp3-proxy` | compatibility-specific | compatibility layer |

## Standalone

Standalone is **Universal External Proxy Compatibility**. It consumes only:

```text
Leg0 -> configured SOCKS5 endpoint A
Leg1 -> configured SOCKS5 endpoint B
```

The external proxy owns node protocols, credentials, TLS, Reality, HY2, Snell,
node selection, and outbound changes. SMP3 does not inspect or implement those
protocols. Two listeners may belong to one external proxy process or to two
different processes.

## Native

Native is **High-Performance Mihomo Integration**. `mihomo-smp3` contains the
SMP3 adapter and selects child outbounds inside Mihomo. It does not require
`smp3-client` in the proxy path. This positioning describes lower architectural
overhead from direct child-outbound integration; no unmeasured percentage
performance claim is made.

## Compatibility

`smp3-proxy` is a Compatibility / Legacy integration product. It remains
supported separately, but is neither Standalone nor Native and is never a
dependency of the Standalone build.

## Listener semantics

```text
24444 = Native ingress
24445 = Standalone ingress
```

These roles remain distinct. R18 does not change wire behavior or listener
semantics.
