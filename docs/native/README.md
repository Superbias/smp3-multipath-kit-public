# SMP3 Native

Native is **High-Performance Mihomo Integration**. The canonical artifact is
`mihomo-smp3`; it embeds the SMP3 Native adapter and uses Mihomo child
outbounds directly.

```text
Application -> Mihomo mixed/SOCKS5 ingress
            -> SMP3 Native adapter
               -> Leg0 -> Mihomo child outbound A
               -> Leg1 -> Mihomo child outbound B
                  -> SMP3 server :24444
```

Native does not require a separate `smp3-client` process. Mihomo owns the node
protocols, node credentials, TLS/Reality/HY2/Snell handling, and child-outbound
selection. R18 does not change Native routing, child-outbound semantics, wire
behavior, or server primary listener semantics.

Native uses adaptive scheduling by default. To opt into the v2.5 aggregation
scheduler, set `scheduler-mode: aggregation` on the `smp3` proxy. The other
supported values remain `adaptive` and `static`.

```yaml
proxies:
  - name: smp3-main
    type: smp3
    server: 10.66.66.1
    port: 24444
    password: change-me
    scheduler-mode: aggregation
    legs:
      - proxy: line-path
      - proxy: public-hy2
```

Build and test Native separately:

```bash
./scripts/build-native.sh
```

The build uses the pinned Mihomo revision and the repository's existing Native
adapter injection workflow. Native is intentionally vendor-specific; that is
the distinction from Standalone.
