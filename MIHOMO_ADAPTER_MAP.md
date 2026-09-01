# Mihomo SMP3 Adapter Map

Pinned target: MetaCubeX/mihomo v1.19.28, commit cbd11db1e13a75d8e680e0fe7742c95be4cba2be.

## Canonical boundary

The adapter imports github.com/Superbias/smp3-multipath-kit-public/smp3core. Core remains stdlib-only and does not know Mihomo, proxy names, Snell, Hysteria2, or fallback policy.

## TCP path

- Config parser: type smp3 is injected into adapter/parser.go.
- Child validation: config.parseProxies calls outbound.ValidateSMP3Config. Missing, duplicate, self-referential, and invalid fallback names fail closed.
- Child lookup: configured names resolve from tunnel.Proxies() to C.Proxy.
- Aggregate dial: each child receives C.Metadata for the SMP3 aggregate server:port, never the application destination.
- HELLO: crypto/rand SessionID and nonce plus smp3core.EncodeHelloParts v4/ModeStream; Destination is C.Metadata.RemoteAddress().
- Bootstrap: leg0 creates one StreamEngine per application connection. Initial leg0 failure returns an error.
- Activation: StreamConfig.OnActivate coalesces one leg1 dial.
- Repair: OnLegDown schedules same-SessionID, same-LegID redial and AttachLeg rejoin.
- Fallback: leg1 tries its preferred child and then the configured fallback after dial/HELLO failure.
- Application bridge: outbound.NewConn wraps StreamEngine.AppConn(). Closing it enters Core graceful close.
- Context: bootstrap uses caller DialContext; repair uses a logical-session context closed with Engine.Done or adapter Close.

## UDP boundary

ListenPacketContext returns C.ErrNotSupport. Mihomo SMP3 UDP and full carrier-health policy remain deferred.
