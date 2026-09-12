# SMP3 v2.4.0 product build workflows

The product lines have explicit build ownership. `build.sh` is the canonical
Standalone build and does not fetch, build, or import any proxy core.

```bash
# Standalone only: smp3-client + smp3-server
./build.sh

# Native only: pinned Mihomo + SMP3 Native adapter
./scripts/build-native.sh

# Compatibility only: pinned sing-box + smp3-proxy overlay
./scripts/build-compatibility.sh

# Optional legacy all-product convenience command
./scripts/build-phase6-artifacts.sh
```

The standalone workflow produces:

```text
smp3-server-linux-amd64
smp3-server-windows-amd64.exe
smp3-client-linux-amd64
smp3-client-windows-amd64.exe
STANDALONE_SHA256SUMS
```

The Native workflow produces `mihomo-smp3-*` and `NATIVE_SHA256SUMS`. The
optional Compatibility workflow produces `smp3-proxy-*` and
`COMPATIBILITY_SHA256SUMS`. The integrated Dashboard is part of `smp3-server`.

The workflows preserve the SMP3 wire, Core, scheduler, ACK, retransmission,
Native adapter, and Carrier semantics. External proxy protocols remain outside
the Standalone product.
