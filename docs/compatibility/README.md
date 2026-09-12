# Compatibility / Legacy integration

`smp3-proxy` is the optional Compatibility / Legacy product for existing
sing-box-shaped configurations. It is not Standalone and it is not Native.

```text
Compatibility -> smp3-proxy -> sing-box compatibility runtime
```

Its build workflow is isolated from Standalone:

```bash
./scripts/build-compatibility.sh
```

This workflow may fetch the pinned sing-box source and apply the existing
compatibility overlay. The Standalone workflow never calls it and does not
require its source, binary, config, or runtime.
