# Build required

This alpha2.3-r10 artifact is a clean source release; no prebuilt binary is bundled.

Run `./build.sh` in WSL/Linux with network access for:

- the pinned sing-box revision;
- the pinned Go 1.25.5 toolchain when not already cached;
- the upstream Go module dependency graph.

Expected binary version:

`1.14.0-beta.14-smp3-alpha2.3-r10`

For deterministic support and testing, rebuild/replace both the landing-server and client binaries from the same r10 source release.
