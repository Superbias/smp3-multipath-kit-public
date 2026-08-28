# Build required

This alpha2.3-r11 artifact is a source candidate; no prebuilt r11 binary is bundled.

Run `./build.sh` in network-enabled WSL/Linux. It requires:

- pinned sing-box `v1.14.0-beta.14` commit `4902660f8424fef3c2a60dfcdce7aeadfe3f3b88`;
- Go `1.25.5` for the injected/full build;
- upstream Go module dependencies.

Expected binary version:

`1.14.0-beta.14-smp3-alpha2.3-r11`

Build both client and landing-server binaries from this same r11 candidate, then run the matrix in `R11_ACCEPTANCE.md`.
