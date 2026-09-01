# SMP3 2.0.1 Installer Release Closure Report

## STATUS

`SMP3 2.0.1: RELEASED`  
`INSTALLER RELEASE: PASS`  
`WINDOWS MIHOMO ONE-CLICK: PUBLISHED`  
`LINUX STANDALONE ONE-CLICK: PUBLISHED`  
`RUNTIME/WIRE CHANGED: NO`

This is an installer/operations patch release. It does not change SMP3
protocol, Core, Wire, HELLO, scheduler, Stream, Datagram, repair, or recovery
semantics.

## VERSION / TAG / COMMIT

- Version: `2.0.1`
- Tag: `v2.0.1` (annotated)
- Commit message: `release: SMP3 2.0.1 installer and operations tooling`
- Release commit: `b5cefdf077263d7ebd2538350d4842fa53a192ec`

## FROZEN RUNTIME CHECK

- `core/` source diff: none
- standalone `server/` runtime source diff: none
- Mihomo/sing adapter semantics: unchanged
- Wire/HELLO/scheduler/Stream/Datagram/repair/recovery: unchanged
- Core checksum: `17/17` PASS
- Wire checksum: `22/22` PASS
- Existing SMP3 test count: `160` PASS
- No production process, service, config, firewall rule, or remote host was touched

## INSTALLER TESTS

- PowerShell installer tests: `POWERSHELL_INSTALLER_TESTS=PASS`
- Linux installer lifecycle: `LINUX_INSTALLER_LIFECYCLE=PASS`
- `bash -n`: PASS
- standalone example `-check`: PASS
- Mihomo example `-t`: PASS
- `validate-kit.sh`: PASS
- shellcheck: `NOT AVAILABLE`

Covered behaviors include architecture gates, exact process/path detection,
multiple-core fail-safe, stable Release selection, exact SHA256SUMS matching,
atomic replacement, backup/restore, supervisor relaunch protection, config
required fail-closed, systemd unit generation, `smp3ctl` lifecycle/log/update/
rollback/version commands, failed update rollback, five-generation rotation,
and config-preserving uninstall.

## FORMAL ASSETS

All six assets passed the ELF64/amd64 or PE32+/amd64 target checker:

| Asset | SHA256 |
|---|---|
| `smp3-server-linux-amd64` | `1718655a5807e18db5d8cb17be0e705fa584a54bbf2e1f00d3646a274c4a9934` |
| `smp3-server-windows-amd64.exe` | `37310efa64eba79ae267ee9dbde7cc2db3442e0d5d8ac184360b2c9aebf2cce5` |
| `mihomo-smp3-linux-amd64` | `363038e17118446086c3fca66efd7ea717ee8fac450c5f726f0451edeca75842` |
| `mihomo-smp3-windows-amd64.exe` | `df364855c899648b6e2d4d785a702514f1fc87a64a662fab28489be5b234ffd3` |
| `smp3-proxy-linux-amd64` | `3fb50b3c63bf3ae31e7b9b47694037e8c7fe21d49869e950ae1e145dfd113ff0` |
| `smp3-proxy-windows-amd64.exe` | `b9f06039586ad1ccdff0074b25c987bcaae87553300ef5d092a1024b7d4910c3` |

`dist/SHA256SUMS` verification: PASS. The server/sing hashes are not claimed
byte-identical to the previous 2.0.0 candidate; the source/runtime behavior is
frozen and the new hashes are recorded as the canonical 2.0.1 asset set.

## SOURCE ARCHIVE

- Filename: `smp3-multipath-kit-2.0.1-source.zip`
- SHA256: `6280c01aacf91d5b6284568765f438d6d8ac92540c36711c72b04879e67bce7c`
- Size: `505799` bytes
- Contains installer scripts/tests, examples, bilingual deployment docs,
  runtime source, build scripts, and validation tools.
- Excludes `.git`, `.work`, `.release-stage`, private configs, credentials,
  temporary test output, production evidence, relay/manager binaries, and
  release binaries.
- Fresh extraction: `unzip -t` and `validate-kit.sh` PASS.

## PUBLIC SCRIPT URL VALIDATION

Expected raw URLs:

```text
https://raw.githubusercontent.com/Superbias/smp3-multipath-kit-public/main/scripts/install-mihomo-smp3.ps1
https://raw.githubusercontent.com/Superbias/smp3-multipath-kit-public/main/scripts/install-smp3-server.sh
https://raw.githubusercontent.com/Superbias/smp3-multipath-kit-public/main/scripts/smp3ctl.sh
```

The published Windows script was downloaded and PowerShell-parser checked in a
disposable test context; no real Mihomo was replaced. The published Linux
script and `smp3ctl.sh` were downloaded and passed `bash -n`. The README URLs,
script filenames, Release URLs, and example config paths match the repository.

## GIT PUSH

- `main`: PASS (`37bc9dd..ff252a2` release-tree fast-forward, followed by report-only `ff252a2..50e0834`)
- `v2.0.1` tag: PASS
- force push: not used

Earlier remote documentation synchronization used authenticated GitHub Contents
API commits because Git transport was temporarily unavailable. The final 2.0.1
release tree, including dist binaries, was then pushed through normal Git from
`37bc9dd` to `ff252a2`; no force push was used.

## GITHUB RELEASE

- Release: `SMP3 2.0.1`
- URL: https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.0.1
- Assets: six formal binaries, `SHA256SUMS`, and
  `smp3-multipath-kit-2.0.1-source.zip`
- No relay tool, manager binary, private config, credential, or `.work` evidence
  uploaded.

## POST-RELEASE VERIFICATION

Release API metadata reports exactly eight assets with exact filenames, sizes,
and SHA256 digests matching the local release outputs, including the digest of
the `SHA256SUMS` file itself. The six binary formats and architectures match the
target checker; `SHA256SUMS` matches all six binaries; source archive integrity
and fresh extraction validation pass.

## KNOWN LIMITATIONS

- Windows cannot reliably restart an arbitrary owning GUI/service; the user may
  need to start it manually after a safe replacement.
- Linux systemd behavior was validated through an isolated lifecycle harness;
  users should verify status after first real-host installation.
- shellcheck was unavailable during validation.
- Runtime binary SHA values may differ between builds because of toolchain/build
  metadata; no semantic runtime change is implied.

## PRODUCTION SAFETY

Production was left untouched. The current production standalone server remains
the separately accepted deployment; upgrading it to 2.0.1 is a future,
independently authorized operation.
