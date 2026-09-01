# SMP3 2.1.0 Carrier-Agnostic Release Closure Report

## STATUS

```text
SMP3 2.1.0: RELEASED
CARRIER-AGNOSTIC RELEASE: PASS
RUNTIME/WIRE SEMANTICS CHANGED: NO
PRODUCTION TOUCHED: NO
```

## VERSION/TAG/COMMIT

- Release: `2.1.0`
- Tag: [`v2.1.0`](https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.1.0)
- Release commit: `d70d4c7df9f1cdeda57bf9a6caaf75fa17835ed2`
- Post-release main integration commit: `2747c65` (merged the remote historical 2.0.1 report update)
- Release commit message: `release: SMP3 2.1.0 carrier-agnostic adapters`
- Force push: not used

## FILES CHANGED

The release commit contains the sing adapter generic-role refactor/tests,
carrier-neutral README and deployment docs, changelog/release notes, version
metadata, build/package tooling, and six formal `dist/` binaries plus checksums
and source archive. The post-release closure report is committed separately on
`main`. Existing historical reports and temporary directories were not staged.

The only build-tool correction was in `scripts/apply_mihomo_adapter.py`: the
Core module replacement is now calculated relative to the actual Mihomo and kit
paths, so the default nested `.work/phase6e-build/mihomo` checkout is
reproducible. No runtime behavior changed.

## CARRIER-AGNOSTIC FINAL AUDIT

```text
CORE_CARRIER_AGNOSTIC: YES
SERVER_CARRIER_AGNOSTIC: YES
MIHOMO_CARRIER_AGNOSTIC: YES
SING_CARRIER_AGNOSTIC: YES
IP_FAMILY_CARRIER_AGNOSTIC: YES
LEG_PROTOCOL_SWAP_CONFIG_ONLY: YES
```

The sing runtime policy contains zero non-test `hy2`, `hysteria`, or `snell`
matches and no `proxy.Type()` protocol branch. Adaptive roles are generic
`carrierPrimary`/`carrierFallback`; logs use configured outbound tags.

## BACKWARD COMPATIBILITY

Existing `legs`, `leg1_fallback`, and `leg1_adaptive` configuration fields are
unchanged. The existing Hy2/Snell-named configuration fixtures passed without
schema changes. The adapter uses the same generic reliable TCP dial boundary
for Snell, Hysteria2, VLESS, Trojan, TUIC, Shadowsocks, VMess, Direct, or any
other child outbound that provides the required capability.

## TESTS/RACE

```text
sing tagged protocol/multipath -count=10: PASS
focused primary/fallback state tests -count=300: PASS
sing protocol/multipath -race -count=3: PASS
sing tagged vet: PASS
validate-kit.sh: PASS
test count: 162 (39 smp3core + 106 legacy_semantic + 17 sing_adapter)
```

The previous baseline was 160; two generic-role regression tests were added.
The full archive was freshly extracted and passed `validate-kit.sh`.

## CORE/WIRE INTEGRITY

```text
Core migration parity: PASS, 17/17 unchanged
Wire golden SHA256: PASS, 22/22 unchanged
Core import purity: PASS
gofmt: PASS
git diff --check: PASS
```

`core/`, `server/`, `adapters/mihomo/`, and `src/option/` had no release diff.

## FORMAL ASSETS

All six target-format checks passed (Linux ELF64/amd64, Windows PE32+/amd64).
Local SHA256 values match the digest displayed by the public GitHub Release.

| Asset | Size (bytes) | SHA256 |
|---|---:|---|
| `smp3-server-linux-amd64` | 4773554 | `3711912e8a9d753be90110a9db54b2a3022c6804a8d5e27caafa1452f3a8a879` |
| `smp3-server-windows-amd64.exe` | 4887040 | `4d98e979548e7bd00a3c9fc5075c845bc7d58da680f5cc3bdcfe2e64393b2553` |
| `mihomo-smp3-linux-amd64` | 51346057 | `fac57548e6730e76d860567214c1a9fa670688f16734b4cae562ae4fd1ad82cf` |
| `mihomo-smp3-windows-amd64.exe` | 49360896 | `6b3d4ccb058440d14f869bc5bc1d285e5f310a8f9beb8c5dfb3fb3ac767c6e6e` |
| `smp3-proxy-linux-amd64` | 79835284 | `2b8f6fd8caf2de8802a7e9bac7d8f3426a836d7edeea61004cbfa93e0cee2874` |
| `smp3-proxy-windows-amd64.exe` | 80500736 | `2d1610955cf719f775b3e2ef063cfaf94e1fe113b51f2c72d19629a754223af2` |
| `SHA256SUMS` | 556 | `4d5c2f8c418eaf1486c69ae543782b66ff760df09f962ab8c50384ddc15faa5f` |
| `smp3-multipath-kit-2.1.0-source.zip` | 508242 | `4f24374aa3b85eb520753c362836051384241fd7a4d27bc123776808c9f8ecf3` |

`SHA256SUMS` verification passed for all six binaries. The source archive
passed checksum verification, `unzip -t`, forbidden-content exclusion checks,
and fresh-extract `validate-kit.sh`.

## SOURCE ARCHIVE

```text
smp3-multipath-kit-2.1.0-source.zip
SHA256=4f24374aa3b85eb520753c362836051384241fd7a4d27bc123776808c9f8ecf3
```

The archive excludes `.git`, `.work`, `.release-stage`, graphify output,
credentials, private configs, temporary outputs, and release binaries.

## INSTALLER COMPATIBILITY

The existing installers select the latest stable GitHub Release by default and
accept an explicit version using the `v<version>` tag endpoint. Their exact
asset selection and `SHA256SUMS` filename matching remain generic; no installer
logic change was needed for 2.1.0. The Linux lifecycle and PowerShell installer
tests passed, and the public Release exposes the exact `v2.1.0` server/Mihomo
assets expected by those installers.

## DOCUMENTATION

README and bilingual deployment docs now state that SMP3 does not require a
specific proxy protocol. They describe primary/fallback carrier roles, the
required reliable TCP dial capability, example protocol choices, delegated
IPv4/IPv6 selection, and the distinction between standalone server and client
carrier termination. Historical 2.0.0/2.0.1 behavior and example names remain
identified as historical/examples.

## GITHUB RELEASE

- Public URL: https://github.com/Superbias/smp3-multipath-kit-public/releases/tag/v2.1.0
- Title: `SMP3 2.1.0`
- Label: `Latest`; not marked pre-release
- Tag commit shown by GitHub: `d70d4c7`
- Uploaded assets: exactly 8 custom assets listed in the table above
- GitHub UI also shows its two automatic source-code archives; those are not
  counted as custom release assets.
- Page-visible SHA256 digests and sizes matched the local artifacts for all 8
  custom assets.

## POST-RELEASE VERIFICATION

The public page was checked after publication. It shows the release title,
Latest label, existing `v2.1.0` tag, release commit `d70d4c7`, and all 8 custom
assets with the expected names, sizes, and SHA256 digests.

## PRODUCTION SAFETY

No production server update, Mihomo replacement, SSH operation, service restart,
live fault injection, or migration was performed in this release closure.

Known non-blocking note: GitHub warns that the two `smp3-proxy` binaries exceed
its recommended 50 MB file size; upload completed successfully. The embedded
runtime identities remain on the validated 2.0.0 baseline because `server/` and
Mihomo runtime source were intentionally unchanged.
