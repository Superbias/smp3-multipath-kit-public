# SMP3 Phase 6E Release Closure Report

## STATUS

`PHASE 6E: RELEASE CLOSURE CANDIDATE`

Final publication status is filled after the tag push and GitHub Release
asset round-trip. Phase 6E performed no production restart, migration, or
configuration edit.

## VERSION

- Release version: `2.0.0`
- Git tag: `v2.0.0` (created after the closure commit)
- sing client display version: `1.14.0-beta.14-smp3-2.0.0`
- standalone server display version: `2.0.0`
- pinned sing-box: `v1.14.0-beta.14`, commit `4902660f8424fef3c2a60dfcdce7aeadfe3f3b88`
- pinned Mihomo: `v1.19.28`, commit `cbd11db1e13a75d8e680e0fe7742c95be4cba2be`
- Go toolchain target: `go1.25.5`

## SOURCE COMMIT

- Release source commit: `78f1383`
- Closure report commit: recorded by the final `v2.0.0` tag
- Core source was not changed in Phase 6E. The stale primary Core manifest was
  synchronized to the accepted Phase 6C-R2 manifest and now verifies `17/17`.

## FINAL REGRESSION

- `validate-kit.sh`: PASS
- Test count: `smp3core=39 + legacy_semantic=105 + sing_adapter=16 = 160`
- Core normal/race/vet: PASS
- Wire golden: `22/22` PASS and unchanged
- Relevant gofmt and `git diff --check`: PASS
- Source injector and shell syntax: PASS
- Sensitive information scan: `SENSITIVE_FINDINGS=0`
- Public IPv4 scan on source archive: `PUBLIC_IPV4_FINDINGS=0`
- Forbidden release content scan: PASS
- No Phase 6E live fault injection or production restart was run

The source archive was extracted to a fresh temporary directory and its
structure checks plus full `validate-kit.sh` passed.

## CORE / WIRE

- Canonical Core: `17/17` SHA256 entries PASS, using the corrected primary
  `CORE_CANONICAL_SHA256SUMS` manifest.
- Wire golden fixtures: `22/22` SHA256 entries PASS.
- Core imports remain standard-library-only.

## FORMAL ARTIFACTS

All six artifacts passed the explicit target checker:

| Asset | Target | SHA256 |
|---|---|---|
| `smp3-server-linux-amd64` | ELF64 amd64 / linux | `146a44556392b527c357cd661f824602ff1d0d615ee27330f4c915f395f83bbf` |
| `smp3-server-windows-amd64.exe` | PE32+ amd64 / windows | `a4df177b70c676589cb30699d06d7e0ba5954fb9dbac28ad9096eca1d6410a10` |
| `mihomo-smp3-linux-amd64` | ELF64 amd64 / linux | `363038e17118446086c3fca66efd7ea717ee8fac450c5f726f0451edeca75842` |
| `mihomo-smp3-windows-amd64.exe` | PE32+ amd64 / windows | `df364855c899648b6e2d4d785a702514f1fc87a64a662fab28489be5b234ffd3` |
| `smp3-proxy-linux-amd64` | ELF64 amd64 / linux | `ef8e3becc9903b8dc1876fbb650b68bce1354b1615b5694cf20a04dbd8ebf439` |
| `smp3-proxy-windows-amd64.exe` | PE32+ amd64 / windows | `a4d73a81bd4c8df9a73c9303dc5e111a671d2d6d7a559df55dccf9ec5f0aa797` |

`dist/SHA256SUMS` verifies all six assets. The relay and Mihomo core-manager
are validation/tools artifacts and are not end-user release assets.

## SOURCE ARCHIVE

- Filename: `smp3-multipath-kit-2.0.0-source.zip`
- SHA256: `81eac25de102fc28aa11de80aebb994c812cfbb5f3c89c5f509510573c5cffb3`
- Contents: source, canonical Core, standalone server, adapters, tests,
  configs with placeholders, build scripts, target checker and docs.
- Excludes: `.git`, `.work`, `.release-stage`, graphify output, binaries,
  private/temporary configs, credentials, relay source and live evidence.
- Fresh extraction and `validate-kit.sh`: PASS.

## PRODUCTION / RELEASE BINARY IDENTITY

- Production remains standalone active/enabled and owns `:24444`.
- Production standalone binary SHA remains the locked Phase 6D artifact:
  `2ce96d533eb9e777d3a87ded7a304b9d387a74de88390482192737851cbbc841`.
- The new 2.0.0 release standalone Linux SHA is
  `146a44556392b527c357cd661f824602ff1d0d615ee27330f4c915f395f83bbf`.
- The hashes differ because 2.0.0 deliberately changes the embedded release
  version from the deployed r11 identity; the difference is disclosed and not
  treated as byte identity. No production binary was replaced.
- The release source retains the validated Phase 6D standalone behavior; any
  future production upgrade must be a separately authorized deployment.

## DOCUMENTATION

README, Chinese README variants, CHANGELOG, RELEASE_NOTES, BUILD_STATUS and
TEST_RESULTS now describe the 2.0.0 architecture and six formal assets.
Documentation records that Snell/Hysteria2 terminate externally, the standalone
server is the production landing endpoint, and the canonical Core is reusable.

The H3 100 MiB result remains `INCONCLUSIVE / EXTERNAL HARNESS-INTEROP ISSUE`:
the same failure reproduces on the direct official aioquic path without bridge,
SOCKS5 or SMP3. It is not marked PASS and is not an SMP3 release blocker.

## RELEASE NOTES

The release notes include:

- standalone SMP3 server and canonical Core;
- Mihomo and sing-box integrations;
- TCP/UDP adaptive, stripe and duplicate modes;
- same-session leg repair and persistent UDP association recreation;
- production migration and rollback validation;
- carrier-specific stream HOL and UDP unreliability limitations;
- H3 external harness limitation and Hysteria2 detection-latency note.

## GIT COMMIT / TAG / PUSH

- Source release commit: `78f1383` (`release: SMP3 2.0.0 standalone core`)
- Final closure commit: created after this report
- Annotated tag: `v2.0.0`
- `main` push: pending at report creation
- tag push: pending at report creation

## GITHUB RELEASE

- Release: `v2.0.0`
- Assets: six formal binaries, `SHA256SUMS`, and the clean source archive
- Relay, manager, credentials, private configs and `.work` evidence: not uploaded
- Publication and post-download verification: filled after upload

## POST-RELEASE VERIFICATION

The final verification records downloaded Release assets, checks them against
`SHA256SUMS`, rechecks binary target formats, and confirms the source archive
SHA256. This section is completed before the final status is reported.

## PRODUCTION SAFETY

Phase 6E did not restart or edit production. The accepted production state is:

```text
standalone active/enabled
:24444 owner = standalone
old smp3-proxy inactive/disabled/preserved
```

## FINAL GATE

The final status may be promoted to:

```text
PHASE 6E: COMPLETE
SMP3 RELEASE READY: YES
RELEASE PUBLISHED: YES
PRODUCTION STANDALONE: HEALTHY
```

only after the tag push, GitHub Release creation, and downloaded asset
checksum/target verification succeed.
