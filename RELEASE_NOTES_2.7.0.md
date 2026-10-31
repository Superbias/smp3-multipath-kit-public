# SMP3 v2.7.0 Release Notes

## Included

- Shared N-Leg Core across Standalone and Native Mihomo integration.
- V7 coordinated drain and runtime reload support.
- Standalone client and server builds for Linux amd64/arm64 and Windows amd64.
- Standalone Front builds for Linux amd64 and Windows amd64.
- Native Mihomo builds from pinned upstream v1.19.28 for Linux amd64 and Windows amd64.
- Compatibility proxy builds from pinned sing-box v1.14.0-beta.14 for Linux amd64 and Windows amd64.
- ARM64 Android Standalone debug APK and the matching Android/arm64 client runtime binary.
- `SHA256SUMS` and `BUILD_MANIFEST.json` covering all binary release assets and their source/build identities.

## Validation scope

The available Phase42 evidence validates diagnostic in-process barriers and local build/test subsets. It does not establish cross-host atomic snapshots, independent daemon interoperability, real WAN qualification, or a matched .3/.4 A/B result. The Phase42 overall P0-C verdict was OPEN / FAIL. This release publication does not change that test evidence. The Go module test/vet suite and pinned Mihomo/sing-box builds were run for this release; see `BUILD_MANIFEST.json` for the recorded scope.

## Known limitations

- The historical L4 1,384,448-byte peak remains unexplained; the diagnostic harness reached 2 MiB under local conditions.
- Pilot08's original Carrier close initiator and wire FIN/RST cause remain unknown; local syscall ordering does not establish the wire initiator.
- Formal .3/.4 A/B, real WAN, strict isolated TCP blackhole qualification, COMMIT_UNCERTAIN and forced-recovery fault matrices were not completed in the available evidence.
- The Android APK is debug-signed and intended for sideload/testing; no maintainers' release signing key was available.
- The Android APK was built with Android Gradle Plugin 8.5.2 against compileSdk 35; Gradle emitted its compatibility warning. No physical Android-device test was run.

Use the supplied SHA256SUMS to verify each binary. Keep a backup of existing configuration before upgrading.
