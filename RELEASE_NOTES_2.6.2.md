# SMP3 v2.6.2

This release packages the shared Core recovery correction from the C1 candidate. Native Mihomo and Standalone client/server build against the same canonical Core source. The recovery probe residence clock starts when the candidate record is exposed at the cumulative-ACK frontier. SMP3 wire/HELLO versions and configuration schema are unchanged.

## Assets

- Native Mihomo SMP3 core: Linux/amd64 and Windows/amd64.
- Standalone SMP3 client: Linux/amd64 and Windows/amd64.
- Standalone SMP3 server: Linux/amd64 and Windows/amd64.
- Pinned sing-box compatibility client: Linux/amd64 and Windows/amd64, built from commit `4902660f8424fef3c2a60dfcdce7aeadfe3f3b88`.
- Android Standalone APK: arm64-v8a, debug-signed; its Go client PIE was rebuilt from this release source.
- `SHA256SUMS` covering the released artifacts.

## Validation status

This artifact build does not change the engineering gate result. The current audit remains overall FAIL: migration parity requires maintainer policy approval, the Windows raw Carrier calibration is below its frozen floor, and production validation is NOT RUN. The full matrix and remaining boundaries are recorded in `SMP3_SHARED_CORE_RECOVERY_FIX.md` and `SMP3_RELEASE_RESULTS.md`.

The artifacts do not imply deployment approval.
