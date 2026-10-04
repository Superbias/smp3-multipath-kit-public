# SMP3 v2.6.1

Patch release containing the validated global carrier capacity scheduler, bounded HOL-aware assignment, dynamic capacity estimation, and the rebuilt Android ARM64 standalone client.

Assets include standalone SMP3 client/server binaries for Linux and Windows, the pinned Mihomo SMP3 builds, and `smp3-android-standalone-2.6.1-debug.apk`. Checksums are in `SHA256SUMS`.

The Android client emits the production aggregation settings (`scheduler_mode: aggregation`, `capacity_mode: dynamic`, preferred-leg startup with bounded activation) and embeds the current SMP3 client runtime.
