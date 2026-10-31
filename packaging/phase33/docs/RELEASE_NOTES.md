# SMP3 v2.7.0-selfuse.3 Phase33 Release Notes

**Controlled self-use candidate; not a general release.** This bundle adds explicit Standalone Client runtime N-Leg configuration reload, per-session tail route add/drain/remove, force removal, and a reload operation/result record. It keeps the canonical shared Core as the only scheduler, ACK, recovery and reassembly implementation. B0 remains default; B1 is disabled.

Linux Client supports SIGHUP and the included systemd unit maps `systemctl reload smp3-client` to it. Windows and generic CLI use the same-directory local request/result marker (`--reload`, `--force-reload`). Saving the config file alone does not reload. The operation is staged per Session with compensation, not a globally atomic transaction.

The final Go1.25.5 build includes Linux amd64, Linux arm64 cross-build, and Windows amd64 Client/Landing binaries. The package does not include `smp3-front`; Realm is documented as TCP byte forwarding only, but actual Realm compatibility was not exercised because no Realm binary was available.

Windows/WSL six-module tests and Vet passed; WSL Race passed. Windows Race could not link in the available clang environment. Local two-process reload/rejoin tests verified a stable Session, SHA, EOF, ACK/unique bytes and generation increase. The fixed N=1/N=8 process comparison used a paced local relay and showed equal ~24.3 Mbps; this is not an N-leg speedup claim. A dynamic 16MiB reload case recorded ~48.7% payload-equivalent retransmit telemetry and remains a known concern.

P0-C and general release remain FAIL / NOT READY. Actual Realm, WAN, kernel blackhole, Windows 512MiB/60s, long resource soak, Native runtime reload parity and Windows Race remain NOT RUN, NOT EXERCISED or environment-blocked. Review `KNOWN_ISSUES.md`, `SECURITY_NOTES.md`, `UPGRADE_AND_ROLLBACK.md`, and the Phase33 engineering evidence package before operator use.

No production server was contacted or changed during this phase.
