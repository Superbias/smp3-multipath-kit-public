# SMP3 release results — FAIL

Current final C1 source: B0 907b242c5f2532f774a3db1a877cf32740dcdfab, compared with frozen C0 source and the exact process/build evidence in artifacts/shared-core-recovery-20261014/.

| Gate | Result |
|---|---|
| C0 and C1 Native/Standalone healthy RTT 20/100/200/400 ms | Each 24/24 PASS; single-leg C0-to-C1 median changes are between −0.135% and +0.238% Native and −0.036% and +0.088% Standalone |
| C1 real-process slow/blackhole recovery | Native 24/24 and Standalone 24/24 PASS against frozen B and unchanged fixed 55–65 s window; SHA, EOF/drain and connections verified |
| Core simulated RTT matrix | 16/16 PASS; 41 samples/scenario; not a real process test |
| C1 shared degradation/recovery soak | Native and Standalone ran 1,800 traffic seconds each; all raw windows retained. First Native post-restore 10 s window is below 1.5B in 10/15 cycles; next 10 s window passes 15/15 |
| Linux raw Carrier calibration | 50, 200, simultaneous 50+200, fragmented 100 Mbps PASS; separate from Core |
| Windows raw Carrier calibration | FAIL: two 400 ms RTT windows measured 33.397/33.974 Mbps against 95 Mbps floor; test tool/platform outcome, no Windows throughput certification |
| Go test, vet, race | PASS, all local modules, Go 1.22.6 |
| go mod verify | PASS, all five modules with GOWORK disabled |
| Legacy race3 and queue race30 | C1 PASS on retained candidate runs; B0 historical race3 fails silent-primary 3/3, queue race30 has 3 failures in rerun 2 |
| Migration parity | **FAIL**: B0 31, C0 57, C1 59. C0→C1 adds two test paths; no golden/allowlist/checker change |
| Mixed-version new-session interoperability | Both tested C0/C1 client/server directions PASS locally with 1 GiB SHA/EOF/drain; does not prove arbitrary versions or old-session continuity |
| Production canary and live Carrier capacity | NOT RUN / ENVIRONMENT BLOCKED |

The specific recovery fix passes its measured acceptance. Overall release remains FAIL until parity review is resolved and the remaining platform/legacy risks are handled. The complete reasoning, code-level invariant audit, raw evidence map, and manual rollback limits are in docs/SMP3_SHARED_CORE_RECOVERY_FIX.md.
