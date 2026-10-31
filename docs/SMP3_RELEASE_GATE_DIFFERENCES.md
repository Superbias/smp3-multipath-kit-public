# SMP3 migration parity differences — final C1 audit

**Result: FAIL.** The original checker, frozen goldens, and allowlists were left unchanged. No checks were skipped. The complete B0/C0/C1 raw failure sets, hashes, classifications, command log, and before/after manifest verification are in artifacts/shared-core-recovery-20261014/parity-matrix-run1/.

| Candidate | Failure paths | Checker exit | Change |
|---|---:|---:|---|
| B0, 907b242c5f2532f774a3db1a877cf32740dcdfab | 31 | 1 | baseline |
| C0, frozen 341-file candidate | 57 | 1 | +26, removed 0 from B0 |
| C1, current candidate | 59 | 1 | +2, removed 0 from C0 |

B0→C0 contains 26 newly reported failure paths. Across the union, source comparison classifies 14 files as identical, 22 as newline-only CRLF/LF normalization, 6 as source-content changes, and 15 as added/removed source. This describes content comparison; it does not waive or reclassify the raw checker failure.

C0→C1 adds exactly two checker paths: stream_release_matched_config_test.go and stream_release_raw_rtt_test.go. These are expected new tests for matched settings and raw RTT calibration. The related changed source records are stream_admission.go, stream_blocker_feedback_test.go, and stream_release_audit_test.go. No C0 failure path was removed. Although task-related, both additions require explicit parity policy review; they remain failures until that review is approved.

The comparison used the same checker against all three inputs. CORE_SOURCE_MANIFEST_AFTER was restored byte-for-byte after every checker run; before and after hashes match. No frozen golden, allowlist, checker implementation, or skip changed. The final C1 failure set remains 59 and is not a release PASS.

The earlier historical input-51 candidate is not the frozen C0 used here. Do not merge its counts with this B0/C0/C1 matrix. For a line-by-line decision, use parity-matrix.json and its preserved raw checker output.
