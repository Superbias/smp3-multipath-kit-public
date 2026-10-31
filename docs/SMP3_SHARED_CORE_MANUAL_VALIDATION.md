# SMP3 shared Core: manual rollback and limited validation

**Plan only.** No production, Clash Party, profile, route, or installed binary was accessed or changed during this audit. Overall release status is FAIL. This document describes steps for an authorized operator; it does not authorize them.

## Three separate responses

| Response | Effect | New and existing connections |
|---|---|---|
| Turn off the dynamic policy with capacity-mode fixed | Stops dynamic admission/recovery policy for a comparison. Shared ACK, queue, rescue, CLOSE, and lifecycle code remains. The experiments do not prove this mode is a safe stop-loss. | Applying a config may require a host rebuild or restart. Existing sessions may be affected. |
| Roll back code | Restore the exact reviewed client and server binaries and their matching configuration. | Client exit, service restart, and replacement may interrupt existing sessions. Protocol compatibility does not promise session continuity. |
| Business stop-loss candidate through leg0 | Manually select the independently tested Snell leg0 route for one observable application's new connection. | Treat existing stalled sessions separately; an application owner decides whether to retry or terminate them. A route switch does not migrate an existing TCP session. |

Prior task notes record that the Snell leg0 selector was available in Clash Party and that three lightweight HTTPS probes through it completed in 119/155/147 ms. This confirms basic reachability at that time. It does not verify the current route, the intended production destination, sustained application throughput, or current health. Recheck the exact business path before relying on it as an emergency route. The SMP3 aggregation node name alone does not prove which physical route carries a flow.

## Information to capture before validation

An authorized operator must record, with credentials removed:

- Installed Native and server versions/commits, binary SHA256 values, and rollback target hashes.
- Effective client and server settings from the active processes, not only saved profiles. Record asymmetry instead of assuming both ends use the same values.
- Leg mapping, live RTT of each leg, and the unchanged healthy leg0 single-path capacity B.
- The intended test application's exact destination and an independent leg0 route to that destination.
- Existing sessions and new test connections separately, plus current production SLOs and an operator responsible for retries.

The user-provided client values are only configuration inputs for planning: aggregation scheduler, dynamic capacity mode, activation threshold 80 Mbps and 10 s activation window, 64 KiB chunks, queue 256, bandwidth priors 128/500 Mbps, reorder 4096, inflight 1024, ACK 20 ms, retransmit 1500 ms, recovery 15 s, redial 1 s, preferred leg 0, and 500 ms startup grace. Credentials are deliberately omitted. These values do not substitute for an effective runtime dump. Historical server settings and the client activation window differed; verify the current pair manually.

## Limited validation sequence

1. Resolve the remaining release gates or record explicit risk acceptance from the policy owner. Do not change the original comparison window or lower B.
2. In an isolated environment, build and run the exact proposed Native/client and server versions with the actual sanitized settings. First establish the healthy leg0 single-path B, then healthy dual-path performance, then slow, blackhole, and recovery. Preserve every raw sample.
3. Only after separate human authorization, have an operator select one test application and one new connection. Keep the existing production path untouched. Observe the new connection alongside existing sessions and check payload integrity, EOF, fixed-window throughput, admission/recovery state, queue/ledger, provider references where observable, process resources, and telemetry.
4. Stop and review immediately on checksum/EOF failure, a fixed 10-second window below 90% of the unchanged B, an application pause longer than 2 seconds, repeated recovery-state cycling without dual-path benefit, unbounded ledger/provider/resource growth, or loss of observability. These are proposed stop conditions for the limited exercise; use stricter existing production SLOs when they apply.
5. Review all samples and existing-session effects before any expansion. Do not promise that reloads, route changes, upgrades, restarts, or rollback preserve active sessions.

## Manual-only rollback aids

Read-only planning commands are:

- Windows Native plan: powershell -ExecutionPolicy Bypass -File scripts/manual-smp3-native-rollback.ps1 -Action Plan
- Standalone server plan: bash scripts/manual-smp3-server-rollback.sh plan
- Readiness check against sanitized placeholders: python scripts/check-release-canary-readiness.py docs/SMP3_SHARED_CORE_CANARY_INPUT.example.json

The readiness check is expected to return ENVIRONMENT BLOCKED with exit code 2 until real, sanitized, reviewed evidence is supplied. Plan/readiness mode performs no backup, restore, deployment, route selection, or production request.

Before backup or restore, a human operator must confirm the precise old binary and matching config hashes, approve a maintenance window and session interruption, and record an authorization reference. If the installed hash differs from the reviewed rollback target, stop and re-audit; do not bypass the check. Restore only the exact reviewed pair. Verify service health, leg handshakes, a finite payload/EOF test, and the intended business route manually. Do not automatically select a different binary or route on failure.

The rollback scripts' Backup and Restore modes have not been executed in this task. They are aids for an authorized operator and do not themselves grant authorization. Production canary and rollback remain NOT RUN / ENVIRONMENT BLOCKED.
