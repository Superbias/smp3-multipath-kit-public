# SMP3 manual rollback and canary boundary

**Release status: FAIL / NOT READY. Operations described here have not been executed.** This C1 audit did not access or modify production, Clash Party, active routes, profiles, services, or installed binaries. There is no automatic deployment, route switch, restore, or release step.

See docs/SMP3_SHARED_CORE_RECOVERY_FIX.md for the measured result and blockers, and docs/SMP3_SHARED_CORE_MANUAL_VALIDATION.md for the complete human-run sequence.

## Recovery choices

- Policy off: capacity-mode fixed is a comparison or dynamic-policy-off control. It leaves common ACK, queue, rescue, CLOSE, and lifecycle behavior in place. It is not a proven production stop-loss.
- Code rollback: restore only the exact reviewed client/server binary pair and their matching configurations. A client exit or service restart may break active sessions; protocol interop does not promise preservation.
- Business stop-loss candidate: a separately selectable leg0 Snell path is a candidate for one application's new connection. Earlier task notes record three successful lightweight HTTPS probes, but do not establish current sustained business-path health. An operator must confirm the target route and unchanged single-path B first. Old connections need application-owner direction.

Do not promise existing-session preservation after reload, route change, upgrade, process restart, or rollback. Do not treat the known old-server downlink issue as resolved by restoring the old server.

## Plan-only commands

- Native rollback plan: powershell -ExecutionPolicy Bypass -File scripts/manual-smp3-native-rollback.ps1 -Action Plan
- Server rollback plan: bash scripts/manual-smp3-server-rollback.sh plan
- Canary readiness against sanitized placeholders: python scripts/check-release-canary-readiness.py docs/SMP3_SHARED_CORE_CANARY_INPUT.example.json

The readiness check should report ENVIRONMENT BLOCKED until the operator provides reviewed current versions, exact effective settings for both ends, both RTTs, an unchanged healthy leg0 B, a verified route to the intended destination, and observable test-application details. Supplying those inputs only produces a review plan; it does not authorize or execute a canary.

Backup/Restore are disruptive-capable and require a separate human decision, a maintenance window, exact hash checks, matching configurations, and a known application retry plan. The scripts are not proof of an approved rollback target. A hash mismatch is a stop condition. Restore one exact pair; do not fall back automatically to an arbitrary version or route.

## Limited validation

After release gates are resolved or the responsible policy owner records an explicit acceptance, repeat the tests in isolation with the exact proposed versions and settings. Keep the pre-change healthy single-leg B and original 10-second comparison window fixed. Retain every sample, including failed windows.

Only a separate human authorization may start a one-application, one-new-connection observation. Keep the current route for existing connections. Stop on integrity or EOF failure, a fixed 10-second window below 90% of unchanged B, an application pause longer than 2 seconds, recovery cycling without benefit, growing ledger/provider/resources, or missing telemetry; stricter existing production SLOs take precedence. Review the result and existing-session impact before any expansion.

At this point production RTT, exact runtime versions/settings, and sustained business-path capacity are unavailable in the local evidence set. Production validation and canary are NOT RUN / ENVIRONMENT BLOCKED.
