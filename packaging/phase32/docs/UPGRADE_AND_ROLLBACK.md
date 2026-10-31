# Upgrade and Rollback

## Upgrade

1. Verify the deployment ZIP SHA-256 against its sidecar and run `scripts/verify-checksums.sh` after extraction.
2. Back up `/etc/smp3`, `/usr/local/bin/smp3-client` or `smp3-server`, and the relevant systemd unit. The Phase31 `scripts/upgrade.sh` also creates a timestamped backup; retain its exact path.
3. Stop the service; install the new binary/config/unit; run `smp3-client -c ... -check` or `smp3-server -c ... -check`; then start and inspect status/logs.
4. For Client, use the new `routes.legs` schema or leave the two-leg fields unchanged. Do not change protocol version or route count during an active application session.

## Rollback to Phase31

1. Stop `smp3-client`/`smp3-landing`.
2. Locate the timestamped backup created by the upgrade script (or the manual backup directory). Preserve N-leg evidence before changing files.
3. Run `sudo bash scripts/rollback.sh client BACKUP_DIR` and/or `sudo bash scripts/rollback.sh landing BACKUP_DIR` from a trusted copy of the matching Phase31 bundle, or restore the saved binary/config/unit manually.
4. Phase31 Client understands only the old `routes.leg0` / `routes.leg1` fields. Restore the Phase31 config with exactly two legs. Restore a mutually compatible Landing binary as well.
5. Run `-check`, start services, and verify telemetry. Existing TCP sessions may disconnect during rollback; no live-session preservation is promised across version or config changes.

Rollback is an operator action and was not executed against production in Phase32.
