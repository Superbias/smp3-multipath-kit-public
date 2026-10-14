#!/usr/bin/env bash
# Manual production rollback aid. This file has NOT been applied to production.
set -euo pipefail
ACTION="${1:-plan}"
SERVICE=smp3-standalone.service
BINARY=/opt/smp3-standalone/smp3-server
CONFIG=/opt/smp3-standalone/config.json
OLD_SHA=8a7d228d5ce51bc4e2aee132787a0b1c243f7110bb5870c749774d7fd2a68741
OLD_CONFIG_SHA=0e34651585c8b855d8ec1d18d9c4a7eb8051a174ee969a89ae8c654151608081
if [ "$ACTION" = plan ] || [ "$ACTION" = --help ]; then
  printf '%s\n' 'NOT EXECUTED: backup or restore requires explicit human authorization.' \
    'Policy fixed is a strategy-off control, not a proven production emergency path.' \
    'Backup: set SMP3_OPERATOR_APPROVAL to the authorization record and SMP3_ROLLBACK_DIR to a new /opt/smp3-standalone/rollback-* directory, then run this script backup on the server.' \
    'Restore: additionally set SMP3_EXPECTED_CURRENT_SHA to the reviewed installed candidate hash, then run restore.' \
    'Restore stops the service; existing sessions may terminate. Business retries are manual.' \
    "Service=$SERVICE Binary=$BINARY Config=$CONFIG OldBinary=$OLD_SHA OldConfig=$OLD_CONFIG_SHA"
  exit 0
fi
case "$ACTION" in backup|restore) ;; *) echo 'unknown action' >&2; exit 2;; esac
: "${SMP3_OPERATOR_APPROVAL:?Explicit human authorization record required}"
: "${SMP3_ROLLBACK_DIR:?New or existing reviewed backup directory required}"
[ "$(id -u)" = 0 ] || { echo 'run manually as server root' >&2; exit 2; }
BACKUP="$SMP3_ROLLBACK_DIR"
[[ "$BACKUP" =~ ^/opt/smp3-standalone/rollback-[A-Za-z0-9_-]+$ ]] || { echo 'invalid backup path' >&2; exit 2; }
check_sha() { [ "$(sha256sum "$1" | cut -d' ' -f1)" = "$2" ] || { echo "hash mismatch: $1" >&2; exit 3; }; }
if [ "$ACTION" = backup ]; then
  check_sha "$BINARY" "$OLD_SHA"
  check_sha "$CONFIG" "$OLD_CONFIG_SHA"
  mkdir -m 700 -- "$BACKUP" # Refuse to overwrite a previous backup.
  cp -p -- "$BINARY" "$BACKUP/smp3-server"
  cp -p -- "$CONFIG" "$BACKUP/config.json"
  check_sha "$BACKUP/smp3-server" "$OLD_SHA"
  check_sha "$BACKUP/config.json" "$OLD_CONFIG_SHA"
  printf '%s\n' "$SMP3_OPERATOR_APPROVAL" > "$BACKUP/authorization.txt"
  chmod 600 "$BACKUP/authorization.txt"
  echo 'Backup verified; no service change.'
  exit 0
fi
: "${SMP3_EXPECTED_CURRENT_SHA:?Reviewed current binary hash required}"
[[ "$SMP3_EXPECTED_CURRENT_SHA" =~ ^[0-9a-f]{64}$ ]] || exit 2
check_sha "$BINARY" "$SMP3_EXPECTED_CURRENT_SHA"
check_sha "$BACKUP/smp3-server" "$OLD_SHA"
check_sha "$BACKUP/config.json" "$OLD_CONFIG_SHA"
PID="$(systemctl show "$SERVICE" --property=MainPID --value)"
[ "$(readlink "/proc/$PID/exe")" = "$BINARY" ] || { echo 'running executable path mismatch' >&2; exit 3; }
# This is the disruptive boundary. It is reached only in manually authorized restore.
systemctl stop "$SERVICE"
cp -p -- "$BACKUP/smp3-server" "$BINARY.rollback.$$.tmp"
cp -p -- "$BACKUP/config.json" "$CONFIG.rollback.$$.tmp"
mv -f -- "$BINARY.rollback.$$.tmp" "$BINARY"
mv -f -- "$CONFIG.rollback.$$.tmp" "$CONFIG"
check_sha "$BINARY" "$OLD_SHA"
check_sha "$CONFIG" "$OLD_CONFIG_SHA"
systemctl start "$SERVICE"
systemctl is-active --quiet "$SERVICE"
echo 'Old files restored and service active. Application health and retries still require operator checks.'
