#!/usr/bin/env bash
set -euo pipefail
[[ ${EUID:-$(id -u)} -eq 0 ]] || { echo "run with sudo" >&2; exit 1; }
[[ $# -eq 2 ]] || { echo "usage: $0 landing|front|client BACKUP_DIR" >&2; exit 2; }
component="$1"; backup="$2"
case "$component" in
  landing) binary=smp3-server; config=landing.json; service=smp3-landing ;;
  front) binary=smp3-front; config=front.json; service=smp3-front ;;
  client) binary=smp3-client; config=client.json; service=smp3-client ;;
  *) echo "unknown component: $component" >&2; exit 2 ;;
esac
[[ -d "$backup" ]] || { echo "backup directory not found: $backup" >&2; exit 1; }
systemctl stop "$service" || true
[[ -f "$backup/$binary" ]] || { echo "previous binary missing in backup" >&2; exit 1; }
install -o root -g root -m 0755 "$backup/$binary" "/usr/local/bin/$binary"
if [[ -f "$backup/$config" ]]; then install -o root -g smp3 -m 0640 "$backup/$config" "/etc/smp3/$config"; fi
if [[ -f "$backup/$service.service" ]]; then install -o root -g root -m 0644 "$backup/$service.service" "/etc/systemd/system/$service.service"; fi
systemctl daemon-reload
if [[ -f "$backup/service.was-active" ]]; then systemctl start "$service"; fi
echo "Rollback restored $binary and any backed-up config; verify with systemctl status $service and journalctl -u $service"
