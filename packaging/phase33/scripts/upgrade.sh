#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/install-common.sh"
require_root
require_linux
[[ $# -eq 1 ]] || { echo "usage: $0 landing|client" >&2; exit 2; }
component="$1"
case "$component" in
  landing) binary=smp3-server; config=landing.json; service=smp3-landing ;;
  client) binary=smp3-client; config=client.json; service=smp3-client ;;
  *) echo "unknown component: $component" >&2; exit 2 ;;
esac
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup="/var/backups/smp3/upgrade-$stamp"
install -d -m 0700 "$backup"
was_active=false
if systemctl is-active --quiet "$service"; then
  touch "$backup/service.was-active"
  was_active=true
fi
backup_existing "/usr/local/bin/$binary" "$backup"
backup_existing "/etc/smp3/$config" "$backup"
backup_existing "/etc/systemd/system/$service.service" "$backup"
[[ -f "$backup/$binary" ]] || { echo "no installed binary to upgrade: /usr/local/bin/$binary" >&2; exit 1; }
"$(dirname "$0")/install-$component.sh"
if $was_active; then
  if systemctl restart "$service" && systemctl is-active --quiet "$service"; then
    echo "Upgrade complete; previous files: $backup"
    exit 0
  fi
  echo "new service did not become active; restoring previous files" >&2
  install -o root -g root -m 0755 "$backup/$binary" "/usr/local/bin/$binary"
  if [[ -f "$backup/$config" ]]; then install -o root -g smp3 -m 0640 "$backup/$config" "/etc/smp3/$config"; fi
  if [[ -f "$backup/$service.service" ]]; then install -o root -g root -m 0644 "$backup/$service.service" "/etc/systemd/system/$service.service"; fi
  systemctl daemon-reload
  systemctl start "$service" || true
  echo "automatic restore attempted; inspect systemctl status and journalctl; backup: $backup" >&2
  exit 1
fi
echo "Files upgraded; service was inactive and remains stopped. Previous files: $backup"
