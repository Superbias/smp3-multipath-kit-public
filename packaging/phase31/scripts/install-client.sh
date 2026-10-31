#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/install-common.sh"
require_root
require_linux
root="$(bundle_root)"
arch="$(linux_arch)"
binary="bin/$arch/smp3-client"
verify_bundle_file "$root" "$binary"
verify_bundle_file "$root" "configs/client.example.json"
verify_bundle_file "$root" "systemd/smp3-client.service"
ensure_smp3_user
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup="/var/backups/smp3/install-$stamp"
backup_existing /usr/local/bin/smp3-client "$backup"
backup_existing /etc/smp3/client.json "$backup"
backup_existing /etc/systemd/system/smp3-client.service "$backup"
install -o root -g root -m 0755 "$root/$binary" /usr/local/bin/smp3-client
install_config_if_missing "$root/configs/client.example.json" /etc/smp3/client.json
install -o root -g root -m 0644 "$root/systemd/smp3-client.service" /etc/systemd/system/smp3-client.service
systemctl daemon-reload
echo "Installed Client. Configure upstream SOCKS and TLS trust before starting: systemctl enable --now smp3-client"
