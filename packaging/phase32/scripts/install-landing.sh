#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/install-common.sh"
require_root
require_linux
root="$(bundle_root)"
arch="$(linux_arch)"
binary="bin/$arch/smp3-server"
verify_bundle_file "$root" "$binary"
verify_bundle_file "$root" "configs/landing.example.json"
verify_bundle_file "$root" "systemd/smp3-landing.service"
ensure_smp3_user
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup="/var/backups/smp3/install-$stamp"
backup_existing /usr/local/bin/smp3-server "$backup"
backup_existing /etc/smp3/landing.json "$backup"
backup_existing /etc/systemd/system/smp3-landing.service "$backup"
install -o root -g root -m 0755 "$root/$binary" /usr/local/bin/smp3-server
install_config_if_missing "$root/configs/landing.example.json" /etc/smp3/landing.json
install -o root -g root -m 0644 "$root/systemd/smp3-landing.service" /etc/systemd/system/smp3-landing.service
systemctl daemon-reload
echo "Installed Landing. Edit /etc/smp3/landing.json and install TLS files before starting: systemctl enable --now smp3-landing"
