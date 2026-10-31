#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/install-common.sh"
require_root
require_linux
root="$(bundle_root)"
arch="$(linux_arch)"
binary="bin/$arch/smp3-front"
verify_bundle_file "$root" "$binary"
verify_bundle_file "$root" "configs/front.example.json"
verify_bundle_file "$root" "systemd/smp3-front.service"
ensure_smp3_user
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup="/var/backups/smp3/install-$stamp"
backup_existing /usr/local/bin/smp3-front "$backup"
backup_existing /etc/smp3/front.json "$backup"
backup_existing /etc/systemd/system/smp3-front.service "$backup"
install -o root -g root -m 0755 "$root/$binary" /usr/local/bin/smp3-front
install_config_if_missing "$root/configs/front.example.json" /etc/smp3/front.json
install -o root -g root -m 0644 "$root/systemd/smp3-front.service" /etc/systemd/system/smp3-front.service
systemctl daemon-reload
echo "Installed Front. Restrict allowed_sources in /etc/smp3/front.json before starting: systemctl enable --now smp3-front"
