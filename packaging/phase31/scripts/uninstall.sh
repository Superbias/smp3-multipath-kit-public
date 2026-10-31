#!/usr/bin/env bash
set -euo pipefail
[[ ${EUID:-$(id -u)} -eq 0 ]] || { echo "run with sudo" >&2; exit 1; }
purge=false
if [[ ${1:-} == --purge-config ]]; then purge=true; shift; fi
if [[ $# -eq 0 ]]; then
  components=(landing front client)
else
  components=("$@")
fi
for component in ${components[*]}; do
  case "$component" in
    landing) binary=smp3-server; config=landing.json; service=smp3-landing ;;
    front) binary=smp3-front; config=front.json; service=smp3-front ;;
    client) binary=smp3-client; config=client.json; service=smp3-client ;;
    *) echo "unknown component: $component" >&2; exit 2 ;;
  esac
  systemctl disable --now "$service" 2>/dev/null || true
  rm -f "/etc/systemd/system/$service.service" "/usr/local/bin/$binary"
  if $purge; then rm -f "/etc/smp3/$config"; fi
done
systemctl daemon-reload
echo "Removed selected SMP3 binaries and units. TLS certificates and /var/lib/smp3 data were preserved."
