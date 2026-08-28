#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
BIN="$ROOT/dist/smp3-proxy-linux-amd64"
CFG_SRC="${1:-$ROOT/config/server.json}"
[ "$(id -u)" -eq 0 ] || { echo 'run as root (sudo ./install-server.sh)'; exit 1; }
[ -x "$BIN" ] || { echo "build first: ./build.sh"; exit 1; }
[ -f "$CFG_SRC" ] || { echo "create $ROOT/config/server.json from server.example.json first"; exit 1; }

install -d -m 0750 /etc/smp3-proxy
install -m 0640 "$CFG_SRC" /etc/smp3-proxy/config.json.new
"$BIN" check -c /etc/smp3-proxy/config.json.new

# alpha1 used the provisional sing-box-mp service name. Stop it if present
# so an in-place test upgrade cannot leave two local engines competing.
if systemctl list-unit-files sing-box-mp.service >/dev/null 2>&1; then
  systemctl disable --now sing-box-mp.service || true
fi
if systemctl list-unit-files smp3-proxy.service >/dev/null 2>&1; then
  systemctl stop smp3-proxy.service || true
fi
install -m 0755 "$BIN" /usr/local/bin/smp3-proxy
mv -f /etc/smp3-proxy/config.json.new /etc/smp3-proxy/config.json
chmod 0640 /etc/smp3-proxy/config.json

cat >/etc/systemd/system/smp3-proxy.service <<'UNIT'
[Unit]
Description=SMP3 Multipath proxy alpha2.3
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/smp3-proxy run -c /etc/smp3-proxy/config.json
Restart=on-failure
RestartSec=2
LimitNOFILE=1048576
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now smp3-proxy
sleep 1
systemctl --no-pager --full status smp3-proxy

echo '[+] server installed/updated'
echo '    config: /etc/smp3-proxy/config.json'
echo '    logs:   journalctl -u smp3-proxy -f'
