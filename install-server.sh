#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
BIN="$ROOT/dist/smp3-server-linux-amd64"
CFG_SRC="${1:-$ROOT/config/standalone-server.example.json}"
PREFIX="/opt/smp3-standalone"
CONFIG="$PREFIX/config.json"
UNIT="/etc/systemd/system/smp3-standalone.service"

[ "$(id -u)" -eq 0 ] || { echo 'run as root (sudo ./install-server.sh)'; exit 1; }
[ -x "$BIN" ] || { echo "missing $BIN; build release artifacts first" >&2; exit 1; }
[ -f "$CFG_SRC" ] || { echo "missing config: $CFG_SRC" >&2; exit 1; }

install -d -m 0750 "$PREFIX"
install -m 0640 "$CFG_SRC" "$CONFIG.new"
"$BIN" -c "$CONFIG.new" -check
install -m 0755 "$BIN" "$PREFIX/smp3-server"
mv -f "$CONFIG.new" "$CONFIG"
chown root:root "$PREFIX/smp3-server" "$CONFIG"
chmod 0755 "$PREFIX/smp3-server"
chmod 0640 "$CONFIG"

cat >"$UNIT" <<'UNIT'
[Unit]
Description=SMP3 standalone server 2.0.0
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/opt/smp3-standalone/smp3-server -c /opt/smp3-standalone/config.json
Restart=on-failure
RestartSec=2
LimitNOFILE=1048576
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now smp3-standalone
systemctl --no-pager --full status smp3-standalone

echo '[+] standalone SMP3 server installed/updated'
echo "    config: $CONFIG"
echo '    logs:   journalctl -u smp3-standalone -f'
