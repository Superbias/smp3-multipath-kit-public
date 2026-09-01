#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
INSTALLER="$ROOT/scripts/install-smp3-server.sh"
CTL_SOURCE="$ROOT/scripts/smp3ctl.sh"

bash -n "$INSTALLER" "$CTL_SOURCE"
grep -q '/opt/smp3-standalone/smp3-server' "$INSTALLER"
grep -q '/etc/smp3-standalone/config.json' "$INSTALLER"
grep -q 'STOP: CHECKSUM_MISMATCH' "$INSTALLER"
grep -q 'STOP: CONFIG_REQUIRED' "$INSTALLER"
grep -q 'STOP: UNSUPPORTED_ARCH' "$INSTALLER"
grep -q 'STOP: SYSTEMD_REQUIRED' "$INSTALLER"
grep -q 'rotate_backups' "$INSTALLER"
grep -q 'smp3ctl status' "$CTL_SOURCE"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/assets" "$TMP/bin" "$TMP/root/fake-state"

make_server() {
    local version="$1" output="$2"
    cat >"$output" <<EOF
#!/usr/bin/env bash
if [[ "\${1:-}" == "-version" ]]; then echo "$version"; exit 0; fi
if [[ "\${1:-}" == "-c" ]]; then exit 0; fi
exit 2
EOF
    chmod 0755 "$output"
}

make_sums() {
    (cd "$(dirname "$1")" && sha256sum "$(basename "$1")" > SHA256SUMS)
}

cat >"$TMP/bin/systemctl" <<'SYSTEMCTL'
#!/usr/bin/env bash
set -euo pipefail
state="${SMP3_INSTALL_TEST_ROOT:?}/fake-state"
command="${1:-}"
shift || true
case "$command" in
  is-active) [[ "${1:-}" != "--quiet" ]] || shift; test -f "$state/active" ;;
  is-enabled) [[ "${1:-}" != "--quiet" ]] || shift; test -f "$state/enabled" ;;
  start) [[ ! -f "$state/fail-start" ]] || exit 1; touch "$state/active" ;;
  stop) rm -f "$state/active" ;;
  restart) touch "$state/active" ;;
  enable) touch "$state/enabled" ;;
  disable) rm -f "$state/enabled" "$state/active" ;;
  daemon-reload) : ;;
  show) echo 0 ;;
  *) : ;;
esac
SYSTEMCTL
chmod 0755 "$TMP/bin/systemctl"

cat >"$TMP/bin/journalctl" <<'JOURNALCTL'
#!/usr/bin/env bash
echo 'fake journal output'
JOURNALCTL
chmod 0755 "$TMP/bin/journalctl"

make_server 2.0.1 "$TMP/assets/smp3-server-linux-amd64"
make_sums "$TMP/assets/smp3-server-linux-amd64"
printf '%s' '{"tag_name":"v2.0.1","draft":false,"prerelease":false,"assets":[{"name":"smp3-server-linux-amd64","browser_download_url":"file:///fixture/server"},{"name":"SHA256SUMS","browser_download_url":"file:///fixture/sums"}]}' >"$TMP/release.json"

export SMP3_INSTALL_TEST_ROOT="$TMP/root"
export SMP3_INSTALLER_TEST_MODE=1
export SMP3_INSTALLER_TEST_RELEASE_JSON="$TMP/release.json"
export SMP3_INSTALLER_TEST_ASSET_DIR="$TMP/assets"
export PATH="$TMP/bin:$PATH"
cp "$ROOT/examples/smp3-server-config.example.json" "$TMP/config.json"

bash "$INSTALLER" --config "$TMP/config.json" --version 2.0.1 >"$TMP/install.out"
test -x "$TMP/root/opt/smp3-standalone/smp3-server"
test -f "$TMP/root/etc/smp3-standalone/config.json"
test "$(sha256sum "$TMP/root/opt/smp3-standalone/smp3-server" | awk '{print $1}')" = "$(sha256sum "$TMP/assets/smp3-server-linux-amd64" | awk '{print $1}')"
test "$(stat -c '%a' "$TMP/root/etc/smp3-standalone/config.json")" = 600
"$TMP/root/usr/local/bin/smp3ctl" status >/dev/null
"$TMP/root/usr/local/bin/smp3ctl" check >/dev/null
test "$("$TMP/root/usr/local/bin/smp3ctl" version)" = 2.0.1
"$TMP/root/usr/local/bin/smp3ctl" stop >/dev/null
"$TMP/root/usr/local/bin/smp3ctl" start >/dev/null
"$TMP/root/usr/local/bin/smp3ctl" restart >/dev/null
"$TMP/root/usr/local/bin/smp3ctl" logs >/dev/null

make_server 2.0.2 "$TMP/assets/smp3-server-linux-amd64"
make_sums "$TMP/assets/smp3-server-linux-amd64"
printf '%s' '{"tag_name":"v2.0.2","draft":false,"prerelease":false,"assets":[{"name":"smp3-server-linux-amd64","browser_download_url":"file:///fixture/server"},{"name":"SHA256SUMS","browser_download_url":"file:///fixture/sums"}]}' >"$TMP/release.json"
"$TMP/root/usr/local/bin/smp3ctl" update --version 2.0.2 >"$TMP/update.out"
test "$("$TMP/root/opt/smp3-standalone/smp3-server" -version)" = 2.0.2
test "$(find "$TMP/root/opt/smp3-standalone/backups" -name 'smp3-server.backup' | wc -l)" -ge 1

make_server 2.0.3 "$TMP/assets/smp3-server-linux-amd64"
make_sums "$TMP/assets/smp3-server-linux-amd64"
printf '%s' '{"tag_name":"v2.0.3","draft":false,"prerelease":false,"assets":[{"name":"smp3-server-linux-amd64","browser_download_url":"file:///fixture/server"},{"name":"SHA256SUMS","browser_download_url":"file:///fixture/sums"}]}' >"$TMP/release.json"
touch "$TMP/root/fake-state/fail-start"
if bash "$INSTALLER" --update --version 2.0.3 >"$TMP/start-fail.out" 2>&1; then
    echo 'service start failure was not rolled back' >&2
    exit 1
fi
grep -q 'UPDATE FAILED — ROLLED BACK' "$TMP/start-fail.out"
rm -f "$TMP/root/fake-state/fail-start"
test "$("$TMP/root/opt/smp3-standalone/smp3-server" -version)" = 2.0.2

printf '%s' bad >"$TMP/assets/smp3-server-linux-amd64"
if bash "$INSTALLER" --update --version 2.0.2 >"$TMP/checksum.out" 2>&1; then
    echo 'checksum mismatch was accepted' >&2
    exit 1
fi
grep -q 'CHECKSUM_MISMATCH' "$TMP/checksum.out"
test "$("$TMP/root/opt/smp3-standalone/smp3-server" -version)" = 2.0.2

"$TMP/root/usr/local/bin/smp3ctl" rollback >"$TMP/rollback.out"
test "$("$TMP/root/opt/smp3-standalone/smp3-server" -version)" = 2.0.1

for version in 2.0.3 2.0.4 2.0.5 2.0.6 2.0.7 2.0.8; do
    make_server "$version" "$TMP/assets/smp3-server-linux-amd64"
    make_sums "$TMP/assets/smp3-server-linux-amd64"
    printf '{"tag_name":"v%s","draft":false,"prerelease":false,"assets":[{"name":"smp3-server-linux-amd64","browser_download_url":"file:///fixture/server"},{"name":"SHA256SUMS","browser_download_url":"file:///fixture/sums"}]}' "$version" >"$TMP/release.json"
    bash "$INSTALLER" --update --version "$version" >"$TMP/rotation-$version.out"
done
test "$(find "$TMP/root/opt/smp3-standalone/backups" -mindepth 1 -maxdepth 1 -type d | wc -l)" -le 5

bash "$INSTALLER" --uninstall >"$TMP/uninstall.out"
test -f "$TMP/root/etc/smp3-standalone/config.json"
test ! -e "$TMP/root/opt/smp3-standalone/smp3-server"
test ! -e "$TMP/root/usr/local/bin/smp3ctl"

echo 'LINUX_INSTALLER_LIFECYCLE=PASS'
