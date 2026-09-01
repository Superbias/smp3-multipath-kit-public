#!/usr/bin/env bash
set -euo pipefail

REPOSITORY="Superbias/smp3-multipath-kit-public"
ASSET_NAME="smp3-server-linux-amd64"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
API_BASE="${SMP3_RELEASE_API_BASE:-https://api.github.com/repos/$REPOSITORY}"
REQUESTED_VERSION=""
CONFIG_ARG=""
ACTION="install"
NO_START=0
PURGE=0

if [[ -n "${SMP3_INSTALL_TEST_ROOT:-}" ]]; then
    TEST_ROOT="${SMP3_INSTALL_TEST_ROOT%/}"
    PREFIX="$TEST_ROOT/opt/smp3-standalone"
    ETC_DIR="$TEST_ROOT/etc/smp3-standalone"
    UNIT="$TEST_ROOT/etc/systemd/system/smp3-standalone.service"
    CTL="$TEST_ROOT/usr/local/bin/smp3ctl"
else
    PREFIX="/opt/smp3-standalone"
    ETC_DIR="/etc/smp3-standalone"
    UNIT="/etc/systemd/system/smp3-standalone.service"
    CTL="/usr/local/bin/smp3ctl"
fi
BIN="$PREFIX/smp3-server"
CONFIG_PATH="$ETC_DIR/config.json"
STATE="$PREFIX/install-state.json"
BACKUPS="$PREFIX/backups"

usage() {
    cat <<'USAGE'
Usage:
  install-smp3-server.sh --config PATH [--install|--update] [--version VERSION]
  install-smp3-server.sh --check
  install-smp3-server.sh --update [--version VERSION]
  install-smp3-server.sh --rollback
  install-smp3-server.sh --uninstall [--purge]

The default action is install. A config is required for a first install.
USAGE
}

die() {
    echo "$1" >&2
    exit 1
}

require_root() {
    [[ "${SMP3_INSTALLER_TEST_MODE:-0}" == 1 && -n "${SMP3_INSTALL_TEST_ROOT:-}" ]] && return 0
    [[ "$(id -u)" -eq 0 ]] || die "STOP: ROOT_REQUIRED — run this command as root (for example: sudo ...)"
}

require_platform() {
    [[ "$(uname -m)" == "x86_64" || "$(uname -m)" == "amd64" ]] || die "STOP: UNSUPPORTED_ARCH — Linux amd64 is required"
    command -v systemctl >/dev/null 2>&1 || die "STOP: SYSTEMD_REQUIRED — systemctl was not found"
    command -v curl >/dev/null 2>&1 || die "STOP: CURL_REQUIRED — curl was not found"
    command -v sha256sum >/dev/null 2>&1 || die "STOP: SHA256SUM_REQUIRED — sha256sum was not found"
}

json_field() {
    local key="$1"
    sed -n "s/.*\"$key\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" | head -n 1
}

json_bool() {
    local key="$1"
    sed -n "s/.*\"$key\"[[:space:]]*:[[:space:]]*\(true\|false\).*/\1/p" | head -n 1
}

fetch_release() {
    if [[ -n "${SMP3_INSTALLER_TEST_RELEASE_JSON:-}" ]]; then
        cat "$SMP3_INSTALLER_TEST_RELEASE_JSON"
        return
    fi
    local endpoint="$API_BASE/releases/latest"
    if [[ -n "$REQUESTED_VERSION" ]]; then
        endpoint="$API_BASE/releases/tags/v${REQUESTED_VERSION#v}"
    fi
    curl -fsSL --retry 3 --retry-all-errors --retry-delay 1 \
        -H 'User-Agent: smp3-installer' "$endpoint" \
        || die "STOP: RELEASE_FETCH_FAILED — unable to read GitHub Release metadata"
}

release_value() {
    local json="$1"
    local tag draft prerelease
    tag="$(printf '%s' "$json" | json_field tag_name)"
    draft="$(printf '%s' "$json" | json_bool draft)"
    prerelease="$(printf '%s' "$json" | json_bool prerelease)"
    [[ -n "$tag" ]] || die "STOP: RELEASE_FETCH_FAILED — Release has no tag_name"
    [[ "$draft" != "true" && "$prerelease" != "true" ]] || die "STOP: RELEASE_NOT_STABLE — prerelease/draft releases are refused"
    printf '%s\n' "$tag"
}

download_asset() {
    local tag="$1" name="$2" output="$3"
    if [[ -n "${SMP3_INSTALLER_TEST_ASSET_DIR:-}" ]]; then
        local fixture="$SMP3_INSTALLER_TEST_ASSET_DIR/$name"
        [[ -f "$fixture" ]] || die "STOP: RELEASE_ASSET_NOT_FOUND — $fixture"
        cp -f "$fixture" "$output"
        return
    fi
    local url="https://github.com/$REPOSITORY/releases/download/$tag/$name"
    curl -fsSL --retry 3 --retry-all-errors --retry-delay 1 \
        -H 'User-Agent: smp3-installer' -o "$output" "$url" \
        || die "STOP: RELEASE_DOWNLOAD_FAILED — $name"
}

manifest_sha() {
    local manifest="$1" name="$2" value
    value="$(awk -v target="$name" '$2 == target || $2 == "*" target { print $1; exit }' "$manifest")"
    [[ "$value" =~ ^[0-9A-Fa-f]{64}$ ]] || die "STOP: CHECKSUM_ENTRY_NOT_FOUND — exact entry for $name"
    printf '%s\n' "${value,,}"
}

sha256() {
    sha256sum "$1" | awk '{print tolower($1)}'
}

binary_version() {
    "$1" -version 2>/dev/null | head -n 1 || true
}

service_active() { systemctl is-active --quiet smp3-standalone; }
service_enabled() { systemctl is-enabled --quiet smp3-standalone; }

ensure_config() {
    if [[ -n "$CONFIG_ARG" ]]; then
        [[ -f "$CONFIG_ARG" ]] || die "STOP: CONFIG_REQUIRED — config file not found: $CONFIG_ARG"
        [[ -r "$CONFIG_ARG" ]] || die "STOP: CONFIG_REQUIRED — config file is not readable: $CONFIG_ARG"
        CONFIG_SOURCE="$CONFIG_ARG"
    elif [[ -f "$CONFIG_PATH" ]]; then
        CONFIG_SOURCE="$CONFIG_PATH"
    else
        die "STOP: CONFIG_REQUIRED — provide --config PATH (start from examples/smp3-server-config.example.json)"
    fi
}

write_unit() {
    install -d -m 0755 "$(dirname "$UNIT")"
    cat >"$UNIT.new" <<'UNIT'
[Unit]
Description=SMP3 standalone server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/opt/smp3-standalone/smp3-server -c /etc/smp3-standalone/config.json
Restart=on-failure
RestartSec=2
LimitNOFILE=1048576
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
UNIT
    mv -f "$UNIT.new" "$UNIT"
}

write_ctl() {
    install -d -m 0755 "$(dirname "$CTL")"
    cat >"$CTL.new" <<EOF
#!/usr/bin/env bash
set -euo pipefail
INSTALLER="$PREFIX/install-smp3-server.sh"
UNIT="smp3-standalone"
case "\${1:-status}" in
  status) exec "\$INSTALLER" --check ;;
  start) exec systemctl start "\$UNIT" ;;
  stop) exec systemctl stop "\$UNIT" ;;
  restart) exec systemctl restart "\$UNIT" ;;
  logs) shift; if [[ "\${1:-}" == "-f" ]]; then exec journalctl -u "\$UNIT" -f; else exec journalctl -u "\$UNIT" -n 100 --no-pager; fi ;;
  check) exec "\$INSTALLER" --check ;;
  update) shift; exec "\$INSTALLER" --update "\$@" ;;
  rollback) exec "\$INSTALLER" --rollback ;;
  version) exec "$PREFIX/smp3-server" -version ;;
  *) echo "usage: smp3ctl status|start|stop|restart|logs [-f]|check|update|rollback|version" >&2; exit 2 ;;
esac
EOF
    chmod 0755 "$CTL.new"
    mv -f "$CTL.new" "$CTL"
}

write_state() {
    local generation="$1" old_sha="$2" new_sha="$3" new_version="$4" config_changed="$5" was_active="$6" was_enabled="$7"
    cat >"$STATE.new" <<EOF
{
  "original_path": "$BIN",
  "original_sha256": "$old_sha",
  "backup_path": "$generation/smp3-server.backup",
  "installed_version": "$new_version",
  "installed_sha256": "$new_sha",
  "config_changed": $config_changed,
  "service_was_active": $was_active,
  "service_was_enabled": $was_enabled,
  "updated_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
    chmod 0600 "$STATE.new"
    mv -f "$STATE.new" "$STATE"
}

read_state_field() {
    local key="$1"
    [[ -f "$STATE" ]] || return 1
    sed -n "s/.*\"$key\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" "$STATE" | head -n 1
}

latest_generation() {
    [[ -d "$BACKUPS" ]] || die "STOP: BACKUP_NOT_FOUND — no backup directory exists"
    find "$BACKUPS" -mindepth 1 -maxdepth 1 -type d -print | sort | tail -n 1
}

rotate_backups() {
    [[ -d "$BACKUPS" ]] || return 0
    local generations=()
    mapfile -t generations < <(find "$BACKUPS" -mindepth 1 -maxdepth 1 -type d -print | sort)
    while (( ${#generations[@]} > 5 )); do
        rm -rf "${generations[0]}"
        generations=("${generations[@]:1}")
    done
}

print_listener() {
    if command -v ss >/dev/null 2>&1; then
        ss -ltnp 2>/dev/null | grep ':24444' || true
    else
        echo 'Listener: ss unavailable'
    fi
}

do_check() {
    if [[ ! -x "$BIN" ]]; then
        echo 'SMP3 STANDALONE SERVER: NOT INSTALLED'
        return 1
    fi
    local sha version active enabled config_result
    sha="$(sha256 "$BIN")"
    version="$(binary_version "$BIN")"
    active='NO'; enabled='NO'
    service_active && active='YES' || true
    service_enabled && enabled='YES' || true
    config_result='NOT_AVAILABLE'
    if [[ -f "$CONFIG_PATH" ]]; then
        "$BIN" -c "$CONFIG_PATH" -check >/dev/null 2>&1 && config_result='PASS' || config_result='FAIL'
    fi
    echo "Service active: $active"
    echo "Service enabled: $enabled"
    echo "MainPID: $(systemctl show -p MainPID --value smp3-standalone 2>/dev/null || echo 0)"
    echo "Binary: $BIN"
    echo "Binary version: ${version:-UNKNOWN}"
    echo "Binary SHA256: $sha"
    echo "Config check: $config_result"
    echo 'Listener:'
    print_listener
    echo 'SMP3 STANDALONE SERVER: INSTALLED'
}

rollback_transaction() {
    local generation="$1" had_binary="$2" had_config="$3" had_unit="$4" was_active="$5" was_enabled="$6"
    systemctl stop smp3-standalone >/dev/null 2>&1 || true
    if [[ "$had_binary" == YES && -f "$generation/smp3-server.backup" ]]; then
        install -d -m 0755 "$PREFIX"
        cp -f "$generation/smp3-server.backup" "$BIN"
    else
        rm -f "$BIN"
    fi
    if [[ "$had_config" == YES && -f "$generation/config.backup" ]]; then
        install -d -m 0755 "$ETC_DIR"
        cp -f "$generation/config.backup" "$CONFIG_PATH"
        chmod 0600 "$CONFIG_PATH"
    elif [[ "$had_config" == NO ]]; then
        rm -f "$CONFIG_PATH"
    fi
    if [[ "$had_unit" == YES && -f "$generation/service.unit.backup" ]]; then
        cp -f "$generation/service.unit.backup" "$UNIT"
    elif [[ "$had_unit" == NO ]]; then
        rm -f "$UNIT"
    fi
    systemctl daemon-reload >/dev/null 2>&1 || true
    if [[ "$was_enabled" == YES ]]; then systemctl enable smp3-standalone >/dev/null 2>&1 || true; else systemctl disable smp3-standalone >/dev/null 2>&1 || true; fi
    if [[ "$was_active" == YES ]]; then systemctl start smp3-standalone >/dev/null 2>&1 || true; fi
    rm -rf "$generation"
}

do_install_or_update() {
    require_root
    ensure_config
    install -d -m 0755 "$PREFIX" "$ETC_DIR" "$BACKUPS"

    local release_json tag release_version asset_url sums_url
    release_json="$(fetch_release)"
    tag="$(release_value "$release_json")"
    release_version="${tag#v}"
    asset_url="$(printf '%s' "$release_json" | sed -n "s/.*\"browser_download_url\"[[:space:]]*:[[:space:]]*\"\([^\"]*\/$ASSET_NAME\)\".*/\1/p" | head -n 1)"
    sums_url="$(printf '%s' "$release_json" | sed -n 's/.*"browser_download_url"[[:space:]]*:[[:space:]]*"\([^"]*\/SHA256SUMS\)".*/\1/p' | head -n 1)"
    [[ -n "$asset_url" || -n "${SMP3_INSTALLER_TEST_ASSET_DIR:-}" ]] || die "STOP: RELEASE_ASSET_NOT_FOUND — exact $ASSET_NAME asset"
    [[ -n "$sums_url" || -n "${SMP3_INSTALLER_TEST_ASSET_DIR:-}" ]] || die "STOP: RELEASE_ASSET_NOT_FOUND — exact SHA256SUMS asset"

    local candidate sums expected actual old_sha old_version had_binary had_config had_unit was_active was_enabled generation config_changed
    candidate="$PREFIX/.smp3-server.$$.download"
    sums="$PREFIX/.SHA256SUMS.$$.download"
    if [[ -n "${SMP3_INSTALLER_TEST_ASSET_DIR:-}" ]]; then
        download_asset "$tag" "$ASSET_NAME" "$candidate"
        download_asset "$tag" "SHA256SUMS" "$sums"
    else
        curl -fsSL --retry 3 --retry-all-errors --retry-delay 1 -H 'User-Agent: smp3-installer' -o "$candidate" "$asset_url" || die "STOP: RELEASE_DOWNLOAD_FAILED — $ASSET_NAME"
        curl -fsSL --retry 3 --retry-all-errors --retry-delay 1 -H 'User-Agent: smp3-installer' -o "$sums" "$sums_url" || die "STOP: RELEASE_DOWNLOAD_FAILED — SHA256SUMS"
    fi
    expected="$(manifest_sha "$sums" "$ASSET_NAME")"
    actual="$(sha256 "$candidate")"
    [[ "$actual" == "$expected" ]] || { rm -f "$candidate" "$sums"; die "STOP: CHECKSUM_MISMATCH — $ASSET_NAME"; }
    chmod 0755 "$candidate"
    local reported_version
    reported_version="$(binary_version "$candidate")"
    [[ -n "$reported_version" ]] || { rm -f "$candidate" "$sums"; die "STOP: VERSION_CHECK_FAILED — downloaded server did not report a version"; }
    "$candidate" -c "$CONFIG_SOURCE" -check >/dev/null 2>&1 || { rm -f "$candidate" "$sums"; die "STOP: CONFIG_CHECK_FAILED — supplied config was rejected by the selected server"; }
    rm -f "$sums"

    old_sha=''; old_version=''; had_binary=NO; had_config=NO; had_unit=NO; was_active=NO; was_enabled=NO; config_changed=NO
    if [[ -x "$BIN" ]]; then
        had_binary=YES; old_sha="$(sha256 "$BIN")"; old_version="$(binary_version "$BIN")"
        if [[ "$old_sha" == "$actual" ]]; then
            rm -f "$candidate"
            echo "ALREADY UP TO DATE: $release_version"
            return 0
        fi
    fi
    [[ -f "$CONFIG_PATH" ]] && had_config=YES
    [[ -f "$UNIT" ]] && had_unit=YES
    service_active && was_active=YES || true
    service_enabled && was_enabled=YES || true
    if [[ "$CONFIG_SOURCE" != "$CONFIG_PATH" ]]; then config_changed=YES; fi

    generation="$BACKUPS/$(date -u +%Y%m%dT%H%M%SZ)-$$"
    mkdir -p "$generation"
    if [[ "$had_binary" == YES ]]; then
        cp -f "$BIN" "$generation/smp3-server.backup"
        [[ "$(sha256 "$generation/smp3-server.backup")" == "$old_sha" ]] || die "STOP: BACKUP_HASH_MISMATCH — old binary backup"
    fi
    if [[ "$had_config" == YES && "$config_changed" == YES ]]; then
        cp -f "$CONFIG_PATH" "$generation/config.backup"
        chmod 0600 "$generation/config.backup"
    fi
    if [[ "$had_unit" == YES ]]; then cp -f "$UNIT" "$generation/service.unit.backup"; fi

    systemctl stop smp3-standalone >/dev/null 2>&1 || true
    if [[ "$had_binary" == YES ]]; then mv -f "$BIN" "$generation/smp3-server.backup"; fi
    if ! mv -f "$candidate" "$BIN"; then
        rollback_transaction "$generation" "$had_binary" "$had_config" "$had_unit" "$was_active" "$was_enabled"
        die "UPDATE FAILED — ROLLED BACK"
    fi
    chmod 0755 "$BIN"
    if [[ "$config_changed" == YES ]]; then
        install -m 0600 "$CONFIG_SOURCE" "$CONFIG_PATH.new"
        if ! mv -f "$CONFIG_PATH.new" "$CONFIG_PATH"; then
            rollback_transaction "$generation" "$had_binary" "$had_config" "$had_unit" "$was_active" "$was_enabled"
            die "UPDATE FAILED — ROLLED BACK"
        fi
    fi
    write_unit
    systemctl daemon-reload
    if [[ "$had_binary" == NO || "$was_enabled" == YES ]]; then
        systemctl enable smp3-standalone >/dev/null
    else
        systemctl disable smp3-standalone >/dev/null 2>&1 || true
    fi
    if [[ "$NO_START" -eq 0 ]]; then
        if ! systemctl start smp3-standalone >/dev/null 2>&1 || ! service_active; then
            rollback_transaction "$generation" "$had_binary" "$had_config" "$had_unit" "$was_active" "$was_enabled"
            die "UPDATE FAILED — ROLLED BACK"
        fi
    fi
    if ! "$BIN" -c "$CONFIG_PATH" -check >/dev/null 2>&1; then
        rollback_transaction "$generation" "$had_binary" "$had_config" "$had_unit" "$was_active" "$was_enabled"
        die "UPDATE FAILED — ROLLED BACK"
    fi
    write_ctl
    install -m 0755 "$0" "$PREFIX/install-smp3-server.sh.new"
    mv -f "$PREFIX/install-smp3-server.sh.new" "$PREFIX/install-smp3-server.sh"
    write_state "$generation" "$old_sha" "$actual" "$release_version" "$config_changed" "$was_active" "$was_enabled"
    chown root:root "$BIN" "$CONFIG_PATH" "$STATE" 2>/dev/null || true
    rotate_backups
    echo "SMP3 STANDALONE SERVER: INSTALLED"
    echo "Version: $release_version"
    echo "SHA256: $actual"
    if [[ "$NO_START" -eq 1 ]]; then echo 'Service: not started (--no-start)'; else echo 'Service: active'; fi
}

do_rollback() {
    require_root
    [[ -f "$BIN" ]] || die "STOP: BINARY_NOT_FOUND — no installed standalone binary"
    local generation backup expected actual was_active was_enabled current_backup
    backup="$(read_state_field backup_path || true)"
    [[ -n "$backup" && -f "$backup" ]] || die "STOP: BACKUP_NOT_FOUND — install state has no verified binary backup"
    generation="$(dirname "$backup")"
    expected="$(read_state_field original_sha256 || true)"
    [[ -n "$expected" ]] || die "STOP: BACKUP_NOT_FOUND — install state is incomplete"
    actual="$(sha256 "$backup")"
    [[ "$actual" == "$expected" ]] || die "STOP: BACKUP_HASH_MISMATCH — verified backup does not match state"
    was_active=NO; was_enabled=NO
    service_active && was_active=YES || true
    service_enabled && was_enabled=YES || true
    current_backup="$generation/smp3-server.current.$$.bak"
    cp -f "$BIN" "$current_backup"
    systemctl stop smp3-standalone >/dev/null 2>&1 || true
    cp -f "$backup" "$BIN.new"
    chmod 0755 "$BIN.new"
    mv -f "$BIN.new" "$BIN"
    "$BIN" -c "$CONFIG_PATH" -check >/dev/null 2>&1 || { cp -f "$current_backup" "$BIN"; rm -f "$current_backup"; die "ROLLBACK FAILED — current binary restored"; }
    if [[ "$was_enabled" == YES ]]; then systemctl enable smp3-standalone >/dev/null; else systemctl disable smp3-standalone >/dev/null 2>&1 || true; fi
    if [[ "$was_active" == YES ]]; then systemctl start smp3-standalone >/dev/null; fi
    rm -f "$current_backup"
    echo 'ROLLBACK STATUS=RESTORED'
    echo "Restored SHA256: $expected"
}

do_uninstall() {
    require_root
    systemctl disable --now smp3-standalone >/dev/null 2>&1 || true
    rm -f "$UNIT" "$CTL"
    rm -f "$BIN" "$PREFIX/install-smp3-server.sh" "$STATE"
    rm -rf "$BACKUPS"
    if [[ "$PURGE" -eq 1 ]]; then
        rm -rf "$ETC_DIR" "$PREFIX"
        echo 'Config: purged (--purge)'
    else
        echo "Config preserved: $CONFIG_PATH"
        rmdir "$ETC_DIR" 2>/dev/null || true
        rmdir "$PREFIX" 2>/dev/null || true
    fi
    systemctl daemon-reload >/dev/null 2>&1 || true
    echo 'UNINSTALL STATUS=REMOVED'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --help|-h) usage; exit 0 ;;
        --install) ACTION=install ;;
        --check) ACTION=check ;;
        --update) ACTION=update ;;
        --rollback) ACTION=rollback ;;
        --uninstall) ACTION=uninstall ;;
        --config) [[ $# -ge 2 ]] || die 'STOP: INVALID_ARGUMENTS — --config requires a path'; CONFIG_ARG="$2"; shift ;;
        --version) [[ $# -ge 2 ]] || die 'STOP: INVALID_ARGUMENTS — --version requires a value'; REQUESTED_VERSION="$2"; shift ;;
        --no-start) NO_START=1 ;;
        --purge) PURGE=1 ;;
        *) die "STOP: INVALID_ARGUMENTS — unknown option: $1" ;;
    esac
    shift
done

if [[ "$ACTION" == check ]]; then
    require_platform
    do_check
elif [[ "$ACTION" == rollback ]]; then
    require_platform
    do_rollback
elif [[ "$ACTION" == uninstall ]]; then
    require_platform
    do_uninstall
else
    require_platform
    do_install_or_update
fi
