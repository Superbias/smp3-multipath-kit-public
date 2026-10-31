#!/usr/bin/env bash
set -euo pipefail

bundle_root() { cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd; }
require_root() { [[ ${EUID:-$(id -u)} -eq 0 ]] || { echo "run with sudo" >&2; exit 1; }; }
require_linux() { [[ $(uname -s) == Linux ]] || { echo "Linux is required" >&2; exit 1; }; }
linux_arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo linux-amd64 ;;
    aarch64|arm64) echo linux-arm64 ;;
    *) echo "unsupported Linux architecture: $(uname -m)" >&2; exit 1 ;;
  esac
}
verify_bundle_file() {
  local root="$1" rel="$2"
  [[ -f "$root/$rel" ]] || { echo "missing bundle file: $rel" >&2; exit 1; }
  (cd "$root" && grep -F "  $rel" manifest/SHA256SUMS | sha256sum -c --status -) || {
    echo "checksum missing or invalid: $rel" >&2; exit 1;
  }
}
ensure_smp3_user() {
  getent group smp3 >/dev/null || groupadd --system smp3
  id -u smp3 >/dev/null 2>&1 || useradd --system --gid smp3 --home-dir /var/lib/smp3 --create-home --shell /usr/sbin/nologin smp3
  install -d -o smp3 -g smp3 -m 0750 /var/lib/smp3
  install -d -o root -g smp3 -m 0750 /etc/smp3
}
backup_existing() {
  local path="$1" backup_dir="$2"
  if [[ -e "$path" ]]; then
    install -d -m 0700 "$backup_dir"
    cp -a -- "$path" "$backup_dir/$(basename "$path")"
  fi
}
install_config_if_missing() {
  local source="$1" target="$2"
  if [[ -e "$target" ]]; then
    echo "preserving existing configuration: $target"
    return
  fi
  install -o root -g smp3 -m 0640 "$source" "$target"
}
