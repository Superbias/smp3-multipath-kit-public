#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${OUT:-$ROOT/dist}"
WORK="${WORK:-$ROOT/.work/standalone-build}"
RELEASE_VERSION="$(awk -F= '$1 == "kit_version" { print $2; exit }' "$ROOT/VERSION")"
test -n "$RELEASE_VERSION" || { echo "missing kit_version in $ROOT/VERSION" >&2; exit 2; }

command -v go >/dev/null || { echo 'missing go' >&2; exit 2; }
command -v sha256sum >/dev/null || { echo 'missing sha256sum' >&2; exit 2; }
export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.25.5+auto}"
mkdir -p "$OUT" "$WORK"
source "$ROOT/scripts/build-external-common.sh"
PYTHON_BIN="$(r18_python_bin)"

cd "$ROOT"
"$PYTHON_BIN" "$ROOT/scripts/check-standalone-dependencies.py" "$ROOT"

build_target() {
  local goos="$1" output="$2" package_path="$3" ldflags="$4"
  echo "[standalone] build target=$goos/amd64 output=$output"
  CGO_ENABLED=0 GOOS="$goos" GOARCH=amd64 GOWORK="$ROOT/go.work" \
    go build -trimpath -ldflags "$ldflags" -o "$output" "$package_path"
}

SERVER_LDFLAGS="-X github.com/Superbias/smp3-multipath-kit-public/server.Version=$RELEASE_VERSION -buildid="
CLIENT_LDFLAGS="-X github.com/Superbias/smp3-multipath-kit-public/client.Version=$RELEASE_VERSION -buildid="

build_target linux "$OUT/smp3-server-linux-amd64" ./cmd/smp3-server "$SERVER_LDFLAGS"
build_target windows "$OUT/smp3-server-windows-amd64.exe" ./cmd/smp3-server "$SERVER_LDFLAGS"
build_target linux "$OUT/smp3-client-linux-amd64" ./cmd/smp3-client "$CLIENT_LDFLAGS"
build_target windows "$OUT/smp3-client-windows-amd64.exe" ./cmd/smp3-client "$CLIENT_LDFLAGS"

CHECKER="$WORK/check-binary-target"
EVIDENCE="$OUT/STANDALONE_ARTIFACTS.jsonl"
: > "$EVIDENCE"
r18_build_checker "$ROOT" "$CHECKER"
r18_check_artifact "$CHECKER" linux/amd64 "$OUT/smp3-server-linux-amd64" "$EVIDENCE" >/dev/null
r18_check_artifact "$CHECKER" windows/amd64 "$OUT/smp3-server-windows-amd64.exe" "$EVIDENCE" >/dev/null
r18_check_artifact "$CHECKER" linux/amd64 "$OUT/smp3-client-linux-amd64" "$EVIDENCE" >/dev/null
r18_check_artifact "$CHECKER" windows/amd64 "$OUT/smp3-client-windows-amd64.exe" "$EVIDENCE" >/dev/null
r18_write_manifest "$OUT" "$OUT/STANDALONE_SHA256SUMS" \
  smp3-server-linux-amd64 smp3-server-windows-amd64.exe \
  smp3-client-linux-amd64 smp3-client-windows-amd64.exe
echo "[standalone] artifacts ready in $OUT"
