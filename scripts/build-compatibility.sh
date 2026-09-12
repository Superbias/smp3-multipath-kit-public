#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${OUT:-$ROOT/dist}"
WORK="${WORK:-$ROOT/.work/compatibility-build}"
SING_ROOT="${SING_ROOT:-$WORK/sing-box}"
SING_TAG="v1.14.0-beta.14"
SING_REV="4902660f8424fef3c2a60dfcdce7aeadfe3f3b88"
RELEASE_VERSION="$(awk -F= '$1 == "kit_version" { print $2; exit }' "$ROOT/VERSION")"
test -n "$RELEASE_VERSION" || { echo "missing kit_version in $ROOT/VERSION" >&2; exit 2; }
RUNTIME_SING_VERSION="1.14.0-beta.14-smp3-${RELEASE_VERSION}"

command -v go >/dev/null || { echo 'missing go' >&2; exit 2; }
command -v git >/dev/null || { echo 'missing git' >&2; exit 2; }
mkdir -p "$OUT" "$WORK"
source "$ROOT/scripts/build-external-common.sh"
PYTHON_BIN="$(r18_python_bin)"
r18_prepare_checkout "$SING_ROOT" https://github.com/SagerNet/sing-box.git "$SING_TAG" "$SING_REV"
"$PYTHON_BIN" "$ROOT/scripts/apply_source.py" "$SING_ROOT" "$WORK/sing-source-work"

SING_TAGS="$(tr -d '\r' < "$SING_ROOT/release/DEFAULT_BUILD_TAGS_OTHERS")"
# The pinned sing-box release lists old tfo-go linkname compatibility tags.
# Go 1.25 removed the referenced net internals, so omit only those tags.
SING_TAGS="${SING_TAGS//,badlinkname/}"
SING_TAGS="${SING_TAGS//,tfogo_checklinkname0/}"
SING_LDFLAGS_SHARED="$(tr -d '\r' < "$SING_ROOT/release/LDFLAGS")"
SING_LDFLAGS="-X github.com/sagernet/sing-box/constant.Version=$RUNTIME_SING_VERSION $SING_LDFLAGS_SHARED -s -w -buildid="
(
  cd "$SING_ROOT"
  GOWORK=off go test -tags "$SING_TAGS" ./protocol/multipath
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOWORK=off \
    go build -trimpath -tags "$SING_TAGS" -ldflags "$SING_LDFLAGS" \
    -o "$OUT/smp3-proxy-linux-amd64" ./cmd/sing-box
  CGO_ENABLED=0 GOOS=windows GOARCH=amd64 GOWORK=off \
    go build -trimpath -tags "$SING_TAGS" -ldflags "$SING_LDFLAGS" \
    -o "$OUT/smp3-proxy-windows-amd64.exe" ./cmd/sing-box
)

CHECKER="$WORK/check-binary-target"
EVIDENCE="$OUT/COMPATIBILITY_ARTIFACTS.jsonl"
: > "$EVIDENCE"
r18_build_checker "$ROOT" "$CHECKER"
r18_check_artifact "$CHECKER" linux/amd64 "$OUT/smp3-proxy-linux-amd64" "$EVIDENCE" >/dev/null
r18_check_artifact "$CHECKER" windows/amd64 "$OUT/smp3-proxy-windows-amd64.exe" "$EVIDENCE" >/dev/null
r18_write_manifest "$OUT" "$OUT/COMPATIBILITY_SHA256SUMS" \
  smp3-proxy-linux-amd64 smp3-proxy-windows-amd64.exe
echo "[compatibility] artifacts ready in $OUT"
