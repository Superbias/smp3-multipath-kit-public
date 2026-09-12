#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${OUT:-$ROOT/dist}"
WORK="${WORK:-$ROOT/.work/native-build}"
MIHOMO_ROOT="${MIHOMO_ROOT:-$WORK/mihomo}"
MIHOMO_TAG="v1.19.28"
MIHOMO_REV="cbd11db1e13a75d8e680e0fe7742c95be4cba2be"
RELEASE_VERSION="$(awk -F= '$1 == "kit_version" { print $2; exit }' "$ROOT/VERSION")"
test -n "$RELEASE_VERSION" || { echo "missing kit_version in $ROOT/VERSION" >&2; exit 2; }

command -v go >/dev/null || { echo 'missing go' >&2; exit 2; }
command -v git >/dev/null || { echo 'missing git' >&2; exit 2; }
mkdir -p "$OUT" "$WORK"
source "$ROOT/scripts/build-external-common.sh"
PYTHON_BIN="$(r18_python_bin)"
r18_prepare_checkout "$MIHOMO_ROOT" https://github.com/MetaCubeX/mihomo.git "$MIHOMO_TAG" "$MIHOMO_REV"
"$PYTHON_BIN" "$ROOT/scripts/apply_mihomo_adapter.py" "$MIHOMO_ROOT" "$ROOT"

(
  cd "$MIHOMO_ROOT"
  GOWORK=off go test -mod=mod ./adapter/... ./config/...
  GOWORK=off go build -mod=mod .
  MIHOMO_LDFLAGS="-X github.com/metacubex/mihomo/constant.Version=$RELEASE_VERSION -buildid="
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOWORK=off \
    go build -trimpath -mod=mod -ldflags "$MIHOMO_LDFLAGS" -o "$OUT/mihomo-smp3-linux-amd64" .
  CGO_ENABLED=0 GOOS=windows GOARCH=amd64 GOWORK=off \
    go build -trimpath -mod=mod -ldflags "$MIHOMO_LDFLAGS" -o "$OUT/mihomo-smp3-windows-amd64.exe" .
)

CHECKER="$WORK/check-binary-target"
EVIDENCE="$OUT/NATIVE_ARTIFACTS.jsonl"
: > "$EVIDENCE"
r18_build_checker "$ROOT" "$CHECKER"
r18_check_artifact "$CHECKER" linux/amd64 "$OUT/mihomo-smp3-linux-amd64" "$EVIDENCE" >/dev/null
r18_check_artifact "$CHECKER" windows/amd64 "$OUT/mihomo-smp3-windows-amd64.exe" "$EVIDENCE" >/dev/null
r18_write_manifest "$OUT" "$OUT/NATIVE_SHA256SUMS" \
  mihomo-smp3-linux-amd64 mihomo-smp3-windows-amd64.exe
echo "[native] artifacts ready in $OUT"
