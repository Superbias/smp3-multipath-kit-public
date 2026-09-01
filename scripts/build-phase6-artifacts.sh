#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="2.0.0"
SING_TAG="v1.14.0-beta.14"
SING_REV="4902660f8424fef3c2a60dfcdce7aeadfe3f3b88"
MIHOMO_TAG="v1.19.28"
MIHOMO_REV="cbd11db1e13a75d8e680e0fe7742c95be4cba2be"
WORK="${WORK:-$ROOT/.work/phase6e-build}"
OUT="${OUT:-$ROOT/dist}"
SING_ROOT="${SING_ROOT:-$WORK/sing-box}"
MIHOMO_ROOT="${MIHOMO_ROOT:-$WORK/mihomo}"
CHECKER_ROOT="$ROOT/tools/check-binary-target"
CHECKER="$WORK/check-binary-target"
MANIFEST="$OUT/ARTIFACTS_SHA256"

command -v go >/dev/null || { echo 'missing go' >&2; exit 2; }
command -v python3 >/dev/null || { echo 'missing python3' >&2; exit 2; }
command -v git >/dev/null || { echo 'missing git' >&2; exit 2; }
export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.25.5+auto}"
mkdir -p "$WORK" "$OUT"
cd "$ROOT"

prepare_checkout() {
  local path="$1" url="$2" tag="$3" revision="$4"
  if [ ! -d "$path/.git" ]; then
    git clone --filter=blob:none --no-checkout --branch "$tag" "$url" "$path"
  fi
  (
    cd "$path"
    if ! git cat-file -e "$revision^{commit}" 2>/dev/null; then
      git -c http.version=HTTP/1.1 -c http.maxRequests=1 fetch --no-tags --filter=blob:none origin "$revision"
    fi
    git checkout --detach "$revision"
    [ "$(git rev-parse HEAD)" = "$revision" ] || {
      echo "pinned revision mismatch in $path" >&2
      exit 1
    }
  )
}

build_module_target() {
  local module_dir="$1" goos="$2" goarch="$3" output="$4" package_path="$5"
  echo "[+] build target=$goos/$goarch output=$output"
  (
    cd "$module_dir"
    CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" GOWORK=off \
      go build -trimpath -o "$output" "$package_path"
  )
}

build_workspace_target() {
  local goos="$1" goarch="$2" output="$3" package_path="$4"
  echo "[+] build target=$goos/$goarch output=$output"
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" GOWORK="$ROOT/go.work" \
    go build -trimpath -o "$output" "$package_path"
}

prepare_checkout "$SING_ROOT" https://github.com/SagerNet/sing-box.git "$SING_TAG" "$SING_REV"
prepare_checkout "$MIHOMO_ROOT" https://github.com/MetaCubeX/mihomo.git "$MIHOMO_TAG" "$MIHOMO_REV"

echo '[+] building content/architecture checker'
(
  cd "$CHECKER_ROOT"
  CGO_ENABLED=0 GOOS="$(go env GOOS)" GOARCH="$(go env GOARCH)" GOWORK=off \
    go build -trimpath -o "$CHECKER" .
)

echo '[+] building standalone server targets'
build_workspace_target linux amd64 "$OUT/smp3-server-linux-amd64" ./cmd/smp3-server
build_workspace_target windows amd64 "$OUT/smp3-server-windows-amd64.exe" ./cmd/smp3-server

echo '[+] injecting and building pinned sing targets'
python3 "$ROOT/scripts/apply_source.py" "$SING_ROOT" "$WORK/sing-source-work"
SING_TAGS="$(cat "$SING_ROOT/release/DEFAULT_BUILD_TAGS_OTHERS")"
SING_LDFLAGS_SHARED="$(cat "$SING_ROOT/release/LDFLAGS")"
SING_LDFLAGS="-X github.com/sagernet/sing-box/constant.Version=1.14.0-beta.14-smp3-$VERSION $SING_LDFLAGS_SHARED -s -w -buildid="
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

echo '[+] injecting and building pinned Mihomo targets'
python3 "$ROOT/scripts/apply_mihomo_adapter.py" "$MIHOMO_ROOT" "$ROOT"
(
  cd "$MIHOMO_ROOT"
  GOWORK=off go test -mod=mod ./adapter/... ./config/...
  GOWORK=off go build -mod=mod .
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOWORK=off \
    go build -trimpath -mod=mod -o "$OUT/mihomo-smp3-linux-amd64" .
  CGO_ENABLED=0 GOOS=windows GOARCH=amd64 GOWORK=off \
    go build -trimpath -mod=mod -o "$OUT/mihomo-smp3-windows-amd64.exe" .
)

echo '[+] fail-closed artifact verification'
: > "$MANIFEST"
check_artifact() {
  local target="$1" path="$2"
  test -f "$path" || { echo "missing artifact: $path" >&2; exit 1; }
  "$CHECKER" -target "$target" -file "$path" | tee -a "$MANIFEST"
}
check_artifact linux/amd64 "$OUT/smp3-server-linux-amd64"
check_artifact windows/amd64 "$OUT/smp3-server-windows-amd64.exe"
check_artifact linux/amd64 "$OUT/mihomo-smp3-linux-amd64"
check_artifact windows/amd64 "$OUT/mihomo-smp3-windows-amd64.exe"
check_artifact linux/amd64 "$OUT/smp3-proxy-linux-amd64"
check_artifact windows/amd64 "$OUT/smp3-proxy-windows-amd64.exe"

(
  cd "$OUT"
  sha256sum \
    smp3-server-linux-amd64 smp3-server-windows-amd64.exe \
    mihomo-smp3-linux-amd64 mihomo-smp3-windows-amd64.exe \
    smp3-proxy-linux-amd64 smp3-proxy-windows-amd64.exe > SHA256SUMS
  sha256sum -c SHA256SUMS
)
echo "[+] formal artifacts and checksums ready in $OUT"
