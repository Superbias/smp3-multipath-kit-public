#!/usr/bin/env bash
set -euo pipefail
BASE_REV="4902660f8424fef3c2a60dfcdce7aeadfe3f3b88"
BASE_LABEL="sing-box v1.14.0-beta.14"
ROOT="$(cd "$(dirname "$0")" && pwd)"
WORK="${WORK:-$ROOT/.work}"
SRC="$WORK/sing-box"
OUT="$ROOT/dist"
mkdir -p "$WORK" "$OUT"
command -v git >/dev/null || { echo 'missing git'; exit 1; }
command -v go >/dev/null || { echo 'missing go (Go 1.21+ bootstrap required; pinned sing-box needs Go 1.25.5)'; exit 1; }
command -v python3 >/dev/null || { echo 'missing python3'; exit 1; }

# The pinned sing-box revision declares `go 1.25.5`. Go 1.21+ can bootstrap
# the exact toolchain automatically through GOTOOLCHAIN when network access is
# available. This avoids accidentally compiling the fixed source base with a
# materially different local compiler.
export GOTOOLCHAIN=go1.25.5+auto
echo "[+] Go toolchain target: $GOTOOLCHAIN"

# These reliability tests exercise the transport core without needing the
# sing-box repository or external Go modules.
echo '[+] running standalone SMP3 core reliability tests'
(
  cd "$ROOT/src/protocol/multipath"
  go test core.go protocol.go adaptive.go core_test.go adaptive_test.go protocol_test.go -v
)

if [ ! -d "$SRC/.git" ]; then
  git clone https://github.com/SagerNet/sing-box.git "$SRC"
fi
cd "$SRC"
if ! git cat-file -e "$BASE_REV^{commit}" 2>/dev/null; then
  echo "[+] fetching pinned revision only (HTTP/1.1, no tag enumeration)"
  git -c http.version=HTTP/1.1 -c http.maxRequests=1 fetch --no-tags --filter=blob:none origin "$BASE_REV"
fi
git reset --hard
git clean -fdx
git checkout --detach "$BASE_REV"
[ "$(git rev-parse HEAD)" = "$BASE_REV" ] || { echo 'base revision mismatch'; exit 1; }
echo "[+] fixed base: $BASE_LABEL ($BASE_REV)"
python3 "$ROOT/scripts/apply_source.py" "$SRC"
gofmt -w constant/proxy.go include/registry.go option/multipath.go protocol/multipath/*.go
# Use the project's own default non-Naive build tag set and mandatory shared
# linker flags. release/LDFLAGS contains flags such as -checklinkname=0 that
# official sing-box builds require on modern Go versions. We intentionally use
# DEFAULT_BUILD_TAGS_OTHERS for both targets to avoid pulling the optional
# Naive/cronet runtime into this small transport-only build.
TAGS="$(cat release/DEFAULT_BUILD_TAGS_OTHERS)"
LDFLAGS_SHARED="$(cat release/LDFLAGS)"
LDFLAGS="-X github.com/sagernet/sing-box/constant.Version=1.14.0-beta.14-smp3-alpha2.3-r10 $LDFLAGS_SHARED -s -w -buildid="

echo '[+] compiling/testing injected multipath package against pinned sing-box'
go test -tags "$TAGS" ./protocol/multipath

echo '[+] building linux/amd64'
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -tags "$TAGS" -ldflags "$LDFLAGS" -o "$OUT/smp3-proxy-linux-amd64" ./cmd/sing-box

echo '[+] building windows/amd64'
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -trimpath -tags "$TAGS" -ldflags "$LDFLAGS" -o "$OUT/smp3-proxy-windows-amd64.exe" ./cmd/sing-box

(
  cd "$OUT"
  sha256sum smp3-proxy-linux-amd64 smp3-proxy-windows-amd64.exe | tee SHA256SUMS
)
echo '[+] done:'
ls -lh "$OUT"
