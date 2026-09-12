#!/usr/bin/env bash
set -euo pipefail

# Backward-compatible all-product build entrypoint. The canonical `build.sh`
# is Standalone-only; this legacy name explicitly opts into all three product
# workflows and never changes their ownership boundaries.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
"$ROOT/scripts/build-standalone.sh" "$@"
"$ROOT/scripts/build-native.sh" "$@"
"$ROOT/scripts/build-compatibility.sh" "$@"

OUT="${OUT:-$ROOT/dist}"
cat "$OUT/STANDALONE_SHA256SUMS" "$OUT/NATIVE_SHA256SUMS" "$OUT/COMPATIBILITY_SHA256SUMS" > "$OUT/SHA256SUMS"
(
  cd "$OUT"
  sha256sum -c SHA256SUMS
)
echo "[all-products] Standalone, Native, and Compatibility artifacts ready in $OUT"
