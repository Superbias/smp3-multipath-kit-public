#!/usr/bin/env bash
set -euo pipefail
# Windows checkout workaround: normalize only isolated build-script copies.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STAGE="$ROOT/.work/slow-leg-native-source"
mkdir -p "$STAGE/scripts"
for name in build-native.sh build-external-common.sh apply_mihomo_adapter.py; do
  tr -d '\r' < "$ROOT/scripts/$name" > "$STAGE/scripts/$name"
done
for name in core adapters tools VERSION; do
  ln -sfn "../../$name" "$STAGE/$name"
done
WORK="$ROOT/.work/native-build" OUT="${SMP3_NATIVE_AUDIT_OUT:-$ROOT/artifacts/slow-leg/native}" bash "$STAGE/scripts/build-native.sh"
