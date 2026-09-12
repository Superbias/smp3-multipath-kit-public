#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
command -v go >/dev/null || { echo "missing go"; exit 1; }
command -v python3 >/dev/null || { echo "missing python3"; exit 1; }

echo '[+] JSON syntax'
python3 -m json.tool "$ROOT/config/client.example.json" >/dev/null
python3 -m json.tool "$ROOT/config/client-legacy.example.json" >/dev/null
python3 -m json.tool "$ROOT/config/client-adaptive.example.json" >/dev/null
python3 -m json.tool "$ROOT/config/server.example.json" >/dev/null
python3 -m json.tool "$ROOT/config/server-hy2-snell.example.json" >/dev/null
python3 -m json.tool "$ROOT/config/standalone-server.example.json" >/dev/null
python3 -m json.tool "$ROOT/examples/smp3-client-config.example.json" >/dev/null
python3 -m json.tool "$ROOT/examples/smp3-server-config.example.json" >/dev/null

echo '[+] Test* count'
python3 "$ROOT/scripts/check-test-count.py" "$ROOT"

echo '[+] Core import purity'
python3 "$ROOT/scripts/check-core-imports.py" "$ROOT"

echo '[+] Core migration parity'
python3 "$ROOT/scripts/check-core-migration-parity.py" "$ROOT"

echo '[+] canonical Core standalone tests'
( cd "$ROOT/core" && GOTOOLCHAIN=local go test ./... && GOTOOLCHAIN=local go vet ./... )

echo '[+] standalone SMP3 reliability tests'
python3 "$ROOT/scripts/run-standalone-go.py" "$ROOT" test -race -count=5
python3 "$ROOT/scripts/run-standalone-go.py" "$ROOT" vet

echo '[+] standalone sidecar client tests'
( cd "$ROOT/client" && GOTOOLCHAIN=local go test ./... && GOTOOLCHAIN=local go test ./... -race && GOTOOLCHAIN=local go vet ./... )
( cd "$ROOT/cmd/smp3-client" && GOTOOLCHAIN=local go test ./... && GOTOOLCHAIN=local go vet ./... )

echo '[+] source injector syntax'
python3 - "$ROOT/scripts/apply_source.py" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1])
compile(path.read_text(), str(path), 'exec')
PY

echo '[+] shell syntax'
bash -n "$ROOT/build.sh" "$ROOT/install-server.sh" "$ROOT/validate-kit.sh" "$ROOT/package-release.sh"

echo '[+] installer tests'
bash "$ROOT/scripts/tests/test-linux-installer.sh"
if command -v powershell.exe >/dev/null 2>&1 && command -v wslpath >/dev/null 2>&1; then
  powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "$(wslpath -w "$ROOT/scripts/tests/test-mihomo-installer.ps1")"
else
  echo '[=] Windows installer test skipped: powershell.exe/wslpath unavailable'
fi

echo '[+] kit validation passed'
