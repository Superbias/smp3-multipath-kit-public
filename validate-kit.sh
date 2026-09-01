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

echo '[+] source injector syntax'
python3 - "$ROOT/scripts/apply_source.py" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1])
compile(path.read_text(), str(path), 'exec')
PY

echo '[+] shell syntax'
bash -n "$ROOT/build.sh" "$ROOT/install-server.sh" "$ROOT/validate-kit.sh" "$ROOT/package-release.sh"

echo '[+] kit validation passed'
