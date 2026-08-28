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

echo '[+] standalone SMP3 reliability tests'
(
  cd "$ROOT/src/protocol/multipath"
  go test core.go protocol.go adaptive.go datagram.go core_test.go adaptive_test.go protocol_test.go datagram_test.go -race -count=5
  go vet core.go protocol.go adaptive.go datagram.go core_test.go adaptive_test.go protocol_test.go datagram_test.go
)

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
