#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
VERSION="2.1.0"
NAME="smp3-multipath-kit-$VERSION"
STAGE_ROOT="${STAGE_ROOT:-$ROOT/.release-stage}"
STAGE="$STAGE_ROOT/$NAME"
OUT_ZIP="${1:-$ROOT/$NAME-source.zip}"

rm -rf "$STAGE"
mkdir -p "$STAGE" "$STAGE/config" "$STAGE/patches" "$STAGE/scripts" \
  "$STAGE/src" "$STAGE/core" "$STAGE/server" "$STAGE/cmd" "$STAGE/adapters" "$STAGE/examples" \
  "$STAGE/tools/check-binary-target" "$STAGE/dist"

for file in \
  README.md README-zh_CN.md README.zh-CN.md RELEASE_NOTES.md CHANGELOG.md \
  DEPLOYMENT.md DEPLOYMENT.zh-CN.md \
  BUILD_STATUS.md TEST_RESULTS.txt SECURITY.md NOTICE.md VERSION build.sh \
  validate-kit.sh install-server.sh install-client.ps1 package-release.sh \
  UPGRADE-alpha2-to-alpha2.1.md UPGRADE-alpha2.1-to-alpha2.2.md \
  go.work \
  CORE_SOURCE_MANIFEST_BEFORE CORE_SOURCE_MANIFEST_AFTER CORE_CANONICAL_SHA256SUMS \
  MIHOMO_ADAPTER_MAP.md; do
  [ -f "$ROOT/$file" ] && cp -a "$ROOT/$file" "$STAGE/$file"
done

cp -a "$ROOT/config/." "$STAGE/config/"
cp -a "$ROOT/patches/." "$STAGE/patches/"
cp -a "$ROOT/scripts/." "$STAGE/scripts/"
cp -a "$ROOT/src/." "$STAGE/src/"
cp -a "$ROOT/core/." "$STAGE/core/"
cp -a "$ROOT/server/." "$STAGE/server/"
cp -a "$ROOT/cmd/." "$STAGE/cmd/"
cp -a "$ROOT/adapters/." "$STAGE/adapters/"
cp -a "$ROOT/examples/." "$STAGE/examples/"
cp -a "$ROOT/tools/check-binary-target/." "$STAGE/tools/check-binary-target/"
cp -a "$ROOT/dist/BUILD_REQUIRED.md" "$STAGE/dist/"

# Keep this archive source-only. Release binaries and SHA256SUMS are separate
# assets; the repository may retain its formal dist artifacts independently.
find "$STAGE" -type d \( -name .git -o -name .work -o -name .release-stage \
  -o -name graphify-out -o -name __pycache__ \) -prune -exec rm -rf {} +
find "$STAGE" -type f \( -name '*.pyc' -o -name '*.pyo' -o -name '*.log' \
  -o -name '*.tmp' -o -name '*.bak' -o -name 'config.json' -o -name 'client.json' \
  -o -name 'server.json' -o -name '*.key' -o -name '*.pem' -o -name '*.p12' \
  -o -name '*.pfx' \) -delete

chmod 0755 "$STAGE/build.sh" "$STAGE/validate-kit.sh" "$STAGE/install-server.sh" \
  "$STAGE/package-release.sh"
find "$STAGE/scripts" -maxdepth 1 -type f -name '*.sh' -exec chmod 0755 {} +
find "$STAGE" -type f -name '*.json' -exec chmod 0644 {} +

python3 - "$STAGE" <<'PY'
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
secret_keys = {'password', 'psk', 'obfs_password', 'private_key', 'token', 'api_key'}
allowed_prefixes = ('YOUR_', 'CHANGE_')
problems = []

def walk(value, path):
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower() in secret_keys and isinstance(child, str) and child and not child.startswith(allowed_prefixes):
                problems.append(f'{path}: {key} is not a placeholder')
            walk(child, path)
    elif isinstance(value, list):
        for child in value:
            walk(child, path)

for path in root.rglob('*.json'):
    try:
        walk(json.loads(path.read_text(encoding='utf-8')), path.name)
    except json.JSONDecodeError as exc:
        problems.append(f'{path}: invalid JSON: {exc}')

patterns = (
    re.compile(r'-----BEGIN (?:RSA |EC |OPENSSH |PRIVATE )?PRIVATE KEY-----'),
    re.compile(r'/home/[^/\s]+/'),
    re.compile(r'C:\\\\Users\\[^\\\s]+\\'),
)
for path in root.rglob('*'):
    if not path.is_file() or path.name in {'package-release.sh', 'release-scan.py'}:
        continue
    text = path.read_text(encoding='utf-8', errors='ignore')
    for pattern in patterns:
        if pattern.search(text):
            problems.append(f'{path}: forbidden sensitive/path pattern {pattern.pattern}')
            break

if problems:
    raise SystemExit('\n'.join(problems))
PY

rm -f "$OUT_ZIP" "$OUT_ZIP.sha256"
if command -v zip >/dev/null 2>&1; then
  (cd "$STAGE_ROOT" && zip -q -r "$OUT_ZIP" "$NAME")
else
  python3 - "$STAGE_ROOT" "$NAME" "$OUT_ZIP" <<'PY'
from pathlib import Path
import sys
import zipfile

stage_root = Path(sys.argv[1])
name = sys.argv[2]
output = Path(sys.argv[3])
with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
    for path in sorted((stage_root / name).rglob('*')):
        if path.is_file():
            archive.write(path, path.relative_to(stage_root))
PY
fi

sha256sum "$OUT_ZIP" > "$OUT_ZIP.sha256"
unzip -t "$OUT_ZIP" >/dev/null
echo "[+] clean source archive: $OUT_ZIP"
echo "[+] sha256: $OUT_ZIP.sha256"
