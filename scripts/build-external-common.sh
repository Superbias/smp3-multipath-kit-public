#!/usr/bin/env bash
set -euo pipefail

r18_python_bin() {
  local candidate="${PYTHON_BIN:-python3}"
  if command -v "$candidate" >/dev/null 2>&1 && "$candidate" --version >/dev/null 2>&1; then
    printf '%s' "$candidate"
    return 0
  fi
  candidate=python
  command -v "$candidate" >/dev/null 2>&1 || {
    echo 'missing Python 3' >&2
    return 1
  }
  printf '%s' "$candidate"
}

r18_prepare_checkout() {
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

r18_build_checker() {
  local root="$1" output="$2"
  mkdir -p "$(dirname "$output")"
  (
    cd "$root/tools/check-binary-target"
    CGO_ENABLED=0 GOWORK=off go build -trimpath -o "$output" .
  )
}

r18_check_artifact() {
  local checker="$1" target="$2" path="$3" evidence="$4"
  test -f "$path" || { echo "missing artifact: $path" >&2; exit 1; }
  "$checker" -target "$target" -file "$path" | tee -a "$evidence"
}

r18_write_manifest() {
  local output="$1" manifest="$2"
  shift 2
  (
    cd "$output"
    sha256sum "$@" > "$manifest"
    sha256sum -c "$(basename "$manifest")"
  )
}
