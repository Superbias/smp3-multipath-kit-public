#!/usr/bin/env bash
set -euo pipefail
base="${SMP3_TELEMETRY_URL:-http://127.0.0.1:24500}"
for endpoint in api/v1/status api/v1/legs api/v1/sessions api/v1/traffic; do
  printf '\n== %s ==\n' "$endpoint"
  curl --fail --silent --show-error "$base/$endpoint"
  printf '\n'
done
