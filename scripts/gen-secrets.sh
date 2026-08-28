#!/usr/bin/env bash
set -euo pipefail
command -v openssl >/dev/null || { echo 'openssl is required'; exit 1; }
echo "SMP3_PASSWORD=$(openssl rand -hex 32)"
echo "PUBLIC_SNELL_PSK=$(openssl rand -base64 24 | tr -d '\n')"
