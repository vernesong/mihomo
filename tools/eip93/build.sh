#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OVERLAY_DIR="${ROOT_DIR}/.eip93-overlay"
PYTHON="${PYTHON:-python3}"
GO="${GO:-go}"

echo "Generating AN7581/AN7583 EIP93 Go overlay..."
"${PYTHON}" "${SCRIPT_DIR}/overlay.py" --go "${GO}" --out "${OVERLAY_DIR}"

echo "Building Mihomo linux-arm64 with EIP93 overlay..."
cd "${ROOT_DIR}"
mkdir -p bin
GOARCH=arm64 GOOS=linux CGO_ENABLED=0 "${GO}" build \
  -overlay="${OVERLAY_DIR}/overlay.json" \
  -tags "with_gvisor" \
  -trimpath \
  -ldflags "-w -s -buildid=" \
  -o bin/mihomo-linux-arm64 "$@"

echo "Successfully built bin/mihomo-linux-arm64 (with AN7581/AN7583 EIP93 acceleration)"
