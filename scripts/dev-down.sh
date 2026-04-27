#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--wipe" ]]; then
  docker compose down -v
  echo "Stopped postgres and removed data volume."
else
  docker compose down
  echo "Stopped postgres (volume preserved). Use './scripts/dev-down.sh --wipe' to remove data."
fi
