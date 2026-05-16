#!/usr/bin/env bash
# Run M1.1+ sqlx-cli migrations against acct_poc_queue_v21.
#
# Prereq: bash scripts/create-poc-v21-db.sh has been run.
#
# Usage: bash poc/queue-extension-v21/scripts/run-migrations.sh

set -euo pipefail

CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${DATABASE_URL:=postgres://acct:acct_dev@localhost:5111/acct_poc_queue_v21}"
export DATABASE_URL

cd "$CRATE_DIR"
exec sqlx migrate run --source db/migrations
