#!/usr/bin/env bash
# Run sqlx-cli migrations against the poc_v3 database.
#
# Prereq: bash poc/ledger-v3/scripts/create-poc-v3-db.sh has been run.
#
# Idempotent: sqlx-cli skips migrations that are already applied (tracked in
# the _sqlx_migrations table).
#
# Usage: bash poc/ledger-v3/scripts/run-migrations.sh

set -euo pipefail

CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${DATABASE_URL:=postgres://acct:acct_dev@localhost:5111/poc_v3}"
export DATABASE_URL

cd "$CRATE_DIR"
exec sqlx migrate run --source db/migrations
