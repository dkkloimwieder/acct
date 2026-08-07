#!/usr/bin/env bash
# Run sqlx-cli migrations against the poc_v3_2 database.
#
# Prereq: bash poc/ledger-v3.2/scripts/create-poc-v3-2-db.sh has been run.
#
# Idempotent: sqlx-cli skips migrations already applied (tracked in _sqlx_migrations).
#
# Usage: bash poc/ledger-v3.2/scripts/run-migrations.sh
#   Revert one step: sqlx migrate revert --source db/migrations (with DATABASE_URL set)

set -euo pipefail

CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${DATABASE_URL:=postgres://acct:acct_dev@localhost:5111/poc_v3_2}"
export DATABASE_URL

cd "$CRATE_DIR"
exec sqlx migrate run --source db/migrations
