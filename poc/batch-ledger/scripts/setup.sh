#!/usr/bin/env bash
# acct-c0ko (P1 of acct-qdp5 PoC) — create acct_poc DB + apply PoC migrations.
#
# Idempotent. Safe to re-run.

set -euo pipefail
cd "$(dirname "$0")/.."

ADMIN_URL="${POC_ADMIN_URL:-postgres://acct:acct_dev@localhost:5111/postgres}"
DB_URL="${POC_DATABASE_URL:-postgres://acct:acct_dev@localhost:5111/acct_poc}"

if ! psql "$ADMIN_URL" -tAc "SELECT 1 FROM pg_database WHERE datname = 'acct_poc'" 2>/dev/null | grep -q 1; then
    echo "Creating acct_poc database..."
    psql "$ADMIN_URL" -c "CREATE DATABASE acct_poc"
else
    echo "acct_poc database already exists."
fi

echo "Applying PoC migrations..."
sqlx migrate run --database-url "$DB_URL" --source db/migrations

echo "Done. acct_poc is up to date."
