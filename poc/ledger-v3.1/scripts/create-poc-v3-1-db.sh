#!/usr/bin/env bash
# Create the poc_v3_1 database in the dev postgres container (design-v3.1 Path C PoC).
#
# Idempotent: skips CREATE DATABASE if the database already exists.
# Does NOT load any pgrx extensions — those land with install-direct-c.sh (P2,
# acct-2ttr.3) and install-routed-c.sh (P3, acct-2ttr.4). For now the database holds
# only the sqlx-cli migrations.
#
# Usage: bash poc/ledger-v3.1/scripts/create-poc-v3-1-db.sh

set -euo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
SUPERUSER="${SUPERUSER:-acct}"
DB_NAME="${DB_NAME:-poc_v3_1}"

echo "==> creating database $DB_NAME (if not exists)"
EXISTS=$(docker exec "$CONTAINER" psql -U "$SUPERUSER" -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'")
if [ "$EXISTS" != "1" ]; then
    docker exec "$CONTAINER" psql -U "$SUPERUSER" -d postgres -c "CREATE DATABASE $DB_NAME"
else
    echo "    $DB_NAME already exists; skipping CREATE"
fi

echo "==> done"
