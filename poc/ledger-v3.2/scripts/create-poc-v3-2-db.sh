#!/usr/bin/env bash
# Create the poc_v3_2 database in the dev postgres container (ledger-v3.2).
#
# Idempotent: skips CREATE DATABASE if the database already exists.
# Does NOT load any pgrx extensions — those land with the hot-path phase. The
# database holds the sqlx-cli migrations.
#
# Usage: bash poc/ledger-v3.2/scripts/create-poc-v3-2-db.sh

set -euo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
SUPERUSER="${SUPERUSER:-acct}"
DB_NAME="${DB_NAME:-poc_v3_2}"

echo "==> creating database $DB_NAME (if not exists)"
EXISTS=$(docker exec "$CONTAINER" psql -U "$SUPERUSER" -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'")
if [ "$EXISTS" != "1" ]; then
    docker exec "$CONTAINER" psql -U "$SUPERUSER" -d postgres -c "CREATE DATABASE $DB_NAME"
else
    echo "    $DB_NAME already exists; skipping CREATE"
fi

echo "==> done"
