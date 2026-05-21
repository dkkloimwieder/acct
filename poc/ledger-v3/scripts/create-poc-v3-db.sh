#!/usr/bin/env bash
# Create the poc_v3 database in the dev postgres container.
#
# Idempotent: skips CREATE DATABASE if the database already exists.
# Does NOT load any pgrx extensions — those land with install-direct.sh
# (Phase 2 / acct-llt2) and install-routed.sh (Phase 4 / acct-1xx). For now
# the database holds only the sqlx-cli migrations.
#
# Usage: bash poc/ledger-v3/scripts/create-poc-v3-db.sh

set -euo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
SUPERUSER="${SUPERUSER:-acct}"
DB_NAME="${DB_NAME:-poc_v3}"

echo "==> creating database $DB_NAME (if not exists)"
EXISTS=$(docker exec "$CONTAINER" psql -U "$SUPERUSER" -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'")
if [ "$EXISTS" != "1" ]; then
    docker exec "$CONTAINER" psql -U "$SUPERUSER" -d postgres -c "CREATE DATABASE $DB_NAME"
else
    echo "    $DB_NAME already exists; skipping CREATE"
fi

echo "==> done"
