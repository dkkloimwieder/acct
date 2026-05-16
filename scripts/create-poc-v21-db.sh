#!/usr/bin/env bash
# Create the acct_poc_queue_v21 database and load poc_v21_ledger.
#
# Prereq: bash poc/queue-extension-v21/scripts/install-into-container.sh
# (or shared_preload_libraries already contains poc_v21_ledger and the
# .so/.control/.sql artifacts are in pkglibdir/extension).
#
# Usage: bash scripts/create-poc-v21-db.sh

set -euo pipefail

CONTAINER="${CONTAINER:-acct-postgres}"
SUPERUSER="${SUPERUSER:-acct}"
DB_NAME="${DB_NAME:-acct_poc_queue_v21}"

echo "==> creating database $DB_NAME (if not exists)"
EXISTS=$(docker exec "$CONTAINER" psql -U "$SUPERUSER" -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'")
if [ "$EXISTS" != "1" ]; then
    docker exec "$CONTAINER" psql -U "$SUPERUSER" -d postgres -c "CREATE DATABASE $DB_NAME"
else
    echo "    $DB_NAME already exists; skipping CREATE"
fi

echo "==> loading extensions into $DB_NAME"
docker exec "$CONTAINER" psql -U "$SUPERUSER" -d "$DB_NAME" -c \
    "CREATE EXTENSION IF NOT EXISTS poc_v21_ledger"

echo "==> verifying"
docker exec "$CONTAINER" psql -U "$SUPERUSER" -d "$DB_NAME" -c \
    "SELECT extname, extversion FROM pg_extension WHERE extname = 'poc_v21_ledger'"

echo "==> testing smoke fn"
docker exec "$CONTAINER" psql -U "$SUPERUSER" -d "$DB_NAME" -c \
    "SELECT poc_v21_hello()"

echo "==> done"
