#!/usr/bin/env bash
set -euo pipefail

# Run the cargo integration test suite against an ephemeral 'acct_test'
# database inside the existing dev Postgres instance. The dev DB ('acct')
# is untouched.
#
# Steps: drop+recreate acct_test, sqlx migrate run, seed db/fixtures/small,
# cargo test, drop acct_test on exit.

cd "$(dirname "$0")/.."

ADMIN_DB="${ADMIN_DB:-acct}"
TEST_DB="${TEST_DB:-acct_test}"
PG_USER="${PG_USER:-acct}"
PG_PASS="${PG_PASS:-acct_dev}"
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5111}"

TEST_URL="postgres://${PG_USER}:${PG_PASS}@${PG_HOST}:${PG_PORT}/${TEST_DB}"

drop_test_db() {
  docker compose exec -T postgres psql -U "$PG_USER" -d "$ADMIN_DB" \
    -c "DROP DATABASE IF EXISTS ${TEST_DB} WITH (FORCE)" >/dev/null 2>&1 || true
}
trap drop_test_db EXIT

drop_test_db
docker compose exec -T postgres psql -U "$PG_USER" -d "$ADMIN_DB" \
  -c "CREATE DATABASE ${TEST_DB}" >/dev/null

DATABASE_URL="$TEST_URL" sqlx migrate run --source db/migrations >/dev/null

docker compose exec -T postgres psql -U "$PG_USER" -d "$TEST_DB" -v ON_ERROR_STOP=1 \
  < db/fixtures/small/seed.sql >/dev/null

TEST_DATABASE_URL="$TEST_URL" cargo test "$@"
