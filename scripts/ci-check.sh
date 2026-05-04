#!/usr/bin/env bash
set -euo pipefail

# Local clean-DB schema integrity check. Verifies the migration chain:
#   1. applies cleanly to a fresh DB
#   2. 'sqlx migrate info' shows every migration installed
#   3. reverts cleanly back to empty
#   4. redeploys cleanly
# Then prints the schema SHA (pg_dump -s | sha256sum) for branch-diff
# visibility.
#
# Operates on an ephemeral 'acct_ci' DB inside the existing dev Postgres
# instance. The dev 'acct' DB is untouched.
#
# Run before pushing schema changes. There is no remote CI; this script
# IS the check.

cd "$(dirname "$0")/.."

ADMIN_DB="${ADMIN_DB:-acct}"
CI_DB="${CI_DB:-acct_ci}"
PG_USER="${PG_USER:-acct}"
PG_PASS="${PG_PASS:-acct_dev}"
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5111}"

CI_URL="postgres://${PG_USER}:${PG_PASS}@${PG_HOST}:${PG_PORT}/${CI_DB}"

drop_ci_db() {
  docker compose exec -T postgres psql -U "$PG_USER" -d "$ADMIN_DB" \
    -c "DROP DATABASE IF EXISTS ${CI_DB} WITH (FORCE)" >/dev/null 2>&1 || true
}
trap drop_ci_db EXIT

if ! docker compose exec -T postgres pg_isready -U "$PG_USER" -d "$ADMIN_DB" >/dev/null 2>&1; then
  echo "ERROR: Postgres container not running. Run ./scripts/dev-up.sh first." >&2
  exit 1
fi

drop_ci_db
docker compose exec -T postgres psql -U "$PG_USER" -d "$ADMIN_DB" \
  -c "CREATE DATABASE ${CI_DB}" >/dev/null

echo "==> Step 1/6: sqlx migrate run on fresh $CI_DB"
DATABASE_URL="$CI_URL" sqlx migrate run --source db/migrations

echo "==> Step 2/6: sqlx migrate info — assert every migration installed"
INFO_OUTPUT=$(DATABASE_URL="$CI_URL" sqlx migrate info --source db/migrations)
echo "$INFO_OUTPUT"
if echo "$INFO_OUTPUT" | grep -qE 'pending'; then
  echo "FAIL: at least one migration is not installed" >&2
  exit 1
fi

echo "==> Step 3/6: revert every migration in reverse order"
while true; do
  INSTALLED_COUNT=$(DATABASE_URL="$CI_URL" sqlx migrate info --source db/migrations \
    | grep -c 'installed' || true)
  if [ "$INSTALLED_COUNT" -eq 0 ]; then
    echo "all migrations reverted"
    break
  fi
  DATABASE_URL="$CI_URL" sqlx migrate revert --source db/migrations
done

echo "==> Step 4/6: redeploy on cleaned $CI_DB"
DATABASE_URL="$CI_URL" sqlx migrate run --source db/migrations
INFO_OUTPUT=$(DATABASE_URL="$CI_URL" sqlx migrate info --source db/migrations)
if echo "$INFO_OUTPUT" | grep -qE 'pending'; then
  echo "FAIL: redeploy left migrations pending" >&2
  echo "$INFO_OUTPUT" >&2
  exit 1
fi

echo "==> Step 5/6: schema digest"
# PG 18 pg_dump emits a randomized `\restrict <key>` / `\unrestrict <key>`
# header/footer pair on every invocation (security hardening; prevents
# psql-restriction replay). Strip them before hashing so the digest is
# stable across runs of the same schema.
SHA=$(docker compose exec -T postgres pg_dump -U "$PG_USER" -s "$CI_DB" \
        | grep -vE '^\\(restrict|unrestrict) ' \
        | sha256sum | awk '{print $1}')
echo "schema SHA256 = $SHA"

echo
echo "==> Step 6/6: sql-lint anti-pattern scan (acct-du2 Phase 5)"
echo "    Errors fail the build; warnings are advisory (review against REVIEW.md)."
./scripts/sql-lint.sh

echo
echo "OK: schema integrity check passed."
