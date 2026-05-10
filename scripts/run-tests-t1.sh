#!/usr/bin/env bash
set -euo pipefail

# T1 tier — invariant-probe binaries only (tests/*_t1.rs). ~40
# binaries, ~2 minutes wall-time (per-binary cargo startup is the
# floor, ~0.8s × 40 ≈ 30s of overhead alone).
#
# Use this for pure schema-add or pure-add-helper changes where
# you're confident the modification doesn't touch ledger flow,
# dispatcher, recon, or reservations. Catches schema regressions
# (CHECK / FK / append-only triggers) and per-table invariants.
#
# When in doubt, prefer scripts/run-tests-fast.sh (~3 min) over
# this — it adds workflow matrices that catch cross-document
# regressions T1 alone would miss.

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

# Build --test flags for *_t1.rs binaries only.
TEST_FLAGS=()
for f in tests/*_t1.rs; do
  base="$(basename "$f" .rs)"
  TEST_FLAGS+=( --test "$base" )
done

TEST_DATABASE_URL="$TEST_URL" RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}" \
  cargo test "${TEST_FLAGS[@]}" "$@"
