#!/usr/bin/env bash
set -euo pipefail

# Fast tier of the integration test suite — excludes:
#   * property_*.rs (24 binaries, ~30-65s each — dominate full-suite wall-time)
#   * conformance.rs (100-case harness, ~34s)
#   * outbox_drain.rs (~53s; outbox-stress shape)
#   * load_*.rs (9 binaries, stress tests — most #[ignore]'d but binary load adds time)
#
# Runs ~99 binaries (~940 tests) in ~4-5 minutes, vs the full
# suite's ~13-15 min.
#
# Use this for iteration + per-commit checks. Run scripts/run-tests.sh
# (full suite, including property tests + conformance + outbox_drain)
# before crossing a milestone or at session end. Property tests are
# the regression net for cost dispatch / recon / reservations under
# random workloads — DO run the full suite when changing
# _post_posting_lines_apply_event, cost_method_strategies,
# run_daily_reconciliation, or reserve_inventory.
#
# Faster still: scripts/run-tests-t1.sh runs only T1 invariant probes
# (~40 binaries, ~50-60s) — sufficient for pure schema-add changes.

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

# Build --test flags for every binary EXCEPT the slow exclusions.
TEST_FLAGS=()
for f in tests/*.rs; do
  base="$(basename "$f" .rs)"
  case "$base" in
    property_*)        continue ;;
    conformance)       continue ;;
    outbox_drain)      continue ;;
    load_*)            continue ;;
    *)                 TEST_FLAGS+=( --test "$base" ) ;;
  esac
done

TEST_DATABASE_URL="$TEST_URL" RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}" \
  cargo test "${TEST_FLAGS[@]}" "$@"
