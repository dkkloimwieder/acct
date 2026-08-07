#!/usr/bin/env bash
# setup-cc1-for-perf.sh — acct-q6sx: stand up a clean cc=1 healthy regime (1
# committer, 1 pool, router batch 200) so a flamegraph captures the committer
# doing steady plan_and_write apply work. Does NOT start the load and does NOT
# run perf (perf must run as root via sudo — committer is uid 999). After this
# exits, the caller starts a long load + the user runs `sudo perf record -p PID`.
#
# PERSISTENT REGIME — no auto-restore. This ALTER SYSTEMs a NON-default routed
# regime (committer_count=1, batch_window_us=20000; batch_size_max=200 and
# affinity_scheme=0 already equal the boot defaults) and does NOT reset it on exit.
# The change lands in postgresql.auto.conf, so it SURVIVES docker restart and would
# silently mislabel any later no-pin bench run — common.sh's assert_routed_gucs
# fails loud when it does. Restore production defaults when done profiling:
#     bash bench/setup-cc1-for-perf.sh --restore
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
SCEN="${SCEN:-s2}"
asys()    { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.$1 = $2" >/dev/null; }
reload()  { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT pg_reload_conf()" >/dev/null; }
psql_v3() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }

# --restore — partner stanza: RESET the routed GUCs this script pins back to their
# boot defaults + restart (committer_count is read at _PG_init, so a reload alone
# won't respawn the 4-committer pool). Connects via postgres so it works even if
# poc_v3_1 was dropped. Verify with assert_routed_gucs after.
if [ "${1:-}" = "--restore" ]; then
  echo "==> restoring production-default routed GUCs (RESET committer_count/batch_size_max/batch_window_us/affinity_scheme + restart)" >&2
  for g in committer_count batch_size_max batch_window_us affinity_scheme; do
    docker exec "$CONTAINER" psql -U acct -d postgres -tAc "ALTER SYSTEM RESET ledger_routed_c.$g" >/dev/null 2>&1 || true
  done
  docker exec "$CONTAINER" psql -U acct -d postgres -tAc "SELECT pg_reload_conf()" >/dev/null 2>&1 || true
  restart_db
  assert_routed_gucs || { echo "WARN: GUCs still not at defaults after restore" >&2; exit 1; }
  echo "==> restored." >&2
  exit 0
fi

build_harness
asys committer_count 1
asys batch_size_max 200; asys batch_window_us 20000; asys affinity_scheme 0; reload

docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2
docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
restart_db
"$BIN" --dsn "$DIRECT_DSN" run --scenario "$SCEN" --mode direct-per-call --duration 1s \
  --max-callers 1 --method-mix all-fifo --seed-count 1 --seed-skus 1 --seed-locations 1 \
  --seed-depth 0 --no-sampler --output "${RESULTS_DIR}/.perf-seed.json" >/dev/null

echo "READY committers=$(psql_v3 "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'ledger_routed_c_committer%'") pools=$(psql_v3 'SELECT count(*) FROM pool')" >&2