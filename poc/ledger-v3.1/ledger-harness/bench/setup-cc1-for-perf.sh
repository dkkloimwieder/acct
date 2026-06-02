#!/usr/bin/env bash
# setup-cc1-for-perf.sh — acct-q6sx: stand up a clean cc=1 healthy regime (1
# committer, 1 pool, router batch 200) so a flamegraph captures the committer
# doing steady plan_and_write apply work. Does NOT start the load and does NOT
# run perf (perf must run as root via sudo — committer is uid 999). After this
# exits, the caller starts a long load + the user runs `sudo perf record -p PID`.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
SCEN="${SCEN:-s2}"
asys()    { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.$1 = $2" >/dev/null; }
reload()  { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT pg_reload_conf()" >/dev/null; }
psql_v3() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }

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