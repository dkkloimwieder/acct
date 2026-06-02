#!/usr/bin/env bash
# measure-apply-spans.sh — acct-q6sx: read the committer's per-span ns counters
# under a HEALTHY cc=1 steady load to get the apply-pipeline breakdown without the
# cc=4 near-empty-group distortion.
#
# The committer already decomposes its work (committer.rs:604-625) into:
#   prep      = pipeline_ns - (pool_lock + hydrate + apply)   (decode + pg_xact_status
#               triage + dedup SELECT against trx + line-decode)
#   pool_lock = acquire_pool_locks FOR UPDATE
#   hydrate   = hydrate_snapshot
#   apply     = plan_and_write (plan + INSERT trx/trx_line/posting_line + aggregate UPDATE)
#   fsync     = txn_ns - pipeline_ns   (the COMMIT)
# All are exposed via ledger_routed_c_committer_*_ns_total() and live in shmem.
#
# Setup: 1 committer, 1 pool, single-push (NO caller batching -> no backpressure
# distortion), router batch_size_max=200 so groups fill. One restart resets the
# counters; we run REPS steady windows and read the CUMULATIVE spans at the end.
#
# Bench-only. Cluster touch (DROP/CREATE poc_v3_1 + restart acct-postgres).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
SCEN="${SCEN:-s2}"
RBATCH="${RBATCH:-200}"
WINDOW="${WINDOW:-20000}"
REPS="${REPS:-3}"
DUR="${DUR:-20s}"
OUT="${OUT:-${RESULTS_DIR}/apply_spans.csv}"
LOGF="${RESULTS_DIR}/apply_spans.log"
log() { echo "[apply-spans] $*" | tee -a "$LOGF" >&2; }

asys()    { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.$1 = $2" >/dev/null; }
reload()  { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT pg_reload_conf()" >/dev/null; }
psql_v3() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }
restore() {
  asys committer_count 4 2>/dev/null || true
  asys batch_size_max 50 2>/dev/null || true
  asys batch_window_us 500 2>/dev/null || true
  asys affinity_scheme 0 2>/dev/null || true
  reload 2>/dev/null || true
}
trap restore EXIT

clean_seed() {
  docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2
  docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
  ( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
  restart_db   # respawns committer pool at committer_count=1 AND zeroes the span counters
  "$BIN" --dsn "$DIRECT_DSN" run --scenario "$SCEN" --mode direct-per-call --duration 1s \
    --max-callers 1 --method-mix all-fifo --seed-count 1 --seed-skus 1 --seed-locations 1 \
    --seed-depth 0 --no-sampler --output "${RESULTS_DIR}/.apply-spans-seed.json" >/dev/null
}

spans() {  # read cumulative span counters -> breakdown
  local pipe lock hyd app txn grp trx
  pipe="$(psql_v3 'SELECT ledger_routed_c_committer_pipeline_ns_total()')"
  lock="$(psql_v3 'SELECT ledger_routed_c_committer_pool_lock_ns_total()')"
  hyd="$(psql_v3 'SELECT ledger_routed_c_committer_hydrate_ns_total()')"
  app="$(psql_v3 'SELECT ledger_routed_c_committer_apply_ns_total()')"
  txn="$(psql_v3 'SELECT ledger_routed_c_committer_txn_ns_total()')"
  grp="$(psql_v3 'SELECT ledger_routed_c_committer_pipeline_count()')"
  trx="$(psql_v3 'SELECT ledger_routed_c_committer_trx_committed_total()')"
  python3 - "$pipe" "$lock" "$hyd" "$app" "$txn" "$grp" "$trx" "$OUT" <<'PY'
import sys
pipe,lock,hyd,app,txn,grp,trx = (int(x) for x in sys.argv[1:8])
out = sys.argv[8]
prep = pipe - (lock + hyd + app)
fsync = txn - pipe
tot = txn if txn else 1
def pct(x): return 100.0*x/tot
rows = [("prep",prep),("apply",app),("fsync",fsync),("hydrate",hyd),("pool_lock",lock)]
print(f"groups={grp} trx={trx} trx_per_group={trx/grp:.1f}" if grp else "no groups")
print(f"{'span':<10} {'seconds':>10} {'%txn':>8}")
for n,v in rows: print(f"{n:<10} {v/1e9:>10.2f} {pct(v):>7.1f}%")
with open(out,"w") as f:
    f.write("span,seconds,pct_txn\n")
    f.write(f"_meta,groups={grp};trx={trx};trx_per_group={trx/grp:.2f}\n" if grp else "_meta,nogroups\n")
    for n,v in rows: f.write(f"{n},{v/1e9:.3f},{pct(v):.2f}\n")
PY
}

build_harness
log "=== apply-span breakdown (acct-q6sx): cc=1 single-push, router_batch=$RBATCH window=${WINDOW}us reps=$REPS dur=$DUR ==="
asys committer_count 1
asys batch_size_max "$RBATCH"; asys batch_window_us "$WINDOW"; asys affinity_scheme 0; reload
clean_seed
run="$(psql_v3 "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'ledger_routed_c_committer%'")"
log "committers running: $run (want 1); spans zeroed by restart"
dsn="$(dsn_for_scenario "$SCEN")"

for rep in $(seq 1 "$REPS"); do
  wait_for_quiet_host || log "  NOTE: rep $rep busy host"
  out="${RESULTS_DIR}/.apply_spans_r${rep}.json"
  log "RUN rep=$rep/$REPS cc=1 single-push (load1=$(host_load1))"
  timeout 360 "$BIN" --dsn "$dsn" run --scenario "$SCEN" --mode routed \
    --duration "$DUR" --max-callers 1 --output "$out" >/dev/null 2>&1 \
    || { log "  FAIL rep=$rep"; continue; }
  tput="$(python3 -c "import json;print(f\"{json.load(open('$out'))['throughput_trx_per_sec']:.0f}\")")"
  log "  rep=$rep tput=$tput"
done

log "=== cumulative committer span breakdown (healthy cc=1 regime) ==="
spans | tee -a "$LOGF" >&2
restore
log "=== done. CSV: $OUT ==="