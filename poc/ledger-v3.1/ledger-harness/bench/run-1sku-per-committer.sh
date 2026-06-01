#!/usr/bin/env bash
# run-1sku-per-committer.sh — acct-0usf.1: does committer parallelism scale when
# each committer owns EXACTLY ONE SKU (one pinned pool), with ZERO cross-committer
# contention and a STATIC affinity assignment (no routing/steal calc as a confound)?
#
# This is "reproduce the STEP-A committer-scaling question, but with SKUs pinned
# and at a sensible batch" — the clean affinity measurement.
#
# Owner of a commit_group under affinity_scheme=1 is mix64(min_pool_id)%cc. mix64
# is NOT a clean modulo, so contiguous ids 1..N collide (1,2,3,4 -> owners 1,2,1,2
# at cc=4). We therefore drive, per committer count, the LOWEST id set that hits
# each owner exactly once (precomputed offline against affinity.rs's splitmix64):
#   cc=1 -> pools {1}        owners {0}
#   cc=2 -> pools {1,2}      owners {1,0}
#   cc=4 -> pools {1,2,6,7}  owners {1,2,0,3}
# The run loads its universe via SELECT id FROM pool, so we seed a superset then
# DELETE the non-target pools — the run then draws from exactly the 1:1 set, with
# NO production code change.
#
# affinity_steal_ms is set >> run duration => the pin is STATIC: owners claim, no
# one steals. steals==0 in the PIN CHECK proves each committer committed only its
# own SKU (1-SKU-per-committer held). Batch is held at the czz4 sweet spot
# (200, wide window). Workload = s2 simple receipts: single-pool-per-trx, self-
# seeding, pools never drain over the run.
#
# Bench-only. One clean+seed+restart per committer arm; load-gated; GUCs restored
# on exit. Cluster touch (DROP/CREATE poc_v3_1 + restart acct-postgres per arm).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
SCEN="${SCEN:-s2}"                  # Simple receipts: single-pool-per-trx, uniform
COUNTS="${COUNTS:-1 2 4}"           # committer_count arms (each = 1 SKU per committer)
REPS="${REPS:-5}"
CALLERS="${CALLERS:-200}"
DUR="${DUR:-20s}"
BATCH="${BATCH:-200}"               # czz4 sweet spot
WINDOW="${WINDOW:-20000}"           # wide so BATCH binds, not the window
STEAL_MS="${STEAL_MS:-60000}"       # >> DUR => static pin (no age-gated steal)
OUT="${OUT:-${RESULTS_DIR}/affinity_1sku_per_committer.csv}"
SWEEP_LOG="${RESULTS_DIR}/affinity_1sku.log"
PARSE="$HERE/parse-committer-sampler.py"
log() { echo "[1sku] $*" | tee -a "$SWEEP_LOG" >&2; }

# Precomputed 1:1 sets + the superset id to seed (the max target id) per cc.
target_pools() { case "$1" in 1) echo "1" ;; 2) echo "1 2" ;; 4) echo "1 2 6 7" ;; *) echo "" ;; esac; }
seed_max()     { case "$1" in 1) echo 1 ;;   2) echo 2 ;;   4) echo 7 ;;        *) echo 0 ;; esac; }

asys()    { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.$1 = $2" >/dev/null; }
reload()  { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT pg_reload_conf()" >/dev/null; }
psql_v3() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }
restore() {
  asys committer_count 4 2>/dev/null || true
  asys affinity_scheme 0 2>/dev/null || true
  asys affinity_steal_ms 5 2>/dev/null || true
  asys batch_size_max 50 2>/dev/null || true
  asys batch_window_us 500 2>/dev/null || true
  reload 2>/dev/null || true
}
trap restore EXIT

extract() {  # $1=json -> "tput cg p50 p99"
  python3 - "$1" <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); r=d['routed']; a=d['ack_latency_us']
print(f"{d['throughput_trx_per_sec']:.0f} {r['commit_group_size_avg']:.2f} {a['p50']} {a['p99']}")
PY
}

# Fresh DB, seed the superset (1..seed_max), then delete pools NOT in the 1:1 set
# so the run's universe is EXACTLY the target. Restart respawns the committer pool
# at the ALTER SYSTEM committer_count.
clean_seed_pinned() {
  local cc="$1" smax keep keepcsv
  smax="$(seed_max "$cc")"; keep="$(target_pools "$cc")"; keepcsv="$(echo "$keep" | tr ' ' ',')"
  docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2
  docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
  ( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
  restart_db
  "$BIN" --dsn "$DIRECT_DSN" run --scenario "$SCEN" --mode direct-per-call --duration 1s \
    --max-callers 1 --method-mix all-fifo \
    --seed-count "$smax" --seed-skus "$smax" --seed-locations 1 --seed-depth 0 \
    --no-sampler --output "${RESULTS_DIR}/.1sku-seed.json" >/dev/null
  # Trim to the 1:1 set (dependency order: trx_line -> pool_state/pool_lock -> pool).
  if [ "$(echo "$keep" | wc -w)" != "$smax" ]; then
    psql_v3 "DELETE FROM trx_line  WHERE pool_id NOT IN ($keepcsv)" >/dev/null
    psql_v3 "DELETE FROM pool_state WHERE pool_id NOT IN ($keepcsv)" >/dev/null
    psql_v3 "DELETE FROM pool_lock  WHERE pool_id NOT IN ($keepcsv)" >/dev/null
    psql_v3 "DELETE FROM pool       WHERE id      NOT IN ($keepcsv)" >/dev/null
  fi
}

build_harness
log "=== 1-SKU-per-committer scaling (acct-0usf.1): scen=$SCEN counts=[$COUNTS] reps=$REPS callers=$CALLERS batch=$BATCH window=${WINDOW}us steal=${STEAL_MS}ms affinity=1 ==="
echo "committers,pool_set,rep,throughput_trx_s,cg_avg,ack_p50_us,ack_p99_us,busy_frac,lock_of_busy,running_of_busy,lwlock_of_busy,io_of_busy,top_wait_type,top_wait_event,top_wait_samples,owned_claims,steals,load1_end" > "$OUT"
dsn="$(dsn_for_scenario "$SCEN")"

for cc in $COUNTS; do
  keep="$(target_pools "$cc")"; want="$(echo "$keep" | wc -w)"
  [ -n "$keep" ] || { log "no 1:1 pool set defined for cc=$cc — skip"; continue; }
  log "--- cc=$cc : target pools {$keep} (owners distinct by construction) ---"
  asys committer_count "$cc"
  clean_seed_pinned "$cc"
  asys affinity_scheme 1; asys affinity_steal_ms "$STEAL_MS"
  asys batch_size_max "$BATCH"; asys batch_window_us "$WINDOW"; reload
  pc="$(psql_v3 "SELECT count(*) FROM pool")"
  run="$(psql_v3 "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'ledger_routed_c_committer%'")"
  log "  pools=$pc (want $want: $keep) committers_running=$run (want $cc)"
  [ "$pc" = "$want" ] || { log "  ABORT cc=$cc: pool count $pc != target $want"; exit 1; }
  for rep in $(seq 1 "$REPS"); do
    wait_for_quiet_host || log "  NOTE: cc=$cc r$rep busy host"
    out="${RESULTS_DIR}/.1sku_cc${cc}_r${rep}.json"; smp="${out%.json}.sampler.txt"
    log "RUN cc=$cc rep=$rep/$REPS (load1=$(host_load1))"
    LEDGER_V3_1_PRINT_SAMPLER=1 timeout 360 "$BIN" --dsn "$dsn" run --scenario "$SCEN" --mode routed \
      --duration "$DUR" --max-callers "$CALLERS" --output "$out" >/dev/null 2>&1 \
      || { log "  FAIL cc=$cc r$rep"; continue; }
    read -r tput cg p50 p99 <<<"$(extract "$out")"
    if [ -f "$smp" ]; then read -r busy lob rob wob iob tt te tn <<<"$(python3 "$PARSE" "$smp")"
    else busy=; lob=; rob=; wob=; iob=; tt=none; te=none; tn=0; fi
    owned="$(psql_v3 "SELECT ledger_routed_c_affinity_owned_claims_total()")"
    steals="$(psql_v3 "SELECT ledger_routed_c_affinity_steals_total()")"
    le="$(host_load1)"; ps="$(echo "$keep" | tr ' ' '-')"
    echo "$cc,$ps,$rep,$tput,$cg,$p50,$p99,$busy,$lob,$rob,$wob,$iob,$tt,$te,$tn,$owned,$steals,$le" >> "$OUT"
    log "  cc=$cc r$rep tput=$tput cg=$cg lock=$lob lw=$wob top=${tt}/${te} owned=$owned steals=$steals load=$le"
  done
done
restore
log "=== done. CSV: $OUT ==="
log "    PIN PROOF: steals must be ~0 every arm (owner-only claims => each committer held only its 1 SKU)."
log "    SCALING: compare throughput across cc=1/2/4; lwlk%/lock% + top_wait name the ceiling if it flattens."
column -s, -t "$OUT" >&2 || cat "$OUT"
