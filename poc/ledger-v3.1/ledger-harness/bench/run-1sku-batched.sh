#!/usr/bin/env bash
# run-1sku-batched.sh — acct-ruex CORRECTED setup. Identical pinned-pool machinery
# to run-1sku-per-committer.sh (acct-0usf.1), but ingress is CALLER-SIDE BATCHED
# instead of single-push, so the staging-ring LWLock handoffs are removed and we
# see the committer drain ceiling in the regime that matters.
#
# THE SETUP (exact):
#   - Sweep unit = COUNTS = {1,2,4} = committers = SKUs = callers, all coupled.
#       cc=1 -> 1 SKU (pool {1}),     1 committer, 1 caller
#       cc=2 -> 2 SKUs (pools {1,2}), 2 committers, 2 callers
#       cc=4 -> 4 SKUs (pools {1,2,6,7}), 4 committers, 4 callers
#     1 SKU per committer, pinned via affinity (steal >> run = static pin).
#   - CALLER-SIDE batch = CBATCH = 8192 = HALF the 16384-slot ring ("4000+",
#     "half the arena"). Each caller pushes 8192 trx per ledger_enqueue_trx_batch_c
#     call (backpressured). THIS is the test: saturate ingress, remove the
#     per-trx staging-lock handoffs.
#   - ROUTER batch_size_max = RBATCH = 200 (the commit-group target). Each SKU's
#     stream is chopped into commit groups of <=200 -> one fsync per ~200 trx.
#   - Question: does committer drain scale 1->2->4 when each owns one pinned SKU,
#     ingress is batch-saturated (no staging-lock fight), and groups fill to 200?
#
# NO ARENA CHUNKING (per instruction). At cc=4, 4 callers each allocating 8192
# payloads under one SPILLOVER_ARENA.exclusive() WILL likely convoy on the arena
# lock and starve the committers (top_wait=LWLock/ledger_v31_spillover_arena,
# busy_frac drops). That is a RESULT to report, not a bug to fix here.
#
# Requires ledger_enqueue_trx_batch_c already deployed (install-routed-c.sh,
# already done). enqueue.rs is UNCHANGED from the deployed .so (no chunking), so
# NO redeploy is needed. Bench-only; cluster touch (DROP/CREATE + restart/arm).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
SCEN="${SCEN:-s2}"                  # Simple receipts: single-pool-per-trx
COUNTS="${COUNTS:-1 2 4}"           # coupled: committers = SKUs = callers
REPS="${REPS:-5}"
DUR="${DUR:-20s}"
CBATCH="${CBATCH:-8192}"            # CALLER-side batch = half the 16384 ring (the test)
RBATCH="${RBATCH:-200}"            # ROUTER batch_size_max = commit-group target
WINDOW="${WINDOW:-20000}"          # router coalesce window (wide so RBATCH binds)
STEAL_MS="${STEAL_MS:-60000}"      # >> DUR => static affinity pin
OUT="${OUT:-${RESULTS_DIR}/affinity_1sku_batched.csv}"
SWEEP_LOG="${RESULTS_DIR}/affinity_1sku_batched.log"
PARSE="$HERE/parse-committer-sampler.py"
log() { echo "[1sku-batch] $*" | tee -a "$SWEEP_LOG" >&2; }

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
  asys batch_size_max 200 2>/dev/null || true
  asys batch_window_us 500 2>/dev/null || true
  asys queue_full_timeout_ms 5000 2>/dev/null || true
  reload 2>/dev/null || true
}
trap restore EXIT

extract() {  # $1=json -> "tput cg p50 p99 errs"
  python3 - "$1" <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); r=d['routed']; a=d['ack_latency_us']
print(f"{d['throughput_trx_per_sec']:.0f} {r['commit_group_size_avg']:.2f} {a['p50']} {a['p99']} {d.get('errors_total',0)}")
PY
}

# Fresh DB, seed the superset (1..seed_max), then delete pools NOT in the 1:1 set
# so the run's universe is EXACTLY the target. Restart respawns the committer pool
# at the ALTER SYSTEM committer_count. (Identical to run-1sku-per-committer.sh.)
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
    --no-sampler --output "${RESULTS_DIR}/.1sku-batched-seed.json" >/dev/null
  # Trim to the 1:1 set. FK children-first order:
  #   posting_line_dimension -> posting_line -> trx_line -> pool_state/pool_lock -> pool
  if [ "$(echo "$keep" | wc -w)" != "$smax" ]; then
    psql_v3 "DELETE FROM posting_line_dimension WHERE posting_line_id IN (SELECT pl.id FROM posting_line pl JOIN trx_line t ON t.id = pl.trx_line_id WHERE t.pool_id NOT IN ($keepcsv))" >/dev/null
    psql_v3 "DELETE FROM posting_line WHERE trx_line_id IN (SELECT id FROM trx_line WHERE pool_id NOT IN ($keepcsv))" >/dev/null
    psql_v3 "DELETE FROM trx_line  WHERE pool_id NOT IN ($keepcsv)" >/dev/null
    psql_v3 "DELETE FROM pool_state WHERE pool_id NOT IN ($keepcsv)" >/dev/null
    psql_v3 "DELETE FROM pool_lock  WHERE pool_id NOT IN ($keepcsv)" >/dev/null
    psql_v3 "DELETE FROM pool       WHERE id      NOT IN ($keepcsv)" >/dev/null
  fi
}

build_harness
# Pre-flight: the batch entry-point must already be deployed.
if ! docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc \
      "SELECT 1 FROM pg_proc WHERE proname='ledger_enqueue_trx_batch_c'" 2>/dev/null | grep -q 1; then
  log "NOTE: ledger_enqueue_trx_batch_c not in poc_v3_1 yet — clean_seed_pinned's CREATE EXTENSION defines it from the deployed .sql."
fi
log "=== 1-SKU-per-committer CALLER-BATCHED (acct-ruex): scen=$SCEN counts=[$COUNTS] reps=$REPS caller_batch=$CBATCH router_batch=$RBATCH window=${WINDOW}us steal=${STEAL_MS}ms affinity=1 ==="
echo "committers,pool_set,caller_batch,router_batch,callers,rep,throughput_trx_s,cg_avg,ack_p50_us,ack_p99_us,enqueue_errors,busy_frac,lock_of_busy,running_of_busy,lwlock_of_busy,io_of_busy,top_wait_type,top_wait_event,top_wait_samples,owned_claims,steals,load1_end" > "$OUT"
dsn="$(dsn_for_scenario "$SCEN")"

for cc in $COUNTS; do
  keep="$(target_pools "$cc")"; want="$(echo "$keep" | wc -w)"
  [ -n "$keep" ] || { log "no 1:1 pool set defined for cc=$cc — skip"; continue; }
  log "--- cc=$cc : SKUs/committers/callers=$cc, pinned pools {$keep}, caller_batch=$CBATCH ---"
  asys committer_count "$cc"
  clean_seed_pinned "$cc"
  asys affinity_scheme 1; asys affinity_steal_ms "$STEAL_MS"
  asys batch_size_max "$RBATCH"; asys batch_window_us "$WINDOW"
  asys queue_full_timeout_ms 60000; reload   # high so batch backpressure waits, not errors
  pc="$(psql_v3 "SELECT count(*) FROM pool")"
  run="$(psql_v3 "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'ledger_routed_c_committer%'")"
  log "  pools=$pc (want $want: $keep) committers_running=$run (want $cc)"
  [ "$pc" = "$want" ] || { log "  ABORT cc=$cc: pool count $pc != target $want"; exit 1; }
  for rep in $(seq 1 "$REPS"); do
    wait_for_quiet_host || log "  NOTE: cc=$cc r$rep busy host"
    out="${RESULTS_DIR}/.1skub_cc${cc}_r${rep}.json"; smp="${out%.json}.sampler.txt"
    log "RUN cc=$cc rep=$rep/$REPS callers=$cc caller_batch=$CBATCH (load1=$(host_load1))"
    LEDGER_V3_1_PRINT_SAMPLER=1 timeout 360 "$BIN" --dsn "$dsn" run --scenario "$SCEN" --mode routed \
      --batch-size "$CBATCH" --duration "$DUR" --max-callers "$cc" --output "$out" >/dev/null 2>&1 \
      || { log "  FAIL cc=$cc r$rep"; continue; }
    read -r tput cg p50 p99 errs <<<"$(extract "$out")"
    if [ -f "$smp" ]; then read -r busy lob rob wob iob tt te tn <<<"$(python3 "$PARSE" "$smp")"
    else busy=; lob=; rob=; wob=; iob=; tt=none; te=none; tn=0; fi
    owned="$(psql_v3 "SELECT ledger_routed_c_affinity_owned_claims_total()")"
    steals="$(psql_v3 "SELECT ledger_routed_c_affinity_steals_total()")"
    le="$(host_load1)"; ps="$(echo "$keep" | tr ' ' '-')"
    echo "$cc,$ps,$CBATCH,$RBATCH,$cc,$rep,$tput,$cg,$p50,$p99,$errs,$busy,$lob,$rob,$wob,$iob,$tt,$te,$tn,$owned,$steals,$le" >> "$OUT"
    log "  cc=$cc r$rep tput=$tput cg=$cg errs=$errs lock=$lob lw=$wob top=${tt}/${te} steals=$steals load=$le"
  done
done
restore
log "=== done. CSV: $OUT ==="
log "    SCALING: throughput across cc=1/2/4 (each = 1 SKU/committer/caller, caller_batch=$CBATCH)."
log "    cg should be ~$RBATCH (groups fill to the router cap); WAL amortized ~${RBATCH}x per fsync."
log "    PIN PROOF: steals ~0. cc=4 may show top_wait=spillover_arena (arena convoy, NO chunking) — that is the result."
column -s, -t "$OUT" >&2 || cat "$OUT"
