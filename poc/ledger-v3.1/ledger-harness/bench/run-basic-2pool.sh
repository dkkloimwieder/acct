#!/usr/bin/env bash
# run-basic-2pool.sh — acct-0usf.1 THE MOST BASIC ASSESSMENT.
#
# Before testing affinity, answer the precondition it depends on:
#   does spreading 2 disjoint hot pools across 2 committers beat 1 committer?
# PURE committer parallelism — affinity OFF throughout, so the affinity claim
# path (which has a known head-cursor bug) is NOT exercised and cannot
# contaminate the result.
#
# Workload: a 2-pool universe (2 skus × 1 location), scenario s2 (Simple,
# RECEIPTS so the pools never drain over the run), overlapped 50/50 via
# --pareto-hot-pool-pct 50 --pareto-hot-traffic-pct 50. On 2 pools that splits
# traffic ~50/50 across pool-1 and pool-2, one pool per submission (Simple = 1
# line), no cross-pool overlap, no shared row lock. The two pools CAN be drained
# fully in parallel; the only question is whether the committer tier does so.
#
# Arms (affinity OFF, scheme=0, both):
#   A: committer_count=1  — one worker commits both pools' groups serially.
#   B: committer_count=2  — two workers; disjoint pools share no lock, so even a
#                           first-come claim tends one pool each (no affinity needed).
#
# Read:
#   B/A ~2x   => committer parallelism is REAL; routed path has headroom affinity
#                could (under contention) capture. Worth fixing the claim loop.
#   B/A ~1x   => committer parallelism gives NOTHING (router / WAL / single
#                staging ring serializes upstream). Affinity is dead on arrival
#                regardless of implementation. STOP and report that.
#
# committer_count is read at _PG_init, so each arm restarts to respawn the pool
# at the new count (clean_seed restart_db). RECEIPTS means no reseed between reps.
#
# Bench-only.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
DUR="${DUR:-20s}"
REPS="${REPS:-3}"
CALLERS="${CALLERS:-200}"         # s2 default; <=max_connections(500) on direct DSN
SCEN="${SCEN:-s2}"                # Simple receipts
COUNTS="${COUNTS:-1 2}"           # committer_count arms
OUT="${RESULTS_DIR}/basic_2pool.csv"
SWEEP_LOG="${RESULTS_DIR}/basic_2pool.log"
log() { echo "[basic2p] $*" | tee -a "$SWEEP_LOG" >&2; }

oneline_get() { python3 -c "import sys,json,re
s=sys.stdin.read()
m=re.search(r'(\{.*\})', s, re.S)
d=json.loads(m.group(1)) if m else {}
print(d.get('$2',''))" <<<"$1"; }

psql_v3() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }
set_committers() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.committer_count = $1" >/dev/null; }
set_affinity_off() {
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.affinity_scheme = 0" >/dev/null
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT pg_reload_conf()" >/dev/null
}
restore() { set_committers 4 >/dev/null 2>&1 || true; set_affinity_off >/dev/null 2>&1 || true; }
trap restore EXIT

# Fresh DB + 2-pool seed (2 skus, 1 location, receipts so no depth needed) + restart.
clean_seed_2pool() {
  docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2
  docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
  ( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
  restart_db
  # seed EXACTLY 2 pools via the run --method-mix reseed path (receipts; depth 0)
  "$BIN" --dsn "$DIRECT_DSN" run --scenario "$SCEN" --mode direct-per-call --duration 1s \
    --max-callers 1 --method-mix all-fifo \
    --seed-count 2 --seed-skus 2 --seed-locations 1 --seed-depth 0 \
    --no-sampler --output "${RESULTS_DIR}/.basic2p-seed.json" >/dev/null
}

running_committers() { psql_v3 "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'ledger_routed_c_committer%'"; }
pool_count() { psql_v3 "SELECT count(*) FROM pool"; }
# Per-pool receipt counts after a run — proves the 50/50 split actually happened.
pool_split() { psql_v3 "SELECT string_agg(c::text, '/') FROM (SELECT count(*) c FROM trx_line GROUP BY pool_id ORDER BY pool_id) s"; }

build_harness
log "=== BASIC 2-pool parallelism check (acct-0usf.1) scen=$SCEN callers=$CALLERS reps=$REPS counts=[$COUNTS] affinity=OFF 50/50 split ==="
echo "committer_count,rep,callers,pools,pool_split,throughput_trx_s,commit_group_avg,committer_busy_frac,lock_of_busy,running_of_busy,lwlock_of_busy,committer_samples,load1_end" > "$OUT"

for n in $COUNTS; do
  log "--- committer_count=$n : set + clean + restart + seed (2 pools) ---"
  set_committers "$n"
  clean_seed_2pool
  set_affinity_off
  seen="$(running_committers)"; pc="$(pool_count)"
  log "  committers running: $seen (requested $n), pools=$pc"
  [ "$pc" = "2" ] || { log "  ABORT: expected 2 pools, got $pc"; exit 1; }
  for rep in $(seq 1 "$REPS"); do
    out="${RESULTS_DIR}/.basic2p_n${n}_r${rep}.json"
    wait_for_quiet_host || log "  NOTE: n=$n r$rep busy host"
    log "RUN cc=$n rep=$rep/$REPS (load1=$(host_load1))"
    line="$(timeout 360 "$BIN" --dsn "$DIRECT_DSN" run --scenario "$SCEN" --mode routed --duration "$DUR" \
      --max-callers "$CALLERS" --pareto-hot-pool-pct 50 --pareto-hot-traffic-pct 50 \
      --output "$out" 2>/dev/null)" || { log "  FAIL cc=$n r$rep"; continue; }
    le="$(host_load1)"; split="$(pool_split)"
    tput="$(oneline_get "$line" throughput_trx_per_sec)"; cg="$(oneline_get "$line" commit_group_avg)"
    cbf="$(oneline_get "$line" committer_busy_frac)"
    lob="$(oneline_get "$line" committer_lock_frac_of_busy)"
    rob="$(oneline_get "$line" committer_running_frac_of_busy)"
    wob="$(oneline_get "$line" committer_lwlock_frac_of_busy)"
    csm="$(oneline_get "$line" committer_samples)"
    echo "$n,$rep,$CALLERS,$pc,$split,$tput,$cg,$cbf,$lob,$rob,$wob,$csm,$le" >> "$OUT"
    log "  cc=$n r$rep tput=$tput cg=$cg split=$split busy=$cbf of_busy(lock=$lob run=$rob lw=$wob) load=$le"
  done
done
restore
log "=== done. CSV: $OUT ==="
column -s, -t "$OUT" | tee -a "$SWEEP_LOG" >&2 || cat "$OUT"
