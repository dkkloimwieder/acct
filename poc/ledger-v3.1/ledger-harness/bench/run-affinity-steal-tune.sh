#!/usr/bin/env bash
# run-affinity-steal-tune.sh — acct-0usf STEP 3 V0 steal-window tuning.
#
# The V0 smoke (N=2, steal_ms=5) showed ~74% of ON-arm claims were STEALS, not
# owned claims: on a hot pool every group shares one min-pool → one owner
# committer → the other committers own nothing and steal after 5ms. Affinity
# barely pins. This script asks: does a LONGER steal window convert steals into
# owned claims (real pinning, bigger lock drop) — or does pinning a hot pool to
# one committer just re-serialize onto one worker and lose throughput?
#
# Sweeps affinity_steal_ms over STEAL_MS_LIST for the ON arm; one OFF baseline
# per scenario (steal_ms irrelevant when scheme=0). steal_ms is a CSV column.
# Records the owned/steals engagement counters per ON run so pinning is MEASURED
# (steal_frac → 0 means the longer window pinned; stays high means even a long
# window can't, because a single owner can't drain a 1000-caller hot pool).
#
# Default scenarios: s5 (single hot pool — where steal_ms matters most) + s10
# (Pareto receipts — multi-owner, the realistic regime). Small N; this is a
# parameter probe to PICK steal_ms before the full N=5 sweep, not the sweep.
#
# Bench-only. ABAB not needed here (we compare ON@steal_ms vs the per-scenario
# OFF baseline and across steal_ms; each ON run is load-gated + load1-recorded).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
DUR="${DUR:-20s}"
REPS="${REPS:-3}"
AFF_ON_SCHEME="${AFF_ON_SCHEME:-1}"
STEAL_MS_LIST="${STEAL_MS_LIST:-5 20 50 100 250}"
SCENARIOS=( ${TUNE_SCENARIOS:-s5 s10} )
SEED_SKUS="${SEED_SKUS:-1000}"; SEED_LOCS="${SEED_LOCS:-10}"; SEED_COUNT="${SEED_COUNT:-10000}"
OUT="${RESULTS_DIR}/affinity_steal_tune.csv"
SWEEP_LOG="${RESULTS_DIR}/affinity_steal_tune.log"
log() { echo "[stealtune] $*" | tee -a "$SWEEP_LOG" >&2; }
depth_for() { case "$1" in s5|s6) echo 10 ;; s7|s8|s9) echo 1000 ;; s11|s15|s19) echo 100 ;; s14|s16|s17|s18|s20|s21) echo 10 ;; *) echo 0 ;; esac; }

oneline_get() { python3 -c "import sys,json,re
s=sys.stdin.read()
m=re.search(r'(\{.*\})', s, re.S)
d=json.loads(m.group(1)) if m else {}
print(d.get('$2',''))" <<<"$1"; }

psql_v3() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }
set_scheme() { # $1=scheme $2=steal_ms
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.affinity_scheme = $1" >/dev/null
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.affinity_steal_ms = $2" >/dev/null
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT pg_reload_conf()" >/dev/null
}
owned_total() { psql_v3 "SELECT ledger_routed_c_affinity_owned_claims_total()"; }
steals_total() { psql_v3 "SELECT ledger_routed_c_affinity_steals_total()"; }
restore_defaults() { set_scheme 0 5 >/dev/null 2>&1 || true; }
trap restore_defaults EXIT

clean_seed() {
  local sid="$1" depth; depth="$(depth_for "$sid")"
  docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2
  docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
  ( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
  restart_db
  "$BIN" --dsn "$DIRECT_DSN" run --scenario "$sid" --mode direct-per-call --duration 1s \
    --max-callers 1 --depth "$depth" --method-mix all-fifo --seed-count "$SEED_COUNT" \
    --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" --seed-depth "$depth" \
    --no-sampler --output "${RESULTS_DIR}/.tune-seed-$sid.json" >/dev/null
}

# $1=sid $2=depth $3=dsn $4=rep $5=arm $6=scheme $7=steal_ms
run_one() {
  local sid="$1" depth="$2" dsn="$3" rep="$4" arm="$5" scheme="$6" steal="$7"
  set_scheme "$scheme" "$steal"
  local o0=0 s0=0; if [ "$arm" = "on" ]; then o0="$(owned_total)"; s0="$(steals_total)"; fi
  local out="${RESULTS_DIR}/.tune_${sid}_${arm}_sm${steal}_r${rep}.json"
  wait_for_quiet_host || log "  NOTE: $sid $arm sm=$steal r$rep busy host"
  log "RUN $sid $arm scheme=$scheme steal_ms=$steal rep=$rep/$REPS (load1=$(host_load1))"
  local line
  line="$(timeout 360 "$BIN" --dsn "$dsn" run --scenario "$sid" --mode routed --duration "$DUR" \
    --depth "$depth" --output "$out" 2>/dev/null)" || { log "  FAIL $sid $arm sm=$steal r$rep"; return 1; }
  local le o1=0 s1=0 od=0 sd=0; le="$(host_load1)"
  if [ "$arm" = "on" ]; then o1="$(owned_total)"; s1="$(steals_total)"; od=$((o1-o0)); sd=$((s1-s0)); fi
  local tput cg lob rob wob cbf csm
  tput="$(oneline_get "$line" throughput_trx_per_sec)"; cg="$(oneline_get "$line" commit_group_avg)"
  cbf="$(oneline_get "$line" committer_busy_frac)"
  lob="$(oneline_get "$line" committer_lock_frac_of_busy)"
  rob="$(oneline_get "$line" committer_running_frac_of_busy)"
  wob="$(oneline_get "$line" committer_lwlock_frac_of_busy)"
  csm="$(oneline_get "$line" committer_samples)"
  echo "$sid,$rep,$arm,$scheme,$steal,$tput,$cg,$cbf,$lob,$rob,$wob,$csm,$od,$sd,$le" >> "$OUT"
  log "  $sid $arm sm=$steal r$rep tput=$tput busy=$cbf lock=$lob lw=$wob owned+=$od steals+=$sd load=$le"
}

build_harness
log "=== affinity steal_ms TUNE (acct-0usf STEP 3 V0) scenarios=${SCENARIOS[*]} steal_ms=[$STEAL_MS_LIST] reps=$REPS ==="
echo "scenario,rep,arm,scheme,steal_ms,throughput_trx_s,commit_group_avg,committer_busy_frac,lock_of_busy,running_of_busy,lwlock_of_busy,committer_samples,owned_delta,steal_delta,load1_end" > "$OUT"

for sid in "${SCENARIOS[@]}"; do
  depth="$(depth_for "$sid")"; dsn="$(dsn_for_scenario "$sid")"
  log "--- $sid : clean+seed (depth=$depth) ---"
  clean_seed "$sid"
  for rep in $(seq 1 "$REPS"); do
    run_one "$sid" "$depth" "$dsn" "$rep" off 0 5 || true            # OFF baseline (steal_ms n/a)
    for sm in $STEAL_MS_LIST; do
      run_one "$sid" "$depth" "$dsn" "$rep" on "$AFF_ON_SCHEME" "$sm" || true
    done
  done
done
restore_defaults
log "=== done. CSV: $OUT ==="
column -s, -t "$OUT" | tee -a "$SWEEP_LOG" >&2 || cat "$OUT"
