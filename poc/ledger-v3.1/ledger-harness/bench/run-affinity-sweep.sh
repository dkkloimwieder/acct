#!/usr/bin/env bash
# run-affinity-sweep.sh — acct-0usf STEP 3 affinity OFF-vs-ON sweep driver.
#
# Measures whether committer→pool affinity (acct-0usf STEP 3, GUC
# ledger_routed_c.affinity_scheme, default off) shrinks the committer lock-wait
# handoff and converts it to throughput, against the STEP 2 pre-registered
# hypotheses H1/H2/H3 (results/POC-REPORT.md "### acct-0usf STEP 2" block).
#
# Reusable across variants V0–V3: AFF_ON_SCHEME selects the ON-arm scheme value
# (V0 min-pool = 1). The OFF arm is always scheme 0. Per-arm comparison is done
# against the OFF arm measured IN THIS SAME SWEEP (matched committer_count + load),
# never against the STEP 1 cc=4 profile table — per the STEP 2 comparison rules.
#
# Methodology (the STEP 2 rigor bar):
#   - Only the 13 CANDIDATE scenarios. s6 (lock 0%) and s7 (lock 9%) are a-priori
#     SKIP per STEP 1 (not lock-bound; affinity moot) and are NOT run here.
#   - N reps/cell (default 5). One clean_seed per scenario (reps reuse the seeded
#     universe; receipts self-replenish, depletions are deep-seeded).
#   - ABAB INTERLEAVING: each rep runs OFF then ON back-to-back so host-load drift
#     cannot alias onto one arm. affinity_scheme is Sighup (ALTER SYSTEM +
#     pg_reload_conf) — committers re-read it live, no restart between arms.
#   - Load-gated per arm on a quiet host; load1 recorded per run.
#   - Engagement counters (owned_claims/steals) snapshotted around each ON run so
#     we can PROVE affinity engaged (steals≈0 = pinning; steals≈owned = degraded
#     to OFF) rather than infer it from throughput.
#
# Instruments consumed: STEP 1a committer pipeline spans + STEP 1b committer-
# segmented wait sampler (both live in the bench .so). Requires the V0 .so
# (a106e98+) installed: it adds affinity_scheme/affinity_steal_ms GUCs and the
# ledger_routed_c_affinity_{owned_claims,steals}_total accessors.
#
# Bench-only. Default config never changes the production path (scheme stays 0
# except inside the ON arm of this script).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
DUR="${DUR:-20s}"
REPS="${REPS:-5}"
AFF_ON_SCHEME="${AFF_ON_SCHEME:-1}"     # ON-arm scheme: V0 min-pool = 1
AFF_STEAL_MS="${AFF_STEAL_MS:-5}"
# 13 CANDIDATE scenarios only (STEP 1 verdict). s6/s7 excluded a-priori.
SCENARIOS=( ${AFF_SCENARIOS:-s5 s8 s9 s10 s11 s14 s15 s16 s17 s18 s19 s20 s21} )
SEED_SKUS="${SEED_SKUS:-1000}"; SEED_LOCS="${SEED_LOCS:-10}"; SEED_COUNT="${SEED_COUNT:-10000}"
OUT="${RESULTS_DIR}/affinity_sweep_v${AFF_ON_SCHEME}.csv"
SWEEP_LOG="${RESULTS_DIR}/affinity_sweep_v${AFF_ON_SCHEME}.log"
log() { echo "[affsweep] $*" | tee -a "$SWEEP_LOG" >&2; }
depth_for() { case "$1" in s5|s6) echo 10 ;; s7|s8|s9) echo 1000 ;; s11|s15|s19) echo 100 ;; s14|s16|s17|s18|s20|s21) echo 10 ;; *) echo 0 ;; esac; }

oneline_get() { python3 -c "import sys,json,re
s=sys.stdin.read()
m=re.search(r'(\{.*\})', s, re.S)
d=json.loads(m.group(1)) if m else {}
print(d.get('$2',''))" <<<"$1"; }

psql_v3() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }
set_scheme() { # $1=scheme int
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.affinity_scheme = $1" >/dev/null
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.affinity_steal_ms = $AFF_STEAL_MS" >/dev/null
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT pg_reload_conf()" >/dev/null
}
owned_total() { psql_v3 "SELECT ledger_routed_c_affinity_owned_claims_total()"; }
steals_total() { psql_v3 "SELECT ledger_routed_c_affinity_steals_total()"; }
restore_defaults() { set_scheme 0 >/dev/null 2>&1 || true; }
trap restore_defaults EXIT

clean_seed() { # $1=sid — fresh DB + extension + restart (respawns committers) + seed
  local sid="$1" depth; depth="$(depth_for "$sid")"
  docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2
  docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
  ( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
  restart_db   # cold shmem + respawn committers (DROP DATABASE wedges the committer; restart mandatory). Resets engagement counters to 0.
  "$BIN" --dsn "$DIRECT_DSN" run --scenario "$sid" --mode direct-per-call --duration 1s \
    --max-callers 1 --depth "$depth" --method-mix all-fifo --seed-count "$SEED_COUNT" \
    --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" --seed-depth "$depth" \
    --no-sampler --output "${RESULTS_DIR}/.aff-seed-$sid.json" >/dev/null
}

running_committers() { psql_v3 "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'ledger_routed_c_committer%'"; }

# One measured run. $1=sid $2=depth $3=dsn $4=rep $5=arm(off|on) $6=scheme
run_arm() {
  local sid="$1" depth="$2" dsn="$3" rep="$4" arm="$5" scheme="$6"
  set_scheme "$scheme"
  local owned0=0 steals0=0
  if [ "$arm" = "on" ]; then owned0="$(owned_total)"; steals0="$(steals_total)"; fi
  local out="${RESULTS_DIR}/.aff_${sid}_r${rep}_${arm}.json"
  wait_for_quiet_host || log "  NOTE: $sid r$rep $arm on a busy host (load gate timed out)"
  log "RUN $sid rep=$rep/$REPS arm=$arm scheme=$scheme dur=$DUR (load1=$(host_load1))"
  local line
  line="$(timeout 360 "$BIN" --dsn "$dsn" run --scenario "$sid" --mode routed --duration "$DUR" \
    --depth "$depth" --output "$out" 2>/dev/null)" || { log "  FAIL $sid r$rep $arm"; return 1; }
  local load_end owned1=0 steals1=0 od=0 sd=0
  load_end="$(host_load1)"
  if [ "$arm" = "on" ]; then
    owned1="$(owned_total)"; steals1="$(steals_total)"
    od=$(( owned1 - owned0 )); sd=$(( steals1 - steals0 ))
  fi
  local tput cg callers spl shy sap sco spr cbf lob rob wob csm
  tput="$(oneline_get "$line" throughput_trx_per_sec)"
  cg="$(oneline_get "$line" commit_group_avg)"
  callers="$(python3 -c "import json;print(json.load(open('$out'))['callers'])" 2>/dev/null || echo)"
  spl="$(oneline_get "$line" span_pool_lock_frac)";  shy="$(oneline_get "$line" span_hydrate_frac)"
  sap="$(oneline_get "$line" span_apply_frac)";      sco="$(oneline_get "$line" span_commit_frac)"
  spr="$(oneline_get "$line" span_prep_frac)";       cbf="$(oneline_get "$line" committer_busy_frac)"
  lob="$(oneline_get "$line" committer_lock_frac_of_busy)"
  rob="$(oneline_get "$line" committer_running_frac_of_busy)"
  wob="$(oneline_get "$line" committer_lwlock_frac_of_busy)"
  csm="$(oneline_get "$line" committer_samples)"
  echo "$sid,$rep,$arm,$scheme,$callers,$depth,$tput,$cg,$spl,$shy,$sap,$sco,$spr,$cbf,$lob,$rob,$wob,$csm,$od,$sd,$load_end" >> "$OUT"
  log "  $sid r$rep $arm tput=$tput cg=$cg busy=$cbf of_busy(lock=$lob run=$rob lw=$wob) owned+=$od steals+=$sd load=$load_end"
}

build_harness
log "=== affinity OFF/ON sweep (acct-0usf STEP 3, ON-scheme=$AFF_ON_SCHEME steal_ms=$AFF_STEAL_MS) ==="
log "scenarios=${SCENARIOS[*]} reps=$REPS dur=$DUR committer_count=4 sampler=ON ABAB-interleaved"
echo "scenario,rep,arm,scheme,callers,depth,throughput_trx_s,commit_group_avg,span_pool_lock,span_hydrate,span_apply,span_commit,span_prep,committer_busy_frac,lock_of_busy,running_of_busy,lwlock_of_busy,committer_samples,owned_delta,steal_delta,load1_end" > "$OUT"

for sid in "${SCENARIOS[@]}"; do
  depth="$(depth_for "$sid")"; dsn="$(dsn_for_scenario "$sid")"
  log "--- $sid : clean+seed (depth=$depth dsn=$(basename "$dsn")) ---"
  clean_seed "$sid"
  seen="$(running_committers)"; log "  committers running: $seen (expect 4)"
  for rep in $(seq 1 "$REPS"); do
    run_arm "$sid" "$depth" "$dsn" "$rep" off 0              || true   # A
    run_arm "$sid" "$depth" "$dsn" "$rep" on "$AFF_ON_SCHEME" || true  # B  (ABAB across reps)
  done
done
restore_defaults
log "=== done. CSV: $OUT ==="
column -s, -t "$OUT" | tee -a "$SWEEP_LOG" >&2 || cat "$OUT"
