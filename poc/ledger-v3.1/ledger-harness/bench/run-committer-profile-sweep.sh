#!/usr/bin/env bash
# run-committer-profile-sweep.sh — the acct-0usf STEP 1 deliverable: a per-scenario
# "where does committer wall-time go" table, measured directly, affinity OFF.
#
# This is the methodology fix the acct-xdwk lever-2 affinity pass lacked. That
# pass INFERRED its bottleneck ("cross-committer pool_lock FOR UPDATE handoff
# dominates") from a single pg_stat_statements snapshot on a single-hot-pool run,
# then generalized it to all workloads. Here we MEASURE the committer pipeline
# decomposition on every scenario, with two independent instruments:
#
#   STEP 1a in-process spans  (committer_*_ns_total): wall-time fractions of the
#     whole committer transaction — pool_lock / hydrate / apply / commit / prep.
#   STEP 1b committer-segmented wait sampler: of BUSY committer-time, the
#     lock-wait (the handoff affinity could remove) vs on-CPU running (irreducible)
#     vs LWLock shmem-ring contention (a separate bottleneck) vs IO split, plus
#     committer pool utilization (busy_frac).
#
# A scenario whose lock_frac_of_busy is small is one where committer→pool affinity
# is a priori pointless — record it and skip it in the STEP 3 affinity variants.
#
# Fixed config (this is a PROFILE, not a knob sweep): committer_count=4, affinity
# OFF (the GUC was reverted with lever 2; nothing to set), router_pack_disjoint
# left at its installed default, sampler ON (we want the breakdown, not peak TPS).
#
# N reps per scenario, ONE clean_seed per scenario (reps reuse the seeded
# universe; receipt scenarios self-replenish the aggregate and depletion
# scenarios are deep-seeded so N×duration can't exhaust them). Each rep is
# load-gated on a quiet host (daily-driver workstation; external CPU swings
# routed throughput ~2x). The structural span/wait FRACTIONS are far more
# load-robust than absolute throughput — that is the point of measuring fractions.
#
# Bench-only. No production-path change.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
DUR="${DUR:-20s}"
REPS="${REPS:-5}"
# Full contention spectrum: single-hot (s5), disjoint (s6), deep-zipf (s7-9),
# Pareto receipts (s10-11), Pareto builds (s14-17), Pareto mixed (s18-21).
SCENARIOS=( ${PROFILE_SCENARIOS:-s5 s6 s7 s8 s9 s10 s11 s14 s15 s16 s17 s18 s19 s20 s21} )
SEED_SKUS="${SEED_SKUS:-1000}"; SEED_LOCS="${SEED_LOCS:-10}"; SEED_COUNT="${SEED_COUNT:-10000}"
OUT="${RESULTS_DIR}/committer_profile_sweep.csv"
SWEEP_LOG="${RESULTS_DIR}/committer_profile_sweep.log"
log() { echo "[profile] $*" | tee -a "$SWEEP_LOG" >&2; }
depth_for() { case "$1" in s5|s6) echo 10 ;; s7|s8|s9) echo 1000 ;; s11|s15|s19) echo 100 ;; s14|s16|s17|s18|s20|s21) echo 10 ;; *) echo 0 ;; esac; }

# Pull a numeric field out of the harness STDOUT one-liner (robust: the one-liner
# is single-line flat JSON; avoids the nested-report jget fragility).
oneline_get() { python3 -c "import sys,json,re
s=sys.stdin.read()
m=re.search(r'(\{.*\})', s, re.S)
d=json.loads(m.group(1)) if m else {}
print(d.get('$2',''))" <<<"$1"; }

clean_seed() { # $1=sid — fresh DB + extension + restart (respawns committers) + seed
  local sid="$1" depth; depth="$(depth_for "$sid")"
  docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2
  docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
  ( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
  restart_db   # cold shmem + respawn committer pool (DROP DATABASE wedges the committer; restart is mandatory)
  "$BIN" --dsn "$DIRECT_DSN" run --scenario "$sid" --mode direct-per-call --duration 1s \
    --max-callers 1 --depth "$depth" --method-mix all-fifo --seed-count "$SEED_COUNT" \
    --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" --seed-depth "$depth" \
    --no-sampler --output "${RESULTS_DIR}/.profile-seed-$sid.json" >/dev/null
}

running_committers() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'ledger_routed_c_committer%'" 2>/dev/null | tr -d '[:space:]'; }

build_harness
# Fixed-config profile: assert the ambient regime IS the production default before
# logging cc=4/affinity=OFF as fact — a stale pin would silently mislabel the CSV.
assert_routed_gucs
log "=== committer pipeline PROFILE sweep (acct-0usf STEP 1 deliverable) ==="
log "scenarios=${SCENARIOS[*]} reps=$REPS dur=$DUR committer_count=4 affinity=OFF sampler=ON"
# CSV: one row per (scenario, rep). Span fracs are of the whole committer txn;
# committer_*_of_busy are of non-idle committer samples.
echo "scenario,rep,callers,depth,throughput_trx_s,commit_group_avg,span_pool_lock,span_hydrate,span_apply,span_commit,span_prep,committer_busy_frac,lock_of_busy,running_of_busy,lwlock_of_busy,committer_samples,load1_end" > "$OUT"

for sid in "${SCENARIOS[@]}"; do
  depth="$(depth_for "$sid")"; dsn="$(dsn_for_scenario "$sid")"
  log "--- $sid : clean+seed (depth=$depth dsn=$(basename "$dsn")) ---"
  clean_seed "$sid"
  seen="$(running_committers)"; log "  committers running: $seen (expect 4)"
  for rep in $(seq 1 "$REPS"); do
    out="${RESULTS_DIR}/.profile_${sid}_r${rep}.json"
    wait_for_quiet_host || log "  NOTE: $sid r$rep ran on a busy host (load gate timed out)"
    log "RUN $sid rep=$rep/$REPS dur=$DUR (load1=$(host_load1))"
    # Sampler ON (no --no-sampler) so the committer-segmented wait histogram is
    # populated. Capture STDOUT (the flat one-liner) for the span+wait fields.
    line="$(timeout 360 "$BIN" --dsn "$dsn" run --scenario "$sid" --mode routed --duration "$DUR" \
      --depth "$depth" --output "$out" 2>/dev/null)" || { log "  FAIL $sid r$rep"; continue; }
    load_end="$(host_load1)"
    tput="$(oneline_get "$line" throughput_trx_per_sec)"
    cg="$(oneline_get "$line" commit_group_avg)"
    callers="$(python3 -c "import json;print(json.load(open('$out'))['callers'])" 2>/dev/null || echo)"
    spl="$(oneline_get "$line" span_pool_lock_frac)"
    shy="$(oneline_get "$line" span_hydrate_frac)"
    sap="$(oneline_get "$line" span_apply_frac)"
    sco="$(oneline_get "$line" span_commit_frac)"
    spr="$(oneline_get "$line" span_prep_frac)"
    cbf="$(oneline_get "$line" committer_busy_frac)"
    lob="$(oneline_get "$line" committer_lock_frac_of_busy)"
    rob="$(oneline_get "$line" committer_running_frac_of_busy)"
    wob="$(oneline_get "$line" committer_lwlock_frac_of_busy)"
    csm="$(oneline_get "$line" committer_samples)"
    echo "$sid,$rep,$callers,$depth,$tput,$cg,$spl,$shy,$sap,$sco,$spr,$cbf,$lob,$rob,$wob,$csm,$load_end" >> "$OUT"
    log "  $sid r$rep tput=$tput cg=$cg | span(lock=$spl apply=$sap commit=$sco) | busy=$cbf of_busy(lock=$lob run=$rob lw=$wob) samples=$csm load=$load_end"
  done
done
log "=== done. CSV: $OUT ==="
column -s, -t "$OUT" | tee -a "$SWEEP_LOG" >&2 || cat "$OUT"
