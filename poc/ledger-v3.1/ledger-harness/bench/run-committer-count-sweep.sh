#!/usr/bin/env bash
# run-committer-count-sweep.sh — test whether COMMITTER PARALLELISM is the real
# routed throughput dial (acct-235v follow-on). The batch_window_us sweep showed
# throughput is flat vs batch size because per-group overhead (locks, aggregate
# upserts, commits) is already collapsed to ~0 at cg~27 — the ceiling is
# per-submission committer work. If that's right, throughput should scale with
# committer_count on MULTI-POOL scenarios (work spreads across distinct pools),
# but NOT on the single-hot-pool s5 (all committers serialize on pool[0]'s
# FOR UPDATE — the genuine single-pool limit).
#
# committer_count is read once at _PG_init (GUC doc), so each setting needs a
# postmaster RESTART to respawn the worker pool — done here via ALTER SYSTEM +
# restart_db. batch_window_us held at the 500us default throughout.
#
# Bench-only. One clean+seed per (scenario, committer_count); committer canary.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
DUR="${DUR:-30s}"
COUNTS="${COUNTS:-2 4 8 16}"
SCENARIOS=( ${CC_SCENARIOS:-s7 s10 s5} )   # s5 = single-pool control (should NOT scale)
SEED_SKUS="${SEED_SKUS:-1000}"; SEED_LOCS="${SEED_LOCS:-10}"; SEED_COUNT="${SEED_COUNT:-10000}"
OUT="${RESULTS_DIR}/committer_count_sweep.csv"
SWEEP_LOG="${RESULTS_DIR}/committer_count_sweep.log"
log() { echo "[ccsweep] $*" | tee -a "$SWEEP_LOG" >&2; }
depth_for() { case "$1" in s5|s6) echo 10 ;; s7|s8|s9) echo 1000 ;; s11|s15|s19) echo 100 ;; s14|s16|s17|s18|s20|s21) echo 10 ;; *) echo 0 ;; esac; }
jget() { grep -oE "\"$2\"[[:space:]]*:[[:space:]]*[0-9.]+" "$1" | head -1 | grep -oE '[0-9.]+$'; }

set_committers() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.committer_count = $1" >/dev/null; }
restore_defaults() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.committer_count = 4" >/dev/null 2>&1 || true; }
trap restore_defaults EXIT

clean_seed() { # $1=sid  — fresh DB + extension + restart (also respawns committers at the SET count) + seed
  local sid="$1" depth; depth="$(depth_for "$sid")"
  docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2
  docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
  ( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
  restart_db   # respawns the committer pool at the ALTER SYSTEM count
  "$BIN" --dsn "$DIRECT_DSN" run --scenario "$sid" --mode direct-per-call --duration 1s \
    --max-callers 1 --depth "$depth" --method-mix all-fifo --seed-count "$SEED_COUNT" \
    --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" --seed-depth "$depth" \
    --no-sampler --output "${RESULTS_DIR}/.ccsweep-seed-$sid.json" >/dev/null
}

committer_ready() { local sid="$1" dsn cj; dsn="$(dsn_for_scenario "$sid")"; cj="${RESULTS_DIR}/.ccsweep-canary-$sid.json"
  timeout 90 "$BIN" --dsn "$dsn" run --scenario "$sid" --mode routed --duration 3s --max-callers 8 \
    --depth "$(depth_for "$sid")" --no-sampler --output "$cj" >/dev/null 2>&1 || true
  local c; c="$(jget "$cj" trx_committed_total || echo 0)"; [ "${c%%.*}" -gt 0 ] 2>/dev/null; }

# verify the committer pool actually came up at the requested size.
# The pool size lives in backend_type ('ledger_routed_c_committer_N'), NOT in
# application_name (which is empty for these BGWorkers — a detection gotcha).
running_committers() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc \
  "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'ledger_routed_c_committer%'" 2>/dev/null | tr -d '[:space:]'; }

build_harness
log "=== committer_count sweep (acct-235v follow-on) ==="
log "scenarios=${SCENARIOS[*]} counts=[$COUNTS] dur=$DUR window=500us(default)"
echo "scenario,committer_count,committers_seen,callers,depth,throughput_trx_s,commit_group_avg,commits_per_s,pool_lock_per_trx,trx_materialized,errors,ack_p50_us,ack_p95_us,ack_p99_us,load1_end" > "$OUT"

for sid in "${SCENARIOS[@]}"; do
  depth="$(depth_for "$sid")"; dsn="$(dsn_for_scenario "$sid")"
  for n in $COUNTS; do
    log "--- $sid committer_count=$n : set + clean + restart + seed ---"
    set_committers "$n"
    clean_seed "$sid"
    seen="$(running_committers)"; log "  committers running: $seen (requested $n)"
    committer_ready "$sid" || log "  WARN: $sid canary did not drain"
    out="${RESULTS_DIR}/ccsweep_${sid}_n${n}.json"
    wait_for_quiet_host || log "  NOTE: $sid cc=$n ran on a busy host (load gate timed out)"
    log "RUN $sid cc=$n dur=$DUR -> $(basename "$out") (load1=$(host_load1))"
    timeout 360 "$BIN" --dsn "$dsn" run --scenario "$sid" --mode routed --duration "$DUR" \
      --depth "$depth" --output "$out" >/dev/null 2>&1 || { log "  FAIL $sid cc=$n"; continue; }
    load_end="$(host_load1)"
    dur_s="$(jget "$out" duration_secs)"; trx="$(jget "$out" trx_materialized || jget "$out" trx_committed_total)"
    cg="$(jget "$out" commit_group_size_avg)"; drains="$(jget "$out" drains_total)"
    lock="$(jget "$out" pool_lock_acquisitions_total)"; tput="$(jget "$out" throughput_trx_per_sec)"; err="$(jget "$out" errors_total)"
    cps="$(python3 -c "print(f'{$drains/$dur_s:.1f}')" 2>/dev/null||echo)"
    lpt="$(python3 -c "print(f'{$lock/$trx:.2f}' if $trx else '')" 2>/dev/null||echo)"
    p50="$(python3 -c "import json;print(json.load(open('$out'))['ack_latency_us']['p50'])")"
    p95="$(python3 -c "import json;print(json.load(open('$out'))['ack_latency_us']['p95'])")"
    p99="$(python3 -c "import json;print(json.load(open('$out'))['ack_latency_us']['p99'])")"
    callers="$(python3 -c "import json;print(json.load(open('$out'))['callers'])")"
    echo "$sid,$n,$seen,$callers,$depth,$tput,$cg,$cps,$lpt,$trx,$err,$p50,$p95,$p99,$load_end" >> "$OUT"
    log "  cc=$n cg=$cg tput=$tput commits/s=$cps ack_p99=${p99}us load1_end=$load_end"
  done
done
restore_defaults
log "=== done. CSV: $OUT ==="
column -s, -t "$OUT" | tee -a "$SWEEP_LOG" >&2 || cat "$OUT"
