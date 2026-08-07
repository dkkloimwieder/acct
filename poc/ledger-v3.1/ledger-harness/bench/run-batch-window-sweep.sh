#!/usr/bin/env bash
# run-batch-window-sweep.sh — characterize the routed throughput<->latency dial
# (acct-235v). Holds load fixed on a hot scenario and sweeps
# `ledger_routed_c.batch_window_us`, retuned LIVE (ALTER SYSTEM SET +
# pg_reload_conf — the GUC is GucContext::Sighup, read per-tick via
# batch_window_us_now()), capturing commit_group_size_avg / throughput /
# ack-latency p50/p95/p99 at each setting.
#
# The window gate: the router defers a tick when the oldest pending candidate has
# waited < batch_window_us, letting more pool-overlapping submissions accumulate
# into one commit_group. Larger window => larger cg => fewer pool_locks/upserts
# per trx => higher throughput, at the cost of added ack latency. window=0
# disables the gate (drain-as-fast-as-possible, smallest groups, lowest latency).
#
# Hot scenarios only — the dial has leverage where there IS cross-caller overlap
# to coalesce (s5 single-hot-pool, s7 deep-zipf). On disjoint workloads (s6)
# there is nothing to batch and the window does nothing but add latency.
#
# Bench-only. No clean-DB per step (we want the SAME warm universe across window
# settings so the only independent variable is batch_window_us). One clean+seed
# at the start per scenario; committer-readiness canary; then the window sweep.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
DUR="${DUR:-30s}"                     # per-window measurement length
WINDOWS="${WINDOWS:-0 250 500 1000 2000 5000 10000}"   # batch_window_us settings
SCENARIOS=( ${SWEEP_SCENARIOS:-s5 s7} )
SEED_SKUS="${SEED_SKUS:-1000}"
SEED_LOCS="${SEED_LOCS:-10}"
SEED_COUNT="${SEED_COUNT:-10000}"
OUT="${RESULTS_DIR}/batch_window_sweep.csv"
SWEEP_LOG="${RESULTS_DIR}/batch_window_sweep.log"

log() { echo "[bwsweep] $*" | tee -a "$SWEEP_LOG" >&2; }

# Crossover seed depths (mirrors run-routed-longdur.sh). Deplete scenarios MUST
# seed >0 or depletions hit an empty universe and the committer drops ~every
# submission (drop-and-continue) — valid behavior, but not a throughput cell.
depth_for() {
  case "$1" in
    s5|s6)                   echo 10 ;;
    s7|s8|s9)                echo 1000 ;;
    s11|s15|s19)             echo 100 ;;
    s14|s16|s17|s18|s20|s21) echo 10 ;;
    *)                       echo 0 ;;   # s1-s4, s10/s12/s13 = receipts-only
  esac
}

set_window() {
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc \
    "ALTER SYSTEM SET ledger_routed_c.batch_window_us = $1" >/dev/null
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT pg_reload_conf()" >/dev/null
  # Confirm it took (read-back); the router picks it up on its next tick.
  local got
  got="$(docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SHOW ledger_routed_c.batch_window_us" | tr -d '[:space:]')"
  [ "$got" = "$1" ] || log "WARN: requested window=$1 but SHOW=$got"
}

restore_window_default() { set_window 500; }
trap restore_window_default EXIT

# Pull a numeric field out of a routed report json (pretty JSON: "k": N).
jget() { grep -oE "\"$2\"[[:space:]]*:[[:space:]]*[0-9.]+" "$1" | head -1 | grep -oE '[0-9.]+$'; }
jgetn() { # nested: ack_latency_us.pXX
  python3 - "$1" "$2" "$3" <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); print(d.get(sys.argv[2],{}).get(sys.argv[3],0))
PY
}

committer_ready() {  # drive a short routed canary on $1's dsn; confirm commit>0
  local sid="$1" dsn cj
  dsn="$(dsn_for_scenario "$sid")"; cj="${RESULTS_DIR}/.bwsweep-canary-$sid.json"
  timeout 90 "$BIN" --dsn "$dsn" run --scenario "$sid" --mode routed \
    --duration 3s --max-callers 8 --depth "$(depth_for "$sid")" \
    --no-sampler --output "$cj" >/dev/null 2>&1 || true
  local c; c="$(jget "$cj" trx_committed_total || echo 0)"
  [ "${c%%.*}" -gt 0 ] 2>/dev/null
}

build_harness
log "=== batch_window_us sweep (acct-235v) ==="
log "scenarios=${SCENARIOS[*]} windows(us)=[$WINDOWS] dur=$DUR"
echo "scenario,batch_window_us,callers,depth,throughput_trx_s,commit_group_avg,pool_lock_per_trx,trx_materialized,errors,submitted_unseen,ack_p50_us,ack_p95_us,ack_p99_us" > "$OUT"

for sid in "${SCENARIOS[@]}"; do
  depth="$(depth_for "$sid")"; dsn="$(dsn_for_scenario "$sid")"
  log "--- $sid (depth=$depth) : clean + seed ONCE, then sweep windows ---"
  # One clean+restart+seed so every window setting hits the same warm universe.
  docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2
  docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
  ( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
  restart_db
  "$BIN" --dsn "$DIRECT_DSN" run --scenario "$sid" --mode direct-per-call --duration 1s \
    --max-callers 1 --depth "$depth" --method-mix all-fifo --seed-count "$SEED_COUNT" \
    --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" --seed-depth "$depth" \
    --no-sampler --output "${RESULTS_DIR}/.bwsweep-seed-$sid.json" >/dev/null
  committer_ready "$sid" || log "WARN: $sid committer canary did not drain; results may be 0"

  for w in $WINDOWS; do
    set_window "$w"
    out="${RESULTS_DIR}/bwsweep_${sid}_w${w}.json"
    log "RUN $sid window=${w}us dur=$DUR -> $(basename "$out")"
    timeout 360 "$BIN" --dsn "$dsn" run --scenario "$sid" --mode routed \
      --duration "$DUR" --depth "$depth" --output "$out" >/dev/null 2>&1 || { log "  FAIL $sid w=$w"; continue; }
    tput="$(jget "$out" throughput_trx_per_sec)"
    cg="$(jget "$out" commit_group_size_avg)"
    lock="$(jget "$out" pool_lock_acquisitions_total)"
    mat="$(jget "$out" trx_committed_total)"
    err="$(jget "$out" errors_total)"
    # submitted_but_unseen + ack percentiles come from the top-level + nested objs
    unseen="$(python3 -c "import json;d=json.load(open('$out'));print(d.get('submitted_but_unseen','') or 0)" 2>/dev/null || echo 0)"
    p50="$(jgetn "$out" ack_latency_us p50)"; p95="$(jgetn "$out" ack_latency_us p95)"; p99="$(jgetn "$out" ack_latency_us p99)"
    callers="$(python3 -c "import json;print(json.load(open('$out')).get('callers',''))" 2>/dev/null)"
    lpt="$(python3 -c "print(f'{$lock/$mat:.2f}' if $mat else '')" 2>/dev/null || echo "")"
    echo "$sid,$w,$callers,$depth,$tput,$cg,$lpt,$mat,$err,$unseen,$p50,$p95,$p99" >> "$OUT"
    log "  cg=$cg tput=$tput ack_p99=${p99}us lock/trx=$lpt"
  done
done

restore_window_default
log "=== done. CSV: $OUT ==="
column -s, -t "$OUT" | tee -a "$SWEEP_LOG" >&2 || cat "$OUT"
