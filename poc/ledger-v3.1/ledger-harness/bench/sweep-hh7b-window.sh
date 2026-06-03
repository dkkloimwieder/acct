#!/usr/bin/env bash
# sweep-hh7b-window.sh — acct-hh7b Phase 1: TIME-WINDOW-ONLY commit_group formation.
#
# acct-p1al left open that commit_group size is co-limited by TWO entangled knobs:
# batch_size_max (a hard DOCUMENT-count cap) and batch_window_us (the time gate).
# This harness makes the WINDOW the SOLE gate — batch_size_max is pinned
# non-binding (=100000) and router_pack_disjoint stays on (the p1al production
# default) — then sweeps batch_window_us x committer_count on one workload.
#
# At each cell it captures the usual throughput/cg/latency/named-wait panel (same
# as run-batch-size-sweep) PLUS the window-only failure-mode panel that answers
# "can a pure time gate form an UNBOUNDED group?":
#   - ledger_routed_c_router_max_group_size()       high-water group size
#   - ledger_routed_c_router_submission_histogram()  log2 group-size DISTRIBUTION
#   - ledger_routed_c_arena_outstanding/bump_offset   spillover-arena memory pressure
#   - committer dropped_submissions                   staging backpressure
#
# committer_count is restart-only (read in _PG_init), so it is the OUTER loop:
# set it, then clean_seed (which restart_db's — respawning the committer pool AND
# zeroing the cluster-lifetime stat counters). The window sweep then runs ASCENDING
# within each committer_count, so the cumulative max_group / arena_bump curves are
# monotone-interpretable: the reading after window W reflects the high-water across
# all windows <= W for that committer_count.
#
# Env knobs:
#   SCENARIO     workload (s2 s5 s6 s7 s10 ...)            default s2
#   COMMITTERS   space-separated restart-only counts        default "4"
#   WINDOWS      space-separated batch_window_us values     default "0 500 2000 10000 50000"
#   REPS         reps per cell (load-gated, median later)   default 2
#   CALLERS      override scenario default max-callers       default "" (scenario default)
#   DUR          per-cell measurement length                default 20s
#   SEED_COUNT/SEED_SKUS/SEED_LOCS  universe sizing          default 10000/1000/10
#
# Bench-only. One clean+seed per committer_count; load-gated each rep.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
SCENARIO="${SCENARIO:-s2}"
COMMITTERS="${COMMITTERS:-4}"
WINDOWS="${WINDOWS:-0 500 2000 10000 50000}"
REPS="${REPS:-2}"
CALLERS="${CALLERS:-}"
DUR="${DUR:-20s}"
NONBINDING_CAP="${NONBINDING_CAP:-100000}"   # batch_size_max held non-binding => window is the sole gate
SAMPLER="${SAMPLER:-1}"
SEED_SKUS="${SEED_SKUS:-1000}"; SEED_LOCS="${SEED_LOCS:-10}"; SEED_COUNT="${SEED_COUNT:-10000}"
OUT="${OUT:-${RESULTS_DIR}/hh7b_window_${SCENARIO}.csv}"
SWEEP_LOG="${RESULTS_DIR}/hh7b_window.log"
PARSE="$HERE/parse-committer-sampler.py"
log() { echo "[hh7b] $*" | tee -a "$SWEEP_LOG" >&2; }
depth_for() { case "$1" in s5|s6) echo 10 ;; s7|s8|s9) echo 1000 ;; s11|s15|s19) echo 100 ;; s14|s16|s17|s18|s20|s21) echo 10 ;; *) echo 0 ;; esac; }

asys() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.$1 = $2" >/dev/null; }
reload() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT pg_reload_conf()" >/dev/null; }
restore_defaults() {  # back to the p1al production defaults
  asys batch_size_max 200 2>/dev/null || true
  asys batch_window_us 500 2>/dev/null || true
  asys committer_count 4 2>/dev/null || true
  asys router_pack_disjoint on 2>/dev/null || true
  asys affinity_scheme 0 2>/dev/null || true
  reload 2>/dev/null || true
}
trap restore_defaults EXIT

extract() {  # $1=json -> "tput cg commits_s locks_trx trx p50 p99 drop"
  python3 - "$1" <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); r=d['routed']; a=d['ack_latency_us']; dur=d['duration_secs']
trx=r['trx_committed_total']; dr=r['drains_total']; lk=r['pool_lock_acquisitions_total']
print(f"{d['throughput_trx_per_sec']:.0f} {r['commit_group_size_avg']:.2f} {dr/dur:.1f} "
      f"{(lk/trx if trx else 0):.2f} {trx} {a['p50']} {a['p99']} {r['dropped_submissions_total']}")
PY
}

# max_group | histogram(comma) | arena_outstanding | arena_bump_offset | dropped_total
read_getters() {
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc \
    "SELECT ledger_routed_c_router_max_group_size(), ledger_routed_c_router_submission_histogram(), ledger_routed_c_arena_outstanding(), ledger_routed_c_arena_bump_offset(), ledger_routed_c_committer_dropped_submissions_total()"
}

# DROP DATABASE WITH (FORCE) races the committer BGWorker's reconnect loop: FORCE
# terminates current sessions, but a committer can reconnect to poc_v3_1 in the gap
# and the DROP fails "being accessed by other users". Terminate non-self backends
# then retry a few times — one attempt lands in a reconnect gap.
drop_db_robust() {
  local i
  for i in $(seq 1 10); do
    docker exec "$CONTAINER" psql -U acct -d postgres -tAc \
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='poc_v3_1' AND pid<>pg_backend_pid()" >/dev/null 2>&1 || true
    if docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2 2>/dev/null; then
      return 0
    fi
    log "    drop_db retry $i/10 (committer reconnect race)"; sleep 1
  done
  docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2  # final: surface error
}

clean_seed() {  # fresh universe + restart at current committer_count (zeroes stat counters)
  local depth; depth="$(depth_for "$SCENARIO")"
  drop_db_robust
  docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
  ( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
  restart_db   # respawns committer pool at the ALTER SYSTEM committer_count; zeroes shmem stats
  "$BIN" --dsn "$DIRECT_DSN" run --scenario "$SCENARIO" --mode direct-per-call --duration 1s \
    --max-callers 1 --depth "$depth" --method-mix all-fifo --seed-count "$SEED_COUNT" \
    --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" --seed-depth "$depth" \
    --no-sampler --output "${RESULTS_DIR}/.hh7b-seed.json" >/dev/null
}

committer_ready() { local dsn cj; dsn="$(dsn_for_scenario "$SCENARIO")"; cj="${RESULTS_DIR}/.hh7b-canary.json"
  timeout 90 "$BIN" --dsn "$dsn" run --scenario "$SCENARIO" --mode routed --duration 3s --max-callers 8 \
    --depth "$(depth_for "$SCENARIO")" --no-sampler --output "$cj" >/dev/null 2>&1 || true
  python3 -c "import json;print(json.load(open('$cj'))['routed']['trx_committed_total'])" 2>/dev/null | grep -qvx 0; }

build_harness
log "=== hh7b window-only sweep: scenario=$SCENARIO committers=[$COMMITTERS] windows(us)=[$WINDOWS] reps=$REPS callers=${CALLERS:-default} cap=$NONBINDING_CAP(non-binding) pack=on dur=$DUR ==="
echo "scenario,committers,callers,window_us,batch_size_max,rep,throughput_trx_s,cg_avg,commits_s,locks_per_trx,trx,ack_p50_us,ack_p99_us,dropped,max_group,group_hist,arena_outstanding,arena_bump,busy_frac,lock_of_busy,running_of_busy,lwlock_of_busy,io_of_busy,top_wait_type,top_wait_event,top_wait_samples,load1_end" > "$OUT"

dsn="$(dsn_for_scenario "$SCENARIO")"; depth="$(depth_for "$SCENARIO")"
caller_arg=(); [ -n "$CALLERS" ] && caller_arg=(--max-callers "$CALLERS")

for cc in $COMMITTERS; do
  log "--- committer_count=$cc : set + clean_seed (fresh universe, zeroed counters) ---"
  asys committer_count "$cc"; reload
  clean_seed
  running="$(docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'ledger_routed_c_committer%'" | tr -d '[:space:]')"
  log "    committers running: $running (requested $cc)"
  # Pin the window-only regime: non-binding cap, packing on.
  asys batch_size_max "$NONBINDING_CAP"; asys router_pack_disjoint on; reload
  committer_ready || log "    WARN: cc=$cc canary did not drain"

  for w in $WINDOWS; do
    asys batch_window_us "$w"; reload
    for rep in $(seq 1 "$REPS"); do
      wait_for_quiet_host || log "    NOTE: cc=$cc w=$w rep=$rep ran busy"
      out="${RESULTS_DIR}/hh7bw_${SCENARIO}_cc${cc}_w${w}_r${rep}.json"
      smp="${out%.json}.sampler.txt"
      log "RUN $SCENARIO cc=$cc window=${w}us rep=$rep/$REPS (load1=$(host_load1))"
      LEDGER_V3_1_PRINT_SAMPLER="$SAMPLER" timeout "$HARNESS_TIMEOUT" "$BIN" --dsn "$dsn" run --scenario "$SCENARIO" \
        --mode routed --duration "$DUR" --depth "$depth" "${caller_arg[@]}" --output "$out" >/dev/null 2>&1 \
        || { log "  FAIL cc=$cc w=$w rep=$rep"; continue; }
      read -r tput cg cps lpt trx p50 p99 drop <<<"$(extract "$out")"
      IFS='|' read -r maxg hist aout abump drptot <<<"$(read_getters)"
      hist="${hist//,/;}"   # CSV-safe (histogram is ;-joined buckets)
      if [ "$SAMPLER" = "1" ] && [ -f "$smp" ]; then
        read -r busy lob rob wob iob tw_type tw_event tw_n <<<"$(python3 "$PARSE" "$smp")"
      else
        busy=; lob=; rob=; wob=; iob=; tw_type=none; tw_event=none; tw_n=0
      fi
      le="$(host_load1)"
      echo "$SCENARIO,$cc,${CALLERS:-def},$w,$NONBINDING_CAP,$rep,$tput,$cg,$cps,$lpt,$trx,$p50,$p99,$drop,$maxg,$hist,$aout,$abump,$busy,$lob,$rob,$wob,$iob,$tw_type,$tw_event,$tw_n,$le" >> "$OUT"
      log "  cc=$cc w=$w r$rep cg=$cg tput=$tput max_grp=$maxg hist=$hist ack_p99=${p99}us lk/trx=$lpt arena_bump=$abump drop=$drop wait=${tw_type}/${tw_event} load=$le"
    done
  done
done

restore_defaults
log "=== done. CSV: $OUT ==="
column -s, -t "$OUT" 2>/dev/null | tee -a "$SWEEP_LOG" >&2 || cat "$OUT"
