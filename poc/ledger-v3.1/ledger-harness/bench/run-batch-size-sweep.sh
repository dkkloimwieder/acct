#!/usr/bin/env bash
# run-batch-size-sweep.sh — does making commit_groups LARGER raise routed
# throughput, and WHAT does the committer block on at each size? (acct-czz4
# batch-diag; supersedes the N=1 acct-xdwk lever-1 sweep.)
#
# Throughput on a contended pool ~= trx-per-group / (per-group commit cost), so a
# bigger batch amortizes the per-group cost (FOR UPDATE handoff on hot pools;
# commit/WAL LWLock on disjoint many-pool work) over more trx. This sweep raises
# batch_size_max and measures throughput, achieved cg, locks/trx, ack latency,
# AND the NAMED committer wait event (the mechanism), at N>=5 reps/cell.
#
# Rigor (acct-czz4):
#   - REPS>=5 per cell, each load-gated (common.sh wait_for_quiet_host), load1
#     recorded per rep. One row PER REP; batch-aggregate.py rolls up median+IQR.
#   - LEDGER_V3_1_PRINT_SAMPLER=1 so each rep writes a <output>.sampler.txt;
#     parse-committer-sampler.py extracts the of-busy fractions + the dominant
#     NAMED wait event (largest committer histogram bucket excluding idle/on-CPU).
#
# Knobs swept LIVE (GucContext::Sighup — ALTER SYSTEM + pg_reload_conf, no
# restart): ledger_routed_c.batch_size_max (the per-group cap). batch_window_us
# is held WIDE ($WINDOW) so the cap — not the coalesce window — is the binding
# constraint: with a wide window the router accumulates a deep backlog per tick
# and packs up to batch_size_max into each group.
#
# committer_count is set ONCE (restart-only GUC); COMMITTERS=1 = "single drain",
# which removes cross-committer FOR UPDATE contention entirely, isolating the
# pure batching/commit-amortization effect.
#
# Bench-only. One clean+seed per run (after committer_count is set); load-gated.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
SCENARIO="${SCENARIO:-s5}"
COMMITTERS="${COMMITTERS:-4}"
SIZES="${SIZES:-50 100 200 400 800}"
REPS="${REPS:-5}"                     # acct-czz4: N>=5 reps/cell, load-gated each
SAMPLER="${SAMPLER:-1}"               # 1 => dump committer wait histogram per rep
WINDOW="${WINDOW:-20000}"            # batch_window_us held wide so batch_size_max binds
PACK="${PACK:-off}"                   # router_pack_disjoint: on packs disjoint pool-components (acct-xdwk lever 1b)
AFFINITY_SCHEME="${AFFINITY_SCHEME:-0}"     # acct-0usf: 0=off (default), 1=min_pool committer→pool pin
AFFINITY_STEAL_MS="${AFFINITY_STEAL_MS:-5}" # high (e.g. 60000 >> run) => CLEAN static pin: owner never starves, non-owners never steal
DUR="${DUR:-20s}"
SEED_SKUS="${SEED_SKUS:-1000}"; SEED_LOCS="${SEED_LOCS:-10}"; SEED_COUNT="${SEED_COUNT:-10000}"
OUT="${OUT:-${RESULTS_DIR}/batchdiag_${SCENARIO}_cc${COMMITTERS}_pack${PACK}.csv}"
SWEEP_LOG="${RESULTS_DIR}/batchdiag.log"
PARSE="$HERE/parse-committer-sampler.py"
AGG="$HERE/batch-aggregate.py"
log() { echo "[batchdiag] $*" | tee -a "$SWEEP_LOG" >&2; }
depth_for() { case "$1" in s5|s6) echo 10 ;; s7|s8|s9) echo 1000 ;; s11|s15|s19) echo 100 ;; s14|s16|s17|s18|s20|s21) echo 10 ;; *) echo 0 ;; esac; }

asys() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.$1 = $2" >/dev/null; }
reload() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT pg_reload_conf()" >/dev/null; }
restore_defaults() {
  asys batch_size_max 50 2>/dev/null || true
  asys batch_window_us 500 2>/dev/null || true
  asys committer_count 4 2>/dev/null || true
  asys router_pack_disjoint off 2>/dev/null || true
  asys affinity_scheme 0 2>/dev/null || true
  asys affinity_steal_ms 5 2>/dev/null || true
  reload 2>/dev/null || true
}
trap restore_defaults EXIT

extract() {  # $1=json -> echoes "tput cg commits_s locks_trx trx p50 p99 drop"
  python3 - "$1" <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); r=d['routed']; a=d['ack_latency_us']; dur=d['duration_secs']
trx=r['trx_committed_total']; dr=r['drains_total']; lk=r['pool_lock_acquisitions_total']
print(f"{d['throughput_trx_per_sec']:.0f} {r['commit_group_size_avg']:.2f} {dr/dur:.1f} "
      f"{(lk/trx if trx else 0):.2f} {trx} {a['p50']} {a['p99']} {r['dropped_submissions_total']}")
PY
}

clean_seed() {
  local depth; depth="$(depth_for "$SCENARIO")"
  docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2
  docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
  ( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
  restart_db   # respawns committer pool at the ALTER SYSTEM committer_count
  "$BIN" --dsn "$DIRECT_DSN" run --scenario "$SCENARIO" --mode direct-per-call --duration 1s \
    --max-callers 1 --depth "$depth" --method-mix all-fifo --seed-count "$SEED_COUNT" \
    --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" --seed-depth "$depth" \
    --no-sampler --output "${RESULTS_DIR}/.bssweep-seed.json" >/dev/null
}

committer_ready() { local dsn cj; dsn="$(dsn_for_scenario "$SCENARIO")"; cj="${RESULTS_DIR}/.bssweep-canary.json"
  timeout 90 "$BIN" --dsn "$dsn" run --scenario "$SCENARIO" --mode routed --duration 3s --max-callers 8 \
    --depth "$(depth_for "$SCENARIO")" --no-sampler --output "$cj" >/dev/null 2>&1 || true
  python3 -c "import json;print(json.load(open('$cj'))['routed']['trx_committed_total'])" 2>/dev/null | grep -qvx 0; }

build_harness
log "=== batch-diag sweep: scenario=$SCENARIO committers=$COMMITTERS sizes=[$SIZES] reps=$REPS window=${WINDOW}us pack=$PACK affinity=${AFFINITY_SCHEME}/steal${AFFINITY_STEAL_MS}ms sampler=$SAMPLER dur=$DUR ==="
asys committer_count "$COMMITTERS"
clean_seed
running="$(docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'ledger_routed_c_committer%'" | tr -d '[:space:]')"
log "committers running: $running (requested $COMMITTERS)"
committer_ready || log "WARN: canary did not drain"
asys batch_window_us "$WINDOW"; asys router_pack_disjoint "$PACK"
asys affinity_scheme "$AFFINITY_SCHEME"; asys affinity_steal_ms "$AFFINITY_STEAL_MS"; reload

dsn="$(dsn_for_scenario "$SCENARIO")"; depth="$(depth_for "$SCENARIO")"
echo "scenario,committers,pack,batch_size_max,window_us,rep,throughput_trx_s,cg_avg,commits_s,locks_per_trx,trx,ack_p50_us,ack_p99_us,dropped,busy_frac,lock_of_busy,running_of_busy,lwlock_of_busy,io_of_busy,top_wait_type,top_wait_event,top_wait_samples,load1_end" > "$OUT"
for sz in $SIZES; do
  asys batch_size_max "$sz"; reload
  for rep in $(seq 1 "$REPS"); do
    wait_for_quiet_host || log "  NOTE: sz=$sz rep=$rep ran busy"
    out="${RESULTS_DIR}/bssweep_${SCENARIO}_cc${COMMITTERS}_sz${sz}_r${rep}.json"
    smp="${out%.json}.sampler.txt"
    log "RUN $SCENARIO cc=$COMMITTERS batch_size_max=$sz rep=$rep/$REPS (load1=$(host_load1))"
    LEDGER_V3_1_PRINT_SAMPLER="$SAMPLER" timeout 360 "$BIN" --dsn "$dsn" run --scenario "$SCENARIO" \
      --mode routed --duration "$DUR" --depth "$depth" --output "$out" >/dev/null 2>&1 \
      || { log "  FAIL sz=$sz rep=$rep"; continue; }
    read -r tput cg cps lpt trx p50 p99 drop <<<"$(extract "$out")"
    # Named committer wait diagnostic from the sampler dump (best-effort).
    if [ "$SAMPLER" = "1" ] && [ -f "$smp" ]; then
      read -r busy lob rob wob iob tw_type tw_event tw_n <<<"$(python3 "$PARSE" "$smp")"
    else
      busy=; lob=; rob=; wob=; iob=; tw_type=none; tw_event=none; tw_n=0
    fi
    le="$(host_load1)"
    echo "$SCENARIO,$COMMITTERS,$PACK,$sz,$WINDOW,$rep,$tput,$cg,$cps,$lpt,$trx,$p50,$p99,$drop,$busy,$lob,$rob,$wob,$iob,$tw_type,$tw_event,$tw_n,$le" >> "$OUT"
    log "  sz=$sz r$rep cg=$cg tput=$tput cmt/s=$cps lk/trx=$lpt ack_p99=${p99}us top_wait=${tw_type}/${tw_event}(${tw_n}) load=$le"
  done
done
# acct-0usf pin check: with affinity on, owned_claims should dominate and steals
# should be ≈0 (cluster-lifetime counters, reset by clean_seed's restart). Nonzero
# steals => the age-gated steal fired => the static pin leaked => run is suspect.
if [ "$AFFINITY_SCHEME" != "0" ]; then
  owned="$(docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT ledger_routed_c_affinity_owned_claims_total()" 2>/dev/null | tr -d '[:space:]')"
  steals="$(docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT ledger_routed_c_affinity_steals_total()" 2>/dev/null | tr -d '[:space:]')"
  log "PIN CHECK (affinity_scheme=$AFFINITY_SCHEME steal=${AFFINITY_STEAL_MS}ms): owned_claims=$owned steals=$steals  (steals≈0 => clean static pin; nonzero => steal leaked, run suspect)"
fi
restore_defaults
log "=== done. CSV: $OUT ==="
python3 "$AGG" "$OUT" | tee -a "$SWEEP_LOG" >&2 || cat "$OUT"
