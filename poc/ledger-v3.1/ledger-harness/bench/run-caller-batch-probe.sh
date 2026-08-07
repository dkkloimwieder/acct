#!/usr/bin/env bash
# run-caller-batch-probe.sh — acct-ruex: is the LWLock/ledger_v31_staging_queue
# throughput ceiling (acct-0usf.1, commit 4007738) producer-side lock-acquisition
# contention, or downstream drain capacity?
#
# Method: SATURATE the staging ring from the caller side. Each caller pushes a
# HALF-RING batch (~8192 of the 16384-slot ring) per call through the backpressured
# ledger_enqueue_trx_batch_c — one staging-lock acquisition per chunk-that-fits,
# then CV-wait for the committers to free slots. That removes the per-trx ingress
# lock HANDOFFS (where the LWLock contention lives) while keeping the ring full, so
# the residual commit throughput is the pure downstream drain ceiling (router
# grouping + committer commit + WAL). The committer sampler then NAMES that ceiling.
#
# Fixed: router batch_size_max=200 (the commit-group cap), committer_count=4,
# affinity OFF. Swept: caller count 1/2/4. With the ring saturated and committers
# fixed, throughput should be ~constant across caller count (committer-bound); the
# absolute level vs the 200-caller single-push number (~4348) is the answer:
#   >> 4348  => ingress lock handoffs WERE the ceiling; drain has headroom.
#   ~= 4348  => downstream drain was always the ceiling; the lock fix won't help.
# enqueue_errors must stay ~0 (backpressure waits, it doesn't error); nonzero =>
# even a 60s backpressure deadline elapsed = drain badly outpaced (still a result).
#
# Bench-only. One clean+seed; per-arm restart for a clean shmem/committer slate;
# load-gated; GUCs restored on exit. Cluster touch (DROP/CREATE poc_v3_1 + restart
# acct-postgres). Requires ledger_enqueue_trx_batch_c deployed (install-routed-c.sh).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
SCEN="${SCEN:-s2}"                  # Simple receipts: single-pool-per-trx
CALLERS="${CALLERS:-1 2 4}"
CBATCH="${CBATCH:-8192}"            # caller-side batch ≈ half the 16384-slot ring
RBATCH="${RBATCH:-200}"            # router batch_size_max (commit-group cap)
WINDOW="${WINDOW:-20000}"          # router coalesce window
COMMITTERS="${COMMITTERS:-4}"
REPS="${REPS:-5}"
DUR="${DUR:-20s}"
TIMEOUT_MS="${TIMEOUT_MS:-60000}"  # queue_full_timeout_ms: high so saturation backpressure waits, not errors
SEED_COUNT="${SEED_COUNT:-1000}"; SEED_SKUS="${SEED_SKUS:-1000}"; SEED_LOCS="${SEED_LOCS:-1}"
OUT="${OUT:-${RESULTS_DIR}/caller_batch_probe.csv}"
PROBE_LOG="${RESULTS_DIR}/caller_batch_probe.log"
PARSE="$HERE/parse-committer-sampler.py"
log() { echo "[cbprobe] $*" | tee -a "$PROBE_LOG" >&2; }

asys()    { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.$1 = $2" >/dev/null; }
reload()  { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT pg_reload_conf()" >/dev/null; }
psql_v3() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }
restore() {
  asys committer_count 4 2>/dev/null || true
  asys batch_size_max 200 2>/dev/null || true
  asys batch_window_us 500 2>/dev/null || true
  asys affinity_scheme 0 2>/dev/null || true
  asys queue_full_timeout_ms 5000 2>/dev/null || true
  reload 2>/dev/null || true
}
trap restore EXIT

extract() {  # $1=json -> "tput cg p50 p99 trx errors dropped"
  python3 - "$1" <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); r=d['routed']; a=d['ack_latency_us']
print(f"{d['throughput_trx_per_sec']:.0f} {r['commit_group_size_avg']:.2f} {a['p50']} {a['p99']} "
      f"{r['trx_committed_total']} {d.get('errors_total',0)} {r.get('dropped_submissions_total',0)}")
PY
}

clean_seed() {
  docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2
  docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
  ( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
  restart_db   # respawns committer pool at the ALTER SYSTEM committer_count
  "$BIN" --dsn "$DIRECT_DSN" run --scenario "$SCEN" --mode direct-per-call --duration 1s \
    --max-callers 1 --method-mix all-fifo --seed-count "$SEED_COUNT" \
    --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" --seed-depth 0 \
    --no-sampler --output "${RESULTS_DIR}/.cbprobe-seed.json" >/dev/null
}

build_harness
# Pre-flight: the batch entry-point must be deployed (install-routed-c.sh).
if ! docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc \
      "SELECT 1 FROM pg_proc WHERE proname='ledger_enqueue_trx_batch_c'" 2>/dev/null | grep -q 1; then
  log "NOTE: ledger_enqueue_trx_batch_c not found in poc_v3_1 yet — clean_seed's CREATE EXTENSION will define it if the new .so/.sql is deployed."
fi

log "=== caller-batch saturation probe (acct-ruex): scen=$SCEN callers=[$CALLERS] caller_batch=$CBATCH router_batch=$RBATCH committers=$COMMITTERS reps=$REPS dur=$DUR timeout=${TIMEOUT_MS}ms ==="
asys committer_count "$COMMITTERS"
asys batch_size_max "$RBATCH"; asys batch_window_us "$WINDOW"
asys affinity_scheme 0; asys queue_full_timeout_ms "$TIMEOUT_MS"; reload
clean_seed
running="$(psql_v3 "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'ledger_routed_c_committer%'")"
pools="$(psql_v3 "SELECT count(*) FROM pool")"
log "committers running: $running (want $COMMITTERS); pools seeded: $pools"

echo "callers,caller_batch,router_batch,committers,rep,throughput_trx_s,cg_avg,ack_p50_us,ack_p99_us,trx_committed,enqueue_errors,dropped,busy_frac,lock_of_busy,running_of_busy,lwlock_of_busy,io_of_busy,top_wait_type,top_wait_event,top_wait_samples,load1_end" > "$OUT"
dsn="$(dsn_for_scenario "$SCEN")"

for cc in $CALLERS; do
  log "--- callers=$cc (caller_batch=$CBATCH, router_batch=$RBATCH) ---"
  restart_db   # clean shmem/committer/staging slate; seed + GUCs persist
  run="$(psql_v3 "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'ledger_routed_c_committer%'")"
  log "  committers_running=$run (want $COMMITTERS)"
  for rep in $(seq 1 "$REPS"); do
    wait_for_quiet_host || log "  NOTE: callers=$cc r$rep busy host"
    out="${RESULTS_DIR}/.cbprobe_c${cc}_r${rep}.json"; smp="${out%.json}.sampler.txt"
    log "RUN callers=$cc rep=$rep/$REPS (load1=$(host_load1))"
    LEDGER_V3_1_PRINT_SAMPLER=1 timeout 360 "$BIN" --dsn "$dsn" run --scenario "$SCEN" --mode routed \
      --batch-size "$CBATCH" --duration "$DUR" --max-callers "$cc" --output "$out" >/dev/null 2>&1 \
      || { log "  FAIL callers=$cc r$rep"; continue; }
    read -r tput cg p50 p99 trx errs drop <<<"$(extract "$out")"
    if [ -f "$smp" ]; then read -r busy lob rob wob iob tt te tn <<<"$(python3 "$PARSE" "$smp")"
    else busy=; lob=; rob=; wob=; iob=; tt=none; te=none; tn=0; fi
    le="$(host_load1)"
    echo "$cc,$CBATCH,$RBATCH,$COMMITTERS,$rep,$tput,$cg,$p50,$p99,$trx,$errs,$drop,$busy,$lob,$rob,$wob,$iob,$tt,$te,$tn,$le" >> "$OUT"
    log "  callers=$cc r$rep tput=$tput cg=$cg trx=$trx errs=$errs lock=$lob lw=$wob top=${tt}/${te} load=$le"
  done
done
restore
log "=== done. CSV: $OUT ==="
log "    Compare tput across callers (should be ~constant: ring saturated, committers fixed) AND vs ~4348 single-push:"
log "    >> 4348 => ingress lock was the ceiling; ~= 4348 => downstream drain is. top_wait names the drain limiter."
log "    enqueue_errors must be ~0 (clean backpressure); nonzero => 60s deadline elapsed = drain badly outpaced."
column -s, -t "$OUT" >&2 || cat "$OUT"
