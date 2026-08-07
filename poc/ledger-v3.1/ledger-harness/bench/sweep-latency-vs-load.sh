#!/usr/bin/env bash
# sweep-latency-vs-load.sh — acct-at8x: latency vs offered load (the SLO curve).
#
# acct-hh7b measured latency only under full-blast open-loop overload, where
# multi-second ack p50 is staging-ring admission backpressure at saturation —
# not intrinsic latency. This sweep paces the caller pool (--target-rate,
# absolute-schedule interval pacing, production single-push --batch-size 1) and
# walks offered rate from well below to past the known ceiling, capturing at
# each rate BOTH latency surfaces:
#   ack_latency_us        admission: the ledger_enqueue_trx_c call returning
#   committed_latency_us  full pipeline: enqueue -> observed materialize
# plus achieved-vs-offered rate, cg, drops. GUCs stay at PRODUCTION defaults
# (cc=4, batch_size_max=200, batch_window_us=500, pack=on): this measures what
# the shipped config delivers, i.e. where the latency knee sits and what the max
# sustainable rate is under a p99 SLO band.
#
# Env: SCENARIO, RATES (space list, trx/s), CALLERS (override), REPS, DUR,
#      SEED_COUNT/SEED_SKUS/SEED_LOCS.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"
CONTAINER="${CONTAINER:-acct-postgres}"
SCENARIO="${SCENARIO:-s10}"
RATES="${RATES:-100 250 500 750 1000 1250 1500}"
CALLERS="${CALLERS:-}"
REPS="${REPS:-2}"
DUR="${DUR:-20s}"
SEED_SKUS="${SEED_SKUS:-1000}"; SEED_LOCS="${SEED_LOCS:-10}"; SEED_COUNT="${SEED_COUNT:-10000}"
export LOAD_GATE_TIMEOUT="${LOAD_GATE_TIMEOUT:-45}"
OUT="${OUT:-${RESULTS_DIR}/latency_vs_load_${SCENARIO}.csv}"
LOG="${RESULTS_DIR}/latency_vs_load.log"
log() { echo "[lat-vs-load] $*" | tee -a "$LOG" >&2; }
depth_for() { case "$1" in s5|s6) echo 10 ;; s7|s8|s9) echo 1000 ;; s11|s15|s19) echo 100 ;; s14|s16|s17|s18|s20|s21) echo 10 ;; *) echo 0 ;; esac; }
asys() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.$1 = $2" >/dev/null; }
reload() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT pg_reload_conf()" >/dev/null; }
production_defaults() { asys batch_size_max 200; asys batch_window_us 500; asys committer_count 4; asys router_pack_disjoint on; asys router_window_size 1000; reload 2>/dev/null||true; }
trap production_defaults EXIT
drop_db_robust() { local i; for i in $(seq 1 10); do
    docker exec "$CONTAINER" psql -U acct -d postgres -tAc "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='poc_v3_1' AND pid<>pg_backend_pid()" >/dev/null 2>&1||true
    docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2 2>/dev/null && return 0
    log "  drop_db retry $i/10"; sleep 1; done
    docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2; }
extract() { python3 - "$1" <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); r=d['routed']; a=d['ack_latency_us']; c=d['committed_latency_us']
trx=r['trx_committed_total']; dr=r['drains_total']; lk=r['pool_lock_acquisitions_total']
print(f"{d['throughput_trx_per_sec']:.0f} {a['p50']} {a['p99']} {c['p50']} {c['p99']} "
      f"{r['commit_group_size_avg']:.2f} {(lk/trx if trx else 0):.2f} {trx} {r['dropped_submissions_total']} {d.get('errors_total',0)}")
PY
}

build_harness
depth="$(depth_for "$SCENARIO")"; dsn="$(dsn_for_scenario "$SCENARIO")"
caller_arg=(); [ -n "$CALLERS" ] && caller_arg=(--max-callers "$CALLERS")
log "=== latency-vs-load: scenario=$SCENARIO callers=${CALLERS:-default} rates=[$RATES] reps=$REPS dur=$DUR (production GUC defaults) ==="
production_defaults
drop_db_robust
docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2||true
docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
restart_db
"$BIN" --dsn "$DIRECT_DSN" run --scenario "$SCENARIO" --mode direct-per-call --duration 1s --max-callers 1 \
  --depth "$depth" --method-mix all-fifo --seed-count "$SEED_COUNT" --seed-skus "$SEED_SKUS" \
  --seed-locations "$SEED_LOCS" --seed-depth "$depth" --no-sampler --output "${RESULTS_DIR}/.lvl-seed.json" >/dev/null
# committer-readiness canary
cj="${RESULTS_DIR}/.lvl-canary.json"
timeout 90 "$BIN" --dsn "$dsn" run --scenario "$SCENARIO" --mode routed --duration 3s --max-callers 8 \
  --depth "$depth" --no-sampler --output "$cj" >/dev/null 2>&1 || true
python3 -c "import json;print(json.load(open('$cj'))['routed']['trx_committed_total'])" 2>/dev/null | grep -qvx 0 \
  || log "WARN: canary did not drain"

echo "scenario,callers,offered_rate,rep,achieved_tput,ack_p50_us,ack_p99_us,committed_p50_us,committed_p99_us,cg_avg,locks_per_trx,trx,dropped,errors,load1_end" > "$OUT"
for rate in $RATES; do
  for rep in $(seq 1 "$REPS"); do
    wait_for_quiet_host || log "  NOTE: rate=$rate rep=$rep ran busy"
    out="${RESULTS_DIR}/lvl_${SCENARIO}_r${rate}_rep${rep}.json"
    log "RUN $SCENARIO offered=$rate trx/s rep=$rep/$REPS (load1=$(host_load1))"
    timeout "$HARNESS_TIMEOUT" "$BIN" --dsn "$dsn" run --scenario "$SCENARIO" --mode routed \
      --duration "$DUR" --depth "$depth" --batch-size 1 --target-rate "$rate" "${caller_arg[@]}" \
      --no-sampler --output "$out" >/dev/null 2>&1 || { log "  FAIL rate=$rate rep=$rep"; continue; }
    read -r tput ap50 ap99 cp50 cp99 cg lpt trx drop err <<<"$(extract "$out")"
    le="$(host_load1)"
    echo "$SCENARIO,${CALLERS:-def},$rate,$rep,$tput,$ap50,$ap99,$cp50,$cp99,$cg,$lpt,$trx,$drop,$err,$le" >> "$OUT"
    log "  rate=$rate r$rep achieved=$tput ack_p50=$((ap50/1000))ms ack_p99=$((ap99/1000))ms cmt_p50=$((cp50/1000))ms cmt_p99=$((cp99/1000))ms cg=$cg drop=$drop load=$le"
  done
done
production_defaults
log "=== done. CSV: $OUT ==="
column -s, -t "$OUT" 2>/dev/null || cat "$OUT"
