#!/usr/bin/env bash
# probe-hh7b-rwsize.sh — acct-hh7b Phase 1 escalation probe.
#
# The window sweep showed max_group pegged at exactly 1000 in every cell: with
# batch_size_max non-binding, router_window_size (the per-tick scan budget,
# default 1000) is the REAL group-size ceiling. This probe confirms that — and
# answers "is a safety ceiling on group size still needed?" — by holding the
# window WIDE (non-gating) on a disjoint, commit-amortizable workload (s6) and
# raising router_window_size past 1000. If max_group tracks router_window_size,
# it is the binding ceiling; the consequence panel (throughput, ack p99,
# committer lock-hold fraction, arena pressure, drops) then shows whether a
# truly-uncapped group RUNS AWAY (lock-hold/tail/memory blowup) or just stops
# helping — i.e. whether router_window_size must stay as a protective ceiling.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"
CONTAINER="${CONTAINER:-acct-postgres}"
SCENARIO="${SCENARIO:-s6}"
COMMITTERS="${COMMITTERS:-4}"
WINDOW="${WINDOW:-50000}"                 # wide => window never gates; backlog+rwsize set group size
RWSIZES="${RWSIZES:-1000 2000 4000 8000}"
REPS="${REPS:-2}"
DUR="${DUR:-20s}"
SEED_SKUS="${SEED_SKUS:-1000}"; SEED_LOCS="${SEED_LOCS:-10}"; SEED_COUNT="${SEED_COUNT:-10000}"
export LOAD_GATE_TIMEOUT="${LOAD_GATE_TIMEOUT:-45}"
OUT="${OUT:-${RESULTS_DIR}/hh7b_rwsize_probe_${SCENARIO}.csv}"
LOG="${RESULTS_DIR}/hh7b_window.log"
PARSE="$HERE/parse-committer-sampler.py"
log() { echo "[hh7b-rws] $*" | tee -a "$LOG" >&2; }
depth_for() { case "$1" in s5|s6) echo 10 ;; s7|s8|s9) echo 1000 ;; s11|s15|s19) echo 100 ;; s14|s16|s17|s18|s20|s21) echo 10 ;; *) echo 0 ;; esac; }
asys() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.$1 = $2" >/dev/null; }
reload() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT pg_reload_conf()" >/dev/null; }
restore_defaults() { asys batch_size_max 200; asys batch_window_us 500; asys committer_count 4; asys router_pack_disjoint on; asys router_window_size 1000; reload 2>/dev/null||true; }
trap restore_defaults EXIT
drop_db_robust() { local i; for i in $(seq 1 10); do
    docker exec "$CONTAINER" psql -U acct -d postgres -tAc "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='poc_v3_1' AND pid<>pg_backend_pid()" >/dev/null 2>&1||true
    docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2 2>/dev/null && return 0
    log "  drop_db retry $i/10"; sleep 1; done
    docker exec "$CONTAINER" psql -U acct -d postgres -c "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2; }
extract() { python3 - "$1" <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); r=d['routed']; a=d['ack_latency_us']; dur=d['duration_secs']
trx=r['trx_committed_total']; lk=r['pool_lock_acquisitions_total']
print(f"{d['throughput_trx_per_sec']:.0f} {r['commit_group_size_avg']:.2f} {(lk/trx if trx else 0):.2f} {trx} {a['p50']} {a['p99']} {r['dropped_submissions_total']}")
PY
}
read_getters() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT ledger_routed_c_router_max_group_size(), ledger_routed_c_router_submission_histogram(), ledger_routed_c_arena_outstanding(), ledger_routed_c_arena_bump_offset(), ledger_routed_c_committer_dropped_submissions_total()"; }

build_harness
depth="$(depth_for "$SCENARIO")"; dsn="$(dsn_for_scenario "$SCENARIO")"
log "=== rwsize escalation: scenario=$SCENARIO cc=$COMMITTERS window=${WINDOW}us(wide) rwsizes=[$RWSIZES] reps=$REPS dur=$DUR ==="
asys committer_count "$COMMITTERS"; reload
drop_db_robust
docker exec "$CONTAINER" psql -U acct -d postgres -c "CREATE DATABASE poc_v3_1" >&2
( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2||true
docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
restart_db
"$BIN" --dsn "$DIRECT_DSN" run --scenario "$SCENARIO" --mode direct-per-call --duration 1s --max-callers 1 \
  --depth "$depth" --method-mix all-fifo --seed-count "$SEED_COUNT" --seed-skus "$SEED_SKUS" \
  --seed-locations "$SEED_LOCS" --seed-depth "$depth" --no-sampler --output "${RESULTS_DIR}/.rws-seed.json" >/dev/null
asys batch_size_max 100000; asys router_pack_disjoint on; asys batch_window_us "$WINDOW"; reload

echo "scenario,committers,router_window_size,window_us,rep,throughput_trx_s,cg_avg,locks_per_trx,trx,ack_p50_us,ack_p99_us,dropped,max_group,group_hist,arena_outstanding,arena_bump,busy_frac,lock_of_busy,lwlock_of_busy,io_of_busy,top_wait_type,top_wait_event,load1_end" > "$OUT"
for rws in $RWSIZES; do
  asys router_window_size "$rws"; reload
  for rep in $(seq 1 "$REPS"); do
    wait_for_quiet_host || log "  NOTE: rws=$rws rep=$rep ran busy"
    out="${RESULTS_DIR}/rwsprobe_${SCENARIO}_rws${rws}_r${rep}.json"; smp="${out%.json}.sampler.txt"
    log "RUN $SCENARIO cc=$COMMITTERS router_window_size=$rws rep=$rep/$REPS (load1=$(host_load1))"
    LEDGER_V3_1_PRINT_SAMPLER=1 timeout "$HARNESS_TIMEOUT" "$BIN" --dsn "$dsn" run --scenario "$SCENARIO" \
      --mode routed --duration "$DUR" --depth "$depth" --output "$out" >/dev/null 2>&1 || { log "  FAIL rws=$rws rep=$rep"; continue; }
    read -r tput cg lpt trx p50 p99 drop <<<"$(extract "$out")"
    IFS='|' read -r maxg hist aout abump drptot <<<"$(read_getters)"; hist="${hist//,/;}"
    if [ -f "$smp" ]; then read -r busy lob rob wob iob tw_type tw_event tw_n <<<"$(python3 "$PARSE" "$smp")"
    else busy=; lob=; wob=; iob=; tw_type=none; tw_event=none; fi
    le="$(host_load1)"
    echo "$SCENARIO,$COMMITTERS,$rws,$WINDOW,$rep,$tput,$cg,$lpt,$trx,$p50,$p99,$drop,$maxg,$hist,$aout,$abump,$busy,$lob,$wob,$iob,$tw_type,$tw_event,$le" >> "$OUT"
    log "  rws=$rws r$rep cg=$cg tput=$tput max_grp=$maxg ack_p99=$((p99/1000))ms lk/trx=$lpt arena_bump=$abump drop=$drop lock_of_busy=$lob wait=${tw_type}/${tw_event} load=$le"
  done
done
restore_defaults
log "=== done. CSV: $OUT ==="
column -s, -t "$OUT" 2>/dev/null || cat "$OUT"
