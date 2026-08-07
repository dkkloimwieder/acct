#!/usr/bin/env bash
# acct-0at4.10.3 (C) — drift-detection soak (FEEDBACK-TESTING #11).
#
# WHY THIS EXISTS / WHAT IT IS NOT
#   30 s runs never cross a checkpoint, accumulate WAL, burn a visible number of
#   xids, cycle autovacuum, or reveal arena / metadata bloat — an arena leak was
#   once found by code reading, not by any test. This runs a SUSTAINED routed
#   workload and samples the cluster's *drift* signals over time.
#
#   It is deliberately NOT an absolute-throughput soak: this is a noisy
#   daily-driver host (project_pocv3_bench_host_is_noisy_workstation), where a
#   multi-hour throughput/p99 curve is dominated by background load, not
#   architecture. So the load is driven OPEN-LOOP at a fixed arrival rate
#   (--target-rate): xid burn, WAL bytes, and table growth are then ~constant
#   per minute regardless of host noise (the committer keeps up; only latency
#   absorbs load), and the DRIFT SLOPES — which is the whole deliverable — are
#   load-robust. Absolute committed-latency is recorded but not trusted.
#
# WHAT IT SAMPLES (every INTERVAL s -> timestamped CSV row)
#   age(datfrozenxid) of poc_v3_1  ── ARCH-2 xid-burn slope (enqueue forces a real
#                                     xid via GetCurrentFullTransactionId), and
#                                     whether anti-wraparound autovacuum freezes
#                                     it (a downward step in the series).
#   WAL insert LSN (abs bytes)     ── WAL accumulation rate.
#   arena_outstanding/allocs/frees/bump/freelist ── committer shmem arena LEAK
#                                     check: outstanding must stay bounded, not
#                                     climb monotonically.
#   pool_state / pool_lock rowcount ── routed-metadata BLOAT (should stabilize).
#   committer trx_committed        ── throughput sawtooth (delta/interval).
#   pg_stat_checkpointer (num_timed / num_requested / buffers_written)
#   pg_stat_bgwriter    (buffers_clean / buffers_alloc)   ── checkpoint sawtooth.
#   host load1                     ── to correlate any latency dips with host noise.
#
#   Post-run, drift-analyze.py fits a per-minute linear slope (+ R²) to every
#   column and writes results/drift-soak-<ts>.md (slope table + ASCII sparklines
#   + xid-wraparound extrapolation) alongside the raw results/drift-soak-<ts>.csv.
#   matplotlib is absent in-tree, so the slope table + sparklines ARE the plot.
#
# CLUSTER TOUCH: restart acct-postgres (clean arena/committer baseline) + reseed.
# Does NOT DROP/CREATE poc_v3_1 (avoids the committer-wedge; the drift slopes need
# a clean *arena* baseline, which the restart gives — arena counters are shmem).
#
# USAGE / ENV
#   bash bench/run-drift-soak.sh                 # 20 min, s2 routed @ 500 trx/s
#   DUR=7200 TARGET_RATE=800 bash bench/run-drift-soak.sh   # 2 h quiet-host soak
#
#   SCEN(s2), DUR(1200 s), TARGET_RATE(500; empty => closed-loop full blast),
#   MAX_CALLERS(64), INTERVAL(5 s), SEED_COUNT(500)/SEED_SKUS(200)/SEED_LOCS(10),
#   METHOD(all-wac), DEPTH(0), OUT_CSV/OUT_MD (results/drift-soak-<ts>.{csv,md}).

set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

SCEN="${SCEN:-s2}"
DUR="${DUR:-1200}"
TARGET_RATE="${TARGET_RATE:-500}"
MAX_CALLERS="${MAX_CALLERS:-64}"
INTERVAL="${INTERVAL:-5}"
METHOD="${METHOD:-all-wac}"
DEPTH="${DEPTH:-0}"
SEED_COUNT="${SEED_COUNT:-500}"
SEED_SKUS="${SEED_SKUS:-200}"
SEED_LOCS="${SEED_LOCS:-10}"
TS="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
OUT_CSV="${OUT_CSV:-$RESULTS_DIR/drift-soak-${TS}.csv}"
OUT_MD="${OUT_MD:-$RESULTS_DIR/drift-soak-${TS}.md}"
RUN_JSON="$RESULTS_DIR/.drift-soak-run.json"
RUNLOG="$RESULTS_DIR/.drift-soak-run.log"

command -v python3 >/dev/null || { echo "FATAL: python3 required" >&2; exit 2; }
DBNAME="${DIRECT_DSN##*/}"
psql_row() { docker exec "$CONTAINER" psql -U acct -d "$DBNAME" -tAc "$1" 2>/dev/null; }

# One row of drift metrics, pipe-delimited (psql -A default sep). Column order is
# FIXED — drift-analyze.py and the CSV header below both depend on it. All reads
# are shmem counters or cheap catalog lookups; pool_state/pool_lock count(*) is a
# few-thousand-row scan, negligible at INTERVAL cadence.
SAMPLE_SQL="SELECT
  (SELECT age(datfrozenxid) FROM pg_database WHERE datname='${DBNAME}'),
  pg_wal_lsn_diff(pg_current_wal_insert_lsn(),'0/0')::bigint,
  ledger_routed_c_arena_outstanding(),
  ledger_routed_c_arena_total_allocs(),
  ledger_routed_c_arena_total_frees(),
  ledger_routed_c_arena_bump_offset(),
  ledger_routed_c_arena_freelist_count(),
  ledger_routed_c_committer_trx_committed_total(),
  (SELECT count(*) FROM pool_state),
  (SELECT count(*) FROM pool_lock),
  (SELECT num_timed FROM pg_stat_checkpointer),
  (SELECT num_requested FROM pg_stat_checkpointer),
  (SELECT buffers_written FROM pg_stat_checkpointer),
  (SELECT buffers_clean FROM pg_stat_bgwriter),
  (SELECT buffers_alloc FROM pg_stat_bgwriter)"

CSV_HEADER="t_epoch,t_s,load1,age_xid,wal_bytes,arena_outstanding,arena_allocs,arena_frees,arena_bump,arena_freelist,trx_committed,pool_state_rows,pool_lock_rows,ckpt_timed,ckpt_req,ckpt_buffers,bgw_clean,bgw_alloc"

build_harness

echo "==> drift soak: scen=$SCEN dur=${DUR}s rate=${TARGET_RATE:-fullblast} callers=$MAX_CALLERS interval=${INTERVAL}s"
echo "==> host load: $(cat /proc/loadavg)"

# Clean arena/committer/shmem baseline (does NOT wipe the data volume).
restart_db
assert_routed_gucs || { echo "FATAL: routed GUC drift" >&2; exit 2; }

# pgbouncer up (routed 200-caller path). Provision if down.
if ! psql "$POOLER_DSN" -c 'SELECT 1' >/dev/null 2>&1; then
    echo "==> pgbouncer down — provisioning"; bash "$HERE/setup-pgbouncer.sh" up >/dev/null 2>&1
fi

echo "==> seed $SEED_COUNT pools ($METHOD, depth=$DEPTH) via direct dsn"
harness --dsn "$DIRECT_DSN" run --scenario "$SCEN" --mode direct-per-call --duration 1s \
    --max-callers 1 --depth "$DEPTH" --method-mix "$METHOD" \
    --seed-count "$SEED_COUNT" --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" \
    --seed-depth "$DEPTH" --no-sampler --output "$RESULTS_DIR/.drift-seed.json" >/dev/null 2>&1 \
    || { echo "FATAL: seed failed" >&2; exit 2; }

# Committer canary: a short routed run must drain, else the arena is wedged and
# the soak would sample a dead committer.
timeout 60 "$BIN" --dsn "$POOLER_DSN" run --scenario "$SCEN" --mode routed --duration 3s \
    --max-callers 8 --depth "$DEPTH" --no-sampler --output "$RESULTS_DIR/.drift-canary.json" >/dev/null 2>&1 || true
cmt="$(grep -oE '"trx_committed_total"[[:space:]]*:[[:space:]]*[0-9]+' "$RESULTS_DIR/.drift-canary.json" 2>/dev/null | grep -oE '[0-9]+$' | head -1)"
[ "${cmt:-0}" -gt 0 ] && echo "==> committer canary ok (trx_committed=$cmt)" || echo "WARN: committer canary drained 0 — soak may sample a wedged committer"

# ── launch driver (open-loop) + time-series sampler ──────────────────────────
echo "$CSV_HEADER" > "$OUT_CSV"
RATE_ARG=(); [ -n "$TARGET_RATE" ] && RATE_ARG=(--batch-size 1 --target-rate "$TARGET_RATE")

echo "==> launching routed soak (${DUR}s) + ${INTERVAL}s sampler -> $OUT_CSV"
"$BIN" --dsn "$POOLER_DSN" run --scenario "$SCEN" --mode routed --duration "${DUR}s" \
    --max-callers "$MAX_CALLERS" --depth "$DEPTH" "${RATE_ARG[@]}" \
    --no-sampler --output "$RUN_JSON" > "$RUNLOG" 2>&1 &
HPID=$!

T0="$(date +%s)"
sampler() {
    while kill -0 "$HPID" 2>/dev/null; do
        local now load1 row
        now="$(date +%s)"
        load1="$(awk '{print $1}' /proc/loadavg)"
        row="$(psql_row "$SAMPLE_SQL" | tr '|' ',')"
        [ -n "$row" ] && echo "${now},$((now-T0)),${load1},${row}" >> "$OUT_CSV"
        sleep "$INTERVAL"
    done
}
sampler & SPID=$!

wait "$HPID"; HRC=$?
kill "$SPID" 2>/dev/null || true; wait "$SPID" 2>/dev/null || true
echo "==> soak exited rc=$HRC; host load: $(cat /proc/loadavg)"
[ "$HRC" -eq 0 ] || { echo "WARN: harness rc=$HRC — see $RUNLOG"; tail -5 "$RUNLOG"; }

nsamp="$(($(wc -l < "$OUT_CSV") - 1))"
echo "==> $nsamp samples -> $OUT_CSV"

# ── analyze: per-metric drift slope + sparklines -> tracked .md ───────────────
ACHIEVED="$(grep -oE '"throughput_trx_per_sec"[[:space:]]*:[[:space:]]*[0-9.]+' "$RUN_JSON" 2>/dev/null | grep -oE '[0-9.]+$' | head -1)"
python3 "$HERE/drift-analyze.py" "$OUT_CSV" "$OUT_MD" \
    "scen=$SCEN dur=${DUR}s rate=${TARGET_RATE:-fullblast} callers=$MAX_CALLERS achieved=${ACHIEVED:-?}trx/s interval=${INTERVAL}s ts=$TS" \
    || { echo "FATAL: analyze failed" >&2; exit 2; }
echo "==> report: $OUT_MD"
echo "==> csv:    $OUT_CSV"
