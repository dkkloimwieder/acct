#!/usr/bin/env bash
# run-batch-size-sweep.sh — does making commit_groups LARGER raise routed
# throughput? (acct-xdwk, lever 1). The measured bottleneck (acct-235v) is the
# per-pool `pool_lock FOR UPDATE`: committers serialize on a hot pool's row lock
# (~29ms handoff). Throughput on a contended pool ~= trx-per-group /
# (lock-hold + handoff), so a bigger batch should amortize the handoff over more
# trx. This sweep raises the achievable batch size and measures throughput,
# achieved cg, locks/trx, and ack latency.
#
# Knobs swept LIVE (both GucContext::Sighup — ALTER SYSTEM + pg_reload_conf, no
# restart): ledger_routed_c.batch_size_max (the per-group cap). batch_window_us
# is held WIDE ($WINDOW) so the cap — not the coalesce window — is the binding
# constraint: with a wide window the router accumulates a deep backlog per tick
# and packs up to batch_size_max into each group.
#
# committer_count is set ONCE (restart-only GUC); COMMITTERS=1 = "single drain",
# which removes cross-committer FOR UPDATE contention entirely, isolating the
# pure batching/commit-amortization effect from the affinity problem.
#
# Bench-only. One clean+seed per run (after committer_count is set); load-gated.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

CONTAINER="${CONTAINER:-acct-postgres}"
SCENARIO="${SCENARIO:-s5}"
COMMITTERS="${COMMITTERS:-4}"
SIZES="${SIZES:-1 5 10 25 50 100 200}"
WINDOW="${WINDOW:-20000}"            # batch_window_us held wide so batch_size_max binds
PACK="${PACK:-off}"                   # router_pack_disjoint: on packs disjoint pool-components (acct-xdwk lever 1b)
DUR="${DUR:-20s}"
SEED_SKUS="${SEED_SKUS:-1000}"; SEED_LOCS="${SEED_LOCS:-10}"; SEED_COUNT="${SEED_COUNT:-10000}"
OUT="${OUT:-${RESULTS_DIR}/batch_size_sweep_${SCENARIO}_cc${COMMITTERS}_pack${PACK}.csv}"
SWEEP_LOG="${RESULTS_DIR}/batch_size_sweep.log"
log() { echo "[bssweep] $*" | tee -a "$SWEEP_LOG" >&2; }
depth_for() { case "$1" in s5|s6) echo 10 ;; s7|s8|s9) echo 1000 ;; s11|s15|s19) echo 100 ;; s14|s16|s17|s18|s20|s21) echo 10 ;; *) echo 0 ;; esac; }

asys() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "ALTER SYSTEM SET ledger_routed_c.$1 = $2" >/dev/null; }
reload() { docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT pg_reload_conf()" >/dev/null; }
restore_defaults() {
  asys batch_size_max 50 2>/dev/null || true
  asys batch_window_us 500 2>/dev/null || true
  asys committer_count 4 2>/dev/null || true
  asys router_pack_disjoint off 2>/dev/null || true
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
log "=== batch_size sweep: scenario=$SCENARIO committers=$COMMITTERS sizes=[$SIZES] window=${WINDOW}us pack=$PACK dur=$DUR ==="
asys committer_count "$COMMITTERS"
clean_seed
running="$(docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -tAc "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'ledger_routed_c_committer%'" | tr -d '[:space:]')"
log "committers running: $running (requested $COMMITTERS)"
committer_ready || log "WARN: canary did not drain"
asys batch_window_us "$WINDOW"; asys router_pack_disjoint "$PACK"; reload

dsn="$(dsn_for_scenario "$SCENARIO")"; depth="$(depth_for "$SCENARIO")"
echo "scenario,committers,pack,batch_size_max,window_us,throughput_trx_s,cg_avg,commits_s,locks_per_trx,trx,ack_p50_us,ack_p99_us,dropped,load1_end" > "$OUT"
for sz in $SIZES; do
  asys batch_size_max "$sz"; reload
  wait_for_quiet_host || log "  NOTE: size=$sz ran busy"
  out="${RESULTS_DIR}/bssweep_${SCENARIO}_cc${COMMITTERS}_sz${sz}.json"
  log "RUN $SCENARIO cc=$COMMITTERS batch_size_max=$sz (load1=$(host_load1))"
  timeout 360 "$BIN" --dsn "$dsn" run --scenario "$SCENARIO" --mode routed --duration "$DUR" \
    --depth "$depth" --output "$out" >/dev/null 2>&1 || { log "  FAIL sz=$sz"; continue; }
  read -r tput cg cps lpt trx p50 p99 drop <<<"$(extract "$out")"
  echo "$SCENARIO,$COMMITTERS,$PACK,$sz,$WINDOW,$tput,$cg,$cps,$lpt,$trx,$p50,$p99,$drop,$(host_load1)" >> "$OUT"
  log "  sz=$sz cg=$cg tput=$tput commits/s=$cps locks/trx=$lpt ack_p99=${p99}us"
done
restore_defaults
log "=== done. CSV: $OUT ==="
column -s, -t "$OUT" >&2 || cat "$OUT"
