#!/usr/bin/env bash
# run-routed-longdur.sh — long-duration routed-ONLY cleanliness sweep (acct-0516).
#
# Drives every scenario S1-S21 through *routed* mode at TWO durations (1m, then
# 10m) and surfaces any cell where the 10m verdict differs structurally from the
# 1m verdict (new non-zero counter, order-of-magnitude ratio shift, superlinear
# p99 growth, drains/s shift, end-of-drain unseen). Direct-per-call and
# direct-batched are deliberately dropped — their committer/shmem path is absent
# so they do not develop new failure modes with duration; their verdicts already
# live in POC-REPORT.md artifact (b) (run-crossover.sh).
#
# Builds on common.sh: build_harness, dsn_for_scenario, restart_db, $BIN,
# $DIRECT_DSN, $POOLER_DSN, $RESULTS_DIR, $WS_DIR, $CONTAINER.
#
# --- Design decisions (surfaced + chosen at claim time, acct-0516) ---
#
#  (a) Clean-DB = a2 (DROP/CREATE DATABASE + reapply migrations + re-CREATE
#      EXTENSION) ONCE per scenario before the 1m run, PLUS a postmaster
#      restart. The restart is NOT cosmetic — see the FINDING below. a2 surfaces
#      migration / extension-init bugs (its reason for being chosen over a1);
#      the restart cold-inits the cluster-lifetime shmem arena so the routed
#      committer actually comes back. We do NOT cargo-rebuild the extension per
#      iteration (the .so is installed cluster-wide; BUILD_EXT=1 to do it once).
#
#      FINDING (diagnosed 2026-05-28, acct-0516): pure a2 WEDGES the routed
#      committer. The router/committer/recovery workers are
#      shared_preload_libraries BGWorkers and the staging/commit shmem arena
#      lives for the CLUSTER lifetime. `DROP DATABASE WITH (FORCE)` severs the
#      committer's SPI connection and orphans the in-arena staging entries, but
#      does NOT clear the arena. The committer never resumes against the
#      recreated DB; orphaned entries jam the staging queue (observed: 16384
#      pending, 0 drains, then new enqueues fail because staging is full) — and
#      it does NOT recover on its own (still 0 drains minutes later). A
#      postmaster restart (docker restart) re-execs _PG_init, which re-creates
#      the arena COLD and respawns a fresh committer that attaches to the
#      recreated poc_v3_1. (This is also why a1's docker-restart clean keeps
#      routed healthy in run-crossover.sh; the routed-c README's "only a3 clears
#      the arena" is imprecise — any postmaster restart clears it; a3 only adds
#      an on-disk wipe.) So the clean here = DROP/CREATE + migrations +
#      CREATE EXTENSION + docker restart. This is bench methodology, not a
#      production defect (you would not DROP DATABASE under a live committer in
#      prod); recorded in POC-REPORT.md §(g), not filed as a bug.
#
#  (b) Run ordering FIXED by user: per scenario, clean -> 1m -> (NO clean) ->
#      10m -> next. The 10m inherits the 1m terminal state so accumulated state
#      (drift, pool sequencing, arena pressure, queue back-pressure) can
#      surface. The timed runs therefore NEVER pass --method-mix (that would
#      TRUNCATE+reseed and wipe the universe). Only the once-per-scenario warmup
#      seeds.
#
#  (c) Seed depth = crossover depths (user choice after the c1 premise was shown
#      false). Each FIFO layer is seeded with PER_LAYER_QTY = 1e9 units
#      (seed.rs) and depletions are 1-100 units (workload.rs), so a pool cannot
#      exhaust in 11 min at ANY depth — even depth 1 is ~2h of headroom at full
#      throughput — and routed/provisional mode never walks layers anyway. c1's
#      deep seeds (s5=100000, s7-9=10000 ≈ 1e8 rows / ~10GB/scenario) would buy
#      nothing. depth_for() mirrors run-crossover.sh exactly, preserving
#      comparability to artifact (b). Override any cell with SEED_PLAN_<sid>.
#
#  (d) pgbouncer: s5-s9 (1000 callers) route via :6432 (dsn_for_scenario).
#      ensure_pgbouncer() verifies it before the sweep; the per-cell readiness
#      probe re-establishes pooled backends after each clean+restart.
#
# Bench-only. Touches no production code. If a 10m cell uncovers a defect, file
# a fresh bd issue (blocked-by acct-0516) — fixing is OUT of scope here.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/common.sh"

# --- knobs (env-overridable for smoke tests) ---
DUR_SHORT="${DUR_SHORT:-60}"      # "1m" run, seconds
DUR_LONG="${DUR_LONG:-600}"       # "10m" run, seconds
BUILD_EXT="${BUILD_EXT:-0}"       # 1 = cargo-build+install extensions once at start
SEED_COUNT="${SEED_COUNT:-10000}" # pools in the universe (matches crossover)
SEED_SKUS="${SEED_SKUS:-1000}"
SEED_LOCS="${SEED_LOCS:-10}"
CANARY_DUR="${CANARY_DUR:-3}"     # committer-readiness canary run length, seconds
CANARY_RETRIES="${CANARY_RETRIES:-2}"
MODE="routed"
MIG_DIR="$WS_DIR/db/migrations"
SWEEP_LOG="$RESULTS_DIR/longdur_sweep.log"

# Full S1-S21. Override for smoke: SCENARIOS_OVERRIDE="s1 s5".
if [ -n "${SCENARIOS_OVERRIDE:-}" ]; then
  # shellcheck disable=SC2206
  SCENARIOS=( ${SCENARIOS_OVERRIDE} )
else
  SCENARIOS=(s1 s2 s3 s4 s5 s6 s7 s8 s9 s10 s11 s12 s13 s14 s15 s16 s17 s18 s19 s20 s21)
fi

mkdir -p "$RESULTS_DIR"
log() { echo "[longdur] $*" | tee -a "$SWEEP_LOG" >&2; }

# depth_for <sid> — crossover seed depth (run-crossover.sh depth_for). Each
# layer holds 1e9 units, so this is a lock-hold-realism / comparability knob,
# NOT an exhaustion buffer. Override with SEED_PLAN_<sid>="<depth>".
depth_for() {
  local sid="$1" d
  case "$sid" in
    s5|s6)                   d=10 ;;
    s7|s8|s9)                d=1000 ;;
    s11|s15|s19)             d=100 ;;
    s14|s16|s17|s18|s20|s21) d=10 ;;
    *)                       d=0 ;;
  esac
  local ov="SEED_PLAN_${sid}"
  echo "${!ov:-$d}"
}

# method_for <sid> — universe method assignment (run-crossover.sh method_for /
# scenarios.rs expected_method_mix).
method_for() {
  case "$1" in
    s1|s2) echo all-wac ;;
    s3|s4) echo mixed ;;
    *)     echo all-fifo ;;
  esac
}

# clean_db — option a2 + postmaster restart (see header FINDING). Fresh DB
# schema (surfaces migration/ext-init bugs) AND a cold shmem arena + fresh
# committer (so routed actually drains).
clean_db() {
  local nmig; nmig="$(ls "$MIG_DIR"/*.sql 2>/dev/null | wc -l)"
  log "clean(a2): DROP/CREATE poc_v3_1 + $nmig migrations + CREATE EXTENSION + restart"
  docker exec "$CONTAINER" psql -U acct -d postgres -c \
    "DROP DATABASE IF EXISTS poc_v3_1 WITH (FORCE)" >&2
  docker exec "$CONTAINER" psql -U acct -d postgres -c \
    "CREATE DATABASE poc_v3_1" >&2
  ( cd "$WS_DIR" && DATABASE_URL="$DIRECT_DSN" sqlx migrate run --source db/migrations >&2 )
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c \
    "CREATE EXTENSION IF NOT EXISTS ledger_direct_c CASCADE" >&2 || true
  docker exec "$CONTAINER" psql -U acct -d poc_v3_1 -c \
    "CREATE EXTENSION IF NOT EXISTS ledger_routed_c CASCADE" >&2
  # Cold-init the cluster-lifetime arena + respawn the committer against the
  # recreated DB. WITHOUT this, the committer stays wedged and routed drains 0.
  restart_db
}

# wait_dsn_ready <dsn> — probe SELECT 1 until the DSN answers (covers pgbouncer
# re-establishing pooled backends after the restart).
wait_dsn_ready() {
  local dsn="$1" tries=0
  until psql "$dsn" -c 'SELECT 1' >/dev/null 2>&1; do
    tries=$((tries+1))
    if [ "$tries" -gt 120 ]; then
      log "WARN: dsn not ready after 120 probes: ${dsn##*@}"
      return 1
    fi
    sleep 0.5
  done
}

ensure_pgbouncer() {
  local need=0 s
  for s in "${SCENARIOS[@]}"; do
    case "$s" in s5|s6|s7|s8|s9) need=1 ;; esac
  done
  [ "$need" -eq 1 ] || return 0
  if psql "$POOLER_DSN" -c 'SELECT 1' >/dev/null 2>&1; then
    log "pgbouncer reachable (${POOLER_DSN##*@})"
  else
    log "pgbouncer not reachable — running setup-pgbouncer.sh"
    "$HERE/setup-pgbouncer.sh" >&2 || log "WARN: setup-pgbouncer.sh failed; s5-s9 may error"
  fi
}

# seed_universe <sid> — warmup reseed via the direct path (mirrors crossover).
# TRUNCATEs + reseeds SEED_COUNT pools to depth layers with the scenario method.
seed_universe() {
  local sid="$1" depth method
  depth="$(depth_for "$sid")"
  method="$(method_for "$sid")"
  log "seed $sid: count=$SEED_COUNT depth=$depth method=$method"
  timeout 1800 "$BIN" --dsn "$DIRECT_DSN" run \
    --scenario "$sid" --mode direct-per-call --duration 1s \
    --max-callers 1 --depth "$depth" \
    --method-mix "$method" --seed-count "$SEED_COUNT" \
    --seed-skus "$SEED_SKUS" --seed-locations "$SEED_LOCS" --seed-depth "$depth" \
    --no-sampler --output "$RESULTS_DIR/.longdur-seed-$sid.json" >/dev/null
}

# committer_ready <sid> — readiness gate (user choice). Drive a short routed
# canary and confirm the committer drains it (trx_materialized > 0). If 0 (arena
# not cold / committer not attached), docker-restart and retry. Returns nonzero
# if the committer never drains — the timed runs would then be all-zero and that
# is itself a finding.
committer_ready() {
  local sid="$1" attempt=0 mat
  local cj="$RESULTS_DIR/.longdur-canary-$sid.json"
  local cdsn; cdsn="$(dsn_for_scenario "$sid")"
  while [ "$attempt" -le "$CANARY_RETRIES" ]; do
    # Light probe: scenario DSN (pooler for s5-s9) + capped callers — we only
    # need to confirm the committer drains, not reproduce the scenario's load.
    timeout 120 "$BIN" --dsn "$cdsn" run \
      --scenario "$sid" --mode "$MODE" --duration "${CANARY_DUR}s" --max-callers 8 \
      --depth "$(depth_for "$sid")" --no-sampler --output "$cj" >/dev/null 2>&1 || true
    # The --output JSON (report.rs) carries the routed block, not the stdout
    # "trx_materialized" line; readiness = committer committed >0 this window.
    # report.rs writes PRETTY JSON ("key": N with a space), so the regex must
    # tolerate whitespace around the colon — a no-space pattern silently reads 0.
    mat="$(grep -oE '"trx_committed_total"[[:space:]]*:[[:space:]]*[0-9]+' "$cj" 2>/dev/null | head -1 | grep -oE '[0-9]+$' || echo 0)"
    if [ "${mat:-0}" -gt 0 ]; then
      log "  committer canary ok ($sid: trx_committed_total=$mat)"
      return 0
    fi
    attempt=$((attempt+1))
    log "  committer canary drained 0 (attempt $attempt/$CANARY_RETRIES) — restarting + retrying"
    restart_db
    wait_dsn_ready "$DIRECT_DSN" || true
  done
  log "  WARN: committer never drained for $sid — timed cells may be all-zero (FINDING)"
  return 1
}

# run_cell <sid> <dur> <dsn> <depth> — one timed routed run. Records errors and
# returns nonzero on failure WITHOUT aborting the sweep. NOTE: no --method-mix
# (would wipe the seeded universe — decision b).
run_cell() {
  local sid="$1" dur="$2" dsn="$3" depth="$4"
  local out="$RESULTS_DIR/longdur_${sid}_${dur}s.json"
  local err="$RESULTS_DIR/longdur_${sid}_${dur}s.err"
  local to=$((dur + 300))   # duration + drain/settle headroom (NOT common.sh's 600s default)
  # Load gate: acct-postgres shares a daily-driver workstation; external CPU load
  # (Chrome, other agents) swings routed throughput ~2x. Hold each timed run until
  # the host is quiet so the absolute numbers are trustworthy (acct-235v).
  wait_for_quiet_host || log "  NOTE: $sid/${dur}s started on a busy host (load gate timed out)"
  log "RUN $sid / $MODE / ${dur}s (depth=$depth, ${dsn##*@}, timeout=${to}s, load1=$(host_load1)) -> $(basename "$out")"
  if timeout "$to" "$BIN" --dsn "$dsn" run \
      --scenario "$sid" --mode "$MODE" --duration "${dur}s" \
      --depth "$depth" --output "$out" 2> "$err"; then
    rm -f "$err"
    log "  ok: $(basename "$out") (load1_end=$(host_load1))"
    return 0
  else
    log "  FAIL: $sid/${dur}s (rc=$?) — see $(basename "$err")"
    return 1
  fi
}

# --- main sweep ---
log "=== routed-only long-duration sweep (acct-0516) ==="
log "scenarios: ${SCENARIOS[*]}"
log "durations: short=${DUR_SHORT}s long=${DUR_LONG}s | clean=a2+restart | depth=crossover"

build_harness
if [ "$BUILD_EXT" = "1" ]; then
  log "BUILD_EXT=1 — building+installing routed-c once"
  "$WS_DIR/scripts/install-routed-c.sh" >&2 || log "WARN: install-routed-c failed"
fi
ensure_pgbouncer
# Routed-only sweep that never SETs the committer GUCs itself; assert they're at
# production defaults so a stale pin can't silently mislabel the 1m/10m cells.
assert_routed_gucs

FAILS=()
for sid in "${SCENARIOS[@]}"; do
  depth="$(depth_for "$sid")"
  dsn="$(dsn_for_scenario "$sid")"
  log "--- scenario $sid (depth=$depth) ---"

  clean_db
  wait_dsn_ready "$DIRECT_DSN" || log "WARN: $sid direct dsn not ready; attempting seed anyway"
  seed_universe "$sid"
  committer_ready "$sid" || FAILS+=("${sid}/committer-wedged")
  wait_dsn_ready "$dsn" || log "WARN: $sid run dsn not ready; attempting run anyway"

  if [ -n "${DURATIONS:-}" ]; then
    # Single (or custom) duration list per scenario — e.g. DURATIONS="120" for the
    # clean load-gated §(g) re-measurement. Each run gets its own clean+seed above.
    for d in $DURATIONS; do
      run_cell "$sid" "$d" "$dsn" "$depth" || FAILS+=("${sid}/${d}s")
    done
  else
    run_cell "$sid" "$DUR_SHORT" "$dsn" "$depth" || FAILS+=("${sid}/${DUR_SHORT}s")
    # NO clean here — 10m inherits the 1m terminal state (decision b).
    run_cell "$sid" "$DUR_LONG"  "$dsn" "$depth" || FAILS+=("${sid}/${DUR_LONG}s")
  fi
done

log "=== sweep done. results in $RESULTS_DIR ==="
if [ "${#FAILS[@]}" -gt 0 ]; then
  log "CELLS THAT ERRORED (${#FAILS[@]}): ${FAILS[*]}"
  log "(harness/run errors are themselves findings — inspect .err files)"
fi
