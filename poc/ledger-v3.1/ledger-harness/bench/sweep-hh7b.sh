#!/usr/bin/env bash
# sweep-hh7b.sh — acct-hh7b Phase 1 driver: time-window-only commit_group
# formation across the pool-overlap spectrum, staged DECISIVE-FIRST (like p1al).
#
# Each scenario is run window-only (batch_size_max non-binding, pack on) via
# sweep-hh7b-window.sh. Ordering puts the two questions that need no committer_count
# axis first (so the core data lands even if the run is cut short), then the
# committer_count BALANCE axis where hh7b's pool-distribution thesis lives:
#
#   1. s2  cc4  — the p1al cap-binding spread (64 pools / 16 callers). Q1: does
#                 dropping the doc cap (window-only) recover/beat cg=148@cap200?
#                 Fine window grid for the cg-vs-window curve.
#   2. s7  cc4  — deep zipf, 1000 callers. Highest arrival x widest window =>
#                 the FAILURE-MODE probe: does an uncapped window form an
#                 unbounded group (max_group / arena_bump / ack_p99 blowup)?
#   3. s5  cc{1,2,4} — single hot pool, max overlap. HYPOTHESIS: fewer committers
#                 match/beat more (committers can't parallelize one pool; larger
#                 batches maximize per-pool coalescing).
#   4. s6  cc{2,4,8} — disjoint stripes. CONTRAST: committers DO parallelize, so
#                 higher cc should help (235v: 958->1234->1791 trx/s cc2/4/8).
#   5. s10 cc{1,4,8} — Pareto intertwined (~13.5 pools/receipt). HYPOTHESIS: like
#                 s5, intertwined work favors fewer committers + larger windows.
#
# WINDOWS use a finer grid on the curve/failure-mode scenarios (s2,s7) and a
# coarser one on the committer_count-axis scenarios (s5,s6,s10) to bound runtime.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE="$HERE/sweep-hh7b-window.sh"

export REPS="${REPS:-2}" DUR="${DUR:-20s}"
# Short load-gate timeout: host is noisy (chrome up); proceed-and-flag per the
# acct-hh7b run decision rather than stalling 10 min per cell on a load spike.
export LOAD_GATE_TIMEOUT="${LOAD_GATE_TIMEOUT:-45}"

FINE="0 500 2000 10000 50000"
COARSE="0 2000 10000 50000"

run() { echo "[hh7b-driver] $(date -u +%H:%M:%S) START $*" >&2
  ( "$@" ) && echo "[hh7b-driver] $(date -u +%H:%M:%S) DONE $*" >&2 \
          || echo "[hh7b-driver] $(date -u +%H:%M:%S) FAIL $* (continuing)" >&2; }

# 1. s2 — p1al cap-binding spread (64 pools, 16 callers). Window-vs-cap core.
run env SCENARIO=s2 COMMITTERS="4" WINDOWS="$FINE" CALLERS=16 \
       SEED_COUNT=64 SEED_SKUS=64 SEED_LOCS=1 bash "$ENGINE"

# 2. s7 — deep zipf, 1000 callers. Failure-mode probe (uncapped group growth).
run env SCENARIO=s7 COMMITTERS="4" WINDOWS="$FINE" bash "$ENGINE"

# 3. s5 — single hot pool, max overlap. Fewer-committers hypothesis.
run env SCENARIO=s5 COMMITTERS="1 2 4" WINDOWS="$COARSE" bash "$ENGINE"

# 4. s6 — disjoint stripes. More-committers contrast.
run env SCENARIO=s6 COMMITTERS="2 4 8" WINDOWS="$COARSE" bash "$ENGINE"

# 5. s10 — Pareto intertwined. Fewer-committers + larger-window hypothesis.
run env SCENARIO=s10 COMMITTERS="1 4 8" WINDOWS="$COARSE" bash "$ENGINE"

echo "[hh7b-driver] $(date -u +%H:%M:%S) ALL SCENARIOS COMPLETE" >&2
